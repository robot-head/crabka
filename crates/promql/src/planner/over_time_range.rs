//! Leaf source and `LogicalPlan` assembly for the `*_over_time` range-selector
//! operator path.
//!
//! This is the `*_over_time` sibling of [`super::rate_range`]. It shares the
//! exact `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize ->
//! RangeManipulate` plumbing and differs only in the final projection: instead of
//! a rate-family UDF taking `(timestamp, timestamp_range, value_range, range_ms)`,
//! it projects an `*_over_time` UDF taking `(timestamp, timestamp_range,
//! value_range)` — and, for `quantile_over_time`, a leading `phi` literal.
//!
//! The assembled chain is
//! `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize -> RangeManipulate
//! -> Projection(labels..., prom_<fn>_over_time([phi,] timestamp, timestamp_range,
//! value_range) AS value)`.
//!
//! # Window semantics
//!
//! Identical to the rate path: the window is exactly `(eval_time - range,
//! eval_time]`, left-open and right-closed, with **no** 5m lookback, matching
//! Prometheus matrix-selector semantics and the interpreter's
//! `over_time_sample_from_series` (which filters on `range_start < ts <=
//! range_end`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crabka_blockstore::{Labels, SeriesFingerprint};
use datafusion::catalog::MemTable;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, col, lit};
use datafusion::prelude::SessionContext;

use crate::PromqlError;
use crate::error::Result;
use crate::extension::normalize::SeriesNormalize;
use crate::extension::planner::prom_session_context;
use crate::extension::range_manipulate::{RANGE_SUFFIX, RangeManipulate};
use crate::extension::series_divide::SeriesDivide;
use crate::functions::OverTimeFamily;

/// Leaf-batch column carrying the per-sample timestamp in epoch milliseconds.
pub const TIME_COLUMN: &str = "timestamp";
/// Leaf-batch column carrying the per-sample float value.
pub const VALUE_COLUMN: &str = "value";
/// Projection output column name carrying the `*_over_time` UDF result.
pub const OVER_TIME_VALUE_COLUMN: &str = "value";

/// Resolve a matrix-selector `*_over_time` function name to its operator-path
/// [`OverTimeFamily`] (the float UDF-chain path). Returns `None` for the
/// experimental members (`mad_over_time`, `first_over_time`, the
/// `ts_of_*_over_time` family) — which have no operator-leaf UDF and instead route
/// through the engine's shared `apply_outer_range_fn` kernel — and for any
/// function outside the `*_over_time` set.
#[must_use]
pub fn over_time_family_from_function_name(name: &str) -> Option<OverTimeFamily> {
    match name {
        "sum_over_time" => Some(OverTimeFamily::Sum),
        "avg_over_time" => Some(OverTimeFamily::Avg),
        "count_over_time" => Some(OverTimeFamily::Count),
        "min_over_time" => Some(OverTimeFamily::Min),
        "max_over_time" => Some(OverTimeFamily::Max),
        "stddev_over_time" => Some(OverTimeFamily::Stddev),
        "stdvar_over_time" => Some(OverTimeFamily::Stdvar),
        "last_over_time" => Some(OverTimeFamily::Last),
        "present_over_time" => Some(OverTimeFamily::Present),
        "quantile_over_time" => Some(OverTimeFamily::Quantile),
        _ => None,
    }
}

/// One float sample with its series identity resolved to a label set.
pub struct LabeledSample {
    pub fp: SeriesFingerprint,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// The assembled operator plan plus the per-series labels needed to reattach
/// label sets to the projected `*_over_time` values.
pub struct OverTimeRangePlan {
    /// Session context whose physical planner understands the custom operators
    /// and whose registry holds the `*_over_time` UDFs, with the leaf table registered.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> RangeManipulate -> Projection`
    /// logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for assembling the result.
    pub labels_by_fp: BTreeMap<SeriesFingerprint, Labels>,
}

/// Build the leaf table and operator chain that evaluates
/// `f_over_time(selector[range])` at a single eval instant `eval_time_ms` with
/// range width `range_ms`. `phi` is the quantile literal for
/// [`OverTimeFamily::Quantile`] and ignored for every other family.
///
/// `samples` are the float samples of the matched series over the exact range
/// window `(eval_time_ms - range_ms, eval_time_ms]`. Stale-NaN markers must be
/// filtered by the caller before the values reach the operator chain; genuine
/// NaN values are carried through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if the Arrow batch, table, or projection plan cannot be
/// constructed.
pub async fn plan_over_time_range_selector(
    samples: Vec<LabeledSample>,
    eval_time_ms: i64,
    range_ms: i64,
    family: OverTimeFamily,
    phi: f64,
) -> Result<OverTimeRangePlan> {
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = BTreeMap::new();
    for sample in &samples {
        for (name, _) in sample.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(sample.fp)
            .or_insert_with(|| sample.labels.clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    let mut rows = samples;
    rows.sort_by(|left, right| {
        left.fp
            .cmp(&right.fp)
            .then_with(|| left.ts_ms.cmp(&right.ts_ms))
    });

    let schema = leaf_schema(&label_names);
    let batch = build_leaf_batch(Arc::clone(&schema), &label_names, &rows)?;

    let ctx = prom_session_context();
    let table = MemTable::try_new(schema, vec![vec![batch]])
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    ctx.register_table("prom_over_time_leaf", Arc::new(table))?;
    let leaf = ctx
        .table("prom_over_time_leaf")
        .await?
        .into_optimized_plan()?;

    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    let range = RangeManipulate::new(
        eval_time_ms,
        eval_time_ms,
        range_ms.max(1),
        range_ms,
        TIME_COLUMN.to_string(),
        VALUE_COLUMN.to_string(),
        normalize,
    )
    .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let range = LogicalPlan::Extension(Extension {
        node: Arc::new(range),
    });

    let udf = ctx
        .udf(family.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let time_range_column = format!("{TIME_COLUMN}{RANGE_SUFFIX}");
    let value_range_column = format!("{VALUE_COLUMN}{RANGE_SUFFIX}");

    // `quantile_over_time` threads the `phi` literal ahead of the windowed
    // columns; the other families take only the three windowed columns.
    let mut udf_args: Vec<Expr> = Vec::with_capacity(4);
    if matches!(family, OverTimeFamily::Quantile) {
        udf_args.push(lit(phi));
    }
    udf_args.push(col(TIME_COLUMN));
    udf_args.push(col(time_range_column));
    udf_args.push(col(value_range_column));
    let over_time_call = udf.call(udf_args).alias(OVER_TIME_VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(over_time_call);

    let plan = LogicalPlanBuilder::from(range)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(OverTimeRangePlan {
        ctx,
        plan,
        labels_by_fp,
    })
}

fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // Nullable so an ABSENT label (NULL) stays distinct from a PRESENT-but-
        // empty-valued label (`""`); see `super::leaf::leaf_schema`.
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    Arc::new(Schema::new(fields))
}

fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[LabeledSample],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty one.
        let values = rows
            .iter()
            .map(|row| row.labels.get(name).map(str::to_string))
            .collect::<Vec<Option<String>>>();
        columns.push(Arc::new(StringArray::from(values)));
    }
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.ts_ms),
    )));
    columns.push(Arc::new(Float64Array::from_iter_values(
        rows.iter().map(|row| row.value),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    fn labeled(job: &str, ts_ms: i64, value: f64) -> LabeledSample {
        let mut labels = Labels::new();
        labels.insert("job", job);
        LabeledSample {
            fp: labels.fingerprint(),
            labels,
            ts_ms,
            value,
        }
    }

    async fn run(
        samples: Vec<LabeledSample>,
        eval_time_ms: i64,
        range_ms: i64,
        family: OverTimeFamily,
        phi: f64,
    ) -> Vec<(String, f64)> {
        let plan = plan_over_time_range_selector(samples, eval_time_ms, range_ms, family, phi)
            .await
            .unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got = Vec::new();
        for batch in &batches {
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name(OVER_TIME_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((job.value(row).to_string(), value.value(row)));
            }
        }
        got
    }

    /// `avg_over_time` over the engine's basic window (3,5 -> 4.0) flows through
    /// the full operator chain.
    #[tokio::test]
    async fn avg_over_time_plan_reduces_window() {
        let samples = vec![labeled("a", 60_000, 3.0), labeled("a", 120_000, 5.0)];
        let got = run(samples, 120_000, 120_000, OverTimeFamily::Avg, 0.0).await;
        assert!(got.len() == 1);
        assert!(got[0].0 == "a");
        assert!(approx_eq(got[0].1, 4.0));
    }

    /// `quantile_over_time(0.5, ...)` over 2,4,4,4,5,5,7,9 yields the median 4.5.
    #[tokio::test]
    async fn quantile_over_time_plan_threads_phi() {
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let samples = values
            .iter()
            .enumerate()
            .map(|(i, v)| labeled("a", (i64::try_from(i).unwrap() + 1) * 60_000, *v))
            .collect();
        let got = run(samples, 480_000, 480_000, OverTimeFamily::Quantile, 0.5).await;
        assert!(got.len() == 1);
        assert!(approx_eq(got[0].1, 4.5));
    }

    /// `present_over_time` yields 1.0 when the window has samples.
    #[tokio::test]
    async fn present_over_time_plan_signals_presence() {
        let samples = vec![labeled("a", 60_000, 42.0)];
        let got = run(samples, 120_000, 120_000, OverTimeFamily::Present, 0.0).await;
        assert!(approx_eq(got[0].1, 1.0));
    }

    /// An empty window emits NULL (not a NaN sentinel), so the assembler drops
    /// the series and aggregates skip it.
    #[tokio::test]
    async fn empty_window_emits_null() {
        use arrow::array::Array;

        // A sample on the left edge (ts == range_start) is excluded by the
        // left-open window, leaving the window empty.
        let samples = vec![labeled("a", 0, 5.0)];
        let plan =
            plan_over_time_range_selector(samples, 120_000, 120_000, OverTimeFamily::Sum, 0.0)
                .await
                .unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = batches[0]
            .column_by_name(OVER_TIME_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(value.len() == 1);
        assert!(value.is_null(0));
    }
}
