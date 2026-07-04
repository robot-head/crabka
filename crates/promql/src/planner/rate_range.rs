//! Leaf source and `LogicalPlan` assembly for the rate-family range-selector
//! operator path.
//!
//! This is the matrix-selector sibling of [`super::leaf`]. Where the instant
//! path selects a single sample per series within the lookback window, the rate
//! path materializes a full range window `(t - range, t]` per series and folds
//! it through [`RangeManipulate`] into windowed `RangeArray` columns, then
//! projects a rate-family [`datafusion::logical_expr::ScalarUDF`] over those
//! columns to produce one float per series.
//!
//! The assembled chain is
//! `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize -> RangeManipulate
//! -> Projection(labels..., prom_<fn>(timestamp, timestamp_range, value_range,
//! range_ms) AS value)`.
//!
//! # Window semantics (vs. the instant path)
//!
//! A range selector does **not** apply the 5m lookback. The window is exactly
//! `(eval_time - range, eval_time]`, left-open and right-closed, matching
//! Prometheus' matrix-selector semantics and the interpreter's
//! `range_function_sample_from_series`. The caller fetches samples over
//! `(eval_time - range, eval_time]` and passes the window's range width as
//! `range_ms`; `RangeManipulate` re-derives the per-step window and the UDF
//! re-derives `range_start = eval_timestamp - range_ms`.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use crabka_blockstore::{Labels, SeriesFingerprint};
use datafusion::{
    catalog::MemTable,
    execution::FunctionRegistry,
    logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, col, lit},
    prelude::SessionContext,
};

use crate::{
    PromqlError,
    error::Result,
    extension::{
        normalize::SeriesNormalize,
        planner::prom_session_context,
        range_manipulate::{RANGE_SUFFIX, RangeManipulate},
        series_divide::SeriesDivide,
    },
};

/// Leaf-batch column carrying the per-sample timestamp in epoch milliseconds.
/// This is the operator chain's time index; `RangeManipulate` reuses the name
/// for the scalar eval-timestamp column it emits.
pub const TIME_COLUMN: &str = "timestamp";
/// Leaf-batch column carrying the per-sample float value.
pub const VALUE_COLUMN: &str = "value";
/// Projection output column name carrying the rate-family UDF result.
pub const RATE_VALUE_COLUMN: &str = "value";

/// Which rate-family `ScalarUDF` a range-selector plan projects. The registered
/// UDF names (`prom_rate`, …) are the seam to [`crate::functions::rate`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RateUdfKind {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
}

impl RateUdfKind {
    /// The registered UDF name this kind projects.
    fn udf_name(self) -> &'static str {
        match self {
            Self::Rate => "prom_rate",
            Self::Increase => "prom_increase",
            Self::Delta => "prom_delta",
            Self::Irate => "prom_irate",
            Self::Idelta => "prom_idelta",
        }
    }

    /// Resolve the matrix-selector `PromQL` function name to its UDF kind. Returns
    /// `None` for any function outside the operator-path rate family.
    #[must_use]
    pub fn from_function_name(name: &str) -> Option<Self> {
        match name {
            "rate" => Some(Self::Rate),
            "increase" => Some(Self::Increase),
            "delta" => Some(Self::Delta),
            "irate" => Some(Self::Irate),
            "idelta" => Some(Self::Idelta),
            _ => None,
        }
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
/// label sets to the projected rate values.
pub struct RateRangePlan {
    /// Session context whose physical planner understands the custom operators
    /// and whose registry holds the rate UDFs, with the leaf table registered.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> RangeManipulate -> Projection`
    /// logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for assembling the result.
    pub labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
}

/// Build the leaf table and operator chain that evaluates `f(selector[range])`
/// at a single eval instant `eval_time_ms` with range width `range_ms`.
///
/// `samples` are the float samples of the matched series over the exact range
/// window `(eval_time_ms - range_ms, eval_time_ms]`. Stale-NaN markers must be
/// filtered out by the caller (matching the interpreter's `eval_matrix_selector`
/// staleness handling) before the values reach the operator chain; genuine NaN
/// values are carried through unchanged, as the interpreter does.
///
/// # Errors
///
/// Returns an error if the Arrow batch, table, or projection plan cannot be
/// constructed.
pub async fn plan_rate_range_selector(
    samples: Vec<LabeledSample>,
    eval_time_ms: i64,
    range_ms: i64,
    kind: RateUdfKind,
) -> Result<RateRangePlan> {
    // Collect the distinct label names across all matched series; these become
    // the label columns carried through the operator chain and projected out.
    let mut label_names: BTreeSet<String> = BTreeSet::new();
    let mut labels_by_fp = std::collections::BTreeMap::new();
    for sample in &samples {
        for (name, _) in sample.labels.iter() {
            label_names.insert(name.clone());
        }
        labels_by_fp
            .entry(sample.fp)
            .or_insert_with(|| sample.labels.clone());
    }
    let label_names: Vec<String> = label_names.into_iter().collect();

    // Sort the rows so each series forms a contiguous, time-ordered run. The
    // fingerprint key groups series; SeriesDivide then splits on label columns.
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
    ctx.register_table("prom_rate_leaf", Arc::new(table))?;
    let leaf = ctx.table("prom_rate_leaf").await?.into_optimized_plan()?;

    // SeriesDivide on every label column splits the sorted input into exact
    // per-series batches.
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    // SeriesNormalize sorts each per-series batch by timestamp. The offset is
    // already folded into eval_time_ms by the caller, so it is zero here. NaN is
    // NOT filtered here: matrix selectors keep genuine NaN (only stale-NaN is
    // dropped, which the caller already did), so the operator chain must not
    // strip it.
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    // RangeManipulate folds the samples into the single eval step's window
    // (t - range, t]. A single grid step: start == end == eval_time_ms, and any
    // positive interval covers exactly one point.
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

    // Project the label columns through plus the rate-family UDF over the
    // windowed columns, aliased to the result value column.
    let udf = ctx
        .udf(kind.udf_name())
        .map_err(|error| PromqlError::Exec(error.to_string()))?;
    let time_range_column = format!("{TIME_COLUMN}{RANGE_SUFFIX}");
    let value_range_column = format!("{VALUE_COLUMN}{RANGE_SUFFIX}");
    let rate_call = udf
        .call(vec![
            col(TIME_COLUMN),
            col(time_range_column),
            col(value_range_column),
            lit(range_ms),
        ])
        .alias(RATE_VALUE_COLUMN);

    let mut projections: Vec<Expr> = label_names.iter().map(col).collect();
    projections.push(rate_call);

    let plan = LogicalPlanBuilder::from(range)
        .project(projections)
        .map_err(|error| PromqlError::Exec(error.to_string()))?
        .build()
        .map_err(|error| PromqlError::Exec(error.to_string()))?;

    Ok(RateRangePlan {
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
    use arrow::array::{Float64Array, StringArray};
    use assert2::{assert, check};

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

    /// `rate(counter[5m])` over the engine's canonical counter window
    /// (0..240s stepping by 1.0, eval at t=300s) yields 5/300 through the full
    /// operator chain, matching `extrapolate::rate_extrapolates_counter_window`.
    #[tokio::test]
    async fn rate_range_plan_reproduces_counter_window() {
        let samples = vec![
            labeled("a", 0, 0.0),
            labeled("a", 60_000, 1.0),
            labeled("a", 120_000, 2.0),
            labeled("a", 180_000, 3.0),
            labeled("a", 240_000, 4.0),
        ];
        let plan = plan_rate_range_selector(samples, 300_000, 300_000, RateUdfKind::Rate)
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
                .column_by_name(RATE_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((job.value(row).to_string(), value.value(row)));
            }
        }
        check!(got.len() == 1);
        check!(got[0].0 == "a");
        check!(approx_eq(got[0].1, 5.0 / 300.0));
    }

    /// `increase` reset correction flows through the chain: 1,2,1 -> 2.0.
    #[tokio::test]
    async fn increase_range_plan_corrects_reset() {
        let samples = vec![
            labeled("a", 0, 1.0),
            labeled("a", 60_000, 2.0),
            labeled("a", 120_000, 1.0),
        ];
        let plan = plan_rate_range_selector(samples, 120_000, 120_000, RateUdfKind::Increase)
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
            .column_by_name(RATE_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(approx_eq(value.value(0), 2.0));
    }

    /// A single-sample window has no rate; the UDF emits NULL (not a NaN
    /// sentinel), so the assembler drops the series and aggregates skip it.
    #[tokio::test]
    async fn single_sample_window_yields_null() {
        use arrow::array::Array;

        let samples = vec![labeled("a", 60_000, 1.0)];
        let plan = plan_rate_range_selector(samples, 60_000, 60_000, RateUdfKind::Rate)
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
            .column_by_name(RATE_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(value.is_null(0));
    }
}
