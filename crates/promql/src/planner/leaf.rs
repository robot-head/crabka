//! Leaf source and `LogicalPlan` assembly for the instant-vector-selector
//! operator path.
//!
//! The custom operators [`SeriesDivide`], [`SeriesNormalize`], and
//! [`InstantManipulate`] read per-series batches. Each batch carries the label
//! columns of the series plus an `Int64` `timestamp` column and a `Float64`
//! `value` column. The `MetricStore::scan` seam returns fingerprint, timestamp,
//! and value rows with no label columns. This module fills that gap: it
//! materializes the labels of the matched series, keyed by fingerprint, into
//! label columns beside the samples. It then registers the result as an
//! in-memory leaf table and assembles the
//! `SeriesDivide -> SeriesNormalize -> InstantManipulate` chain that selects one
//! sample per series inside the lookback window.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use crabka_blockstore::{Labels, SeriesFingerprint};
use crabka_units::prelude::*;
use datafusion::{
    catalog::MemTable,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};

use crate::{
    PromqlError,
    error::Result,
    extension::{
        instant_manipulate::InstantManipulate, normalize::SeriesNormalize,
        planner::prom_session_context, series_divide::SeriesDivide,
    },
};

/// Leaf-batch column with the per-sample timestamp in epoch milliseconds.
///
/// This column is the time index of the operator chain, so
/// [`InstantManipulate`] rewrites it to the grid eval timestamp on output.
pub const TIME_COLUMN: &str = "timestamp";
/// Leaf-batch column carrying the per-sample float value.
pub const VALUE_COLUMN: &str = "value";
/// Leaf-batch column that keeps the original sample timestamp.
///
/// This column is not the time index, so the operator chain carries it through
/// unchanged with `take`. The engine then recovers the true timestamp of the
/// selected sample. The interpreter reports that timestamp as
/// `InstantSample.ts_ms`, and `timestamp()` reads it.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";

/// One float sample with its series identity resolved to a label set.
pub struct LabeledSample {
    pub fp: SeriesFingerprint,
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// The assembled operator plan plus the per-series labels needed to reattach
/// label sets to the selected samples.
pub struct InstantSelectorPlan {
    /// Session context whose physical planner understands the custom operators
    /// and where the leaf table is registered.
    pub ctx: SessionContext,
    /// The `SeriesDivide -> SeriesNormalize -> InstantManipulate` logical plan.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for assembling the result.
    pub labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
}

/// Builds the leaf table and operator chain for a bare instant-vector selector.
///
/// The chain evaluates the selector at `eval_time_ms` with the given
/// `lookback_delta`. `samples` are the float samples of the matched series over
/// the scan window `(eval_time_ms - lookback_delta, eval_time_ms]`. The caller
/// must filter out the stale-NaN markers before the values reach
/// [`InstantManipulate`]. This matches the staleness handling of the
/// interpreter.
///
/// # Errors
///
/// Returns an error if this function cannot build the Arrow batch or the table.
pub async fn plan_instant_vector_selector(
    samples: Vec<LabeledSample>,
    eval_time_ms: i64,
    lookback_delta: Time,
) -> Result<InstantSelectorPlan> {
    // Collect the distinct label names across all matched series; these become
    // the label columns carried through the operator chain.
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
    ctx.register_table("prom_leaf", Arc::new(table))?;
    let leaf = ctx.table("prom_leaf").await?.into_optimized_plan()?;

    // SeriesDivide on every label column splits the sorted input into exact
    // per-series batches.
    let divide = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesDivide {
            tag_columns: label_names.clone(),
            input: leaf,
        }),
    });
    // SeriesNormalize sorts each per-series batch by timestamp. The offset is
    // already folded into eval_time_ms by the caller, so it is zero here.
    let normalize = LogicalPlan::Extension(Extension {
        node: Arc::new(SeriesNormalize {
            offset_ms: 0,
            time_index: TIME_COLUMN.to_string(),
            need_filter_out_nan: false,
            input: divide,
        }),
    });
    // InstantManipulate selects, for the single eval step, the latest sample
    // within (eval_time - lookback, eval_time], dropping NaN.
    let instant = LogicalPlan::Extension(Extension {
        node: Arc::new(InstantManipulate {
            start_ms: eval_time_ms,
            end_ms: eval_time_ms,
            // A single grid step: any positive stride covers exactly one point.
            step_ms: lookback_delta.millis_i64().max(1),
            lookback_delta_ms: lookback_delta.millis_i64(),
            time_index: TIME_COLUMN.to_string(),
            field_column: VALUE_COLUMN.to_string(),
            input: normalize,
        }),
    });

    Ok(InstantSelectorPlan {
        ctx,
        plan: instant,
        labels_by_fp,
    })
}

fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // Label columns are nullable so an ABSENT label (NULL) is distinguishable
        // from a PRESENT-but-empty-valued label (`""`). The reconstruction
        // (`engine::labels_from_batch`) maps NULL -> absent and `""` ->
        // present-empty, preserving the byte-exact label set through the chain.
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

fn build_leaf_batch(
    schema: Arc<Schema>,
    label_names: &[String],
    rows: &[LabeledSample],
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // `None` (NULL) for an ABSENT label; `Some("")` for a PRESENT-empty
        // label. The two must stay distinct so the reconstructed fingerprint
        // matches the original series identity.
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
    // Duplicate of the sample timestamp, carried through the chain unchanged.
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|row| row.ts_ms),
    )));
    RecordBatch::try_new(schema, columns).map_err(|error| PromqlError::Exec(error.to_string()))
}
