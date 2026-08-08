use std::collections::BTreeMap;

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use crabka_blockstore::{Labels, SeriesFingerprint};

use super::labels::labels_without_metric_name;
use crate::{
    PromqlError,
    error::Result,
    planner::{aggregate::AGGREGATE_VALUE_COLUMN, leaf, over_time_range, rate_range, scalar_math},
    result::{InstantSample, QueryResult, SampleValue},
};

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// planner-path output batch.
///
/// This function treats only `Utf8` columns as labels and skips the
/// `timestamp`/`value` columns.
fn labels_from_batch(batch: &RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == leaf::TIME_COLUMN
            || field.name() == leaf::VALUE_COLUMN
            || field.name() == leaf::SAMPLE_TIME_COLUMN
        {
            continue;
        }
        if let Some(column) = batch.column(index).as_any().downcast_ref::<StringArray>() {
            // NULL -> the label is ABSENT (skip); a non-null value (including
            // `""`) -> the label is PRESENT with that value. This preserves the
            // present-empty-vs-absent distinction the leaf encodes, so the
            // reconstructed fingerprint matches the original series identity.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// rate-range projection output batch.
///
/// The rate projection carries only label (`Utf8`) columns plus the float
/// `value` result column, so every non-`value` `Utf8` column is a label.
fn labels_from_rate_batch(batch: &RecordBatch, row: usize) -> Labels {
    let mut labels = Labels::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        if field.name() == rate_range::RATE_VALUE_COLUMN {
            continue;
        }
        if let Some(column) = batch.column(index).as_any().downcast_ref::<StringArray>() {
            // NULL -> absent (skip); any non-null value (including `""`) ->
            // present with that value. See `labels_from_batch`.
            if !column.is_null(row) {
                labels.insert(field.name().clone(), column.value(row).to_string());
            }
        }
    }
    labels
}

/// Assembles instant-vector-selector output batches into a result.
///
/// Output rows carry label columns plus `timestamp`, `value`, and
/// `sample_timestamp`. `sample_timestamp` holds the selected sample's true
/// timestamp. This function recovers the result labels from `labels_by_fp`,
/// keyed by the row's reconstructed fingerprint.
pub(super) fn assemble_selector_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
) -> Result<QueryResult> {
    // InstantManipulate emits at most one row per series (the single grid step).
    let mut by_fp: BTreeMap<SeriesFingerprint, (i64, f64)> = BTreeMap::new();
    for batch in batches {
        let sample_timestamps = batch
            .column_by_name(leaf::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Int64 sample-timestamp column".to_string())
            })?;
        let values = batch
            .column_by_name(leaf::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("planner leaf missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            let fp = labels_from_batch(batch, row).fingerprint();
            let ts_ms = sample_timestamps.value(row);
            let value = values.value(row);
            by_fp
                .entry(fp)
                .and_modify(|latest| {
                    if ts_ms > latest.0 {
                        *latest = (ts_ms, value);
                    }
                })
                .or_insert((ts_ms, value));
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, (ts_ms, value))| {
            labels_by_fp.get(&fp).cloned().map(|labels| InstantSample {
                labels,
                ts_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assembles rate-family projection output batches into a result.
///
/// Output rows carry label columns plus one `value` column. This function
/// reattaches the eval timestamp and drops the metric name. It also drops NULL
/// rows, which are the UDF's "no value" marker for a window with too few
/// samples. This is exactly how the interpreter omits no-value series. A
/// non-null NaN row is a genuine NaN value, so the function KEEPS it and
/// propagates it.
pub(super) fn assemble_rate_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(rate_range::RATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("rate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (the series has no value at this
            // step): drop it. A non-null NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| InstantSample {
                // Rate-family results drop the metric name, matching
                // `eval_range_function_call`'s `labels_without_metric_name`.
                labels: labels_without_metric_name(labels),
                ts_ms: time_ms,
                value: SampleValue::Float(value),
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assembles `*_over_time` projection output batches into a result.
///
/// Output rows carry label columns plus one `value` column. This function
/// reattaches the eval timestamp and drops NULL rows, which are the UDF's "no
/// value" marker for an empty window. This is exactly how the interpreter omits
/// no-value series. A non-null NaN row is a genuine NaN value, so the function
/// KEEPS it and propagates it.
///
/// `preserve_metric_name` keeps `__name__` only for `last_over_time`. Every
/// other family drops it, which matches the interpreter's `eval_over_time_call`
/// and `OverTimeFn::preserves_metric_name`.
pub(super) fn assemble_over_time_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    preserve_metric_name: bool,
) -> Result<QueryResult> {
    let mut by_fp: BTreeMap<SeriesFingerprint, f64> = BTreeMap::new();
    for batch in batches {
        let values = batch
            .column_by_name(over_time_range::OVER_TIME_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("over_time projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL is the no-value marker (an empty window): drop it. A non-null
            // NaN is a genuine NaN value: keep it.
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            // The over_time projection carries only label (`Utf8`) columns plus
            // the float `value` result, so `labels_from_rate_batch` (which reads
            // exactly that shape) reconstructs the fingerprint.
            let fp = labels_from_rate_batch(batch, row).fingerprint();
            by_fp.insert(fp, value);
        }
    }

    let samples = by_fp
        .into_iter()
        .filter_map(|(fp, value)| {
            labels_by_fp.get(&fp).map(|labels| {
                let labels = if preserve_metric_name {
                    labels.clone()
                } else {
                    labels_without_metric_name(labels)
                };
                InstantSample {
                    labels,
                    ts_ms: time_ms,
                    value: SampleValue::Float(value),
                }
            })
        })
        .collect();
    Ok(QueryResult::InstantVector(samples))
}

/// Assembles simple-aggregation output batches into a result.
///
/// Output rows carry exactly the grouping label columns plus `value`. This
/// function reads the label set directly from the batch, with no fingerprint
/// lookup, and reattaches the eval timestamp. An empty grouping, which is
/// `by ()` or no modifier, returns one row with an empty label set.
///
/// A NULL aggregate result means the group had no value-bearing input. This
/// happens when every member was a no-value series that the pre-aggregate NULL
/// filter dropped, or when the NaN-ignoring `min`/`max` UDAF saw only nulls. The
/// function drops such a result, which matches the interpreter: the interpreter
/// forms no group when no sample reaches it. A non-null NaN result is a genuine
/// aggregated NaN, for example a `sum` over a group that holds a genuine NaN or
/// an all-NaN `min`/`max` group, and the function KEEPS it.
pub(super) fn assemble_aggregate_batches(
    batches: &[RecordBatch],
    time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(AGGREGATE_VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("aggregate projection missing Float64 value column".to_string())
            })?;
        for row in 0..batch.num_rows() {
            // A NULL aggregate = no value-bearing input for the group: drop it
            // (the interpreter never forms such a group). A non-null NaN is a
            // genuine aggregated NaN: keep it.
            if values.is_null(row) {
                continue;
            }
            // The grouping labels are exactly the batch's non-`value` Utf8
            // columns; `labels_from_rate_batch` reads precisely those.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                ts_ms: time_ms,
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}

/// Assembles per-row scalar-math projection output batches into a result.
///
/// Output rows carry the metadata-free label columns plus one `value` column.
/// The leaf already dropped the metric name, this function reads the label set
/// directly from the batch, and the eval timestamp is reattached. The function
/// keeps every row: the scalar-math functions never drop a float sample, so
/// `f(NaN)` and `sqrt(-1)` surface as `NaN`. This matches the interpreter's
/// `eval_unary_float_call`, `eval_clamp_call`, and `eval_round_call`.
pub(super) fn assemble_scalar_math_batches(
    batches: &[RecordBatch],
    _time_ms: i64,
) -> Result<QueryResult> {
    let mut samples = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(scalar_math::VALUE_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                PromqlError::Exec("scalar-math projection missing Float64 value column".to_string())
            })?;
        let sample_timestamps = batch
            .column_by_name(scalar_math::SAMPLE_TIME_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| {
                PromqlError::Exec(
                    "scalar-math projection missing Int64 sample-timestamp column".to_string(),
                )
            })?;
        for row in 0..batch.num_rows() {
            // The scalar-math projection carries label (`Utf8`) columns plus the
            // float `value` result and the Int64 `sample_timestamp`;
            // `labels_from_rate_batch` reads only the string label columns (it
            // skips the Int64 timestamp), reconstructing the labelset.
            let labels = labels_from_rate_batch(batch, row);
            samples.push(InstantSample {
                labels,
                // Scalar-math functions report the inner sample's timestamp
                // unchanged (the interpreter keeps `sample.ts_ms`).
                ts_ms: sample_timestamps.value(row),
                value: SampleValue::Float(values.value(row)),
            });
        }
    }
    Ok(QueryResult::InstantVector(samples))
}
