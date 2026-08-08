//! Float-sample Arrow codec.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, Float64Array, Float64Builder, Int64Array, Int64Builder, UInt64Array,
        UInt64Builder,
    },
    record_batch::RecordBatch,
};

use crate::{
    arrow_codec::{require_non_null, typed_column},
    histogram::HistogramCodecError,
    schema::{COL_FINGERPRINT, COL_TIMESTAMP, float_sample_schema},
};

const COL_VALUE: &str = "value";

/// Encodes `(fingerprint, timestamp, value)` rows into a `RecordBatch` that
/// matches [`float_sample_schema`].
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn encode_float_samples(rows: &[(u64, i64, f64)]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();

    for (fingerprint, timestamp, value) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp);
        values.append_value(*value);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
    ];

    Ok(RecordBatch::try_new(float_sample_schema(), columns)?)
}

/// Decodes a float-sample `RecordBatch` into `(fingerprint, timestamp, value)`
/// rows.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_float_samples(
    batch: &RecordBatch,
) -> Result<Vec<(u64, i64, f64)>, HistogramCodecError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let values = typed_column::<Float64Array>(batch, COL_VALUE)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        require_non_null(fingerprints, row, COL_FINGERPRINT)?;
        require_non_null(timestamps, row, COL_TIMESTAMP)?;
        require_non_null(values, row, COL_VALUE)?;

        rows.push((
            fingerprints.value(row),
            timestamps.value(row),
            values.value(row),
        ));
    }

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn float_samples_round_trip() {
        let rows = [(1_u64, 100_i64, 1.5_f64), (2, 200, -3.0), (1, 300, 0.0)];

        let batch = encode_float_samples(&rows).unwrap();
        let decoded = decode_float_samples(&batch).unwrap();

        assert!(decoded == rows);
    }
}
