//! Shared Arrow decode helpers for metrics block codecs.

use arrow::{array::Array, record_batch::RecordBatch};

use crate::histogram::HistogramCodecError;

// cargo-mutants: exercised through the sample and histogram codec decode tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn schema_mismatch(column: &str) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!("column `{column}` missing or wrong type"))
}

// cargo-mutants: generic downcast glue is covered by caller-specific schema tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn typed_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, HistogramCodecError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| schema_mismatch(name))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| schema_mismatch(name))
}

// cargo-mutants: error formatting is validated through required-column decode tests.
#[cfg_attr(test, mutants::skip)]
fn null_required_column(column: &str, row: usize) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!(
        "column `{column}` contains null for required row {row}"
    ))
}

// cargo-mutants: exercised through the sample and histogram codec null-column tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn require_non_null(
    array: &dyn Array,
    row: usize,
    column: &str,
) -> Result<(), HistogramCodecError> {
    if array.is_null(row) {
        Err(null_required_column(column, row))
    } else {
        Ok(())
    }
}
