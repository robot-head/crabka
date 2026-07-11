//! Block column conventions and per-block metadata.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::{
    error::{BlockStoreError, Result},
    labels::SeriesFingerprint,
};

/// Mandatory column: the series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory column: the event timestamp in nanoseconds (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Metadata recorded for each written block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub fingerprints: Vec<SeriesFingerprint>,
}

/// Validate that an Arrow schema carries the mandatory columns with the
/// required types. Payload columns are unconstrained.
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    validate_against(schema, &crate::block_index::series_block_schema())
}

/// Validate an Arrow schema against a declared signal block schema.
pub fn validate_against(schema: &Schema, decl: &crate::block_index::BlockSchema) -> Result<()> {
    for col in &decl.required {
        let found = schema.column_with_name(&col.name).ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!("missing `{}` column", col.name))
        })?;
        if found.1.data_type() != &col.data_type {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be {:?}, got {:?}",
                col.name,
                col.data_type,
                found.1.data_type()
            )));
        }
        if !col.nullable && found.1.is_nullable() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be non-nullable",
                col.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn validates_required_block_schema_columns() {
        for (_name, schema, want_valid) in [
            (
                "required columns",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                    Field::new("line", DataType::Utf8, true),
                ]),
                true,
            ),
            (
                "missing fingerprint",
                Schema::new(vec![Field::new(COL_TIMESTAMP, DataType::Int64, false)]),
                false,
            ),
            (
                "wrong timestamp type",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Utf8, false),
                ]),
                false,
            ),
            (
                "nullable fingerprint",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, true),
                    Field::new(COL_TIMESTAMP, DataType::Int64, false),
                ]),
                false,
            ),
            (
                "nullable timestamp",
                Schema::new(vec![
                    Field::new(COL_FINGERPRINT, DataType::UInt64, false),
                    Field::new(COL_TIMESTAMP, DataType::Int64, true),
                ]),
                false,
            ),
        ] {
            assert2::assert!(validate_block_schema(&schema).is_ok() == want_valid);
        }
    }
}
