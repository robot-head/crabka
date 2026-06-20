//! Block metadata and schema validation.

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::block_index::{BlockSchema, series_block_schema};
use crate::{BlockStoreError, Result, SeriesFingerprint};

/// Mandatory logs/metrics block column: series fingerprint (`UInt64`).
pub const COL_FINGERPRINT: &str = "series_fingerprint";
/// Mandatory logs/metrics/profile block column: timestamp (`Int64`).
pub const COL_TIMESTAMP: &str = "timestamp";

/// Metadata for one tenant-scoped Parquet block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub fingerprints: Vec<SeriesFingerprint>,
}

/// Validate an Arrow schema against the logs/metrics series-block declaration.
pub fn validate_block_schema(schema: &Schema) -> Result<()> {
    validate_against(schema, &series_block_schema())
}

/// Validate an Arrow schema against a signal's declared block schema.
pub fn validate_against(schema: &Schema, decl: &BlockSchema) -> Result<()> {
    for col in &decl.required {
        let (_, found) = schema.column_with_name(&col.name).ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!("missing `{}` column", col.name))
        })?;
        if found.data_type() != &col.data_type {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be {:?}, got {:?}",
                col.name,
                col.data_type,
                found.data_type()
            )));
        }
        if found.is_nullable() != col.nullable {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` nullable must be {}, got {}",
                col.name,
                col.nullable,
                found.is_nullable()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;

    use super::*;

    #[test]
    fn validate_block_schema_accepts_mandatory_columns() {
        let schema = Schema::new(vec![
            Field::new(COL_FINGERPRINT, DataType::UInt64, false),
            Field::new(COL_TIMESTAMP, DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]);
        assert!(validate_block_schema(&schema).is_ok());
    }

    #[test]
    fn validate_block_schema_rejects_missing_timestamp() {
        let schema = Schema::new(vec![Field::new(COL_FINGERPRINT, DataType::UInt64, false)]);
        assert!(validate_block_schema(&schema).is_err());
    }
}
