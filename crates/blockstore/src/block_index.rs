//! Pluggable per-signal block index and schema declaration.

use arrow::datatypes::DataType;
use serde::{Serialize, de::DeserializeOwned};

use crate::block::BlockMeta;

/// One required column in a signal block schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl RequiredColumn {
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// A signal's declared block schema and sort key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSchema {
    pub required: Vec<RequiredColumn>,
    pub sort_key: Vec<String>,
}

/// The logs/metrics block declaration.
#[must_use]
pub fn series_block_schema() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(crate::block::COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(crate::block::COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            crate::block::COL_FINGERPRINT.to_string(),
            crate::block::COL_TIMESTAMP.to_string(),
        ],
    }
}

/// Signal-specific index seam.
pub trait BlockIndex: Default + Serialize + DeserializeOwned {
    fn add_block(&mut self, meta: &BlockMeta);
    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String>;
    fn block_count(&self, tenant: &str) -> usize;
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use assert2::assert;

    use super::*;
    use crate::block::validate_against;

    #[test]
    fn series_declaration_lists_mandatory_columns() {
        let decl = series_block_schema();
        let names: Vec<&str> = decl.required.iter().map(|c| c.name.as_str()).collect();
        assert!(names == vec!["series_fingerprint", "timestamp"]);
        assert!(decl.sort_key == vec!["series_fingerprint".to_string(), "timestamp".to_string()]);
    }

    #[test]
    fn validate_against_accepts_matching_schema() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("line", DataType::Utf8, true),
        ]);
        assert!(validate_against(&schema, &decl).is_ok());
    }

    #[test]
    fn validate_against_rejects_wrong_type() {
        let decl = series_block_schema();
        let schema = Schema::new(vec![
            Field::new("series_fingerprint", DataType::UInt64, false),
            Field::new("timestamp", DataType::Utf8, false),
        ]);
        assert!(validate_against(&schema, &decl).is_err());
    }
}
