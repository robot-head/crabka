//! Flattened profile-samples block schema.
//!
//! Crabka stores one row per profile sample. The raw
//! `(stacktrace_partition, stacktrace_id)` slot is resolved through the block's
//! symbol DB at query time, after merge-before-symbolize has reduced the sample
//! set.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::{
    block::{COL_FINGERPRINT, COL_TIMESTAMP},
    block_index::{BlockSchema, RequiredColumn},
};

/// The 5-part `name:sample_type:sample_unit:period_type:period_unit` string.
pub const PCOL_PROFILE_TYPE: &str = "profile_type";
/// Leaf-node index into the symbol-DB partition's parent-pointer tree.
pub const PCOL_STACKTRACE_ID: &str = "stacktrace_id";
/// The sample value for this profile type.
pub const PCOL_VALUE: &str = "value";
/// Which symbol-DB partition resolves this stacktrace id.
pub const PCOL_STACKTRACE_PARTITION: &str = "stacktrace_partition";
/// Precomputed per-profile total.
pub const PCOL_TOTAL_VALUE: &str = "total_value";
/// Optional span association.
pub const PCOL_SPAN_ID: &str = "span_id";
/// Optional trace association.
pub const PCOL_TRACE_ID: &str = "trace_id";

fn profile_type_dict() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

#[must_use]
pub fn profile_samples_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
        Field::new(PCOL_STACKTRACE_ID, DataType::UInt64, false),
        Field::new(PCOL_VALUE, DataType::Int64, false),
        Field::new(PCOL_STACKTRACE_PARTITION, DataType::UInt64, false),
        Field::new(PCOL_TOTAL_VALUE, DataType::Int64, false),
        Field::new(PCOL_SPAN_ID, DataType::UInt64, true),
        Field::new(PCOL_TRACE_ID, DataType::Binary, true),
    ]))
}

#[must_use]
pub fn profile_samples_decl() -> BlockSchema {
    BlockSchema {
        required: vec![
            RequiredColumn::new(COL_FINGERPRINT, DataType::UInt64, false),
            RequiredColumn::new(PCOL_PROFILE_TYPE, profile_type_dict(), false),
            RequiredColumn::new(COL_TIMESTAMP, DataType::Int64, false),
        ],
        sort_key: vec![
            COL_FINGERPRINT.to_string(),
            PCOL_PROFILE_TYPE.to_string(),
            COL_TIMESTAMP.to_string(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;

    #[test]
    fn mandatory_columns_match_blockstore() {
        let schema = profile_samples_schema();
        assert!(
            schema
                .column_with_name(COL_FINGERPRINT)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
        assert!(
            schema
                .column_with_name(COL_TIMESTAMP)
                .unwrap()
                .1
                .data_type()
                == &DataType::Int64
        );
    }

    #[test]
    fn profile_type_is_dictionary_encoded() {
        let schema = profile_samples_schema();
        let (_, field) = schema.column_with_name(PCOL_PROFILE_TYPE).unwrap();
        match field.data_type() {
            DataType::Dictionary(key, value) => {
                assert!(key.as_ref() == &DataType::Int32);
                assert!(value.as_ref() == &DataType::Utf8);
            }
            other => panic!("expected Dictionary<Int32,Utf8>, got {other:?}"),
        }
    }

    #[test]
    fn raw_stacktrace_slot_columns_are_unsigned() {
        let schema = profile_samples_schema();
        assert!(
            schema
                .column_with_name(PCOL_STACKTRACE_ID)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
        assert!(
            schema
                .column_with_name(PCOL_STACKTRACE_PARTITION)
                .unwrap()
                .1
                .data_type()
                == &DataType::UInt64
        );
    }

    #[test]
    fn value_and_total_value_are_int64() {
        let schema = profile_samples_schema();
        assert!(schema.column_with_name(PCOL_VALUE).unwrap().1.data_type() == &DataType::Int64);
        assert!(
            schema
                .column_with_name(PCOL_TOTAL_VALUE)
                .unwrap()
                .1
                .data_type()
                == &DataType::Int64
        );
    }

    #[test]
    fn cross_signal_join_keys_are_nullable() {
        let schema = profile_samples_schema();
        let span = schema.column_with_name(PCOL_SPAN_ID).unwrap().1;
        let trace = schema.column_with_name(PCOL_TRACE_ID).unwrap().1;
        assert!(span.data_type() == &DataType::UInt64 && span.is_nullable());
        assert!(trace.data_type() == &DataType::Binary && trace.is_nullable());
    }

    #[test]
    fn decl_requires_fp_type_ts_and_sorts_by_them() {
        let decl = profile_samples_decl();
        let names: Vec<&str> = decl
            .required
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert!(names == vec![COL_FINGERPRINT, PCOL_PROFILE_TYPE, COL_TIMESTAMP]);
        assert!(
            decl.sort_key
                == vec![
                    COL_FINGERPRINT.to_string(),
                    PCOL_PROFILE_TYPE.to_string(),
                    COL_TIMESTAMP.to_string(),
                ]
        );
    }
}
