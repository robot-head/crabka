//! Profile samples table column contract.

pub use crabka_blockstore::{
    COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_schema,
};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    #[test]
    fn samples_schema_has_fold_keys_and_value() {
        let schema = profile_samples_schema();
        for (column, want) in [
            (PCOL_STACKTRACE_PARTITION, DataType::UInt64),
            (PCOL_STACKTRACE_ID, DataType::UInt64),
            (PCOL_VALUE, DataType::Int64),
        ] {
            assert2::assert!(schema.column_with_name(column).unwrap().1.data_type() == &want);
        }
        let (_, field) = schema.column_with_name(PCOL_TRACE_ID).unwrap();
        assert2::assert!(field.is_nullable() && field.data_type() == &DataType::Binary);
    }
}
