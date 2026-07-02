//! Build profile-samples `RecordBatch`es.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, BinaryBuilder, Int64Builder, StringDictionaryBuilder, UInt64Builder},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};

use crate::{
    error::{BlockStoreError, Result},
    profile_schema::profile_samples_schema,
};

/// One flattened profile sample row.
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileSampleRow {
    pub series_fingerprint: u64,
    pub timestamp: i64,
    pub profile_type: String,
    pub stacktrace_id: u64,
    pub value: i64,
    pub stacktrace_partition: u64,
    pub total_value: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// Encode rows into a `RecordBatch` matching `profile_samples_schema()`.
pub fn encode_profile_samples(rows: &[ProfileSampleRow]) -> Result<RecordBatch> {
    let mut fp = UInt64Builder::new();
    let mut ts = Int64Builder::new();
    let mut profile_type = StringDictionaryBuilder::<Int32Type>::new();
    let mut stacktrace_id = UInt64Builder::new();
    let mut value = Int64Builder::new();
    let mut partition = UInt64Builder::new();
    let mut total_value = Int64Builder::new();
    let mut span_id = UInt64Builder::new();
    let mut trace_id = BinaryBuilder::new();

    for row in rows {
        fp.append_value(row.series_fingerprint);
        ts.append_value(row.timestamp);
        profile_type
            .append(&row.profile_type)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?;
        stacktrace_id.append_value(row.stacktrace_id);
        value.append_value(row.value);
        partition.append_value(row.stacktrace_partition);
        total_value.append_value(row.total_value);
        match row.span_id {
            Some(value) => span_id.append_value(value),
            None => span_id.append_null(),
        }
        match &row.trace_id {
            Some(value) => trace_id.append_value(value),
            None => trace_id.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fp.finish()),
        Arc::new(ts.finish()),
        Arc::new(profile_type.finish()),
        Arc::new(stacktrace_id.finish()),
        Arc::new(value.finish()),
        Arc::new(partition.finish()),
        Arc::new(total_value.finish()),
        Arc::new(span_id.finish()),
        Arc::new(trace_id.finish()),
    ];

    RecordBatch::try_new(profile_samples_schema(), columns)
        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BinaryArray, Int64Array, UInt64Array};
    use assert2::assert;

    use super::*;
    use crate::{
        PCOL_STACKTRACE_ID, PCOL_TRACE_ID, PCOL_VALUE, profile_samples_decl,
        profile_samples_schema, validate_against,
    };

    fn row(fp: u64, ts: i64, stack: u64, value: i64, trace: Option<Vec<u8>>) -> ProfileSampleRow {
        ProfileSampleRow {
            series_fingerprint: fp,
            timestamp: ts,
            profile_type: "process_cpu:cpu:nanoseconds:cpu:nanoseconds".to_string(),
            stacktrace_id: stack,
            value,
            stacktrace_partition: 0,
            total_value: 1_000,
            span_id: None,
            trace_id: trace,
        }
    }

    #[test]
    fn encode_matches_schema_and_columns() {
        let rows = vec![
            row(1, 100, 7, 50, Some(vec![0xAB; 16])),
            row(1, 100, 9, 30, None),
        ];
        let batch = encode_profile_samples(&rows).unwrap();
        assert!(batch.schema() == profile_samples_schema());
        assert!(batch.num_rows() == 2);
        validate_against(&batch.schema(), &profile_samples_decl()).unwrap();

        let stacks = batch
            .column_by_name(PCOL_STACKTRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert!(stacks.value(0) == 7 && stacks.value(1) == 9);

        let values = batch
            .column_by_name(PCOL_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(values.value(0) == 50);

        let traces = batch
            .column_by_name(PCOL_TRACE_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(traces.value(0) == [0xAB; 16].as_slice());
        assert!(traces.is_null(1));
    }
}
