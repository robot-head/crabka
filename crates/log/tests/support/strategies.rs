//! Shared proptest strategies for `crabka-log` integration tests.

use bytes::Bytes;
use crabka_protocol::records::{Record, RecordBatch};
use proptest::prelude::*;

/// Arbitrary [`RecordBatch`] with the given record-count range and bounded key
/// and value sizes.
///
/// Each record gets a sequential `offset_delta`, so the batch is internally
/// consistent. The batch's `base_offset` stays at `0`, because [`crabka_log::Log::append`]
/// overwrites it with the next assigned offset.
pub fn arb_batch(records_min: usize, records_max: usize) -> impl Strategy<Value = RecordBatch> {
    (
        records_min..=records_max,
        any::<i64>().prop_map(i64::saturating_abs),
    )
        .prop_flat_map(|(n, ts)| {
            let records = proptest::collection::vec(arb_record(), n..=n);
            (Just(n), Just(ts), records).prop_map(|(n, ts, records)| {
                let n_i32 = i32::try_from(n).unwrap_or(i32::MAX);
                let mut b = RecordBatch {
                    base_offset: 0,
                    base_timestamp: ts,
                    max_timestamp: ts.saturating_add(i64::from(n_i32)),
                    last_offset_delta: (n_i32 - 1).max(0),
                    ..RecordBatch::default()
                };
                b.records = records
                    .into_iter()
                    .enumerate()
                    .map(|(i, r)| Record {
                        offset_delta: i32::try_from(i).unwrap_or(i32::MAX),
                        ..r
                    })
                    .collect();
                b
            })
        })
}

fn arb_record() -> impl Strategy<Value = Record> {
    (
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=128).prop_map(Bytes::from)),
        proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=512).prop_map(Bytes::from)),
    )
        .prop_map(|(key, value)| Record {
            key,
            value,
            ..Default::default()
        })
}

/// A vector of arbitrary batches. Each batch holds 1..=4 records.
pub fn arb_batches(count_min: usize, count_max: usize) -> impl Strategy<Value = Vec<RecordBatch>> {
    proptest::collection::vec(arb_batch(1, 4), count_min..=count_max)
}
