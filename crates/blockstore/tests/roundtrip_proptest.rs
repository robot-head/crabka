//! Property test for the log block round-trip (blockstore plan Task 8).
//!
//! Generates arbitrary log rows, writes them to a Parquet block, reads them
//! back, and asserts the round-trip preserves every row (as a multiset) and the
//! descriptor's fingerprint set. The deterministic example-based round-trips
//! live in `tests/parquet.rs`; this is the generative complement.

use std::collections::{BTreeMap, BTreeSet};

use crabka_blockstore::{BlockKey, LogRow, TimeRange, read_log_block, write_log_block};
use proptest::prelude::*;

/// A total ordering over a row's full contents, so the read-back (sorted by
/// `(fingerprint, timestamp)`) and the input compare as multisets even when two
/// rows share a `(fingerprint, timestamp)` pair.
fn row_sort_key(row: &LogRow) -> (u64, i64, String, BTreeMap<String, String>) {
    (
        row.series_fingerprint,
        row.timestamp_ns,
        row.line.clone(),
        row.structured_metadata.clone(),
    )
}

fn arb_row() -> impl Strategy<Value = (u64, i64, String, BTreeMap<String, String>)> {
    (
        any::<u64>(),
        0_i64..1_000_000_000_000_i64,
        "[a-zA-Z0-9 ,.:|=_/-]{0,40}",
        proptest::collection::btree_map("[a-zA-Z0-9_]{1,12}", "[a-zA-Z0-9_ ]{0,20}", 0..4_usize),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn write_then_read_preserves_all_rows(raw in proptest::collection::vec(arb_row(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();

        let min_ts = raw.iter().map(|(_, ts, _, _)| *ts).min().unwrap();
        let max_ts = raw.iter().map(|(_, ts, _, _)| *ts).max().unwrap();
        let key = BlockKey::new(
            "tenant-prop",
            0,
            0,
            i64::try_from(raw.len()).unwrap(),
            TimeRange::new(min_ts, max_ts).unwrap(),
        );

        let rows: Vec<LogRow> = raw
            .iter()
            .map(|(fp, ts, line, metadata)| LogRow::new(*fp, *ts, line.clone(), metadata.clone()))
            .collect();

        let descriptor = write_log_block(dir.path(), &key, rows.clone()).unwrap();

        let expected_fingerprints: BTreeSet<u64> =
            rows.iter().map(|row| row.series_fingerprint).collect();
        prop_assert_eq!(&descriptor.fingerprints, &expected_fingerprints);

        let mut got = read_log_block(dir.path(), &key).unwrap();
        let mut want = rows;
        got.sort_by_key(row_sort_key);
        want.sort_by_key(row_sort_key);

        prop_assert_eq!(got, want);
    }
}
