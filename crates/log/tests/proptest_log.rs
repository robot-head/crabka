//! Property-based round-trip and truncation tests for [`crabka_log::Log`].

mod support;

use crabka_ids::Offset;
use crabka_log::{Log, LogConfig};
use crabka_units::prelude::{ByteSize, gibibytes};
use proptest::prelude::*;
use support::strategies::arb_batches;
use tempfile::tempdir;

/// A read budget larger than anything these properties generate, so the byte
/// budget never clips the result.
const NO_LIMIT: ByteSize = gibibytes(4);

proptest! {
    /// Appending an arbitrary list of batches and then reading the whole
    /// log back yields the same total number of records.
    #[test]
    fn write_then_read_records_match(batches in arb_batches(0, 8)) {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut expected_record_count: usize = 0;
        for mut b in batches.clone() {
            expected_record_count += b.records.len();
            log.append(&mut b).unwrap();
        }
        let out = log.read(Offset(0), NO_LIMIT).unwrap();
        let actual_record_count: usize = out.batches.iter().map(|b| b.records.len()).sum();
        prop_assert_eq!(actual_record_count, expected_record_count);
    }

    /// After appending and then truncating to an arbitrary offset within
    /// `[0, log_end)`, `log_end_offset()` is `<= trunc_to`. Truncation
    /// happens on batch boundaries: if `trunc_to` falls in the middle of
    /// a batch, the entire batch is dropped, so the resulting log end can
    /// be lower than the requested point. We also assert the log can
    /// still be read end-to-end without error.
    #[test]
    fn random_truncate_then_read(
        batches in arb_batches(1, 6),
        seed in 0u64..1024,
    ) {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for mut b in batches.clone() {
            log.append(&mut b).unwrap();
        }
        let log_end = log.log_end_offset();
        // `arb_batches(1, 6)` guarantees at least one batch with >= 1
        // record, so `log_end >= 1`.
        prop_assume!(log_end >= 1);
        let trunc_to = Offset(i64::try_from(seed).unwrap_or(i64::MAX) % log_end.0);
        log.truncate_to(trunc_to).unwrap();
        let after = log.log_end_offset();
        prop_assert!(
            after <= trunc_to,
            "log_end_offset {after} > trunc_to {trunc_to}"
        );
        // Reading post-truncation must succeed.
        let _ = log.read(log.log_start_offset(), NO_LIMIT).unwrap();
    }
}
