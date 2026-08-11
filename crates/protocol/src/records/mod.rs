//! Typed v2 record batch decoder/encoder.
//!
//! This module handles the modern Kafka `RecordBatch` format, which Produce,
//! Fetch, and log storage all use. The `crabka-records-legacy` crate
//! implements the legacy v0/v1 `MessageSet` conversion.

pub(crate) mod borrowed;
mod crc;
mod error;
mod file_region;
pub mod header;
pub mod metadata;
pub(crate) mod owned;
mod payload;
mod produce_passthrough;
pub mod remote_log_metadata;

pub use borrowed::{
    Record as RecordBorrowed, RecordBatch as RecordBatchBorrowed,
    RecordHeader as RecordHeaderBorrowed, ValidatedBatch, count_records_in_v2_batches,
    validate_one_v2_batch,
};
pub use error::RecordsError;
pub use file_region::FileRegion;
pub use header::{
    Attributes, CRC_COVERAGE_START, HEADER_LEN, RecordBatchHeader, TimestampType,
    patch_base_offset_and_leader_epoch,
};
pub use owned::{Record, RecordBatch, RecordHeader};
pub use payload::{RecordsPayload, RecordsPayloadBorrowed};
pub use produce_passthrough::{
    PartitionRecordSlice, ProduceFraming, ProduceFramingTopic, produce_framing,
    produce_record_slices,
};

/// Advance a Kafka producer sequence, wrapping after [`i32::MAX`] back to zero.
///
/// Kafka producer sequences use the non-negative half of the signed `int32`
/// range. Masking off the sign bit implements arithmetic modulo `2^31`
/// without an overflowing signed addition.
#[must_use]
pub const fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    sequence.wrapping_add(increment) & i32::MAX
}

/// Move a Kafka producer sequence backwards, wrapping zero to [`i32::MAX`].
#[must_use]
pub const fn decrement_sequence(sequence: i32, decrement: i32) -> i32 {
    sequence.wrapping_sub(decrement) & i32::MAX
}

#[cfg(test)]
mod sequence_tests {
    use super::{decrement_sequence, increment_sequence};

    #[test]
    fn producer_sequence_increment_wraps_at_signed_maximum() {
        assert2::check!(increment_sequence(7, 2) == 9);
        assert2::check!(increment_sequence(i32::MAX, 1) == 0);
        assert2::check!(increment_sequence(i32::MAX - 1, 3) == 1);
    }

    #[test]
    fn producer_sequence_decrement_wraps_at_zero() {
        assert2::check!(decrement_sequence(9, 2) == 7);
        assert2::check!(decrement_sequence(0, 1) == i32::MAX);
        assert2::check!(decrement_sequence(1, 3) == i32::MAX - 1);
    }

    #[test]
    fn producer_sequence_increment_and_decrement_round_trip() {
        for (sequence, amount) in [(0, 0), (0, 1), (7, 11), (i32::MAX - 2, 5)] {
            assert2::check!(
                decrement_sequence(increment_sequence(sequence, amount), amount) == sequence
            );
        }
    }
}
