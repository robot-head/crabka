//! Typed v2 record batch decoder/encoder.
//!
//! This module handles the modern Kafka `RecordBatch` format used by
//! Produce, Fetch, and log storage. Legacy v0/v1 `MessageSet` conversion is
//! implemented in `crabka-records-legacy`.

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
pub use header::HEADER_LEN;
pub use header::{
    Attributes, CRC_COVERAGE_START, RecordBatchHeader, TimestampType,
    patch_base_offset_and_leader_epoch,
};
pub use owned::{Record, RecordBatch, RecordHeader};
pub use payload::{RecordsPayload, RecordsPayloadBorrowed};
pub use produce_passthrough::{
    PartitionRecordSlice, ProduceFraming, ProduceFramingTopic, produce_framing,
    produce_record_slices,
};
