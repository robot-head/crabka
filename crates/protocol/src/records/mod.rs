//! Typed v2 record batch decoder/encoder.
//!
//! This module handles the modern Kafka `RecordBatch` format used by
//! Produce, Fetch, and log storage. Legacy v0/v1 `MessageSet` conversion is
//! implemented in `crabka-records-legacy`.

pub(crate) mod borrowed;
mod crc;
mod error;
pub mod header;
pub mod metadata;
pub(crate) mod owned;
mod payload;

pub use borrowed::{
    Record as RecordBorrowed, RecordBatch as RecordBatchBorrowed,
    RecordHeader as RecordHeaderBorrowed,
};
pub use error::RecordsError;
pub use header::HEADER_LEN;
pub use header::{Attributes, RecordBatchHeader, TimestampType};
pub use owned::{Record, RecordBatch, RecordHeader};
pub use payload::{RecordsPayload, RecordsPayloadBorrowed};
