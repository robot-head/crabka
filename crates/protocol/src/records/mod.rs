//! Typed v2 record batch decoder/encoder.
//!
//! See `docs/superpowers/specs/2026-05-11-crabka-records-1c-design.md`.
//! v0/v1 record batches are deferred to `crabka-log`.

mod borrowed;
mod crc;
mod error;
pub mod header;
mod owned;

pub use borrowed::{
    Record as RecordBorrowed, RecordBatch as RecordBatchBorrowed,
    RecordHeader as RecordHeaderBorrowed,
};
pub use error::RecordsError;
pub use header::HEADER_LEN;
pub use header::{Attributes, RecordBatchHeader, TimestampType};
pub use owned::{Record, RecordBatch, RecordHeader};
