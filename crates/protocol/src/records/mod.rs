//! Typed v2 record batch decoder/encoder.
//!
//! See `docs/superpowers/specs/2026-05-11-crabka-records-1c-design.md`.
//! v0/v1 record batches are deferred to `crabka-log`.

mod crc;
mod error;
pub mod header;

pub use error::RecordsError;
pub use header::HEADER_LEN;
pub use header::{Attributes, RecordBatchHeader, TimestampType};
