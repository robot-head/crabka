//! Kafka identifier newtypes used by the WAL-head replay path.
//!
//! The head records, per partition, the low- and high-water WAL offsets it has
//! materialized ([`PartitionWatermark`](crate::PartitionWatermark)) — two
//! adjacent offsets whose transposition would silently invert the range, and a
//! `(PartitionIndex, Offset)` pair at
//! [`apply_wal_record_at`](crate::WalHead::apply_wal_record_at). These are the
//! canonical cross-crate [`crabka_ids`] types, so the same `Offset` flows in
//! from `crabka-metrics-service` (the caller) without a conversion at the seam.

pub use crabka_ids::{Offset, PartitionIndex};
