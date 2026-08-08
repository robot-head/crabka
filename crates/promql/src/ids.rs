//! Kafka identifier newtypes used by the WAL-head replay path.
//!
//! The head records the low- and high-water WAL offsets it has materialized for
//! each partition ([`PartitionWatermark`](crate::PartitionWatermark)). These two
//! offsets are adjacent, and a transposition of them inverts the range. The head
//! also records a `(PartitionIndex, Offset)` pair at
//! [`apply_wal_record_at`](crate::WalHead::apply_wal_record_at). Both are the
//! canonical cross-crate [`crabka_ids`] types, so the caller,
//! `crabka-metrics-service`, passes the same `Offset` in without a conversion.

pub use crabka_ids::{Offset, PartitionIndex};
