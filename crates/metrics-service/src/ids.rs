//! Kafka identifier newtypes for the WAL-head and ruler-state replay paths.
//!
//! The replay code carries a partition index and an offset together everywhere:
//! in `WalHeadConsumerRecord`, in `WalHeadPartitionOffset`, in the
//! `MissingValue` error variants, and in the `committed_offsets` maps. These
//! newtypes make a transposed `{ partition, offset }` a compile error.
//!
//! These are the canonical cross-crate [`crabka_ids`] types. This crate gives
//! the same `Offset`/`PartitionIndex` to
//! `crabka_promql::WalHead::apply_wal_record_at`, so that boundary needs no
//! conversion. Advance an offset to the next commit position with `offset + 1`.

pub use crabka_ids::{Offset, PartitionIndex};
