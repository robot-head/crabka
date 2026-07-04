//! Kafka identifier newtypes for the WAL-head and ruler-state replay paths.
//!
//! The replay code carries a partition index and an offset together everywhere
//! (`WalHeadConsumerRecord`, `WalHeadPartitionOffset`, the `MissingValue` error
//! variants, the `committed_offsets` maps); wrapping them makes a transposed
//! `{ partition, offset }` a compile error. These are the canonical cross-crate
//! [`crabka_ids`] types — the same `Offset`/`PartitionIndex` this crate hands to
//! `crabka_promql::WalHead::apply_wal_record_at`, so no conversion is needed at
//! that boundary. Advance an offset to the next commit position with `offset + 1`.

pub use crabka_ids::{Offset, PartitionIndex};
