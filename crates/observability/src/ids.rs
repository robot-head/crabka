//! Kafka identifier newtypes for the log WAL / compaction path.
//!
//! The compactor and querier carry a partition index and a log offset together
//! everywhere — [`WalPosition`](crate::WalPosition), [`KafkaWalRecord`](crate::KafkaWalRecord),
//! the per-partition `partition_offsets` map inside
//! [`CompactionFrontier`](crate::CompactionFrontier), and the `commit_positions`
//! map in the compaction loop. Wrapping them makes a transposed
//! `{ partition, offset }` a compile error. These are the canonical cross-crate
//! [`crabka_ids`] types — the same `Offset`/`PartitionIndex` this crate consumes
//! from the Kafka WAL, so no conversion is needed once a value is inside the
//! domain layer. Raw `i32`/`i64` are wrapped at the WAL-decode boundary (the
//! consumer `poll`) and unwrapped with `.get()` when handed to the raw
//! `crabka_blockstore::BlockKey` on-disk key. Advance an offset to the next
//! commit position with `offset + 1`.

pub use crabka_ids::{Offset, PartitionIndex};
