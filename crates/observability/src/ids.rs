//! Kafka identifier newtypes for the log WAL / compaction path.
//!
//! The compactor and querier carry a partition index and a log offset together
//! everywhere: [`WalPosition`](crate::WalPosition),
//! [`KafkaWalRecord`](crate::KafkaWalRecord), the per-partition
//! `partition_offsets` map inside
//! [`CompactionFrontier`](crate::CompactionFrontier), and the
//! `commit_positions` map in the compaction loop. The newtypes make a
//! transposed `{ partition, offset }` a compile error.
//!
//! These are the canonical cross-crate [`crabka_ids`] types, the same
//! `Offset` and `PartitionIndex` this crate consumes from the Kafka WAL, so a
//! value inside the domain layer needs no conversion. The WAL-decode
//! boundary, that is the consumer `poll`, wraps the raw `i32` and `i64`
//! values. `.get()` unwraps them again for the raw
//! `crabka_blockstore::BlockKey` on-disk key. Advance an offset to the next
//! commit position with `offset + 1`.

pub use crabka_ids::{Offset, PartitionIndex};
