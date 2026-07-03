//! Newtypes over the bare Kafka `(partition, offset)` integers the WAL-head and
//! ruler-state replay paths thread around.
//!
//! Both a partition index and an offset are otherwise just integers, and the
//! replay code carries them together everywhere (`WalHeadConsumerRecord`,
//! `WalHeadPartitionOffset`, the `MissingValue` error variants, the
//! `committed_offsets` maps). Wrapping them makes a transposed
//! `{ partition, offset }` — the textbook swap bug — a compile error instead of
//! a silently mis-committed offset.
//!
//! These types are *not* serialised: none of the structs that hold them derive
//! `Serialize`/`Deserialize`, so no `#[serde(transparent)]` is needed and there
//! is no wire/JSON contract to preserve. The raw `i32`/`i64` still live on the
//! dependency-owned [`crabka_client_consumer::ConsumerRecord`] and on
//! `crabka_promql::WalHead::apply_wal_record_at`; conversion happens explicitly
//! at those boundaries via [`From`]/[`Into`].

use derive_more::{Display, From, Into};

/// A Kafka partition index (`i32`). Ordered so it can key the per-partition
/// committed-offset `BTreeMap`. Distinct from [`Offset`] so the two fields of a
/// `{ partition, offset }` pair cannot be transposed at a construction site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct PartitionIndex(pub i32);

/// A Kafka log offset (`i64`). Ordered so the replay loop can take the running
/// `max` of the next-offset-to-commit. Distinct from [`PartitionIndex`] so it
/// cannot be swapped with a partition at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct Offset(pub i64);

impl Offset {
    /// The next offset after this one — i.e. the Kafka commit offset once the
    /// record at `self` has been replayed. Replaces the bare `offset + 1`
    /// arithmetic without exposing `Add<i64>`, which would let an unrelated
    /// integer be added to an offset.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}
