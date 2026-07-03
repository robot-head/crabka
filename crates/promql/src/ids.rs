//! Newtypes over the bare Kafka `(partition, offset)` integers the WAL-head
//! tracks as it replays the metrics WAL into the hot head.
//!
//! The head records, per partition, the low- and high-water WAL offsets it has
//! materialized ([`PartitionWatermark`](crate::PartitionWatermark)). Those two
//! offsets are otherwise just adjacent `i64` fields — the textbook swap shape:
//! transpose `low_water_offset` and `high_water_offset` in a struct literal and
//! it still compiles, silently inverting the range. Wrapping the offset in
//! [`Offset`] makes that a compile error, and pairing it with a distinct
//! [`PartitionIndex`] stops a partition being passed where an offset is expected
//! (and vice versa) at [`apply_wal_record_at`](crate::WalHead::apply_wal_record_at)
//! and [`record_offset`](crate::InMemoryMetricStore::record_offset).
//!
//! These types are *not* serialised: `PartitionWatermark` and the in-memory
//! store derive no `Serialize`/`Deserialize`, so there is no wire/JSON contract
//! to preserve and no `#[serde(transparent)]` is needed. The raw `i32`/`i64`
//! still live on the caller side (`crabka-metrics-service` owns its own
//! `(PartitionIndex, Offset)` pair over the same Kafka integers); conversion
//! happens explicitly at the crate boundary via [`From`]/[`Into`].

use derive_more::{Display, From, Into};

/// A Kafka partition index (`i32`). Ordered so it can key the per-partition
/// watermark `BTreeMap`. Distinct from [`Offset`] so the two cannot be swapped
/// at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct PartitionIndex(pub i32);

/// A Kafka WAL offset (`i64`). Ordered so the head can take the running
/// `min`/`max` of the offsets it has materialized. Distinct from
/// [`PartitionIndex`] so it cannot be swapped with a partition at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct Offset(pub i64);
