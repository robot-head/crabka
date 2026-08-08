//! Newtypes over the connector-offset maps.
//!
//! A [`SourceOffset`](crate::SourceOffset) is a pair of [`OffsetMap`]s. One
//! names *which* stream a position belongs to, and one names *where* in that
//! stream. Both are the same `BTreeMap<String, OffsetValue>`, so with a raw-map
//! signature a call site can transpose the two and still compile. That silently
//! corrupts every persisted checkpoint. [`PartitionMap`] and [`PositionMap`]
//! give the two halves distinct types, so the compiler rejects the swap.

use derive_more::{Deref, From, Into};
use serde::{Deserialize, Serialize};

use crate::record::OffsetMap;

/// The stream-identifying half of a [`SourceOffset`](crate::SourceOffset),
/// which is Kafka Connect's `sourcePartition`. It names *which* stream a
/// position belongs to, for example a database table, a file path, or a log
/// shard.
///
/// `#[serde(transparent)]` keeps the serialised checkpoint byte-identical to
/// the bare map, so persisted offsets round-trip unchanged. [`Deref`] exposes
/// the underlying map's read API (`get`, `contains_key`, …) at the call site.
#[derive(Debug, Clone, PartialEq, Default, Deref, From, Into, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionMap(pub OffsetMap);

/// The position-within-a-stream half of a [`SourceOffset`](crate::SourceOffset),
/// which is Kafka Connect's `sourceOffset`. It names *where* in the stream the
/// read has reached, for example a log sequence number, a byte offset, or a row
/// id.
///
/// `#[serde(transparent)]` keeps the serialised checkpoint byte-identical to
/// the bare map, so persisted offsets round-trip unchanged. [`Deref`] exposes
/// the underlying map's read API (`get`, `contains_key`, …) at the call site.
#[derive(Debug, Clone, PartialEq, Default, Deref, From, Into, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PositionMap(pub OffsetMap);
