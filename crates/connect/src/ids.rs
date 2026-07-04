//! Newtypes over the connector-offset maps.
//!
//! A [`SourceOffset`](crate::SourceOffset) is a pair of [`OffsetMap`]s: one
//! naming *which* stream a position belongs to, one naming *where* in that
//! stream. Both are the same `BTreeMap<String, OffsetValue>`, so a raw-map
//! signature lets the two be transposed at a call site and still compile — a
//! silent corruption of every persisted checkpoint. [`PartitionMap`] and
//! [`PositionMap`] give the two halves distinct types so the compiler rejects
//! the swap.

use derive_more::{Deref, From, Into};
use serde::{Deserialize, Serialize};

use crate::record::OffsetMap;

/// The stream-identifying half of a [`SourceOffset`](crate::SourceOffset) —
/// Kafka Connect's `sourcePartition`. Names *which* stream a position belongs
/// to (a database table, a file path, a log shard).
///
/// `#[serde(transparent)]` keeps the serialised checkpoint byte-identical to
/// the bare map, so persisted offsets round-trip unchanged. [`Deref`] exposes
/// the underlying map's read API (`get`, `contains_key`, …) at the call site.
#[derive(Debug, Clone, PartialEq, Default, Deref, From, Into, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PartitionMap(pub OffsetMap);

/// The position-within-a-stream half of a [`SourceOffset`](crate::SourceOffset)
/// — Kafka Connect's `sourceOffset`. Names *where* in the stream reading has
/// reached (a log sequence number, a byte offset, a row id).
///
/// `#[serde(transparent)]` keeps the serialised checkpoint byte-identical to
/// the bare map, so persisted offsets round-trip unchanged. [`Deref`] exposes
/// the underlying map's read API (`get`, `contains_key`, …) at the call site.
#[derive(Debug, Clone, PartialEq, Default, Deref, From, Into, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PositionMap(pub OffsetMap);
