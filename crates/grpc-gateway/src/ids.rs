//! Newtypes for the gateway's Kafka record-coordinate domain values.
//!
//! A `(partition, offset)` pair plus a `timestamp` identifies a produced or
//! consumed record. In raw form the partition is an `i32`, and the offset and
//! timestamp are two adjacent `i64`s. That pair is the textbook swap shape:
//! transpose the two in a struct literal or a call and the code still compiles
//! and mislabels the record. These wrappers make the compiler reject the
//! mix-up.
//!
//! The gateway owns these only *between* its wire edges. It reads the raw
//! primitives out of the native producer and consumer types and the generated
//! protobuf, wraps them here, and unwraps back to the primitive when it
//! re-encodes. The canonical cross-crate `Offset` and `PartitionIndex`
//! newtypes, which `protocol` owns, are a separate staged rollout. Until that
//! lands, these crate-local types convert at the boundary with `From` and
//! `Into`.
//!
//! Several carrying structs are serialised: `WebhookResponse` as JSON over
//! HTTP, `ForwardResult` on the internal forward wire, and `ClaimValue` as the
//! compacted dedup-topic value. Every newtype is therefore
//! `#[serde(transparent)]`. The encoded JSON is exactly the inner primitive and
//! never a wrapping object, which keeps the byte shape identical.

/// The canonical cross-crate Kafka `(partition, offset)` coordinate.
/// `Timestamp` stays gateway-local, because it is not one of the shared core
/// identifiers.
pub use crabka_ids::{Offset, PartitionIndex};
use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A record's timestamp in epoch milliseconds. It is an `i64` on the wire.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(pub i64);
