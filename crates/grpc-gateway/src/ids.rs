//! Newtypes for the gateway's Kafka record-coordinate domain values.
//!
//! A produced/consumed record is identified by a `(partition, offset)` pair
//! plus a `timestamp`. In raw form these are `i32` (partition) and two adjacent
//! `i64`s (offset, timestamp) — the offset/timestamp pair is the textbook swap
//! shape: transpose them in a struct literal or a call and it still compiles,
//! silently mislabelling the record. These wrappers make the compiler reject
//! the mix-up.
//!
//! The gateway owns these only *between* its wire edges — it reads the raw
//! primitives out of the native producer/consumer types and the generated
//! protobuf, wraps them here, and unwraps back to the primitive when it
//! re-encodes. The canonical cross-crate `Offset`/`PartitionIndex` newtypes
//! (owned by `protocol`) are a separate staged rollout; these crate-local
//! types convert at the boundary via `From`/`Into` until that lands.
//!
//! Several carrying structs are serialised — `WebhookResponse` (JSON over
//! HTTP), `ForwardResult` (the internal forward wire), and `ClaimValue` (the
//! compacted dedup-topic value) — so every newtype is `#[serde(transparent)]`:
//! the encoded JSON is exactly the inner primitive, never a wrapping object,
//! keeping the byte shape identical.

/// The canonical cross-crate Kafka `(partition, offset)` coordinate. `Timestamp`
/// stays gateway-local (it is not one of the shared core identifiers).
pub use crabka_ids::{Offset, PartitionIndex};
use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A record's timestamp in epoch milliseconds (`i64` on the wire).
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
