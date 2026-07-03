//! Canonical newtypes for Crabka's cross-crate Kafka identifiers.
//!
//! A raw `i64` or `i32` carries no meaning, and Kafka's domain is full of
//! same-typed values that are catastrophic to mix up — a log offset, a producer
//! id, and a timestamp are all `i64`. Threading them as bare integers lets a
//! caller transpose two arguments and still compile. These newtypes give the
//! recurring *cross-crate* identifiers a single shared type so the compiler
//! rejects the mix-up, and so the same `Offset` flows through the log, broker,
//! consensus, replication, and observability crates rather than each crate
//! minting its own incompatible copy.
//!
//! # Wire boundary
//!
//! The generated Kafka wire codec (`crabka-protocol`'s `generated` module) stays
//! raw — it must be byte-exact. Wrap a value in one of these newtypes when it
//! enters the hand-written domain layer, and unwrap it (`.0` or [`Into`]) when
//! it is written back to a generated message or an on-disk format. Every newtype
//! here is `#[serde(transparent)]`, so a serialized field is encoded as the bare
//! inner primitive.
//!
//! See `docs/newtype-safety-rollout.md` and the code style guide's
//! "Newtypes for Domain Values" section.

use core::ops::{Add, AddAssign, Sub};

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A record offset within a topic partition's log (KIP wire type: `int64`).
///
/// Ordered, so it works as a `BTreeMap` key and in watermark comparisons.
/// Advance or rewind by a count with `offset + n` / `offset - n` (where `n: i64`);
/// the delta between two offsets is `a.0 - b.0`. Adding two offsets is
/// meaningless and is deliberately not implemented.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub struct Offset(pub i64);

impl Offset {
    /// The inner `i64` — use at the wire/generated boundary and for arithmetic
    /// against other integer quantities.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl Add<i64> for Offset {
    type Output = Offset;
    fn add(self, count: i64) -> Offset {
        Offset(self.0 + count)
    }
}

impl Sub<i64> for Offset {
    type Output = Offset;
    fn sub(self, count: i64) -> Offset {
        Offset(self.0 - count)
    }
}

impl AddAssign<i64> for Offset {
    fn add_assign(&mut self, count: i64) {
        self.0 += count;
    }
}

/// A partition index within a topic (KIP wire type: `int32`).
///
/// Ordered and hashable so it keys the per-partition maps that pervade the
/// broker and replication paths. Partition indices are identifiers, not
/// quantities, so no arithmetic is implemented.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub struct PartitionIndex(pub i32);

impl PartitionIndex {
    /// The inner `i32` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// A broker / node / replica / controller identifier.
///
/// Crabka carries this as a `u64` internally (consensus peer id, KIP-853 voter
/// id, partition replica/leader/ISR id); on the Kafka wire the same value is an
/// `int32` (`broker.id`, `node.id`, replica ids), converted at the protocol
/// boundary. It is an identifier, not a quantity, so no arithmetic is provided.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub struct NodeId(pub u64);

impl NodeId {
    /// The inner `u64` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A Kafka idempotent/transactional producer id (KIP-98 wire type: `int64`).
///
/// Identifies a producer session across the broker's producer-state and
/// transaction paths and the log's aborted-transaction index. `-1`
/// (`NO_PRODUCER_ID`) is a valid sentinel and round-trips as the inner `i64`.
/// It is an identifier, not a quantity, so no arithmetic is provided.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub struct ProducerId(pub i64);

impl ProducerId {
    /// The inner `i64` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// A partition leader epoch (KIP-320 wire type: `int32`).
///
/// Monotonic per-partition counter bumped on every leader change; used to fence
/// stale leaders and to bound follower log truncation. `-1`
/// (`UNKNOWN_LEADER_EPOCH`) is a valid wire sentinel and round-trips as the inner
/// `i32`. Ordered so epochs compare directly; advance to the next epoch with
/// [`LeaderEpoch::next`].
///
/// Note: the deterministic consensus core (`crabka-kraft-core`) tracks its own
/// always-non-negative epoch as a `u32` (`crabka_kraft_core::types::Epoch`);
/// `crabka-raft` converts to and from this wire type at the controller boundary.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub struct LeaderEpoch(pub i32);

impl LeaderEpoch {
    /// The inner `i32` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }

    /// The next epoch after this one (a leader change bumps the epoch by one).
    #[must_use]
    pub const fn next(self) -> Self {
        LeaderEpoch(self.0 + 1)
    }
}
