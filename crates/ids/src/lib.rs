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
//! # Comparison against raw primitives
//!
//! For ergonomics, each newtype implements `PartialEq`/`PartialOrd` against its
//! own inner primitive in both directions, so `offset >= 0`, `node_id == 7`, or
//! `epoch == LeaderEpoch::UNKNOWN` read without an explicit `.0`. This is scoped
//! to *comparisons only*: a newtype still cannot be passed where its primitive is
//! expected (or vice versa), used as a differently-typed map key, or compared
//! against a *different* newtype — so the argument-transposition safety that
//! motivates these types is preserved. Sentinel values that carry Kafka meaning
//! are exposed as named constants ([`Offset::ZERO`], [`ProducerId::NONE`],
//! [`LeaderEpoch::UNKNOWN`]) rather than bare integers.
//!
//! See `docs/newtype-safety-rollout.md` and the code style guide's
//! "Newtypes for Domain Values" section.

use core::{
    cmp::Ordering,
    ops::{Add, AddAssign, Sub},
};

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
    /// The log's first offset — also the initial log-start offset and the
    /// high-watermark of an empty partition.
    pub const ZERO: Self = Offset(0);

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
    /// KIP-98 `NO_PRODUCER_ID` (`-1`): no idempotent/transactional producer is
    /// assigned.
    pub const NONE: Self = ProducerId(-1);

    /// The inner `i64` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Whether this is the [`ProducerId::NONE`] sentinel (no producer assigned).
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == Self::NONE.0
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
    /// KIP-320 `UNKNOWN_LEADER_EPOCH` (`-1`): the leader epoch is unknown or
    /// unset (e.g. an older client, or a partition with no elected leader yet).
    pub const UNKNOWN: Self = LeaderEpoch(-1);

    /// The epoch a partition's first leader starts at (`0`).
    pub const INITIAL: Self = LeaderEpoch(0);

    /// The inner `i32` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Whether this is a real epoch rather than the
    /// [`UNKNOWN`](LeaderEpoch::UNKNOWN) sentinel.
    #[must_use]
    pub const fn is_known(self) -> bool {
        self.0 >= 0
    }

    /// The next epoch after this one (a leader change bumps the epoch by one).
    #[must_use]
    pub const fn next(self) -> Self {
        LeaderEpoch(self.0 + 1)
    }
}

/// A Kafka request API key, as the raw wire `int16`.
///
/// This is the numeric code in a request header (`ApiKey` field). It is distinct
/// from the typed [`crabka_protocol::ApiKey`] *enum*, which names each key
/// (`Produce`, `Fetch`, …): this newtype is the boundary value threaded through
/// hand-written header construction and the tap/proxy frame parsers, paired with
/// an [`ApiVersion`] — two adjacent `i16`s that must not be transposed.
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
pub struct ApiKey(pub i16);

impl ApiKey {
    /// The inner `i16` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

/// A Kafka request/response API version (wire type: `int16`).
///
/// Paired with an [`ApiKey`] in a request header; the two adjacent `i16`s are the
/// textbook swap shape, which these distinct types prevent.
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
pub struct ApiVersion(pub i16);

impl ApiVersion {
    /// The inner `i16` — use at the wire/generated boundary.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

/// Generates cross-type comparison impls (both directions) so a domain newtype
/// can be compared directly against its raw inner primitive — e.g. `offset >= 0`
/// or `node_id == 7` — without an explicit `.0`. Deliberately scoped to
/// comparisons (not argument passing or map keys); see the module docs.
macro_rules! impl_primitive_cmp {
    ($ty:ty, $inner:ty) => {
        impl PartialEq<$inner> for $ty {
            #[inline]
            fn eq(&self, other: &$inner) -> bool {
                self.0 == *other
            }
        }
        impl PartialEq<$ty> for $inner {
            #[inline]
            fn eq(&self, other: &$ty) -> bool {
                *self == other.0
            }
        }
        impl PartialOrd<$inner> for $ty {
            #[inline]
            fn partial_cmp(&self, other: &$inner) -> Option<Ordering> {
                self.0.partial_cmp(other)
            }
        }
        impl PartialOrd<$ty> for $inner {
            #[inline]
            fn partial_cmp(&self, other: &$ty) -> Option<Ordering> {
                self.partial_cmp(&other.0)
            }
        }
    };
}

impl_primitive_cmp!(Offset, i64);
impl_primitive_cmp!(PartitionIndex, i32);
impl_primitive_cmp!(NodeId, u64);
impl_primitive_cmp!(ProducerId, i64);
impl_primitive_cmp!(LeaderEpoch, i32);
impl_primitive_cmp!(ApiKey, i16);
impl_primitive_cmp!(ApiVersion, i16);

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{ApiKey, ApiVersion, LeaderEpoch, NodeId, Offset, PartitionIndex, ProducerId};

    #[test]
    fn accessors_and_advance_return_the_inner_value() {
        check!(Offset(9).get() == 9);
        check!(PartitionIndex(4).get() == 4);
        check!(NodeId(7).get() == 7);
        check!(ProducerId(42).get() == 42);
        check!(LeaderEpoch(3).get() == 3);
        check!(ApiKey(18).get() == 18);
        check!(ApiVersion(2).get() == 2);
        check!(LeaderEpoch(4).next() == LeaderEpoch(5));
    }

    #[test]
    fn compares_against_raw_primitive_in_both_directions() {
        check!(Offset(5) == 5);
        check!(5 == Offset(5));
        check!(Offset(5) != 6);
        check!(Offset(5) > 3);
        check!(3 < Offset(5));
        check!(Offset::ZERO >= 0);
        check!(NodeId(7) == 7);
        check!(ApiKey(18) == 18);
    }

    #[test]
    fn newtype_to_newtype_comparison_still_holds() {
        check!(Offset(1) < Offset(2));
        check!(Offset(2) == Offset(2));
        let mut xs = [Offset(3), Offset(1), Offset(2)];
        xs.sort();
        check!(xs == [Offset(1), Offset(2), Offset(3)]);
    }

    #[test]
    fn sentinels_carry_kafka_meaning() {
        check!(LeaderEpoch::UNKNOWN == -1);
        check!(!LeaderEpoch::UNKNOWN.is_known());
        check!(LeaderEpoch::INITIAL == 0);
        check!(LeaderEpoch::INITIAL.is_known());
        check!(ProducerId::NONE == -1);
        check!(ProducerId::NONE.is_none());
        check!(!ProducerId(0).is_none());
        check!(Offset::ZERO == 0);
    }
}
