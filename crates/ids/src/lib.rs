//! Canonical newtypes for Crabka's cross-crate Kafka identifiers.
//!
//! A raw `i64` or `i32` carries no meaning, and Kafka's domain is full of
//! values of the same type that are catastrophic to mix up. A log offset, a
//! producer id, and a timestamp are all `i64`. If the code threads them as bare
//! integers, a caller can transpose two arguments and the code still compiles.
//! These newtypes give the recurring *cross-crate* identifiers one shared type,
//! so the compiler rejects the mix-up. The same `Offset` then flows through the
//! log, broker, consensus, replication, and observability crates, and no crate
//! mints its own incompatible copy.
//!
//! # Wire boundary
//!
//! The generated Kafka wire codec, the `generated` module of
//! `crabka-protocol`, stays raw. It must be byte-exact. Wrap a value in one of
//! these newtypes when it enters the hand-written domain layer. Unwrap it with
//! `.0` or [`Into`] when you write it back to a generated message or to an
//! on-disk format. Every newtype here is `#[serde(transparent)]`, so a
//! serialized field carries the bare inner primitive.
//!
//! # Comparison against raw primitives
//!
//! Each newtype implements `PartialEq` and `PartialOrd` against its own inner
//! primitive in both directions. `offset >= 0`, `node_id == 7`, and
//! `epoch == LeaderEpoch::UNKNOWN` thus read without an explicit `.0`. This
//! applies to *comparisons only*. You still cannot pass a newtype where its
//! primitive is expected, and you cannot pass a primitive where its newtype is
//! expected. You cannot use a newtype as a map key of a different type, and you
//! cannot compare it against a *different* newtype. The argument-transposition
//! safety that motivates these types thus stays.
//!
//! Named constants hold the sentinel values that carry Kafka meaning:
//! [`Offset::ZERO`], [`ProducerId::NONE`], and [`LeaderEpoch::UNKNOWN`].
//!
//! See `docs/newtype-safety-rollout.md` and the code style guide's
//! "Newtypes for Domain Values" section.

use core::{
    cmp::Ordering,
    ops::{Add, AddAssign, Sub},
};

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A record offset within a topic partition's log. KIP wire type: `int64`.
///
/// The type is ordered, so it works as a `BTreeMap` key and in watermark
/// comparisons. Advance or rewind by a count with `offset + n` or `offset - n`,
/// where `n: i64`. The delta between two offsets is `a.0 - b.0`. The addition
/// of two offsets has no meaning, and this crate does not implement it.
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
    /// The log's first offset. It is also the initial log-start offset and the
    /// high-watermark of an empty partition.
    pub const ZERO: Self = Offset(0);

    /// The inner `i64`. Use it at the wire and generated boundary, and for
    /// arithmetic against other integer quantities.
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

/// A partition index within a topic. KIP wire type: `int32`.
///
/// The type is ordered and hashable, so it keys the per-partition maps in the
/// broker and replication paths. A partition index is an identifier, not a
/// quantity, so this crate implements no arithmetic.
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
    /// The inner `i32`. Use it at the wire and generated boundary.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

/// A broker, node, replica, or controller identifier.
///
/// Crabka carries this value as a `u64` internally: the consensus peer id, the
/// KIP-853 voter id, and the partition replica, leader, and ISR ids. On the
/// Kafka wire the same value is an `int32`: `broker.id`, `node.id`, and the
/// replica ids. The protocol boundary converts between the two. The value is an
/// identifier, not a quantity, so this crate supplies no arithmetic.
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
    /// The inner `u64`. Use it at the wire and generated boundary.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A Kafka idempotent or transactional producer id. KIP-98 wire type: `int64`.
///
/// The id identifies a producer session across the broker's producer-state and
/// transaction paths and across the log's aborted-transaction index. `-1`, the
/// `NO_PRODUCER_ID` sentinel, is valid and round-trips as the inner `i64`. The
/// id is an identifier, not a quantity, so this crate supplies no arithmetic.
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
    /// KIP-98 `NO_PRODUCER_ID` (`-1`): no idempotent or transactional producer
    /// is assigned.
    pub const NONE: Self = ProducerId(-1);

    /// The inner `i64`. Use it at the wire and generated boundary.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    /// Whether this is the [`ProducerId::NONE`] sentinel, which means that no
    /// producer is assigned.
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 == Self::NONE.0
    }
}

/// A partition leader epoch. KIP-320 wire type: `int32`.
///
/// This is a monotonic per-partition counter, and every leader change
/// increments it. It fences stale leaders and it bounds follower log
/// truncation. `-1`, the `UNKNOWN_LEADER_EPOCH` sentinel, is a valid wire value
/// and round-trips as the inner `i32`. The type is ordered, so epochs compare
/// directly. Advance to the next epoch with [`LeaderEpoch::next`].
///
/// Note: the deterministic consensus core, `crabka-kraft-core`, tracks its own
/// always-non-negative epoch as a `u32`, `crabka_kraft_core::types::Epoch`.
/// `crabka-raft` converts to and from this wire type at the controller
/// boundary.
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
    /// unset. Examples are an older client, and a partition with no elected
    /// leader yet.
    pub const UNKNOWN: Self = LeaderEpoch(-1);

    /// The epoch a partition's first leader starts at (`0`).
    pub const INITIAL: Self = LeaderEpoch(0);

    /// The inner `i32`. Use it at the wire and generated boundary.
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

    /// The next epoch after this one. A leader change increments the epoch by
    /// one.
    #[must_use]
    pub const fn next(self) -> Self {
        LeaderEpoch(self.0 + 1)
    }
}

/// A Kafka request API key, as the raw wire `int16`.
///
/// This is the numeric code in the `ApiKey` field of a request header. It is
/// different from the typed `crabka_protocol::ApiKey` *enum*, which names each
/// key, such as `Produce` and `Fetch`. This newtype is the boundary value that
/// goes through the hand-written header construction and through the tap and
/// proxy frame parsers. It pairs with an [`ApiVersion`], and the two are
/// adjacent `i16`s that must not be transposed.
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
    /// The inner `i16`. Use it at the wire and generated boundary.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

/// A Kafka request and response API version. Wire type: `int16`.
///
/// It pairs with an [`ApiKey`] in a request header. The two adjacent `i16`s are
/// the textbook swap shape, and these distinct types prevent that swap.
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
    /// The inner `i16`. Use it at the wire and generated boundary.
    #[must_use]
    pub const fn get(self) -> i16 {
        self.0
    }
}

/// Generates cross-type comparison impls in both directions, so the code can
/// compare a domain newtype directly against its raw inner primitive without an
/// explicit `.0`. Examples are `offset >= 0` and `node_id == 7`. The macro is
/// scoped to comparisons, not to argument passing and not to map keys. See the
/// module docs.
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
    use super::{ApiKey, ApiVersion, LeaderEpoch, NodeId, Offset, PartitionIndex, ProducerId};

    #[test]
    fn accessors_and_advance_return_the_inner_value() {
        assert2::assert!(Offset(9).get() == 9);
        assert2::assert!(PartitionIndex(4).get() == 4);
        assert2::assert!(NodeId(7).get() == 7);
        assert2::assert!(ProducerId(42).get() == 42);
        assert2::assert!(LeaderEpoch(3).get() == 3);
        assert2::assert!(ApiKey(18).get() == 18);
        assert2::assert!(ApiVersion(2).get() == 2);
        assert2::assert!(LeaderEpoch(4).next() == LeaderEpoch(5));
    }

    #[test]
    fn compares_against_raw_primitive_in_both_directions() {
        assert2::assert!(Offset(5) == 5);
        assert2::assert!(5 == Offset(5));
        assert2::assert!(Offset(5) != 6);
        assert2::assert!(Offset(5) > 3);
        assert2::assert!(3 < Offset(5));
        assert2::assert!(Offset::ZERO >= 0);
        assert2::assert!(NodeId(7) == 7);
        assert2::assert!(ApiKey(18) == 18);
    }

    #[test]
    fn newtype_to_newtype_comparison_still_holds() {
        assert2::assert!(Offset(1) < Offset(2));
        assert2::assert!(Offset(2) == Offset(2));
        let mut xs = [Offset(3), Offset(1), Offset(2)];
        xs.sort();
        assert2::assert!(xs == [Offset(1), Offset(2), Offset(3)]);
    }

    #[test]
    fn sentinels_carry_kafka_meaning() {
        assert2::assert!(LeaderEpoch::UNKNOWN == -1);
        assert2::assert!(!LeaderEpoch::UNKNOWN.is_known());
        assert2::assert!(LeaderEpoch::INITIAL == 0);
        assert2::assert!(LeaderEpoch::INITIAL.is_known());
        assert2::assert!(ProducerId::NONE == -1);
        assert2::assert!(ProducerId::NONE.is_none());
        assert2::assert!(!ProducerId(0).is_none());
        assert2::assert!(Offset::ZERO == 0);
    }
}
