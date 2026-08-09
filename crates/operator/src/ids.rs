//! Newtypes for the operator's domain counts and generations.
//!
//! Several places in the operator carry runs of adjacent same-typed
//! primitives with different meanings. This is the transposition hazard: a
//! swap of two arguments or of two struct fields still compiles and produces a
//! wrong value without a warning.
//!
//! - A rebalance proposal summary is six adjacent `i32`s: the `replica` and
//!   `leader` movement counts, and the before and after `max` replica and
//!   leader counts. The operator copies them field-by-field from the
//!   rebalancer RPC type onto the CRD status type. A swap of two of them makes
//!   the Kubernetes status report a wrong value.
//! - A CA's `cert_generation` and `key_generation` are two adjacent `u64`
//!   monotonic counters. The operator threads them from the parsed Secret
//!   annotations, through the rotation planner, onto the CRD status.
//! - A cluster rollup sums `replicas` and `ready_replicas`, two adjacent
//!   `i32`s, across pools. A transposition of the two inverts the ratio of
//!   ready to total.
//!
//! These wrappers make the compiler reject the mix-up. A newtype that lands on
//! a `Serialize` or `Deserialize` CRD type, that is, `OptimizationResult` and
//! `CertificateAuthorityStatus`, is `#[serde(transparent)]`. The encoded
//! Kubernetes JSON is then byte-identical to the bare primitive, and the CRD
//! schema and the stored status do not change.
//!
//! `ClusterRollup` is internal. It is `pub(crate)` and never serialized, so
//! its count newtypes need no serde impls. They convert to and from the raw
//! `Option<i32>` read out of pool status at the aggregation boundary.

use core::cmp::Ordering;

use derive_more::{Add, AddAssign, Display, From, Into};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Number of partition replica reassignments in a rebalance proposal.
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
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct ReplicaMovementCount(pub i32);

/// Number of leadership changes in a rebalance proposal.
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
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct LeaderMovementCount(pub i32);

/// Max replicas on any one broker, before or after a proposal applies.
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
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct MaxReplicasCount(pub i32);

/// Max partitions led by any one broker, before or after a proposal applies.
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
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct MaxLeadersCount(pub i32);

/// Monotonic generation of a CA's active signing *cert*. The operator
/// increments it on same-key renewal and on key promotion.
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
    Add,
    AddAssign,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct CertGeneration(pub u64);

/// Monotonic generation of a CA's active signing *key*. The operator
/// increments it only on key replacement.
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
    Add,
    AddAssign,
    Display,
    From,
    Into,
    Serialize,
    Deserialize,
    JsonSchema,
)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct KeyGeneration(pub u64);

/// Total desired broker replicas across a cluster's pools.
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
    Add,
    AddAssign,
    Display,
    From,
    Into,
)]
pub struct ReplicaCount(pub i32);

/// Total *ready* broker replicas across a cluster's pools.
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
    Add,
    AddAssign,
    Display,
    From,
    Into,
)]
pub struct ReadyReplicaCount(pub i32);

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

impl_primitive_cmp!(ReplicaMovementCount, i32);
impl_primitive_cmp!(LeaderMovementCount, i32);
impl_primitive_cmp!(MaxReplicasCount, i32);
impl_primitive_cmp!(MaxLeadersCount, i32);
impl_primitive_cmp!(CertGeneration, u64);
impl_primitive_cmp!(KeyGeneration, u64);
impl_primitive_cmp!(ReplicaCount, i32);
impl_primitive_cmp!(ReadyReplicaCount, i32);
