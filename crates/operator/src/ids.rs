//! Newtypes for the operator's domain counts and generations.
//!
//! Several places in the operator carry runs of adjacent same-typed
//! primitives whose meanings differ — the textbook transposition hazard,
//! where swapping two arguments (or two struct fields) still compiles and
//! silently produces a wrong value:
//!
//! - A rebalance proposal summary is six adjacent `i32`s
//!   (`replica`/`leader` movement counts, and the before/after `max`
//!   replica/leader counts). They are copied field-by-field from the
//!   rebalancer RPC type onto the CRD status type — swap two and the k8s
//!   status silently lies.
//! - A CA's `cert_generation` and `key_generation` are two adjacent `u64`
//!   monotonic counters threaded from the parsed Secret annotations, through
//!   the rotation planner, onto the CRD status.
//! - A cluster rollup sums `replicas` and `ready_replicas` (two adjacent
//!   `i32`s) across pools; transposing them inverts the ready/total ratio.
//!
//! These wrappers make the compiler reject the mix-up. Where a newtype lands
//! on a `Serialize`/`Deserialize` CRD type (`OptimizationResult`,
//! `CertificateAuthorityStatus`), it is `#[serde(transparent)]` so the
//! encoded Kubernetes JSON is byte-identical to the bare primitive — the CRD
//! schema and stored status are unchanged.
//!
//! `ClusterRollup` is internal (`pub(crate)`, never serialized), so its
//! count newtypes need no serde impls; they convert to/from the raw
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

/// Max replicas on any one broker (before or after a proposal applies).
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

/// Max partitions led by any one broker (before or after a proposal applies).
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

/// Monotonic generation of a CA's active signing *cert* (bumped on same-key
/// renewal and on key promotion).
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

/// Monotonic generation of a CA's active signing *key* (bumped only on key
/// replacement).
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
