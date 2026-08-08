//! Domain newtypes for the audit crate.
//!
//! These types wrap the same-typed primitives that recur across the hash-chain,
//! the spool, and the verifier. A transposed call site, for example
//! `set_depth(bytes, count)`, is then a compile error and not a silent
//! corruption. See the [newtype guidance] in the style guide.
//!
//! [newtype guidance]: ../../../docs/style_guides/code_style_guide.md

use core::cmp::Ordering;

use derive_more::{Add, AddAssign, Display, From, Into};

/// Per-broker hash-chain sequence number in each record's `seq` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct Seq(pub u64);

/// Epoch-millisecond timestamp for the checkpoint `time` and the OCSF `time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct EpochMs(pub i64);

/// Count of chained data records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct RecordCount(pub u64);

/// Count of signed checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct CheckpointCount(pub u64);

/// Number of bytes currently held in the spool.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into, Add, AddAssign,
)]
pub struct SpoolBytes(pub u64);

/// Configured upper bound on spool size in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct MaxSpoolBytes(pub u64);

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

impl_primitive_cmp!(Seq, u64);
impl_primitive_cmp!(EpochMs, i64);
impl_primitive_cmp!(RecordCount, u64);
impl_primitive_cmp!(CheckpointCount, u64);
impl_primitive_cmp!(SpoolBytes, u64);
impl_primitive_cmp!(MaxSpoolBytes, u64);
