//! Newtypes over the bare integers the driver and report layers thread
//! around, so a mix-up between two same-typed values (a message count vs a
//! window length, a wallclock epoch-ms vs a ms-into-run offset) is a compile
//! error rather than a silently wrong benchmark number.
//!
//! Each type is `#[serde(transparent)]`, so the on-disk `RunOutput` JSON is
//! byte-identical to the bare primitive it wraps — the report aggregator and
//! any external tooling reading the artifacts see no change.

use core::cmp::Ordering;

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

/// A window length in **seconds** (e.g. the Prometheus `rate()` window the
/// resource capture is measured over). Distinct from any count so the two
/// adjacent `u64` arguments of `capture_resource` cannot be transposed.
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
pub struct DurationSeconds(pub u64);

/// A number of Kafka messages (produced, consumed, or dropped).
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
pub struct MessageCount(pub u64);

/// Milliseconds **into a run** — a time-series sample offset relative to the
/// measurement (or wallclock) window start. Ordered so it can key the
/// per-offset averaging maps. Not to be confused with [`WallclockMs`], which
/// is an absolute unix-epoch timestamp.
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
pub struct TimeOffsetMs(pub u64);

/// An **absolute** wallclock timestamp in unix-epoch milliseconds (`i64` to
/// match `chrono::Utc::now().timestamp_millis()`). Distinct from the
/// run-relative [`TimeOffsetMs`] so the two paired start/end fields on
/// `RunOutput` cannot be swapped.
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
pub struct WallclockMs(pub i64);

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

impl_primitive_cmp!(DurationSeconds, u64);
impl_primitive_cmp!(MessageCount, u64);
impl_primitive_cmp!(TimeOffsetMs, u64);
impl_primitive_cmp!(WallclockMs, i64);
