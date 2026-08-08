//! Newtypes over the bare integers that the driver and report layers thread
//! around. A mix-up between two same-typed values becomes a compile error and
//! not a silently wrong benchmark number. Two such pairs are a message count
//! against a window length, and a wallclock epoch-ms against a ms-into-run
//! offset.
//!
//! Each type is `#[serde(transparent)]`, so the on-disk `RunOutput` JSON is
//! byte-identical to the bare primitive it wraps. The report aggregator and
//! any external tool that reads the artifacts see no change.

use core::cmp::Ordering;

use crabka_units::prelude::*;
use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};

use crate::numeric::saturating_u64_to_i64;

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

/// Milliseconds **into a run**. This is a time-series sample offset relative to
/// the start of the measurement window or the wallclock window. It is ordered,
/// so it can key the per-offset averaging maps. Do not confuse it with
/// [`WallclockMs`], which is an absolute unix-epoch timestamp.
///
/// This stays an integer and does not become a [`Time`], because it is a
/// *coordinate* on the fixed sampling grid. The cross-run averaging in
/// [`crate::aggregate`] keys `BTreeMap`s by it, which a `f64`-backed quantity
/// cannot do. [`Self::as_time`] and [`Self::since`] are the seams that turn a
/// pair of coordinates into the extent between them.
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

impl TimeOffsetMs {
    /// This offset as the time extent from the window start.
    #[must_use]
    pub fn as_time(self) -> Time {
        Time::from_millis(saturating_u64_to_i64(self.0))
    }

    /// The extent from `earlier` to this offset. This is no time at all when
    /// the two are the wrong way round.
    #[must_use]
    pub fn since(self, earlier: Self) -> Time {
        Time::from_millis(saturating_u64_to_i64(self.0.saturating_sub(earlier.0)))
    }
}

/// An **absolute** wallclock timestamp in unix-epoch milliseconds. It is an
/// `i64` to match `chrono::Utc::now().timestamp_millis()`. It is distinct from
/// the run-relative [`TimeOffsetMs`], so no one can swap the paired start and
/// end fields on `RunOutput`.
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

impl_primitive_cmp!(MessageCount, u64);
impl_primitive_cmp!(TimeOffsetMs, u64);
impl_primitive_cmp!(WallclockMs, i64);

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn offsets_convert_to_extents_and_differences() {
        check!(TimeOffsetMs(2_500).as_time() == millis(2500));
        check!(TimeOffsetMs(6_000).since(TimeOffsetMs(4_000)) == secs(2));
        // Out of order: no negative extents.
        check!(TimeOffsetMs(4_000).since(TimeOffsetMs(6_000)) == Time::ZERO);
    }
}
