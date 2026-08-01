//! Validated checkpoint runtime scalar values.

use std::str::FromStr;

use crabka_units::{ByteSize, Time, convert::ByteSizeExt as _, mebibytes, secs};
use refined_type::rule::{GreaterEqualUsize, GreaterUsize};

/// Default checkpoint trigger threshold in committed WAL frames.
///
/// A frame count, not a magnitude, so it stays a plain integer.
pub const DEFAULT_CHECKPOINT_FRAMES: u64 = 10_000;
/// Default checkpoint trigger threshold in committed WAL bytes.
pub const DEFAULT_CHECKPOINT_BYTES: ByteSize = mebibytes(64);
/// Default Kafka `DeleteRecords` timeout.
pub const DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT: Time = secs(30);
/// Default checkpoint threshold polling interval.
pub const DEFAULT_CHECKPOINT_POLL_INTERVAL: Time = secs(1);
/// Default idle-suspend polling interval.
pub const DEFAULT_IDLE_SUSPEND_POLL_INTERVAL: Time = secs(1);

/// Checkpoint part size large enough to contain the fixed part header.
///
/// The validated magnitude is stored as a `usize` so the type stays `Eq` for the
/// operator's CRD spec, which diffs by equality; [`Self::into_value`] is the seam
/// that hands out a dimensioned [`ByteSize`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointPartBytes(usize);

impl CheckpointPartBytes {
    /// Validate a checkpoint part size.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is less than eight bytes.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterEqualUsize::<8>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated part size.
    #[must_use]
    pub fn into_value(self) -> ByteSize {
        ByteSize::from_bytes(u64::try_from(self.0).unwrap_or(u64::MAX))
    }
}

impl FromStr for CheckpointPartBytes {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// A positive `usize` runtime value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveUsize(usize);

impl PositiveUsize {
    /// Validate a positive `usize`.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Return the validated value.
    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

impl FromStr for PositiveUsize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_units::convert::TimeExt as _;

    use super::*;

    #[test]
    fn owns_cross_layer_checkpoint_defaults() {
        check!(DEFAULT_CHECKPOINT_FRAMES == 10_000);
        check!(DEFAULT_CHECKPOINT_BYTES.bytes_u64() == 67_108_864);
        check!(DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT.millis_i32() == 30_000);
        check!(DEFAULT_CHECKPOINT_POLL_INTERVAL.millis_i64() == 1_000);
        check!(DEFAULT_IDLE_SUSPEND_POLL_INTERVAL.millis_i64() == 1_000);
    }

    #[test]
    fn checkpoint_part_size_carries_its_dimension() {
        let part = CheckpointPartBytes::new(4_096).expect("a 4 KiB part is valid");

        check!(part.into_value() == crabka_units::kibibytes(4));
        check!(CheckpointPartBytes::new(7).is_err());
    }
}
