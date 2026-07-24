//! Validated checkpoint runtime scalar values.

use std::str::FromStr;

use refined_type::rule::{GreaterEqualUsize, GreaterUsize};

/// Default checkpoint trigger threshold in committed WAL frames.
pub const DEFAULT_CHECKPOINT_FRAMES: u64 = 10_000;
/// Default checkpoint trigger threshold in committed WAL bytes.
pub const DEFAULT_CHECKPOINT_BYTES: u64 = 67_108_864;
/// Default Kafka `DeleteRecords` timeout in milliseconds.
pub const DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS: i32 = 30_000;
/// Default checkpoint threshold polling interval in milliseconds.
pub const DEFAULT_CHECKPOINT_POLL_INTERVAL_MS: u64 = 1_000;
/// Default idle-suspend polling interval in milliseconds.
pub const DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS: u64 = 1_000;

/// Checkpoint part size large enough to contain the fixed part header.
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

    /// Return the validated byte count.
    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
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
    use super::*;

    #[test]
    fn owns_cross_layer_checkpoint_defaults() {
        assert_eq!(DEFAULT_CHECKPOINT_FRAMES, 10_000);
        assert_eq!(DEFAULT_CHECKPOINT_BYTES, 67_108_864);
        assert_eq!(DEFAULT_CHECKPOINT_DELETE_RECORDS_TIMEOUT_MS, 30_000);
        assert_eq!(DEFAULT_CHECKPOINT_POLL_INTERVAL_MS, 1_000);
        assert_eq!(DEFAULT_IDLE_SUSPEND_POLL_INTERVAL_MS, 1_000);
    }
}
