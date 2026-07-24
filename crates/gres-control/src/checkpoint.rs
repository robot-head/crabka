//! Validated checkpoint runtime scalar values.

use std::str::FromStr;

use refined_type::rule::{GreaterEqualUsize, GreaterUsize};

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
