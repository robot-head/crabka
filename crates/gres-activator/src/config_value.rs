//! Validated values accepted at activator configuration boundaries.

use std::str::FromStr;

use refined_type::rule::{GreaterU64, NonEmptyString};

/// A millisecond count greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveMillis(u64);

impl PositiveMillis {
    /// Validate and construct a positive millisecond count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> Result<Self, String> {
        GreaterU64::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
    }
}

impl FromStr for PositiveMillis {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}

/// A non-empty owned string.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NonEmptyValue(String);

impl NonEmptyValue {
    /// Validate and construct a non-empty value.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is empty.
    pub fn new(value: String) -> Result<Self, String> {
        NonEmptyString::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Consume the validated value.
    #[must_use]
    pub fn into_value(self) -> String {
        self.0
    }
}

impl FromStr for NonEmptyValue {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value.to_owned())
    }
}
