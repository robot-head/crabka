//! Validated scalar values accepted at Schema Registry configuration boundaries.

use std::str::FromStr;

use crabka_units::prelude::*;
use refined_type::rule::GreaterI32;

/// A time extent greater than zero, written in the operator form (`"10s"`,
/// `"500ms"`).
///
/// The unit is mandatory: a bare number would leave the caller guessing whether
/// `30` meant seconds or milliseconds, which is the failure [`Time`] exists to
/// prevent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositiveTime(Time);

impl PositiveTime {
    /// Validate and construct a positive time extent.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not greater than zero.
    pub fn new(value: Time) -> Result<Self, String> {
        if value > Time::ZERO {
            Ok(Self(value))
        } else {
            Err("must be greater than 0".to_string())
        }
    }

    /// Consume the validated extent.
    #[must_use]
    pub const fn into_value(self) -> Time {
        self.0
    }
}

impl FromStr for PositiveTime {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        crabka_units::parse::time(value)
            .map_err(|error| error.to_string())
            .and_then(Self::new)
    }
}

/// A byte count greater than zero, written in the operator form (`"1MiB"`,
/// `"16777216B"`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PositiveSize(ByteSize);

impl PositiveSize {
    /// Validate and construct a positive byte count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not greater than zero.
    pub fn new(value: ByteSize) -> Result<Self, String> {
        if value > ByteSize::ZERO {
            Ok(Self(value))
        } else {
            Err("must be greater than 0".to_string())
        }
    }

    /// Consume the validated count.
    #[must_use]
    pub const fn into_value(self) -> ByteSize {
        self.0
    }
}

impl FromStr for PositiveSize {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        crabka_units::parse::byte_size(value)
            .map_err(|error| error.to_string())
            .and_then(Self::new)
    }
}

/// A 32-bit signed integer greater than zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositiveI32(i32);

impl PositiveI32 {
    /// Validate and construct a positive 32-bit signed integer.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not greater than zero.
    pub fn new(value: i32) -> Result<Self, String> {
        GreaterI32::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

impl FromStr for PositiveI32 {
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
    use std::str::FromStr;

    use assert2::check;
    use crabka_units::prelude::*;

    use super::{PositiveI32, PositiveSize, PositiveTime};

    #[test]
    fn config_value_from_str_checks_refined_boundaries() {
        check!(PositiveTime::from_str("1ms").is_ok());
        check!(PositiveTime::from_str("0s").is_err());
        check!(PositiveSize::from_str("1B").is_ok());
        check!(PositiveSize::from_str("0B").is_err());
        check!(PositiveI32::from_str("1").is_ok());
        check!(PositiveI32::from_str("0").is_err());
    }

    #[test]
    fn config_value_from_str_demands_an_explicit_unit() {
        // A bare number is what the dimension exists to reject: `30` could be
        // seconds or milliseconds, bytes or mebibytes.
        check!(PositiveTime::from_str("30").is_err());
        check!(PositiveSize::from_str("1024").is_err());
    }

    #[test]
    fn config_value_parses_the_operator_form() {
        check!(PositiveTime::from_str("10s").map(PositiveTime::into_value) == Ok(secs(10)));
        check!(PositiveSize::from_str("1MiB").map(PositiveSize::into_value) == Ok(mebibytes(1)));
    }
}
