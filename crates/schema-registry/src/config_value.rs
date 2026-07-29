//! Validated dimensionless values accepted at Schema Registry boundaries.

use std::str::FromStr;

use refined_type::rule::GreaterI32;

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

    use super::PositiveI32;

    #[test]
    fn config_value_from_str_checks_refined_boundaries() {
        check!(PositiveI32::from_str("1").is_ok());
        check!(PositiveI32::from_str("0").is_err());
    }
}
