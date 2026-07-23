//! Validated scalar values accepted at broker configuration boundaries.

use refined_type::rule::{GreaterI32, GreaterI64, GreaterU64, GreaterUsize, MinMaxU32};

type RefinedPositiveMillis = GreaterU64<0>;
type RefinedPositiveI32 = GreaterI32<0>;
type RefinedPositiveI64 = GreaterI64<0>;
type RefinedPositiveCount = GreaterUsize<0>;
type RefinedPercentage = MinMaxU32<0, 100>;

/// A millisecond count greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveMillis(u64);

impl PositiveMillis {
    /// Validate and construct a positive millisecond count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: u64) -> Result<Self, refined_type::result::Error<u64>> {
        RefinedPositiveMillis::new(value).map(|value| Self(value.into_value()))
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> u64 {
        self.0
    }
}

/// A 32-bit signed integer greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveI32(i32);

impl PositiveI32 {
    /// Validate and construct a positive 32-bit signed integer.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not greater than zero.
    pub fn new(value: i32) -> Result<Self, refined_type::result::Error<i32>> {
        RefinedPositiveI32::new(value).map(|value| Self(value.into_value()))
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> i32 {
        self.0
    }
}

/// A 64-bit signed integer greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveI64(i64);

impl PositiveI64 {
    /// Validate and construct a positive 64-bit signed integer.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is not greater than zero.
    pub fn new(value: i64) -> Result<Self, refined_type::result::Error<i64>> {
        RefinedPositiveI64::new(value).map(|value| Self(value.into_value()))
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> i64 {
        self.0
    }
}

/// A platform-sized count greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PositiveCount(usize);

impl PositiveCount {
    /// Validate and construct a positive count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> Result<Self, refined_type::result::Error<usize>> {
        RefinedPositiveCount::new(value).map(|value| Self(value.into_value()))
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

/// An inclusive percentage from zero through one hundred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Percentage(u32);

impl Percentage {
    /// Validate and construct an inclusive percentage.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is greater than one hundred.
    pub fn new(value: u32) -> Result<Self, refined_type::result::Error<u32>> {
        RefinedPercentage::new(value).map(|value| Self(value.into_value()))
    }

    /// Consume the validated value.
    #[must_use]
    pub const fn into_value(self) -> u32 {
        self.0
    }
}

/// Parse a positive millisecond count.
///
/// # Errors
///
/// Returns an error when `value` is not an integer greater than zero.
pub fn parse_positive_millis(value: &str) -> Result<PositiveMillis, String> {
    value
        .parse::<u64>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveMillis::new(value).map_err(|error| error.to_string()))
}

/// Parse a positive 32-bit signed integer.
///
/// # Errors
///
/// Returns an error when `value` is not an integer greater than zero.
pub fn parse_positive_i32(value: &str) -> Result<PositiveI32, String> {
    value
        .parse::<i32>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveI32::new(value).map_err(|error| error.to_string()))
}

/// Parse a positive 64-bit signed integer.
///
/// # Errors
///
/// Returns an error when `value` is not an integer greater than zero.
pub fn parse_positive_i64(value: &str) -> Result<PositiveI64, String> {
    value
        .parse::<i64>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveI64::new(value).map_err(|error| error.to_string()))
}

/// Parse a positive platform-sized count.
///
/// # Errors
///
/// Returns an error when `value` is not an integer greater than zero.
pub fn parse_positive_count(value: &str) -> Result<PositiveCount, String> {
    value
        .parse::<usize>()
        .map_err(|error| error.to_string())
        .and_then(|value| PositiveCount::new(value).map_err(|error| error.to_string()))
}

/// Parse an inclusive percentage from zero through one hundred.
///
/// # Errors
///
/// Returns an error when `value` is not an integer in `0..=100`.
pub fn parse_percentage(value: &str) -> Result<Percentage, String> {
    value
        .parse::<u32>()
        .map_err(|error| error.to_string())
        .and_then(|value| Percentage::new(value).map_err(|error| error.to_string()))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct TestArgs {
        #[arg(long, value_parser = parse_positive_millis)]
        delay_ms: PositiveMillis,
    }

    #[test]
    fn refined_scalar_boundaries() {
        assert!(parse_positive_millis("1").is_ok());
        assert!(parse_positive_millis("0").is_err());
        assert!(parse_positive_i32("1").is_ok());
        assert!(parse_positive_i32("0").is_err());
        assert!(parse_positive_i64("1").is_ok());
        assert!(parse_positive_i64("-1").is_err());
        assert!(parse_positive_count("1").is_ok());
        assert!(parse_positive_count("0").is_err());
        assert!(parse_percentage("0").is_ok());
        assert!(parse_percentage("100").is_ok());
        assert!(parse_percentage("101").is_err());
    }

    #[test]
    fn clap_value_parser_uses_refined_validation() {
        let args = TestArgs::try_parse_from(["test", "--delay-ms", "1"])
            .expect("positive milliseconds should parse");
        assert!(args.delay_ms.into_value() == 1);

        assert!(TestArgs::try_parse_from(["test", "--delay-ms", "0"]).is_err());
    }
}
