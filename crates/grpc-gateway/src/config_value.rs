//! Validated scalar values accepted at gateway configuration boundaries.
//!
//! Two families live here. The `refined_integer!` newtypes wrap the
//! *dimensionless counts* a gateway configures — a replication factor, a
//! partition count, a retry budget — and reject an out-of-range value at parse
//! time. The dimensioned settings (timeouts, intervals, body limits, ratios)
//! are `crabka-units` quantities instead, so the functions below both parse the
//! operator's unit-carrying string (`"30s"`, `"4MiB"`, `"2.5%"`) for a `clap`
//! `value_parser` and range-check an already-deserialized quantity from a TOML
//! config file.

use std::str::FromStr;

use crabka_units::{parse, prelude::*};
use refined_type::rule::{GreaterI16, GreaterU32, MinMaxU32};

macro_rules! refined_integer {
    (
        $(#[$meta:meta])*
        $name:ident($primitive:ty) => $rule:ty
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name($primitive);

        impl $name {
            /// Validate and construct the value.
            ///
            /// # Errors
            ///
            /// Returns an error when `value` violates the documented range.
            pub fn new(value: $primitive) -> Result<Self, String> {
                <$rule>::new(value)
                    .map(|value| Self(value.into_value()))
                    .map_err(|error| error.to_string())
            }

            /// Consume the validated value.
            #[must_use]
            pub const fn into_value(self) -> $primitive {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value
                    .parse()
                    .map_err(|error: std::num::ParseIntError| error.to_string())
                    .and_then(Self::new)
            }
        }
    };
}

refined_integer!(
    /// A 16-bit signed integer greater than zero.
    PositiveI16(i16) => GreaterI16<0>
);
refined_integer!(
    /// A 32-bit unsigned integer greater than zero.
    PositiveU32(u32) => GreaterU32<0>
);
refined_integer!(
    /// A partition count representable by Kafka's signed 32-bit field.
    PartitionCount(u32) => MinMaxU32<1, 2_147_483_647>
);

// ---------------------------------------------------------------------------
// Dimensioned values
// ---------------------------------------------------------------------------

/// Range-check an already-parsed time extent, naming the offending field.
///
/// # Errors
///
/// Returns a message when `value` is not greater than zero.
pub fn positive_time(field: &str, value: Time) -> Result<Time, String> {
    if value > secs(0) {
        Ok(value)
    } else {
        Err(format!("{field} must be greater than zero"))
    }
}

/// Range-check an already-parsed time extent that may be zero.
///
/// # Errors
///
/// Returns a message when `value` is negative.
pub fn non_negative_time(field: &str, value: Time) -> Result<Time, String> {
    if value >= secs(0) {
        Ok(value)
    } else {
        Err(format!("{field} must not be negative"))
    }
}

/// Range-check an already-parsed byte count, naming the offending field.
///
/// # Errors
///
/// Returns a message when `value` is not greater than zero.
pub fn positive_byte_size(field: &str, value: ByteSize) -> Result<ByteSize, String> {
    if value > bytes(0) {
        Ok(value)
    } else {
        Err(format!("{field} must be greater than zero"))
    }
}

/// Range-check an already-parsed fraction, naming the offending field.
///
/// # Errors
///
/// Returns a message when `value` falls outside the inclusive range `0..=1`.
pub fn unit_ratio(field: &str, value: Ratio) -> Result<Ratio, String> {
    if (fraction(0.0)..=fraction(1.0)).contains(&value) {
        Ok(value)
    } else {
        Err(format!("{field} must be between 0% and 100%"))
    }
}

/// `clap` value parser for a strictly positive time extent (`"30s"`, `"500ms"`).
///
/// # Errors
///
/// Returns a message when the value carries no unit, is not a duration, or is
/// not greater than zero.
pub fn parse_positive_time(input: &str) -> Result<Time, String> {
    positive_time(
        input,
        parse::time(input).map_err(|error| error.to_string())?,
    )
}

/// `clap` value parser for a time extent that may be zero (`"0s"`, `"30s"`).
///
/// # Errors
///
/// Returns a message when the value carries no unit, is not a duration, or is
/// negative.
pub fn parse_non_negative_time(input: &str) -> Result<Time, String> {
    non_negative_time(
        input,
        parse::time(input).map_err(|error| error.to_string())?,
    )
}

/// `clap` value parser for a strictly positive byte count (`"4MiB"`, `"512KiB"`).
///
/// # Errors
///
/// Returns a message when the value carries no unit, is not a size, or is not
/// greater than zero.
pub fn parse_positive_byte_size(input: &str) -> Result<ByteSize, String> {
    positive_byte_size(
        input,
        parse::byte_size(input).map_err(|error| error.to_string())?,
    )
}

/// `clap` value parser for a fraction in `0..=1` (`"2.5%"`, `"0.025"`).
///
/// # Errors
///
/// Returns a message when the value is not a fraction, or falls outside the
/// inclusive range `0..=1`.
pub fn parse_unit_ratio(input: &str) -> Result<Ratio, String> {
    unit_ratio(
        input,
        parse::ratio(input).map_err(|error| error.to_string())?,
    )
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn refined_integer_boundaries() {
        check!(PositiveI16::new(0).is_err());
        check!(PositiveI16::new(1).is_ok());
        check!(PositiveU32::new(0).is_err());
        check!(PositiveU32::new(1).is_ok());
        check!(PartitionCount::new(0).is_err());
        check!(PartitionCount::new(2_147_483_647).is_ok());
        check!(PartitionCount::new(2_147_483_648).is_err());
    }

    #[test]
    fn refined_integer_from_str_is_checked() {
        check!("1".parse::<PositiveU32>().is_ok());
        check!("0".parse::<PositiveU32>().is_err());
        check!("not-an-integer".parse::<PositiveU32>().is_err());
    }

    #[test]
    fn quantity_parsers_accept_unit_carrying_values() {
        check!(parse_positive_time("30s") == Ok(secs(30)));
        check!(parse_positive_time("500ms") == Ok(millis(500)));
        check!(parse_non_negative_time("0s") == Ok(secs(0)));
        check!(parse_positive_byte_size("4MiB") == Ok(mebibytes(4)));
        check!(parse_positive_byte_size("512KiB") == Ok(kibibytes(512)));
        check!(parse_unit_ratio("2.5%") == Ok(fraction(0.025)));
        check!(parse_unit_ratio("0.25") == Ok(percent(25)));
    }

    #[test]
    fn quantity_parsers_reject_bare_numbers() {
        for input in ["30", "4096", "1.5"] {
            check!(parse_positive_time(input).is_err());
            check!(parse_positive_byte_size(input).is_err());
        }
    }

    #[test]
    fn quantity_parsers_enforce_their_range() {
        for (_name, input) in [("zero", "0s"), ("negative", "-1s")] {
            check!(parse_positive_time(input).is_err());
        }
        check!(parse_non_negative_time("-1ms").is_err());
        check!(parse_positive_byte_size("0B").is_err());
        for (_name, input) in [("above one", "101%"), ("negative", "-1%")] {
            check!(parse_unit_ratio(input).is_err());
        }
    }

    #[test]
    fn range_check_messages_name_the_field() {
        assert!(
            positive_time("poll_timeout", secs(0))
                .unwrap_err()
                .contains("poll_timeout")
        );
        assert!(
            non_negative_time("clock_skew", Time::from_millis(-1))
                .unwrap_err()
                .contains("clock_skew")
        );
        assert!(
            positive_byte_size("max_body", bytes(0))
                .unwrap_err()
                .contains("max_body")
        );
        assert!(
            unit_ratio("dirty_ratio", percent(200))
                .unwrap_err()
                .contains("dirty_ratio")
        );
    }
}
