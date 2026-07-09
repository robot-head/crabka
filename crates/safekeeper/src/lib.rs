//! Sans-IO `PostgreSQL` safekeeper protocol primitives.

use std::{fmt, num::ParseIntError, str::FromStr};

use thiserror::Error;

pub mod conn;
pub mod frame;
pub mod ingest;
pub mod protocol;
pub mod topic;

const LSN_HALF_BITS: u32 = 32;
const LSN_HALF_MAX_HEX_DIGITS: usize = 8;

/// A `PostgreSQL` log sequence number.
///
/// `PostgreSQL` displays LSNs as two hexadecimal `u32` halves separated by `/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Lsn(pub u64);

impl Lsn {
    /// Returns the raw 64-bit LSN value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let high = self.0 >> LSN_HALF_BITS;
        let low = self.0 & u64::from(u32::MAX);
        write!(formatter, "{high:X}/{low:X}")
    }
}

impl FromStr for Lsn {
    type Err = ParseLsnError;

    fn from_str(raw_lsn: &str) -> Result<Self, Self::Err> {
        let (high, low) = raw_lsn
            .split_once('/')
            .ok_or(ParseLsnError::MissingSeparator)?;

        if low.contains('/') {
            return Err(ParseLsnError::TooManySeparators);
        }

        let high = parse_lsn_half(high, LsnHalf::High)?;
        let low = parse_lsn_half(low, LsnHalf::Low)?;

        Ok(Self((u64::from(high) << LSN_HALF_BITS) | u64::from(low)))
    }
}

/// Error returned when parsing a `PostgreSQL` LSN string fails.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseLsnError {
    /// The input did not contain the `/` separator between the high and low half.
    #[error("LSN must contain a '/' separator")]
    MissingSeparator,

    /// The input contained more than one `/` separator.
    #[error("LSN must contain exactly one '/' separator")]
    TooManySeparators,

    /// One side of the `X/Y` form was empty.
    #[error("LSN {half} half must not be empty")]
    EmptyHalf {
        /// Empty half.
        half: LsnHalf,
    },

    /// One side of the `X/Y` form did not fit in `PostgreSQL`'s 32-bit half.
    #[error("LSN {half} half must fit in 8 hexadecimal digits")]
    HalfTooWide {
        /// Over-wide half.
        half: LsnHalf,
    },

    /// One side of the `X/Y` form contained non-hexadecimal digits.
    #[error("LSN {half} half contains invalid hexadecimal digits")]
    InvalidHex {
        /// Invalid half.
        half: LsnHalf,
        /// Parse failure source.
        #[source]
        source: ParseIntError,
    },
}

/// Identifies one side of `PostgreSQL`'s `X/Y` LSN text form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LsnHalf {
    /// The high 32 bits before `/`.
    High,
    /// The low 32 bits after `/`.
    Low,
}

impl fmt::Display for LsnHalf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::High => formatter.write_str("high"),
            Self::Low => formatter.write_str("low"),
        }
    }
}

fn parse_lsn_half(raw_half: &str, half: LsnHalf) -> Result<u32, ParseLsnError> {
    if raw_half.is_empty() {
        return Err(ParseLsnError::EmptyHalf { half });
    }

    if raw_half.len() > LSN_HALF_MAX_HEX_DIGITS {
        return Err(ParseLsnError::HalfTooWide { half });
    }

    u32::from_str_radix(raw_half, 16).map_err(|source| ParseLsnError::InvalidHex { half, source })
}
