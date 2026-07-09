use std::{fmt, num::ParseIntError, str::FromStr};

use thiserror::Error;

/// `PostgreSQL` WAL page size in bytes.
pub const XLOG_BLCKSZ: u64 = 8 * 1024;

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

    /// Returns the WAL segment number for `wal_segment_size` bytes per segment.
    ///
    /// # Panics
    ///
    /// Panics when `wal_segment_size` is zero.
    #[must_use]
    pub const fn segment_number(self, wal_segment_size: u64) -> u64 {
        assert!(wal_segment_size > 0, "WAL segment size must be non-zero");
        self.0 / wal_segment_size
    }

    /// Returns this LSN's byte offset within its WAL segment.
    ///
    /// # Panics
    ///
    /// Panics when `wal_segment_size` is zero.
    #[must_use]
    pub const fn segment_offset(self, wal_segment_size: u64) -> u64 {
        assert!(wal_segment_size > 0, "WAL segment size must be non-zero");
        self.0 % wal_segment_size
    }

    /// Returns the LSN of the first byte in this LSN's WAL page.
    #[must_use]
    pub const fn page_start(self) -> Self {
        Self(self.0 - self.page_offset())
    }

    /// Returns this LSN's byte offset within its 8 KiB WAL page.
    #[must_use]
    pub const fn page_offset(self) -> u64 {
        self.0 % XLOG_BLCKSZ
    }

    /// Returns whether this LSN points at the start of a WAL page.
    #[must_use]
    pub const fn is_page_aligned(self) -> bool {
        self.page_offset() == 0
    }

    /// Returns this LSN if page-aligned, otherwise the first LSN of the next page.
    ///
    /// # Panics
    ///
    /// Panics if rounding up would overflow `u64`.
    #[must_use]
    pub const fn next_page_start(self) -> Self {
        if self.is_page_aligned() {
            return self;
        }

        let bytes_to_next_page = XLOG_BLCKSZ - self.page_offset();
        let Some(next_page_start) = self.0.checked_add(bytes_to_next_page) else {
            panic!("rounding LSN up to next WAL page must not overflow");
        };
        Self(next_page_start)
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
    EmptyHalf { half: LsnHalf },

    /// One side of the `X/Y` form did not fit in `PostgreSQL`'s 32-bit half.
    #[error("LSN {half} half must fit in 8 hexadecimal digits")]
    HalfTooWide { half: LsnHalf },

    /// One side of the `X/Y` form contained non-hexadecimal digits.
    #[error("LSN {half} half contains invalid hexadecimal digits")]
    InvalidHex {
        half: LsnHalf,
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

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn lsn_display_is_postgres_hex_form() {
        assert!(Lsn(0x16_B374_D848).to_string() == "16/B374D848");
    }

    #[test]
    fn lsn_parse_accepts_postgres_hex_form() {
        assert!("16/B374D848".parse::<Lsn>() == Ok(Lsn(0x16_B374_D848)));
        assert!("0/0".parse::<Lsn>() == Ok(Lsn(0)));
        assert!("FFFFFFFF/FFFFFFFF".parse::<Lsn>() == Ok(Lsn(u64::MAX)));
        assert!("00000016/0000000a".parse::<Lsn>() == Ok(Lsn(0x16_0000_000A)));
    }

    #[test]
    fn lsn_parse_rejects_invalid_forms() {
        assert!("16B374D848".parse::<Lsn>() == Err(ParseLsnError::MissingSeparator));
        assert!("16/B374/D848".parse::<Lsn>() == Err(ParseLsnError::TooManySeparators));
        assert!(
            "/B374D848".parse::<Lsn>()
                == Err(ParseLsnError::EmptyHalf {
                    half: LsnHalf::High
                })
        );
        assert!("16/".parse::<Lsn>() == Err(ParseLsnError::EmptyHalf { half: LsnHalf::Low }));
        assert!(
            "100000000/0".parse::<Lsn>()
                == Err(ParseLsnError::HalfTooWide {
                    half: LsnHalf::High
                })
        );
        assert!(
            "0/100000000".parse::<Lsn>() == Err(ParseLsnError::HalfTooWide { half: LsnHalf::Low })
        );
        assert!(let Err(ParseLsnError::InvalidHex {
            half: LsnHalf::Low,
            source: _
        }) = "16/nope".parse::<Lsn>());
    }

    #[test]
    fn segment_and_page_arithmetic() {
        let wal_segment_size = 1024 * 1024;
        let lsn = Lsn(wal_segment_size + XLOG_BLCKSZ + 24);

        assert!(lsn.segment_number(wal_segment_size) == 1);
        assert!(lsn.segment_offset(wal_segment_size) == XLOG_BLCKSZ + 24);
        assert!(lsn.page_offset() == 24);
        assert!(lsn.page_start() == Lsn(wal_segment_size + XLOG_BLCKSZ));
    }

    #[test]
    fn page_alignment_helpers_round_to_the_next_page_start() {
        let page_start = Lsn(2 * XLOG_BLCKSZ);
        let inside_page = Lsn(2 * XLOG_BLCKSZ + 1);

        assert!(page_start.is_page_aligned());
        assert!(!inside_page.is_page_aligned());
        assert!(page_start.next_page_start() == page_start);
        assert!(inside_page.next_page_start() == Lsn(3 * XLOG_BLCKSZ));
    }
}
