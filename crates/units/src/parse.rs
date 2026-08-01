//! Parsing the human forms of a quantity: `512MiB`, `30s`, `10MiB/s`, `25%`.
//!
//! Operators write sizes and durations with units, and a config file that spells
//! them out is one that cannot be misread. Each parser takes a magnitude followed
//! by a unit — optionally separated by spaces — and is case-insensitive, so
//! `512MiB`, `512 mib`, and `512MIB` are the same value.
//!
//! A unit is required. `30` is not a duration: whether it means 30 seconds or 30
//! milliseconds is exactly the question the type is meant to settle. The single
//! exception is `0`, which is unambiguous in any unit.
//!
//! Binary and decimal size prefixes are distinct, as in the JEDEC/IEC split:
//! `KiB`/`MiB`/`GiB`/`TiB` step by 1024, `kB`/`MB`/`GB`/`TB` by 1000.
//!
//! Exponent notation (`1e6`) is not accepted; a unit expresses the scale.

use crate::{
    ByteRate, ByteSize, Frequency, Ratio, Time,
    convert::{ByteRateExt, ByteSizeExt, FrequencyExt, RatioExt, TimeExt},
    fraction,
};

/// Why a quantity string could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The input was empty or all whitespace.
    #[error("empty quantity")]
    Empty,
    /// The magnitude was there but the unit was missing.
    #[error(
        "missing unit in `{input}`: write the unit explicitly, as in `512MiB`, `30s`, `10MiB/s`"
    )]
    MissingUnit {
        /// The offending input.
        input: String,
    },
    /// The magnitude was not a decimal number.
    #[error("invalid number `{number}` in `{input}`")]
    InvalidNumber {
        /// The offending input.
        input: String,
        /// The part that should have been a number.
        number: String,
    },
    /// The unit is not one this dimension knows.
    #[error("unknown {dimension} unit `{unit}` in `{input}`")]
    UnknownUnit {
        /// Which dimension was being parsed.
        dimension: &'static str,
        /// The unrecognised unit.
        unit: String,
        /// The offending input.
        input: String,
    },
    /// The magnitude parsed but is infinite or `NaN`.
    #[error("`{input}` is not a finite quantity")]
    NotFinite {
        /// The offending input.
        input: String,
    },
    /// The quantity is outside the accepted sign range.
    #[error("`{input}` must be {requirement}")]
    OutOfRange {
        /// The offending input.
        input: String,
        /// The accepted sign range.
        requirement: &'static str,
    },
}

/// The magnitude and the lowercased unit of `input`.
///
/// A unitless `0` yields an empty unit, which every dimension reads as zero.
fn split(input: &str) -> Result<(f64, String), ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }

    let split_at = trimmed
        .char_indices()
        .position(|(index, ch)| {
            !(ch.is_ascii_digit() || ch == '.' || (index == 0 && (ch == '-' || ch == '+')))
        })
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    let number = number.trim();
    let unit = unit.trim();

    let magnitude: f64 = number.parse().map_err(|_| ParseError::InvalidNumber {
        input: input.to_owned(),
        number: number.to_owned(),
    })?;
    if !magnitude.is_finite() {
        return Err(ParseError::NotFinite {
            input: input.to_owned(),
        });
    }
    if unit.is_empty() && magnitude != 0.0 {
        return Err(ParseError::MissingUnit {
            input: input.to_owned(),
        });
    }

    Ok((magnitude, unit.to_lowercase()))
}

fn finite(input: &str, value: f64) -> Result<f64, ParseError> {
    value
        .is_finite()
        .then_some(value)
        .ok_or_else(|| ParseError::NotFinite {
            input: input.to_owned(),
        })
}

/// The byte scale named by a lowercased size unit.
fn size_scale(unit: &str) -> Option<f64> {
    match unit {
        "" | "b" | "byte" | "bytes" => Some(1.0),
        "kib" | "kibibyte" | "kibibytes" => Some(1024.0),
        "mib" | "mebibyte" | "mebibytes" => Some(1_048_576.0),
        "gib" | "gibibyte" | "gibibytes" => Some(1_073_741_824.0),
        "tib" | "tebibyte" | "tebibytes" => Some(1_099_511_627_776.0),
        "kb" | "kilobyte" | "kilobytes" => Some(1_000.0),
        "mb" | "megabyte" | "megabytes" => Some(1_000_000.0),
        "gb" | "gigabyte" | "gigabytes" => Some(1_000_000_000.0),
        "tb" | "terabyte" | "terabytes" => Some(1_000_000_000_000.0),
        _ => None,
    }
}

/// The seconds-per-unit named by a lowercased time unit, as a numerator over a
/// denominator.
///
/// Sub-second units are given as a division rather than a fractional multiplier
/// because `1e-3` is not exactly a thousandth in binary, while `x / 1e3` is
/// correctly rounded. The difference shows up when the result is rendered back:
/// `250 * 1e-9` is not the nearest `f64` to 250 nanoseconds, and prints as
/// `0.00000025000000000000004s`.
fn time_scale(unit: &str) -> Option<(f64, f64)> {
    match unit {
        "ns" | "nanosecond" | "nanoseconds" => Some((1.0, 1e9)),
        "us" | "µs" | "μs" | "microsecond" | "microseconds" => Some((1.0, 1e6)),
        "ms" | "millisecond" | "milliseconds" => Some((1.0, 1e3)),
        "" | "s" | "sec" | "secs" | "second" | "seconds" => Some((1.0, 1.0)),
        "m" | "min" | "mins" | "minute" | "minutes" => Some((60.0, 1.0)),
        "h" | "hr" | "hrs" | "hour" | "hours" => Some((3600.0, 1.0)),
        "d" | "day" | "days" => Some((86_400.0, 1.0)),
        _ => None,
    }
}

/// A byte count: `1024`&nbsp;→ error, `1024B`, `512MiB`, `1.5 GiB`, `2MB`.
///
/// # Errors
///
/// [`ParseError`] if the magnitude is not a finite decimal number, the unit is
/// missing on a non-zero magnitude, or the unit is not a size.
pub fn byte_size(input: &str) -> Result<ByteSize, ParseError> {
    let (magnitude, unit) = split(input)?;
    let scale = size_scale(&unit).ok_or_else(|| ParseError::UnknownUnit {
        dimension: "size",
        unit,
        input: input.to_owned(),
    })?;
    Ok(ByteSize::from_bytes_f64(finite(input, magnitude * scale)?))
}

/// A time extent: `30s`, `500ms`, `7d`, `1.5h`, `250 us`.
///
/// # Errors
///
/// [`ParseError`] if the magnitude is not a finite decimal number, the unit is
/// missing on a non-zero magnitude, or the unit is not a duration.
pub fn time(input: &str) -> Result<Time, ParseError> {
    let (magnitude, unit) = split(input)?;
    let (numerator, denominator) = time_scale(&unit).ok_or_else(|| ParseError::UnknownUnit {
        dimension: "time",
        unit,
        input: input.to_owned(),
    })?;
    Ok(Time::from_secs_f64(finite(
        input,
        magnitude * numerator / denominator,
    )?))
}

/// A byte throughput: `10MiB/s`, `1048576B/s`, `64KiBps`, `5 MB / sec`.
///
/// The rate is a size unit over a time unit; `/s` may also be written as the
/// suffix `ps`, and the time unit may be any duration (`1GiB/h`).
///
/// # Errors
///
/// [`ParseError`] if the magnitude is not a finite decimal number, the unit is
/// missing on a non-zero magnitude, or either half of the compound unit is not a
/// size and a duration respectively.
pub fn byte_rate(input: &str) -> Result<ByteRate, ParseError> {
    let (magnitude, unit) = split(input)?;
    let unknown = || ParseError::UnknownUnit {
        dimension: "rate",
        unit: unit.clone(),
        input: input.to_owned(),
    };

    let (size_unit, time_unit) = if let Some((size, time)) = unit.split_once('/') {
        (size.trim(), time.trim())
    } else if let Some(size) = unit.strip_suffix("ps") {
        (size.trim(), "s")
    } else if unit.is_empty() {
        ("", "s")
    } else {
        return Err(unknown());
    };

    let size = size_scale(size_unit).ok_or_else(unknown)?;
    let (numerator, denominator) = time_scale(time_unit).ok_or_else(unknown)?;
    Ok(ByteRate::from_bytes_per_sec_f64(finite(
        input,
        magnitude * size * denominator / numerator,
    )?))
}

/// An event rate: `100/s`, `2.5Hz`, `60/min`.
///
/// # Errors
///
/// [`ParseError`] if the magnitude is not a finite decimal number, the unit is
/// missing on a non-zero magnitude, or the unit is neither hertz nor a
/// per-duration.
pub fn frequency(input: &str) -> Result<Frequency, ParseError> {
    let (magnitude, unit) = split(input)?;
    let unknown = || ParseError::UnknownUnit {
        dimension: "frequency",
        unit: unit.clone(),
        input: input.to_owned(),
    };

    let (numerator, denominator) = match unit.as_str() {
        "" | "hz" | "hertz" => (1.0, 1.0),
        _ => {
            let time_unit = unit.strip_prefix('/').ok_or_else(unknown)?.trim();
            time_scale(time_unit).ok_or_else(unknown)?
        }
    };
    Ok(Frequency::from_per_sec(finite(
        input,
        magnitude * denominator / numerator,
    )?))
}

/// A dimensionless fraction: `25%`, `0.25`, `1`.
///
/// Unlike the other dimensions a bare number is accepted, because a fraction's
/// unit *is* "none"; `%` scales by a hundredth.
///
/// # Errors
///
/// [`ParseError`] if the magnitude is not a finite decimal number, or the unit is
/// anything other than `%`.
pub fn ratio(input: &str) -> Result<Ratio, ParseError> {
    let trimmed = input.trim();
    let (body, scale) = match trimmed.strip_suffix('%') {
        Some(body) => (body.trim(), 0.01),
        None => (trimmed, 1.0),
    };
    if body.is_empty() {
        return Err(ParseError::Empty);
    }
    let magnitude: f64 = body.parse().map_err(|_| ParseError::InvalidNumber {
        input: input.to_owned(),
        number: body.to_owned(),
    })?;
    if !magnitude.is_finite() {
        return Err(ParseError::NotFinite {
            input: input.to_owned(),
        });
    }
    Ok(fraction(finite(input, magnitude * scale)?))
}

fn require_sign<T>(
    input: &str,
    value: T,
    magnitude: f64,
    allow_zero: bool,
) -> Result<T, ParseError> {
    if !magnitude.is_finite() {
        return Err(ParseError::NotFinite {
            input: input.to_owned(),
        });
    }
    if magnitude > 0.0 || (allow_zero && magnitude == 0.0) {
        Ok(value)
    } else {
        Err(ParseError::OutOfRange {
            input: input.to_owned(),
            requirement: if allow_zero {
                "non-negative"
            } else {
                "greater than zero"
            },
        })
    }
}

/// A strictly positive time extent.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, positive time.
pub fn positive_time(input: &str) -> Result<Time, ParseError> {
    let value = time(input)?;
    require_sign(input, value, value.secs_f64(), false)
}

/// A non-negative time extent.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, non-negative time.
pub fn non_negative_time(input: &str) -> Result<Time, ParseError> {
    let value = time(input)?;
    require_sign(input, value, value.secs_f64(), true)
}

/// A strictly positive byte count.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, positive byte count.
pub fn positive_byte_size(input: &str) -> Result<ByteSize, ParseError> {
    let value = byte_size(input)?;
    require_sign(input, value, value.bytes_f64(), false)
}

/// A non-negative byte count.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, non-negative byte count.
pub fn non_negative_byte_size(input: &str) -> Result<ByteSize, ParseError> {
    let value = byte_size(input)?;
    require_sign(input, value, value.bytes_f64(), true)
}

/// A strictly positive byte throughput.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, positive byte throughput.
pub fn positive_byte_rate(input: &str) -> Result<ByteRate, ParseError> {
    let value = byte_rate(input)?;
    require_sign(input, value, value.bytes_per_sec_f64(), false)
}

/// A non-negative byte throughput.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, non-negative byte throughput.
pub fn non_negative_byte_rate(input: &str) -> Result<ByteRate, ParseError> {
    let value = byte_rate(input)?;
    require_sign(input, value, value.bytes_per_sec_f64(), true)
}

/// A strictly positive dimensionless ratio.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, positive ratio.
pub fn positive_ratio(input: &str) -> Result<Ratio, ParseError> {
    let value = ratio(input)?;
    require_sign(input, value, value.as_f64(), false)
}

/// A non-negative dimensionless ratio.
///
/// # Errors
///
/// [`ParseError`] if the value is not a finite, non-negative ratio.
pub fn non_negative_ratio(input: &str) -> Result<Ratio, ParseError> {
    let value = ratio(input)?;
    require_sign(input, value, value.as_f64(), true)
}

/// A dimensionless ratio in the inclusive range from zero through one.
///
/// # Errors
///
/// [`ParseError`] if the value is not finite or lies outside `0..=1`.
pub fn unit_ratio(input: &str) -> Result<Ratio, ParseError> {
    let value = non_negative_ratio(input)?;
    if value <= Ratio::ONE {
        Ok(value)
    } else {
        Err(ParseError::OutOfRange {
            input: input.to_owned(),
            requirement: "between 0% and 100%",
        })
    }
}
