//! Rendering a quantity in the form an operator wrote it.
//!
//! `uom`'s own formatting needs the unit named at the call site
//! (`size.into_format_args(mebibyte, Abbreviation)`), which is the wrong shape for
//! a log line or a config dump where the magnitude decides the unit. [`Human`]
//! picks the unit for you: `536870912` bytes prints as `512MiB`, `604800` seconds
//! as `7d`, `1536` bytes as `1.5KiB`.
//!
//! # Exactness
//!
//! Sizes, time extents, and byte rates round-trip: whatever these types print,
//! [`crate::parse`] reads back as the same value. That is what picks the unit —
//! the largest one that leaves the magnitude at or above one *and* representable
//! in at most three decimal places, so `1TiB + 1B` prints as `1099511627777B`
//! rather than rounding to `1TiB`. A quantity that is not a whole number of base
//! units, or is too large for that arithmetic, prints its base-unit value, which
//! reads back exactly as well.
//!
//! Fractions and event rates render to nine decimal places and are for display
//! only. A non-finite quantity prints its raw value and base unit, for diagnostics.

use core::fmt::{self, Display, Formatter};

use num_traits::cast::ToPrimitive;

use crate::{ByteRate, ByteSize, Frequency, Ratio, Time};

/// Descending size units, in bytes.
const SIZES: &[(i128, &str)] = &[
    (1_099_511_627_776, "TiB"),
    (1_073_741_824, "GiB"),
    (1_048_576, "MiB"),
    (1024, "KiB"),
    (1, "B"),
];

/// Descending time units, in nanoseconds.
const TIMES: &[(i128, &str)] = &[
    (86_400_000_000_000, "d"),
    (3_600_000_000_000, "h"),
    (60_000_000_000, "m"),
    (1_000_000_000, "s"),
    (1_000_000, "ms"),
    (1_000, "µs"),
    (1, "ns"),
];

/// Nanoseconds per second: the sub-unit [`TIMES`] counts in.
const NANOS_PER_SECOND: f64 = 1e9;

/// One thousandth, the resolution a rendered magnitude may use.
const MILLI: i128 = 1_000;

/// How far a scaled magnitude may sit from a whole sub-unit and still be treated
/// as that sub-unit count.
///
/// The base unit is `f64`, and 250 nanoseconds has no exact representation in
/// seconds; scaling it back up lands a few parts in 10^16 away from `250`. A
/// tolerance a few orders of magnitude above that accepts every quantity built
/// from whole sub-units while still rejecting a genuinely fractional one — half a
/// byte is nowhere near this close to zero or one.
const INTEGRAL_TOLERANCE: f64 = 1e-9;

/// Writes `value` — a magnitude in `base` units — in the largest unit of `units`
/// it fits exactly.
///
/// `subunits_per_base` scales `value` into the integer sub-unit that `units` is
/// tabulated in (bytes for sizes, nanoseconds for extents).
fn write_scaled(
    f: &mut Formatter<'_>,
    value: f64,
    subunits_per_base: f64,
    units: &[(i128, &str)],
    base: &str,
    suffix: &str,
) -> fmt::Result {
    if value.is_finite() {
        let subunits = value * subunits_per_base;
        let rounded = subunits.round();
        let drift = (subunits - rounded).abs();
        // The thousandths multiply inside `write_units` has to fit: `parse` will
        // accept a magnitude near `i128::MAX / 1000` (`10^36B`), which has no
        // thousandths representation. Such a value falls through to the raw
        // base-unit rendering below rather than overflowing.
        if drift <= subunits.abs().max(1.0) * INTEGRAL_TOLERANCE
            && let Some(count) = rounded.to_i128()
            && count.checked_mul(MILLI).is_some()
        {
            return write_units(f, count, units, base, suffix);
        }
    }
    write!(f, "{value}{base}{suffix}")
}

/// Writes an exact sub-unit `count` in the largest unit that divides it to at
/// most three decimal places.
fn write_units(
    f: &mut Formatter<'_>,
    count: i128,
    units: &[(i128, &str)],
    base: &str,
    suffix: &str,
) -> fmt::Result {
    if count == 0 {
        return write!(f, "0{base}{suffix}");
    }
    let scaled = count * MILLI;
    let picked = units
        .iter()
        .copied()
        .find(|&(scale, _)| count.abs() >= scale && scaled % scale == 0);
    match picked {
        Some((scale, unit)) => {
            write_thousandths(f, scaled / scale)?;
            write!(f, "{unit}{suffix}")
        }
        None => write_thousandths(f, scaled).and_then(|()| write!(f, "{base}{suffix}")),
    }
}

/// Writes `thousandths` / 1000, dropping a zero fraction and trailing zeros.
fn write_thousandths(f: &mut Formatter<'_>, thousandths: i128) -> fmt::Result {
    if thousandths < 0 {
        f.write_str("-")?;
    }
    let magnitude = thousandths.unsigned_abs();
    write!(f, "{}", magnitude / 1_000)?;
    let fraction = magnitude % 1_000;
    if fraction != 0 {
        let rendered = format!("{fraction:03}");
        write!(f, ".{}", rendered.trim_end_matches('0'))?;
    }
    Ok(())
}

/// Writes `value` with at most nine decimal places and no trailing zeros.
fn write_trimmed(f: &mut Formatter<'_>, value: f64) -> fmt::Result {
    let rendered = format!("{value:.9}");
    let trimmed = rendered.trim_end_matches('0').trim_end_matches('.');
    f.write_str(trimmed)
}

/// A quantity that renders itself the way an operator would write it.
///
/// ```
/// use crabka_units::{fmt::Human, prelude::*};
///
/// assert_eq!(mebibytes(512).human().to_string(), "512MiB");
/// assert_eq!(days(7).human().to_string(), "7d");
/// assert_eq!(mebibytes_per_sec(10).human().to_string(), "10MiB/s");
/// ```
pub trait Human: Sized {
    /// The [`Display`] adapter this quantity renders through.
    type Rendered: Display;

    /// This quantity, ready to print.
    fn human(self) -> Self::Rendered;
}

/// [`ByteSize`] rendered in binary size units.
#[derive(Debug, Clone, Copy)]
pub struct HumanByteSize(ByteSize);

impl Display for HumanByteSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_scaled(f, self.0.value, 1.0, SIZES, "B", "")
    }
}

impl Human for ByteSize {
    type Rendered = HumanByteSize;

    fn human(self) -> Self::Rendered {
        HumanByteSize(self)
    }
}

/// [`Time`] rendered in time units, from days down to nanoseconds.
#[derive(Debug, Clone, Copy)]
pub struct HumanTime(Time);

impl Display for HumanTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_scaled(f, self.0.value, NANOS_PER_SECOND, TIMES, "s", "")
    }
}

impl Human for Time {
    type Rendered = HumanTime;

    fn human(self) -> Self::Rendered {
        HumanTime(self)
    }
}

/// [`ByteRate`] rendered as a binary size over a second.
#[derive(Debug, Clone, Copy)]
pub struct HumanByteRate(ByteRate);

impl Display for HumanByteRate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write_scaled(f, self.0.value, 1.0, SIZES, "B", "/s")
    }
}

impl Human for ByteRate {
    type Rendered = HumanByteRate;

    fn human(self) -> Self::Rendered {
        HumanByteRate(self)
    }
}

/// [`Frequency`] rendered as events per second.
#[derive(Debug, Clone, Copy)]
pub struct HumanFrequency(Frequency);

impl Display for HumanFrequency {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !self.0.value.is_finite() {
            return write!(f, "{}/s", self.0.value);
        }
        write_trimmed(f, self.0.value)?;
        f.write_str("/s")
    }
}

impl Human for Frequency {
    type Rendered = HumanFrequency;

    fn human(self) -> Self::Rendered {
        HumanFrequency(self)
    }
}

/// [`Ratio`] rendered as a percentage.
#[derive(Debug, Clone, Copy)]
pub struct HumanRatio(Ratio);

impl Display for HumanRatio {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !self.0.value.is_finite() {
            return write!(f, "{}%", self.0.value);
        }
        write_trimmed(f, self.0.value * 100.0)?;
        f.write_str("%")
    }
}

impl Human for Ratio {
    type Rendered = HumanRatio;

    fn human(self) -> Self::Rendered {
        HumanRatio(self)
    }
}
