//! Dimensioned quantities for Crabka.
//!
//! A broker is full of numbers that mean nothing on their own. `max_bytes`,
//! `session_timeout_ms`, and `throttle_bytes_per_sec` are all integers, so a
//! function taking two of them accepts them transposed and still compiles, and a
//! value that crosses a module boundary in the wrong unit — seconds where
//! milliseconds were expected, bits where bytes were expected — is a runtime bug
//! rather than a compile error. Encoding the *unit in the name* only documents
//! the intent; it does not enforce it.
//!
//! This crate gives those magnitudes real types, built on
//! [`uom`](https://docs.rs/uom): [`ByteSize`], [`ByteRate`], [`Time`],
//! [`Frequency`], and [`Ratio`]. Each is a `uom` quantity, so unit conversion is
//! a method call rather than a hand-written multiplication, and arithmetic across
//! dimensions is checked:
//!
//! ```
//! use crabka_units::prelude::*;
//!
//! let batch = mebibytes(8);
//! let window = secs(2);
//! let rate: ByteRate = (batch / window).into();
//! assert_eq!(rate.bytes_per_sec_i64(), 4 * 1024 * 1024);
//! ```
//!
//! # Magnitudes, not instants
//!
//! These types model *extents*: how many bytes, how long, how fast. They do not
//! model *points* — a log offset, a leader epoch, or an epoch-milliseconds
//! timestamp is an identifier or a coordinate, not a dimensioned magnitude, and
//! stays a newtype in `crabka-ids`. An absolute nanosecond timestamp also cannot
//! round-trip through `f64` seconds (see below), which is a second reason instants
//! do not belong here.
//!
//! # Storage type
//!
//! Every quantity stores `f64` in SI base units (bytes, seconds, bytes/second,
//! hertz). One storage type across all five dimensions is what makes
//! cross-dimension arithmetic usable — `uom` only combines quantities that share
//! it — and `f64` is the only choice wide enough for both ends of the range the
//! broker needs, because `uom`'s integer quantities store base units and so
//! cannot represent a sub-second duration at all.
//!
//! `f64` represents every integer below 2^53 exactly, which covers byte counts up
//! to 8 PiB and durations up to 285 years of milliseconds, so conversions back to
//! wire integers are exact over the whole range Kafka can express. Absolute
//! nanosecond timestamps (~1.8 × 10^18) are the exception, and are excluded above.
//!
//! # Wire boundary
//!
//! The generated Kafka codec (`crabka-protocol`'s `generated` module) stays raw
//! integers — it must be byte-exact. Convert when a value enters or leaves the
//! hand-written domain layer, using the extension traits in [`convert`]:
//!
//! ```
//! use crabka_units::prelude::*;
//!
//! // Decoding a request: raw `int32` milliseconds in, `Time` out.
//! let timeout = Time::from_millis(30_000);
//! assert_eq!(timeout, secs(30));
//!
//! // Encoding a response: `Time` in, raw `int32` milliseconds out.
//! assert_eq!(timeout.millis_i32(), 30_000);
//! ```
//!
//! # Configuration
//!
//! [`parse`] reads the human forms operators write (`512MiB`, `30s`, `10MiB/s`)
//! and [`fmt`] writes them back, with [`serde_units`] wiring both into
//! `#[serde(with = ...)]` so a config struct holds quantities rather than integers.
//!
//! See the code style guide's "Newtypes for Domain Values" section for the
//! sibling rule covering identifiers.

use core::marker::PhantomData;

pub mod convert;
pub mod fmt;
pub mod parse;
pub mod serde_units;

pub use uom;

/// A count of bytes: a message size, a segment size, a buffer capacity, a quota
/// balance.
///
/// `uom`'s information dimension with `f64` storage; the base unit is the byte,
/// so [`ByteSizeExt::bytes_u64`](convert::ByteSizeExt::bytes_u64) is exact for
/// every value the wire can carry. Construct with [`bytes`], [`kibibytes`],
/// [`mebibytes`], or [`gibibytes`].
pub type ByteSize = uom::si::f64::Information;

/// A byte throughput: a producer quota, a replication rate, a measured
/// send/receive rate.
///
/// Divide a [`ByteSize`] by a [`Time`] to get one (the `uom` product of two
/// quantities erases the information *kind*, so the result needs `.into()`).
pub type ByteRate = uom::si::f64::InformationRate;

/// An extent of time: a timeout, a retention window, a backoff, a measured
/// latency.
///
/// Not an instant — see the crate docs. Construct with [`nanos`], [`micros`],
/// [`millis`], [`secs`], [`minutes`], [`hours`], or [`days`], or convert from
/// [`core::time::Duration`] with
/// [`TimeExt::from_std`](convert::TimeExt::from_std).
pub type Time = uom::si::f64::Time;

/// An event rate: records per second, requests per second, polls per second.
///
/// Distinct from [`ByteRate`], which carries the information dimension. The
/// reciprocal of a [`Time`] period.
pub type Frequency = uom::si::f64::Frequency;

/// A dimensionless fraction: a fill factor, a sampling probability, a percentage.
///
/// Multiplying a [`ByteSize`] by one of these yields a [`ByteSize`], so scaling a
/// buffer by a fraction keeps its dimension.
pub type Ratio = uom::si::f64::Ratio;

/// Everything needed to name, build, and unwrap a quantity.
///
/// Glob-import this in modules that work with quantities:
/// `use crabka_units::prelude::*;`.
pub mod prelude {
    pub use crate::{
        ByteRate, ByteSize, Frequency, Ratio, Time, byte_rate, bytes, bytes_per_sec,
        convert::{ByteRateExt, ByteSizeExt, FrequencyExt, RatioExt, StdDurationExt, TimeExt},
        days, fraction, gibibytes, gibibytes_per_sec, hours, kibibytes, kibibytes_per_sec,
        mebibytes, mebibytes_per_sec, micros, millis, minutes, nanos, per_sec, percent, secs,
    };
}

/// Defines a `const fn` constructor that scales an integral magnitude into a
/// quantity's base unit.
///
/// `u32` inputs keep the widening cast lossless, and cover every magnitude worth
/// writing as a literal; reach for [`convert`]'s `from_*` constructors for values
/// that need the full 64-bit range.
macro_rules! scaled_ctor {
    ($(#[$meta:meta])* $name:ident -> $quantity:ident, $numerator:expr, $denominator:expr) => {
        $(#[$meta])*
        #[must_use]
        pub const fn $name(n: u32) -> $quantity {
            $quantity {
                dimension: PhantomData,
                units: PhantomData,
                value: n as f64 * $numerator / $denominator,
            }
        }
    };
}

scaled_ctor!(
    /// `n` bytes.
    bytes -> ByteSize, 1.0, 1.0
);
scaled_ctor!(
    /// `n` kibibytes (1 KiB = 1024 B).
    kibibytes -> ByteSize, 1024.0, 1.0
);
scaled_ctor!(
    /// `n` mebibytes (1 MiB = 1024 KiB).
    mebibytes -> ByteSize, 1_048_576.0, 1.0
);
scaled_ctor!(
    /// `n` gibibytes (1 GiB = 1024 MiB).
    gibibytes -> ByteSize, 1_073_741_824.0, 1.0
);

scaled_ctor!(
    /// `n` nanoseconds.
    nanos -> Time, 1.0, 1e9
);
scaled_ctor!(
    /// `n` microseconds.
    micros -> Time, 1.0, 1e6
);
scaled_ctor!(
    /// `n` milliseconds.
    millis -> Time, 1.0, 1e3
);
scaled_ctor!(
    /// `n` seconds.
    secs -> Time, 1.0, 1.0
);
scaled_ctor!(
    /// `n` minutes.
    minutes -> Time, 60.0, 1.0
);
scaled_ctor!(
    /// `n` hours.
    hours -> Time, 3600.0, 1.0
);
scaled_ctor!(
    /// `n` days.
    days -> Time, 86_400.0, 1.0
);

scaled_ctor!(
    /// `n` bytes per second.
    bytes_per_sec -> ByteRate, 1.0, 1.0
);
scaled_ctor!(
    /// `n` kibibytes per second.
    kibibytes_per_sec -> ByteRate, 1024.0, 1.0
);
scaled_ctor!(
    /// `n` mebibytes per second.
    mebibytes_per_sec -> ByteRate, 1_048_576.0, 1.0
);
scaled_ctor!(
    /// `n` gibibytes per second.
    gibibytes_per_sec -> ByteRate, 1_073_741_824.0, 1.0
);

scaled_ctor!(
    /// `n` events per second.
    per_sec -> Frequency, 1.0, 1.0
);
scaled_ctor!(
    /// `n` percent, as a dimensionless [`Ratio`] (`percent(25) == fraction(0.25)`).
    percent -> Ratio, 1.0, 100.0
);

/// A byte rate given as a byte count over a time extent, without an intermediate
/// division at the call site.
///
/// `byte_rate(mebibytes(8), secs(2))` reads as "8 MiB every 2 s".
#[must_use]
pub fn byte_rate(size: ByteSize, over: Time) -> ByteRate {
    (size / over).into()
}

/// A dimensionless fraction, where `1.0` is the whole.
#[must_use]
pub const fn fraction(value: f64) -> Ratio {
    Ratio {
        dimension: PhantomData,
        units: PhantomData,
        value,
    }
}
