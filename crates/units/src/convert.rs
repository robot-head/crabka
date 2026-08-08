//! Conversions between quantities and the raw numbers at the edges of the system.
//!
//! Quantities live in the hand-written domain layer. At its edges the value must
//! be a plain integer, float, or [`core::time::Duration`]. Those edges are the
//! generated Kafka codec, an on-disk record, a `tokio` timer, and a Prometheus
//! gauge. These extension traits are that seam, so this module states the
//! rounding and saturation rules once instead of at every boundary.
//!
//! Conversions *into* a quantity are exact for every magnitude Kafka can express.
//! Conversions *out* round to nearest and saturate at the target type's bounds
//! instead of wrapping, and they map `NaN` to zero.

use core::{marker::PhantomData, time::Duration};

use num_traits::cast::{NumCast, ToPrimitive};

use crate::{ByteRate, ByteSize, Frequency, Ratio, Time};

/// Rounds to nearest and saturates at `i64`'s bounds; `NaN` becomes `0`.
fn round_i64(value: f64) -> i64 {
    if value.is_nan() {
        return 0;
    }
    let rounded = value.round();
    rounded
        .to_i64()
        .unwrap_or(if rounded < 0.0 { i64::MIN } else { i64::MAX })
}

/// Rounds to nearest and saturates at `i32`'s bounds; `NaN` becomes `0`.
fn round_i32(value: f64) -> i32 {
    if value.is_nan() {
        return 0;
    }
    let rounded = value.round();
    rounded
        .to_i32()
        .unwrap_or(if rounded < 0.0 { i32::MIN } else { i32::MAX })
}

/// Rounds to nearest and saturates into `0..=u64::MAX`; negatives and `NaN`
/// become `0`.
fn round_u64(value: f64) -> u64 {
    if value.is_nan() {
        return 0;
    }
    let rounded = value.round();
    rounded
        .to_u64()
        .unwrap_or(if rounded < 0.0 { 0 } else { u64::MAX })
}

/// Rounds to nearest and saturates into `0..=usize::MAX`; negatives and `NaN`
/// become `0`.
fn round_usize(value: f64) -> usize {
    if value.is_nan() {
        return 0;
    }
    let rounded = value.round();
    rounded
        .to_usize()
        .unwrap_or(if rounded < 0.0 { 0 } else { usize::MAX })
}

/// Widens an integer into `f64`. Exact for every magnitude below 2^53.
fn widen<T: ToPrimitive>(value: T) -> f64 {
    NumCast::from(value).unwrap_or(f64::NAN)
}

/// Byte counts: construction from raw integers, and extraction back to them.
pub trait ByteSizeExt: Sized + Copy {
    /// No bytes.
    const ZERO: Self;

    /// A raw unsigned byte count.
    fn from_bytes(bytes: u64) -> Self;

    /// A raw signed byte count. This keeps the sign, so it also carries a
    /// *delta*, for example a quota balance that has gone into deficit. This
    /// function does not interpret Kafka's `-1` "unlimited" sentinel. Use
    /// [`wire::opt_size_from_bytes_i64`] for that.
    fn from_bytes_i64(bytes: i64) -> Self;

    /// A raw byte count measured as a float (a sampled or averaged size).
    fn from_bytes_f64(bytes: f64) -> Self;

    /// The count as an unsigned byte total.
    fn bytes_u64(self) -> u64;

    /// The count as a `usize`, for buffer capacities and slice lengths.
    fn bytes_usize(self) -> usize;

    /// The count as Kafka's `int32` byte fields (`max_bytes`, `min_bytes`).
    fn bytes_i32(self) -> i32;

    /// The count as Kafka's `int64` byte fields (`retention.bytes`).
    fn bytes_i64(self) -> i64;

    /// The count as a float, for metrics gauges.
    fn bytes_f64(self) -> f64;
}

impl ByteSizeExt for ByteSize {
    const ZERO: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 0.0,
    };

    fn from_bytes(bytes: u64) -> Self {
        Self::from_bytes_f64(widen(bytes))
    }

    fn from_bytes_i64(bytes: i64) -> Self {
        Self::from_bytes_f64(widen(bytes))
    }

    fn from_bytes_f64(bytes: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: bytes,
        }
    }

    fn bytes_u64(self) -> u64 {
        round_u64(self.value)
    }

    fn bytes_usize(self) -> usize {
        round_usize(self.value)
    }

    fn bytes_i32(self) -> i32 {
        round_i32(self.value)
    }

    fn bytes_i64(self) -> i64 {
        round_i64(self.value)
    }

    fn bytes_f64(self) -> f64 {
        self.value
    }
}

/// Time extents: construction from raw integers and [`Duration`], and extraction
/// back to them.
pub trait TimeExt: Sized + Copy {
    /// No elapsed time.
    const ZERO: Self;

    /// A raw millisecond count. This is Kafka's unit for every configured
    /// timeout, interval, and retention window.
    fn from_millis(millis: i64) -> Self;

    /// A raw microsecond count.
    fn from_micros(micros: i64) -> Self;

    /// A raw nanosecond count, for measured latencies.
    fn from_nanos(nanos: i64) -> Self;

    /// A raw second count.
    fn from_secs(secs: i64) -> Self;

    /// A fractional second count.
    fn from_secs_f64(secs: f64) -> Self;

    /// A [`Duration`], which is the unit `tokio`, `std`, and `qubit-clock` use.
    fn from_std(duration: Duration) -> Self;

    /// The extent as Kafka's `int32` millisecond fields (`session_timeout_ms`).
    fn millis_i32(self) -> i32;

    /// The extent as Kafka's `int64` millisecond fields (`retention_ms`).
    fn millis_i64(self) -> i64;

    /// The extent in whole milliseconds, truncated toward zero.
    ///
    /// [`Self::millis_i64`] rounds to nearest, which is correct for a value that
    /// a caller reads back as a duration. Use this function only to match an
    /// external format that truncates. Tempo's `durationMs` is its nanosecond
    /// duration integer-divided by a million, so rounding would report one
    /// millisecond more than Tempo does for the same span.
    fn millis_i64_trunc(self) -> i64;

    /// The extent in microseconds.
    fn micros_i64(self) -> i64;

    /// The extent in nanoseconds.
    fn nanos_i64(self) -> i64;

    /// The extent in whole seconds, rounded to nearest.
    fn secs_i64(self) -> i64;

    /// The extent in fractional seconds, for metrics histograms.
    fn secs_f64(self) -> f64;

    /// The extent as a [`Duration`], for `tokio::time::sleep` and friends.
    ///
    /// A negative or `NaN` extent becomes [`Duration::ZERO`], because `Duration`
    /// cannot represent it and every caller means "do not sleep". A deadline that
    /// has already passed gives such an extent.
    fn to_std(self) -> Duration;
}

impl TimeExt for Time {
    const ZERO: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 0.0,
    };

    fn from_millis(millis: i64) -> Self {
        Self::from_secs_f64(widen(millis) / 1e3)
    }

    fn from_micros(micros: i64) -> Self {
        Self::from_secs_f64(widen(micros) / 1e6)
    }

    fn from_nanos(nanos: i64) -> Self {
        Self::from_secs_f64(widen(nanos) / 1e9)
    }

    fn from_secs(secs: i64) -> Self {
        Self::from_secs_f64(widen(secs))
    }

    fn from_secs_f64(secs: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: secs,
        }
    }

    fn from_std(duration: Duration) -> Self {
        Self::from_secs_f64(duration.as_secs_f64())
    }

    fn millis_i32(self) -> i32 {
        round_i32(self.value * 1e3)
    }

    fn millis_i64(self) -> i64 {
        round_i64(self.value * 1e3)
    }

    fn millis_i64_trunc(self) -> i64 {
        // Divide the exact nanosecond count rather than truncating a scaled
        // float: `0.042 * 1e3` is not exactly 42, so a float `trunc` here would
        // report 41 milliseconds for a 42-millisecond extent.
        self.nanos_i64() / 1_000_000
    }

    fn micros_i64(self) -> i64 {
        round_i64(self.value * 1e6)
    }

    fn nanos_i64(self) -> i64 {
        round_i64(self.value * 1e9)
    }

    fn secs_i64(self) -> i64 {
        round_i64(self.value)
    }

    fn secs_f64(self) -> f64 {
        self.value
    }

    fn to_std(self) -> Duration {
        Duration::try_from_secs_f64(self.value).unwrap_or(Duration::ZERO)
    }
}

/// Byte throughputs: construction from raw integers, and extraction back to them.
pub trait ByteRateExt: Sized + Copy {
    /// No throughput.
    const ZERO: Self;

    /// A raw bytes-per-second rate, as Kafka's quota configs express it.
    fn from_bytes_per_sec(rate: i64) -> Self;

    /// A fractional bytes-per-second rate, as a measurement produces it.
    fn from_bytes_per_sec_f64(rate: f64) -> Self;

    /// The rate in bytes per second.
    fn bytes_per_sec_f64(self) -> f64;

    /// The rate as Kafka's `int64` quota fields.
    fn bytes_per_sec_i64(self) -> i64;

    /// How long transferring `size` takes at this rate.
    ///
    /// A zero or negative rate gives [`TimeExt::ZERO`] and not an infinity,
    /// because every caller reads an unset quota as "unlimited".
    fn time_to_transfer(self, size: ByteSize) -> Time;
}

impl ByteRateExt for ByteRate {
    const ZERO: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 0.0,
    };

    fn from_bytes_per_sec(rate: i64) -> Self {
        Self::from_bytes_per_sec_f64(widen(rate))
    }

    fn from_bytes_per_sec_f64(rate: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: rate,
        }
    }

    fn bytes_per_sec_f64(self) -> f64 {
        self.value
    }

    fn bytes_per_sec_i64(self) -> i64 {
        round_i64(self.value)
    }

    fn time_to_transfer(self, size: ByteSize) -> Time {
        if self.value <= 0.0 {
            return <Time as TimeExt>::ZERO;
        }
        size / self
    }
}

/// Event rates: construction from raw numbers, and extraction back to them.
pub trait FrequencyExt: Sized + Copy {
    /// No events.
    const ZERO: Self;

    /// A raw events-per-second rate.
    fn from_per_sec(rate: f64) -> Self;

    /// A whole-events-per-second rate, as a counter-based limiter configures one.
    fn from_per_sec_u64(rate: u64) -> Self;

    /// The rate in events per second.
    fn per_sec_f64(self) -> f64;

    /// The rate as whole events per second, for a limiter that counts in
    /// integers. Rounds to nearest; negatives and `NaN` become zero.
    fn per_sec_u64(self) -> u64;

    /// The interval between events at this rate.
    ///
    /// A zero or negative rate gives [`TimeExt::ZERO`] and not an infinity.
    fn period(self) -> Time;
}

impl FrequencyExt for Frequency {
    const ZERO: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 0.0,
    };

    fn from_per_sec(rate: f64) -> Self {
        Self {
            dimension: PhantomData,
            units: PhantomData,
            value: rate,
        }
    }

    fn from_per_sec_u64(rate: u64) -> Self {
        Self::from_per_sec(widen(rate))
    }

    fn per_sec_f64(self) -> f64 {
        self.value
    }

    fn per_sec_u64(self) -> u64 {
        round_u64(self.value)
    }

    fn period(self) -> Time {
        if self.value <= 0.0 {
            return <Time as TimeExt>::ZERO;
        }
        1.0 / self
    }
}

/// Dimensionless fractions.
pub trait RatioExt: Sized + Copy {
    /// Nothing.
    const ZERO: Self;

    /// The whole (`1.0`, or 100%).
    const ONE: Self;

    /// The fraction as a plain number, where `1.0` is the whole.
    fn as_f64(self) -> f64;

    /// The fraction as a percentage.
    fn percent_f64(self) -> f64;
}

impl RatioExt for Ratio {
    const ZERO: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 0.0,
    };

    const ONE: Self = Self {
        dimension: PhantomData,
        units: PhantomData,
        value: 1.0,
    };

    fn as_f64(self) -> f64 {
        self.value
    }

    fn percent_f64(self) -> f64 {
        self.value * 100.0
    }
}

/// Converting a [`Duration`] at an API boundary that hands one out.
pub trait StdDurationExt {
    /// This duration as a [`Time`] quantity.
    fn as_time(&self) -> Time;
}

impl StdDurationExt for Duration {
    fn as_time(&self) -> Time {
        Time::from_std(*self)
    }
}

/// Kafka's `-1` sentinel, which means "unlimited", "none", or "no bound"
/// depending on the field, mapped to and from [`Option`].
///
/// A `None` means the field is unset, and the caller decides what unset implies.
/// Do not use these functions for fields where `-1` means something else. A
/// `timeout_ms` of `-1` means "block indefinitely", which is a value and not an
/// absence, and the calling code's own enum models it better.
pub mod wire {
    use super::{ByteRate, ByteRateExt, ByteSize, ByteSizeExt, Time, TimeExt};

    /// `-1` becomes `None`; any other value is a byte count.
    #[must_use]
    pub fn opt_size_from_bytes_i32(raw: i32) -> Option<ByteSize> {
        (raw >= 0).then(|| ByteSize::from_bytes_i64(i64::from(raw)))
    }

    /// `None` becomes `-1`.
    #[must_use]
    pub fn opt_size_to_bytes_i32(size: Option<ByteSize>) -> i32 {
        size.map_or(-1, ByteSizeExt::bytes_i32)
    }

    /// `-1` becomes `None`; any other value is a byte count.
    #[must_use]
    pub fn opt_size_from_bytes_i64(raw: i64) -> Option<ByteSize> {
        (raw >= 0).then(|| ByteSize::from_bytes_i64(raw))
    }

    /// `None` becomes `-1`.
    #[must_use]
    pub fn opt_size_to_bytes_i64(size: Option<ByteSize>) -> i64 {
        size.map_or(-1, ByteSizeExt::bytes_i64)
    }

    /// `-1` becomes `None`; any other value is a millisecond extent.
    #[must_use]
    pub fn opt_time_from_millis_i64(raw: i64) -> Option<Time> {
        (raw >= 0).then(|| Time::from_millis(raw))
    }

    /// `None` becomes `-1`.
    #[must_use]
    pub fn opt_time_to_millis_i64(time: Option<Time>) -> i64 {
        time.map_or(-1, TimeExt::millis_i64)
    }

    /// `-1` becomes `None`; any other value is a bytes-per-second quota.
    #[must_use]
    pub fn opt_rate_from_bytes_per_sec_i64(raw: i64) -> Option<ByteRate> {
        (raw >= 0).then(|| ByteRate::from_bytes_per_sec(raw))
    }

    /// `None` becomes `-1`.
    #[must_use]
    pub fn opt_rate_to_bytes_per_sec_i64(rate: Option<ByteRate>) -> i64 {
        rate.map_or(-1, ByteRateExt::bytes_per_sec_i64)
    }
}
