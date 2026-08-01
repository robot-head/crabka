//! The driver's numeric seams.
//!
//! Two kinds of conversion live here. The `*_to_*` helpers bound a primitive
//! cast (a `u128` of nanoseconds into the `u64` the histogram records, an epoch
//! millisecond into the `i64` `chrono` speaks). The quantity helpers express a
//! [`crabka_units`] quantity as a count of one named unit, which is what a
//! Plotly axis or a CSV column needs: dividing a quantity by one unit of the
//! same dimension yields the count as a dimensionless [`Ratio`], so the scale
//! factor is never written out by hand.

use crabka_units::prelude::*;
use num_traits::ToPrimitive;

pub(crate) fn to_f64<T: ToPrimitive + Copy>(value: T) -> f64 {
    value
        .to_f64()
        .expect("primitive numeric values are representable as f64")
}

pub(crate) fn nonnegative_f64_to_u64(value: f64) -> u64 {
    if value.is_nan() || value <= 0.0 {
        0
    } else if value >= to_f64(u64::MAX) {
        u64::MAX
    } else {
        value.to_u64().unwrap_or_default()
    }
}

pub(crate) fn saturating_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub(crate) fn nonnegative_i64_to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or_default()
}

pub(crate) fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The rate at which `count` events arrived over `window`.
///
/// A zero or negative window yields no rate rather than an infinity, matching
/// how [`crabka_units::convert::FrequencyExt::period`] treats a zero rate.
pub(crate) fn event_rate(count: u64, window: Time) -> Frequency {
    if window <= Time::ZERO {
        return Frequency::ZERO;
    }
    fraction(to_f64(count)) / window
}

/// `size` counted in mebibytes — the unit the report's memory axes are drawn in.
pub(crate) fn mebibytes_f64(size: ByteSize) -> f64 {
    (size / mebibytes(1)).as_f64()
}

/// `extent` counted in milliseconds — the unit the report's latency columns and
/// time-series metric keys are written in.
pub(crate) fn millis_f64(extent: Time) -> f64 {
    (extent / millis(1)).as_f64()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn event_rate_divides_a_count_by_its_window() {
        check!(event_rate(600_000, secs(60)) == per_sec(10_000));
        check!(event_rate(1, millis(500)) == per_sec(2));
    }

    #[test]
    fn event_rate_of_an_empty_window_is_zero() {
        check!(event_rate(5, Time::ZERO) == Frequency::ZERO);
        check!(event_rate(5, secs(0)) == Frequency::ZERO);
    }

    #[test]
    fn quantities_count_themselves_in_a_named_unit() {
        for (counted, want) in [
            (mebibytes_f64(mebibytes(300)), 300.0),
            (mebibytes_f64(kibibytes(512)), 0.5),
            (millis_f64(secs(2)), 2000.0),
            (millis_f64(micros(1500)), 1.5),
        ] {
            check!((counted - want).abs() < f64::EPSILON);
        }
    }
}
