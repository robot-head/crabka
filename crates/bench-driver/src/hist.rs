//! HDR histogram helpers. Latencies are recorded in microseconds with
//! `(min=1us, max=60s, sigfig=3)` — plenty of resolution from sub-ms up to
//! degraded-broker timeouts, without blowing memory.
//!
//! `hdrhistogram` counts raw `u64`s, so microseconds are the histogram's own
//! unit; [`record`] and [`percentiles`] are the seam that converts to and from
//! the [`Time`] extents the rest of the driver passes around.

use crabka_units::prelude::*;
use hdrhistogram::Histogram;

use crate::scenario::LatencyPercentiles;

/// One per-task latency histogram. Recommended way to construct, since
/// the bounds matter for accuracy.
#[must_use]
/// # Panics
/// Panics if synchronized state is poisoned or validated input is missing a field required to produce the output.
pub fn new() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds are valid")
}

/// Record one measured latency, clamping to the histogram's microsecond range
/// so individual outliers don't blow the recorder up.
pub fn record(h: &mut Histogram<u64>, latency: Time) {
    let micros = u64::try_from(latency.micros_i64()).unwrap_or_default();
    let _ = h.record(micros.clamp(1, h.high()));
}

/// Project a histogram into the public percentile shape.
#[must_use]
pub fn percentiles(h: &Histogram<u64>) -> LatencyPercentiles {
    let at = |quantile: f64| Time::from_micros(saturating_micros(h.value_at_quantile(quantile)));
    LatencyPercentiles {
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        p999: at(0.999),
        max: Time::from_micros(saturating_micros(h.max())),
        mean: micros(1) * h.mean(),
        count: h.len(),
    }
}

/// A histogram bucket value, which is a microsecond count, as the `i64` the
/// [`Time`] seam takes.
fn saturating_micros(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn percentiles_report_recorded_extents() {
        let mut h = new();
        for v in [millis(1), millis(2), millis(3), millis(4), millis(5)] {
            record(&mut h, v);
        }
        let p = percentiles(&h);
        check!(p.count == 5);
        check!(p.p50 > Time::ZERO);
        check!(p.max >= p.p99);
        // 3-significant-figure buckets keep a millisecond sample within 0.1%.
        check!((p.max - millis(5)).secs_f64().abs() < millis(1).secs_f64() * 0.01);
        check!((p.mean - millis(3)).secs_f64().abs() < millis(1).secs_f64() * 0.01);
    }

    #[test]
    fn sub_millisecond_latencies_keep_their_resolution() {
        let mut h = new();
        record(&mut h, micros(250));
        let p = percentiles(&h);
        check!(p.p50 >= micros(240));
        check!(p.p50 <= micros(260));
    }

    #[test]
    fn clamps_above_max_into_range() {
        let mut h = new();
        record(&mut h, days(1));
        check!(h.len() == 1);
    }

    #[test]
    fn clamps_below_min_into_range() {
        let mut h = new();
        record(&mut h, Time::ZERO);
        check!(h.len() == 1);
    }
}
