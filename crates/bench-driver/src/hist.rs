//! HDR histogram helpers. Latencies are recorded in microseconds with
//! `(min=1us, max=60s, sigfig=3)` — plenty of resolution from sub-ms up to
//! degraded-broker timeouts, without blowing memory.

use hdrhistogram::Histogram;

use crate::numeric::to_f64;

use crate::scenario::LatencyPercentiles;

/// One per-task latency histogram. Recommended way to construct, since
/// the bounds matter for accuracy.
#[must_use]
/// # Panics
/// Panics if synchronized state is poisoned or validated input is missing a field required to produce the output.
pub fn new() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds are valid")
}

/// Record one latency sample in microseconds, clamping to the histogram
/// range so individual outliers don't blow the recorder up.
pub fn record_us(h: &mut Histogram<u64>, us: u64) {
    let v = us.clamp(1, h.high());
    let _ = h.record(v);
}

/// Project a histogram into the public percentile shape (ms, not μs).
#[must_use]
pub fn percentiles(h: &Histogram<u64>) -> LatencyPercentiles {
    let to_ms = |us: u64| (to_f64(us)) / 1000.0;
    LatencyPercentiles {
        p50_ms: to_ms(h.value_at_quantile(0.50)),
        p95_ms: to_ms(h.value_at_quantile(0.95)),
        p99_ms: to_ms(h.value_at_quantile(0.99)),
        p999_ms: to_ms(h.value_at_quantile(0.999)),
        max_ms: to_ms(h.max()),
        mean_ms: h.mean() / 1000.0,
        count: h.len(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn percentiles_basic() {
        let mut h = new();
        for v in [1_000u64, 2_000, 3_000, 4_000, 5_000] {
            record_us(&mut h, v);
        }
        let p = percentiles(&h);
        check!(p.p50_ms > 0.0);
        assert2::assert!(p.count == 5);
        check!(p.max_ms >= p.p99_ms);
    }

    #[test]
    fn clamps_above_max_into_range() {
        let mut h = new();
        record_us(&mut h, u64::MAX);
        assert2::assert!(h.len() == 1);
    }
}
