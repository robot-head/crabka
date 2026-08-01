//! Pure extrapolation math for the rate-family `PromQL` functions.
//!
//! These free functions are a byte-for-byte port of the interpreter's
//! counter-reset + extrapolation algorithm (see `engine.rs`'s
//! `extrapolated_rate` and `instant_delta`). Factoring the math out here lets
//! the `ScalarUDF`s in [`super::rate`] reuse the *exact* arithmetic the
//! tree-walking engine already validates against the conformance corpus, and
//! lets us unit-test the numbers directly.
//!
//! All inputs are decoded `&[f64]` values paired 1:1 with `&[i64]` millisecond
//! timestamps (as produced by `RangeManipulate`'s `<value>_range` /
//! `<time>_range` columns). `range` is the range-selector window width;
//! `range_end_ms` is the eval instant `t` the window closes on;
//! `range_start_ms` is `t - range`. Every function returns `None` where
//! Prometheus yields no sample (fewer than two points, a zero-width sampled
//! interval, etc.), which the UDF layer renders as a **NULL** cell.

use crabka_units::prelude::*;
use num_traits::ToPrimitive;

/// The reset-correcting / windowed range functions evaluated over a full
/// `(t-range, t]` window: `rate`, `increase`, and `delta`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeKind {
    /// Per-second average rate of increase, counter-reset corrected.
    Rate,
    /// Total increase over the window, counter-reset corrected.
    Increase,
    /// Difference between first and last sample (gauge; no reset correction).
    Delta,
}

impl RangeKind {
    /// Whether this function treats the series as a monotonic counter (and so
    /// applies counter-reset correction and the positive zero-anchor clamp).
    fn is_counter(self) -> bool {
        matches!(self, Self::Rate | Self::Increase)
    }
}

/// The instant functions evaluated over only the last two samples of the
/// window: `irate` and `idelta`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InstantKind {
    /// Per-second instant rate from the last two samples, reset-clamped.
    Irate,
    /// Difference of the last two samples (gauge; no per-second division).
    Idelta,
}

/// Prometheus' extrapolated range estimator, shared by `rate`/`increase`/`delta`.
///
/// This is a direct port of the engine's `extrapolated_rate`.
#[must_use]
pub fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeKind,
) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }

    let is_counter = kind.is_counter();

    let mut result = values[n - 1] - values[0];
    if is_counter {
        for window in values.windows(2) {
            if window[1] < window[0] {
                result += window[0];
            }
        }
    }

    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1).to_f64()?;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms).to_f64()? / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts).to_f64()? / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    if is_counter
        && matches!(result.partial_cmp(&0.0), Some(std::cmp::Ordering::Greater))
        && values[0] >= 0.0
    {
        let duration_to_zero = sampled_interval * (values[0] / result);
        duration_to_start = duration_to_start.min(duration_to_zero);
    }

    let extrapolate_to_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolate_to_interval / sampled_interval;
    if kind == RangeKind::Rate {
        let range_seconds = range.secs_f64();
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}

/// Prometheus' instant estimator, shared by `irate`/`idelta`.
///
/// A direct port of the engine's `instant_delta`: uses only the last two
/// samples, clamps a negative `irate` delta to the last value (counter reset),
/// and divides by the inter-sample interval for `irate` only.
#[must_use]
pub fn instant_delta(timestamps: &[i64], values: &[f64], kind: InstantKind) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let previous = values[n - 2];
    let last = values[n - 1];
    let mut result = last - previous;
    if matches!(kind, InstantKind::Irate) && result < 0.0 {
        result = last;
    }

    if matches!(kind, InstantKind::Irate) {
        let interval = (timestamps[n - 1] - timestamps[n - 2]).to_f64()? / 1000.0;
        if interval <= 0.0 {
            return None;
        }
        result /= interval;
    }
    Some(result)
}

#[cfg(test)]
mod tests {

    use super::*;

    /// Mirror the engine's `approx_eq` tolerance for f64 sample comparisons.
    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    /// Pins `rate` to `engine.rs::instant_rate_extrapolates_counter_window`:
    /// samples at 0..240s stepping by 1.0, `rate(...[5m])` at t=300s == 5/300.
    #[test]
    fn rate_extrapolates_counter_window_like_engine() {
        let timestamps = [0_i64, 60_000, 120_000, 180_000, 240_000];
        let values = [0.0, 1.0, 2.0, 3.0, 4.0];
        // range_end = 300_000, range = 300_000 (5m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            300_000,
            millis(300_000),
            RangeKind::Rate,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 5.0 / 300.0));
    }

    /// Pins `increase` reset correction to
    /// `engine.rs::instant_increase_corrects_counter_resets`: 1,2,1 over [2m]
    /// at t=120s => increase == 2.0 (the drop 2->1 adds back the pre-reset 2).
    #[test]
    fn increase_corrects_counter_resets_like_engine() {
        let timestamps = [0_i64, 60_000, 120_000];
        let values = [1.0, 2.0, 1.0];
        // range_end = 120_000, range = 120_000 (2m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            120_000,
            millis(120_000),
            RangeKind::Increase,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 2.0));
    }

    /// Pins `delta` gauge mode to
    /// `engine.rs::instant_delta_is_gauge_delta_without_reset_correction`:
    /// 4,3 over [1m] at t=60s => delta == -2.0 (no reset correction, the drop
    /// is preserved; first sample at 30s, second at 60s).
    #[test]
    fn delta_is_gauge_delta_without_reset_correction_like_engine() {
        let timestamps = [30_000_i64, 60_000];
        let values = [4.0, 3.0];
        // range_end = 60_000, range = 60_000 (1m) => range_start = 0.
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            60_000,
            millis(60_000),
            RangeKind::Delta,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, -2.0));
    }

    /// Durations just beyond 110% of the average sample interval are capped to
    /// half an interval, matching Prometheus' extrapolation threshold.
    #[test]
    fn extrapolation_threshold_uses_ten_percent_slack() {
        let timestamps = [11_050_i64, 21_050];
        let values = [2.0, 12.0];
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            21_050,
            millis(21_050),
            RangeKind::Delta,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 15.0));
    }

    /// Counter extrapolation clamps the start duration to the extrapolated zero
    /// point when the counter would otherwise project below zero.
    #[test]
    fn counter_zero_anchor_limits_start_extrapolation() {
        let timestamps = [5_000_i64, 15_000];
        let values = [1.0, 4.0];
        let got = extrapolated_rate(
            &timestamps,
            &values,
            0,
            15_000,
            millis(15_000),
            RangeKind::Increase,
        )
        .unwrap();
        assert2::assert!(approx_eq(got, 4.0));
    }

    /// A single sample cannot form a rate: Prometheus yields no value.
    #[test]
    fn single_sample_yields_none() {
        let timestamps = [60_000_i64];
        let values = [1.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
        assert2::assert!(instant_delta(&timestamps, &values, InstantKind::Irate).is_none());
    }

    /// Timestamp/value range arrays must be paired 1:1 before any arithmetic.
    #[test]
    fn mismatched_range_lengths_yield_none() {
        let timestamps = [0_i64, 60_000];
        let values = [1.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
    }

    /// A zero-width sampled interval (two coincident timestamps) yields no value.
    #[test]
    fn zero_width_sampled_interval_yields_none() {
        let timestamps = [60_000_i64, 60_000];
        let values = [1.0, 2.0];
        assert2::assert!(
            extrapolated_rate(
                &timestamps,
                &values,
                0,
                60_000,
                millis(60_000),
                RangeKind::Rate
            )
            .is_none()
        );
    }

    /// Pins `irate` to `engine.rs::instant_irate_uses_last_two_samples_per_second`:
    /// 0,1,3 at 0/60/90s, `irate(...[2m])` at t=90s == (3-1)/((90-60)/1000) == 2/30.
    #[test]
    fn irate_uses_last_two_samples_per_second_like_engine() {
        let timestamps = [0_i64, 60_000, 90_000];
        let values = [0.0, 1.0, 3.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 2.0 / 30.0));
    }

    /// Pins `idelta` to
    /// `engine.rs::instant_idelta_uses_last_two_samples_without_per_second_division`:
    /// 0,1,3 at 0/60/90s, `idelta(...[2m])` at t=90s == 3-1 == 2.0 (no division).
    #[test]
    fn idelta_uses_last_two_samples_without_division_like_engine() {
        let timestamps = [0_i64, 60_000, 90_000];
        let values = [0.0, 1.0, 3.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Idelta).unwrap();
        assert2::assert!(approx_eq(got, 2.0));
    }

    /// `irate` clamps a negative last-pair delta (a counter reset) to the last
    /// value, matching the engine's `instant_delta` reset branch.
    #[test]
    fn irate_clamps_counter_reset_to_last_value() {
        // last pair drops 5 -> 2 over 1s: reset, so result = last (2) / 1s = 2.
        let timestamps = [0_i64, 1_000];
        let values = [5.0, 2.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 2.0));
        // idelta preserves the negative delta (gauge): 2 - 5 = -3.
        let idelta = instant_delta(&timestamps, &values, InstantKind::Idelta).unwrap();
        assert2::assert!(approx_eq(idelta, -3.0));
    }

    /// Equal adjacent counter samples are a zero rate, not a reset.
    #[test]
    fn irate_equal_samples_yield_zero_without_reset_clamp() {
        let timestamps = [0_i64, 1_000];
        let values = [5.0, 5.0];
        let got = instant_delta(&timestamps, &values, InstantKind::Irate).unwrap();
        assert2::assert!(approx_eq(got, 0.0));
    }
}
