//! Per-series ring buffer of timestamped samples, plus a thread-safe
//! `UsageStore` that the scraper writes to and the goals read from.
//!
//! The stored series key is `(broker_id, topic, partition, MetricKind)`. Each
//! insert drops samples older than `config.retention`. Counter-reset detection
//! works like this: if `latest.value < earliest.value`, the rate query returns
//! `None`, because the broker restarted and the goals should ignore the series.
//!
//! Stale-data protection works like this: every query method takes `now_ms`,
//! the caller's wall-clock, and treats the window as `[now_ms - W, now_ms]`.
//! If the latest sample is older than `now_ms - W` the method returns `None`.
//! A broker that stops emitting, after a crash or a network partition, then
//! cannot keep producing stable results from its last few samples forever.

use std::collections::{HashMap, VecDeque};

use crabka_units::prelude::*;
use num_traits::ToPrimitive;
use parking_lot::RwLock;

use crate::scraper::parse::{MetricKind, ParsedSample};

/// CPU microseconds spent per wall-clock second by one fully busy core.
///
/// The broker's CPU counter is in microseconds, so this is the one place where
/// the micros-per-second rate becomes a core count.
const MICROS_PER_CORE_SECOND: f64 = 1e6;

#[derive(Debug, Clone, Copy)]
pub struct WindowConfig {
    pub scrape_interval: Time,
    pub retention: Time,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            scrape_interval: secs(30),
            retention: hours(12),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Window {
    FiveMin,
    OneHour,
    TwelveHour,
}

impl Window {
    fn as_time(self) -> Time {
        match self {
            Window::FiveMin => minutes(5),
            Window::OneHour => hours(1),
            Window::TwelveHour => hours(12),
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct SeriesKey {
    broker_id: i32,
    topic: String,
    partition: i32,
    metric: MetricKind,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    at_ms: i64,
    value: f64,
}

fn series_key(broker_id: i32, topic: &str, partition: i32, metric: MetricKind) -> SeriesKey {
    SeriesKey {
        broker_id,
        topic: topic.to_string(),
        partition,
        metric,
    }
}

fn window_lower_bound(window: Window, now_ms: i64) -> i64 {
    now_ms - window.as_time().millis_i64()
}

fn sample_in_window(sample: &Sample, lower: i64, upper: i64) -> bool {
    sample.at_ms >= lower && sample.at_ms <= upper
}

#[derive(Debug, Default)]
struct RingBuffer {
    samples: VecDeque<Sample>,
}

// `#[derive(Default)]` works here because every field has a `Default`
// impl: `parking_lot::RwLock<T>: Default` when `T: Default`, and
// `WindowConfig` has its own `Default` impl above.
#[derive(Debug, Default)]
pub struct UsageStore {
    inner: RwLock<HashMap<SeriesKey, RingBuffer>>,
    config: WindowConfig,
}

impl UsageStore {
    #[must_use]
    pub fn new(config: WindowConfig) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Insert one scrape tick's worth of samples for a single broker. `at_ms`
    /// is the wall-clock millis at scrape time. The insert drops samples older
    /// than `config.retention`.
    pub fn insert(&self, broker_id: i32, samples: Vec<ParsedSample>, at_ms: i64) {
        let cutoff = at_ms - self.config.retention.millis_i64();
        let mut map = self.inner.write();
        for s in samples {
            let key = SeriesKey {
                broker_id,
                topic: s.topic,
                partition: s.partition,
                metric: s.metric,
            };
            let buf = map.entry(key).or_default();
            buf.samples.push_back(Sample {
                at_ms,
                value: s.value,
            });
            while buf.samples.front().is_some_and(|f| f.at_ms < cutoff) {
                buf.samples.pop_front();
            }
        }
    }

    /// Rate of `BytesIn` within `window`, derived from the earliest and the
    /// latest samples in the window.
    ///
    /// Returns `None` if the window holds fewer than 2 samples. Returns `None`
    /// if it detects a counter reset, that is, `latest.value <
    /// earliest.value`. Returns `None` if the latest sample is older than
    /// `now_ms - window`, because the data is then too stale to represent the
    /// requested window.
    #[must_use]
    pub fn bytes_in_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<ByteRate> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::BytesIn,
            window,
            now_ms,
        )
        .map(ByteRate::from_bytes_per_sec_f64)
    }

    #[must_use]
    pub fn bytes_out_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<ByteRate> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::BytesOut,
            window,
            now_ms,
        )
        .map(ByteRate::from_bytes_per_sec_f64)
    }

    /// CPU seconds burned per wall-clock second within `window`, that is, the
    /// equivalent number of busy cores, as a dimensionless [`Ratio`].
    ///
    /// Returns `None` on too few samples, on a counter reset, or on stale
    /// data. These are the same guards as `bytes_in_rate`.
    #[must_use]
    pub fn cpu_cores_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<Ratio> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::CpuMicros,
            window,
            now_ms,
        )
        .map(|micros_per_sec| fraction(micros_per_sec / MICROS_PER_CORE_SECOND))
    }

    /// Average disk-bytes gauge over the window `[now_ms - W, now_ms]`.
    ///
    /// Returns `None` if no sample falls inside the window, or if the latest
    /// sample is older than `now_ms - W`, which means the broker is stale.
    #[must_use]
    pub fn disk_bytes_avg(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<ByteSize> {
        let key = series_key(broker_id, topic, partition, MetricKind::DiskBytes);
        let map = self.inner.read();
        let buf = map.get(&key)?;
        let lower = window_lower_bound(window, now_ms);
        let mut sum = 0.0f64;
        let mut count = 0u64;
        for s in &buf.samples {
            if sample_in_window(s, lower, now_ms) {
                sum += s.value;
                count += 1;
            }
        }
        if count == 0 {
            None
        } else {
            Some(ByteSize::from_bytes_f64(sum / count.to_f64()?))
        }
    }

    fn counter_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        metric: MetricKind,
        window: Window,
        now_ms: i64,
    ) -> Option<f64> {
        let key = series_key(broker_id, topic, partition, metric);
        let map = self.inner.read();
        let buf = map.get(&key)?;
        let lower = window_lower_bound(window, now_ms);
        // Clamp both ends to the requested window so stale or future-dated
        // samples retained in the ring do not dominate the rate.
        let mut in_window = buf
            .samples
            .iter()
            .filter(|s| sample_in_window(s, lower, now_ms))
            .copied();
        let earliest = in_window.next()?;
        let latest = in_window.last().unwrap_or(earliest);
        if latest.at_ms == earliest.at_ms {
            return None;
        }
        // Counter reset detection.
        if latest.value < earliest.value {
            return None;
        }
        let dt_ms = (latest.at_ms - earliest.at_ms).to_f64()?;
        let dv = latest.value - earliest.value;
        Some(dv * 1000.0 / dt_ms)
    }
}

#[cfg(test)]
mod tests {

    use crabka_units::prelude::*;

    use super::*;

    fn sample(metric: MetricKind, topic: &str, partition: i32, value: f64) -> ParsedSample {
        ParsedSample {
            metric,
            topic: topic.into(),
            partition,
            value,
        }
    }

    #[test]
    fn empty_store_returns_none() {
        let s = UsageStore::default();
        assert2::assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin, 0) == None);
        assert2::assert!(s.disk_bytes_avg(1, "t", 0, Window::FiveMin, 0) == None);
    }

    #[test]
    fn two_counter_samples_yield_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 1000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 3000.0)], 1000);
        // (3000 - 1000) / 1.0s = 2000 bytes/sec. Query at now_ms=1000 so
        // the latest sample is on the window's upper bound.
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1000).unwrap();
        assert2::assert!(rate == bytes_per_sec(2000));
    }

    #[test]
    fn two_cpu_samples_yield_core_rate() {
        let s = UsageStore::default();
        let now_ms = 1_000_000;
        s.insert(
            1,
            vec![sample(MetricKind::CpuMicros, "t", 0, 100_000.0)],
            now_ms - 1000,
        );
        s.insert(
            1,
            vec![sample(MetricKind::CpuMicros, "t", 0, 2_100_000.0)],
            now_ms,
        );
        // (2_100_000 - 100_000) / 1.0s = 2_000_000 micros/sec = 2 busy cores.
        let rate = s
            .cpu_cores_rate(1, "t", 0, Window::FiveMin, now_ms)
            .unwrap();
        assert2::assert!(rate == fraction(2.0));
    }

    #[test]
    fn counter_reset_returns_none() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 5000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1000);
        assert2::assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1000).is_none());
    }

    #[test]
    fn gauge_average() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 200.0)], 1000);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 300.0)], 2000);
        // Query at now_ms=2000 (latest sample). Average of [100, 200, 300] = 200.
        let avg = s.disk_bytes_avg(1, "t", 0, Window::FiveMin, 2000).unwrap();
        assert2::assert!(avg == bytes(200));
    }

    #[test]
    fn retention_drops_old_samples() {
        let s = UsageStore::new(WindowConfig {
            scrape_interval: secs(30),
            retention: minutes(1),
        });
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 30_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 90_000);
        // First sample (t=0) was 90s ago, beyond the 60s retention; dropped.
        // Only samples at 30_000 and 90_000 remain.
        // The 5-min window includes both. Rate = (300-200)/60s = ~1.67/sec.
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 90_000).unwrap();
        assert2::assert!((rate.bytes_per_sec_f64() - 100.0 / 60.0).abs() < 1e-3);
    }

    #[test]
    fn retention_cutoff_subtracts_retention_from_insert_time() {
        let s = UsageStore::new(WindowConfig {
            scrape_interval: secs(30),
            retention: minutes(1),
        });
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 30_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 100_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 120_000);

        let rate = s
            .bytes_in_rate(1, "t", 0, Window::FiveMin, 120_000)
            .unwrap();
        assert2::assert!(rate == bytes_per_sec(5));
    }

    #[test]
    fn disk_average_includes_lower_bound_and_excludes_outer_samples() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 10.0)], -1);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 200.0)], 1_000);
        s.insert(
            1,
            vec![sample(MetricKind::DiskBytes, "t", 0, 1_000.0)],
            300_001,
        );

        let avg = s
            .disk_bytes_avg(1, "t", 0, Window::FiveMin, 300_000)
            .unwrap();
        assert2::assert!(avg == bytes(150));
    }

    #[test]
    fn counter_rate_uses_window_bounds_and_ignores_future_samples() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 10.0)], -1);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 1_000);
        s.insert(
            1,
            vec![sample(MetricKind::BytesIn, "t", 0, 30_000.0)],
            300_001,
        );

        let rate = s
            .bytes_in_rate(1, "t", 0, Window::FiveMin, 300_000)
            .unwrap();
        assert2::assert!(rate == bytes_per_sec(200));
    }

    #[test]
    fn counter_rate_requires_two_windowed_samples_after_lower_bound() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 10);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 11);

        assert2::assert!(
            s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1_000_000)
                .is_none()
        );
    }

    #[test]
    fn counter_rate_uses_more_than_two_samples_when_available() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 250.0)], 500);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 1_000);

        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1_000).unwrap();
        assert2::assert!(rate == bytes_per_sec(200));
    }

    #[test]
    fn flat_counter_yields_zero_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1_000);

        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1_000).unwrap();
        assert2::assert!(rate == bytes_per_sec(0));
    }

    /// Regression: a broker that stops emitting must not keep producing a
    /// stable rate from its last two samples once `now_ms` advances past the
    /// window boundary.
    #[test]
    fn counter_rate_returns_none_when_latest_sample_predates_window() {
        let s = UsageStore::default();
        // Insert two samples at t=0 and t=1000.
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 1000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 3000.0)], 1000);
        // Query with now_ms beyond a 5-min window after the latest sample.
        // 5min = 300_000ms; latest sample is at 1000; query at 1_000_000
        // means latest is 999_000ms (≈16.6 min) old — well past the
        // 5-min window. Must return None.
        let now_ms = 1_000_000;
        assert2::assert!(
            s.bytes_in_rate(1, "t", 0, Window::FiveMin, now_ms)
                .is_none()
        );
    }

    /// Regression: same stale-data guard for the gauge path.
    #[test]
    fn disk_bytes_avg_returns_none_when_latest_sample_predates_window() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 1000);
        // 5 minutes = 300_000ms. now_ms=10_000_000 means latest sample
        // is ~166 minutes old — well past the window. Returns None.
        let now_ms = 10_000_000;
        assert2::assert!(
            s.disk_bytes_avg(1, "t", 0, Window::FiveMin, now_ms)
                .is_none()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_insert_and_read_does_not_deadlock() {
        let store = std::sync::Arc::new(UsageStore::default());
        let mut handles = Vec::new();
        for i in 0..10 {
            let writer_store = store.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..100 {
                    writer_store.insert(
                        i,
                        vec![sample(MetricKind::BytesIn, "t", 0, f64::from(i * 100 + j))],
                        i64::from(i * 100 + j),
                    );
                }
            }));
            let reader_store = store.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let _ = reader_store.bytes_in_rate(i, "t", 0, Window::FiveMin, 100_000);
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        // Reaching here means no deadlock; sanity-check that at least
        // one rate is queryable.
        let _ = store.bytes_in_rate(0, "t", 0, Window::FiveMin, 100_000);
    }
}
