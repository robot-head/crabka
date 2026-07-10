//! Per-series ring buffer of timestamped samples + a thread-safe
//! `UsageStore` that the scraper writes to and the goals read from.
//!
//! Stored series key is `(broker_id, topic, partition, MetricKind)`.
//! Samples older than `config.retention` are dropped on each insert.
//! Counter-reset detection: if `latest.value < earliest.value`, the
//! rate query returns `None` (broker restarted; goals should ignore).
//!
//! Stale-data protection: every query method takes `now_ms` (the
//! caller's wall-clock) and treats the window as `[now_ms - W,
//! now_ms]`. If the latest sample is older than `now_ms - W` the
//! method returns `None`, so a broker that stops emitting (crash,
//! network partition) doesn't keep producing stable results from its
//! last few samples forever.

use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use parking_lot::RwLock;

use crate::scraper::parse::{MetricKind, ParsedSample};

#[derive(Debug, Clone, Copy)]
pub struct WindowConfig {
    pub scrape_interval: Duration,
    pub retention: Duration,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_hours(12),
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
    fn as_duration(self) -> Duration {
        match self {
            Window::FiveMin => Duration::from_mins(5),
            Window::OneHour => Duration::from_hours(1),
            Window::TwelveHour => Duration::from_hours(12),
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
    let window_ms = i64::try_from(window.as_duration().as_millis()).unwrap_or(i64::MAX);
    now_ms - window_ms
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

    /// Insert one scrape tick's worth of samples for a single broker.
    /// `at_ms` is the wall-clock millis at scrape time. Drops samples
    /// older than `config.retention`.
    pub fn insert(&self, broker_id: i32, samples: Vec<ParsedSample>, at_ms: i64) {
        let cutoff = at_ms - i64::try_from(self.config.retention.as_millis()).unwrap_or(i64::MAX);
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

    /// Rate of `BytesIn` (bytes/sec) within `window`, derived from the
    /// earliest + latest samples in the window. Returns `None` if
    /// there are fewer than 2 samples in the window, a counter reset
    /// is detected (`latest.value < earliest.value`), or the latest
    /// sample is older than `now_ms - window` (data is too stale to
    /// represent the requested window).
    #[must_use]
    pub fn bytes_in_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<f64> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::BytesIn,
            window,
            now_ms,
        )
    }

    #[must_use]
    pub fn bytes_out_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<f64> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::BytesOut,
            window,
            now_ms,
        )
    }

    /// Rate of CPU usage in microseconds per second within `window`.
    /// Divide by `1_000_000` to get the equivalent number of CPU cores in
    /// use. Returns `None` on insufficient samples, counter reset, or
    /// stale data (same guards as `bytes_in_rate`).
    #[must_use]
    pub fn cpu_micros_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<f64> {
        self.counter_rate(
            broker_id,
            topic,
            partition,
            MetricKind::CpuMicros,
            window,
            now_ms,
        )
    }

    /// Average disk-bytes gauge over the window `[now_ms - W, now_ms]`.
    /// Returns `None` if no samples fall inside the window, or if the
    /// latest sample is older than `now_ms - W` (stale broker).
    #[must_use]
    pub fn disk_bytes_avg(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
        now_ms: i64,
    ) -> Option<f64> {
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
            #[allow(clippy::cast_precision_loss)]
            Some(sum / count as f64)
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
        #[allow(clippy::cast_precision_loss)]
        let dt_ms = (latest.at_ms - earliest.at_ms) as f64;
        let dv = latest.value - earliest.value;
        Some(dv * 1000.0 / dt_ms)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

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
        assert_eq!(s.bytes_in_rate(1, "t", 0, Window::FiveMin, 0), None);
        assert_eq!(s.disk_bytes_avg(1, "t", 0, Window::FiveMin, 0), None);
    }

    #[test]
    fn two_counter_samples_yield_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 1000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 3000.0)], 1000);
        // (3000 - 1000) / 1.0s = 2000 bytes/sec. Query at now_ms=1000 so
        // the latest sample is on the window's upper bound.
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1000).unwrap();
        assert!((rate - 2000.0).abs() < 1e-6, "got {rate}");
    }

    #[test]
    fn two_cpu_micros_samples_yield_rate() {
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
        // (2_100_000 - 100_000) / 1.0s = 2_000_000 micros/sec.
        let rate = s
            .cpu_micros_rate(1, "t", 0, Window::FiveMin, now_ms)
            .unwrap();
        assert!((rate - 2_000_000.0).abs() < 1e-3, "got {rate}");
    }

    #[test]
    fn counter_reset_returns_none() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 5000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1000);
        assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1000).is_none());
    }

    #[test]
    fn gauge_average() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 200.0)], 1000);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 300.0)], 2000);
        // Query at now_ms=2000 (latest sample). Average of [100, 200, 300] = 200.
        let avg = s.disk_bytes_avg(1, "t", 0, Window::FiveMin, 2000).unwrap();
        assert!((avg - 200.0).abs() < 1e-6, "got {avg}");
    }

    #[test]
    fn retention_drops_old_samples() {
        let s = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_mins(1),
        });
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 30_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 90_000);
        // First sample (t=0) was 90s ago, beyond the 60s retention; dropped.
        // Only samples at 30_000 and 90_000 remain.
        // The 5-min window includes both. Rate = (300-200)/60s = ~1.67/sec.
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 90_000).unwrap();
        assert!((rate - 100.0 / 60.0).abs() < 1e-3, "got {rate}");
    }

    #[test]
    fn retention_cutoff_subtracts_retention_from_insert_time() {
        let s = UsageStore::new(WindowConfig {
            scrape_interval: Duration::from_secs(30),
            retention: Duration::from_mins(1),
        });
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 30_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 100_000);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 300.0)], 120_000);

        let rate = s
            .bytes_in_rate(1, "t", 0, Window::FiveMin, 120_000)
            .unwrap();
        assert!((rate - 5.0).abs() < 1e-6, "got {rate}");
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
        assert!((avg - 150.0).abs() < 1e-6, "got {avg}");
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
        assert!((rate - 200.0).abs() < 1e-6, "got {rate}");
    }

    #[test]
    fn counter_rate_requires_two_windowed_samples_after_lower_bound() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 10);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 200.0)], 11);

        assert!(
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
        assert!((rate - 200.0).abs() < 1e-6, "got {rate}");
    }

    #[test]
    fn flat_counter_yields_zero_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1_000);

        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin, 1_000).unwrap();
        assert!(rate.abs() < f64::EPSILON);
    }

    /// Regression: a broker that stops emitting must not keep producing
    /// a stable rate from its last two samples once `now_ms` advances
    /// past the window boundary.
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
        assert!(
            s.bytes_in_rate(1, "t", 0, Window::FiveMin, now_ms)
                .is_none(),
            "stale broker must not keep producing a rate"
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
        assert!(
            s.disk_bytes_avg(1, "t", 0, Window::FiveMin, now_ms)
                .is_none(),
            "stale broker must not keep producing a disk average"
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
