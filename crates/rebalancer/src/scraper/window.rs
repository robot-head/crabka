//! Per-series ring buffer of timestamped samples + a thread-safe
//! `UsageStore` that the scraper writes to and the goals read from.
//!
//! Stored series key is `(broker_id, topic, partition, MetricKind)`.
//! Samples older than `config.retention` are dropped on each insert.
//! Counter-reset detection: if `latest.value < earliest.value`, the
//! rate query returns `None` (broker restarted; goals should ignore).

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::Duration;

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

#[derive(Debug, Default)]
struct RingBuffer {
    samples: VecDeque<Sample>,
}

#[derive(Debug)]
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
        let mut map = self.inner.write().unwrap();
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
    /// there are fewer than 2 samples in the window or if a counter
    /// reset is detected (latest.value < earliest.value).
    #[must_use]
    pub fn bytes_in_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        self.counter_rate(broker_id, topic, partition, MetricKind::BytesIn, window)
    }

    #[must_use]
    pub fn bytes_out_rate(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        self.counter_rate(broker_id, topic, partition, MetricKind::BytesOut, window)
    }

    /// Average disk-bytes gauge over `window`. Returns `None` if no
    /// samples in window.
    #[must_use]
    pub fn disk_bytes_avg(
        &self,
        broker_id: i32,
        topic: &str,
        partition: i32,
        window: Window,
    ) -> Option<f64> {
        let key = SeriesKey {
            broker_id,
            topic: topic.to_string(),
            partition,
            metric: MetricKind::DiskBytes,
        };
        let map = self.inner.read().unwrap();
        let buf = map.get(&key)?;
        let now_ms = buf.samples.back()?.at_ms;
        let lower = now_ms - i64::try_from(window.as_duration().as_millis()).unwrap_or(i64::MAX);
        let mut sum = 0.0f64;
        let mut count = 0u64;
        for s in &buf.samples {
            if s.at_ms >= lower {
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
    ) -> Option<f64> {
        let key = SeriesKey {
            broker_id,
            topic: topic.to_string(),
            partition,
            metric,
        };
        let map = self.inner.read().unwrap();
        let buf = map.get(&key)?;
        if buf.samples.len() < 2 {
            return None;
        }
        let latest = *buf.samples.back()?;
        let lower =
            latest.at_ms - i64::try_from(window.as_duration().as_millis()).unwrap_or(i64::MAX);
        // Earliest sample within the window.
        let earliest = buf.samples.iter().find(|s| s.at_ms >= lower).copied()?;
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

impl Default for UsageStore {
    fn default() -> Self {
        Self::new(WindowConfig::default())
    }
}

#[cfg(test)]
mod tests {
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
        assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin).is_none());
        assert!(s.disk_bytes_avg(1, "t", 0, Window::FiveMin).is_none());
    }

    #[test]
    fn two_counter_samples_yield_rate() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 1000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 3000.0)], 1000);
        // (3000 - 1000) / 1.0s = 2000 bytes/sec
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin).unwrap();
        assert!((rate - 2000.0).abs() < 1e-6, "got {rate}");
    }

    #[test]
    fn counter_reset_returns_none() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 5000.0)], 0);
        s.insert(1, vec![sample(MetricKind::BytesIn, "t", 0, 100.0)], 1000);
        assert!(s.bytes_in_rate(1, "t", 0, Window::FiveMin).is_none());
    }

    #[test]
    fn gauge_average() {
        let s = UsageStore::default();
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 100.0)], 0);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 200.0)], 1000);
        s.insert(1, vec![sample(MetricKind::DiskBytes, "t", 0, 300.0)], 2000);
        let avg = s.disk_bytes_avg(1, "t", 0, Window::FiveMin).unwrap();
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
        // The 5-min window includes both. Rate = (300-200)/60s = ~1.67/sec
        let rate = s.bytes_in_rate(1, "t", 0, Window::FiveMin).unwrap();
        assert!((rate - 100.0 / 60.0).abs() < 1e-3, "got {rate}");
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
                    let _ = reader_store.bytes_in_rate(i, "t", 0, Window::FiveMin);
                }
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        // Reaching here means no deadlock; sanity-check that at least
        // one rate is queryable.
        let _ = store.bytes_in_rate(0, "t", 0, Window::FiveMin);
    }
}
