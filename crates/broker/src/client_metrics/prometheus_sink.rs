//! Prometheus sink for KIP-714 client metrics.
//!
//! Client metric *names* are dynamic, so this module registers a custom
//! `Collector` instead of static `Family` values. That collector renders a
//! live snapshot at scrape time, with stale points removed, as
//! `crabka_client_*` series.

use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};

use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeMetric},
    metrics::{MetricType, gauge::ConstGauge},
};

/// A single decoded client metric data point destined for Prometheus.
#[derive(Debug, Clone)]
pub(crate) struct DataPoint {
    pub metric: String,
    pub client_instance_id: String,
    pub client_id: String,
    pub value: f64,
}

#[derive(Debug)]
struct StoredPoint {
    value: f64,
    at: Instant,
}

type SeriesKey = (String, String, String);

#[derive(Debug)]
pub(crate) struct ClientMetricsCollector {
    points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    ttl: Duration,
}

impl ClientMetricsCollector {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            points: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    /// Records the latest value for each point, replaces any earlier value,
    /// and removes stale points.
    pub(crate) fn ingest(&self, points: &[DataPoint]) {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        for p in points {
            guard.insert(
                (
                    p.metric.clone(),
                    p.client_instance_id.clone(),
                    p.client_id.clone(),
                ),
                StoredPoint {
                    value: p.value,
                    at: now,
                },
            );
        }
        guard.retain(|_, sp| now.duration_since(sp.at) < self.ttl);
    }

    /// The count of points that are not stale. This method also removes the
    /// stale entries in place.
    #[allow(dead_code)] // diagnostic helper; used in tests and future scrape-metrics endpoint
    pub(crate) fn live_point_count(&self) -> usize {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        guard.retain(|_, sp| now.duration_since(sp.at) < self.ttl);
        guard.len()
    }
}

impl Collector for ClientMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let now = Instant::now();
        let guard = self.points.lock().expect("prom sink mutex poisoned");

        // Group live series by sanitized metric name so that encode_descriptor
        // is called exactly once per name. prometheus-client 0.24 emits a
        // # HELP / # TYPE line on every encode_descriptor call, so calling it
        // N times for N series sharing the same name would produce duplicate
        // descriptor lines → invalid OpenMetrics output.
        let mut by_name: HashMap<String, Vec<(&str, &str, f64)>> = HashMap::new();
        for ((metric, instance, client), sp) in guard.iter() {
            if now.duration_since(sp.at) >= self.ttl {
                continue;
            }
            by_name.entry(sanitize(metric)).or_default().push((
                instance.as_str(),
                client.as_str(),
                sp.value,
            ));
        }

        for (name, series) in &by_name {
            let mut metric_encoder = encoder.encode_descriptor(
                name,
                "client-reported metric (KIP-714)",
                None,
                MetricType::Gauge,
            )?;
            for (instance, client, value) in series {
                let labels = [("client_instance_id", *instance), ("client_id", *client)];
                let family_encoder = metric_encoder.encode_family(&labels)?;
                ConstGauge::new(*value).encode(family_encoder)?;
            }
        }
        Ok(())
    }
}

/// Newtype wrapper around `Arc<ClientMetricsCollector>` that implements
/// `prometheus_client::collector::Collector`. It lets `register_collector` add
/// the shared collector to a `Registry`.
#[derive(Debug)]
pub(crate) struct SharedClientMetricsCollector(pub std::sync::Arc<ClientMetricsCollector>);

impl prometheus_client::collector::Collector for SharedClientMetricsCollector {
    fn encode(
        &self,
        encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        self.0.encode(encoder)
    }
}

/// Prometheus metric names allow `[a-zA-Z0-9_:]`. This function maps every
/// other character to `_`, and adds the prefix `crabka_client_`.
fn sanitize(metric: &str) -> String {
    let body: String = metric
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("crabka_client_{body}")
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn ingest_then_encode_contains_series() {
        use prometheus_client::registry::Registry;
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[DataPoint {
            metric: "org.apache.kafka.consumer.fetch.size".into(),
            client_instance_id: "11111111-1111-1111-1111-111111111111".into(),
            client_id: "svc-1".into(),
            value: 42.0,
        }]);
        let mut reg = Registry::default();
        reg.register_collector(Box::new(sink));
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &reg).unwrap();
        assert!(
            buf.contains("client_instance_id=\"11111111-1111-1111-1111-111111111111\""),
            "got:\n{buf}"
        );
        assert!(buf.contains("42"), "value missing:\n{buf}");
    }

    #[test]
    fn multiple_series_same_metric_encode_once() {
        use prometheus_client::registry::Registry;
        let sink = ClientMetricsCollector::new(std::time::Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "org.apache.kafka.consumer.fetch.size".into(),
                client_instance_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
                client_id: "c1".into(),
                value: 1.0,
            },
            DataPoint {
                metric: "org.apache.kafka.consumer.fetch.size".into(),
                client_instance_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
                client_id: "c2".into(),
                value: 2.0,
            },
        ]);
        let mut reg = Registry::default();
        reg.register_collector(Box::new(sink));
        let mut buf = String::new();
        // Must succeed (no duplicate-descriptor parse error) ...
        prometheus_client::encoding::text::encode(&mut buf, &reg).expect("encode");
        // ... and emit exactly ONE HELP line for the metric name.
        let help_count = buf
            .matches("# HELP crabka_client_org_apache_kafka_consumer_fetch_size")
            .count();
        assert!(
            help_count == 1,
            "expected exactly one HELP line, got {help_count}:\n{buf}"
        );
        // Both series present.
        assert!(
            buf.contains("c1") && buf.contains("c2"),
            "both series must render:\n{buf}"
        );
    }

    #[test]
    fn stale_points_evicted_on_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_millis(0));
        sink.ingest(&[DataPoint {
            metric: "m".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            value: 1.0,
        }]);
        assert_eq!(sink.live_point_count(), 0);
    }
}
