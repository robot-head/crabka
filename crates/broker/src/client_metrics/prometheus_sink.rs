//! Prometheus sink for KIP-714 client metrics. Client metric *names* are
//! dynamic, so we register a custom `Collector` (rather than static
//! `Family`s) that renders a live, staleness-pruned snapshot at scrape time
//! as `crabka_client_*` series.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::gauge::ConstGauge;

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

    /// Record/replace the latest value for each point and prune stale ones.
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

    /// Count of non-stale points (also prunes stale entries in place).
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
        for ((metric, instance, client), sp) in guard.iter() {
            if now.duration_since(sp.at) >= self.ttl {
                continue;
            }
            let name = sanitize(metric);
            let gauge = ConstGauge::new(sp.value);
            let mut metric_encoder = encoder.encode_descriptor(
                &name,
                "client-reported metric (KIP-714)",
                None,
                MetricType::Gauge,
            )?;
            let labels = [
                ("client_instance_id", instance.as_str()),
                ("client_id", client.as_str()),
            ];
            let family_encoder = metric_encoder.encode_family(&labels)?;
            gauge.encode(family_encoder)?;
        }
        Ok(())
    }
}

/// Prometheus metric names allow `[a-zA-Z0-9_:]`; map everything else to `_`
/// and prefix with `crabka_client_`.
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
    use super::*;
    use std::time::Duration;

    #[test]
    fn ingest_then_encode_contains_series() {
        let sink = ClientMetricsCollector::new(Duration::from_secs(60));
        sink.ingest(&[DataPoint {
            metric: "org.apache.kafka.consumer.fetch.size".into(),
            client_instance_id: "11111111-1111-1111-1111-111111111111".into(),
            client_id: "svc-1".into(),
            value: 42.0,
        }]);
        use prometheus_client::registry::Registry;
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
