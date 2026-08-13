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
    encoding::{DescriptorEncoder, EncodeMetric, MetricEncoder},
    metrics::{MetricType, counter::ConstCounter, gauge::ConstGauge},
};

#[derive(Debug, Clone)]
pub(crate) enum PointValue {
    Gauge(f64),
    Counter(f64),
    Histogram {
        count: u64,
        sum: f64,
        buckets: Vec<(f64, u64)>,
    },
}

impl PointValue {
    fn accumulate(&mut self, delta: &Self) -> bool {
        match (self, delta) {
            (Self::Gauge(total), Self::Gauge(value))
            | (Self::Counter(total), Self::Counter(value)) => {
                *total += *value;
                true
            }
            (
                Self::Histogram {
                    count,
                    sum,
                    buckets,
                },
                Self::Histogram {
                    count: delta_count,
                    sum: delta_sum,
                    buckets: delta_buckets,
                },
            ) if buckets
                .iter()
                .map(|(bound, _)| bound)
                .eq(delta_buckets.iter().map(|(bound, _)| bound)) =>
            {
                *count = count.saturating_add(*delta_count);
                *sum += *delta_sum;
                for ((_, count), (_, delta_count)) in buckets.iter_mut().zip(delta_buckets) {
                    *count = count.saturating_add(*delta_count);
                }
                true
            }
            _ => false,
        }
    }

    fn same_type(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Gauge(_), Self::Gauge(_))
                | (Self::Counter(_), Self::Counter(_))
                | (Self::Histogram { .. }, Self::Histogram { .. })
        )
    }

    fn metric_type(&self) -> MetricType {
        match self {
            Self::Gauge(_) => MetricType::Gauge,
            Self::Counter(_) => MetricType::Counter,
            Self::Histogram { .. } => MetricType::Histogram,
        }
    }

    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        match self {
            Self::Gauge(value) => ConstGauge::new(*value).encode(encoder),
            Self::Counter(value) => ConstCounter::new(*value).encode(encoder),
            Self::Histogram {
                count,
                sum,
                buckets,
            } => {
                let mut encoder = encoder;
                encoder.encode_histogram::<[(&str, &str); 0]>(*sum, *count, buckets, None)
            }
        }
    }
}

/// A single decoded client metric data point destined for Prometheus.
#[derive(Debug, Clone)]
pub(crate) struct DataPoint {
    pub metric: String,
    pub client_instance_id: String,
    pub client_id: String,
    pub attributes: Vec<(String, String)>,
    pub value: PointValue,
    pub delta_start: Option<u64>,
}

#[derive(Debug)]
struct StoredPoint {
    attributes: Vec<(String, String)>,
    value: PointValue,
    delta_start: Option<u64>,
    at: Instant,
}

type SeriesKey = (String, String, String, Vec<(String, String)>);

#[derive(Debug)]
pub(crate) struct ClientMetricsCollector {
    points: Mutex<HashMap<SeriesKey, StoredPoint>>,
    ttl: Duration,
}

impl ClientMetricsCollector {
    fn is_live(age: Duration, ttl: Duration) -> bool {
        age < ttl
    }

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
        if self.ttl.is_zero() {
            guard.clear();
            return;
        }
        for p in points {
            let key = (
                p.metric.clone(),
                p.client_instance_id.clone(),
                p.client_id.clone(),
                p.attributes.clone(),
            );
            if let Some(start) = p.delta_start
                && let Some(stored) = guard.get_mut(&key)
                && stored
                    .delta_start
                    .is_some_and(|previous| previous == 0 || start == 0 || previous == start)
                && stored.value.accumulate(&p.value)
            {
                stored.delta_start = Some(start);
                stored.at = now;
                continue;
            }
            guard.insert(
                key,
                StoredPoint {
                    attributes: p.attributes.clone(),
                    value: p.value.clone(),
                    delta_start: p.delta_start,
                    at: now,
                },
            );
        }
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
    }

    /// The count of points that are not stale. This method also removes the
    /// stale entries in place.
    #[cfg(test)]
    pub(crate) fn live_point_count(&self) -> usize {
        let now = Instant::now();
        let mut guard = self.points.lock().expect("prom sink mutex poisoned");
        guard.retain(|_, sp| Self::is_live(now.duration_since(sp.at), self.ttl));
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
        let mut by_name: HashMap<String, Vec<(&str, &str, &StoredPoint)>> = HashMap::new();
        for ((metric, instance, client, _), sp) in guard.iter() {
            if !Self::is_live(now.duration_since(sp.at), self.ttl) {
                continue;
            }
            by_name.entry(sanitize(metric)).or_default().push((
                instance.as_str(),
                client.as_str(),
                sp,
            ));
        }

        for (name, series) in &by_name {
            let Some((_, _, first)) = series.first() else {
                continue;
            };
            let mut metric_encoder = encoder.encode_descriptor(
                name,
                "client-reported metric (KIP-714)",
                None,
                first.value.metric_type(),
            )?;
            for (instance, client, point) in series {
                if !point.value.same_type(&first.value) {
                    continue;
                }
                let mut labels = vec![
                    ("client_instance_id".to_string(), (*instance).to_string()),
                    ("client_id".to_string(), (*client).to_string()),
                ];
                labels.extend(point.attributes.clone());
                let family_encoder = metric_encoder.encode_family(&labels)?;
                point.value.encode(family_encoder)?;
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

    fn encode_collector(collector: impl Collector + 'static) -> String {
        use prometheus_client::registry::Registry;
        let mut registry = Registry::default();
        registry.register_collector(Box::new(collector));
        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
        output
    }

    #[test]
    fn ingest_then_encode_contains_series() {
        use prometheus_client::registry::Registry;
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[DataPoint {
            metric: "org.apache.kafka.consumer.fetch.size".into(),
            client_instance_id: "11111111-1111-1111-1111-111111111111".into(),
            client_id: "svc-1".into(),
            attributes: vec![("rack".into(), "a".into())],
            value: PointValue::Gauge(42.0),
            delta_start: None,
        }]);
        let mut reg = Registry::default();
        reg.register_collector(Box::new(sink));
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &reg).unwrap();
        assert!(
            buf.contains("client_instance_id=\"11111111-1111-1111-1111-111111111111\""),
            "got:\n{buf}"
        );
        assert!(
            buf.contains("rack=\"a\""),
            "attribute label missing:\n{buf}"
        );
        assert!(
            buf.contains("# TYPE crabka_client_org_apache_kafka_consumer_fetch_size gauge"),
            "gauge type missing:\n{buf}"
        );
        assert!(buf.contains("42"), "value missing:\n{buf}");
    }

    #[test]
    fn counter_and_histogram_keep_their_prometheus_types() {
        use prometheus_client::registry::Registry;
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "requests".into(),
                client_instance_id: "i".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Counter(7.0),
                delta_start: None,
            },
            DataPoint {
                metric: "latency".into(),
                client_instance_id: "i".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Histogram {
                    count: 3,
                    sum: 9.5,
                    buckets: vec![(1.0, 1), (5.0, 2), (f64::INFINITY, 3)],
                },
                delta_start: None,
            },
        ]);
        let mut registry = Registry::default();
        registry.register_collector(Box::new(sink));
        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();

        assert!(
            output.contains("# TYPE crabka_client_requests counter"),
            "{output}"
        );
        assert!(
            output.contains("# TYPE crabka_client_latency histogram"),
            "{output}"
        );
        assert!(output.contains("crabka_client_latency_count"), "{output}");
        assert!(output.contains("crabka_client_latency_sum"), "{output}");
        assert!(output.contains("le=\"5.0\""), "{output}");
    }

    #[test]
    fn delta_points_accumulate_per_series() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        for (counter, count, sum, buckets) in [
            (5.0, 2, 4.0, vec![(1.0, 1), (f64::MAX, 1)]),
            (3.0, 3, 6.0, vec![(1.0, 2), (f64::MAX, 1)]),
        ] {
            sink.ingest(&[
                DataPoint {
                    metric: "requests".into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                    value: PointValue::Counter(counter),
                    delta_start: Some(7),
                },
                DataPoint {
                    metric: "latency".into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                    value: PointValue::Histogram {
                        count,
                        sum,
                        buckets,
                    },
                    delta_start: Some(7),
                },
            ]);
        }
        for (metric, first_start, second_start, expected) in [
            ("unknown_previous", 0, 8, 8.0),
            ("unknown_current", 8, 0, 8.0),
            ("reset", 7, 8, 3.0),
        ] {
            for (value, start) in [(5.0, first_start), (3.0, second_start)] {
                sink.ingest(&[DataPoint {
                    metric: metric.into(),
                    client_instance_id: "i".into(),
                    client_id: "c".into(),
                    attributes: vec![],
                    value: PointValue::Counter(value),
                    delta_start: Some(start),
                }]);
            }
            let guard = sink.points.lock().unwrap();
            let point = guard
                .get(&(metric.into(), "i".into(), "c".into(), vec![]))
                .unwrap();
            assert!(
                matches!(point.value, PointValue::Counter(value) if (value - expected).abs() < f64::EPSILON)
            );
        }

        let guard = sink.points.lock().unwrap();
        assert!(guard.values().any(
            |point| matches!(point.value, PointValue::Counter(value) if (value - 8.0).abs() < f64::EPSILON)
        ));
        assert!(guard.values().any(|point| matches!(
            &point.value,
            PointValue::Histogram { count: 5, sum, buckets }
                if (*sum - 10.0).abs() < f64::EPSILON
                    && buckets.as_slice() == [(1.0, 3), (f64::MAX, 2)]
        )));

        let mut total = PointValue::Histogram {
            count: 2,
            sum: 4.0,
            buckets: vec![(1.0, 1), (f64::MAX, 1)],
        };
        assert!(!total.accumulate(&PointValue::Histogram {
            count: 3,
            sum: 6.0,
            buckets: vec![(2.0, 2), (f64::MAX, 1)],
        }));
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
                attributes: vec![],
                value: PointValue::Gauge(1.0),
                delta_start: None,
            },
            DataPoint {
                metric: "org.apache.kafka.consumer.fetch.size".into(),
                client_instance_id: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into(),
                client_id: "c2".into(),
                attributes: vec![],
                value: PointValue::Gauge(2.0),
                delta_start: None,
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
            attributes: vec![],
            value: PointValue::Gauge(1.0),
            delta_start: None,
        }]);
        assert_eq!(sink.live_point_count(), 0);
        assert!(ClientMetricsCollector::is_live(
            Duration::from_nanos(9),
            Duration::from_nanos(10)
        ));
        assert!(!ClientMetricsCollector::is_live(
            Duration::from_nanos(10),
            Duration::from_nanos(10)
        ));
    }

    #[test]
    fn mixed_types_with_one_sanitized_name_do_not_cross_encode() {
        let sink = ClientMetricsCollector::new(Duration::from_mins(1));
        sink.ingest(&[
            DataPoint {
                metric: "same.name".into(),
                client_instance_id: "gauge".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Gauge(1.0),
                delta_start: None,
            },
            DataPoint {
                metric: "same-name".into(),
                client_instance_id: "counter".into(),
                client_id: "c".into(),
                attributes: vec![],
                value: PointValue::Counter(2.0),
                delta_start: None,
            },
        ]);

        let output = encode_collector(sink);
        assert!(
            output.contains("client_instance_id=\"gauge\"")
                ^ output.contains("client_instance_id=\"counter\""),
            "{output}"
        );
    }

    #[test]
    fn shared_wrapper_delegates_encoding_and_sanitize_preserves_valid_punctuation() {
        let sink = std::sync::Arc::new(ClientMetricsCollector::new(Duration::from_mins(1)));
        sink.ingest(&[DataPoint {
            metric: "valid_name:total-bad".into(),
            client_instance_id: "i".into(),
            client_id: "c".into(),
            attributes: vec![],
            value: PointValue::Gauge(3.0),
            delta_start: None,
        }]);

        let output = encode_collector(SharedClientMetricsCollector(sink));
        assert!(
            output.contains("crabka_client_valid_name:total_bad"),
            "{output}"
        );
    }
}
