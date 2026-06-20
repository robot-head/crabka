//! Span-metrics RED processor.

use std::collections::{HashMap, HashSet};

use crate::metricsgen::config::MetricsGenConfig;
use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
use crate::metricsgen::series::{Exemplar, Series, SeriesSample, sorted_labels};

const NS_PER_SEC: f64 = 1_000_000_000.0;

type DimKey = (String, String, String, String, Option<String>);

#[derive(Clone, Debug)]
struct LatencyHistogram {
    bucket_edges_ns: Vec<f64>,
    bucket_counts: Vec<u64>,
    sum_ns: f64,
    count: u64,
}

impl LatencyHistogram {
    fn new(edges_ns: &[f64]) -> Self {
        Self {
            bucket_edges_ns: edges_ns.to_vec(),
            bucket_counts: vec![0; edges_ns.len() + 1],
            sum_ns: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, value_ns: f64) {
        let idx = self
            .bucket_edges_ns
            .iter()
            .position(|&edge| value_ns <= edge)
            .unwrap_or(self.bucket_edges_ns.len());
        self.bucket_counts[idx] += 1;
        self.sum_ns += value_ns;
        self.count += 1;
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "Prometheus histogram samples are f64 values on the output edge"
    )]
    fn cumulative_seconds(&self) -> (Vec<(f64, f64)>, f64, f64) {
        let mut cumulative = 0_u64;
        let buckets = self
            .bucket_edges_ns
            .iter()
            .enumerate()
            .map(|(i, edge_ns)| {
                cumulative += self.bucket_counts[i];
                (*edge_ns / NS_PER_SEC, cumulative as f64)
            })
            .collect();

        (buckets, self.sum_ns / NS_PER_SEC, self.count as f64)
    }
}

#[derive(Clone, Debug)]
struct DimEntry {
    calls: f64,
    size_total: f64,
    latency: LatencyHistogram,
    exemplars: Vec<Exemplar>,
}

/// Pure fold that derives span-metrics RED series from spans.
#[derive(Debug)]
pub struct SpanMetricsRegistry {
    bucket_edges_ns: Vec<f64>,
    max_exemplars: usize,
    enable_target_info: bool,
    enable_status_message: bool,
    entries: HashMap<DimKey, DimEntry>,
    services: HashSet<String>,
}

impl SpanMetricsRegistry {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
            max_exemplars: cfg.max_exemplars_per_series,
            enable_target_info: cfg.enable_target_info,
            enable_status_message: cfg.enable_status_message,
            entries: HashMap::new(),
            services: HashSet::new(),
        }
    }

    pub fn record_span(&mut self, span: &SpanRecord) {
        let key = dim_key(span, self.enable_status_message);
        self.services.insert(span.service_name.clone());
        let bucket_edges_ns = self.bucket_edges_ns.clone();
        let entry = self.entries.entry(key).or_insert_with(|| DimEntry {
            calls: 0.0,
            size_total: 0.0,
            latency: LatencyHistogram::new(&bucket_edges_ns),
            exemplars: Vec::new(),
        });

        entry.calls += 1.0;
        entry.size_total += size_as_f64(span.size_bytes);
        let duration_ns = duration_as_f64(span.duration_ns);
        entry.latency.observe(duration_ns);

        if entry.exemplars.len() < self.max_exemplars {
            entry.exemplars.push(Exemplar {
                value: duration_ns / NS_PER_SEC,
                labels: sorted_labels(vec![
                    ("trace_id".to_string(), hex::encode(span.trace_id)),
                    ("span_id".to_string(), hex::encode(span.span_id)),
                ]),
                timestamp_ms: span.start_ns / 1_000_000,
            });
        }
    }

    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let entries = std::mem::take(&mut self.entries);
        let services = std::mem::take(&mut self.services);
        let mut series = Vec::with_capacity(entries.len() * 3 + services.len());

        for ((service, span_name, span_kind, status_code, status_message), entry) in entries {
            let mut labels = vec![
                ("service".to_string(), service),
                ("span_name".to_string(), span_name),
                ("span_kind".to_string(), span_kind),
                ("status_code".to_string(), status_code),
            ];
            if let Some(status_message) = status_message {
                labels.push(("status_message".to_string(), status_message));
            }
            let labels = sorted_labels(labels);
            series.push(Series {
                name: "traces_spanmetrics_calls_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.calls),
                exemplars: Vec::new(),
                timestamp_ms,
            });
            series.push(Series {
                name: "traces_spanmetrics_size_total".to_string(),
                labels: labels.clone(),
                sample: SeriesSample::Counter(entry.size_total),
                exemplars: Vec::new(),
                timestamp_ms,
            });

            let (buckets, sum, count) = entry.latency.cumulative_seconds();
            series.push(Series {
                name: "traces_spanmetrics_latency".to_string(),
                labels,
                sample: SeriesSample::ClassicHistogram {
                    buckets,
                    sum,
                    count,
                },
                exemplars: entry.exemplars,
                timestamp_ms,
            });
        }

        if self.enable_target_info {
            series.extend(services.into_iter().map(|service| Series {
                name: "traces_target_info".to_string(),
                labels: sorted_labels(vec![("service".to_string(), service)]),
                sample: SeriesSample::Gauge(1.0),
                exemplars: Vec::new(),
                timestamp_ms,
            }));
        }

        series
    }
}

/// Dimension labels for the Tempo-compatible RED series identity.
#[must_use]
pub fn dimension_labels(span: &SpanRecord) -> Vec<(String, String)> {
    sorted_labels(vec![
        ("service".to_string(), span.service_name.clone()),
        ("span_name".to_string(), span.name.clone()),
        (
            "span_kind".to_string(),
            span_kind_dim(span.kind).to_string(),
        ),
        (
            "status_code".to_string(),
            status_dim(span.status).to_string(),
        ),
    ])
}

fn dim_key(span: &SpanRecord, include_status_message: bool) -> DimKey {
    (
        span.service_name.clone(),
        span.name.clone(),
        span_kind_dim(span.kind).to_string(),
        status_dim(span.status).to_string(),
        include_status_message.then(|| span.status_message.clone()),
    )
}

fn span_kind_dim(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Unspecified => "SPAN_KIND_UNSPECIFIED",
        SpanKind::Internal => "SPAN_KIND_INTERNAL",
        SpanKind::Server => "SPAN_KIND_SERVER",
        SpanKind::Client => "SPAN_KIND_CLIENT",
        SpanKind::Producer => "SPAN_KIND_PRODUCER",
        SpanKind::Consumer => "SPAN_KIND_CONSUMER",
    }
}

fn status_dim(status: StatusCode) -> &'static str {
    match status {
        StatusCode::Unset => "STATUS_CODE_UNSET",
        StatusCode::Ok => "STATUS_CODE_OK",
        StatusCode::Error => "STATUS_CODE_ERROR",
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Prometheus counter samples are f64 values on the output edge"
)]
fn size_as_f64(size_bytes: u64) -> f64 {
    size_bytes as f64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Prometheus latency samples are f64 seconds on the output edge"
)]
fn duration_as_f64(duration_ns: i64) -> f64 {
    duration_ns.max(0) as f64
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::series::{Series, SeriesSample};

    fn span(
        service: &str,
        name: &str,
        kind: SpanKind,
        status: StatusCode,
        dur_ns: i64,
        size: u64,
    ) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            parent_span_id: [0; 8],
            name: name.into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            size_bytes: size,
        }
    }

    fn span_with_status_message(message: &str) -> SpanRecord {
        SpanRecord {
            status_message: message.into(),
            ..span(
                "api",
                "GET /x",
                SpanKind::Server,
                StatusCode::Error,
                5_000_000,
                1,
            )
        }
    }

    fn find<'a>(series: &'a [Series], name: &str, span_name: &str) -> &'a Series {
        series
            .iter()
            .find(|s| {
                s.name == name
                    && s.labels
                        .iter()
                        .any(|(k, v)| k == "span_name" && v == span_name)
            })
            .unwrap_or_else(|| panic!("no {name} for {span_name}"))
    }

    #[test]
    fn red_counts_calls_and_size_per_dimension() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            100,
        ));
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            7_000_000,
            150,
        ));
        reg.record_span(&span(
            "api",
            "GET /y",
            SpanKind::Server,
            StatusCode::Error,
            3_000_000,
            50,
        ));

        let out = reg.drain(1_000);

        let calls_x = find(&out, "traces_spanmetrics_calls_total", "GET /x");
        assert!(matches!(calls_x.sample, SeriesSample::Counter(c) if (c - 2.0).abs() < 1e-9));
        let size_x = find(&out, "traces_spanmetrics_size_total", "GET /x");
        assert!(matches!(size_x.sample, SeriesSample::Counter(c) if (c - 250.0).abs() < 1e-9));

        let labels = &calls_x.labels;
        assert!(labels.iter().any(|(k, v)| k == "service" && v == "api"));
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == "span_kind" && v == "SPAN_KIND_SERVER")
        );
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == "status_code" && v == "STATUS_CODE_OK")
        );

        let calls_y = find(&out, "traces_spanmetrics_calls_total", "GET /y");
        assert!(matches!(calls_y.sample, SeriesSample::Counter(c) if (c - 1.0).abs() < 1e-9));
    }

    #[test]
    fn status_message_dimension_is_opt_in() {
        let cfg = MetricsGenConfig {
            enable_status_message: true,
            ..MetricsGenConfig::default()
        };
        let mut reg = SpanMetricsRegistry::new(&cfg);
        reg.record_span(&span_with_status_message("deadline exceeded"));

        let out = reg.drain(1_000);
        let calls = find(&out, "traces_spanmetrics_calls_total", "GET /x");

        assert!(
            calls
                .labels
                .iter()
                .any(|(k, v)| k == "status_message" && v == "deadline exceeded")
        );
    }

    #[test]
    fn latency_histogram_buckets_and_sum() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            7_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        match &lat.sample {
            SeriesSample::ClassicHistogram {
                buckets,
                sum,
                count,
            } => {
                assert!((*count - 2.0).abs() < 1e-9);
                assert!((*sum - 0.012).abs() < 1e-6);
                let le_8ms = buckets
                    .iter()
                    .find(|(le, _)| (*le - 0.008).abs() < 1e-9)
                    .unwrap();
                assert!((le_8ms.1 - 2.0).abs() < 1e-9);
                let le_4ms = buckets
                    .iter()
                    .find(|(le, _)| (*le - 0.004).abs() < 1e-9)
                    .unwrap();
                assert!(le_4ms.1.abs() < 1e-9);
            }
            other => panic!("expected ClassicHistogram, got {other:?}"),
        }
    }

    #[test]
    fn exemplar_carries_trace_id_when_enabled() {
        let cfg = MetricsGenConfig {
            max_exemplars_per_series: 2,
            ..MetricsGenConfig::default()
        };
        let mut reg = SpanMetricsRegistry::new(&cfg);
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert!(lat.exemplars.len() == 1);
        let ex = &lat.exemplars[0];
        assert!(
            ex.labels
                .iter()
                .any(|(k, v)| k == "trace_id" && v == "abababababababababababababababab")
        );
        assert!((ex.value - 0.005).abs() < 1e-6);
    }

    #[test]
    fn exemplars_off_by_default() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        let out = reg.drain(1_000);
        let lat = find(&out, "traces_spanmetrics_latency", "GET /x");
        assert!(lat.exemplars.is_empty());
    }

    #[test]
    fn drain_resets_accumulator() {
        let mut reg = SpanMetricsRegistry::new(&MetricsGenConfig::default());
        reg.record_span(&span(
            "api",
            "GET /x",
            SpanKind::Server,
            StatusCode::Ok,
            5_000_000,
            1,
        ));
        let _ = reg.drain(1_000);
        assert!(reg.drain(2_000).is_empty());
    }
}
