//! Service graph edge pairing processor.

use std::collections::HashMap;

use crate::metricsgen::config::MetricsGenConfig;
use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
use crate::metricsgen::series::{Series, SeriesSample, sorted_labels};

const NS_PER_SEC: f64 = 1_000_000_000.0;

type EdgeKey = ([u8; 16], [u8; 8]);
type LabelKey = (String, String, ConnectionType);

/// Tempo service-graph connection classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConnectionType {
    Unset,
    VirtualNode,
    MessagingSystem,
    Database,
}

impl ConnectionType {
    #[must_use]
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::VirtualNode => "virtual_node",
            Self::MessagingSystem => "messaging_system",
            Self::Database => "database",
        }
    }
}

/// Result of recording one span into the edge store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    Completed,
    Dropped,
    Ignored,
}

/// A half-edge until both client and server sides arrive.
#[derive(Clone, Debug)]
pub struct Edge {
    pub client_service: Option<String>,
    pub server_service: Option<String>,
    pub client_latency_ns: Option<i64>,
    pub server_latency_ns: Option<i64>,
    pub failed: bool,
    pub connection_type: ConnectionType,
    pub first_seen_ns: i64,
}

#[derive(Clone, Debug, Default)]
struct EdgeAgg {
    requests: f64,
    failed: f64,
    client_seconds_sum: f64,
    client_seconds_count: f64,
    server_seconds_sum: f64,
    server_seconds_count: f64,
    messaging_seconds_sum: f64,
    messaging_seconds_count: f64,
}

/// Bounded, TTL'd service-graph edge store.
#[derive(Debug)]
pub struct EdgeStore {
    max_items: usize,
    ttl_ns: i64,
    enable_messaging_latency: bool,
    edges: HashMap<EdgeKey, Edge>,
    aggregates: HashMap<LabelKey, EdgeAgg>,
    unpaired: HashMap<LabelKey, f64>,
    dropped: HashMap<LabelKey, f64>,
}

impl EdgeStore {
    #[must_use]
    pub fn new(cfg: &MetricsGenConfig) -> Self {
        Self {
            max_items: cfg.edge_store_max_items,
            ttl_ns: i64::try_from(cfg.edge_ttl.as_nanos()).unwrap_or(i64::MAX),
            enable_messaging_latency: cfg.enable_messaging_system_latency,
            edges: HashMap::new(),
            aggregates: HashMap::new(),
            unpaired: HashMap::new(),
            dropped: HashMap::new(),
        }
    }

    pub fn record_span(&mut self, span: &SpanRecord, now_ns: i64) -> RecordOutcome {
        let Some(is_client) = edge_side(span.kind) else {
            return RecordOutcome::Ignored;
        };

        let Some(key) = edge_key(span) else {
            return RecordOutcome::Ignored;
        };
        self.expire(now_ns);

        let connection_type = classify(span);
        let failed = span.status == StatusCode::Error;
        let latency_ns = span.duration_ns.max(0);

        if let Some(edge) = self.edges.get_mut(&key) {
            fill_edge(edge, span, is_client, latency_ns);
            edge.failed |= failed;
            if connection_type != ConnectionType::Unset {
                edge.connection_type = connection_type;
            }
            if edge.client_service.is_some() && edge.server_service.is_some() {
                let edge = self.edges.remove(&key).expect("edge exists after get_mut");
                self.complete(edge);
                return RecordOutcome::Completed;
            }
            return RecordOutcome::Recorded;
        }

        if self.edges.len() >= self.max_items {
            *self
                .dropped
                .entry(label_key_for_span(span, is_client, connection_type))
                .or_insert(0.0) += 1.0;
            return RecordOutcome::Dropped;
        }

        let mut edge = Edge {
            client_service: None,
            server_service: None,
            client_latency_ns: None,
            server_latency_ns: None,
            failed,
            connection_type,
            first_seen_ns: now_ns,
        };
        fill_edge(&mut edge, span, is_client, latency_ns);
        self.edges.insert(key, edge);
        RecordOutcome::Recorded
    }

    pub fn expire(&mut self, now_ns: i64) -> usize {
        let expired: Vec<_> = self
            .edges
            .iter()
            .filter(|(_, edge)| now_ns.saturating_sub(edge.first_seen_ns) >= self.ttl_ns)
            .map(|(key, _)| *key)
            .collect();

        for key in &expired {
            let edge = self.edges.remove(key).expect("expired key exists");
            *self
                .unpaired
                .entry(label_key_for_edge(&edge))
                .or_insert(0.0) += 1.0;
        }

        expired.len()
    }

    #[must_use]
    pub fn drain(&mut self, timestamp_ms: i64) -> Vec<Series> {
        let mut out = Vec::new();

        for ((client, server, connection_type), agg) in self.aggregates.drain() {
            let labels = sorted_labels(vec![
                ("client".to_string(), client),
                ("server".to_string(), server),
                (
                    "connection_type".to_string(),
                    connection_type.as_label().to_string(),
                ),
            ]);
            out.push(counter(
                "traces_service_graph_request_total",
                &labels,
                agg.requests,
                timestamp_ms,
            ));
            out.push(counter(
                "traces_service_graph_request_failed_total",
                &labels,
                agg.failed,
                timestamp_ms,
            ));
            push_histogram(
                &mut out,
                "traces_service_graph_request_client_seconds",
                &labels,
                agg.client_seconds_sum,
                agg.client_seconds_count,
                timestamp_ms,
            );
            push_histogram(
                &mut out,
                "traces_service_graph_request_server_seconds",
                &labels,
                agg.server_seconds_sum,
                agg.server_seconds_count,
                timestamp_ms,
            );
            if self.enable_messaging_latency {
                push_histogram(
                    &mut out,
                    "traces_service_graph_request_messaging_system_seconds",
                    &labels,
                    agg.messaging_seconds_sum,
                    agg.messaging_seconds_count,
                    timestamp_ms,
                );
            }
        }

        for (label_key, value) in self.unpaired.drain() {
            let labels = service_graph_labels(label_key);
            out.push(counter(
                "traces_service_graph_unpaired_spans_total",
                &labels,
                value,
                timestamp_ms,
            ));
        }

        for (label_key, value) in self.dropped.drain() {
            let labels = service_graph_labels(label_key);
            out.push(counter(
                "traces_service_graph_dropped_spans_total",
                &labels,
                value,
                timestamp_ms,
            ));
        }

        out
    }

    fn complete(&mut self, edge: Edge) {
        let client = edge.client_service.unwrap_or_default();
        let server = edge.server_service.unwrap_or_default();
        let agg = self
            .aggregates
            .entry((client, server, edge.connection_type))
            .or_default();
        agg.requests += 1.0;
        if edge.failed {
            agg.failed += 1.0;
        }
        if let Some(ns) = edge.client_latency_ns {
            agg.client_seconds_sum += ns_to_seconds(ns);
            agg.client_seconds_count += 1.0;
        }
        if let Some(ns) = edge.server_latency_ns {
            agg.server_seconds_sum += ns_to_seconds(ns);
            agg.server_seconds_count += 1.0;
        }
        if self.enable_messaging_latency
            && edge.connection_type == ConnectionType::MessagingSystem
            && let Some(ns) = edge.server_latency_ns.or(edge.client_latency_ns)
        {
            agg.messaging_seconds_sum += ns_to_seconds(ns);
            agg.messaging_seconds_count += 1.0;
        }
    }
}

fn service_graph_labels((client, server, connection_type): LabelKey) -> Vec<(String, String)> {
    sorted_labels(vec![
        ("client".to_string(), client),
        ("server".to_string(), server),
        (
            "connection_type".to_string(),
            connection_type.as_label().to_string(),
        ),
    ])
}

fn label_key_for_edge(edge: &Edge) -> LabelKey {
    (
        edge.client_service.clone().unwrap_or_default(),
        edge.server_service.clone().unwrap_or_default(),
        edge.connection_type,
    )
}

fn label_key_for_span(
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
) -> LabelKey {
    if is_client {
        (span.service_name.clone(), String::new(), connection_type)
    } else {
        (String::new(), span.service_name.clone(), connection_type)
    }
}

fn edge_key(span: &SpanRecord) -> Option<EdgeKey> {
    match span.kind {
        SpanKind::Client | SpanKind::Producer => Some((span.trace_id, span.span_id)),
        SpanKind::Server | SpanKind::Consumer if span.parent_span_id != [0; 8] => {
            Some((span.trace_id, span.parent_span_id))
        }
        _ => None,
    }
}

fn edge_side(kind: SpanKind) -> Option<bool> {
    match kind {
        SpanKind::Client | SpanKind::Producer => Some(true),
        SpanKind::Server | SpanKind::Consumer => Some(false),
        SpanKind::Unspecified | SpanKind::Internal => None,
    }
}

fn fill_edge(edge: &mut Edge, span: &SpanRecord, is_client: bool, latency_ns: i64) {
    if is_client {
        edge.client_service = Some(span.service_name.clone());
        edge.client_latency_ns = Some(latency_ns);
    } else {
        edge.server_service = Some(span.service_name.clone());
        edge.server_latency_ns = Some(latency_ns);
    }
}

fn counter(name: &str, labels: &[(String, String)], value: f64, timestamp_ms: i64) -> Series {
    Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::Counter(value),
        exemplars: Vec::new(),
        timestamp_ms,
    }
}

fn push_histogram(
    out: &mut Vec<Series>,
    name: &str,
    labels: &[(String, String)],
    sum: f64,
    count: f64,
    timestamp_ms: i64,
) {
    out.push(Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::ClassicHistogram {
            buckets: Vec::new(),
            sum,
            count,
        },
        exemplars: Vec::new(),
        timestamp_ms,
    });
}

fn classify(span: &SpanRecord) -> ConnectionType {
    if has_attr(span, "db.system") {
        ConnectionType::Database
    } else if has_attr(span, "messaging.system") {
        ConnectionType::MessagingSystem
    } else if has_attr(span, "peer.service") {
        ConnectionType::VirtualNode
    } else {
        ConnectionType::Unset
    }
}

fn has_attr(span: &SpanRecord, name: &str) -> bool {
    span.attributes.iter().any(|(key, _)| key == name)
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Prometheus latency samples are f64 seconds on the output edge"
)]
fn ns_to_seconds(ns: i64) -> f64 {
    ns.max(0) as f64 / NS_PER_SEC
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::metricsgen::config::MetricsGenConfig;
    use crate::metricsgen::contract::{SpanKind, SpanRecord, StatusCode};
    use crate::metricsgen::series::{Series, SeriesSample};

    fn span(
        service: &str,
        span_id: [u8; 8],
        parent: [u8; 8],
        kind: SpanKind,
        status: StatusCode,
        dur_ns: i64,
    ) -> SpanRecord {
        SpanRecord {
            tenant: "t".into(),
            trace_id: [0x11; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: dur_ns,
            status,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            size_bytes: 0,
        }
    }

    fn counter(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::Counter(c) => c,
                _ => panic!("{name} not a counter"),
            })
    }

    fn labels_for<'a>(series: &'a [Series], name: &str) -> &'a [(String, String)] {
        &series.iter().find(|s| s.name == name).unwrap().labels
    }

    fn histogram_sum(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::ClassicHistogram { sum, .. } => sum,
                _ => panic!("{name} not a histogram"),
            })
    }

    fn histogram_count(series: &[Series], name: &str) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match s.sample {
                SeriesSample::ClassicHistogram { count, .. } => count,
                _ => panic!("{name} not a histogram"),
            })
    }

    #[test]
    fn pairs_client_then_server_into_one_request() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            10_000_000,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            8_000_000,
        );

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_request_total") - 1.0).abs() < 1e-9);
        assert!(counter(&out, "traces_service_graph_request_failed_total").abs() < 1e-9);

        let req = out
            .iter()
            .find(|s| s.name == "traces_service_graph_request_total")
            .unwrap();
        assert!(
            req.labels
                .iter()
                .any(|(k, v)| k == "client" && v == "frontend")
        );
        assert!(
            req.labels
                .iter()
                .any(|(k, v)| k == "server" && v == "backend")
        );
        assert!(
            req.labels
                .iter()
                .any(|(k, v)| k == "connection_type" && v == "unset")
        );
        assert!(
            (histogram_sum(&out, "traces_service_graph_request_client_seconds") - 0.010).abs()
                < 1e-9
        );
        assert!(
            (histogram_sum(&out, "traces_service_graph_request_server_seconds") - 0.008).abs()
                < 1e-9
        );
    }

    #[test]
    fn unset_connection_type_is_labeled_explicitly() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            1,
        );

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert!(
            labels
                .iter()
                .any(|(k, v)| k == "connection_type" && v == "unset")
        );
    }

    #[test]
    fn failed_when_either_side_errors() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Error,
            1,
        );

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&server, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_request_failed_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn unpaired_half_edge_expires_after_ttl() {
        let cfg = MetricsGenConfig {
            edge_ttl: Duration::from_secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);

        assert!(store.expire(5_000_000_000) == 0);
        assert!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn unpaired_client_span_keeps_service_graph_labels() {
        let cfg = MetricsGenConfig {
            edge_ttl: Duration::from_secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.expire(10_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert!(
            labels
                == [
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "unset".to_string()),
                    ("server".to_string(), String::new()),
                ]
        );
    }

    #[test]
    fn store_full_drops_new_spans() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let a = span("s1", [0x1; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let b = span("s2", [0x2; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);

        assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_dropped_spans_total") - 1.0).abs() < 1e-9);
    }

    #[test]
    fn expired_half_edges_do_not_consume_store_capacity() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            edge_ttl: Duration::from_secs(10),
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let stale = span(
            "stale-client",
            [0x1; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        let fresh = span(
            "fresh-client",
            [0x2; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );

        assert!(store.record_span(&stale, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&fresh, 10_000_000_000) == RecordOutcome::Recorded);

        let out = store.drain(1_000);
        assert!((counter(&out, "traces_service_graph_unpaired_spans_total") - 1.0).abs() < 1e-9);
        assert!(counter(&out, "traces_service_graph_dropped_spans_total").abs() < 1e-9);
    }

    #[test]
    fn dropped_client_span_keeps_service_graph_labels() {
        let cfg = MetricsGenConfig {
            edge_store_max_items: 1,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let a = span("s1", [0x1; 8], [0; 8], SpanKind::Client, StatusCode::Ok, 1);
        let mut b = span(
            "database",
            [0x2; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        b.attributes.push(("db.system".into(), "postgresql".into()));

        assert!(store.record_span(&a, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&b, 1) == RecordOutcome::Dropped);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_dropped_spans_total");
        assert!(
            labels
                == [
                    ("client".to_string(), "database".to_string()),
                    ("connection_type".to_string(), "database".to_string()),
                    ("server".to_string(), String::new()),
                ]
        );
    }

    #[test]
    fn non_client_server_spans_ignored() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let internal = span("s", [0x1; 8], [0; 8], SpanKind::Internal, StatusCode::Ok, 1);

        assert!(store.record_span(&internal, 0) == RecordOutcome::Ignored);
    }

    #[test]
    fn database_connection_type_from_db_system_attr() {
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let mut client = span(
            "frontend",
            [0xA; 8],
            [0; 8],
            SpanKind::Client,
            StatusCode::Ok,
            1,
        );
        client
            .attributes
            .push(("db.system".into(), "postgresql".into()));

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.expire(20_000_000_000) == 1);

        let out = store.drain(1_000);
        let unpaired = out
            .iter()
            .find(|s| s.name == "traces_service_graph_unpaired_spans_total")
            .unwrap();
        assert!(
            unpaired
                .labels
                .iter()
                .any(|(k, v)| k == "connection_type" && v == "database")
        );
    }

    #[test]
    fn messaging_producer_consumer_pair_emits_service_graph_edge() {
        let cfg = MetricsGenConfig {
            enable_messaging_system_latency: true,
            ..MetricsGenConfig::default()
        };
        let mut store = EdgeStore::new(&cfg);
        let mut producer = span(
            "publisher",
            [0xA; 8],
            [0; 8],
            SpanKind::Producer,
            StatusCode::Ok,
            7_000_000,
        );
        producer
            .attributes
            .push(("messaging.system".into(), "kafka".into()));
        let mut consumer = span(
            "worker",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Consumer,
            StatusCode::Ok,
            5_000_000,
        );
        consumer
            .attributes
            .push(("messaging.system".into(), "kafka".into()));

        assert!(store.record_span(&producer, 0) == RecordOutcome::Recorded);
        assert!(store.record_span(&consumer, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        assert!(
            labels
                == [
                    ("client".to_string(), "publisher".to_string()),
                    (
                        "connection_type".to_string(),
                        "messaging_system".to_string(),
                    ),
                    ("server".to_string(), "worker".to_string()),
                ]
        );
        assert!(
            (histogram_sum(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 0.005)
                .abs()
                < 1e-9
        );
        assert!(
            (histogram_count(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 1.0)
                .abs()
                < 1e-9
        );
    }
}
