//! Service graph edge pairing processor.

use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use crate::metricsgen::{
    checkpoint::{CheckpointCodecError, encode_checkpoint_key, parse_checkpoint_key},
    config::MetricsGenConfig,
    contract::{SpanKind, SpanRecord, StatusCode},
    series::{Series, SeriesSample, sorted_labels},
};

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
    client_bucket_counts: Vec<u64>,
    server_seconds_sum: f64,
    server_seconds_count: f64,
    server_bucket_counts: Vec<u64>,
    messaging_seconds_sum: f64,
    messaging_seconds_count: f64,
    messaging_bucket_counts: Vec<u64>,
}

#[derive(Clone, Copy)]
struct HistogramSnapshot<'a> {
    sum: f64,
    count: f64,
    bucket_edges_ns: &'a [f64],
    bucket_counts: &'a [u64],
}

impl EdgeAgg {
    fn new(bucket_count: usize) -> Self {
        Self {
            client_bucket_counts: vec![0; bucket_count],
            server_bucket_counts: vec![0; bucket_count],
            messaging_bucket_counts: vec![0; bucket_count],
            ..Self::default()
        }
    }
}

/// Bounded, TTL'd service-graph edge store.
#[derive(Debug)]
pub struct EdgeStore {
    max_items: usize,
    ttl_ns: i64,
    enable_messaging_latency: bool,
    bucket_edges_ns: Vec<f64>,
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
            bucket_edges_ns: cfg.histogram_buckets_ns.clone(),
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
            // Backfill the peer.service virtual node on the update path too, so the
            // result is order-independent: an edge that transitions to (or already
            // is) VirtualNode gets its peer label set regardless of which span
            // carried the signal first.
            let edge_connection_type = edge.connection_type;
            fill_virtual_node(edge, span, is_client, edge_connection_type);
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
        fill_virtual_node(&mut edge, span, is_client, connection_type);
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
    pub fn checkpoint_entries(&self, tenant: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries: Vec<_> = self
            .edges
            .iter()
            .map(|((trace_id, edge_id), edge)| {
                (
                    encode_checkpoint_key(tenant, trace_id, edge_id).to_vec(),
                    encode_checkpoint_value(edge),
                )
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    pub fn restore_checkpoint_entry(
        &mut self,
        tenant: &str,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), CheckpointCodecError> {
        let (encoded_tenant, trace_id, edge_id) = parse_checkpoint_key(key)?;
        if encoded_tenant != tenant {
            return Ok(());
        }
        let edge_id: [u8; 8] = edge_id
            .try_into()
            .map_err(|_| CheckpointCodecError::BadEdgeId)?;
        let edge = decode_checkpoint_value(value)?;
        self.edges.insert((trace_id, edge_id), edge);
        Ok(())
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
                HistogramSnapshot {
                    sum: agg.client_seconds_sum,
                    count: agg.client_seconds_count,
                    bucket_edges_ns: &self.bucket_edges_ns,
                    bucket_counts: &agg.client_bucket_counts,
                },
                timestamp_ms,
            );
            push_histogram(
                &mut out,
                "traces_service_graph_request_server_seconds",
                &labels,
                HistogramSnapshot {
                    sum: agg.server_seconds_sum,
                    count: agg.server_seconds_count,
                    bucket_edges_ns: &self.bucket_edges_ns,
                    bucket_counts: &agg.server_bucket_counts,
                },
                timestamp_ms,
            );
            if self.enable_messaging_latency {
                push_histogram(
                    &mut out,
                    "traces_service_graph_request_messaging_system_seconds",
                    &labels,
                    HistogramSnapshot {
                        sum: agg.messaging_seconds_sum,
                        count: agg.messaging_seconds_count,
                        bucket_edges_ns: &self.bucket_edges_ns,
                        bucket_counts: &agg.messaging_bucket_counts,
                    },
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
        let bucket_count = self.bucket_edges_ns.len() + 1;
        let agg = self
            .aggregates
            .entry((client, server, edge.connection_type))
            .or_insert_with(|| EdgeAgg::new(bucket_count));
        agg.requests += 1.0;
        if edge.failed {
            agg.failed += 1.0;
        }
        if let Some(ns) = edge.client_latency_ns {
            agg.client_seconds_sum += ns_to_seconds(ns);
            agg.client_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.client_bucket_counts, ns);
        }
        if let Some(ns) = edge.server_latency_ns {
            agg.server_seconds_sum += ns_to_seconds(ns);
            agg.server_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.server_bucket_counts, ns);
        }
        if self.enable_messaging_latency
            && edge.connection_type == ConnectionType::MessagingSystem
            && let Some(ns) = edge.server_latency_ns.or(edge.client_latency_ns)
        {
            agg.messaging_seconds_sum += ns_to_seconds(ns);
            agg.messaging_seconds_count += 1.0;
            observe_latency(&self.bucket_edges_ns, &mut agg.messaging_bucket_counts, ns);
        }
    }
}

fn encode_checkpoint_value(edge: &Edge) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_u8(edge.connection_type as u8);
    buf.put_i64(edge.first_seen_ns);
    buf.put_u8(u8::from(edge.failed));
    put_optional_string(&mut buf, edge.client_service.as_deref());
    put_optional_string(&mut buf, edge.server_service.as_deref());
    put_optional_i64(&mut buf, edge.client_latency_ns);
    put_optional_i64(&mut buf, edge.server_latency_ns);
    buf.to_vec()
}

fn decode_checkpoint_value(mut buf: &[u8]) -> Result<Edge, CheckpointCodecError> {
    if buf.len() < 10 {
        return Err(CheckpointCodecError::Truncated);
    }
    let connection_type = match buf.get_u8() {
        0 => ConnectionType::Unset,
        1 => ConnectionType::VirtualNode,
        2 => ConnectionType::MessagingSystem,
        3 => ConnectionType::Database,
        _ => return Err(CheckpointCodecError::BadConnectionType),
    };
    let first_seen_ns = buf.get_i64();
    let failed = buf.get_u8() != 0;
    let client_service = get_optional_string(&mut buf)?;
    let server_service = get_optional_string(&mut buf)?;
    let client_latency_ns = get_optional_i64(&mut buf)?;
    let server_latency_ns = get_optional_i64(&mut buf)?;
    Ok(Edge {
        client_service,
        server_service,
        client_latency_ns,
        server_latency_ns,
        failed,
        connection_type,
        first_seen_ns,
    })
}

fn put_optional_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            let len = u32::try_from(value.len()).expect("service name too long");
            buf.put_u32(len);
            buf.put_slice(value.as_bytes());
        }
        None => buf.put_u8(0),
    }
}

fn put_optional_i64(buf: &mut BytesMut, value: Option<i64>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            buf.put_i64(value);
        }
        None => buf.put_u8(0),
    }
}

fn get_optional_string(buf: &mut &[u8]) -> Result<Option<String>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    if buf.len() < 4 {
        return Err(CheckpointCodecError::Truncated);
    }
    let len = buf.get_u32() as usize;
    if buf.len() < len {
        return Err(CheckpointCodecError::Truncated);
    }
    let value = String::from_utf8(buf[..len].to_vec()).map_err(|_| CheckpointCodecError::Utf8)?;
    buf.advance(len);
    Ok(Some(value))
}

fn get_optional_i64(buf: &mut &[u8]) -> Result<Option<i64>, CheckpointCodecError> {
    let present = get_presence(buf)?;
    if !present {
        return Ok(None);
    }
    if buf.len() < 8 {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(Some(buf.get_i64()))
}

fn get_presence(buf: &mut &[u8]) -> Result<bool, CheckpointCodecError> {
    if buf.is_empty() {
        return Err(CheckpointCodecError::Truncated);
    }
    Ok(buf.get_u8() != 0)
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

fn fill_virtual_node(
    edge: &mut Edge,
    span: &SpanRecord,
    is_client: bool,
    connection_type: ConnectionType,
) {
    if connection_type != ConnectionType::VirtualNode {
        return;
    }
    let Some(peer) = attr_value(span, "peer.service") else {
        return;
    };
    if is_client {
        edge.server_service = Some(peer.to_string());
    } else {
        edge.client_service = Some(peer.to_string());
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
    histogram: HistogramSnapshot<'_>,
    timestamp_ms: i64,
) {
    out.push(Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::ClassicHistogram {
            buckets: cumulative_buckets_seconds(histogram.bucket_edges_ns, histogram.bucket_counts),
            sum: histogram.sum,
            count: histogram.count,
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

fn attr_value<'a>(span: &'a SpanRecord, name: &str) -> Option<&'a str> {
    span.attributes
        .iter()
        .find_map(|(key, value)| (key == name && !value.is_empty()).then_some(value.as_str()))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Prometheus histogram bucket assignment uses f64 bucket edges"
)]
fn observe_latency(bucket_edges_ns: &[f64], bucket_counts: &mut [u64], ns: i64) {
    let value_ns = ns.max(0) as f64;
    let idx = bucket_edges_ns
        .iter()
        .position(|edge| value_ns <= *edge)
        .unwrap_or(bucket_edges_ns.len());
    if let Some(count) = bucket_counts.get_mut(idx) {
        *count += 1;
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "Prometheus histogram samples are f64 values on the output edge"
)]
fn cumulative_buckets_seconds(bucket_edges_ns: &[f64], bucket_counts: &[u64]) -> Vec<(f64, f64)> {
    let mut cumulative = 0_u64;
    bucket_edges_ns
        .iter()
        .enumerate()
        .map(|(idx, edge_ns)| {
            cumulative += bucket_counts.get(idx).copied().unwrap_or_default();
            (*edge_ns / NS_PER_SEC, cumulative as f64)
        })
        .collect()
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

    use assert2::{assert, check};

    use super::*;
    use crate::metricsgen::{
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
        series::{Series, SeriesSample},
    };

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

    fn histogram_bucket_value(series: &[Series], name: &str, le: f64) -> f64 {
        series
            .iter()
            .find(|s| s.name == name)
            .map_or(0.0, |s| match &s.sample {
                SeriesSample::ClassicHistogram { buckets, .. } => buckets
                    .iter()
                    .find(|(bucket_le, _)| (*bucket_le - le).abs() < 1e-9)
                    .map_or(0.0, |(_, count)| *count),
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
        assert_eq!(
            req.labels,
            [
                ("client".to_string(), "frontend".to_string()),
                ("connection_type".to_string(), "unset".to_string()),
                ("server".to_string(), "backend".to_string()),
            ]
        );
        check!(
            (histogram_sum(&out, "traces_service_graph_request_client_seconds") - 0.010).abs()
                < 1e-9
        );
        check!(
            (histogram_sum(&out, "traces_service_graph_request_server_seconds") - 0.008).abs()
                < 1e-9
        );
    }

    #[test]
    fn request_latency_histograms_include_configured_buckets() {
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
        for (name, le, want) in [
            ("traces_service_graph_request_client_seconds", 0.008, 0.0),
            ("traces_service_graph_request_client_seconds", 0.016, 1.0),
            ("traces_service_graph_request_server_seconds", 0.008, 1.0),
        ] {
            check!(
                (histogram_bucket_value(&out, name, le) - want).abs() < 1e-9,
                "case {name} le={le}"
            );
        }
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
        check!(store.record_span(&client, 0) == RecordOutcome::Recorded);

        check!(store.expire(5_000_000_000) == 0);
        check!(store.expire(10_000_000_000) == 1);

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
    fn virtual_node_uses_peer_service_as_server_label() {
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
            .push(("peer.service".into(), "db-proxy".into()));

        assert!(store.record_span(&client, 0) == RecordOutcome::Recorded);
        assert!(store.expire(20_000_000_000) == 1);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_unpaired_spans_total");
        assert!(
            labels
                == [
                    ("client".to_string(), "frontend".to_string()),
                    ("connection_type".to_string(), "virtual_node".to_string()),
                    ("server".to_string(), "db-proxy".to_string()),
                ]
        );
    }

    #[test]
    fn virtual_node_peer_backfill_is_order_independent_on_edge_update() {
        // An edge created first (no virtual-node signal), then updated by a span
        // carrying peer.service must end up labeled virtual_node WITH the peer
        // backfilled into the server label — the same result as if the
        // virtual-node span had arrived first (the create path).
        let mut store = EdgeStore::new(&MetricsGenConfig::default());
        let server = span(
            "backend",
            [0xB; 8],
            [0xA; 8],
            SpanKind::Server,
            StatusCode::Ok,
            1,
        );
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
            .push(("peer.service".into(), "db-proxy".into()));

        // Server arrives first and creates the edge with no virtual-node signal.
        assert!(store.record_span(&server, 0) == RecordOutcome::Recorded);
        // Client update carries the virtual-node / peer.service signal.
        assert!(store.record_span(&client, 1) == RecordOutcome::Completed);

        let out = store.drain(1_000);
        let labels = labels_for(&out, "traces_service_graph_request_total");
        // peer.service ("db-proxy") backfilled into the server label even though
        // the real server span ("backend") already set it on the create path.
        assert_eq!(
            labels,
            [
                ("client".to_string(), "frontend".to_string()),
                ("connection_type".to_string(), "virtual_node".to_string()),
                ("server".to_string(), "db-proxy".to_string()),
            ]
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
        assert_eq!(
            labels,
            [
                ("client".to_string(), "publisher".to_string()),
                (
                    "connection_type".to_string(),
                    "messaging_system".to_string(),
                ),
                ("server".to_string(), "worker".to_string()),
            ]
        );
        check!(
            (histogram_sum(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 0.005)
                .abs()
                < 1e-9
        );
        check!(
            (histogram_count(
                &out,
                "traces_service_graph_request_messaging_system_seconds"
            ) - 1.0)
                .abs()
                < 1e-9
        );
    }
}
