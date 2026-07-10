//! End-to-end metrics-generator wiring through mock source/sink/clock.

use std::sync::Arc;

use assert2::check;
use crabka_traces::metricsgen::{
    MetricsGenConfig, MetricsGenService, MockClock, MockRemoteWriteSink, MockSpanSource, SpanKind,
    SpanRecord, StatusCode,
};

fn span(
    service: &str,
    kind: SpanKind,
    status: StatusCode,
    span_id: [u8; 8],
    parent_span_id: [u8; 8],
    duration_ns: i64,
) -> SpanRecord {
    SpanRecord {
        tenant: "tenant-1".into(),
        trace_id: [0xAB; 16],
        span_id,
        parent_span_id,
        name: "GET /checkout".into(),
        kind,
        start_ns: 0,
        duration_ns,
        status,
        status_message: String::new(),
        service_name: service.into(),
        attributes: Vec::new(),
        size_bytes: 200,
    }
}

#[tokio::test]
async fn metrics_generator_end_to_end_red_and_service_graph() {
    let cfg = MetricsGenConfig {
        max_exemplars_per_series: 2,
        ..MetricsGenConfig::default()
    };
    let clock = MockClock::new(0);
    let source = Arc::new(MockSpanSource::default());
    let sink = Arc::new(MockRemoteWriteSink::default());
    let service =
        MetricsGenService::new(cfg, Arc::new(clock.clone()), source.clone(), sink.clone());

    source.push_batch(vec![
        span(
            "frontend",
            SpanKind::Client,
            StatusCode::Ok,
            [0xA; 8],
            [0; 8],
            12_000_000,
        ),
        span(
            "checkout",
            SpanKind::Server,
            StatusCode::Ok,
            [0xB; 8],
            [0xA; 8],
            8_000_000,
        ),
    ]);

    assert2::assert!(service.poll_once(100).await.unwrap() == 2);
    clock.set(15_000_000_000);
    assert2::assert!(service.collect_once().await.unwrap() == 1);

    let writes = sink.writes();
    assert2::assert!(writes.len() == 1);
    let payload = &writes[0];
    assert2::assert!(payload.tenant == "tenant-1");

    let calls = payload.series.iter().find(|s| {
        s.name == "traces_spanmetrics_calls_total"
            && s.labels
                .iter()
                .any(|(k, v)| k == "span_name" && v == "GET /checkout")
    });
    assert2::assert!(calls.is_some());

    let latency = payload
        .series
        .iter()
        .find(|s| s.name == "traces_spanmetrics_latency")
        .unwrap();
    assert2::assert!(
        latency
            .exemplars
            .iter()
            .any(|e| e.labels.iter().any(|(k, _)| k == "trace_id"))
    );

    let edge = payload
        .series
        .iter()
        .find(|s| s.name == "traces_service_graph_request_total")
        .unwrap();
    check!(
        edge.labels
            .iter()
            .any(|(k, v)| k == "client" && v == "frontend")
    );
    check!(
        edge.labels
            .iter()
            .any(|(k, v)| k == "server" && v == "checkout")
    );
    check!(source.commits() == 1);
}
