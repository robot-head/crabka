//! Trace-by-id assembly: a trace whose recent spans live in different queriers'
//! live-stores reassembles into one v2 trace, deduped by `spanId`, with
//! COMPLETE/PARTIAL by size. Driven through the public `frontend` API with a
//! multi-querier `MockQuerier`.

use std::sync::Arc;

use assert2::assert;
use crabka_traces::frontend::{
    QueryFrontend,
    backend::{MockQuerier, TracePartial},
    config::FrontendConfig,
    job::{BlockMetaInfo, MockCatalog, RowGroupInfo},
    merge::TraceStatus,
    wire::{
        Metrics, OtlpSpanJson, ResourceSpansJson, ScopeSpansJson, TraceByIdResponseJson,
        TraceEnvelopeJson,
    },
};

fn block(id: &str, start: i64, end: i64) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: start,
        end_ns: end,
        size_bytes: 100,
        row_groups: vec![RowGroupInfo {
            index: 0,
            compressed_bytes: 100,
        }],
    }
}

fn otlp_span(id: &str) -> OtlpSpanJson {
    let mut rest = serde_json::Map::new();
    rest.insert("name".to_string(), serde_json::json!("op"));
    OtlpSpanJson {
        span_id: id.to_string(),
        rest,
    }
}

fn body(span_ids: &[&str]) -> TraceByIdResponseJson {
    TraceByIdResponseJson {
        trace: TraceEnvelopeJson {
            resource_spans: vec![ResourceSpansJson {
                resource: serde_json::Value::Null,
                scope_spans: vec![ScopeSpansJson {
                    scope: serde_json::Value::Null,
                    spans: span_ids.iter().map(|id| otlp_span(id)).collect(),
                }],
            }],
        },
        status: "COMPLETE".to_string(),
        message: String::new(),
    }
}

fn trace_partial(body: TraceByIdResponseJson) -> TracePartial {
    TracePartial {
        trace: body,
        metrics: Metrics {
            completed_jobs: 1,
            ..Metrics::default()
        },
    }
}

#[tokio::test]
async fn trace_split_across_queriers_reassembles() {
    // 2 queriers: A holds spans 01,02; B holds spans 02,03 (02 overlaps).
    let catalog = MockCatalog::new(vec![block("b1", 0, 100)]);
    let backend = MockQuerier::with_querier_count(2);
    backend.stub_trace(trace_partial(body(&["01", "02"])));
    backend.stub_trace(trace_partial(body(&["02", "03"])));
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        max_trace_bytes: 1_000_000,
        max_concurrency: 1,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

    let (trace, metrics, status) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
    // One by-id job per querier.
    assert!(qf.backend_ref().trace_calls().len() == 2);
    let trace = trace.expect("assembled trace");
    assert_eq!(trace.span_count(), 3);
    assert_eq!(metrics.completed_jobs, 2);
    assert_eq!(metrics.total_jobs, 2);
    assert!(matches!(status, TraceStatus::Complete));
}

#[tokio::test]
async fn oversized_trace_is_partial() {
    let catalog = MockCatalog::new(vec![block("b1", 0, 100)]);
    let backend = MockQuerier::with_querier_count(1);
    backend.stub_trace(trace_partial(body(&["01", "02", "03"])));
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        max_trace_bytes: 1,
        max_concurrency: 1,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

    let (trace, _m, status) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
    assert!(trace.is_some());
    assert!(matches!(status, TraceStatus::Partial));
}

#[tokio::test]
async fn missing_trace_is_none() {
    let catalog = MockCatalog::new(vec![block("b1", 0, 100)]);
    let backend = MockQuerier::with_querier_count(2);
    // Both queriers return empty (default partial).
    let cfg = FrontendConfig {
        max_concurrency: 1,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
    let (trace, _m, status) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
    assert!(trace.is_none());
    assert!(matches!(status, TraceStatus::Complete));
}
