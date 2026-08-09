//! Shard-equivalence.
//!
//! A search sharded across N jobs, that is Live plus per-block plus
//! per-row-group, equals the unsharded search over identical data. Both give
//! the same trace set and honor `limit` and `spss`. The tests drive the public
//! `frontend` API with `MockQuerier` and `MockCatalog`.

use std::sync::Arc;

use assert2::check;
use crabka_traces::frontend::{
    QueryFrontend,
    backend::{MockQuerier, SearchPartial},
    config::FrontendConfig,
    job::{BlockMetaInfo, MockCatalog, RowGroupInfo},
    wire::{Metrics, SpanJson, SpanSetJson, TraceJson},
};
use crabka_units::{ByteSize, convert::ByteSizeExt as _, millis};

fn block(id: &str, start: i64, end: i64, rgs: &[u64]) -> BlockMetaInfo {
    let row_groups = rgs
        .iter()
        .enumerate()
        .map(|(i, &b)| RowGroupInfo {
            index: u32::try_from(i).unwrap(),
            compressed: ByteSize::from_bytes(b),
        })
        .collect();
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: start,
        end_ns: end,
        size: ByteSize::from_bytes(rgs.iter().sum()),
        row_groups,
    }
}

fn trace_with_spans(tid: &str, start: u64, span_ids: &[&str]) -> TraceJson {
    let spans: Vec<SpanJson> = span_ids
        .iter()
        .map(|&s| SpanJson {
            span_id: s.to_string(),
            start_time_unix_nano: start.to_string(),
            duration_nanos: "1".to_string(),
            attributes: vec![],
        })
        .collect();
    let matched = u32::try_from(spans.len()).unwrap();
    TraceJson {
        trace_id: tid.to_string(),
        root_service_name: "svc".to_string(),
        root_trace_name: "GET /".to_string(),
        start_time_unix_nano: start.to_string(),
        duration: millis(1),
        span_sets: vec![SpanSetJson { spans, matched }],
    }
}

fn partial(traces: Vec<TraceJson>, bytes: u64) -> SearchPartial {
    SearchPartial {
        traces,
        metrics: Metrics {
            completed_jobs: 1,
            inspected_bytes: bytes,
            inspected_traces: 1,
            ..Metrics::default()
        },
    }
}

#[tokio::test]
async fn sharded_search_equals_unsharded() {
    // Two blocks; b2 is large (> budget, 2 row-groups) so it fans into 2 jobs.
    // With a hot window we also get a Live job:
    //   [Live, b1(whole), b2(rg0), b2(rg1)] = 4 jobs.
    // trace 01 is split: b1 has span 01, b2-rg0 has span 02 (same traceID).
    // trace 02 lives wholly in b2-rg1. Live returns nothing.
    let catalog = MockCatalog::new(vec![
        block("b1", 0, 100, &[500]),
        block("b2", 100, 200, &[15_000, 15_000]),
    ]);
    let backend = MockQuerier::new();
    // Dispatch order = plan order = [Live, b1, b2-rg0, b2-rg1] (max_concurrency 1).
    backend.stub_search(partial(vec![], 0)); // Live: empty
    backend.stub_search(partial(vec![trace_with_spans("01", 50, &["01"])], 100)); // b1
    backend.stub_search(partial(vec![trace_with_spans("01", 40, &["02"])], 200)); // b2-rg0
    backend.stub_search(partial(vec![trace_with_spans("02", 150, &["03"])], 300)); // b2-rg1

    let cfg = FrontendConfig {
        target_per_job: ByteSize::from_bytes(10_000),
        max_concurrency: 1,
        hot_frontier_ns: 150,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

    let resp = qf.search("t1", "{ }", 0, 300, 20, 10).await.unwrap();

    let t1_spans: usize = resp.traces[1]
        .span_sets
        .iter()
        .map(|ss| ss.spans.len())
        .sum();
    check!(
        (
            qf.backend_ref().search_calls().len(),
            resp.traces
                .iter()
                .map(|trace| trace.trace_id.as_str())
                .collect::<Vec<_>>(),
            t1_spans,
            (
                resp.metrics.total_jobs,
                resp.metrics.completed_jobs,
                resp.metrics.total_blocks,
                resp.metrics.inspected_bytes,
            ),
        ) == (4, vec!["02", "01"], 2, (4, 4, 2, 600))
    );
}

#[tokio::test]
async fn limit_and_spss_applied_after_merge() {
    let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
    let backend = MockQuerier::new();
    backend.stub_search(partial(
        vec![
            trace_with_spans("01", 100, &["01", "02", "03", "04", "05"]),
            trace_with_spans("02", 300, &["06"]),
            trace_with_spans("03", 200, &["07"]),
        ],
        10,
    ));
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
    // limit 2 (newest-first => 300, 200), spss 2.
    let resp = qf.search("t1", "{ }", 0, 300, 2, 2).await.unwrap();
    assert2::assert!(
        resp.traces
            .iter()
            .map(|trace| trace.start_time_unix_nano.as_str())
            .collect::<Vec<_>>()
            == vec!["300", "200"]
    );
    for t in &resp.traces {
        for ss in &t.span_sets {
            assert2::assert!(ss.spans.len() <= 2);
        }
    }
}
