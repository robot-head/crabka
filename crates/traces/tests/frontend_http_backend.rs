//! `HttpQuerier` request-shape + Tempo-JSON parse, pinned by a loopback axum
//! stub. Verifies the querier's real scan-job contract
//! (`block`/`rowGroupStart`/`rowGroupEnd`), `X-Scope-OrgID`, and that a 404 by-id
//! response degrades to an empty partial.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use assert2::assert;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use crabka_traces::frontend::backend::{
    QuerierBackend, SearchJobRequest, TagValuesJobRequest, TraceByIdJobRequest,
};
use crabka_traces::frontend::http_backend::HttpQuerier;
use crabka_traces::frontend::job::JobShard;

type Log = Arc<Mutex<Vec<String>>>;

#[tokio::test]
async fn http_querier_search_job_sends_scan_params_and_parses() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));

    let app = Router::new()
        .route(
            "/api/search",
            get(
                |State(s): State<Log>, headers: axum::http::HeaderMap, uri: axum::http::Uri| async move {
                    let params = uri.query().unwrap_or_default().to_string();
                    let tenant = headers
                        .get("x-scope-orgid")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    s.lock().unwrap().push(format!("{tenant}|{params}"));
                    axum::Json(serde_json::json!({
                        "traces": [{
                            "traceID": "0a".repeat(16),
                            "rootServiceName": "svc",
                            "rootTraceName": "GET /",
                            "startTimeUnixNano": "1",
                            "durationMs": 1,
                            "spanSets": [{ "spans": [], "matched": 0 }]
                        }],
                        "metrics": { "totalBlocks": "1", "inspectedTraces": "1", "inspectedBytes": "64" }
                    }))
                },
            ),
        )
        .with_state(seen.clone());

    let addr = spawn(app).await;
    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();

    let out = backend
        .search_job(&SearchJobRequest {
            tenant: "tenant-x".to_string(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100_000_000_000,
            limit: 20,
            spss: 3,
            shard: JobShard::Block {
                block_id: "blk-1".to_string(),
                row_group_start: 2,
                row_group_end: 5,
            },
        })
        .await
        .unwrap();

    assert!(out.traces.len() == 1);
    assert!(out.metrics.inspected_bytes == 64);
    assert!(out.metrics.total_blocks == 1);

    let log = seen.lock().unwrap();
    assert!(log.len() == 1);
    let entry = &log[0];
    assert!(entry.starts_with("tenant-x|"));
    // The real querier scan-job params.
    assert!(entry.contains("block=blk-1"));
    assert!(entry.contains("rowGroupStart=2"));
    assert!(entry.contains("rowGroupEnd=5"));
    // start/end are epoch seconds.
    assert!(entry.contains("start=0"));
    assert!(entry.contains("end=100"));
}

#[tokio::test]
async fn http_querier_live_shard_sends_no_scan_params() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route(
            "/api/search",
            get(|State(s): State<Log>, uri: axum::http::Uri| async move {
                s.lock()
                    .unwrap()
                    .push(uri.query().unwrap_or_default().to_string());
                axum::Json(serde_json::json!({ "traces": [], "metrics": {} }))
            }),
        )
        .with_state(seen.clone());
    let addr = spawn(app).await;
    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();

    backend
        .search_job(&SearchJobRequest {
            tenant: "t1".to_string(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 1,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
        })
        .await
        .unwrap();

    let log = seen.lock().unwrap();
    assert!(!log[0].contains("block="));
    assert!(!log[0].contains("rowGroup"));
}

#[tokio::test]
async fn http_querier_by_id_404_is_empty_partial() {
    let app = Router::new().route(
        "/api/v2/traces/{trace_id}",
        get(|| async { (axum::http::StatusCode::NOT_FOUND, "trace not found") }),
    );
    let addr = spawn(app).await;
    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();

    let out = backend
        .trace_by_id_job(&TraceByIdJobRequest {
            tenant: "t1".to_string(),
            trace_id: [9; 16],
            start_ns: 0,
            end_ns: 1,
            querier: Some(0),
        })
        .await
        .unwrap();

    assert!(out.trace.is_empty());
}

#[tokio::test]
async fn http_querier_by_id_parses_v2_envelope() {
    let app = Router::new().route(
        "/api/v2/traces/{trace_id}",
        get(|| async {
            axum::Json(serde_json::json!({
                "trace": {
                    "resourceSpans": [{
                        "resource": { "attributes": [] },
                        "scopeSpans": [{
                            "scope": {},
                            "spans": [{ "spanId": "BgYGBgYGBgY=", "name": "op" }]
                        }]
                    }]
                },
                "status": "COMPLETE",
                "message": ""
            }))
        }),
    );
    let addr = spawn(app).await;
    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();

    let out = backend
        .trace_by_id_job(&TraceByIdJobRequest {
            tenant: "t1".to_string(),
            trace_id: [10; 16],
            start_ns: 0,
            end_ns: 1,
            querier: Some(0),
        })
        .await
        .unwrap();

    assert!(out.trace.span_count() == 1);
    assert!(out.trace.status == "COMPLETE");
}

#[tokio::test]
async fn http_querier_tag_values_encodes_tag_path_segment() {
    let seen: Log = Arc::new(Mutex::new(Vec::new()));
    let app =
        Router::new()
            .route(
                "/api/v2/search/tag/{tag}/values",
                get(
                    |State(s): State<Log>,
                     axum::extract::Path(tag): axum::extract::Path<String>| async move {
                        s.lock().unwrap().push(tag);
                        axum::Json(serde_json::json!({ "tagValues": [], "metrics": {} }))
                    },
                ),
            )
            .with_state(seen.clone());
    let addr = spawn(app).await;
    let backend = HttpQuerier::new(vec![addr.to_string()], Duration::from_secs(5)).unwrap();

    // `#` interpolated raw would start a URL fragment and truncate the path
    // (the stub's `{tag}/values` route would never match). Path-segment
    // encoding round-trips it intact.
    backend
        .tag_values_job(&TagValuesJobRequest {
            tenant: "t1".to_string(),
            tag: "a#b".to_string(),
            start_ns: 0,
            end_ns: 1,
            shard: JobShard::Live,
        })
        .await
        .unwrap();

    let log = seen.lock().unwrap();
    assert!(log.len() == 1);
    assert!(log[0] == "a#b");
}

async fn spawn(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}
