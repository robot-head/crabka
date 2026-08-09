//! Router-level behavioral coverage for the query-frontend.
//!
//! The tests drive the *real* `HttpQuerier` pool against loopback stub
//! queriers, which mirrors the role binary's wiring, with an in-memory
//! `TraceIndexCatalog`.
//!
//! The frontend shards a search, metrics or tag query into a live shard plus
//! one job per catalog block or row-group range. It probes the live shard when
//! the window reaches `hot_frontier_ns`.
//!
//! The live shard sends **no** scan params. A cold-block shard sends the
//! querier's real `block`, `rowGroupStart` and `rowGroupEnd` params, which is
//! the authoritative slice-5 querier contract. Stub queriers therefore separate
//! the tiers by the presence of a `block=` param, not by a tier header.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use assert2::check;
use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    routing::get,
};
use crabka_traces::frontend::{
    QueryFrontend,
    config::FrontendConfig,
    http_backend::HttpQuerier,
    job::{BlockMetaInfo, RowGroupInfo, TraceIndexCatalog},
    server::router_with_backend,
};
use crabka_units::{
    ByteSize,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tokio::{sync::Barrier, time::timeout};
use tower::ServiceExt as _;

/// Mirror the role binary's builder.
///
/// This builds the new query-frontend router from a list of querier URLs, which
/// carry a scheme and may use the comma form, plus a pre-resolved block catalog
/// and a frontend config.
fn build_router(querier_urls: &str, cfg: FrontendConfig, catalog: TraceIndexCatalog) -> Router {
    let backend =
        HttpQuerier::new(parse_addrs(querier_urls), cfg.request_timeout.to_std()).unwrap();
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    router_with_backend(qf)
}

/// Strip the scheme from a comma-separated querier URL list into bare host:port,
/// exactly as the binary's `parse_querier_addrs` does.
fn parse_addrs(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|raw| {
            let url = url::Url::parse(raw).unwrap();
            let host = url.host_str().unwrap();
            url.port()
                .map_or_else(|| host.to_string(), |port| format!("{host}:{port}"))
        })
        .collect()
}

/// A catalog with a single block covering `[0, far-future]` for `tenant-a`.
///
/// A query window over `[0, end]` therefore plans `[Live, Block]`, which is 2
/// shards, when the window reaches `hot_frontier_ns`. One row-group keeps it a
/// whole-block job.
fn single_block_catalog() -> TraceIndexCatalog {
    let block = BlockMetaInfo {
        block_id: "blocks/a.parquet".to_string(),
        start_ns: 0,
        end_ns: 10_000_000_000,
        size: ByteSize::from_bytes(100),
        row_groups: vec![RowGroupInfo {
            index: 0,
            compressed: ByteSize::from_bytes(100),
        }],
    };
    TraceIndexCatalog::new(BTreeMap::from([("tenant-a".to_string(), vec![block])]))
}

/// Config that always probes the live tier, through `hot_frontier_ns = 0`, and
/// runs shards with the default high concurrency.
fn two_shard_cfg() -> FrontendConfig {
    FrontendConfig {
        hot_frontier_ns: 0,
        ..FrontendConfig::default()
    }
}

/// True when the querier received a cold-block shard, which carries `block=`.
/// It is false for the live shard, which sends no scan params.
fn is_backend_shard(query: &str) -> bool {
    query.contains("block=")
}

/// A complete matched span in the querier's `search_json` shape.
///
/// The frontend parses spans into the typed
/// [`crabka_traces::frontend::wire::SpanJson`], which needs
/// `startTimeUnixNano` and `durationNanos`. Stub spans must therefore carry
/// both.
fn span(span_id: &str) -> Value {
    json!({
        "spanID": span_id,
        "startTimeUnixNano": "1000000000",
        "durationNanos": "1000"
    })
}

async fn spawn(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn get_text(app: Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri(uri)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

// --- by-id: tenant + window reach the querier ------------------------------

#[tokio::test]
async fn by_id_forwards_tenant_and_window_to_querier() {
    let app = Router::new()
        .route("/api/v2/traces/{trace_id}", get(record_by_id))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, FrontendConfig::default(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/traces/0123456789abcdef0123456789abcdef?start=1&end=2")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    // The v2 envelope wraps the querier's trace body.
    assert2::assert!(json["status"] == "COMPLETE");
    let echoed = json["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0].clone();
    // The querier sees the right trace id in its path, the tenant header, and
    // start/end as epoch seconds.
    check!(echoed["path"] == "/api/v2/traces/0123456789abcdef0123456789abcdef");
    check!(echoed["tenant"] == "tenant-a");
    check!(echoed["query"].as_str().unwrap().contains("start=1"));
    check!(echoed["query"].as_str().unwrap().contains("end=2"));
}

async fn record_by_id(State(()): State<()>, headers: HeaderMap, uri: Uri) -> axum::Json<Value> {
    // Echo the request back inside a v2 trace body so the frontend's assemble
    // step surfaces it.
    axum::Json(json!({
        "trace": {
            "resourceSpans": [{
                "resource": { "attributes": [] },
                "scopeSpans": [{
                    "scope": {},
                    "spans": [{
                        "spanId": "AQEBAQEBAQE=",
                        "name": "echo",
                        "path": uri.path(),
                        "query": uri.query().unwrap_or_default(),
                        "tenant": headers
                            .get("x-scope-orgid")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default(),
                    }]
                }]
            }]
        },
        "status": "COMPLETE",
        "message": ""
    }))
}

// --- search: merge / dedupe across shards ----------------------------------

#[tokio::test]
async fn merges_duplicate_trace_results_across_shards() {
    let app = Router::new()
        .route("/api/search", get(sharded_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    // Same traceID from both shards reunions into one trace; the two distinct
    // spans (one per shard) merge into one spanSet with matched accumulated.
    let span_sets = json["traces"][0]["spanSets"].as_array().unwrap();
    check!(
        (
            json["traces"].as_array().unwrap().len(),
            &json["traces"][0]["traceID"],
            span_sets.len(),
            &span_sets[0]["matched"],
            span_sets[0]["spans"].as_array().unwrap().len(),
            &json["metrics"]["totalBlocks"],
            &json["metrics"]["inspectedTraces"],
        ) == (
            1,
            &serde_json::json!("0123456789abcdef0123456789abcdef"),
            1,
            &serde_json::json!(2),
            2,
            &serde_json::json!(1),
            &serde_json::json!(12),
        )
    );
}

#[tokio::test]
async fn deduplicates_spans_across_shards() {
    // Both shards return the same single span; the merge collapses to one.
    let app = Router::new()
        .route("/api/search", get(overlapping_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let span_sets = json["traces"][0]["spanSets"].as_array().unwrap();
    let spans = span_sets[0]["spans"].as_array().unwrap();

    check!(
        (
            json["traces"].as_array().unwrap().len(),
            span_sets.len(),
            &span_sets[0]["matched"],
            spans.iter().map(|span| &span["spanID"]).collect::<Vec<_>>(),
        ) == (
            1,
            1,
            &serde_json::json!(1),
            vec![&serde_json::json!("1111111111111111")],
        )
    );
}

#[tokio::test]
async fn caps_merged_traces_to_limit_newest_first() {
    // Each shard returns three distinct traces; unmerged that is six traces with
    // interleaved start times. `limit` applies AFTER merge, newest-first.
    let app = Router::new()
        .route("/api/search", get(many_trace_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3&limit=2")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let traces = json["traces"].as_array().unwrap();
    check!(
        traces
            .iter()
            .map(|trace| &trace["startTimeUnixNano"])
            .collect::<Vec<_>>()
            == vec![&serde_json::json!("600"), &serde_json::json!("500")]
    );
}

#[tokio::test]
async fn defaults_merged_trace_limit_to_twenty() {
    // Single block, no live tier (frontier in the future): one shard returning 25
    // distinct traces, more than Tempo's default 20.
    let app = Router::new()
        .route("/api/search", get(overflow_trace_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let router = build_router(&upstream, cfg, single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(json["traces"].as_array().unwrap().len() == 20);
}

/// Config for the spss tests: two shards, Live plus one block.
///
/// `max_concurrency = 1` dispatches them in plan order, so the reunioned span
/// order is deterministic: live spans first, then backend spans.
fn ordered_two_shard_cfg() -> FrontendConfig {
    FrontendConfig {
        hot_frontier_ns: 0,
        max_concurrency: 1,
        ..FrontendConfig::default()
    }
}

#[tokio::test]
async fn caps_span_sets_per_trace_to_spss() {
    // Each shard returns the same trace with two distinct spans. The merge
    // reunions all four spans into the first spanSet; spss then caps the spans
    // kept in that spanSet (preserving `matched`).
    let app = Router::new()
        .route("/api/search", get(span_pair_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, ordered_two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3&spss=2")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let span_sets = json["traces"][0]["spanSets"].as_array().unwrap();
    let spans = span_sets[0]["spans"].as_array().unwrap();
    // spss=2 ⇒ first two spans kept (live shard's pair), matched is the true sum.
    check!(
        (
            spans.iter().map(|span| &span["spanID"]).collect::<Vec<_>>(),
            &span_sets[0]["matched"],
        ) == (
            vec![
                &serde_json::json!("1111111111111111"),
                &serde_json::json!("2222222222222222"),
            ],
            &serde_json::json!(4),
        )
    );
}

#[tokio::test]
async fn defaults_span_sets_per_trace_to_three() {
    // No `spss` ⇒ Tempo's default of 3: of the four reunioned spans, three kept.
    let app = Router::new()
        .route("/api/search", get(span_pair_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, ordered_two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let spans = json["traces"][0]["spanSets"][0]["spans"]
        .as_array()
        .unwrap();
    assert2::assert!(spans.len() == 3);
}

#[tokio::test]
async fn dispatches_search_shards_concurrently() {
    // A barrier of width 2 only releases once both shard requests are in flight,
    // so a serial dispatch would deadlock past the timeout.
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/api/search", get(concurrent_search_response))
        .with_state(barrier);
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = timeout(
        Duration::from_secs(2),
        router.oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("sharded search should not serialize shard requests")
    .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(
        json["traces"][0]["spanSets"][0]["spans"]
            .as_array()
            .unwrap()
            .len()
            == 2
    );
}

// --- search: backend row-group job forwarding ------------------------------

#[tokio::test]
async fn forwards_backend_row_group_job_to_querier() {
    // A block larger than target_bytes_per_job fans into per-row-group-range
    // jobs; the querier receives the real scan params.
    let app = Router::new()
        .route("/api/search", get(query_echo_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        target_per_job: ByteSize::from_bytes(100),
        ..FrontendConfig::default()
    };
    let block = BlockMetaInfo {
        block_id: "blocks/a.parquet".to_string(),
        start_ns: 0,
        end_ns: 10_000_000_000,
        size: ByteSize::from_bytes(100),
        row_groups: vec![
            RowGroupInfo {
                index: 0,
                compressed: ByteSize::from_bytes(40),
            },
            RowGroupInfo {
                index: 1,
                compressed: ByteSize::from_bytes(60),
            },
        ],
    };
    let catalog = TraceIndexCatalog::new(BTreeMap::from([("tenant-a".to_string(), vec![block])]));
    let router = build_router(&upstream, cfg, catalog);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=10")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let received_query = json["traces"][0]["rootTraceName"].as_str().unwrap();
    // The 100-byte block at a 100-byte budget stays one whole-block job:
    // [rg0, rg1) => rowGroupStart=0, rowGroupEnd=2.
    check!(received_query.contains("block=blocks%2Fa.parquet"));
    check!(received_query.contains("rowGroupStart=0"));
    check!(received_query.contains("rowGroupEnd=2"));
}

#[tokio::test]
async fn uses_tenant_specific_backend_row_group_jobs() {
    let app = Router::new()
        .route("/api/search", get(query_echo_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        target_per_job: ByteSize::from_bytes(100),
        ..FrontendConfig::default()
    };
    let catalog = TraceIndexCatalog::new(BTreeMap::from([
        (
            "tenant-a".to_string(),
            vec![BlockMetaInfo {
                block_id: "blocks/tenant-a.parquet".to_string(),
                start_ns: 0,
                end_ns: 10_000_000_000,
                size: ByteSize::from_bytes(100),
                row_groups: vec![RowGroupInfo {
                    index: 0,
                    compressed: ByteSize::from_bytes(100),
                }],
            }],
        ),
        (
            "tenant-b".to_string(),
            vec![BlockMetaInfo {
                block_id: "blocks/tenant-b.parquet".to_string(),
                start_ns: 0,
                end_ns: 10_000_000_000,
                size: ByteSize::from_bytes(100),
                row_groups: vec![RowGroupInfo {
                    index: 2,
                    compressed: ByteSize::from_bytes(100),
                }],
            }],
        ),
    ]));
    let router = build_router(&upstream, cfg, catalog);

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=10")
                .header("x-scope-orgid", "tenant-b")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let received_query = json["traces"][0]["rootTraceName"].as_str().unwrap();
    // tenant-b's block has a single row-group at index 2 => [2, 3).
    check!(received_query.contains("block=blocks%2Ftenant-b.parquet"));
    check!(received_query.contains("rowGroupStart=2"));
    check!(received_query.contains("rowGroupEnd=3"));
}

// --- metrics: query_range / instant sharding -------------------------------

#[tokio::test]
async fn metrics_query_range_is_a_single_unsharded_job() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/api/metrics/query_range", get(record_metrics))
        .with_state(seen.clone());
    let upstream = spawn(app).await;
    // A catalog with a cold block + the live tier: a *search* would shard here,
    // but metrics must NOT — sharding double-counts cold blocks (the
    // no-restriction job already scans cold-before-frontier + live) and is plain
    // wrong for non-additive aggregates, so metrics is one unrestricted job over
    // the full hot+cold union.
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(
                    "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=1&end=3&step=1",
                )
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    // Returned verbatim — no cross-shard summing.
    assert2::assert!(
        json["series"][0]["samples"]
            == json!([
                {"timestampMs": "1000000000", "value": 1.0},
                {"timestampMs": "2000000000", "value": 2.0}
            ])
    );

    let log = seen.lock().unwrap();
    assert2::assert!(log.len() == 1); // exactly one job, not Live + per-block
    assert2::assert!(!log[0].contains("block=")); // unrestricted -> full hot+cold union
}

#[tokio::test]
async fn metrics_query_limits_exemplars() {
    let app = Router::new()
        .route("/api/metrics/query_range", get(sharded_metrics_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(
                    "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=1&end=3&step=1&exemplars=1",
                )
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(json["series"][0]["exemplars"].as_array().unwrap().len() == 1);
}

#[tokio::test]
async fn metrics_instant_query_is_a_single_unsharded_job() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/api/metrics/query", get(record_metrics))
        .with_state(seen.clone());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri(
                    "/api/metrics/query?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=1&end=3",
                )
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(
        json["series"][0]["samples"]
            == json!([
                {"timestampMs": "1000000000", "value": 1.0},
                {"timestampMs": "2000000000", "value": 2.0}
            ])
    );

    let log = seen.lock().unwrap();
    assert2::assert!(log.len() == 1);
    assert2::assert!(!log[0].contains("block="));
}

// --- v2 tag discovery / values sharding ------------------------------------

#[tokio::test]
async fn shards_v2_tag_discovery_across_live_frontier() {
    let app = Router::new()
        .route("/api/v2/search/tags", get(sharded_tags_response))
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tags?scope=span&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    // Both shards' span-scope tags union+dedupe into one scope, sorted.
    assert2::assert!(
        json["scopes"]
            == serde_json::json!([{
                "name": "span",
                "tags": ["backend.tag", "live.tag"],
            }])
    );
}

#[tokio::test]
async fn dispatches_tag_discovery_shards_concurrently() {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/api/v2/search/tags", get(concurrent_tags_response))
        .with_state(barrier);
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = timeout(
        Duration::from_secs(2),
        router.oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tags?scope=span&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("sharded tag discovery should not serialize shard requests")
    .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(json["scopes"][0]["tags"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn shards_v2_tag_values_across_live_frontier() {
    let app = Router::new()
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(sharded_tag_values_response),
        )
        .with_state(());
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tag/.svc/values?start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(
        json["tagValues"]
            == json!([
                { "type": "string", "value": "backend" },
                { "type": "string", "value": "live" },
            ])
    );
}

#[tokio::test]
async fn dispatches_tag_value_shards_concurrently() {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(concurrent_tag_values_response),
        )
        .with_state(barrier);
    let upstream = spawn(app).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = timeout(
        Duration::from_secs(2),
        router.oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tag/.svc/values?start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("sharded tag values should not serialize shard requests")
    .unwrap();

    assert2::assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert2::assert!(json["tagValues"].as_array().unwrap().len() == 2);
}

// --- validation + capacity --------------------------------------------------

#[tokio::test]
async fn search_requires_valid_start_and_end() {
    let app = Router::new()
        .route("/api/search", get(overlapping_search_response))
        .with_state(());
    let upstream = spawn(app).await;

    let (status, body) = get_text(
        build_router(&upstream, FrontendConfig::default(), single_block_catalog()),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D",
    )
    .await;
    assert2::assert!(status == StatusCode::BAD_REQUEST);
    assert2::assert!(body == "missing query parameter start");

    let (status, body) = get_text(
        build_router(&upstream, FrontendConfig::default(), single_block_catalog()),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0",
    )
    .await;
    assert2::assert!(status == StatusCode::BAD_REQUEST);
    assert2::assert!(body == "missing query parameter end");

    let (status, body) = get_text(
        build_router(&upstream, FrontendConfig::default(), single_block_catalog()),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=bogus&end=1",
    )
    .await;
    assert2::assert!(status == StatusCode::BAD_REQUEST);
    assert2::assert!(body == "invalid query parameter start");
}

#[tokio::test]
async fn drains_concurrent_search_requests() {
    // The new frontend bounds concurrency within a single request's fan-out (no
    // cross-request router queue), so two concurrent requests both proceed and
    // drain to OK — the observable contract the legacy queue test checked.
    let app = Router::new()
        .route("/api/search", get(slow_search_response))
        .with_state(());
    let upstream = spawn(app).await;
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        max_concurrency: 1,
        ..FrontendConfig::default()
    };
    let first_router = build_router(&upstream, cfg.clone(), single_block_catalog());
    let second_router = build_router(&upstream, cfg, single_block_catalog());

    let first = tokio::spawn(async move {
        first_router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1")
                    .header("x-scope-orgid", "tenant-a")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    });
    tokio::task::yield_now().await;
    let second = tokio::spawn(async move {
        second_router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1")
                    .header("x-scope-orgid", "tenant-a")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    });

    let (first, second) = timeout(Duration::from_secs(2), async {
        (first.await.unwrap(), second.await.unwrap())
    })
    .await
    .expect("concurrent frontend requests should drain");

    assert2::assert!(first == StatusCode::OK);
    assert2::assert!(second == StatusCode::OK);
}

// --- stub querier responses -------------------------------------------------

async fn sharded_search_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    let (span_id, inspected_traces) = if is_backend_shard(uri.query().unwrap_or_default()) {
        ("1111111111111111", 5)
    } else {
        ("2222222222222222", 7)
    };
    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": "1000000000",
            "durationMs": 2,
            "spanSets": [{
                "spans": [span(span_id)],
                "matched": 1
            }]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": inspected_traces, "inspectedBytes": 0 }
    }))
}

async fn concurrent_search_response(
    State(barrier): State<Arc<Barrier>>,
    uri: Uri,
) -> axum::Json<Value> {
    let backend = is_backend_shard(uri.query().unwrap_or_default());
    barrier.wait().await;
    let span_id = if backend {
        "1111111111111111"
    } else {
        "2222222222222222"
    };
    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": "1000000000",
            "durationMs": 2,
            "spanSets": [{
                "spans": [span(span_id)],
                "matched": 1
            }]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

async fn overlapping_search_response() -> axum::Json<Value> {
    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": "1000000000",
            "durationMs": 2,
            "spanSets": [{
                "spans": [span("1111111111111111")],
                "matched": 1
            }]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

async fn many_trace_search_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    let traces: Vec<Value> = if is_backend_shard(uri.query().unwrap_or_default()) {
        [("aa", "100"), ("bb", "300"), ("cc", "500")]
    } else {
        [("dd", "200"), ("ee", "400"), ("ff", "600")]
    }
    .into_iter()
    .map(|(prefix, start)| {
        json!({
            "traceID": prefix.repeat(16),
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": start,
            "durationMs": 2,
            "spanSets": [{
                "spans": [span("1111111111111111")],
                "matched": 1
            }]
        })
    })
    .collect();
    axum::Json(json!({
        "traces": traces,
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

async fn overflow_trace_search_response() -> axum::Json<Value> {
    let traces: Vec<Value> = (0..25)
        .map(|i| {
            json!({
                "traceID": format!("{i:032x}"),
                "rootServiceName": "svc",
                "rootTraceName": "root",
                "startTimeUnixNano": i.to_string(),
                "durationMs": 2,
                "spanSets": [{
                    "spans": [span("1111111111111111")],
                    "matched": 1
                }]
            })
        })
        .collect();
    axum::Json(json!({
        "traces": traces,
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

async fn span_pair_search_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    // Same trace from both shards; each shard contributes two distinct spans, so
    // the merge reunions four spans into one spanSet (matched = 4).
    let span_ids = if is_backend_shard(uri.query().unwrap_or_default()) {
        ["3333333333333333", "4444444444444444"]
    } else {
        ["1111111111111111", "2222222222222222"]
    };
    let spans: Vec<Value> = span_ids.into_iter().map(span).collect();
    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": "1000000000",
            "durationMs": 2,
            "spanSets": [{
                "spans": spans,
                "matched": 2
            }]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

async fn slow_search_response() -> axum::Json<Value> {
    tokio::time::sleep(Duration::from_millis(100)).await;
    axum::Json(json!({
        "traces": [],
        "metrics": { "totalBlocks": 0, "inspectedTraces": 0, "inspectedBytes": 0 }
    }))
}

async fn query_echo_search_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": uri.query().unwrap_or_default(),
            "startTimeUnixNano": "0",
            "durationMs": 1,
            "spanSets": [{
                "spans": [span("1111111111111111")],
                "matched": 1
            }]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 100 }
    }))
}

async fn sharded_metrics_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    let (samples, exemplar) = if is_backend_shard(uri.query().unwrap_or_default()) {
        (
            json!([
                {"timestampMs": "1000000000", "value": 1.0},
                {"timestampMs": "1999999999", "value": 2.0}
            ]),
            json!({
                "labels": [{"key": "trace_id", "value": {"stringValue": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}],
                "timestampMs": "1000000000",
                "value": 1.0
            }),
        )
    } else {
        (
            json!([
                {"timestampMs": "2000000000", "value": 3.0},
                {"timestampMs": "3000000000", "value": 4.0}
            ]),
            json!({
                "labels": [{"key": "trace_id", "value": {"stringValue": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}],
                "timestampMs": "2000000000",
                "value": 3.0
            }),
        )
    };
    axum::Json(json!({
        "series": [{
            "labels": [{"key": "svc", "value": {"stringValue": "api"}}],
            "promLabels": "{svc=\"api\"}",
            "samples": samples,
            "exemplars": [exemplar],
        }]
    }))
}

async fn record_metrics(
    State(seen): State<Arc<Mutex<Vec<String>>>>,
    uri: Uri,
) -> axum::Json<Value> {
    seen.lock()
        .unwrap()
        .push(uri.query().unwrap_or_default().to_string());
    axum::Json(json!({
        "series": [{
            "labels": [{"key": "svc", "value": {"stringValue": "api"}}],
            "promLabels": "{svc=\"api\"}",
            "samples": [
                {"timestampMs": "1000000000", "value": 1.0},
                {"timestampMs": "2000000000", "value": 2.0}
            ],
            "exemplars": [],
        }]
    }))
}

async fn sharded_tags_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    let (tag, inspected_bytes) = if is_backend_shard(uri.query().unwrap_or_default()) {
        ("backend.tag", 10)
    } else {
        ("live.tag", 20)
    };
    axum::Json(json!({
        "scopes": [{
            "name": "span",
            "tags": [tag]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 0, "inspectedBytes": inspected_bytes }
    }))
}

async fn concurrent_tags_response(
    State(barrier): State<Arc<Barrier>>,
    uri: Uri,
) -> axum::Json<Value> {
    let backend = is_backend_shard(uri.query().unwrap_or_default());
    barrier.wait().await;
    let tag = if backend { "backend.tag" } else { "live.tag" };
    axum::Json(json!({
        "scopes": [{
            "name": "span",
            "tags": [tag]
        }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 0, "inspectedBytes": 1 }
    }))
}

async fn sharded_tag_values_response(State(()): State<()>, uri: Uri) -> axum::Json<Value> {
    let value = if is_backend_shard(uri.query().unwrap_or_default()) {
        "backend"
    } else {
        "live"
    };
    axum::Json(json!({
        "tagValues": [{ "type": "string", "value": value }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 5, "inspectedBytes": 0 }
    }))
}

async fn concurrent_tag_values_response(
    State(barrier): State<Arc<Barrier>>,
    uri: Uri,
) -> axum::Json<Value> {
    let backend = is_backend_shard(uri.query().unwrap_or_default());
    barrier.wait().await;
    let value = if backend { "backend" } else { "live" };
    axum::Json(json!({
        "tagValues": [{ "type": "string", "value": value }],
        "metrics": { "totalBlocks": 1, "inspectedTraces": 1, "inspectedBytes": 0 }
    }))
}

// --- error propagation: an upstream querier error must not be swallowed -------

async fn reject_search() -> (StatusCode, String) {
    (
        StatusCode::BAD_REQUEST,
        "parse error: unexpected token".to_string(),
    )
}

/// A search shard that fails, for example on an invalid `TraceQL` query, must
/// surface the querier's status and body. It must not degrade to a silent empty
/// `200`. Search shards partition the data, so a failed shard means missing
/// results.
#[tokio::test]
async fn search_propagates_upstream_querier_error() {
    let upstream = spawn(Router::new().route("/api/search", get(reject_search))).await;
    let router = build_router(&upstream, two_shard_cfg(), single_block_catalog());

    let response = router
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20bad%20%7D&start=0&end=10")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert2::assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert2::assert!(text.contains("parse error: unexpected token"));
}
