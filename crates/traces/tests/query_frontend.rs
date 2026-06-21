use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use assert2::assert;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::IntoResponse;
use axum::routing::get;
use crabka_traces::query_frontend::{
    BackendBlock, BackendRowGroup, QueryFrontendConfig, QueryShard, QueryTier, plan_query_shards,
    plan_time_shards, router,
};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tokio::sync::Barrier;
use tokio::time::timeout;
use tower::ServiceExt as _;

#[test]
fn plan_time_shards_splits_search_at_live_frontier() {
    let shards = plan_time_shards(0, 100, Some(60));

    assert!(
        shards
            == vec![
                QueryShard {
                    tier: QueryTier::Backend,
                    start_ns: 0,
                    end_ns: 59,
                    backend_job: None,
                },
                QueryShard {
                    tier: QueryTier::Live,
                    start_ns: 60,
                    end_ns: 100,
                    backend_job: None,
                },
            ]
    );
}

#[test]
fn plan_time_shards_keeps_single_tier_windows_intact() {
    assert!(
        plan_time_shards(0, 50, Some(60))
            == vec![QueryShard {
                tier: QueryTier::Backend,
                start_ns: 0,
                end_ns: 50,
                backend_job: None,
            }]
    );
    assert!(
        plan_time_shards(60, 100, Some(60))
            == vec![QueryShard {
                tier: QueryTier::Live,
                start_ns: 60,
                end_ns: 100,
                backend_job: None,
            }]
    );
}

#[test]
fn plan_query_shards_groups_backend_row_groups_by_target_bytes() {
    let shards = plan_query_shards(
        0,
        100,
        Some(80),
        100,
        &[
            BackendBlock {
                object_key: "blocks/a.parquet".into(),
                min_time_ns: 0,
                max_time_ns: 50,
                row_groups: vec![
                    BackendRowGroup {
                        index: 0,
                        compressed_bytes: 40,
                    },
                    BackendRowGroup {
                        index: 1,
                        compressed_bytes: 60,
                    },
                    BackendRowGroup {
                        index: 2,
                        compressed_bytes: 40,
                    },
                ],
            },
            BackendBlock {
                object_key: "blocks/outside.parquet".into(),
                min_time_ns: 150,
                max_time_ns: 200,
                row_groups: vec![BackendRowGroup {
                    index: 0,
                    compressed_bytes: 40,
                }],
            },
        ],
    );

    assert!(shards.len() == 3);
    assert!(shards[0].tier == QueryTier::Backend);
    assert!(shards[0].backend_job.as_ref().unwrap().object_key == "blocks/a.parquet");
    assert!(shards[0].backend_job.as_ref().unwrap().row_group_start == 0);
    assert!(shards[0].backend_job.as_ref().unwrap().row_group_end == 2);
    assert!(shards[1].tier == QueryTier::Backend);
    assert!(shards[1].backend_job.as_ref().unwrap().row_group_start == 2);
    assert!(shards[1].backend_job.as_ref().unwrap().row_group_end == 3);
    assert!(
        shards[2]
            == QueryShard {
                tier: QueryTier::Live,
                start_ns: 80,
                end_ns: 100,
                backend_job: None,
            }
    );
}

#[tokio::test]
async fn frontend_proxies_by_id_route_with_tenant_and_query() {
    let upstream_url = spawn_recording_querier().await;
    let cfg = QueryFrontendConfig::new(&upstream_url).unwrap();

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/traces/0123456789abcdef0123456789abcdef?start=1&end=2")
                .header("accept", "application/protobuf")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["path"] == "/api/v2/traces/0123456789abcdef0123456789abcdef");
    assert!(json["query"] == "start=1&end=2");
    assert!(json["accept"] == "application/protobuf");
    assert!(json["tenant"] == "tenant-a");
}

#[tokio::test]
async fn frontend_merges_duplicate_trace_results_across_shards() {
    let upstream_url = spawn_sharded_search_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["traces"].as_array().unwrap().len() == 1);
    assert!(json["traces"][0]["traceID"] == "0123456789abcdef0123456789abcdef");
    let span_sets = json["traces"][0]["spanSets"].as_array().unwrap();
    assert!(span_sets.len() == 1);
    assert!(span_sets[0]["matched"] == 2);
    assert!(span_sets[0]["spans"].as_array().unwrap().len() == 2);
    assert!(json["metrics"]["totalBlocks"] == 3);
    assert!(json["metrics"]["inspectedTraces"] == 12);
}

#[tokio::test]
async fn frontend_dispatches_search_shards_concurrently() {
    let upstream_url = spawn_concurrent_search_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = timeout(
        Duration::from_millis(500),
        router(cfg).oneshot(
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["traces"][0]["spanSets"][0]["spans"]
            .as_array()
            .unwrap()
            .len()
            == 2
    );
}

#[tokio::test]
async fn frontend_deduplicates_spans_across_shards() {
    let upstream_url = spawn_overlapping_search_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let span_sets = json["traces"][0]["spanSets"].as_array().unwrap();
    let spans = span_sets[0]["spans"].as_array().unwrap();

    assert!(json["traces"].as_array().unwrap().len() == 1);
    assert!(span_sets.len() == 1);
    assert!(span_sets[0]["matched"] == 1);
    assert!(spans.len() == 1);
    assert!(spans[0]["spanID"] == "1111111111111111");
}

#[tokio::test]
async fn frontend_forwards_backend_row_group_job_to_querier() {
    let upstream_url = spawn_query_echo_search_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.target_bytes_per_job = 100;
    cfg.backend_blocks = vec![BackendBlock {
        object_key: "blocks/a.parquet".into(),
        min_time_ns: 0,
        max_time_ns: 10_000_000_000,
        row_groups: vec![
            BackendRowGroup {
                index: 0,
                compressed_bytes: 40,
            },
            BackendRowGroup {
                index: 1,
                compressed_bytes: 60,
            },
        ],
    }];

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=10")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let received_query = json["traces"][0]["rootTraceName"].as_str().unwrap();

    assert!(received_query.contains("block=blocks%2Fa.parquet"));
    assert!(received_query.contains("rowGroupStart=0"));
    assert!(received_query.contains("rowGroupEnd=2"));
}

#[tokio::test]
async fn frontend_uses_tenant_specific_backend_row_group_jobs() {
    let upstream_url = spawn_query_echo_search_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.target_bytes_per_job = 100;
    cfg.backend_blocks_by_tenant = BTreeMap::from([
        (
            "tenant-a".to_string(),
            vec![BackendBlock {
                object_key: "blocks/tenant-a.parquet".into(),
                min_time_ns: 0,
                max_time_ns: 10_000_000_000,
                row_groups: vec![BackendRowGroup {
                    index: 0,
                    compressed_bytes: 100,
                }],
            }],
        ),
        (
            "tenant-b".to_string(),
            vec![BackendBlock {
                object_key: "blocks/tenant-b.parquet".into(),
                min_time_ns: 0,
                max_time_ns: 10_000_000_000,
                row_groups: vec![BackendRowGroup {
                    index: 2,
                    compressed_bytes: 100,
                }],
            }],
        ),
    ]);

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=10")
                .header("x-scope-orgid", "tenant-b")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let received_query = json["traces"][0]["rootTraceName"].as_str().unwrap();

    assert!(received_query.contains("block=blocks%2Ftenant-b.parquet"));
    assert!(received_query.contains("rowGroupStart=2"));
    assert!(received_query.contains("rowGroupEnd=3"));
}

#[tokio::test]
async fn frontend_shards_metrics_query_range_across_live_frontier() {
    let upstream_url = spawn_sharded_metrics_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["series"].as_array().unwrap().len() == 1);
    assert!(
        json["series"][0]["points"]
            == json!([
                ["1000000000", 1.0],
                ["1999999999", 2.0],
                ["2000000000", 3.0],
                ["3000000000", 4.0],
            ])
    );
    assert!(json["series"][0]["exemplars"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_dispatches_metric_shards_concurrently() {
    let upstream_url = spawn_concurrent_metrics_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = timeout(
        Duration::from_millis(500),
        router(cfg).oneshot(
            axum::http::Request::builder()
                .uri(
                    "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=1&end=3&step=1",
                )
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("sharded query_range should not serialize shard requests")
    .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["series"][0]["points"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_limits_merged_metric_exemplars_across_shards() {
    let upstream_url = spawn_sharded_metrics_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["series"][0]["exemplars"].as_array().unwrap().len() == 1);
}

#[tokio::test]
async fn frontend_shards_instant_metrics_query_across_live_frontier() {
    let upstream_url = spawn_sharded_metrics_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(json["series"].as_array().unwrap().len() == 1);
    assert!(
        json["series"][0]["points"]
            == json!([
                ["1000000000", 1.0],
                ["1999999999", 2.0],
                ["2000000000", 3.0],
                ["3000000000", 4.0],
            ])
    );
    assert!(json["series"][0]["exemplars"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_dispatches_instant_metric_shards_concurrently() {
    let upstream_url = spawn_concurrent_metrics_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = timeout(
        Duration::from_millis(500),
        router(cfg).oneshot(
            axum::http::Request::builder()
                .uri(
                    "/api/metrics/query?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=1&end=3",
                )
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        ),
    )
    .await
    .expect("sharded instant metrics should not serialize shard requests")
    .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["series"][0]["points"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_shards_v2_tag_discovery_across_live_frontier() {
    let upstream_url = spawn_sharded_tags_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tags?scope=span&start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(
        json["scopes"]
            == json!([
                {
                    "name": "span",
                    "tags": [
                        { "name": "backend.tag", "type": "string" },
                        { "name": "live.tag", "type": "string" },
                    ]
                }
            ])
    );
    assert!(json["metrics"]["totalBlocks"] == 3);
    assert!(json["metrics"]["inspectedBytes"] == 30);
}

#[tokio::test]
async fn frontend_dispatches_tag_discovery_shards_concurrently() {
    let upstream_url = spawn_concurrent_tags_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = timeout(
        Duration::from_millis(500),
        router(cfg).oneshot(
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["scopes"][0]["tags"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_shards_v2_tag_values_across_live_frontier() {
    let upstream_url = spawn_sharded_tag_values_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = router(cfg)
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v2/search/tag/.svc/values?start=1&end=3")
                .header("x-scope-orgid", "tenant-a")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert!(
        json["tagValues"]
            == json!([
                { "type": "string", "value": "backend" },
                { "type": "string", "value": "live" },
            ])
    );
    assert!(json["metrics"]["totalBlocks"] == 3);
    assert!(json["metrics"]["inspectedTraces"] == 12);
}

#[tokio::test]
async fn frontend_dispatches_tag_value_shards_concurrently() {
    let upstream_url = spawn_concurrent_tag_values_querier().await;
    let mut cfg = QueryFrontendConfig::new(&upstream_url).unwrap();
    cfg.live_frontier_ns = Some(2_000_000_000);

    let response = timeout(
        Duration::from_millis(500),
        router(cfg).oneshot(
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

    assert!(response.status().is_success());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(json["tagValues"].as_array().unwrap().len() == 2);
}

#[tokio::test]
async fn frontend_search_requires_valid_start_and_end() {
    let upstream_url = spawn_sharded_search_querier().await;
    let cfg = QueryFrontendConfig::new(&upstream_url).unwrap();

    let (status, body) = get_text(
        router(cfg.clone()),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D",
    )
    .await;
    assert!(status == axum::http::StatusCode::BAD_REQUEST);
    assert!(body == "missing query parameter start");

    let (status, body) = get_text(
        router(cfg.clone()),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0",
    )
    .await;
    assert!(status == axum::http::StatusCode::BAD_REQUEST);
    assert!(body == "missing query parameter end");

    let (status, body) = get_text(
        router(cfg),
        "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=bogus&end=1",
    )
    .await;
    assert!(status == axum::http::StatusCode::BAD_REQUEST);
    assert!(body == "invalid query parameter start");
}

#[tokio::test]
async fn frontend_search_preserves_upstream_text_errors() {
    let upstream_url = spawn_text_error_querier().await;
    let cfg = QueryFrontendConfig::new(&upstream_url).unwrap();

    let (status, body) = get_text(router(cfg), "/api/search?q=%7B&start=0&end=1").await;

    assert!(status == StatusCode::BAD_REQUEST);
    assert!(body == "parse error: expected selector");
}

async fn get_text(app: Router, uri: &str) -> (axum::http::StatusCode, String) {
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

async fn spawn_recording_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(record_request))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn record_request(State(()): State<()>, headers: HeaderMap, uri: Uri) -> axum::Json<Value> {
    axum::Json(json!({
        "path": uri.path(),
        "query": uri.query().unwrap_or_default(),
        "accept": headers
            .get("accept")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
        "tenant": headers
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
    }))
}

async fn spawn_sharded_search_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(sharded_search_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_concurrent_search_querier() -> String {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/{*path}", get(concurrent_search_response))
        .with_state(barrier);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_overlapping_search_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(overlapping_search_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_query_echo_search_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(query_echo_search_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_sharded_metrics_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(sharded_metrics_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_concurrent_metrics_querier() -> String {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/{*path}", get(concurrent_metrics_response))
        .with_state(barrier);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_sharded_tags_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(sharded_tags_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_concurrent_tags_querier() -> String {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/{*path}", get(concurrent_tags_response))
        .with_state(barrier);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_sharded_tag_values_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(sharded_tag_values_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_concurrent_tag_values_querier() -> String {
    let barrier = Arc::new(Barrier::new(2));
    let app = Router::new()
        .route("/{*path}", get(concurrent_tag_values_response))
        .with_state(barrier);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn spawn_text_error_querier() -> String {
    let app = Router::new()
        .route("/{*path}", get(text_error_response))
        .with_state(());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

async fn sharded_search_response(State(()): State<()>, headers: HeaderMap) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let (span_id, total_blocks, inspected_traces) = match tier {
        "backend" => ("1111111111111111", 1, 5),
        "live" => ("2222222222222222", 2, 7),
        other => panic!("unexpected query tier {other}"),
    };

    axum::Json(json!({
        "traces": [{
            "traceID": "0123456789abcdef0123456789abcdef",
            "rootServiceName": "svc",
            "rootTraceName": "root",
            "startTimeUnixNano": "1000000000",
            "durationMs": 2,
            "spanSets": [{
                "spans": [{ "spanID": span_id }],
                "matched": 1
            }]
        }],
        "metrics": {
            "totalBlocks": total_blocks,
            "inspectedTraces": inspected_traces,
            "inspectedBytes": 0
        }
    }))
}

async fn concurrent_search_response(
    State(barrier): State<Arc<Barrier>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    barrier.wait().await;
    let span_id = if tier == "backend" {
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
                "spans": [{ "spanID": span_id }],
                "matched": 1
            }]
        }],
        "metrics": {
            "totalBlocks": 1,
            "inspectedTraces": 1,
            "inspectedBytes": 0
        }
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
                "spans": [{ "spanID": "1111111111111111" }],
                "matched": 1
            }]
        }],
        "metrics": {
            "totalBlocks": 1,
            "inspectedTraces": 1,
            "inspectedBytes": 0
        }
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
                "spans": [{ "spanID": "1111111111111111" }],
                "matched": 1
            }]
        }],
        "metrics": {
            "totalBlocks": 1,
            "inspectedTraces": 1,
            "inspectedBytes": 100
        }
    }))
}

async fn sharded_metrics_response(State(()): State<()>, headers: HeaderMap) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let (points, exemplar) = match tier {
        "backend" => (
            json!([["1000000000", 1.0], ["1999999999", 2.0]]),
            json!({
                "labels": { "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
                "timestamp": "1000000000",
                "value": 1.0
            }),
        ),
        "live" => (
            json!([["2000000000", 3.0], ["3000000000", 4.0]]),
            json!({
                "labels": { "trace_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
                "timestamp": "2000000000",
                "value": 3.0
            }),
        ),
        other => panic!("unexpected query tier {other}"),
    };

    axum::Json(json!({
        "series": [{
            "labels": { "svc": "api" },
            "points": points,
            "exemplars": [exemplar],
        }]
    }))
}

async fn concurrent_metrics_response(
    State(barrier): State<Arc<Barrier>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    barrier.wait().await;
    let timestamp = if tier == "backend" {
        "1000000000"
    } else {
        "2000000000"
    };

    axum::Json(json!({
        "series": [{
            "labels": { "svc": "api" },
            "points": [[timestamp, 1.0]],
            "exemplars": [],
        }]
    }))
}

async fn sharded_tags_response(State(()): State<()>, headers: HeaderMap) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let (tag, total_blocks, inspected_bytes) = match tier {
        "backend" => ("backend.tag", 1, 10),
        "live" => ("live.tag", 2, 20),
        _ => ("unsharded.tag", 99, 99),
    };

    axum::Json(json!({
        "scopes": [{
            "name": "span",
            "tags": [{ "name": tag, "type": "string" }]
        }],
        "metrics": {
            "totalBlocks": total_blocks,
            "inspectedTraces": 0,
            "inspectedBytes": inspected_bytes
        }
    }))
}

async fn concurrent_tags_response(
    State(barrier): State<Arc<Barrier>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    barrier.wait().await;
    let tag = if tier == "backend" {
        "backend.tag"
    } else {
        "live.tag"
    };

    axum::Json(json!({
        "scopes": [{
            "name": "span",
            "tags": [{ "name": tag, "type": "string" }]
        }],
        "metrics": {
            "totalBlocks": 1,
            "inspectedTraces": 0,
            "inspectedBytes": 1
        }
    }))
}

async fn sharded_tag_values_response(
    State(()): State<()>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let (value, total_blocks, inspected_traces) = match tier {
        "backend" => ("backend", 1, 5),
        "live" => ("live", 2, 7),
        _ => ("unsharded", 99, 99),
    };

    axum::Json(json!({
        "tagValues": [{ "type": "string", "value": value }],
        "metrics": {
            "totalBlocks": total_blocks,
            "inspectedTraces": inspected_traces,
            "inspectedBytes": 0
        }
    }))
}

async fn concurrent_tag_values_response(
    State(barrier): State<Arc<Barrier>>,
    headers: HeaderMap,
) -> axum::Json<Value> {
    let tier = headers
        .get("x-crabka-query-tier")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    barrier.wait().await;
    let value = if tier == "backend" { "backend" } else { "live" };

    axum::Json(json!({
        "tagValues": [{ "type": "string", "value": value }],
        "metrics": {
            "totalBlocks": 1,
            "inspectedTraces": 1,
            "inspectedBytes": 0
        }
    }))
}

async fn text_error_response() -> impl IntoResponse {
    (StatusCode::BAD_REQUEST, "parse error: expected selector")
}
