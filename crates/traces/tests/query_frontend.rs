use assert2::assert;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, Uri};
use axum::routing::get;
use crabka_traces::query_frontend::{
    QueryFrontendConfig, QueryShard, QueryTier, plan_time_shards, router,
};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
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
                },
                QueryShard {
                    tier: QueryTier::Live,
                    start_ns: 60,
                    end_ns: 100,
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
            }]
    );
    assert!(
        plan_time_shards(60, 100, Some(60))
            == vec![QueryShard {
                tier: QueryTier::Live,
                start_ns: 60,
                end_ns: 100,
            }]
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
