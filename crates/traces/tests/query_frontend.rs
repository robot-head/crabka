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
    assert!(json["tenant"] == "tenant-a");
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
        "tenant": headers
            .get("x-scope-orgid")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default(),
    }))
}
