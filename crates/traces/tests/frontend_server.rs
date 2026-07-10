//! The frontend axum router round-trips the Tempo endpoints over loopback,
//! backed by `MockQuerier` + `MockCatalog`.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use crabka_traces::frontend::{
    QueryFrontend,
    backend::{MockQuerier, SearchPartial, TracePartial},
    config::FrontendConfig,
    job::{BlockMetaInfo, MockCatalog, RowGroupInfo},
    server::router_with_backend,
    wire::{
        Metrics, OtlpSpanJson, ResourceSpansJson, ScopeSpansJson, SpanSetJson,
        TraceByIdResponseJson, TraceEnvelopeJson, TraceJson,
    },
};

fn block(id: &str) -> BlockMetaInfo {
    BlockMetaInfo {
        block_id: id.to_string(),
        start_ns: 0,
        end_ns: 100,
        size_bytes: 10,
        row_groups: vec![RowGroupInfo {
            index: 0,
            compressed_bytes: 10,
        }],
    }
}

async fn spawn(app: axum::Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

#[tokio::test]
async fn server_round_trips_search_and_echo() {
    let catalog = MockCatalog::new(vec![block("b1")]);
    let backend = MockQuerier::new();
    backend.stub_search(SearchPartial {
        traces: vec![TraceJson {
            trace_id: "01".repeat(16),
            root_service_name: "svc".to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: "1".to_string(),
            duration_ms: 1,
            span_sets: vec![SpanSetJson {
                spans: vec![],
                matched: 0,
            }],
        }],
        metrics: Metrics {
            completed_jobs: 1,
            ..Metrics::default()
        },
    });
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let addr = spawn(router_with_backend(qf)).await;
    let client = reqwest::Client::new();

    let echo = client
        .get(format!("http://{addr}/api/echo"))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    let status = echo.status();
    let body = echo.text().await.unwrap();
    assert!(status.is_success());
    assert_eq!(body.as_str(), "echo");

    let url = format!("http://{addr}/api/search?q=%7B%20%7D&start=0&end=100&limit=20&spss=3");
    let resp = client
        .get(url)
        .header("X-Scope-OrgID", "t1")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    check!(
        (
            &body["traces"][0]["traceID"],
            &body["metrics"]["completedJobs"],
            &body["metrics"]["totalBlocks"],
        ) == (
            &serde_json::json!("01".repeat(16)),
            &serde_json::json!(1),
            &serde_json::json!(1),
        )
    );
}

#[tokio::test]
async fn server_search_requires_query() {
    let catalog = MockCatalog::new(vec![]);
    let backend = MockQuerier::new();
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        FrontendConfig::default(),
    ));
    let addr = spawn(router_with_backend(qf)).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/search?start=0&end=1"))
        .send()
        .await
        .unwrap();
    assert!(resp.status() == reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn server_by_id_returns_v2_envelope() {
    let catalog = MockCatalog::new(vec![block("b1")]);
    let backend = MockQuerier::with_querier_count(1);
    let mut span_rest = serde_json::Map::new();
    span_rest.insert("name".to_string(), serde_json::json!("op"));
    backend.stub_trace(TracePartial {
        trace: TraceByIdResponseJson {
            trace: TraceEnvelopeJson {
                resource_spans: vec![ResourceSpansJson {
                    resource: serde_json::Value::Null,
                    scope_spans: vec![ScopeSpansJson {
                        scope: serde_json::Value::Null,
                        spans: vec![OtlpSpanJson {
                            span_id: "BgYGBgYGBgY=".to_string(),
                            rest: span_rest,
                        }],
                    }],
                }],
            },
            status: "COMPLETE".to_string(),
            message: String::new(),
        },
        metrics: Metrics {
            completed_jobs: 1,
            ..Metrics::default()
        },
    });
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let addr = spawn(router_with_backend(qf)).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("http://{addr}/api/v2/traces/{}", "0a".repeat(16)))
        .header("X-Scope-OrgID", "t1")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        (
            &body["status"],
            &body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["spanId"]
        ) == (
            &serde_json::json!("COMPLETE"),
            &serde_json::json!("BgYGBgYGBgY=")
        )
    );
}

#[tokio::test]
async fn server_by_id_404_when_missing() {
    let catalog = MockCatalog::new(vec![block("b1")]);
    let backend = MockQuerier::with_querier_count(1);
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let addr = spawn(router_with_backend(qf)).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/v2/traces/{}", "0a".repeat(16)))
        .send()
        .await
        .unwrap();
    assert!(resp.status() == reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn server_tags_round_trip() {
    let catalog = MockCatalog::new(vec![block("b1")]);
    let backend = MockQuerier::new();
    backend.stub_tag_names(crabka_traces::frontend::backend::TagNamesPartial {
        tags: vec![crabka_traceql::ScopedTag {
            scope: crabka_traceql::TagScope::Span,
            tags: vec!["http.method".to_string()],
        }],
        metrics: Metrics {
            completed_jobs: 1,
            ..Metrics::default()
        },
    });
    let cfg = FrontendConfig {
        hot_frontier_ns: i64::MAX,
        ..FrontendConfig::default()
    };
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let addr = spawn(router_with_backend(qf)).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/api/v2/search/tags?start=0&end=100"))
        .header("X-Scope-OrgID", "t1")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["scopes"][0]
            == serde_json::json!({
                "name": "span",
                "tags": ["http.method"],
            })
    );
}
