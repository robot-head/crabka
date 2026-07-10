#![recursion_limit = "512"]

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use crabka_blockstore::Labels;
use crabka_metrics::{BucketSpan, Limits, NativeHistogram, OverridesProvider, ResetHint, wire::pb};
use crabka_promql::{
    EngineOpts, InMemoryMetricStore, PrometheusApiState, QueryFrontendOptions,
    RulerAlertStateRecord, RulerGroupStateRecord, prometheus_router,
};
use prost::Message;
use serde_json::Value;
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use tower::ServiceExt;

const RULE_GROUP_YAML: &str = "
name: latency
interval: 30s
rules:
  - record: job:http_request_duration_seconds:p99
    expr: histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))
  - alert: HighLatency
    expr: job:http_request_duration_seconds:p99 > 1
    for: 5m
    labels:
      severity: page
    annotations:
      summary: high latency
";

fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}

async fn response_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    serde_json::from_slice(&bytes).expect("json response")
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    String::from_utf8(bytes.to_vec()).expect("utf8 response")
}

#[tokio::test]
async fn query_endpoint_returns_prometheus_vector_envelope() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("vector"));
    assert_eq!(
        body["data"]["result"][0]["metric"]["__name__"].as_str(),
        Some("up")
    );
    assert_eq!(
        body["data"]["result"][0]["metric"]["job"].as_str(),
        Some("api")
    );
    assert_eq!(
        // Prometheus MarshalTimestamp emits whole seconds as a bare JSON integer.
        body["data"]["result"][0]["value"][0].as_i64(),
        Some(10)
    );
    assert_eq!(body["data"]["result"][0]["value"][1].as_str(), Some("1"));
}

#[tokio::test]
async fn query_endpoint_accepts_rfc3339_time_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=1970-01-01T00%3A00%3A10Z")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("vector"));
    assert_eq!(body["data"]["result"][0]["value"][0].as_i64(), Some(10));
    assert_eq!(body["data"]["result"][0]["value"][1].as_str(), Some("1"));
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_envelope() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 10.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=request_duration_seconds&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("vector"));
    assert!(body["data"]["result"][0].get("value").is_none());
    assert_eq!(
        body["data"]["result"][0]["metric"]["__name__"].as_str(),
        Some("request_duration_seconds")
    );
    assert_eq!(body["data"]["result"][0]["histogram"][0].as_i64(), Some(10));
    assert_eq!(
        body["data"]["result"][0]["histogram"][1]["count"].as_str(),
        Some("4")
    );
    assert_eq!(
        body["data"]["result"][0]["histogram"][1]["sum"].as_str(),
        Some("10")
    );
    assert_eq!(
        body["data"]["result"][0]["histogram"][1]["buckets"].clone(),
        serde_json::json!([])
    );
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_buckets() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.25,
            zero_count: 3.0,
            count: 10.0,
            sum: 7.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![2.0, 4.0],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=request_duration_seconds&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["data"]["result"][0]["histogram"][1]["buckets"],
        serde_json::json!([
            [1, "-1", "-0.5", "1"],
            [3, "-0.25", "0.25", "3"],
            [0, "0.5", "1", "2"],
            [0, "1", "2", "4"],
        ])
    );
}

#[tokio::test]
async fn query_endpoint_returns_native_histogram_custom_buckets() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        NativeHistogram {
            schema: -53,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 6.0,
            sum: 2.3,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![1.0, 2.0, 3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: Some(vec![0.1, 0.5]),
            start_timestamp_ms: None,
        },
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=request_duration_seconds&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["data"]["result"][0]["histogram"][1]["buckets"],
        serde_json::json!([
            [0, "-Inf", "0.1", "1"],
            [0, "0.1", "0.5", "2"],
            [0, "0.5", "+Inf", "3"],
        ])
    );
}

#[tokio::test]
async fn query_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("query=up&time=10"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("vector"));
    assert_eq!(
        body["data"]["result"][0]["metric"]["__name__"].as_str(),
        Some("up")
    );
    assert_eq!(body["data"]["result"][0]["value"][0].as_i64(), Some(10));
    assert_eq!(body["data"]["result"][0]["value"][1].as_str(), Some("1"));
}

#[tokio::test]
async fn query_endpoint_honors_limit_parameter_for_vectors() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "vector");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn query_endpoint_treats_zero_limit_as_disabled() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10&limit=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "vector");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn query_endpoint_rejects_invalid_limit_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10&limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "invalid limit parameter");
}

#[tokio::test]
async fn query_range_endpoint_is_available_under_mimir_prefix() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/query_range?query=up&start=60&end=120&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("matrix"));
    assert_eq!(
        body["data"]["result"][0]["metric"]["job"].as_str(),
        Some("api")
    );
    assert_eq!(
        body["data"]["result"][0]["values"].clone(),
        serde_json::json!([[60, "1"], [120, "2"]])
    );
}

#[tokio::test]
async fn matrix_integral_second_ts_is_bare_integer() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=60&end=60&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let text = response_text(response).await;
    // Byte-exact: a whole-second timestamp is a bare integer, never `60.0`.
    assert!(
        text.contains("[60,\"1\"]"),
        "expected bare-integer timestamp token, got: {text}"
    );
    assert!(
        !text.contains("60.0"),
        "whole-second timestamp must not be a JSON float, got: {text}"
    );

    let body: Value = serde_json::from_str(&text).expect("json response");
    let ts = &body["data"]["result"][0]["values"][0][0];
    assert_eq!(ts, 60);
}

#[tokio::test]
async fn query_range_endpoint_can_use_query_frontend_split_and_merge() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0, 1.0), (60_000, 2.0), (120_000, 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", "api")]),
            ts_ms,
            value,
        );
    }
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 1,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=120&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"]["job"], "api");
    assert_eq!(body["data"]["result"][0]["values"][0][1], "1");
    assert_eq!(body["data"]["result"][0]["values"][1][1], "2");
    assert_eq!(body["data"]["result"][0]["values"][2][1], "3");
}

fn labels_on_two_query_shards() -> (Labels, Labels) {
    let mut shard_one = None;
    let mut shard_two = None;
    for id in 0..100 {
        let candidate = labels(&[("__name__", "up"), ("series", &id.to_string())]);
        if candidate.fingerprint().is_multiple_of(2) && shard_one.is_none() {
            shard_one = Some(candidate);
        } else if candidate.fingerprint() % 2 == 1 && shard_two.is_none() {
            shard_two = Some(candidate);
        }
        if shard_one.is_some() && shard_two.is_some() {
            break;
        }
    }
    (
        shard_one.expect("series for first query shard"),
        shard_two.expect("series for second query shard"),
    )
}

fn labels_on_uneven_query_shards() -> (Labels, Labels, Labels) {
    let mut first_even = None;
    let mut second_even = None;
    let mut odd = None;
    for id in 0..100 {
        let candidate = labels(&[("__name__", "up"), ("series", &id.to_string())]);
        if candidate.fingerprint().is_multiple_of(2) {
            if first_even.is_none() {
                first_even = Some(candidate);
            } else if second_even.is_none() {
                second_even = Some(candidate);
            }
        } else if odd.is_none() {
            odd = Some(candidate);
        }
        if first_even.is_some() && second_even.is_some() && odd.is_some() {
            break;
        }
    }
    (
        first_even.expect("first series for first query shard"),
        second_even.expect("second series for first query shard"),
        odd.expect("series for second query shard"),
    )
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_sum() {
    let (shard_one, shard_two) = labels_on_two_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", shard_one, 0, 1.0);
    store.push_float("tenant-a", shard_two, 0, 2.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=sum%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "3");
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_avg() {
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", first_even, 0, 2.0);
    store.push_float("tenant-a", second_even, 0, 10.0);
    store.push_float("tenant-a", odd, 0, 3.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=avg%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "5");
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_stdvar() {
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", first_even, 0, 2.0);
    store.push_float("tenant-a", second_even, 0, 10.0);
    store.push_float("tenant-a", odd, 0, 3.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=stdvar%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    let value = body["data"]["result"][0]["values"][0][1]
        .as_str()
        .expect("stdvar sample value")
        .parse::<f64>()
        .expect("stdvar sample parses as float");
    assert!((value - (38.0 / 3.0)).abs() < 1e-9);
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_topk() {
    let (first_even, second_even, odd) = labels_on_uneven_query_shards();
    let expected_high = second_even
        .get("series")
        .expect("high value series label")
        .to_string();
    let expected_mid = odd
        .get("series")
        .expect("mid value series label")
        .to_string();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", first_even, 0, 2.0);
    store.push_float("tenant-a", second_even, 0, 10.0);
    store.push_float("tenant-a", odd, 0, 3.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=topk%282%2C%20up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    let mut selected = body["data"]["result"]
        .as_array()
        .expect("topk result array")
        .iter()
        .map(|series| {
            (
                series["metric"]["series"]
                    .as_str()
                    .expect("series label")
                    .to_string(),
                series["values"][0][1]
                    .as_str()
                    .expect("sample value")
                    .parse::<f64>()
                    .expect("sample value parses"),
            )
        })
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut expected = vec![(expected_high, 10.0), (expected_mid, 3.0)];
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(selected, expected);
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_min() {
    let (shard_one, shard_two) = labels_on_two_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", shard_one, 0, 5.0);
    store.push_float("tenant-a", shard_two, 0, 2.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=min%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "2");
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_max() {
    let (shard_one, shard_two) = labels_on_two_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", shard_one, 0, 5.0);
    store.push_float("tenant-a", shard_two, 0, 2.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=max%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "5");
}

#[tokio::test]
async fn query_range_endpoint_query_frontend_reduces_sharded_group() {
    let (shard_one, shard_two) = labels_on_two_query_shards();

    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", shard_one, 0, 5.0);
    store.push_float("tenant-a", shard_two, 0, 2.0);
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default()).with_query_frontend(
            QueryFrontendOptions {
                split_interval_ms: 60_000,
                shard_count: 2,
            },
        ),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=group%28up%29&start=0&end=0&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["metric"], serde_json::json!({}));
    assert_eq!(body["data"]["result"][0]["values"][0][0], 0);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "1");
}

#[tokio::test]
async fn query_range_endpoint_honors_limit_parameter_for_matrices() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        60_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=60&end=60&step=60&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn query_range_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("query=up&start=60&end=120&step=60"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["resultType"].as_str(), Some("matrix"));
    assert_eq!(
        body["data"]["result"][0]["metric"]["job"].as_str(),
        Some("api")
    );
    assert_eq!(
        body["data"]["result"][0]["values"].clone(),
        serde_json::json!([[60, "1"], [120, "2"]])
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_duration_literal_step() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        2.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=60&end=120&step=1m")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");
    assert_eq!(body["data"]["result"][0]["values"][0][0], 60);
    assert_eq!(body["data"]["result"][0]["values"][0][1], "1");
    assert_eq!(body["data"]["result"][0]["values"][1][0], 120);
    assert_eq!(body["data"]["result"][0]["values"][1][1], "2");
}

#[tokio::test]
async fn query_range_endpoint_rejects_end_before_start() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=120&end=60&step=60")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "end timestamp must not be before start time");
}

#[tokio::test]
async fn query_range_endpoint_rejects_invalid_limit_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=60&end=120&step=60&limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "invalid limit parameter");
}

#[tokio::test]
async fn query_range_endpoint_returns_prometheus_error_for_missing_step() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=60&end=120")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "missing step parameter");
}

#[tokio::test]
async fn query_endpoint_requires_scope_org_id() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
}

#[tokio::test]
async fn query_endpoint_returns_422_when_max_samples_is_exceeded() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts {
            max_samples: 1,
            ..EngineOpts::default()
        },
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert_eq!(
        body["error"],
        "execution error: query exceeds max_samples=1"
    );
}

#[tokio::test]
async fn query_endpoint_applies_runtime_max_samples_per_query() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        1.0,
    );
    let limits = Limits {
        max_samples_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=up&time=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert_eq!(
        body["error"],
        "execution error: query exceeds max_samples=1"
    );
}

#[tokio::test]
async fn series_endpoint_returns_matching_label_sets() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match%5B%5D=up%7Bjob%3D%22api%22%7D&start=10&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(body["data"][0]["__name__"], "up");
    assert_eq!(body["data"][0]["job"], "api");
    assert_eq!(body["data"][0]["instance"], "a");
}

#[tokio::test]
async fn series_endpoint_accepts_or_label_matchers() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match%5B%5D=up%7Bjob%3D%22api%22%20or%20job%3D%22web%22%7D&start=10&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    let data = body["data"].as_array().expect("data array");
    assert_eq!(data.len(), 2);
    let jobs = data
        .iter()
        .map(|series| series["job"].as_str().expect("job"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(jobs, std::collections::BTreeSet::from(["api", "web"]));
}

#[tokio::test]
async fn series_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/series")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "match%5B%5D=up%7Bjob%3D%22api%22%7D&start=10&end=10",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(body["data"][0]["__name__"], "up");
    assert_eq!(body["data"][0]["job"], "api");
    assert_eq!(body["data"][0]["instance"], "a");
}

#[tokio::test]
async fn series_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match%5B%5D=up&start=10&end=10&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
}

#[tokio::test]
async fn series_endpoint_rejects_invalid_limit_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "invalid limit parameter");
}

#[tokio::test]
async fn series_endpoint_rejects_end_before_start() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/series?match%5B%5D=up&start=20&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "end timestamp must not be before start time");
}

#[tokio::test]
async fn labels_endpoint_returns_label_names_for_matchers() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/labels?match%5B%5D=up&start=10&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"],
        serde_json::json!(["__name__", "instance", "job"])
    );
}

#[tokio::test]
async fn labels_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("match%5B%5D=up&start=10&end=10"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"],
        serde_json::json!(["__name__", "instance", "job"])
    );
}

#[tokio::test]
async fn labels_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/labels?match%5B%5D=up&start=10&end=10&limit=2")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!(["__name__", "instance"]));
}

#[tokio::test]
async fn label_values_endpoint_is_available_under_mimir_prefix() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/label/job/values?match%5B%5D=up&start=10&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!(["api", "web"]));
}

#[tokio::test]
async fn label_values_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/label/job/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("match%5B%5D=up&start=10&end=10"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!(["api", "web"]));
}

#[tokio::test]
async fn label_values_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "web"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("job", job)]),
            10_000,
            1.0,
        );
    }
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/label/job/values?match%5B%5D=up&start=10&end=10&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!(["api"]));
}

#[tokio::test]
async fn metadata_endpoint_is_available_under_mimir_prefix() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/metadata")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!({}));
}

#[tokio::test]
async fn metadata_endpoint_returns_metric_metadata() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata(
        "tenant-b",
        "http_requests_total",
        "gauge",
        "Wrong tenant.",
        "",
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/metadata?metric=http_requests_total")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["http_requests_total"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(body["data"]["http_requests_total"][0]["type"], "counter");
    assert_eq!(
        body["data"]["http_requests_total"][0]["help"],
        "Total HTTP requests."
    );
    assert_eq!(body["data"]["http_requests_total"][0]["unit"], "requests");
}

#[tokio::test]
async fn metadata_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/metadata?limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_object().unwrap().len(), 1);
    assert_eq!(body["data"]["http_requests_total"][0]["type"], "counter");
}

#[tokio::test]
async fn metadata_endpoint_treats_zero_limit_as_disabled() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/metadata?limit=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_object().unwrap().len(), 2);
}

#[tokio::test]
async fn metadata_endpoint_rejects_invalid_limit_parameter_with_prometheus_error() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/metadata?limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert_eq!(body["error"], "invalid limit parameter");
}

#[tokio::test]
async fn metadata_endpoint_honors_limit_per_metric_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "HTTP requests from another target.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/metadata?limit_per_metric=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"].as_object().unwrap().len(), 2);
    assert_eq!(
        body["data"]["http_requests_total"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(body["data"]["up"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn rules_endpoint_returns_empty_groups() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["groups"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn rules_endpoint_rejects_invalid_exclude_alerts_parameter_with_prometheus_error() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?exclude_alerts=maybe")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(
        body["error"].as_str(),
        Some("invalid exclude_alerts parameter")
    );
}

#[tokio::test]
async fn rules_endpoint_returns_loaded_recording_rules() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let group = &body["data"]["groups"][0];
    let rule = &group["rules"][0];
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(group["file"].as_str(), Some("team-a"));
    assert_eq!(group["interval"].as_i64(), Some(30));
    assert_eq!(
        group["lastEvaluation"].as_str(),
        Some("0001-01-01T00:00:00Z")
    );
    assert_eq!(group["evaluationTime"].as_f64(), Some(0.0));
    assert_eq!(group["lastError"].as_str(), Some(""));
    assert_eq!(group["limit"].as_i64(), Some(0));
    assert_eq!(group["name"].as_str(), Some("latency"));
    assert_eq!(
        rule["lastEvaluation"].as_str(),
        Some("0001-01-01T00:00:00Z")
    );
    assert_eq!(rule["evaluationTime"].as_f64(), Some(0.0));
    assert_eq!(rule["lastError"].as_str(), Some(""));
    assert_eq!(rule["health"].as_str(), Some("ok"));
    assert_eq!(
        rule["name"].as_str(),
        Some("job:http_request_duration_seconds:p99")
    );
    assert_eq!(
        rule["query"].as_str(),
        Some(
            "histogram_quantile(0.99, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))"
        )
    );
    assert_eq!(rule["type"].as_str(), Some("recording"));
}

#[tokio::test]
async fn rules_endpoint_reports_group_last_evaluation_from_ruler_state() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(Arc::clone(&state));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    state.apply_ruler_group_state(RulerGroupStateRecord {
        tenant: "tenant-a".to_string(),
        namespace: "team-a".to_string(),
        group: "latency".to_string(),
        last_eval_ms: 90_000,
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["data"]["groups"][0]["lastEvaluation"],
        "1970-01-01T00:01:30Z"
    );
}

#[tokio::test]
async fn rules_endpoint_filters_by_rule_type() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alert_rules = body["data"]["groups"][0]["rules"].as_array().unwrap();
    assert_eq!(alert_rules.len(), 1);
    assert_eq!(alert_rules[0]["type"].as_str(), Some("alerting"));
    assert_eq!(alert_rules[0]["name"].as_str(), Some("HighLatency"));
    assert_eq!(alert_rules[0]["duration"].as_i64(), Some(300));
    assert_eq!(alert_rules[0]["labels"]["severity"].as_str(), Some("page"));
    assert_eq!(
        alert_rules[0]["annotations"]["summary"].as_str(),
        Some("high latency")
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=record")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let recording_rules = body["data"]["groups"][0]["rules"].as_array().unwrap();
    assert_eq!(recording_rules.len(), 1);
    assert_eq!(recording_rules[0]["type"].as_str(), Some("recording"));
    assert_eq!(
        recording_rules[0]["name"].as_str(),
        Some("job:http_request_duration_seconds:p99")
    );
}

#[tokio::test]
async fn rules_endpoint_rejects_invalid_type_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=notify")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(body["error"].as_str(), Some("invalid type parameter"));
}

#[tokio::test]
async fn rules_endpoint_can_exclude_alert_payloads() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alert_rule = &body["data"]["groups"][0]["rules"][0];
    assert!(alert_rule.get("alerts").is_some());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert&exclude_alerts=true")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alert_rule = &body["data"]["groups"][0]["rules"][0];
    assert_eq!(alert_rule["type"].as_str(), Some("alerting"));
    assert_eq!(alert_rule["name"].as_str(), Some("HighLatency"));
    assert!(alert_rule.get("alerts").is_none());
}

#[tokio::test]
async fn rules_endpoint_embeds_evaluated_alerts() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        0,
        1.0,
    );
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    labels:
      severity: page
    annotations:
      summary: instance down
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let rule = &body["data"]["groups"][0]["rules"][0];
    let alerts = rule["alerts"].as_array().unwrap();
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(rule["name"].as_str(), Some("InstanceDown"));
    assert_eq!(
        rule["lastEvaluation"].as_str(),
        Some("1970-01-01T00:00:00Z")
    );
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        alerts[0]["labels"]["alertname"].as_str(),
        Some("InstanceDown")
    );
    assert_eq!(alerts[0]["labels"]["job"].as_str(), Some("api"));
    assert_eq!(alerts[0]["labels"]["instance"].as_str(), Some("a"));
    assert_eq!(alerts[0]["labels"]["severity"].as_str(), Some("page"));
    assert_eq!(
        alerts[0]["annotations"]["summary"].as_str(),
        Some("instance down")
    );
    assert_eq!(alerts[0]["state"].as_str(), Some("firing"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));
    assert_eq!(alerts[0]["value"].as_str(), Some("1"));
}

#[tokio::test]
async fn rules_endpoint_expands_value_and_labels_in_alert_templates() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        0,
        2.0,
    );
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    labels:
      detail: 'v={{ $value }}'
    annotations:
      summary: '{{ $labels.job }} is {{ $value }}'
      passthrough: '{{ humanize $value }}'
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alerts = body["data"]["groups"][0]["rules"][0]["alerts"]
        .as_array()
        .unwrap();
    // $labels and $value expanded; unknown actions pass through verbatim.
    assert_eq!(alerts.len(), 1);
    assert_eq!(
        alerts[0]["annotations"]["summary"].as_str(),
        Some("api is 2")
    );
    assert_eq!(
        alerts[0]["annotations"]["passthrough"].as_str(),
        Some("{{ humanize $value }}")
    );
    assert_eq!(alerts[0]["labels"]["detail"].as_str(), Some("v=2"));
}

#[tokio::test]
async fn rules_endpoint_reports_alert_evaluation_errors_per_rule() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: unsupported
rules:
  - alert: UnsupportedAlert
    expr: up @ start()
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules?type=alert")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    let rule = &body["data"]["groups"][0]["rules"][0];
    assert_eq!(rule["name"].as_str(), Some("UnsupportedAlert"));
    assert_eq!(rule["health"].as_str(), Some("err"));
    assert!(
        rule["lastError"]
            .as_str()
            .is_some_and(|error| error.contains("start"))
    );
    assert_eq!(rule["alerts"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn alerts_endpoint_returns_empty_alerts_under_mimir_prefix() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["alerts"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn alerts_endpoint_omits_inactive_configured_alerting_rules() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert!(alerts.is_empty());
}

#[tokio::test]
async fn alerts_endpoint_evaluates_alerting_rules() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        0,
        1.0,
    );
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    labels:
      severity: page
    annotations:
      summary: instance down
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["name"].as_str(), Some("InstanceDown"));
    assert_eq!(alerts[0]["query"].as_str(), Some("up > 0"));
    assert_eq!(alerts[0]["duration"].as_i64(), Some(0));
    assert_eq!(
        alerts[0]["labels"]["alertname"].as_str(),
        Some("InstanceDown")
    );
    assert_eq!(alerts[0]["labels"]["job"].as_str(), Some("api"));
    assert_eq!(alerts[0]["labels"]["instance"].as_str(), Some("a"));
    assert_eq!(alerts[0]["labels"]["severity"].as_str(), Some("page"));
    assert_eq!(
        alerts[0]["annotations"]["summary"].as_str(),
        Some("instance down")
    );
    assert_eq!(alerts[0]["state"].as_str(), Some("firing"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));
    assert_eq!(alerts[0]["value"].as_str(), Some("1"));
}

#[tokio::test]
async fn alerts_endpoint_marks_for_duration_alerts_pending() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        0,
        1.0,
    );
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["duration"].as_i64(), Some(300));
    assert_eq!(alerts[0]["state"].as_str(), Some("pending"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));
    assert_eq!(alerts[0]["value"].as_str(), Some("1"));
}

#[tokio::test]
async fn alerts_endpoint_fires_for_duration_alerts_after_active_duration() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        0,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        300_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(Arc::clone(&state));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["state"].as_str(), Some("pending"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));

    state.set_ruler_evaluation_time_ms(300_000);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["duration"].as_i64(), Some(300));
    assert_eq!(alerts[0]["state"].as_str(), Some("firing"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));
    assert_eq!(alerts[0]["value"].as_str(), Some("1"));
}

#[tokio::test]
async fn alerts_endpoint_replays_compacted_alert_state() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        300_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    state.set_ruler_evaluation_time_ms(300_000);
    let app = prometheus_router(Arc::clone(&state));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: availability
rules:
  - alert: InstanceDown
    expr: up > 0
    for: 5m
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let alert_labels = BTreeMap::from([
        ("alertname".to_string(), "InstanceDown".to_string()),
        ("instance".to_string(), "a".to_string()),
        ("job".to_string(), "api".to_string()),
    ]);
    state.apply_ruler_alert_state(RulerAlertStateRecord {
        tenant: "tenant-a".to_string(),
        rule_id: "InstanceDown\nup > 0".to_string(),
        labels: alert_labels,
        active_since_ms: Some(0),
    });

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let alerts = body["data"]["alerts"].as_array().unwrap();
    assert_eq!(alerts.len(), 1);
    assert_eq!(alerts[0]["state"].as_str(), Some("firing"));
    assert_eq!(alerts[0]["activeAt"].as_str(), Some("1970-01-01T00:00:00Z"));
}

#[tokio::test]
async fn alertmanagers_endpoint_returns_empty_discovery_lists() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alertmanagers")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(
        body["data"]["activeAlertmanagers"].clone(),
        serde_json::json!([])
    );
    assert_eq!(
        body["data"]["droppedAlertmanagers"].clone(),
        serde_json::json!([])
    );
}

#[tokio::test]
async fn targets_endpoint_returns_empty_discovery_lists() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/targets")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["activeTargets"].clone(), serde_json::json!([]));
    assert_eq!(
        body["data"]["droppedTargets"].clone(),
        serde_json::json!([])
    );
    assert_eq!(
        body["data"]["droppedTargetCounts"].clone(),
        serde_json::json!({})
    );
}

#[tokio::test]
async fn scrape_pools_endpoint_returns_empty_pool_list() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/scrape_pools")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn target_metadata_endpoint_returns_empty_metadata_list() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/targets/metadata?metric=up&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn target_metadata_endpoint_returns_metric_metadata() {
    let mut store = InMemoryMetricStore::new();
    store.push_metadata(
        "tenant-a",
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    store.push_metadata("tenant-a", "up", "gauge", "Target health.", "");
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/targets/metadata?metric=http_requests_total&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        body["data"][0].clone(),
        serde_json::json!({
            "target": {},
            "metric": "http_requests_total",
            "type": "counter",
            "help": "Total HTTP requests.",
            "unit": "requests",
        })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_post_form_body() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/format_query")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("query=sum%28up%29"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].as_str(), Some("sum(up)"));
}

#[tokio::test]
async fn parse_query_endpoint_accepts_post_form_body() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/parse_query")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("query=up"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["type"].as_str(), Some("vectorSelector"));
    assert_eq!(body["data"]["name"].as_str(), Some("up"));
    assert_eq!(body["data"]["matchers"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn ruler_config_rules_crud_round_trips_yaml_groups() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(RULE_GROUP_YAML))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/config/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["Content-Type"].to_str().unwrap(),
        "application/yaml"
    );
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("ruler yaml");
    assert_eq!(yaml["team-a"][0]["name"].as_str(), Some("latency"));
    assert_eq!(yaml["team-a"][0]["interval"].as_str(), Some("30s"));
    assert_eq!(
        yaml["team-a"][0]["rules"][0]["record"].as_str(),
        Some("job:http_request_duration_seconds:p99")
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/config/v1/rules/team-a/latency")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("group yaml");
    assert_eq!(yaml["name"], "latency");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/config/v1/rules")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("tenant yaml");
    assert_eq!(yaml, serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/prometheus/config/v1/rules/team-a/latency")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn ruler_config_rejects_invalid_rule_groups() {
    let app = prometheus_router(Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    )));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: broken
rules:
  - record: missing_expr
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("expr"))
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/config/v1/rules/team-a")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/yaml")
                .body(Body::from(
                    "
name: broken
rules:
  - record: bad_query
    expr: sum(
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("PromQL"))
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/config/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&response_text(response).await).expect("tenant yaml");
    assert_eq!(yaml, serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
}

#[tokio::test]
async fn query_exemplars_endpoint_returns_empty_list() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_exemplars?query=up&start=10&end=20")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"], serde_json::json!([]));
}

#[tokio::test]
async fn query_exemplars_endpoint_returns_matching_exemplars() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "web")]),
        labels(&[("trace_id", "ignored")]),
        10_500,
        9.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_exemplars?query=http_requests_total%7Bjob%3D%22api%22%7D&start=10&end=11")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(
        body["data"][0]["seriesLabels"]["__name__"].as_str(),
        Some("http_requests_total")
    );
    assert_eq!(body["data"][0]["seriesLabels"]["job"].as_str(), Some("api"));
    assert_eq!(
        body["data"][0]["exemplars"][0]["labels"]["trace_id"].as_str(),
        Some("abc")
    );
    assert_eq!(
        body["data"][0]["exemplars"][0]["labels"]["span_id"].as_str(),
        Some("def")
    );
    assert_eq!(body["data"][0]["exemplars"][0]["value"].as_str(), Some("7"));
    assert_eq!(
        body["data"][0]["exemplars"][0]["timestamp"].as_f64(),
        Some(10.5)
    );
}

#[tokio::test]
async fn query_exemplars_endpoint_accepts_or_label_matchers() {
    let mut store = InMemoryMetricStore::new();
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "api")]),
        10_500,
        7.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "web")]),
        labels(&[("trace_id", "web")]),
        10_600,
        9.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "db")]),
        labels(&[("trace_id", "db")]),
        10_700,
        11.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_exemplars?query=http_requests_total%7Bjob%3D%22api%22%20or%20job%3D%22web%22%7D&start=10&end=11")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"], "success");
    let data = body["data"].as_array().expect("data array");
    assert_eq!(data.len(), 2);
    let jobs = data
        .iter()
        .map(|series| series["seriesLabels"]["job"].as_str().expect("job"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(jobs, std::collections::BTreeSet::from(["api", "web"]));
}

#[tokio::test]
async fn query_exemplars_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[("__name__", "http_requests_total"), ("job", "api")]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query_exemplars")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=http_requests_total%7Bjob%3D%22api%22%7D&start=10&end=11",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(body["data"][0]["seriesLabels"]["job"].as_str(), Some("api"));
    assert_eq!(
        body["data"][0]["exemplars"][0]["labels"]["trace_id"].as_str(),
        Some("abc")
    );
    assert_eq!(
        body["data"][0]["exemplars"][0]["timestamp"].as_f64(),
        Some(10.5)
    );
}

#[tokio::test]
async fn query_exemplars_endpoint_rejects_end_before_start() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_exemplars?query=up&start=20&end=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(
        body["error"].as_str(),
        Some("end timestamp must not be before start time")
    );
}

#[tokio::test]
async fn remote_read_endpoint_returns_snappy_protobuf_response() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: Vec::new(),
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["Content-Type"].to_str().unwrap(),
        "application/x-protobuf"
    );
    assert_eq!(
        response.headers()["Content-Encoding"].to_str().unwrap(),
        "snappy"
    );
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let decoded = SnappyDecoder::new()
        .decompress_vec(&bytes)
        .expect("snappy response");
    let read_response =
        pb::v1::ReadResponse::decode(decoded.as_slice()).expect("remote read response");
    assert!(read_response.results.is_empty());
}

#[tokio::test]
async fn remote_read_endpoint_accepts_listed_snappy_content_encoding() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: Vec::new(),
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "identity, snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn remote_read_endpoint_rejects_end_before_start() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 20_000,
            end_timestamp_ms: 10_000,
            matchers: Vec::new(),
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(
        body["error"].as_str(),
        Some("end timestamp must not be before start time")
    );
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_float_samples() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        30_000,
        3.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        20_000,
        9.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 20_000,
            matchers: vec![
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                },
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "job".into(),
                    value: "api".into(),
                },
            ],
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let decoded = SnappyDecoder::new()
        .decompress_vec(&bytes)
        .expect("snappy response");
    let read_response =
        pb::v1::ReadResponse::decode(decoded.as_slice()).expect("remote read response");
    let series = &read_response.results[0].timeseries[0];
    assert_eq!(read_response.results.len(), 1);
    assert_eq!(read_response.results[0].timeseries.len(), 1);
    assert_eq!(
        series
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("__name__", "up"), ("instance", "a"), ("job", "api")]
    );
    assert_eq!(
        series
            .samples
            .iter()
            .map(|sample| (sample.timestamp, sample.value))
            .collect::<Vec<_>>(),
        vec![(10_000, 1.0), (20_000, 2.0)]
    );
}

#[tokio::test]
async fn remote_read_endpoint_rejects_selected_series_over_tenant_limit() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let limits = Limits {
        max_fetched_series_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 10_000,
            matchers: vec![
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                },
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "job".into(),
                    value: "api".into(),
                },
            ],
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("execution"));
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("series per query exceeded"))
    );
}

#[tokio::test]
async fn remote_read_endpoint_rejects_samples_over_tenant_limit() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        20_000,
        2.0,
    );
    let limits = Limits {
        max_samples_per_query: 1,
        ..Limits::default()
    };
    let state = Arc::new(
        PrometheusApiState::new(Arc::new(store), EngineOpts::default())
            .with_query_limits(OverridesProvider::new(limits)),
    );
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 20_000,
            matchers: vec![
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                },
                pb::v1::LabelMatcher {
                    r#type: pb::v1::label_matcher::Type::Eq as i32,
                    name: "job".into(),
                    value: "api".into(),
                },
            ],
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("execution"));
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains("samples per query exceeded"))
    );
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_exemplars() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        1.0,
    );
    store.push_exemplar(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        labels(&[("trace_id", "abc"), ("span_id", "def")]),
        10_500,
        7.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 11_000,
            matchers: vec![pb::v1::LabelMatcher {
                r#type: pb::v1::label_matcher::Type::Eq as i32,
                name: "__name__".into(),
                value: "http_requests_total".into(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let decoded = SnappyDecoder::new()
        .decompress_vec(&bytes)
        .expect("snappy response");
    let read_response =
        pb::v1::ReadResponse::decode(decoded.as_slice()).expect("remote read response");
    let series = &read_response.results[0].timeseries[0];
    assert_eq!(series.exemplars.len(), 1);
    assert_eq!(series.exemplars[0].timestamp, 10_500);
    assert!((series.exemplars[0].value - 7.0).abs() < f64::EPSILON);
    assert_eq!(
        series.exemplars[0]
            .labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("span_id", "def"), ("trace_id", "abc")]
    );
}

#[tokio::test]
async fn remote_read_endpoint_returns_matching_native_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 10.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: vec![pb::v1::Query {
            start_timestamp_ms: 10_000,
            end_timestamp_ms: 10_000,
            matchers: vec![pb::v1::LabelMatcher {
                r#type: pb::v1::label_matcher::Type::Eq as i32,
                name: "__name__".into(),
                value: "request_duration_seconds".into(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![pb::v1::ResponseType::Samples as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024)
        .await
        .expect("response body");
    let decoded = SnappyDecoder::new()
        .decompress_vec(&bytes)
        .expect("snappy response");
    let read_response =
        pb::v1::ReadResponse::decode(decoded.as_slice()).expect("remote read response");
    let series = &read_response.results[0].timeseries[0];
    assert_eq!(read_response.results.len(), 1);
    assert_eq!(read_response.results[0].timeseries.len(), 1);
    assert!(series.samples.is_empty());
    assert_eq!(series.histograms.len(), 1);
    assert_eq!(series.histograms[0].timestamp, 10_000);
    assert!((series.histograms[0].sum - 10.0).abs() < f64::EPSILON);
    assert_eq!(
        &series.histograms[0].count,
        &Some(pb::v1::histogram::Count::CountFloat(4.0))
    );
}

#[tokio::test]
async fn remote_read_endpoint_rejects_unsupported_streamed_xor_response_type() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);
    let request = pb::v1::ReadRequest {
        queries: Vec::new(),
        accepted_response_types: vec![pb::v1::ResponseType::StreamedXorChunks as i32],
    };
    let compressed = SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy request");

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/read")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-protobuf")
                .header("Content-Encoding", "snappy")
                .body(Body::from(compressed))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("execution"));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_returns_label_name_counts() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "other"), ("zone", "hidden")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_names")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    // Mimir returns the cardinality object directly, with no status envelope.
    assert!(body.get("status").is_none());
    assert_eq!(body["label_names_count"].as_i64(), Some(3));
    assert_eq!(body["label_values_count_total"].as_i64(), Some(4));
    assert_eq!(
        body["cardinality"].clone(),
        serde_json::json!([
            {"label_name": "job", "label_values_count": 2},
            {"label_name": "__name__", "label_values_count": 1},
            {"label_name": "instance", "label_values_count": 1},
        ])
    );
}

#[tokio::test]
async fn cardinality_label_names_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_names?limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    // job has two distinct values, so it sorts first under the limit.
    assert_eq!(
        body["cardinality"]
            .as_array()
            .expect("cardinality array")
            .len(),
        1
    );
    assert_eq!(body["cardinality"][0]["label_name"].as_str(), Some("job"));
    assert_eq!(
        body["cardinality"][0]["label_values_count"].as_i64(),
        Some(2)
    );
}

#[tokio::test]
async fn cardinality_label_names_endpoint_filters_selector_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("zone", "us")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_names?selector=up%7Bjob%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["label_names_count"].as_i64(), Some(3));
    assert_eq!(body["label_values_count_total"].as_i64(), Some(3));
    assert_eq!(
        body["cardinality"].clone(),
        serde_json::json!([
            {"label_name": "__name__", "label_values_count": 1},
            {"label_name": "instance", "label_values_count": 1},
            {"label_name": "job", "label_values_count": 1},
        ])
    );
}

#[tokio::test]
async fn cardinality_label_names_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/cardinality/label_names")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("limit=1"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(
        body["cardinality"]
            .as_array()
            .expect("cardinality array")
            .len(),
        1
    );
    assert_eq!(body["cardinality"][0]["label_name"].as_str(), Some("job"));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_rejects_invalid_limit_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_names?limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(body["error"].as_str(), Some("invalid limit parameter"));
}

#[tokio::test]
async fn cardinality_label_names_endpoint_accepts_documented_count_methods() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    for count_method in ["inmemory", "active"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/v1/cardinality/label_names?count_method={count_method}"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(
            body["cardinality"]
                .as_array()
                .expect("cardinality array")
                .len(),
            2
        );
    }
}

#[tokio::test]
async fn cardinality_label_names_endpoint_rejects_invalid_count_method_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_names?count_method=blocks")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(
        body["error"].as_str(),
        Some("invalid count_method parameter")
    );
}

#[tokio::test]
async fn cardinality_active_series_endpoint_returns_series_labels_under_mimir_prefix() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "hidden"), ("instance", "z")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/cardinality/active_series")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    // Mimir active_series returns a bare object whose `data` array holds flat
    // label maps -- no status envelope, no seriesLabels/metric wrapper.
    assert!(body.get("status").is_none());
    assert_eq!(
        body["data"].clone(),
        serde_json::json!([
            {"__name__": "up", "instance": "a", "job": "api"},
            {"__name__": "up", "instance": "b", "job": "web"},
        ])
    );
}

#[tokio::test]
async fn cardinality_active_series_endpoint_filters_selector_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/prometheus/api/v1/cardinality/active_series?selector=up%7Bjob%3D%22api%22%7D",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert!(body.get("status").is_none());
    assert_eq!(
        body["data"].clone(),
        serde_json::json!([
            {"__name__": "up", "instance": "a", "job": "api"},
        ])
    );
}

#[tokio::test]
async fn cardinality_active_series_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/cardinality/active_series?limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert!(body.get("status").is_none());
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
}

#[tokio::test]
async fn cardinality_active_series_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/prometheus/api/v1/cardinality/active_series")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("selector=up%7Bjob%3D%22api%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert!(body.get("status").is_none());
    assert_eq!(
        body["data"].clone(),
        serde_json::json!([
            {"__name__": "up", "instance": "a", "job": "api"},
        ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_returns_label_value_counts() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "hidden"), ("instance", "z")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_values")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    // Mimir nests per-value cardinality under each label, with no envelope.
    assert!(body.get("status").is_none());
    assert_eq!(body["series_count_total"].as_i64(), Some(3));
    assert_eq!(
        body["labels"].clone(),
        serde_json::json!([
        {
            "label_name": "__name__",
            "label_values_count": 1,
            "series_count": 3,
            "cardinality": [{"label_value": "up", "series_count": 3}],
        },
        {
            "label_name": "instance",
            "label_values_count": 3,
            "series_count": 3,
            "cardinality": [
                {"label_value": "a", "series_count": 1},
                {"label_value": "b", "series_count": 1},
                {"label_value": "c", "series_count": 1},
            ],
        },
        {
            "label_name": "job",
            "label_values_count": 2,
            "series_count": 3,
            "cardinality": [
                {"label_value": "api", "series_count": 2},
                {"label_value": "web", "series_count": 1},
            ],
        },
        ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_filters_label_names_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_values?label_names%5B%5D=job")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["series_count_total"].as_i64(), Some(2));
    assert_eq!(
        body["labels"].clone(),
        serde_json::json!([
            {
                "label_name": "job",
                "label_values_count": 2,
                "series_count": 2,
                "cardinality": [
                    {"label_value": "api", "series_count": 1},
                    {"label_value": "web", "series_count": 1},
                ],
            },
        ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_filters_selector_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("zone", "us")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_values?selector=up%7Bjob%3D%22api%22%7D&label_names%5B%5D=job")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["series_count_total"].as_i64(), Some(1));
    assert_eq!(
        body["labels"].clone(),
        serde_json::json!([
            {
                "label_name": "job",
                "label_values_count": 1,
                "series_count": 1,
                "cardinality": [{"label_value": "api", "series_count": 1}],
            },
        ])
    );
}

#[tokio::test]
async fn cardinality_label_values_endpoint_honors_limit_parameter() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_values?limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    // limit caps each label's nested per-value cardinality array.
    let labels = body["labels"].as_array().expect("labels array");
    assert!(!labels.is_empty());
    for label in labels {
        assert_eq!(
            label["cardinality"]
                .as_array()
                .expect("cardinality array")
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn cardinality_label_values_endpoint_accepts_post_form_body() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/cardinality/label_values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(Body::from("limit=2"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    let labels = body["labels"].as_array().expect("labels array");
    // __name__ has the highest series_count, so it sorts first.
    assert!(body.get("status").is_none());
    assert_eq!(body["series_count_total"].as_i64(), Some(2));
    assert_eq!(labels.len(), 3);
    assert_eq!(labels[0]["label_name"].as_str(), Some("__name__"));
}

#[tokio::test]
async fn cardinality_label_values_endpoint_rejects_invalid_limit_parameter() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/cardinality/label_values?limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(body["error"].as_str(), Some("invalid limit parameter"));
}

#[tokio::test]
async fn format_query_endpoint_returns_formatted_expression() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/format_query?query=foo%2Fbar")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"].as_str(), Some("foo / bar"));
}

#[tokio::test]
async fn parse_query_endpoint_is_available_under_mimir_prefix() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/parse_query?query=up%7Bjob%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["type"].as_str(), Some("vectorSelector"));
    assert_eq!(body["data"]["name"].as_str(), Some("up"));
    assert_eq!(
        body["data"]["matchers"].clone(),
        serde_json::json!([
            {
                "name": "job",
                "type": "=",
                "value": "api"
            }
        ])
    );
}

#[tokio::test]
async fn parse_query_endpoint_returns_prometheus_error_for_missing_query() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/parse_query")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(body["error"].as_str(), Some("missing query parameter"));
}

#[tokio::test]
async fn status_buildinfo_endpoint_returns_prometheus_envelope() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/buildinfo")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(
        body["data"]["version"].as_str(),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(body["data"]["branch"].as_str(), Some(""));
    assert_eq!(body["data"]["goVersion"].as_str(), Some(""));
}

#[tokio::test]
async fn status_flags_endpoint_returns_prometheus_flag_strings() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/flags")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["query.lookback-delta"].as_str(), Some("5m"));
    assert!(body["data"]["log.level"].as_str().is_some());
}

#[tokio::test]
async fn status_config_endpoint_is_available_under_mimir_prefix() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/status/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert!(
        body["data"]["yaml"]
            .as_str()
            .is_some_and(|yaml| yaml.contains("global:"))
    );
}

#[tokio::test]
async fn status_tsdb_endpoint_returns_tenant_cardinality_stats() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        20_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "api")]),
        30_000,
        3.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "hidden"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/tsdb?limit=2")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["headStats"]["numSeries"].as_i64(), Some(3));
    assert_eq!(body["data"]["headStats"]["minTime"].as_i64(), Some(10_000));
    assert_eq!(body["data"]["headStats"]["maxTime"].as_i64(), Some(30_000));
    assert_eq!(
        body["data"]["seriesCountByMetricName"].clone(),
        serde_json::json!([
            {"name": "up", "value": 2},
            {"name": "errors_total", "value": 1},
        ])
    );
    assert_eq!(
        body["data"]["labelValueCountByLabelName"].clone(),
        serde_json::json!([
            {"name": "__name__", "value": 2},
            {"name": "instance", "value": 2},
        ])
    );
    assert_eq!(
        body["data"]["seriesCountByLabelValuePair"].clone(),
        serde_json::json!([
            {"name": "__name__=up", "value": 2},
            {"name": "job=api", "value": 2},
        ])
    );
}

#[tokio::test]
async fn status_tsdb_endpoint_rejects_invalid_limit_parameter_with_prometheus_error() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/tsdb?limit=abc")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("error"));
    assert_eq!(body["errorType"].as_str(), Some("bad_data"));
    assert_eq!(body["error"].as_str(), Some("invalid limit parameter"));
}

#[tokio::test]
async fn status_tsdb_blocks_endpoint_returns_empty_block_list() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/status/tsdb/blocks")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["blocks"].clone(), serde_json::json!([]));
}

#[tokio::test]
async fn status_tsdb_blocks_endpoint_returns_compacted_blocks() {
    let mut store = InMemoryMetricStore::new();
    store.push_tsdb_block(
        "tenant-a",
        "metrics/tenant-a/float/0001.parquet",
        10_000,
        70_000,
        42,
        3,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/tsdb/blocks")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(
        body["data"]["blocks"][0]["ulid"].as_str(),
        Some("metrics/tenant-a/float/0001.parquet")
    );
    assert_eq!(body["data"]["blocks"][0]["minTime"].as_i64(), Some(10_000));
    assert_eq!(body["data"]["blocks"][0]["maxTime"].as_i64(), Some(70_000));
    assert_eq!(
        body["data"]["blocks"][0]["stats"]["numSamples"].as_i64(),
        Some(42)
    );
    assert_eq!(
        body["data"]["blocks"][0]["stats"]["numSeries"].as_i64(),
        Some(3)
    );
}

#[tokio::test]
async fn status_walreplay_endpoint_reports_done() {
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/status/walreplay")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert_eq!(body["data"]["min"].as_i64(), Some(0));
    assert_eq!(body["data"]["max"].as_i64(), Some(0));
    assert_eq!(body["data"]["current"].as_i64(), Some(0));
    assert_eq!(body["data"]["state"].as_str(), Some("done"));
}

#[tokio::test]
async fn status_runtimeinfo_endpoint_is_available_under_mimir_prefix() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "errors_total"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "hidden"), ("job", "ignored")]),
        10_000,
        1.0,
    );
    let state = Arc::new(PrometheusApiState::new(
        Arc::new(store),
        EngineOpts::default(),
    ));
    let app = prometheus_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/status/runtimeinfo")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_json(response).await;
    assert_eq!(body["status"].as_str(), Some("success"));
    assert!(body["data"]["startTime"].as_str().is_some());
    assert!(body["data"]["serverTime"].as_str().is_some());
    assert_eq!(body["data"]["reloadConfigSuccess"].as_bool(), Some(true));
    assert_eq!(body["data"]["timeSeriesCount"].as_i64(), Some(2));
}
