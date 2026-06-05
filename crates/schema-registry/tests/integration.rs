#![cfg(not(target_os = "windows"))]

//! End-to-end gate for schema-registry slice 1: REST -> register -> produce to
//! `_schemas` -> group-less reader replay -> in-memory store -> GET, against a
//! real in-process Crabka broker. Drives the registry's REST router via `tower`'s
//! `oneshot` (no live HTTP socket needed) while the `KafkaStore` talks to the
//! broker over the wire.
//!
//! `flavor = "multi_thread", worker_threads = 2` is required: a single-threaded
//! runtime can't drive the broker's accept loop concurrently with the registry's
//! producer/reader tasks and the test body.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::config::RegistryConfig;
use crabka_schema_registry::kafkastore::KafkaStore;
use crabka_schema_registry::rest::{self, AppState};
use tokio_util::sync::CancellationToken;

async fn boot_registry(
    rf: i32,
) -> (
    crabka_broker::BrokerHandle,
    std::sync::Arc<KafkaStore>,
    CancellationToken,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let cfg = RegistryConfig {
        bootstrap: broker.listen_addr().to_string(),
        schemas_topic: "_schemas".into(),
        schemas_topic_rf: rf,
        client_id: "sr-it".into(),
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    (broker, store, cancel, dir)
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let b = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    serde_json::from_slice(&b).unwrap()
}

/// POST a registration body to `/subjects/{subject}/versions`, assert 200, and
/// return the parsed JSON body (`{"id":N}`).
async fn register(app: &axum::Router, subject: &str, body: &str) -> serde_json::Value {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/subjects/{subject}/versions"))
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "register {subject}");
    body_json(resp).await
}

/// GET `uri` on the router and return the parsed JSON body.
async fn get_json(app: &axum::Router, uri: &str) -> serde_json::Value {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    body_json(resp).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_then_get_round_trips_all_three_formats() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // AVRO
    let avro = register(
        &app,
        "av-value",
        r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#,
    )
    .await;
    assert_eq!(avro["id"], 1);
    // PROTOBUF
    let pb = register(
        &app,
        "pb-value",
        r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; }"}"#,
    )
    .await;
    assert_eq!(pb["id"], 2);
    // JSON
    let js = register(
        &app,
        "js-value",
        r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}}}"}"#,
    )
    .await;
    assert_eq!(js["id"], 3);

    // GET by id round-trips, with schemaType for pb/js and none for avro
    let got_av = get_json(&app, "/schemas/ids/1").await;
    assert!(got_av.get("schemaType").is_none());
    assert!(got_av["schema"].as_str().unwrap().contains("record"));
    let got_pb = get_json(&app, "/schemas/ids/2").await;
    assert_eq!(got_pb["schemaType"], "PROTOBUF");

    // GET /subjects
    let subs = get_json(&app, "/subjects").await;
    let mut names: Vec<String> = subs
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["av-value", "js-value", "pb-value"]);

    // GET subject version 1
    let v1 = get_json(&app, "/subjects/av-value/versions/1").await;
    assert_eq!(v1["version"], 1);
    assert_eq!(v1["id"], 1);

    // idempotent re-register returns same id
    let again = register(
        &app,
        "av-value",
        r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#,
    )
    .await;
    assert_eq!(again["id"], 1);

    // versions list has exactly [1]
    let vers = get_json(&app, "/subjects/av-value/versions").await;
    assert_eq!(vers, serde_json::json!([1]));

    // subject-not-found error shape
    let nf = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/subjects/missing/versions/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(nf.status(), StatusCode::NOT_FOUND);
    let nf_body = body_json(nf).await;
    assert_eq!(nf_body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}
