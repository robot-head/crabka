//! End-to-end gate for schema-registry slice 1: REST -> register -> produce to
//! `_schemas` -> group-less reader replay -> in-memory store -> GET, against a
//! real in-process Crabka broker. Drives the registry's REST router via `tower`'s
//! `oneshot` (no live HTTP socket needed) while the `KafkaStore` talks to the
//! broker over the wire.
//!
//! `flavor = "multi_thread", worker_threads = 2` is required: a single-threaded
//! runtime can't drive the broker's accept loop concurrently with the registry's
//! producer/reader tasks and the test body.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    format::SchemaType,
    kafkastore::{KafkaStore, RegisterSchema},
    rest::{self, AppState},
};
use prost_reflect::{
    prost::Message,
    prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        MethodDescriptorProto, ServiceDescriptorProto,
        field_descriptor_proto::{Label as FieldLabel, Type as FieldType},
    },
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

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
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig::default(),
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

// ── helpers ─────────────────────────────────────────────────────────────────

/// PUT `uri` with a JSON string body, return (status, parsed body).
async fn put_json(
    app: &axum::Router,
    uri: &str,
    body: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    (status, json)
}

/// GET `uri`, return (status, parsed body).
async fn get_status_json(
    app: &axum::Router,
    uri: &str,
) -> (axum::http::StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    (status, json)
}

/// POST `uri` with a JSON string body, return the full response.
async fn post_raw(app: &axum::Router, uri: &str, body: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// POST bytes to `uri`, return the full response.
async fn post_bytes(app: &axum::Router, uri: &str, body: Vec<u8>) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/octet-stream")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// DELETE `uri`, return (status, parsed body).
async fn delete_req(app: &axum::Router, uri: &str) -> (axum::http::StatusCode, serde_json::Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let json = body_json(resp).await;
    (status, json)
}

const AVRO_BODY: &str = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;

// ── /config endpoints ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_endpoints() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Default global compat is BACKWARD
    let (status, body) = get_status_json(&app, "/config").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibilityLevel"], "BACKWARD");

    // PUT /config -> FULL
    let (status, body) = put_json(&app, "/config", r#"{"compatibility":"FULL"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibility"], "FULL");

    // GET /config reflects the change (read-your-writes)
    let (status, body) = get_status_json(&app, "/config").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibilityLevel"], "FULL");

    // PUT /config with invalid level -> 422 / error_code 42203
    let (status, body) = put_json(&app, "/config", r#"{"compatibility":"BOGUS"}"#).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error_code"], 42203);

    // PUT /config/{subject}
    let (status, body) = put_json(&app, "/config/av-value", r#"{"compatibility":"NONE"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibility"], "NONE");

    // GET /config/{subject} reflects it
    let (status, body) = get_status_json(&app, "/config/av-value").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibilityLevel"], "NONE");

    // GET /config/{subject} for a subject with no override -> 404 / 40401
    let (status, body) = get_status_json(&app, "/config/no-such-subject").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

// ── /compatibility endpoint ──────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_endpoint() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register a version first so the subject exists for the check.
    register(&app, "av-value", AVRO_BODY).await;

    // Same schema -> is_compatible: true
    let resp = post_raw(
        &app,
        "/compatibility/subjects/av-value/versions/latest",
        AVRO_BODY,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["is_compatible"], true);

    // Unparseable schema -> 422 / error_code 42201
    let resp = post_raw(
        &app,
        "/compatibility/subjects/av-value/versions/latest",
        r#"{"schema":"{ not avro at all"}"#,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(resp).await;
    assert_eq!(body["error_code"], 42201);

    cancel.cancel();
    broker.shutdown().await;
}

// ── lookup endpoint (POST /subjects/{subject}) ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lookup_endpoint() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register a schema first
    let reg = register(&app, "av-value", AVRO_BODY).await;
    assert_eq!(reg["id"], 1);

    // Lookup the same schema -> 200 with {subject,id,version,schema}
    let resp = post_raw(&app, "/subjects/av-value", AVRO_BODY).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["subject"], "av-value");
    assert_eq!(body["id"], 1);
    assert_eq!(body["version"], 1);
    assert!(body["schema"].as_str().unwrap().contains("record"));

    // Lookup a schema not registered under the subject -> 404 / 40403
    let other = r#"{"schema":"{\"type\":\"record\",\"name\":\"Other\",\"fields\":[]}"}"#;
    let resp = post_raw(&app, "/subjects/av-value", other).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error_code"], 40403);

    // Lookup against a missing subject -> 404 / 40401
    let resp = post_raw(&app, "/subjects/no-such-subject", AVRO_BODY).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

// ── FileDescriptorSet import endpoint ───────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn import_file_descriptor_set_registers_dependencies_first() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    let money = FileDescriptorProto {
        name: Some("common/money.proto".into()),
        package: Some("common".into()),
        syntax: Some("proto3".into()),
        message_type: vec![DescriptorProto {
            name: Some("Money".into()),
            field: vec![FieldDescriptorProto {
                name: Some("cents".into()),
                number: Some(1),
                label: Some(FieldLabel::Optional as i32),
                r#type: Some(FieldType::Int64 as i32),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let order = FileDescriptorProto {
        name: Some("orders/order.proto".into()),
        package: Some("orders".into()),
        syntax: Some("proto3".into()),
        dependency: vec!["common/money.proto".into()],
        message_type: vec![DescriptorProto {
            name: Some("Order".into()),
            field: vec![FieldDescriptorProto {
                name: Some("total".into()),
                number: Some(1),
                label: Some(FieldLabel::Optional as i32),
                r#type: Some(FieldType::Message as i32),
                type_name: Some(".common.Money".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        service: vec![ServiceDescriptorProto {
            name: Some("OrderService".into()),
            method: vec![MethodDescriptorProto {
                name: Some("GetOrder".into()),
                input_type: Some(".orders.Order".into()),
                output_type: Some(".orders.Order".into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let bytes = FileDescriptorSet {
        // Deliberately reverse dependency order. The endpoint must sort before
        // registering so references can resolve.
        file: vec![order, money],
    }
    .encode_to_vec();

    let resp = post_bytes(&app, "/schemas/import", bytes).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(
        body,
        serde_json::json!([
            {"subject":"common/money.proto","id":1,"version":1},
            {"subject":"orders/order.proto","id":2,"version":1}
        ])
    );

    let imported = get_json(&app, "/subjects/orders%2Forder.proto/versions/1").await;
    assert_eq!(imported["schemaType"], "PROTOBUF");
    assert_eq!(imported["references"][0]["name"], "common/money.proto");
    assert_eq!(imported["references"][0]["subject"], "common/money.proto");
    assert_eq!(imported["references"][0]["version"], 1);
    assert!(
        imported["schema"]
            .as_str()
            .unwrap()
            .contains("service OrderService")
    );

    cancel.cancel();
    broker.shutdown().await;
}

// ── request builders (sync, for inline oneshot calls) ────────────────────────

fn req_post(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn req_put(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header("content-type", "application/vnd.schemaregistry.v1+json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

// ── version + error paths ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_and_error_paths() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register one schema
    let reg = register(&app, "av-value", AVRO_BODY).await;
    assert_eq!(reg["id"], 1);

    // GET / -> 200 {}
    let (status, body) = get_status_json(&app, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({}));

    // GET /schemas/ids/9999 -> 404 / 40403
    let (status, body) = get_status_json(&app, "/schemas/ids/9999").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40403);

    // GET /subjects/{s}/versions/latest -> resolves to version object
    let (status, body) = get_status_json(&app, "/subjects/av-value/versions/latest").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["version"], 1);
    assert_eq!(body["id"], 1);

    // GET /subjects/{s}/versions/{n}/schema -> raw schema text (not JSON-wrapped)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/subjects/av-value/versions/1/schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    // Body is the raw schema string, not JSON-wrapped
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.contains("record"),
        "raw schema should contain 'record'"
    );
    // Must NOT be a JSON object wrapping it
    assert!(
        !text.starts_with('{') || text.contains("\"type\""),
        "body looks like a JSON envelope rather than raw schema"
    );

    // GET /subjects/{s}/versions/0 -> 422 / 42202 (version 0 is invalid)
    let (status, body) = get_status_json(&app, "/subjects/av-value/versions/0").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body["error_code"], 42202);

    // GET /subjects/{s}/versions/99 -> 404 / 40402 (valid number, absent version)
    let (status, body) = get_status_json(&app, "/subjects/av-value/versions/99").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40402);

    // POST /subjects/{s}/versions with an invalid schema -> 422 / 42201
    let resp = post_raw(
        &app,
        "/subjects/av-value/versions",
        r#"{"schema":"{ not avro"}"#,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = body_json(resp).await;
    assert_eq!(body["error_code"], 42201);

    cancel.cancel();
    broker.shutdown().await;
}

// ── /compatibility endpoint: non-verbose shape + compatible true ─────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_endpoint_nonverbose_and_compatible() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register the base schema so the subject exists.
    register(&app, "s", AVRO_BODY).await;

    // POST the same schema back → is_compatible: true, no verbose=true → no messages key.
    let r = app
        .clone()
        .oneshot(req_post(
            "/compatibility/subjects/s/versions/latest",
            AVRO_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let b = body_json(r).await;
    assert_eq!(b["is_compatible"], true);
    // Without ?verbose=true the response MUST NOT contain a "messages" key.
    assert!(
        b.get("messages").is_none(),
        "non-verbose response must not include 'messages' key; got: {b}"
    );

    cancel.cancel();
    broker.shutdown().await;
}

// ── /compatibility endpoint: error paths (missing subject / bad version) ─────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_endpoint_error_paths() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Missing subject → 404 / 40401.
    let r = app
        .clone()
        .oneshot(req_post(
            "/compatibility/subjects/nope/versions/latest",
            AVRO_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(r).await["error_code"], 40401);

    // Register a schema so the subject exists.
    register(&app, "s", AVRO_BODY).await;

    // Non-numeric version token → 422 / 42202.
    let r = app
        .clone()
        .oneshot(req_post(
            "/compatibility/subjects/s/versions/abc",
            AVRO_BODY,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(r).await["error_code"], 42202);

    // Numeric version, not present → 404 / 40402.
    let r = app
        .clone()
        .oneshot(req_post("/compatibility/subjects/s/versions/99", AVRO_BODY))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(r).await["error_code"], 40402);

    cancel.cancel();
    broker.shutdown().await;
}

// ── FORWARD compat: removing a required field is rejected, adding with default ok

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forward_compat_enforced() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Set subject-level FORWARD enforcement.
    let (status, _) = put_json(&app, "/config/s", r#"{"compatibility":"FORWARD"}"#).await;
    assert_eq!(status, StatusCode::OK);

    // v1: {id:int, y:int} — both required, no defaults.
    let v1 = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"}]}"}"#;
    register(&app, "s", v1).await;

    // v2: remove y — old reader (with y, no default) cannot read new writer (no y).
    // FORWARD = old reads new → fails → 409.
    let v2_remove_y = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    let r = app
        .clone()
        .oneshot(req_post("/subjects/s/versions", v2_remove_y))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);

    // v2b: keep y, add z with default 0 — old reader ignores extra z → FORWARD compatible.
    let v2b = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"y\",\"type\":\"int\"},{\"name\":\"z\",\"type\":\"int\",\"default\":0}]}"}"#;
    let r = app
        .clone()
        .oneshot(req_post("/subjects/s/versions", v2b))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    cancel.cancel();
    broker.shutdown().await;
}

// ── compatibility enforcement on register ────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/s/versions", base))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Adding a required field (no default) breaks BACKWARD — expect 409.
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    let r = app
        .clone()
        .oneshot(req_post("/subjects/s/versions", bad))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);

    // Adding a field with a default is compatible.
    let good = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\",\"default\":0}]}"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/s/versions", good))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protobuf_compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // v1: a single int32 field. Default global compat is BACKWARD.
    let v1 =
        r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; }"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/pb/versions", v1))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Changing the field's wire kind across groups (int32 → string) breaks
    // BACKWARD — expect 409.
    let bad =
        r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { string id = 1; }"}"#;
    let r = app
        .clone()
        .oneshot(req_post("/subjects/pb/versions", bad))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);

    // Adding a new field is compatible.
    let good = r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; int32 x = 2; }"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/pb/versions", good))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn json_schema_compat_enforced_on_register() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // v1: an object with one integer property bounded by maximum=100. Default
    // global compat is BACKWARD.
    let v1 = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}},\"maximum\":100}"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/js/versions", v1))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    // Adding a required property breaks BACKWARD (cp-calibrated: `required_added`
    // BACKWARD=false) — expect 409.
    let bad = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}},\"maximum\":100,\"required\":[\"a\"]}"}"#;
    let r = app
        .clone()
        .oneshot(req_post("/subjects/js/versions", bad))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::CONFLICT);
    assert_eq!(body_json(r).await["error_code"], 409);

    // Removing a numeric bound loosens the schema (cp-calibrated:
    // `maximum_removed` BACKWARD=true) — compatible, expect 200.
    let good = r#"{"schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}}"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/js/versions", good))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn none_level_bypasses_enforcement() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    app.clone()
        .oneshot(req_put("/config/s", r#"{"compatibility":"NONE"}"#))
        .await
        .unwrap();

    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    app.clone()
        .oneshot(req_post("/subjects/s/versions", base))
        .await
        .unwrap();

    // Incompatible schema is accepted when level is NONE.
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    assert_eq!(
        app.clone()
            .oneshot(req_post("/subjects/s/versions", bad))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_endpoint_real_verdict() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    let base = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}"}"#;
    app.clone()
        .oneshot(req_post("/subjects/s/versions", base))
        .await
        .unwrap();

    // Incompatible candidate: check endpoint returns is_compatible=false + messages.
    let bad = r#"{"schema":"{\"type\":\"record\",\"name\":\"U\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"x\",\"type\":\"int\"}]}"}"#;
    let r = app
        .clone()
        .oneshot(req_post(
            "/compatibility/subjects/s/versions/latest?verbose=true",
            bad,
        ))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let b = body_json(r).await;
    assert_eq!(b["is_compatible"], false);
    assert!(!b["messages"].as_array().unwrap().is_empty());

    cancel.cancel();
    broker.shutdown().await;
}

// ── facade: delete lifecycle + modes + IMPORT/READONLY (broker-backed) ────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facade_soft_then_permanent_delete_version() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    // Distinct record names across versions trip Avro BACKWARD (names must
    // match); NONE bypasses so we can register two versions to delete.
    store.set_subject_compat("av", "NONE".into()).await.unwrap();
    store
        .register(RegisterSchema {
            subject: "av",
            ty: SchemaType::Avro,
            schema: &av("A"),
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap();
    store
        .register(RegisterSchema {
            subject: "av",
            ty: SchemaType::Avro,
            schema: &av("B"),
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap();
    assert_eq!(store.soft_delete_version("av", 1).await.unwrap(), 1);
    assert_eq!(store.store.read().versions("av", false).unwrap(), vec![2]);
    assert_eq!(store.store.read().versions("av", true).unwrap(), vec![1, 2]);
    let err = store.permanent_delete_version("av", 2).await.unwrap_err();
    assert_eq!(err.error_code(), 40407);
    assert_eq!(store.permanent_delete_version("av", 1).await.unwrap(), 1);
    assert!(store.store.read().version("av", Some(1), true).is_none());
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn facade_readonly_blocks_writes_import_allows_explicit_id() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    store
        .register(RegisterSchema {
            subject: "ro",
            ty: SchemaType::Avro,
            schema: &av("A"),
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap();
    store
        .set_subject_mode("ro", "READONLY".into())
        .await
        .unwrap();
    let err = store
        .register(RegisterSchema {
            subject: "ro",
            ty: SchemaType::Avro,
            schema: &av("B"),
            references: &[],
            message_type: None,
            import_id: None,
            import_version: None,
        })
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), 42205);
    store
        .set_subject_mode("imp", "IMPORT".into())
        .await
        .unwrap();
    let reg = store
        .register(RegisterSchema {
            subject: "imp",
            ty: SchemaType::Avro,
            schema: &av("C"),
            references: &[],
            message_type: None,
            import_id: Some(42),
            import_version: Some(5),
        })
        .await
        .unwrap();
    assert_eq!((reg.id, reg.version), (42, 5));
    assert_eq!(
        store
            .store
            .read()
            .version("imp", Some(5), false)
            .unwrap()
            .id,
        42
    );
    // IMPORT requires BOTH id and version: providing only one is rejected.
    assert!(
        store
            .register(RegisterSchema {
                subject: "imp",
                ty: SchemaType::Avro,
                schema: &av("D"),
                references: &[],
                message_type: None,
                import_id: Some(7),
                import_version: None,
            })
            .await
            .is_err(),
        "IMPORT with id but no version must error"
    );
    cancel.cancel();
    broker.shutdown().await;
}

fn av(n: &str) -> String {
    format!("{{\"type\":\"record\",\"name\":\"{n}\",\"fields\":[]}}")
}

// ── REST: delete + mode + lookup endpoints (slice 3) ─────────────────────────

fn req_delete(uri: &str) -> Request<Body> {
    Request::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_delete_version_lifecycle_and_deleted_query() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // NONE compat so two distinct schemas register as v1+v2 (isolates delete lifecycle)
    assert_eq!(
        app.clone()
            .oneshot(req_put("/config/av", r#"{"compatibility":"NONE"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let body = |n: &str| format!(r#"{{"schema":{:?}}}"#, av(n));
    register(&app, "av", &body("A")).await;
    register(&app, "av", &body("B")).await;
    // soft-delete v1 → body is the bare int 1
    let r = app
        .clone()
        .oneshot(req_delete("/subjects/av/versions/1"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(body_json(r).await, serde_json::json!(1));
    assert_eq!(
        get_json(&app, "/subjects/av/versions").await,
        serde_json::json!([2])
    );
    assert_eq!(
        get_json(&app, "/subjects/av/versions?deleted=true").await,
        serde_json::json!([1, 2])
    );
    // GET v1 hidden, ?deleted shows it
    let hidden = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/subjects/av/versions/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);
    let shown = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/subjects/av/versions/1?deleted=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(shown.status(), StatusCode::OK);
    // permanent
    let p = app
        .clone()
        .oneshot(req_delete("/subjects/av/versions/1?permanent=true"))
        .await
        .unwrap();
    assert_eq!(p.status(), StatusCode::OK);
    let gone = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/subjects/av/versions/1?deleted=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_mode_and_lookup_endpoints() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(&app, "a", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    // GET /mode default
    assert_eq!(
        get_json(&app, "/mode").await,
        serde_json::json!({"mode": "READWRITE"})
    );
    // PUT /mode/a READONLY then register → 422 / 42205
    let pm = app
        .clone()
        .oneshot(req_put("/mode/a", r#"{"mode":"READONLY"}"#))
        .await
        .unwrap();
    assert_eq!(pm.status(), StatusCode::OK);
    let blocked = app
        .clone()
        .oneshot(req_post(
            "/subjects/a/versions",
            &format!(r#"{{"schema":{:?}}}"#, av("B")),
        ))
        .await
        .unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(blocked).await["error_code"], 42205);
    // GET /mode/a → READONLY ; DELETE clears
    assert_eq!(
        get_json(&app, "/mode/a").await,
        serde_json::json!({"mode": "READONLY"})
    );
    let dm = app.clone().oneshot(req_delete("/mode/a")).await.unwrap();
    assert_eq!(dm.status(), StatusCode::OK);
    // lookups
    let ids = get_json(&app, "/schemas/ids/1/versions").await;
    assert_eq!(ids, serde_json::json!([{"subject": "a", "version": 1}]));
    let all = get_json(&app, "/schemas").await;
    assert_eq!(all.as_array().unwrap().len(), 1);
    let refby = get_json(&app, "/subjects/a/versions/1/referencedby").await;
    assert_eq!(refby, serde_json::json!([]));
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_subject_soft_then_permanent_and_soft_before_hard() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // NONE so two distinct schemas register as v1+v2 (isolates the delete lifecycle from compat)
    assert_eq!(
        app.clone()
            .oneshot(req_put("/config/s", r#"{"compatibility":"NONE"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    register(&app, "s", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    register(&app, "s", &format!(r#"{{"schema":{:?}}}"#, av("B"))).await;
    // permanent subject delete BEFORE a soft delete → soft-before-hard: 404 / 40405
    let early = app
        .clone()
        .oneshot(req_delete("/subjects/s?permanent=true"))
        .await
        .unwrap();
    assert_eq!(early.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(early).await["error_code"], 40405);
    // soft delete → returns the version array, subject disappears from the live list
    let soft = app
        .clone()
        .oneshot(req_delete("/subjects/s"))
        .await
        .unwrap();
    assert_eq!(soft.status(), StatusCode::OK);
    assert_eq!(body_json(soft).await, serde_json::json!([1, 2]));
    assert!(
        get_json(&app, "/subjects")
            .await
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        get_json(&app, "/subjects?deleted=true").await,
        serde_json::json!(["s"])
    );
    // permanent delete → gone even with ?deleted
    let perm = app
        .clone()
        .oneshot(req_delete("/subjects/s?permanent=true"))
        .await
        .unwrap();
    assert_eq!(perm.status(), StatusCode::OK);
    assert_eq!(
        get_json(&app, "/subjects?deleted=true").await,
        serde_json::json!([])
    );
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_version_permanent_before_soft_is_rejected() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(&app, "v", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    // permanent version delete before a soft delete → 404 / 40407
    let r = app
        .clone()
        .oneshot(req_delete("/subjects/v/versions/1?permanent=true"))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(r).await["error_code"], 40407);
    // soft then permanent succeeds
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/v/versions/1"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/v/versions/1?permanent=true"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_import_mode_registers_explicit_id() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    assert_eq!(
        app.clone()
            .oneshot(req_put("/mode/imp", r#"{"mode":"IMPORT"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let body = format!(r#"{{"schema":{:?},"id":42,"version":5}}"#, av("C"));
    let r = app
        .clone()
        .oneshot(req_post("/subjects/imp/versions", &body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(body_json(r).await["id"], 42);
    let got = get_json(&app, "/subjects/imp/versions/5").await;
    assert_eq!(got["id"], 42);
    assert_eq!(got["version"], 5);
    cancel.cancel();
    broker.shutdown().await;
}

// cp-schema-registry 7.4.0 admin error codes, captured in
// tests/capture_admin_fixtures.rs and pinned here (cp is authority).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_cp_calibrated_admin_error_codes() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(&app, "d", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    // soft-delete, then soft-delete AGAIN -> cp 404 / 40404
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/d"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let again = app
        .clone()
        .oneshot(req_delete("/subjects/d"))
        .await
        .unwrap();
    assert_eq!(again.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(again).await["error_code"], 40404);
    // GET /mode/{subject} with no override -> cp 404 / 40409
    let nomode = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/mode/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(nomode.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(nomode).await["error_code"], 40409);
    // GET /schemas/ids/{unknown}/versions -> cp 404 / 40403
    let noid = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/schemas/ids/999/versions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(noid.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(noid).await["error_code"], 40403);
    cancel.cancel();
    broker.shutdown().await;
}

// Error + edge branches across the facade/REST admin surface (mode gating,
// soft-before-hard, missing subject/version, latest resolution, IMPORT guards).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn rest_admin_edge_and_error_branches() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    assert_eq!(store.topic(), "_schemas");
    let app = rest::router(AppState { store });

    // PUT /mode (global) -> 200, GET reflects it
    assert_eq!(
        app.clone()
            .oneshot(req_put("/mode", r#"{"mode":"READONLY"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        get_json(&app, "/mode").await,
        serde_json::json!({"mode": "READONLY"})
    );
    // global READONLY blocks PUT /config (set_compat gate) -> 422/42205
    let cfg = app
        .clone()
        .oneshot(req_put("/config", r#"{"compatibility":"NONE"}"#))
        .await
        .unwrap();
    assert_eq!(cfg.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(cfg).await["error_code"], 42205);
    // back to READWRITE
    assert_eq!(
        app.clone()
            .oneshot(req_put("/mode", r#"{"mode":"READWRITE"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    // invalid mode (global + subject) -> 422/42204
    for uri in ["/mode", "/mode/x"] {
        let bad = app
            .clone()
            .oneshot(req_put(uri, r#"{"mode":"BOGUS"}"#))
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body_json(bad).await["error_code"], 42204);
    }
    // deletes on a missing subject
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/nope/versions/1"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/nope"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/nope/versions/1?permanent=true"))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    // register a real subject
    register(&app, "x", &format!(r#"{{"schema":{:?}}}"#, av("A"))).await;
    // permanent-delete an absent version (subject exists) -> 404/40402
    let pv = app
        .clone()
        .oneshot(req_delete("/subjects/x/versions/99?permanent=true"))
        .await
        .unwrap();
    assert_eq!(pv.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(pv).await["error_code"], 40402);
    // DELETE /versions/latest soft-deletes the latest live version
    let dl = app
        .clone()
        .oneshot(req_delete("/subjects/x/versions/latest"))
        .await
        .unwrap();
    assert_eq!(dl.status(), StatusCode::OK);
    assert_eq!(body_json(dl).await, serde_json::json!(1));
    // non-numeric version -> 422/42202
    let dbad = app
        .clone()
        .oneshot(req_delete("/subjects/x/versions/abc"))
        .await
        .unwrap();
    assert_eq!(dbad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(dbad).await["error_code"], 42202);
    // IMPORT requires an empty subject / empty registry -> 422/42205
    let import_subject = app
        .clone()
        .oneshot(req_put("/mode/x", r#"{"mode":"IMPORT"}"#))
        .await
        .unwrap();
    assert_eq!(import_subject.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(import_subject).await["error_code"], 42205);
    let import_global = app
        .clone()
        .oneshot(req_put("/mode", r#"{"mode":"IMPORT"}"#))
        .await
        .unwrap();
    assert_eq!(import_global.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(import_global).await["error_code"], 42205);
    // READONLY blocks a delete (ensure_writable) -> 422/42205
    assert_eq!(
        app.clone()
            .oneshot(req_put("/mode/x", r#"{"mode":"READONLY"}"#))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let rod = app
        .clone()
        .oneshot(req_delete("/subjects/x/versions/1"))
        .await
        .unwrap();
    assert_eq!(rod.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(rod).await["error_code"], 42205);

    cancel.cancel();
    broker.shutdown().await;
}

// ── REST: schema references (slice 4) ────────────────────────────────────────

/// A self-contained Avro record (its single field is a primitive, NOT a named
/// reference) — so it parses pre-Task-4. Reference *bookkeeping* (validation,
/// referencedby, delete-protection, GET) is format-agnostic and works now.
fn av_named(name: &str, field_type: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{name}","fields":[{{"name":"f","type":"{field_type}"}}]}}"#
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_references_lifecycle_avro() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(
        &app,
        "base",
        &format!(r#"{{"schema":{:?}}}"#, av_named("Base", "int")),
    )
    .await;
    let body = format!(
        r#"{{"schema":{:?},"references":[{{"name":"Base","subject":"base","version":1}}]}}"#,
        av_named("Dep", "long")
    );
    let r = app
        .clone()
        .oneshot(req_post("/subjects/dep/versions", &body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let dep_id = body_json(r).await["id"].as_i64().unwrap();
    // referencedby lists the referrer's id
    let refby = get_json(&app, "/subjects/base/versions/1/referencedby").await;
    assert_eq!(refby, serde_json::json!([dep_id]));
    // GET the referrer includes references
    let got = get_json(&app, "/subjects/dep/versions/1").await;
    assert_eq!(got["references"][0]["subject"], "base");
    // delete-protection: deleting base v1 while referenced is rejected (422/42206)
    let blocked = app
        .clone()
        .oneshot(req_delete("/subjects/base/versions/1"))
        .await
        .unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(blocked).await["error_code"], 42206);
    // remove the referrer, then base deletes fine
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/dep/versions/1"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(req_delete("/subjects/base/versions/1"))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_reference_not_found_rejected() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let body = format!(
        r#"{{"schema":{:?},"references":[{{"name":"Nope","subject":"nope","version":1}}]}}"#,
        av_named("Dep", "int")
    );
    let r = app
        .clone()
        .oneshot(req_post("/subjects/dep/versions", &body))
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(r).await["error_code"], 42201);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_avro_reference_resolves_end_to_end() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let money = r#"{"type":"record","name":"Money","fields":[{"name":"cents","type":"long"}]}"#;
    register(
        &app,
        "money",
        &serde_json::json!({ "schema": money }).to_string(),
    )
    .await;
    // Order uses Money by name; without the reference it would not parse.
    let order = r#"{"type":"record","name":"Order","fields":[{"name":"price","type":"Money"}]}"#;
    let body = serde_json::json!({
        "schema": order,
        "references": [{ "name": "Money", "subject": "money", "version": 1 }]
    })
    .to_string();
    let r = app
        .clone()
        .oneshot(req_post("/subjects/order/versions", &body))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "Order resolves Money via reference"
    );
    // And without the reference, the same Order is rejected (unresolved type).
    let no_ref = serde_json::json!({ "schema": order }).to_string();
    let bad = app
        .clone()
        .oneshot(req_post("/subjects/order2/versions", &no_ref))
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_protobuf_reference_resolves_end_to_end() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    let money = "syntax = \"proto3\"; package m; message Money { int64 cents = 1; }";
    register(
        &app,
        "money",
        &serde_json::json!({ "schemaType": "PROTOBUF", "schema": money }).to_string(),
    )
    .await;
    let order = "syntax = \"proto3\"; import \"money.proto\"; message Order { m.Money price = 1; }";
    let body = serde_json::json!({
        "schemaType": "PROTOBUF",
        "schema": order,
        "references": [{ "name": "money.proto", "subject": "money", "version": 1 }]
    })
    .to_string();
    let r = app
        .clone()
        .oneshot(req_post("/subjects/order/versions", &body))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "Order links money.proto via reference"
    );
    // Without the reference, the unresolved import is rejected.
    let no_ref = serde_json::json!({ "schemaType": "PROTOBUF", "schema": order }).to_string();
    let bad = app
        .clone()
        .oneshot(req_post("/subjects/order2/versions", &no_ref))
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_json_reference_resolves_end_to_end() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    // The referenced schema: an integer with an upper bound.
    let amount = r#"{"type":"integer","maximum":10}"#;
    register(
        &app,
        "amount",
        &serde_json::json!({ "schemaType": "JSON", "schema": amount }).to_string(),
    )
    .await;
    // Order's property `a` points at the referenced schema via `$ref: "Amount"`.
    // JSON refs are not inlined into the canonical form; the reference only feeds
    // the compatibility diff, so registration succeeds and the link is recorded.
    let order = r#"{"type":"object","properties":{"a":{"$ref":"Amount"}}}"#;
    let body = serde_json::json!({
        "schemaType": "JSON",
        "schema": order,
        "references": [{ "name": "Amount", "subject": "amount", "version": 1 }]
    })
    .to_string();
    let r = app
        .clone()
        .oneshot(req_post("/subjects/order/versions", &body))
        .await
        .unwrap();
    assert_eq!(
        r.status(),
        StatusCode::OK,
        "JSON $ref resolves via reference"
    );
    let order_id = body_json(r).await["id"].as_i64().unwrap();
    // referencedby lists the referrer's id.
    let refby = get_json(&app, "/subjects/amount/versions/1/referencedby").await;
    assert_eq!(refby, serde_json::json!([order_id]));
    cancel.cancel();
    broker.shutdown().await;
}

// GET-by-id includes references, GET /schemas carries schemaType, and
// soft-deleting a whole referenced SUBJECT is reference-protected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_references_get_by_id_list_and_subject_delete_protection() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });
    register(
        &app,
        "base",
        &format!(r#"{{"schema":{:?}}}"#, av_named("Base", "int")),
    )
    .await;
    let body = format!(
        r#"{{"schema":{:?},"references":[{{"name":"Base","subject":"base","version":1}}]}}"#,
        av_named("Dep", "long")
    );
    let dep_id = body_json(
        app.clone()
            .oneshot(req_post("/subjects/dep/versions", &body))
            .await
            .unwrap(),
    )
    .await["id"]
        .as_i64()
        .unwrap();
    // GET /schemas/ids/{referrer_id} includes the references array.
    let by_id = get_json(&app, &format!("/schemas/ids/{dep_id}")).await;
    assert_eq!(by_id["references"][0]["name"], "Base");
    assert_eq!(by_id["references"][0]["subject"], "base");
    // GET /schemas surfaces a non-Avro schema's schemaType.
    register(
        &app,
        "pb",
        r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; }"}"#,
    )
    .await;
    let all = get_json(&app, "/schemas").await;
    assert!(
        all.as_array()
            .unwrap()
            .iter()
            .any(|r| r["schemaType"] == "PROTOBUF"),
        "GET /schemas should carry schemaType for non-Avro rows"
    );
    // Soft-deleting the WHOLE referenced subject is rejected (42206).
    let blocked = app
        .clone()
        .oneshot(req_delete("/subjects/base"))
        .await
        .unwrap();
    assert_eq!(blocked.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(blocked).await["error_code"], 42206);
    cancel.cancel();
    broker.shutdown().await;
}

// ── DELETE /config/{subject} ─────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_subject_compat_reverts_to_global() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Set a per-subject override to FULL
    let (status, body) =
        put_json(&app, "/config/test-subject", r#"{"compatibility":"FULL"}"#).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibility"], "FULL");

    // GET confirms the override is set
    let (status, body) = get_status_json(&app, "/config/test-subject").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["compatibilityLevel"], "FULL");

    // DELETE returns the deleted level
    let (status, body) = delete_req(&app, "/config/test-subject").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "DELETE /config/{{subject}} should be 200"
    );
    assert_eq!(
        body["compatibility"], "FULL",
        "response should echo the deleted level"
    );

    // GET now returns 404 (no per-subject override remains)
    let (status, body) = get_status_json(&app, "/config/test-subject").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_subject_compat_no_override_returns_404() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // No per-subject override was ever set
    let (status, body) = delete_req(&app, "/config/no-override-subject").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40401);

    cancel.cancel();
    broker.shutdown().await;
}

// ── GET /schemas/ids/{id}/schema ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_by_id_schema_returns_raw_string() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register a schema
    let reg = register(&app, "raw-schema-test", AVRO_BODY).await;
    let id = reg["id"].as_i64().unwrap();

    // GET /schemas/ids/{id}/schema → raw schema string (not JSON-wrapped)
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/schemas/ids/{id}/schema"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();

    // Body should contain the schema content
    assert!(
        text.contains("record"),
        "raw schema body should contain the word 'record'"
    );

    // Body should be a JSON value representing the schema itself, NOT {"schema":"..."}
    let v: serde_json::Value = serde_json::from_str(text).expect("body should be valid JSON");
    assert!(
        v.get("schema").is_none(),
        "body should be the raw schema, not a JSON envelope with a 'schema' key"
    );

    // Non-existent id → 404 / 40403
    let (status, body) = get_status_json(&app, "/schemas/ids/9999/schema").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40403);

    cancel.cancel();
    broker.shutdown().await;
}

// ── GET /schemas/ids/{id}/subjects ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_by_id_subjects_returns_all_subjects() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Register the SAME schema under two subjects → they share one schema ID
    let reg1 = register(&app, "subject-alpha", AVRO_BODY).await;
    let reg2 = register(&app, "subject-beta", AVRO_BODY).await;
    assert_eq!(reg1["id"], reg2["id"], "same schema must share one id");
    let shared_id = reg1["id"].as_i64().unwrap();

    // GET /schemas/ids/{id}/subjects → both subjects present
    let body = get_json(&app, &format!("/schemas/ids/{shared_id}/subjects")).await;
    let subjects: Vec<String> =
        serde_json::from_value(body).expect("response should be a JSON array");
    assert!(
        subjects.contains(&"subject-alpha".to_string()),
        "subject-alpha should be in the list"
    );
    assert!(
        subjects.contains(&"subject-beta".to_string()),
        "subject-beta should be in the list"
    );

    // Non-existent id → 404 / 40403
    let (status, body) = get_status_json(&app, "/schemas/ids/9999/subjects").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error_code"], 40403);

    cancel.cancel();
    broker.shutdown().await;
}

// ── ?normalize=true ──────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalize_true_deduplicates_avro_schemas() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // Avro schema with extra whitespace — semantically identical to AVRO_BODY
    let avro_with_spaces = r#"{"schema":"{ \"type\" : \"record\" , \"name\" : \"U\" , \"fields\" : [ { \"name\" : \"id\" , \"type\" : \"int\" } ] }"}"#;

    // Register with normalize=true → normalizes to PCF, stored as canonical form
    let resp_a = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp_a.status(),
        StatusCode::OK,
        "first normalize=true register should succeed"
    );
    let reg_a = body_json(resp_a).await;
    let id_a = reg_a["id"].as_i64().unwrap();

    // Register same schema again with normalize=true → same PCF → same ID
    let resp_b = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    let reg_b = body_json(resp_b).await;
    assert_eq!(
        reg_b["id"].as_i64().unwrap(),
        id_a,
        "second normalize=true registration of same schema must be idempotent (same id)"
    );

    // normalize=true on an invalid Avro → 422 / 42201
    let resp_bad = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-test/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(r#"{"schema":"{ not avro at all"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let err_body = body_json(resp_bad).await;
    assert_eq!(err_body["error_code"], 42201);

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalize_true_json_schema_deduplicates_and_errors() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    // JSON schema with extra whitespace — normalize=true round-trips through serde_json
    let json_body = r#"{"schemaType":"JSON","schema":"{ \"type\": \"object\", \"properties\": { \"id\": { \"type\": \"integer\" } } }"}"#;

    // Register with normalize=true → JSON round-trip strips extraneous whitespace
    let resp_a = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/json-norm/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(json_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    let id_a = body_json(resp_a).await["id"].as_i64().unwrap();

    // Same schema again with normalize=true → idempotent (same id)
    let resp_b = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/json-norm/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(json_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    assert_eq!(body_json(resp_b).await["id"].as_i64().unwrap(), id_a);

    // Invalid JSON with normalize=true → 422 / 42201 (exercises JSON error path in normalize_schema)
    let resp_bad = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/json-norm/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(
                    r#"{"schemaType":"JSON","schema":"not valid { json"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_bad.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(body_json(resp_bad).await["error_code"], 42201);

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalize_true_protobuf_is_noop() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    let pb_body =
        r#"{"schemaType":"PROTOBUF","schema":"syntax = \"proto3\"; message U { int32 id = 1; }"}"#;

    // normalize=true with Protobuf is a no-op — schema is stored verbatim
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/pb-norm/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(pb_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let id = body_json(resp).await["id"].as_i64().unwrap();

    // Re-register identical → same id (idempotent)
    let resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/pb-norm/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(pb_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(body_json(resp2).await["id"].as_i64().unwrap(), id);

    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normalize_true_lookup_finds_normalized_schema() {
    let (broker, store, cancel, _dir) = boot_registry(1).await;
    let app = rest::router(AppState { store });

    let avro_with_spaces = r#"{"schema":"{ \"type\" : \"record\" , \"name\" : \"U\" , \"fields\" : [ { \"name\" : \"id\" , \"type\" : \"int\" } ] }"}"#;

    // Register with normalize=true → stored as PCF
    let reg_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-lookup/versions?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reg_resp.status(), StatusCode::OK);
    let stored_id = body_json(reg_resp).await["id"].as_i64().unwrap();

    // POST /subjects/{s}?normalize=true → exercises the lookup handler's normalize path
    let lookup_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/subjects/norm-lookup?normalize=true")
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(avro_with_spaces))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lookup_resp.status(), StatusCode::OK);
    let found = body_json(lookup_resp).await;
    assert_eq!(found["id"].as_i64().unwrap(), stored_id);
    assert_eq!(found["subject"], "norm-lookup");

    cancel.cancel();
    broker.shutdown().await;
}
