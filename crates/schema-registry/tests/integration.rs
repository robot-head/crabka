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
use crabka_schema_registry::format::SchemaType;
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
        .register("av", SchemaType::Avro, &av("A"), None, None)
        .await
        .unwrap();
    store
        .register("av", SchemaType::Avro, &av("B"), None, None)
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
        .register("ro", SchemaType::Avro, &av("A"), None, None)
        .await
        .unwrap();
    store
        .set_subject_mode("ro", "READONLY".into())
        .await
        .unwrap();
    let err = store
        .register("ro", SchemaType::Avro, &av("B"), None, None)
        .await
        .unwrap_err();
    assert_eq!(err.error_code(), 42205);
    store
        .set_subject_mode("imp", "IMPORT".into())
        .await
        .unwrap();
    let reg = store
        .register("imp", SchemaType::Avro, &av("C"), Some(42), Some(5))
        .await
        .unwrap();
    assert_eq!((reg.id, reg.version), (42, 5));
    assert_eq!(
        store.store.read().version("imp", Some(5), false).unwrap().0,
        42
    );
    // IMPORT requires BOTH id and version: providing only one is rejected.
    assert!(
        store
            .register("imp", SchemaType::Avro, &av("D"), Some(7), None)
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
