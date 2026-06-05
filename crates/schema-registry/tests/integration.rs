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
