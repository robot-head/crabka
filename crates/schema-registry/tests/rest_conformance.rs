//! REST conformance: verifies our router's responses structurally match the
//! byte-exact golden fixtures captured from a real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0`.
//!
//! No Docker required — the suite spins up an in-process broker + `KafkaStore`
//! and drives the `axum` router via `tower::ServiceExt::oneshot` (no live socket).
//!
//! Schema strings and the registration order are taken verbatim from the fixture
//! README (`tests/fixtures/README.md`) so that assigned IDs 1/2/3 match.

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use crabka_broker::{Broker, BrokerConfig};
use crabka_schema_registry::{
    config::{RegistryConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ── fixture helpers ──────────────────────────────────────────────────────────

/// Load and parse a fixture file from `tests/fixtures/<name>`.
fn fixture_value(name: &str) -> serde_json::Value {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let bytes = std::fs::read(&path).unwrap_or_else(|e| panic!("fixture {path}: {e}"));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("fixture {path} is not valid JSON: {e}"))
}

/// For error fixtures: unwrap the `{"_http_status": N, "_body": "<raw>"}` wrapper
/// and return the `_body` JSON as a `serde_json::Value`.
fn fixture_error_body(name: &str) -> (u16, serde_json::Value) {
    let wrapper = fixture_value(name);
    let raw_status = wrapper["_http_status"]
        .as_u64()
        .unwrap_or_else(|| panic!("{name}: missing _http_status"));
    let status = u16::try_from(raw_status)
        .unwrap_or_else(|_| panic!("{name}: _http_status {raw_status} out of u16 range"));
    let body_str = wrapper["_body"]
        .as_str()
        .unwrap_or_else(|| panic!("{name}: _body is not a string"));
    let body: serde_json::Value = serde_json::from_str(body_str)
        .unwrap_or_else(|e| panic!("{name}: _body is not valid JSON: {e}"));
    (status, body)
}

// ── broker / registry boot ───────────────────────────────────────────────────

async fn boot_registry() -> (
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
        schemas_topic_rf: 1,
        client_id: "sr-conformance".into(),
        advertised_url: "http://127.0.0.1:0".into(),
        group_id: "schema-registry".into(),
        leader_eligibility: true,
        security: SecurityConfig::default(),
    };
    let cancel = CancellationToken::new();
    let store = KafkaStore::start(&cfg, cancel.clone()).await.unwrap();
    (broker, store, cancel, dir)
}

// ── request helpers ──────────────────────────────────────────────────────────

async fn body_bytes(resp: axum::response::Response) -> bytes::Bytes {
    axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> serde_json::Value {
    let b = body_bytes(resp).await;
    serde_json::from_slice(&b).unwrap_or_else(|e| {
        panic!(
            "response body is not valid JSON: {e}\n  raw: {}",
            String::from_utf8_lossy(&b)
        )
    })
}

async fn post_register(app: &axum::Router, subject: &str, body: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/subjects/{subject}/versions"))
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn post_lookup(app: &axum::Router, subject: &str, body: &str) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/subjects/{subject}"))
                .header("content-type", "application/vnd.schemaregistry.v1+json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn get_response(app: &axum::Router, uri: &str) -> axum::response::Response {
    app.clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn get_json(app: &axum::Router, uri: &str) -> serde_json::Value {
    body_json(get_response(app, uri).await).await
}

// ── helpers for unordered array comparison ───────────────────────────────────

fn sorted_string_array(v: &serde_json::Value) -> Vec<String> {
    let mut arr: Vec<String> = v
        .as_array()
        .expect("expected a JSON array")
        .iter()
        .map(|x| x.as_str().expect("expected string elements").to_string())
        .collect();
    arr.sort();
    arr
}

// ── the test ─────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn rest_conformance_vs_cp_fixtures() {
    // Schema strings verbatim from the fixture README.
    // NOTE: the PROTOBUF schema registered by cp-schema-registry is normalised
    // (reformatted) before being stored; we must send the EXACT same source so
    // our parser produces the same normalised canonical form.  The fixture body
    // `rest_get_by_id_protobuf.json` shows the normalised version that SR echoes
    // back; we must match that.
    let avro_schema = r#"{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}"#;
    let protobuf_schema = r#"syntax = "proto3"; message User { int32 id = 1; }"#;
    let json_schema = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;

    let (broker, store, cancel, _dir) = boot_registry().await;
    let app = rest::router(AppState { store });

    // ── 1. Register all three schemas (in order so IDs match fixtures) ────────

    // AVRO: schemaType omitted → default.
    let avro_body = serde_json::json!({ "schema": avro_schema }).to_string();
    let r = post_register(&app, "av-value", &avro_body).await;
    assert_eq!(r.status(), StatusCode::OK, "register av-value");
    let avro_reg_resp = body_json(r).await;

    // PROTOBUF:
    let pb_body =
        serde_json::json!({ "schemaType": "PROTOBUF", "schema": protobuf_schema }).to_string();
    let r = post_register(&app, "pb-value", &pb_body).await;
    assert_eq!(r.status(), StatusCode::OK, "register pb-value");
    let pb_reg_resp = body_json(r).await;

    // JSON:
    let js_body = serde_json::json!({ "schemaType": "JSON", "schema": json_schema }).to_string();
    let r = post_register(&app, "js-value", &js_body).await;
    assert_eq!(r.status(), StatusCode::OK, "register js-value");
    let js_reg_resp = body_json(r).await;

    // ── 2. Register response: `{"id":N}` matches fixtures ────────────────────

    let fix_register_avro = fixture_value("rest_register_avro.json");
    let fix_register_pb = fixture_value("rest_register_protobuf.json");
    let fix_register_js = fixture_value("rest_register_json.json");

    assert_eq!(
        avro_reg_resp, fix_register_avro,
        "POST /subjects/av-value/versions matches fixture"
    );
    assert_eq!(
        pb_reg_resp, fix_register_pb,
        "POST /subjects/pb-value/versions matches fixture"
    );
    assert_eq!(
        js_reg_resp, fix_register_js,
        "POST /subjects/js-value/versions matches fixture"
    );

    // ── 3. GET /schemas/ids/1 (AVRO) ─────────────────────────────────────────
    // Fixture: {"schema":"..."} — no schemaType for AVRO.
    let got_avro_by_id = get_json(&app, "/schemas/ids/1").await;
    let fix_avro_by_id = fixture_value("rest_get_by_id_avro.json");
    assert_eq!(
        got_avro_by_id, fix_avro_by_id,
        "GET /schemas/ids/1 structurally matches rest_get_by_id_avro.json"
    );
    // Explicit: no schemaType field for AVRO.
    assert!(
        got_avro_by_id.get("schemaType").is_none(),
        "AVRO GET by id must NOT include schemaType"
    );

    // ── 4. GET /schemas/ids/2 (PROTOBUF) ─────────────────────────────────────
    let got_pb_by_id = get_json(&app, "/schemas/ids/2").await;
    let fix_pb_by_id = fixture_value("rest_get_by_id_protobuf.json");
    // The protobuf schema is normalised by our parser, and by cp-schema-registry.
    // Both sides should produce the same normalised text.  If there is a
    // discrepancy, the comparison below will tell us.
    assert_eq!(
        got_pb_by_id["schemaType"], "PROTOBUF",
        "GET /schemas/ids/2 has schemaType=PROTOBUF"
    );
    // Full structural comparison.
    assert_eq!(
        got_pb_by_id, fix_pb_by_id,
        "GET /schemas/ids/2 structurally matches rest_get_by_id_protobuf.json"
    );

    // ── 5. GET /schemas/ids/3 (JSON) ─────────────────────────────────────────
    let got_js_by_id = get_json(&app, "/schemas/ids/3").await;
    let fix_js_by_id = fixture_value("rest_get_by_id_json.json");
    assert_eq!(
        got_js_by_id, fix_js_by_id,
        "GET /schemas/ids/3 structurally matches rest_get_by_id_json.json"
    );

    // ── 6. GET /subjects/av-value/versions/1 ─────────────────────────────────
    let got_av_ver = get_json(&app, "/subjects/av-value/versions/1").await;
    let fix_av_ver = fixture_value("rest_get_version_avro.json");
    assert_eq!(
        got_av_ver, fix_av_ver,
        "GET /subjects/av-value/versions/1 structurally matches rest_get_version_avro.json"
    );
    // Explicit field checks per spec: AVRO has no schemaType.
    assert!(
        got_av_ver.get("schemaType").is_none(),
        "AVRO get_version must NOT include schemaType"
    );
    assert_eq!(got_av_ver["subject"], "av-value");
    assert_eq!(got_av_ver["version"], 1);
    assert_eq!(got_av_ver["id"], 1);

    // ── 7. GET /subjects/pb-value/versions/1 ─────────────────────────────────
    let got_pb_ver = get_json(&app, "/subjects/pb-value/versions/1").await;
    let fix_pb_ver = fixture_value("rest_get_version_protobuf.json");
    assert_eq!(
        got_pb_ver, fix_pb_ver,
        "GET /subjects/pb-value/versions/1 structurally matches rest_get_version_protobuf.json"
    );
    assert_eq!(got_pb_ver["schemaType"], "PROTOBUF");

    // ── 8. GET /subjects/js-value/versions/1 ─────────────────────────────────
    let got_js_ver = get_json(&app, "/subjects/js-value/versions/1").await;
    let fix_js_ver = fixture_value("rest_get_version_json.json");
    assert_eq!(
        got_js_ver, fix_js_ver,
        "GET /subjects/js-value/versions/1 structurally matches rest_get_version_json.json"
    );

    // ── 9. GET /config ────────────────────────────────────────────────────────
    let got_config = get_json(&app, "/config").await;
    let fix_config = fixture_value("rest_get_config.json");
    assert_eq!(
        got_config, fix_config,
        "GET /config matches rest_get_config.json"
    );

    // ── 10. GET /subjects — compare as sorted set ─────────────────────────────
    let got_subjects = get_json(&app, "/subjects").await;
    let fix_subjects = fixture_value("rest_list_subjects.json");
    let mut got_sorted = sorted_string_array(&got_subjects);
    let mut fix_sorted = sorted_string_array(&fix_subjects);
    got_sorted.sort();
    fix_sorted.sort();
    assert_eq!(
        got_sorted, fix_sorted,
        "GET /subjects (as sorted set) matches rest_list_subjects.json"
    );

    // ── 11. GET /schemas/types — compare as sorted set ────────────────────────
    let got_types = get_json(&app, "/schemas/types").await;
    let fix_types = fixture_value("rest_schema_types.json");
    let mut got_types_sorted = sorted_string_array(&got_types);
    let mut fix_types_sorted = sorted_string_array(&fix_types);
    got_types_sorted.sort();
    fix_types_sorted.sort();
    assert_eq!(
        got_types_sorted, fix_types_sorted,
        "GET /schemas/types (as sorted set) matches rest_schema_types.json"
    );

    // ── 12. Error: subject not found → 404, error_code 40401 ─────────────────
    let nf_resp = get_response(&app, "/subjects/does-not-exist/versions/1").await;
    let nf_status = nf_resp.status();
    let nf_body = body_json(nf_resp).await;

    let (fix_status, fix_err_body) = fixture_error_body("rest_err_subject_not_found.json");
    assert_eq!(
        nf_status.as_u16(),
        fix_status,
        "subject-not-found HTTP status matches fixture"
    );
    assert_eq!(
        nf_body["error_code"], fix_err_body["error_code"],
        "subject-not-found error_code matches fixture (expected 40401)"
    );

    // ── 13. Error: invalid schema → 422, error_code 42201 ────────────────────
    let bad_resp = post_register(
        &app,
        "bad-value",
        r#"{"schema":"{ this is not valid avro"}"#,
    )
    .await;
    let bad_status = bad_resp.status();
    let bad_body = body_json(bad_resp).await;

    let (fix_bad_status, fix_bad_body) = fixture_error_body("rest_err_invalid_schema.json");
    assert_eq!(
        bad_status.as_u16(),
        fix_bad_status,
        "invalid-schema HTTP status matches fixture"
    );
    assert_eq!(
        bad_body["error_code"], fix_bad_body["error_code"],
        "invalid-schema error_code matches fixture (expected 42201)"
    );

    // ── teardown ──────────────────────────────────────────────────────────────
    cancel.cancel();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_type_extension_round_trips_through_rest() {
    let protobuf_schema = r#"syntax = "proto3"; package demo; message Order { int32 id = 1; }"#;
    let (broker, store, cancel, _dir) = boot_registry().await;
    let app = rest::router(AppState { store });

    let body = serde_json::json!({
        "schemaType": "PROTOBUF",
        "messageType": "demo.Order",
        "schema": protobuf_schema,
    })
    .to_string();
    let r = post_register(&app, "orders-value", &body).await;
    assert_eq!(r.status(), StatusCode::OK, "register orders-value");

    let by_id = get_json(&app, "/schemas/ids/1").await;
    assert_eq!(by_id["messageType"], "demo.Order");

    let version = get_json(&app, "/subjects/orders-value/versions/1").await;
    assert_eq!(version["messageType"], "demo.Order");

    let lookup = body_json(post_lookup(&app, "orders-value", &body).await).await;
    assert_eq!(lookup["messageType"], "demo.Order");

    cancel.cancel();
    broker.shutdown().await;
}
