//! Integration test: `SchemaRegistryCodec` encode→frame→decode round-trips for
//! Avro, JSON Schema, and Protobuf against a mock Confluent Schema Registry
//! HTTP server.
//!
//! No broker, no Docker, no real registry. The mock server is a tiny in-process
//! axum server that serves the three endpoints the codec calls.

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use crabka_grpc_gateway::{
    codec::{EncodeBody, RecordCodec, SchemaFormat, SchemaMeta, SchemaSelector},
    schema::{client::SchemaRegistryClient, codec::SchemaRegistryCodec},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

// ── Schemas served by the mock registry ────────────────────────────────────

const AVRO_SCHEMA: &str = r#"{
    "type": "record",
    "name": "O",
    "fields": [
        {"name": "id",   "type": "long"},
        {"name": "k",    "type": "string"}
    ]
}"#;

const JSON_SCHEMA: &str =
    r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}"#;

const PROTO_SCHEMA: &str = r#"syntax = "proto3"; message O { int64 id = 1; string k = 2; }"#;

/// Schema id → (schema string, schemaType string).
type SchemaMap = HashMap<i32, (String, String)>;

/// Build the in-memory schema map used by the mock registry.
fn schema_map() -> SchemaMap {
    let mut m = HashMap::new();
    m.insert(1, (AVRO_SCHEMA.to_owned(), "AVRO".to_owned()));
    m.insert(2, (JSON_SCHEMA.to_owned(), "JSON".to_owned()));
    m.insert(3, (PROTO_SCHEMA.to_owned(), "PROTOBUF".to_owned()));
    m
}

/// Subject → schema id mapping for `/subjects/{subject}/versions/latest`.
fn subject_map() -> HashMap<String, i32> {
    let mut m = HashMap::new();
    m.insert("orders-avro-value".to_owned(), 1);
    m.insert("orders-json-value".to_owned(), 2);
    m.insert("orders-proto-value".to_owned(), 3);
    m
}

// ── Mock registry axum server ───────────────────────────────────────────────

#[derive(Clone)]
struct MockState {
    schemas: Arc<SchemaMap>,
    subjects: Arc<HashMap<String, i32>>,
}

/// `GET /schemas/ids/{id}` → `{"schema":"…","schemaType":"…"}`
async fn get_schema_by_id(
    State(state): State<MockState>,
    Path(id): Path<i32>,
) -> axum::response::Response {
    match state.schemas.get(&id) {
        Some((schema, schema_type)) => Json(json!({
            "schema": schema,
            "schemaType": schema_type
        }))
        .into_response(),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({"error_code": 40403, "message": "Schema not found"})),
        )
            .into_response(),
    }
}

/// `GET /subjects/{subject}/versions/latest` → `{"id":…,"schema":"…","schemaType":"…"}`
async fn get_latest(
    State(state): State<MockState>,
    Path(subject): Path<String>,
) -> axum::response::Response {
    let Some(&id) = state.subjects.get(&subject) else {
        return (
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({"error_code": 40401, "message": "Subject not found"})),
        )
            .into_response();
    };
    let Some((schema, schema_type)) = state.schemas.get(&id) else {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error_code": 50001, "message": "Internal error"})),
        )
            .into_response();
    };
    Json(json!({
        "id": id,
        "schema": schema,
        "schemaType": schema_type
    }))
    .into_response()
}

/// Starts the mock server on `127.0.0.1:0`. Returns the bound port.
///
/// The server runs in the background for the duration of the test process.
async fn start_mock_server() -> u16 {
    let state = MockState {
        schemas: Arc::new(schema_map()),
        subjects: Arc::new(subject_map()),
    };

    let app = Router::new()
        .route("/schemas/ids/{id}", get(get_schema_by_id))
        .route("/subjects/{subject}/versions/latest", get(get_latest))
        .with_state(state);

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the spawned server task a moment to start accepting connections.
    // A small retry loop is more robust than a fixed sleep.
    let probe_addr = format!("127.0.0.1:{port}");
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&probe_addr).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    port
}

/// Build a [`SchemaRegistryCodec`] pointing at the mock server.
fn codec_for(port: u16) -> SchemaRegistryCodec {
    let base_url = format!("http://127.0.0.1:{port}/");
    let client = Arc::new(SchemaRegistryClient::new(&base_url).expect("valid URL"));
    SchemaRegistryCodec::new(client, false)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Assert the 5-byte Confluent frame header at the start of `framed`.
fn assert_confluent_header(framed: &Bytes, expected_id: i32) {
    assert!(framed.len() >= 5, "framed payload too short: {framed:?}");
    let id_bytes = [framed[1], framed[2], framed[3], framed[4]];
    let actual_id = i32::from_be_bytes(id_bytes);
    assert_eq!(
        (framed[0], actual_id),
        (0x00, expected_id),
        "complete Confluent header projection"
    );
}

// ── Round-trip tests ────────────────────────────────────────────────────────

/// Avro encode→frame→decode round-trip against the mock registry (schema id 1).
///
/// The codec fetches the schema from `GET /schemas/ids/1`, serializes the JSON
/// as Avro binary, prepends the 5-byte Confluent header, then decodes back to
/// JSON. Because Avro long fields round-trip as JSON numbers, the JSON output
/// matches the input exactly.
async fn assert_avro_round_trip_via_mock_registry() {
    let port = start_mock_server().await;
    let codec = codec_for(port);

    let json_in = br#"{"id": 1, "k": "a"}"#;

    // ── Encode ──────────────────────────────────────────────────────────────
    let framed = codec
        .encode(
            "orders-avro",
            EncodeBody::Structured {
                json: Bytes::from_static(json_in),
                schema: SchemaSelector {
                    // Explicitly pin schema id 1 (Avro).
                    subject: Some("orders-avro-value".to_owned()),
                    id: Some(1),
                    format: SchemaFormat::Avro,
                },
            },
        )
        .await
        .expect("Avro encode must succeed");

    // ── Framing assertions ───────────────────────────────────────────────────
    // Byte 0: magic 0x00.
    // Bytes 1–4: schema id 1 in big-endian.
    assert_confluent_header(&framed, 1);

    // ── Decode ──────────────────────────────────────────────────────────────
    let expected_value = framed.slice(5..);
    let decoded = codec
        .decode("orders-avro", framed)
        .await
        .expect("Avro decode must succeed");

    let json_out = decoded
        .json
        .as_deref()
        .expect("Avro decode must produce a JSON view");
    let expected: Value = serde_json::from_slice(json_in).unwrap();
    let actual: Value = serde_json::from_slice(json_out).unwrap();
    assert_eq!(
        (decoded.value, decoded.schema, actual),
        (
            expected_value,
            Some(SchemaMeta {
                subject: "orders-avro-value".to_owned(),
                id: 1,
                format: SchemaFormat::Avro,
            }),
            expected,
        ),
        "Avro decoded value"
    );
}

/// JSON Schema encode→frame→decode round-trip against the mock registry (schema id 2).
///
/// For JSON Schema the wire payload IS JSON (no binary transcoding). The framed
/// bytes are: `[0x00][id=2 BE][raw JSON]`. The decode path validates and
/// returns the JSON bytes unchanged.
async fn assert_json_schema_round_trip_via_mock_registry() {
    let port = start_mock_server().await;
    let codec = codec_for(port);

    let json_in = br#"{"id": 1}"#;

    // ── Encode ──────────────────────────────────────────────────────────────
    let framed = codec
        .encode(
            "orders-json",
            EncodeBody::Structured {
                json: Bytes::from_static(json_in),
                schema: SchemaSelector {
                    subject: Some("orders-json-value".to_owned()),
                    id: Some(2),
                    format: SchemaFormat::Json,
                },
            },
        )
        .await
        .expect("JSON Schema encode must succeed");

    // ── Framing assertions ───────────────────────────────────────────────────
    assert_confluent_header(&framed, 2);

    // ── Decode ──────────────────────────────────────────────────────────────
    let expected_value = framed.slice(5..);
    let decoded = codec
        .decode("orders-json", framed)
        .await
        .expect("JSON Schema decode must succeed");

    let json_out = decoded
        .json
        .as_deref()
        .expect("JSON Schema decode must produce a JSON view");
    let expected: Value = serde_json::from_slice(json_in).unwrap();
    let actual: Value = serde_json::from_slice(json_out).unwrap();
    assert_eq!(
        (decoded.value, decoded.schema, actual),
        (
            expected_value,
            Some(SchemaMeta {
                subject: "orders-json-value".to_owned(),
                id: 2,
                format: SchemaFormat::Json,
            }),
            expected,
        ),
        "JSON Schema decoded value"
    );
}

/// Protobuf encode→frame→decode round-trip against the mock registry (schema id 3).
///
/// After the 5-byte Confluent header the Protobuf wire format adds a
/// message-index prefix `[0x00]` (first-message optimization). After decode,
/// the proto3 JSON mapping encodes `int64` fields as decimal **strings**
/// (e.g. `"1"` not `1`) — this is spec-correct and the test explicitly checks
/// for this behavior.
async fn assert_protobuf_round_trip_via_mock_registry() {
    let port = start_mock_server().await;
    let codec = codec_for(port);

    // Use a JSON input with the int64 field as a number; prost_reflect accepts
    // both number and string on the encode side.
    let json_in = br#"{"id": 1, "k": "a"}"#;

    // ── Encode ──────────────────────────────────────────────────────────────
    let framed = codec
        .encode(
            "orders-proto",
            EncodeBody::Structured {
                json: Bytes::from_static(json_in),
                schema: SchemaSelector {
                    subject: Some("orders-proto-value".to_owned()),
                    id: Some(3),
                    format: SchemaFormat::Protobuf,
                },
            },
        )
        .await
        .expect("Protobuf encode must succeed");

    // ── Framing assertions ───────────────────────────────────────────────────
    // Byte 0: magic 0x00.
    // Bytes 1–4: schema id 3 in big-endian.
    // Byte 5: message-index prefix 0x00 (first-message optimization).
    assert_confluent_header(&framed, 3);
    assert_eq!(
        framed[5], 0x00,
        "Protobuf frame must carry message-index prefix 0x00 at byte 5"
    );
    assert!(
        framed.len() > 6,
        "Protobuf frame must contain proto bytes after the message-index prefix"
    );

    // ── Decode ──────────────────────────────────────────────────────────────
    let expected_value = framed.slice(6..);
    let decoded = codec
        .decode("orders-proto", framed)
        .await
        .expect("Protobuf decode must succeed");

    let json_out = decoded
        .json
        .as_deref()
        .expect("Protobuf decode must produce a JSON view");
    let actual: Value = serde_json::from_slice(json_out).unwrap();

    // proto3 JSON: int64 fields are encoded as decimal STRINGS (not numbers),
    // per https://protobuf.dev/programming-guides/proto3/#json.
    assert_eq!(
        (decoded.value, decoded.schema, actual),
        (
            expected_value,
            Some(SchemaMeta {
                subject: "orders-proto-value".to_owned(),
                id: 3,
                format: SchemaFormat::Protobuf,
            }),
            json!({"id": "1", "k": "a"}),
        ),
        "Protobuf decoded value"
    );
}

#[tokio::test]
async fn schema_formats_round_trip_via_mock_registry() {
    for name in ["avro", "json_schema", "protobuf"] {
        match name {
            "avro" => assert_avro_round_trip_via_mock_registry().await,
            "json_schema" => assert_json_schema_round_trip_via_mock_registry().await,
            "protobuf" => assert_protobuf_round_trip_via_mock_registry().await,
            other => unreachable!("unknown case {other}"),
        }
    }
}
