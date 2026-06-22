#![allow(clippy::unreadable_literal)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use assert2::assert;
use async_trait::async_trait;
use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use crabka_blockstore::{
    BlockDescriptor, BlockIndex, BlockKey, LabelIndex, LogRow, TimeRange, labels,
    series_fingerprint, write_log_block, write_log_block_to_object_store, write_log_index_manifest,
    write_tenant_log_index_manifest_to_object_store, write_tenant_log_index_shard_to_object_store,
    write_tenant_log_index_shards_to_object_store,
};
use crabka_observability::{
    CompactionFrontier, InMemoryWalSink, IngestLimitError, KafkaWalHeader, KafkaWalRecord,
    LogIngestLimiter, LogQueryAuthorizer, LogWalConsumer, LogWalSink, QuerierIndexSource,
    QuerierState, QueryAuthorizationError, Role, ServiceConfig, ServiceDependencies,
    SharedCompactionFrontier, SharedLogDeleteRequests, WalConsumerError, WalLogRecord, WalPosition,
    WalSinkError, build_kafka_wal_record, build_querier_state, build_service_router,
    decode_kafka_wal_record, decode_kafka_wal_record_envelope, distributor_router, loki_router,
    otlp_grpc_logs_service, otlp_grpc_logs_service_with_limiter, serve_service_listener,
    write_compaction_frontier_to_object_store,
};
use datafusion::arrow::array::{Float64Array, MapArray, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use flate2::Compression;
use flate2::write::DeflateEncoder;
use flate2::write::GzEncoder;
use futures_util::StreamExt as _;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use prost::Message as _;
use serde_json::{Value, json};
use snap::raw::Encoder as SnappyEncoder;
use std::io::Write as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tower::ServiceExt as _;

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoPushRequest {
    #[prost(message, repeated, tag = "1")]
    streams: Vec<LokiProtoStream>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoStream {
    #[prost(string, tag = "1")]
    labels: String,
    #[prost(message, repeated, tag = "2")]
    entries: Vec<LokiProtoEntry>,
    #[prost(uint64, tag = "3")]
    hash: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoEntry {
    #[prost(message, optional, tag = "1")]
    timestamp: Option<LokiProtoTimestamp>,
    #[prost(string, tag = "2")]
    line: String,
    #[prost(message, repeated, tag = "3")]
    structured_metadata: Vec<LokiProtoLabelPair>,
    #[prost(message, repeated, tag = "4")]
    parsed: Vec<LokiProtoLabelPair>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct LokiProtoLabelPair {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone)]
struct RejectingIngestLimiter;

#[async_trait]
impl LogIngestLimiter for RejectingIngestLimiter {
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError> {
        assert!(tenant == "tenant-a");
        assert!(records.len() == 1);
        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: "tenant write quota exceeded".to_string(),
        })
    }
}

#[derive(Clone)]
struct DenyingQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for DenyingQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: "tenant read ACL denied".to_string(),
        })
    }
}

#[derive(Clone)]
struct FailingWalSink;

#[async_trait]
impl LogWalSink for FailingWalSink {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        Err(WalSinkError::Append)
    }
}

#[derive(Clone)]
struct PendingWalSink;

#[async_trait]
impl LogWalSink for PendingWalSink {
    async fn append(&self, _record: WalLogRecord) -> Result<(), WalSinkError> {
        std::future::pending().await
    }
}

#[tokio::test]
async fn loki_push_endpoint_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19", "api error", {"trace_id": "abc", "status": "500"}],
                                    ["20", "api ok"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 2);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
    assert!(records[1].line == "api ok");
    assert!(records[1].structured_metadata.is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_incomplete_json_value_as_empty_line() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels == labels([("app", "api"), ("env", "prod"), ("service_name", "api")])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line.is_empty());
    assert!(records[0].structured_metadata.is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_ignores_extra_json_value_fields_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", {"trace_id": "abc"}, "extra"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([("trace_id".to_string(), "abc".to_string())])
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_value_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api ok"],
                                    "not-a-push-value"
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type"
    ));
    assert!(body.contains("not-a-push-value"));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_stream_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            "not-a-stream"
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    assert!(body.contains("not-a-stream"));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_returns_server_error_when_wal_append_fails() {
    let app = distributor_router(FailingWalSink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [["19", "api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(&json_body(response).await, "server_error", "wal sink");
}

#[tokio::test]
async fn loki_push_endpoint_accepts_gzipped_json_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    ["19", "api error", {"trace_id": "abc"}]
                ]
            }
        ]
    })
    .to_string();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let payload = encoder.finish().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([("trace_id".to_string(), "abc".to_string())])
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_deflated_json_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [
                    ["19", "api error", {"trace_id": "abc"}]
                ]
            }
        ]
    })
    .to_string();
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload.as_bytes()).unwrap();
    let payload = encoder.finish().unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "deflate")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([("trace_id".to_string(), "abc".to_string())])
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_unsupported_content_encoding_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .header("content-encoding", "br")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "not supported");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_treats_non_json_content_type_as_snappy_protobuf() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "text/plain")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "snappy");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_content_type_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json; charset")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "invalid media");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_json_content_type_parameters() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json; charset=utf-8")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(records[0].line == "api error");
}

#[tokio::test]
async fn deprecated_api_prom_push_endpoint_writes_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
}

#[test]
fn kafka_wal_record_encodes_tenant_series_key_headers_and_json_payload() {
    let labels = labels([("app", "api"), ("env", "prod")]);
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels.clone(),
        timestamp_ns: 1_900_000,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: Some(WalPosition {
            partition: 3,
            offset: 42,
        }),
    };

    let producer_record = build_kafka_wal_record("__crabka_observability_logs_wal", &record)
        .expect("producer record");

    assert!(producer_record.topic == "__crabka_observability_logs_wal");
    assert!(
        producer_record.key.as_deref()
            == Some(format!("tenant-a:{}", series_fingerprint(&labels)).as_bytes())
    );
    assert!(producer_record.timestamp_ms == Some(1));
    assert!(
        producer_record
            .headers
            .iter()
            .any(|header| header.key == "crabka-wal-record-type"
                && header.value.as_deref() == Some(b"log".as_slice()))
    );
    assert!(
        producer_record
            .headers
            .iter()
            .any(|header| header.key == "crabka-tenant"
                && header.value.as_deref() == Some(b"tenant-a".as_slice()))
    );

    let payload: serde_json::Value =
        serde_json::from_slice(producer_record.value.as_deref().unwrap()).unwrap();
    assert!(payload["tenant"] == "tenant-a");
    assert!(payload["labels"]["app"] == "api");
    assert!(payload["timestamp_ns"] == 1_900_000);
    assert!(payload["line"] == "api error");
    assert!(payload["structured_metadata"]["trace_id"] == "abc");
    assert!(payload["position"]["partition"] == 3);
    assert!(payload["position"]["offset"] == 42);
}

#[test]
fn kafka_wal_record_decodes_payload_with_consumed_position() {
    let labels = labels([("app", "api"), ("env", "prod")]);
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels.clone(),
        timestamp_ns: 1_900_000,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        position: None,
    };

    let producer_record = build_kafka_wal_record("__crabka_observability_logs_wal", &record)
        .expect("producer record");
    let decoded = decode_kafka_wal_record(producer_record.value.as_deref().unwrap(), 7, 99)
        .expect("decoded WAL record");

    assert!(
        decoded
            == WalLogRecord {
                position: Some(WalPosition {
                    partition: 7,
                    offset: 99,
                }),
                ..record
            }
    );
}

#[test]
fn kafka_wal_record_decode_rejects_invalid_payload() {
    let error = decode_kafka_wal_record(b"not json", 7, 99).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("wal record deserialization failed")
    );
}

#[test]
fn native_kafka_log_record_rejects_invalid_label_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-9bad".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka label"));
}

#[test]
fn native_kafka_log_record_rejects_invalid_metadata_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-metadata-9bad".to_string(),
                value: Some(b"metadata".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka metadata"));
}

#[test]
fn native_kafka_log_record_rejects_duplicate_label_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"worker".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("duplicate native Kafka label"));
}

#[test]
fn native_kafka_log_record_rejects_duplicate_metadata_header_name() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-metadata-trace_id".to_string(),
                value: Some(b"abc".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-metadata-trace_id".to_string(),
                value: Some(b"def".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("duplicate native Kafka metadata")
    );
}

#[test]
fn native_kafka_log_record_rejects_negative_timestamp_header() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-timestamp-ns".to_string(),
                value: Some(b"-1".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}

#[test]
fn native_kafka_log_record_rejects_negative_broker_timestamp() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(-1),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}

#[test]
fn native_kafka_log_record_rejects_broker_timestamp_overflow() {
    let error = decode_kafka_wal_record_envelope(KafkaWalRecord {
        value: b"api error".to_vec(),
        partition: 3,
        offset: 42,
        timestamp_ms: Some(i64::MAX),
        headers: vec![
            KafkaWalHeader {
                key: "crabka-wal-record-type".to_string(),
                value: Some(b"log-line".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-tenant".to_string(),
                value: Some(b"tenant-a".to_vec()),
            },
            KafkaWalHeader {
                key: "crabka-log-label-app".to_string(),
                value: Some(b"api".to_vec()),
            },
        ],
    })
    .unwrap_err();

    assert!(error.to_string().contains("invalid native Kafka timestamp"));
}

#[tokio::test]
async fn service_router_builds_distributor_role() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_unix_second_ns().to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(records[0].line == "api error");
}

#[tokio::test]
async fn service_router_rejects_stale_loki_push_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [["1000000000", "stale api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("timestamp too old"));
    assert!(body.contains(r#"{app="api", service_name="api"}"#));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn service_router_rejects_future_loki_push_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();
    let timestamp = (current_unix_second_ns() + 15 * 60 * 1_000_000_000).to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [[timestamp, "future api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("timestamp too new"));
    assert!(body.contains(r#"{app="api", service_name="api"}"#));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn service_router_rejects_future_otlp_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();
    let timestamp = (current_unix_second_ns() + 15 * 60 * 1_000_000_000).to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": timestamp,
                                                "body": {"stringValue": "future otlp error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains("timestamp too new"));
    assert!(body.contains(r#"{service_name="checkout"}"#));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn service_router_rejects_loki_push_over_configured_ingest_body_limit_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: Some(1),
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [["19", "api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::TOO_MANY_REQUESTS);
    assert_loki_error(&json_body(response).await, "rate_limited", "ingest body");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn service_router_rejects_loki_push_over_ingest_quota_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default()
            .with_wal_sink(sink.clone())
            .with_ingest_limiter(RejectingIngestLimiter),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_unix_second_ns().to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::TOO_MANY_REQUESTS);
    assert_loki_error(
        &json_body(response).await,
        "rate_limited",
        "tenant write quota exceeded",
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn service_router_times_out_loki_push_when_wal_append_stalls() {
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: Some(1),
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_sink(PendingWalSink),
        None,
    )
    .await
    .unwrap();
    let timestamp = current_unix_second_ns().to_string();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "env": "prod"
                                },
                                "values": [[timestamp, "api timeout error"]]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(
        &json_body(response).await,
        "server_error",
        "wal append timed out",
    );
}

#[tokio::test]
async fn service_listener_serves_distributor_role_on_bound_tcp_listener() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_sink = sink.clone();
    let server = tokio::spawn(async move {
        serve_service_listener(
            listener,
            config,
            ServiceDependencies::default().with_wal_sink(server_sink),
            None,
        )
        .await
        .unwrap();
    });

    let timestamp = current_unix_second_ns().to_string();
    let payload = json!({
        "streams": [
            {
                "stream": {
                    "app": "api",
                    "env": "prod"
                },
                "values": [[timestamp, "api error"]]
            }
        ]
    })
    .to_string();
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(
            format!(
                "POST /loki/api/v1/push HTTP/1.1\r\nHost: {addr}\r\nX-Scope-OrgID: tenant-a\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                payload.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    server.abort();

    assert!(response.starts_with("HTTP/1.1 204 No Content"));
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(records[0].line == "api error");
}

#[tokio::test]
async fn service_listener_serves_otlp_grpc_logs_for_distributor_role() {
    let sink = InMemoryWalSink::default();
    let config = ServiceConfig {
        target: Role::Distributor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_sink = sink.clone();
    let server = tokio::spawn(async move {
        serve_service_listener(
            listener,
            config,
            ServiceDependencies::default().with_wal_sink(server_sink),
            None,
        )
        .await
        .unwrap();
    });

    let mut client = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let mut request = tonic::Request::new(proto_logs_request());
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let response = client.export(request).await.unwrap();
    server.abort();

    assert!(response.get_ref().partial_success.is_none());
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(records[0].line == "api error");
}

#[tokio::test]
async fn loki_push_endpoint_accepts_snappy_protobuf_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api", env="prod"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![
                    LokiProtoLabelPair {
                        name: "status".to_string(),
                        value: "500".to_string(),
                    },
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "abc".to_string(),
                    },
                ],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("app", "api"),
                ("detected_level", "error"),
                ("env", "prod"),
                ("service_name", "api"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_snappy_protobuf_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(vec![0xff, 0xff, 0xff]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_duplicate_protobuf_labels_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api", app="worker"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_duplicate_protobuf_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 19,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "abc".to_string(),
                    },
                    LokiProtoLabelPair {
                        name: "trace_id".to_string(),
                        value: "def".to_string(),
                    },
                ],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "structured metadata",
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["not-a-timestamp", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["not-a-timestamp", "invalid push timestamp"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|s\":[[\"not-a-timestamp\",\"invalid push timestamp\"]]}]}|...\n"
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_line_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["1000000000", 500]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|00\",500]]}]}|..., bigger context ...|ream\":{\"app\":\"api\"},\"values\":[[\"1000000000\",500]]}]}|...\n"
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["-1", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_protobuf_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: -1,
                    nanos: 0,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_out_of_range_protobuf_timestamp_nanos_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.to_string(),
            entries: vec![LokiProtoEntry {
                timestamp: Some(LokiProtoTimestamp {
                    seconds: 0,
                    nanos: 1_000_000_000,
                }),
                line: "api error".to_string(),
                structured_metadata: vec![],
                parsed: vec![],
            }],
            hash: 0,
        }],
    };
    let payload = SnappyEncoder::new()
        .compress_vec(&payload.encode_to_vec())
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_labels_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "bad-label": "api"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "couldn't parse labels: 1:5: parse error: unexpected character inside braces: '-'\n"
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_json_labels_using_last_value() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "streams": [
                            {
                                "stream": {
                                    "app": "api",
                                    "app": "worker"
                                },
                                "values": [
                                    ["19", "api error"]
                                ]
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("app", "worker"),
                ("detected_level", "error"),
                ("service_name", "worker"),
            ])
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_json_labels_with_unknown_service() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {},
                                "values": [
                                    ["19", "api info"]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("detected_level", "info"),
                ("service_name", "unknown_service"),
            ])
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", {"9bad": "metadata"}]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "structured metadata",
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_duplicate_json_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    [
                                        "19",
                                        "api error",
                                        {
                                            "trace_id": "abc",
                                            "trace_id": "def"
                                        }
                                    ]
                                ]
                            }
                        ]
                    }"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "structured metadata",
    );
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_string_json_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    for structured_metadata in [json!({"status": 500}), json!({"nested": {"status": "500"}})] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/push")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "streams": [
                                {
                                    "stream": {
                                        "app": "api"
                                    },
                                    "values": [
                                        ["19", "api error", structured_metadata]
                                    ]
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(text_body(response).await.contains(
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string"
        ));
    }

    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_structured_metadata_like_loki() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/push")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "streams": [
                            {
                                "stream": {
                                    "app": "api"
                                },
                                "values": [
                                    ["19", "api error", null]
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = text_body(response).await;
    assert!(body.contains(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object"
    ));
    assert!(body.contains("api error\",null"));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "scope": {
                                            "attributes": [
                                                {"key": "instrumentation.scope", "value": {"stringValue": "api"}}
                                            ]
                                        },
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "status", "value": {"intValue": "500"}},
                                                    {"key": "trace_id", "value": {"stringValue": "abc"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("deployment_environment", "prod"),
                ("instrumentation_scope", "api"),
                ("service_name", "checkout"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_preserves_severity_fields_as_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "severityText": "ERROR",
                                                "severityNumber": 17,
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "trace_id", "value": {"stringValue": "abc"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("severity_number".to_string(), "17".to_string()),
                ("severity_text".to_string(), "ERROR".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_returns_server_error_when_wal_append_fails() {
    let app = distributor_router(FailingWalSink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(&json_body(response).await, "server_error", "wal sink");
}

#[tokio::test]
async fn otlp_logs_endpoint_normalizes_attribute_names_for_loki_labels_and_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "cloud/region", "value": {"stringValue": "us-west"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "scope": {
                                            "attributes": [
                                                {"key": "instrumentation.scope", "value": {"stringValue": "api"}}
                                            ]
                                        },
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "thread.name", "value": {"stringValue": "worker-1"}},
                                                    {"key": "http.status-code", "value": {"intValue": "500"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("cloud_region", "us-west"),
                ("instrumentation_scope", "api"),
                ("service_name", "checkout"),
            ])
    );
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("http_status_code".to_string(), "500".to_string()),
                ("thread_name".to_string(), "worker-1".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_resource_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}},
                                        {"key": "service_name", "value": {"stringValue": "billing"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_log_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"},
                                                "attributes": [
                                                    {"key": "trace.id", "value": {"stringValue": "abc"}},
                                                    {"key": "trace_id", "value": {"stringValue": "def"}}
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_discovers_service_name_label_from_resource_attributes() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "app", "value": {"stringValue": "checkout"}},
                                        {"key": "deployment.environment", "value": {"stringValue": "prod"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("app", "checkout"),
                ("deployment_environment", "prod"),
                ("service_name", "checkout"),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_uses_unknown_service_when_no_service_name_candidate_exists() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "19",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].labels == labels([("service_name", "unknown_service")]));
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_invalid_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "not-a-timestamp",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_negative_timestamp_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resourceLogs": [
                            {
                                "resource": {
                                    "attributes": [
                                        {"key": "service.name", "value": {"stringValue": "checkout"}}
                                    ]
                                },
                                "scopeLogs": [
                                    {
                                        "logRecords": [
                                            {
                                                "timeUnixNano": "-1",
                                                "body": {"stringValue": "api error"}
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "timestamp");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_accepts_protobuf_payloads() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = proto_logs_request().encode_to_vec();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].labels
            == labels([
                ("deployment_environment", "prod"),
                ("instrumentation_scope", "api"),
                ("service_name", "checkout"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_maps_proto_trace_and_span_ids_to_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    let log = &mut request.resource_logs[0].scope_logs[0].log_records[0];
    log.attributes = vec![proto_key_value("status", any_value::Value::IntValue(500))];
    log.trace_id = vec![
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10,
    ];
    log.span_id = vec![0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18];

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                (
                    "trace_id".to_string(),
                    "0102030405060708090a0b0c0d0e0f10".to_string(),
                ),
                ("span_id".to_string(), "1112131415161718".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_maps_proto_severity_fields_to_structured_metadata() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    let log = &mut request.resource_logs[0].scope_logs[0].log_records[0];
    log.severity_number = 17;
    log.severity_text = "ERROR".to_string();
    log.attributes = vec![proto_key_value(
        "trace_id",
        any_value::Value::StringValue("abc".into()),
    )];

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("severity_number".to_string(), "17".to_string()),
                ("severity_text".to_string(), "ERROR".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_duplicate_normalized_protobuf_attributes_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let mut request = proto_logs_request();
    request.resource_logs[0].resource = Some(Resource {
        attributes: vec![
            proto_key_value(
                "service.name",
                any_value::Value::StringValue("checkout".into()),
            ),
            proto_key_value(
                "service_name",
                any_value::Value::StringValue("billing".into()),
            ),
        ],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "OTLP attribute");
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_accepts_loki_otlp_path() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());
    let payload = proto_logs_request().encode_to_vec();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/otlp/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::NO_CONTENT);
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(records[0].line == "api error");
}

#[tokio::test]
async fn otlp_grpc_logs_service_writes_tenant_scoped_wal_records() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service(sink.clone());
    let mut request = tonic::Request::new(proto_logs_request());
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let response = service.export(request).await.unwrap();

    assert!(response.get_ref().partial_success.is_none());
    let records = sink.records();
    assert!(records.len() == 1);
    assert!(records[0].tenant == "tenant-a");
    assert!(
        records[0].labels
            == labels([
                ("deployment_environment", "prod"),
                ("instrumentation_scope", "api"),
                ("service_name", "checkout"),
            ])
    );
    assert!(records[0].timestamp_ns == 19);
    assert!(records[0].line == "api error");
    assert!(
        records[0].structured_metadata
            == BTreeMap::from([
                ("status".to_string(), "500".to_string()),
                ("trace_id".to_string(), "abc".to_string()),
            ])
    );
}

#[tokio::test]
async fn otlp_grpc_logs_service_rejects_missing_tenant_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service(sink.clone());

    let error = service
        .export(tonic::Request::new(proto_logs_request()))
        .await
        .unwrap_err();

    assert!(error.code() == tonic::Code::InvalidArgument);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_grpc_logs_service_rejects_ingest_quota_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let service = otlp_grpc_logs_service_with_limiter(sink.clone(), RejectingIngestLimiter);
    let mut request = tonic::Request::new(proto_logs_request());
    request
        .metadata_mut()
        .insert("x-scope-orgid", "tenant-a".parse().unwrap());

    let error = service.export(request).await.unwrap_err();

    assert!(error.code() == tonic::Code::ResourceExhausted);
    assert!(error.message().contains("tenant write quota exceeded"));
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn otlp_logs_endpoint_rejects_invalid_protobuf_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(vec![0xff, 0xff, 0xff]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_ready_endpoint_returns_ok_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(body.as_ref() == b"ready\n");
}

#[tokio::test]
async fn status_buildinfo_endpoint_returns_loki_build_info_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/status/buildinfo")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["version"] == env!("CARGO_PKG_VERSION"));
    for field in ["revision", "branch", "buildDate", "buildUser", "goVersion"] {
        assert!(body.get(field).and_then(Value::as_str).is_some());
    }
}

#[tokio::test]
async fn status_log_level_endpoint_returns_current_level() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/log_level")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"message": "Current log level is info"}));
}

#[tokio::test]
async fn status_log_level_endpoint_accepts_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to debug"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_accepts_form_post_body_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("log_level=warn"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to warn"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_prefers_form_body_over_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=debug")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("log_level=warn"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({"status": "success", "message": "Log level set to warn"})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_rejects_invalid_level() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level?log_level=trace")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({"status": "failed", "message": "unrecognized log level \"trace\""})
    );
}

#[tokio::test]
async fn status_log_level_endpoint_rejects_missing_level_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/log_level")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({"status": "failed", "message": "unrecognized log level \"\""})
    );
}

#[tokio::test]
async fn status_config_endpoint_returns_loki_yaml_placeholder() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("target: all"));
}

#[tokio::test]
async fn status_config_diff_mode_returns_loki_error() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config?mode=diff")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(text_body(response).await == "unsupported type <nil>\n");
}

#[tokio::test]
async fn status_config_defaults_mode_returns_loki_defaults_lines() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/config?mode=defaults")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("target: all\n"));
    assert!(body.contains("auth_enabled: true\n"));
}

#[tokio::test]
async fn distributor_router_exposes_loki_ingester_control_endpoints() {
    let app = distributor_router(InMemoryWalSink::default());

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/flush")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    assert!(text_body(response).await == "unset");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(text_body(response).await == "set");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/prepare_shutdown")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(text_body(response).await == "unset");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ingester/shutdown?flush=false&delete_ring_tokens=false&terminate=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/ingester/shutdown?flush=true&delete_ring_tokens=false&terminate=false")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn status_services_endpoint_returns_loki_service_states() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("server => Running\n"));
    assert!(body.contains("querier => Running\n"));
    assert!(body.contains("distributor => Running\n"));
    assert!(body.contains("compactor => Running\n"));
}

#[tokio::test]
async fn status_memberlist_endpoint_reports_memberlist_not_configured() {
    let state = fixture();
    let querier = loki_router(state);
    let distributor = distributor_router(InMemoryWalSink::default());
    let compactor = build_service_router(
        &test_service_config(Role::Compactor, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for app in [querier, distributor, compactor] {
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/memberlist")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        assert!(text_body(response).await == "This instance doesn't use memberlist.");
    }
}

#[tokio::test]
async fn status_ring_aliases_return_loki_ring_pages() {
    let state = fixture();
    let querier = loki_router(state);
    let distributor = distributor_router(InMemoryWalSink::default());
    let compactor = build_service_router(
        &test_service_config(Role::Compactor, tempfile::tempdir().unwrap().keep()),
        ServiceDependencies::default(),
        None,
    )
    .await
    .unwrap();

    for (app, path) in [
        (querier.clone(), "/ring"),
        (querier, "/scheduler/ring"),
        (distributor, "/ring"),
        (compactor, "/ring"),
    ] {
        let response = app
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = text_body(response).await;
        assert!(content_type.starts_with("text/html"));
        assert!(body.contains("Ring Status"));
        assert!(body.contains("ACTIVE"));
    }
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_loki_router() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("loki_build_info"));
    assert!(body.contains("loki_boltdb_shipper_compactor_running"));
    assert!(body.contains("crabka_observability_service_up"));
    assert!(body.contains(r#"component="querier""#));
}

#[tokio::test]
async fn status_metrics_endpoint_returns_prometheus_text_for_distributor_router() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.contains("loki_build_info"));
    assert!(body.contains("loki_boltdb_shipper_compactor_running"));
    assert!(body.contains("crabka_observability_service_up"));
    assert!(body.contains(r#"component="distributor""#));
}

#[tokio::test]
async fn compactor_router_exposes_loki_status_and_ring_endpoints() {
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let ready_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(ready_response.status() == StatusCode::OK);
    assert!(text_body(ready_response).await == "ready\n");

    let services_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        text_body(services_response)
            .await
            .contains("compactor => Running")
    );

    let metrics_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(metrics_response.status() == StatusCode::OK);
    let metrics = text_body(metrics_response).await;
    assert!(metrics.contains("crabka_observability_service_up"));
    assert!(metrics.contains(r#"component="compactor""#));

    let config_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(config_response.status() == StatusCode::OK);
    assert!(text_body(config_response).await.contains("target: all"));

    let ring_response = app
        .oneshot(
            Request::builder()
                .uri("/compactor/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(ring_response.status() == StatusCode::OK);
    let content_type = ring_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let ring_body = text_body(ring_response).await;
    assert!(content_type.starts_with("text/html"));
    assert!(ring_body.contains("Ring Status"));
    assert!(ring_body.contains("ACTIVE"));
}

#[tokio::test]
async fn compactor_delete_endpoint_tracks_and_cancels_delete_requests() {
    let dir = tempfile::tempdir().unwrap().keep();
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=1591616227&end=1591619692")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let list_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(list_response.status() == StatusCode::OK);
    let body = json_body(list_response).await;
    assert!(body.as_array().unwrap().len() == 1);
    assert!(body[0]["request_id"] == "delete-1");
    assert!(body[0]["query"] == "{app=\"api\"} |= \"secret\"");
    assert!(body[0]["start_time"] == 1_591_616_227_i64);
    assert!(body[0]["end_time"] == 1_591_619_692_i64);
    assert!(body[0]["status"] == "received");
    assert!(body[0]["created_at"].as_i64().is_some());

    let other_tenant_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(json_body(other_tenant_response).await == json!([]));

    let cancel_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/delete?request_id=delete-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(cancel_response.status() == StatusCode::NO_CONTENT);

    let list_after_cancel_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(json_body(list_after_cancel_response).await == json!([]));
}

#[tokio::test]
async fn compactor_delete_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"} |= "secret"&start=1591616227&end=1591619692"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::NO_CONTENT);

    let list_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/delete")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(list_response.status() == StatusCode::OK);
    let body = json_body(list_response).await;
    assert!(body.as_array().unwrap().len() == 1);
    assert!(body[0]["query"] == r#"{app="api&edge"} |= "secret""#);
    assert!(body[0]["start_time"] == 1_591_616_227_i64);
    assert!(body[0]["end_time"] == 1_591_619_692_i64);
}

#[tokio::test]
async fn compactor_delete_endpoint_rejects_invalid_requests() {
    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let missing_tenant_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D&start=1591616227")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(missing_tenant_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(missing_tenant_response).await["error"]
            .as_str()
            .unwrap()
            .contains("X-Scope-OrgID")
    );

    let missing_start_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(missing_start_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(missing_start_response).await["error"]
            .as_str()
            .unwrap()
            .contains("start")
    );

    let invalid_query_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=not-logql&start=1591616227")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(invalid_query_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(invalid_query_response)
            .await
            .contains("parse error")
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_stream_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size_bytes;
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["14000000000", "api ok"],
                        ["17000000000", "api later secret"]
                    ]
                }
            ])
    );
    assert!(body["data"]["stats"] == expected_loki_stats_with(block_bytes, 2, 1));
}

#[tokio::test]
async fn compactor_delete_requests_persist_for_configured_querier() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size_bytes;
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();

    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: dir.clone(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let compactor_app =
        build_service_router(&compactor_config, ServiceDependencies::default(), None)
            .await
            .unwrap();
    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let querier_app = build_service_router(&querier_config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body["data"]["result"]
            == json!([
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["14000000000", "api ok"],
                        ["17000000000", "api later secret"]
                    ]
                }
            ])
    );
    assert!(body["data"]["stats"] == expected_loki_stats_with(block_bytes, 2, 1));
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_metric_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 14_000_000_000, "api ok", BTreeMap::new()),
            LogRow::new(api, 15_000_000_000, "api secret", BTreeMap::new()),
            LogRow::new(api, 17_000_000_000, "api later secret", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    let block_bytes = block.size_bytes;
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B10s%5D%29&time=17000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [17, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_is_available_on_distributor_and_compactor_routers() {
    let distributor_app = distributor_router(InMemoryWalSink::default());

    let distributor_response = distributor_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D%20%22bar%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(distributor_response.status() == StatusCode::OK);
    assert!(
        json_body(distributor_response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );

    let config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let compactor_app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let compactor_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bfoo%3D%20%22bar%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(compactor_response.status() == StatusCode::OK);
    assert!(
        json_body(compactor_response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn distributor_ring_endpoint_returns_loki_status_page() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/distributor/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(response).await;
    assert!(content_type.starts_with("text/html"));
    assert!(body.contains("Ring Status"));
    assert!(body.contains("ACTIVE"));
}

#[tokio::test]
async fn ruler_endpoints_match_empty_rule_and_alert_lists() {
    let state = fixture();
    let app = loki_router(state);

    let loki_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(loki_rules_response.status() == StatusCode::BAD_REQUEST);
    let content_type = loki_rules_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(loki_rules_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(
        body == "unable to read rule dir /loki/rules/fake: open /loki/rules/fake: no such file or directory\n"
    );

    let prometheus_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(prometheus_rules_response.status() == StatusCode::OK);
    assert!(
        json_body(prometheus_rules_response).await
            == json!({
                "status": "success",
                "data": {
                    "groups": []
                },
                "errorType": "",
                "error": ""
            })
    );

    let api_prom_rules_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/rules")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(api_prom_rules_response.status() == StatusCode::BAD_REQUEST);
    let content_type = api_prom_rules_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(api_prom_rules_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(
        body == "unable to read rule dir /loki/rules/fake: open /loki/rules/fake: no such file or directory\n"
    );

    let prometheus_alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(prometheus_alerts_response.status() == StatusCode::OK);
    assert!(
        json_body(prometheus_alerts_response).await
            == json!({
                "status": "success",
                "data": {
                    "alerts": []
                },
                "errorType": "",
                "error": ""
            })
    );

    let api_prom_alerts_response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(api_prom_alerts_response.status() == StatusCode::NOT_FOUND);
    let content_type = api_prom_alerts_response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(api_prom_alerts_response).await;
    assert!(content_type.starts_with("text/plain"));
    assert!(body == "404 page not found\n");
}

#[tokio::test]
async fn ruler_rule_group_read_endpoints_return_loki_not_found_errors() {
    let state = fixture();
    let app = loki_router(state);

    for uri in ["/loki/api/v1/rules/default", "/api/prom/rules/default"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = text_body(response).await;
        assert!(
            body == "error parsing /loki/rules/fake/default: /loki/rules/fake/default: open /loki/rules/fake/default: no such file or directory\n"
        );
    }

    for uri in [
        "/loki/api/v1/rules/default/api-errors",
        "/api/prom/rules/default/api-errors",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        let body = text_body(response).await;
        assert!(body == "GetRuleGroup unsupported in rule local store\n");
    }
}

#[tokio::test]
async fn ruler_rule_group_endpoint_stores_and_returns_yaml_rule_groups() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
interval: 1m
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
    for: 2m
    labels:
      severity: page
    annotations:
      summary: API errors detected
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(create_response.status() == StatusCode::ACCEPTED);
    assert!(
        json_body(create_response).await
            == json!({
                "status": "success"
            })
    );

    let group_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(group_response.status() == StatusCode::OK);
    let group_body = text_body(group_response).await;
    assert!(group_body.contains("name: api-errors\n"));
    assert!(group_body.contains("alert: ApiErrors\n"));
    assert!(group_body.contains("severity: page\n"));

    let namespace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::OK);
    let namespace_body = text_body(namespace_response).await;
    assert!(namespace_body.contains("- name: api-errors\n"));
    assert!(namespace_body.contains("alert: ApiErrors\n"));

    let all_rules_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(all_rules_response.status() == StatusCode::OK);
    let all_rules_body = text_body(all_rules_response).await;
    assert!(all_rules_body.contains("default:\n"));
    assert!(all_rules_body.contains("- name: api-errors\n"));
    assert!(all_rules_body.contains("alert: ApiErrors\n"));
}

#[tokio::test]
async fn ruler_rule_group_endpoint_rejects_invalid_rule_shapes() {
    let state = fixture();
    let app = loki_router(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(
                    "\
name: bad-rules
rules:
  - alert: ApiErrors
    record: job:api_errors:rate5m
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "unable to decoded rule group\n");

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(namespace_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(namespace_response).await
            == "error parsing /loki/rules/tenant-a/default: /loki/rules/tenant-a/default: open /loki/rules/tenant-a/default: no such file or directory\n"
    );
}

#[tokio::test]
async fn ruler_rule_groups_persist_across_service_rebuilds() {
    let dir = tempfile::tempdir().unwrap().keep();
    write_log_index_manifest(&dir, &LabelIndex::default(), &BlockIndex::default()).unwrap();
    let config = test_service_config(Role::Querier, dir);

    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let rebuilt_app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();
    let response = rebuilt_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("- name: api-errors\n"));
    assert!(body.contains("alert: ApiErrors\n"));
}

#[tokio::test]
async fn prometheus_rules_endpoint_lists_stored_loki_rule_groups() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
interval: 1m
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
    for: 2m
    labels:
      severity: page
    annotations:
      summary: API errors detected
  - record: job:api_errors:rate5m
    expr: sum(rate({app=\"api\"} |= \"error\" [5m]))
    labels:
      job: api
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "groups": [
                        {
                            "name": "api-errors",
                            "file": "default",
                            "interval": 60,
                            "limit": 0,
                            "rules": [
                                {
                                    "type": "alerting",
                                    "name": "ApiErrors",
                                    "query": "count_over_time({app=\"api\"} |= \"error\" [5m]) > 0",
                                    "duration": 120,
                                    "labels": {
                                        "severity": "page"
                                    },
                                    "annotations": {
                                        "summary": "API errors detected"
                                    },
                                    "alerts": [],
                                    "health": "ok"
                                },
                                {
                                    "type": "recording",
                                    "name": "job:api_errors:rate5m",
                                    "query": "sum(rate({app=\"api\"} |= \"error\" [5m]))",
                                    "labels": {
                                        "job": "api"
                                    },
                                    "health": "ok"
                                }
                            ]
                        }
                    ]
                },
                "errorType": "",
                "error": ""
            })
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/rules")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = text_body(response).await;
    assert!(body.contains("default:"));
    assert!(body.contains("- name: api-errors\n"));
    assert!(body.contains("alert: ApiErrors\n"));
}

async fn post_loki_rule_group_for_test(app: &axum::Router, namespace: &str, rule_group: &str) {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/loki/api/v1/rules/{namespace}"))
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::ACCEPTED);
}

async fn prometheus_rules_body_for_test(app: &axum::Router, uri: &str) -> Value {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status() == StatusCode::OK);
    json_body(response).await
}

#[tokio::test]
async fn prometheus_rules_endpoint_filters_stored_loki_rule_groups() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "default",
            "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
  - record: job:api_errors:rate5m
    expr: sum(rate({app=\"api\"} |= \"error\" [5m]))
",
        ),
        (
            "jobs",
            "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
        ),
    ] {
        post_loki_rule_group_for_test(&app, namespace, rule_group).await;
    }

    let record_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?type=record").await;
    assert!(record_body["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(record_body["data"]["groups"][0]["name"] == "api-errors");
    assert!(
        record_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    assert!(record_body["data"]["groups"][0]["rules"][0]["type"] == "recording");
    assert!(record_body["data"]["groups"][0]["rules"][0]["name"] == "job:api_errors:rate5m");

    let alert_name_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?rule_name[]=WorkerErrors")
            .await;
    assert!(alert_name_body["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(alert_name_body["data"]["groups"][0]["file"] == "jobs");
    assert!(
        alert_name_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 1
    );
    assert!(alert_name_body["data"]["groups"][0]["rules"][0]["name"] == "WorkerErrors");

    let group_file_body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?rule_group[]=api-errors&file[]=default",
    )
    .await;
    assert!(group_file_body["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(group_file_body["data"]["groups"][0]["name"] == "api-errors");
    assert!(group_file_body["data"]["groups"][0]["file"] == "default");
    assert!(
        group_file_body["data"]["groups"][0]["rules"]
            .as_array()
            .unwrap()
            .len()
            == 2
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_lists_firing_loki_rule_alerts() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      severity: page
    annotations:
      summary: API errors detected
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    assert!(
        json_body(alerts_response).await
            == json!({
                "status": "success",
                "data": {
                    "alerts": [
                        {
                            "activeAt": "1970-01-01T00:00:00.000000019Z",
                            "annotations": {
                                "summary": "API errors detected"
                            },
                            "labels": {
                                "alertname": "ApiErrors",
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "severity": "page"
                            },
                            "state": "firing",
                            "value": "1"
                        }
                    ]
                },
                "errorType": "",
                "error": ""
            })
    );

    let rules_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=19&type=alert").await;
    assert!(rules_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");
    assert!(
        rules_body["data"]["groups"][0]["rules"][0]["alerts"][0]["labels"]["alertname"]
            == "ApiErrors"
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_expands_loki_rule_label_and_annotation_templates() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      route: '{{ $labels.app }}-{{ $labels.env }}'
    annotations:
      summary: 'service={{ $labels.app }} value={{ $value }}'
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    let body = json_body(alerts_response).await;
    assert!(body["data"]["alerts"].as_array().unwrap().len() == 1);
    assert!(body["data"]["alerts"][0]["labels"]["route"] == "api-prod");
    assert!(body["data"]["alerts"][0]["annotations"]["summary"] == "service=api value=1");
}

#[tokio::test]
async fn prometheus_alerts_endpoint_expands_compact_loki_rule_templates() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      route: '{{$labels.app}}-{{$labels.env}}'
    annotations:
      summary: 'service={{$labels.app}} value={{$value}}'
",
    )
    .await;

    let alerts_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(alerts_response.status() == StatusCode::OK);
    let body = json_body(alerts_response).await;
    assert!(body["data"]["alerts"].as_array().unwrap().len() == 1);
    assert!(body["data"]["alerts"][0]["labels"]["route"] == "api-prod");
    assert!(body["data"]["alerts"][0]["annotations"]["summary"] == "service=api value=1");
}

#[tokio::test]
async fn prometheus_alerts_endpoint_tracks_pending_alerts_until_for_duration_elapses() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    for: 20ns
    labels:
      severity: page
",
    )
    .await;

    let pending_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(pending_response.status() == StatusCode::OK);
    let pending_body = json_body(pending_response).await;
    assert!(pending_body["data"]["alerts"].as_array().unwrap().len() == 1);
    assert!(pending_body["data"]["alerts"][0]["state"] == "pending");
    assert!(pending_body["data"]["alerts"][0]["activeAt"] == "1970-01-01T00:00:00.000000019Z");

    let firing_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");
    assert!(
        firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["activeAt"]
            == "1970-01-01T00:00:00.000000019Z"
    );
}

#[tokio::test]
async fn prometheus_alerts_endpoint_honors_keep_firing_for_after_condition_resolves() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    keep_firing_for: 50ns
",
    )
    .await;

    let firing_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(firing_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "firing");

    let retained_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=80")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(retained_response.status() == StatusCode::OK);
    let retained_body = json_body(retained_response).await;
    assert!(retained_body["data"]["alerts"].as_array().unwrap().len() == 1);
    assert!(retained_body["data"]["alerts"][0]["state"] == "firing");
    assert!(retained_body["data"]["alerts"][0]["activeAt"] == "1970-01-01T00:00:00.00000004Z");

    let resolved_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/alerts?time=100")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resolved_response.status() == StatusCode::OK);
    let resolved_body = json_body(resolved_response).await;
    assert!(resolved_body["data"]["alerts"] == json!([]));
}

#[tokio::test]
async fn ruler_rule_group_recreate_resets_alert_for_duration_state() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    for: 20ns
";
    post_loki_rule_group_for_test(&app, "default", rule_group).await;

    let first_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=19&type=alert").await;
    assert!(first_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "pending");

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    post_loki_rule_group_for_test(&app, "default", rule_group).await;
    let recreated_body =
        prometheus_rules_body_for_test(&app, "/prometheus/api/v1/rules?time=40&type=alert").await;
    assert!(recreated_body["data"]["groups"][0]["rules"][0]["alerts"][0]["state"] == "pending");
    assert!(
        recreated_body["data"]["groups"][0]["rules"][0]["alerts"][0]["activeAt"]
            == "1970-01-01T00:00:00.00000004Z"
    );
}

#[tokio::test]
async fn prometheus_rules_endpoint_excludes_active_alerts_when_requested() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?time=19&type=alert&exclude_alerts=true",
    )
    .await;

    assert!(body["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(body["data"]["groups"][0]["rules"][0]["name"] == "ApiErrors");
    assert!(body["data"]["groups"][0]["rules"][0]["alerts"] == json!([]));
}

#[tokio::test]
async fn prometheus_rules_endpoint_filters_rules_by_configured_labels() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "default",
        "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
    labels:
      severity: page
      team: api
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
    labels:
      team: batch
",
    )
    .await;

    let body = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&match[]=%7Bseverity%3D%22page%22%7D",
    )
    .await;

    assert!(body["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(body["data"]["groups"][0]["rules"].as_array().unwrap().len() == 1);
    assert!(body["data"]["groups"][0]["rules"][0]["name"] == "ApiErrors");
    assert!(body["data"]["groups"][0]["rules"][0]["labels"]["severity"] == "page");
}

#[tokio::test]
async fn prometheus_rules_endpoint_paginates_rule_groups() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "alpha",
            "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
        ),
        (
            "bravo",
            "\
name: worker-rules
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
",
        ),
        (
            "charlie",
            "\
name: search-rules
rules:
  - alert: SearchErrors
    expr: count_over_time({app=\"search\"} |= \"error\" [30ns]) > 0
",
        ),
    ] {
        post_loki_rule_group_for_test(&app, namespace, rule_group).await;
    }

    let first_page = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=2",
    )
    .await;
    assert!(first_page["data"]["groups"].as_array().unwrap().len() == 2);
    assert!(first_page["data"]["groups"][0]["name"] == "api-rules");
    assert!(first_page["data"]["groups"][1]["name"] == "worker-rules");
    let token = first_page["data"]["groupNextToken"]
        .as_str()
        .expect("expected next page token");

    let second_page = prometheus_rules_body_for_test(
        &app,
        &format!(
            "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=2&group_next_token={token}"
        ),
    )
    .await;
    assert!(second_page["data"]["groups"].as_array().unwrap().len() == 1);
    assert!(second_page["data"]["groups"][0]["name"] == "search-rules");
    assert!(second_page["data"].get("groupNextToken").is_none());
}

#[tokio::test]
async fn prometheus_rules_endpoint_rejects_stale_group_next_token() {
    let state = fixture();
    let app = loki_router(state);
    post_loki_rule_group_for_test(
        &app,
        "alpha",
        "\
name: api-rules
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [30ns]) > 0
",
    )
    .await;
    post_loki_rule_group_for_test(
        &app,
        "bravo",
        "\
name: worker-rules
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [30ns]) > 0
",
    )
    .await;

    let first_page = prometheus_rules_body_for_test(
        &app,
        "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=1",
    )
    .await;
    let token = first_page["data"]["groupNextToken"]
        .as_str()
        .expect("expected next page token")
        .to_string();

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/alpha")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/prometheus/api/v1/rules?exclude_alerts=true&group_limit=1&group_next_token={token}"
                ))
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "group_next_token");
}

#[tokio::test]
async fn prometheus_rules_endpoint_rejects_group_next_token_without_matching_rule_store() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/prometheus/api/v1/rules?group_limit=1&group_next_token=stale")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "group_next_token");
}

#[tokio::test]
async fn ruler_rule_group_delete_endpoint_removes_only_the_named_group() {
    let state = fixture();
    let app = loki_router(state);

    for rule_group in [
        "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
",
        "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
    ] {
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/loki/api/v1/rules/default")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(rule_group))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(create_response.status() == StatusCode::ACCEPTED);
    }

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let deleted_group_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(deleted_group_response.status() == StatusCode::NOT_FOUND);
    assert!(text_body(deleted_group_response).await == "group does not exist\n");

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::OK);
    let namespace_body = text_body(namespace_response).await;
    assert!(!namespace_body.contains("api-errors"));
    assert!(namespace_body.contains("worker-errors"));
    assert!(namespace_body.contains("WorkerErrors"));
}

#[tokio::test]
async fn ruler_namespace_delete_endpoint_removes_only_that_namespace() {
    let state = fixture();
    let app = loki_router(state);

    for (namespace, rule_group) in [
        (
            "default",
            "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
",
        ),
        (
            "jobs",
            "\
name: worker-errors
rules:
  - alert: WorkerErrors
    expr: count_over_time({app=\"worker\"} |= \"error\" [5m]) > 0
",
        ),
    ] {
        let create_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/loki/api/v1/rules/{namespace}"))
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(rule_group))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(create_response.status() == StatusCode::ACCEPTED);
    }

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let deleted_namespace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(deleted_namespace_response.status() == StatusCode::NOT_FOUND);
    assert!(text_body(deleted_namespace_response).await == "no rule groups found\n");

    let other_namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/jobs/worker-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(other_namespace_response.status() == StatusCode::OK);
    let other_namespace_body = text_body(other_namespace_response).await;
    assert!(other_namespace_body.contains("worker-errors"));
    assert!(other_namespace_body.contains("WorkerErrors"));
}

#[tokio::test]
async fn ruler_rule_group_delete_endpoint_removes_empty_namespace() {
    let state = fixture();
    let app = loki_router(state);
    let rule_group = "\
name: api-errors
rules:
  - alert: ApiErrors
    expr: count_over_time({app=\"api\"} |= \"error\" [5m]) > 0
";

    let create_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/yaml")
                .body(Body::from(rule_group))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(create_response.status() == StatusCode::ACCEPTED);

    let delete_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/loki/api/v1/rules/default/api-errors")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::ACCEPTED);

    let namespace_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/rules/default")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(namespace_response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(namespace_response).await
            == "error parsing /loki/rules/tenant-a/default: /loki/rules/tenant-a/default: open /loki/rules/tenant-a/default: no such file or directory\n"
    );
}

#[tokio::test]
async fn ruler_ring_endpoint_returns_loki_status_page() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ruler/ring")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let body = text_body(response).await;
    assert!(content_type.starts_with("text/html"));
    assert!(body.contains("Cortex Ruler Status"));
}

#[tokio::test]
async fn format_query_endpoint_returns_formatted_logql_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D%20%22bar%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bfoo%3D%20%22bar%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "{foo=\"bar\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_prefers_form_body_over_post_query_parameter() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22worker%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "{app=\"worker\"}"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/format_query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(r#"query={app="api"} |= "a&b""#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} |= "a&b""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regex_field_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20method%3D~%22GET%7CPOST%22%20%7C%20path!~%22%2Fhealth.*%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_backtick_field_filter_strings() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20msg%20%3D%20%60api%20error%60%20%7C%20path%20%3D~%20%60%2Fapi%2F.%2B%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | msg="api error" | path=~"/api/.+""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_pattern_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20pattern%20%60%3Cmethod%3E%20%3Cpath%3E%20%28%3Cstatus%3E%29%20%3Cduration%3E%60%20%7C%20status%20%3E%3D%20500")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | pattern "<method> <path> (<status>) <duration>" | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regexp_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20regexp%20%60%28%3FP%3Cmethod%3E%5Cw%2B%29%20%28%3FP%3Cpath%3E%5B%5Cw%2F%5D%2B%29%20%5C%28%28%3FP%3Cstatus%3E%5Cd%2B%29%5C%29%20%28%3FP%3Cduration%3E.*%29%60%20%7C%20status%20%3E%3D%20500")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | regexp "(?P<method>\\w+) (?P<path>[\\w/]+) \\((?P<status>\\d+)\\) (?P<duration>.*)" | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unpack_parser_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20unpack%20%7C%20pod%20%3D%20%22pod-3223f%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | unpack | pod="pod-3223f""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_selected_json_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20json%20first_server%3D%22servers%5B0%5D%22%2C%20ua%3D%22request.headers%5B%5C%22User-Agent%5C%22%5D%22%20%7C%20ua%20%3D%20%22Agent%2F1%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua="Agent/1""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_parameterized_logfmt_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20host%2C%20fwd_ip%3D%22fwd%22%20%7C%20fwd_ip%20%3D%20%22124.133.124.161%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt host, fwd_ip="fwd" | fwd_ip="124.133.124.161""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_line_format_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B.msg%7D%7D%20%7B%7B.status%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{.msg}} {{.status}}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_line_format_template_pipelines() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20.path%20%7C%20replace%20%22%2F%22%20%22_%22%20%7C%20upper%20%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{ .path | replace \"/\" \"_\" | upper }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_additional_template_string_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20.raw%20%7C%20trim%20%7C%20trimPrefix%20%22%2F%22%20%7C%20title%20%7D%7D%20%7B%7B%20.query%20%7C%20urlencode%20%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{ .raw | trim | trimPrefix \"/\" | title }} {{ .query | urlencode }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_logical_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20contains%20%22timeout%22%20.msg%20%7D%7D%20%7B%7B%20.path%20%7C%20hasPrefix%20%22%2Fapi%22%20%7D%7D%20%7B%7B%20.path%20%7C%20hasSuffix%20%22items%22%20%7D%7D%20%7B%7B%20.method%20%7C%20eq%20%22GET%22%20%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{ contains \"timeout\" .msg }} {{ .path | hasPrefix \"/api\" }} {{ .path | hasSuffix \"items\" }} {{ .method | eq \"GET\" }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_spacing_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20alignLeft%205%20.short%20%7D%7D%7C%7B%7B%20alignRight%205%20.long%20%7D%7D%7C%7B%7B%20repeat%203%20.mark%20%7D%7D%7C%7B%7B%20.multi%20%7C%20indent%202%20%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{ alignLeft 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}|{{ .multi | indent 2 }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_regex_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B%20count%20%22o%22%20.word%20%7D%7D%7C%7B%7B%20regexReplaceAll%20%22%28f%29%28o%2B%29%22%20.word%20%22%24%7B1%7Da%22%20%7D%7D%7C%7B%7B%20regexReplaceAllLiteral%20%22%28f%29%28o%2B%29%22%20.word%20%22%24%7B1%7Da%22%20%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | line_format "{{ count \"o\" .word }}|{{ regexReplaceAll \"(f)(o+)\" .word \"${1}a\" }}|{{ regexReplaceAllLiteral \"(f)(o+)\" .word \"${1}a\" }}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_label_format_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20label_format%20route%3Dpath%2C%20summary%3D%60%7B%7B.method%7D%7D%20%7B%7B.status%7D%7D%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | label_format route=path, summary="{{.method}} {{.status}}""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_label_replace_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"label_replace(count_over_time({app="api"} |= "error" [30s]), "service", "$1-api", "app", "(.*)")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_label_join_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=label_join%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22env%22%2C%20%22missing%22%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"label_join(count_over_time({app="api"} |= "error" [30s]), "joined", "/", "app", "env", "missing")"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_vector_function_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=vector%28-2.5e-1%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": "vector(-2.5e-1)"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_metric_binary_arithmetic_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error" [30s])"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_range_selector_before_pipeline() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%20%7C%3D%20%22error%22%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"count_over_time({app="api"}[30s] |= "error" | logfmt | status >= 500)"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_approx_topk_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=approx_topk%282%2C%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"approx_topk(2, count_over_time({app="api"} |= "error" [30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_metric_binary_comparison_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3E%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"count_over_time({app="api"}[30s]) > bool count_over_time({app="api"} |= "error" [30s])"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_metric_binary_set_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20and%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"count_over_time({app="api"}[30s]) and count_over_time({app="api"} |= "error" [30s])"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_metric_binary_matching_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20ignoring%28app%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"count_over_time({app="api"}[30s]) / ignoring(app) count_over_time({app="worker"}[30s])"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_accepts_metric_binary_group_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"sum by(app, env)(count_over_time({env="prod"}[30s])) / on(env) group_left sum by(env)(count_over_time({env="prod"}[30s]))"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_decolorize_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20decolorize%20%7C%3D%20%22error%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | decolorize |= "error""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_drop_and_keep_label_expression_stages() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20drop%20level%2C%20app%3D~%22debug-.*%22%20%7C%20keep%20method%2C%20status%3D%22500%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | drop level, app=~"debug-.*" | keep method, status="500""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20cost%20%7C%20__error__%20%3D%20%22%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | unwrap cost | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_bytes_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20bytes%28size%29%20%7C%20__error__%20%3D%20%22%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | unwrap bytes(size) | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_unwrap_duration_stage() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20unwrap%20duration%28latency%29%20%7C%20__error__%20%3D%20%22%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | unwrap duration(latency) | __error__="""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_pattern_line_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%3E%20%60%3C_%3E%20caller%3Dhttp.go%3A194%20level%3Ddebug%20%3C_%3E%60%20!%3E%20%60%3C_%3E%20healthcheck%20%3C_%3E%60")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} |> "<_> caller=http.go:194 level=debug <_>" !> "<_> healthcheck <_>""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_ignores_logql_comments() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%23%20selector%20comment%0A%7C%3D%20%22error%20%23%20literal%22%20%23%20line%20filter%20comment%0A%7C%20logfmt%20%23%20parser%20comment%0A%7C%20status%20%3E%3D%20500%20%23%20field%20filter%20comment")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} |= "error # literal" | logfmt | status>=500"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_duration_and_bytes_field_filters() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20duration%20%3E%3D%2020ms%20%7C%20bytes_consumed%20%3E%201.5MiB")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | duration>=20000000ns | bytes_consumed>1572864B"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_or_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%20or%20level%20%3D%20%22warn%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | status>=500 or level="warn""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_parenthesized_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20duration%20%3E%3D%2020ms%20or%20%28method%20%3D%20%22GET%22%20and%20size%20%3C%3D%2020KB%29")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | duration>=20000000ns or (method="GET" and size<=20000B)"#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_formats_comma_and_adjacent_field_filter_chains() {
    let state = fixture();
    let app = loki_router(state);

    let comma_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%2C%20path%20!~%20%22%2Fhealth.*%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(comma_response.status() == StatusCode::OK);
    assert!(
        json_body(comma_response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | status>=500 and path!~"/health.*""#
            })
    );

    let adjacent_response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20status%20%3E%3D%20500%20path%20!~%20%22%2Fhealth.*%22")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(adjacent_response.status() == StatusCode::OK);
    assert!(
        json_body(adjacent_response).await
            == json!({
                "status": "success",
                "data": r#"{app="api"} | logfmt | status>=500 and path!~"/health.*""#
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_returns_loki_error_for_invalid_logql() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query?query=%7Bfoo%3D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
            })
    );
}

#[tokio::test]
async fn format_query_endpoint_returns_loki_error_for_missing_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/format_query")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        json_body(response).await
            == json!({
                "status": "invalid-query",
                "error": "parse error : syntax error: unexpected $end"
            })
    );
}

#[tokio::test]
async fn query_endpoint_returns_loki_streams_json_for_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_fans_out_pipe_separated_tenant_header() {
    let (state, tenant_a_bytes, tenant_b_bytes) = multi_tenant_fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-a|tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["29", "tenant-a api error"]
                            ]
                        },
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(
                        tenant_a_bytes + tenant_b_bytes,
                        2,
                        2
                    )
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_returns_loki_streams_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/query")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn deprecated_api_prom_query_endpoint_rejects_metric_results_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B5s%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "rpc error: code = Code(400) desc = legacy endpoints only support streams result type"
    );
}

#[tokio::test]
async fn deprecated_api_prom_query_range_endpoint_returns_loki_streams_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/prom/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [["19", "api error"]]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn deprecated_api_prom_query_range_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response)
            .await
            .pointer("/data/result/0/values/0/1")
            .and_then(Value::as_str)
            == Some("api error")
    );
}

#[tokio::test]
async fn query_endpoint_returns_metric_query_as_loki_vector_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_returns_vector_metrics_as_parquet_when_requested() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=2%2Avector%283%29&time=20")
                .header("X-Scope-OrgID", "tenant-a")
                .header("accept", "application/vnd.apache.parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/vnd.apache.parquet")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let mut reader = ParquetRecordBatchReader::try_new(body, 1024).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());

    assert!(batch.num_rows() == 1);
    assert!(batch.schema().field(0).name() == "timestamp");
    assert!(batch.schema().field(1).name() == "labels");
    assert!(batch.schema().field(2).name() == "value");
    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(timestamps.value(0) == 20);
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 0]);
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((values.value(0) - 6.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn query_endpoint_filters_metric_query_with_scalar_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29%20%3E%201&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [],
                    "stats": expected_loki_stats_with(1819, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_query_scalar_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29%20%2A%202&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_scalar_metric_query_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=2%20-%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30ns%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_ignoring_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20ignoring%28app%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000025, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_group_left_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [0.000000025, "0.666666666"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [0.000000025, "0.333333333"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_arithmetic_group_right_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%2F%20on%28env%29%20group_right%20sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [0.000000025, "1.5"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [0.000000025, "3"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_filters_metric_binary_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30ns%5D%29%20%3E%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3E%20bool%20on%28env%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000025, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_comparison_group_left_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=sum%20by%28app%2C%20env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29%20%3C%20bool%20on%28env%29%20group_left%20sum%20by%28env%29%28count_over_time%28%7Benv%3D%22prod%22%7D%5B30s%5D%29%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "env": "prod"
                    },
                    "value": [0.000000025, "1"]
                },
                {
                    "metric": {
                        "app": "worker",
                        "env": "prod"
                    },
                    "value": [0.000000025, "1"]
                }
            ]))
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_and() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30ns%5D%29%20and%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20and%20on%28env%29%20count_over_time%28%7Bapp%3D%22worker%22%7D%5B30s%5D%29&time=25")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000025, "2"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_metric_binary_set_unless() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30ns%5D%29%20unless%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_filters_scalar_metric_query_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=2%20%3E%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.000000019, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_binary_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%2F%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_metric_binary_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%20%3C%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_binary_set_or() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20or%20count_over_time%28%7Bapp%3D%22worker%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result")
            == Some(&json!([
                {
                    "metric": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        [0.00000003, "1"]
                    ]
                },
                {
                    "metric": {
                        "app": "worker",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        [0.00000003, "1"]
                    ]
                }
            ]))
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_approx_topk_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=approx_topk%282%2C%20count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    assert!(
        text_body(response).await == "approx_topk is not enabled. See -limits.shard_aggregations"
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_metric_query_scalar_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%3E%20bool%200&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_bool_scalar_metric_query_comparison() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=0%20%3E%20bool%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "0"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_metric_query_scalar_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%20%2A%202&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_scalar_metric_query_arithmetic() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=2%20%2A%20count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "2"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_label_replace_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=label_replace%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29%2C%20%22service%22%2C%20%22%241-api%22%2C%20%22app%22%2C%20%22%28.%2A%29%22%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "service": "api-api"
                            },
                            "value": [0.000000019, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_label_join_metric_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=label_join%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22env%22%2C%20%22missing%22%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod",
                                "joined": "api/prod/"
                            },
                            "value": [0.000000019, "1"]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_rejects_metric_pipeline_errors() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%20json%20%5B30ns%5D%29&time=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "JSONParserErr");
}

#[tokio::test]
async fn query_endpoint_accepts_fractional_unix_seconds_time() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=0.000000019",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_includes_loki_stats_object() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/stats")
            .and_then(Value::as_object)
            .is_some()
    );
}

#[tokio::test]
async fn query_endpoint_populates_loki_stats_from_planned_cold_blocks() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["data"]["stats"]["store"]["compressedBytes"] == expected_block_bytes);
    assert!(body["data"]["stats"]["store"]["decompressedBytes"] == expected_block_bytes);
    assert!(body["data"]["stats"]["store"]["decompressedLines"] == 1);
    assert!(body["data"]["stats"]["store"]["totalChunksRef"] == 1);
    assert!(body["data"]["stats"]["store"]["totalChunksDownloaded"] == 1);
    assert!(body["data"]["stats"]["summary"]["totalBytesProcessed"] == expected_block_bytes);
    assert!(body["data"]["stats"]["summary"]["totalLinesProcessed"] == 1);
}

#[tokio::test]
async fn metric_query_endpoint_populates_loki_stats_from_planned_cold_blocks() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body["data"]["resultType"] == "vector");
    assert!(body["data"]["result"][0]["value"][1] == "1");
    assert!(body["data"]["stats"]["store"]["compressedBytes"] == expected_block_bytes);
    assert!(body["data"]["stats"]["store"]["decompressedBytes"] == expected_block_bytes);
    assert!(body["data"]["stats"]["store"]["decompressedLines"] == 1);
    assert!(body["data"]["stats"]["store"]["totalChunksRef"] == 1);
    assert!(body["data"]["stats"]["store"]["totalChunksDownloaded"] == 1);
    assert!(body["data"]["stats"]["summary"]["totalBytesProcessed"] == expected_block_bytes);
    assert!(body["data"]["stats"]["summary"]["totalLinesProcessed"] == 1);
}

#[tokio::test]
async fn metric_query_endpoint_splits_stats_for_cold_blocks_and_hot_tail_samples() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "dev")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&time=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "dev"
                            },
                            "value": [0.00000003, "1"]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "value": [0.00000003, "1"]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_merges_cold_blocks_with_hot_wal_tail() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"],
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_uses_updated_shared_compaction_frontier_for_hot_tail() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: 0,
                offset: 43,
            }),
        })
        .await
        .unwrap();
    let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(0));
    let state = fixture().with_hot_tail_shared_frontier(hot_tail, frontier.clone());
    frontier.advance_partition_offset(WalPosition {
        partition: 0,
        offset: 43,
    });
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_limit_to_stream_results() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward&limit=1",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_applies_backward_direction_before_limit() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=backward&limit=1",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 0, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_defaults_to_backward_direction_before_limit() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&limit=1",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 0, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_rejects_invalid_direction() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&direction=sideways")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "invalid direction 'sideways'");
}

#[tokio::test]
async fn tail_endpoint_streams_hot_wal_tail_over_websocket() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail.clone(), 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20", "api hot error"]
                        ]
                    }
                ]
            })
    );

    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 21,
            line: "api later error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                    "streams": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["21", "api later error"]
                            ]
                        }
                    ]
            })
    );
}

#[tokio::test]
async fn tail_endpoint_applies_limit_to_hot_wal_tail_frame() {
    let hot_tail = InMemoryWalSink::default();
    for (timestamp_ns, line) in [(20, "api first error"), (21, "api second error")] {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod")]),
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            })
            .await
            .unwrap();
    }
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&limit=1"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["20", "api first error"]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn tail_endpoint_defaults_limit_to_one_hundred_entries() {
    let hot_tail = InMemoryWalSink::default();
    for index in 0..101 {
        hot_tail
            .append(WalLogRecord {
                tenant: "tenant-a".to_string(),
                labels: labels([("app", "api"), ("env", "prod")]),
                timestamp_ns: 20 + index,
                line: format!("api error {index}"),
                structured_metadata: BTreeMap::new(),
                position: None,
            })
            .await
            .unwrap();
    }
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=200"
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = socket.next().await.unwrap().unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();
    let values = frame
        .pointer("/streams/0/values")
        .and_then(Value::as_array)
        .unwrap();

    assert!(values.len() == 100);
    assert!(values.first() == Some(&json!(["20", "api error 0"])));
    assert!(values.last() == Some(&json!(["119", "api error 99"])));
}

#[tokio::test]
async fn tail_endpoint_rejects_delay_for_over_five_seconds() {
    let state = fixture();
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?delay_for=6&query=%7Bapp%3D%22api%22%7D")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let error = connect_async(request).await.unwrap_err();
    server.abort();

    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected HTTP websocket error");
    };
    assert!(response.status() == StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn tail_endpoint_accepts_delay_for_at_five_seconds() {
    let state = fixture();
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?delay_for=5&query=%7Bapp%3D%22api%22%7D")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn tail_endpoint_delays_fresh_records_when_delay_for_is_set() {
    let hot_tail = InMemoryWalSink::default();
    let timestamp_ns = i64::try_from(current_unix_epoch_nanos()).unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns,
            line: "api fresh error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, timestamp_ns.saturating_sub(1));
    let app = loki_router(state);
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!(
        "ws://{addr}/loki/api/v1/tail?delay_for=1&query=%7Bapp%3D%22api%22%7D&start={}&end={}",
        timestamp_ns.saturating_sub(1),
        timestamp_ns.saturating_add(1),
    )
    .into_client_request()
    .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    assert!(
        timeout(Duration::from_millis(150), socket.next())
            .await
            .is_err()
    );
    let _ = socket.close(None).await;
    server.abort();
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_tail_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    let compactor_config = ServiceConfig {
        target: Role::Compactor,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: Some("observability/logs".to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();

    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 15_000_000_000,
            line: "api secret".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 17_000_000_000,
            line: "api later secret".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();

    let querier_config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &querier_config,
        ServiceDependencies::default()
            .with_hot_tail(hot_tail, 0)
            .with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request =
        format!("ws://{addr}/loki/api/v1/tail?query=%7Bapp%3D%22api%22%7D&start=0&end=20000000000")
            .into_client_request()
            .unwrap();
    request
        .headers_mut()
        .insert("X-Scope-OrgID", "tenant-a".parse().unwrap());

    let (mut socket, response) = connect_async(request).await.unwrap();
    assert!(response.status() == StatusCode::SWITCHING_PROTOCOLS);
    let message = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let frame: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    server.abort();

    assert!(
        frame
            == json!({
                "streams": [
                    {
                        "stream": {
                            "app": "api",
                            "detected_level": "unknown",
                            "env": "prod"
                        },
                        "values": [
                            ["17000000000", "api later secret"]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_start_end_and_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_range_endpoint_returns_streams_as_parquet_when_requested() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .header("accept", "application/vnd.apache.parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/vnd.apache.parquet")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let mut reader = ParquetRecordBatchReader::try_new(body, 1024).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());

    assert!(batch.num_rows() == 1);
    assert!(batch.schema().field(0).name() == "timestamp");
    assert!(
        batch.schema().field(0).data_type()
            == &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    assert!(batch.schema().field(1).name() == "labels");
    assert!(batch.schema().field(2).name() == "line");
    assert!(batch.schema().field(2).data_type() == &DataType::Utf8);

    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(timestamps.value(0) == 19);
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 3]);
    let keys = labels
        .keys()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let values = labels
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(keys.value(0) == "app");
    assert!(values.value(0) == "api");
    assert!(keys.value(1) == "detected_level");
    assert!(values.value(1) == "unknown");
    assert!(keys.value(2) == "env");
    assert!(values.value(2) == "prod");
    let lines = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(lines.value(0) == "api error");
}

#[tokio::test]
async fn query_range_metric_endpoint_fans_out_pipe_separated_tenant_header() {
    let (state, tenant_a_bytes, tenant_b_bytes) = multi_tenant_fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D%29&start=29&end=29&step=1ns",
                )
                .header("X-Scope-OrgID", "tenant-a|tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.000000029, "1"]
                            ]
                        },
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                [0.000000029, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(
                        tenant_a_bytes + tenant_b_bytes,
                        2,
                        2
                    )
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_metrics_as_parquet_when_requested() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=2%2Avector%283%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .header("accept", "application/vnd.apache.parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/vnd.apache.parquet")
    );
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    let mut reader = ParquetRecordBatchReader::try_new(body, 1024).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());

    assert!(batch.num_rows() == 3);
    assert!(batch.schema().field(0).name() == "timestamp");
    assert!(
        batch.schema().field(0).data_type()
            == &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    );
    assert!(batch.schema().field(1).name() == "labels");
    assert!(batch.schema().field(2).name() == "value");
    assert!(batch.schema().field(2).data_type() == &DataType::Float64);

    let timestamps = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    assert!(timestamps.value(0) == 0);
    assert!(timestamps.value(1) == 10);
    assert!(timestamps.value(2) == 20);
    let labels = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
    assert!(labels.value_offsets() == &[0, 0, 0, 0]);
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!((values.value(0) - 6.0).abs() < f64::EPSILON);
    assert!((values.value(1) - 6.0).abs() < f64::EPSILON);
    assert!((values.value(2) - 6.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn query_range_endpoint_line_format_can_reference_log_timestamp() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%7C%20line_format%20%60%7B%7B%20__timestamp__%20%7C%20unixEpochNanos%20%7D%7D%60&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "19"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_accepts_line_and_timestamp_aliases() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%7C%20line_format%20%60%7B%7B%20line%20%7D%7D%20%7B%7B%20timestamp%20%7C%20unixEpochNanos%20%7D%7D%60&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "api error 19"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_formats_timestamp_with_date_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ __timestamp__ | date \"2006-01-02\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "1970-01-01"]])));
}

#[tokio::test]
async fn query_range_endpoint_logfmt_sanitizes_ansi_prefixed_field_names() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let mut block_index = BlockIndex::default();
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 10, TimeRange::new(10, 10).unwrap()),
        vec![LogRow::new(
            api,
            10,
            "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    block_index.insert(block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%20logfmt%20%7C%20line_format%20%60%7B%7B.msg%7D%7D%60&start=10&end=11")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    let stream = body.pointer("/data/result/0/stream").unwrap();
    assert!(stream.get("_31mstatus") == Some(&json!("503")));
    assert!(stream.get("\u{1b}[31mstatus").is_none());
    assert!(
        body.pointer("/data/result/0/values") == Some(&json!([["10", "colored parser error"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_converts_epoch_strings_with_unix_to_time_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"1679577215000\" | unixToTime | date \"2006-01-02\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "2023-03-23"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_parses_dates_with_to_date_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"2021-11-02\" | toDate \"2006-01-02\" | unixEpoch }} {{ \"2021-11-02\" | toDateInZone \"2006-01-02\" \"America/New_York\" | unixEpoch }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values") == Some(&json!([["19", "1635811200 1635825600"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_exposes_now_template_helper() {
    let state = fixture();
    let app = loki_router(state);
    let before = current_unix_epoch_nanos();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ now | unixEpochNanos }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let after = current_unix_epoch_nanos();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    let value = body
        .pointer("/data/result/0/values/0/1")
        .and_then(Value::as_str)
        .unwrap()
        .parse::<u128>()
        .unwrap();
    assert!(value >= before);
    assert!(value <= after);
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_over_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $q := fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ $q.query }}={{ $q.duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_with_current_dot_over_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ .query }}={{ .duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_with_index_and_value_variables() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $i, $q := fromJson "[{\"query\":\"rate\",\"duration\":30},{\"query\":\"sum\",\"duration\":15}]" }}{{ $i }}:{{ $q.query }}={{ $q.duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "0:rate=30;1:sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_ranges_over_from_json_objects() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $name, $duration := fromJson "{\"rate\":30,\"sum\":15}" }}{{ $name }}={{ $duration }};{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "rate=30;sum=15;"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_uses_range_else_for_empty_from_json_arrays() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ range $q := fromJson "[]" }}{{ $q.query }};{{ else }}none{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "none"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_index_and_slice_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ index (fromJson "{\"servers\":[{\"name\":\"api\"},{\"name\":\"worker\"}],\"status\":200}") "servers" 1 "name" }}|{{ slice "abcdef" 1 4 }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "worker|bcd"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_integer_math_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ add 3 2 5 }} {{ sub 5 2 }} {{ mul 5 2 3 }} {{ div 10 2 }} {{ mod 10 3 }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "10 3 30 5 1"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_float_math_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ addf 3.5 2 5 }} {{ subf 5.5 2 1.5 }} {{ mulf 5.5 2 2.5 }} {{ divf 10 2 4 }} {{ ceil 123.001 }} {{ round 123.555555 3 }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["19", "10.5 2 27.5 1.25 124 123.556"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_base64_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"hello\" | b64enc }} {{ \"aGVsbG8=\" | b64dec }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "aGVsbG8= hello"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_measurement_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ \"1m30s\" | duration }} {{ \"250ms\" | duration_seconds }} {{ \"1.5MiB\" | bytes }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "90 0.25 1572864"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_printf_template_helper() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ printf \"status=%25s\" \"500\" }} {{ printf \"%25-5.5s\" \"GET\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "status=500 GET  "]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_print_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ print \"status=\" 500 }}|{{ urlquery \"a=1 b=two\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values") == Some(&json!([["19", "status=500|a%3D1+b%3Dtwo"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_go_template_escape_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ html \"<a&b>\\\"'\" }}|{{ js \"line\\n\\\"quote\\\" <tag> &=\" }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[
                "19",
                r#"&lt;a&amp;b&gt;&#34;&#39;|line\u000A\"quote\" \u003Ctag\u003E \u0026\u003D"#
            ]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_conditional_template_blocks() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if contains \"error\" __line__ }}error{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "error"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_control_template_variable_declarations() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ if $line := __line__ }}line={{ $line }}{{ else }}missing={{ $line }}{{ end }}|{{ with $payload := fromJson "{\"route\":\"checkout\"}" }}route={{ .route }}/{{ $payload.route }}{{ else }}missing={{ $payload }}{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([["19", "line=api error|route=checkout/checkout"]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_can_reference_root_fields() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ with fromJson "{\"status\":\"200\"}" }}inner={{ .status }} root={{ $.app }}{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "inner=200 root=api"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_json_template_truthiness() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ if fromJson "[]" }}array{{ else }}empty-array{{ end }}|{{ if fromJson "{}" }}object{{ else }}empty-object{{ end }}|{{ if fromJson "0" }}number{{ else }}empty-number{{ end }}|{{ with fromJson "{\"method\":\"GET\"}" }}{{ .method }}{{ else }}missing{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/result/0/values")
            == Some(&json!([[
                "19",
                "empty-array|empty-object|empty-number|GET"
            ]]))
    );
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_else_with_template_blocks() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ with .missing }}primary={{ . }}{{ else with fromJson "{\"fallback\":\"worker\"}" }}fallback={{ .fallback }}{{ else }}none{{ end }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "fallback=worker"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_boolean_template_combinators() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if and (contains \"error\" __line__) (not (contains \"debug\" __line__)) }}matched{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "matched"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_ordering_template_helpers() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ if and (gt 2 1) (le 2 2) }}matched{{ else }}other{{ end }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "matched"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_applies_template_variable_assignments() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} |= \"error\" | line_format `{{ $line := __line__ }}seen={{ $line }}`&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "seen=api error"]])));
}

#[tokio::test]
async fn query_range_endpoint_line_format_reassigns_template_variables() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api"} |= "error" | line_format `{{ $line := __line__ }}{{ $line = print "seen=" $line }}{{ $line }}`&start=0&end=30"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "seen=api error"]])));
}

#[tokio::test]
async fn query_range_endpoint_keep_stage_suppresses_detected_level_fallback() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query={app=\"api\"} | keep app&start=0&end=30&direction=forward",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"app": "api"},
                            "values": [
                                ["10", "api ok"],
                                ["19", "api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_rfc3339_time_bounds() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=1970-01-01T00%3A00%3A00Z&end=1970-01-01T00%3A00%3A00.000000030Z")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_range_endpoint_applies_interval_to_stream_results() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api close error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 29,
            line: "api later error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=19&end=30&direction=forward&interval=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"],
                                ["29", "api later error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(1819, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_excludes_stream_entries_at_end_bound() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 29,
            line: "api boundary error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=19&end=29&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.pointer("/data/result/0/values") == Some(&json!([["19", "api error"]])));
}

#[tokio::test]
async fn query_range_endpoint_accepts_zero_interval_as_noop() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward&interval=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_negative_interval() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=30&interval=-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "interval must be >= 0");
}

#[tokio::test]
async fn query_range_endpoint_applies_since_when_start_is_absent() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&end=30&since=5ns&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_uses_default_end_with_since() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&since=2000000000s&direction=forward")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_since() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&end=1000000000&since=-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'since' parameter: not a valid duration string: \"-1\""
    );
}

#[tokio::test]
async fn query_range_endpoint_defaults_to_recent_range() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_count_over_time_matrix_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_absent_over_time_uses_selector_labels_only() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=absent_over_time%28%7Bapp%3D%22missing%22%2Cenv%3D%22prod%22%7D%5B1ns%5D%29&start=1&end=2&step=1ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "missing",
                                "env": "prod"
                            },
                            "values": [
                                [0.000000001, "1"],
                                [0.000000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_applies_negative_count_over_time_offset() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B1ns%5D%20offset%20-9ns%29&start=10&end=10&step=1ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000001, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_range_selector_before_pipeline() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%20%7C%3D%20%22error%22%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_function_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%28-2.5e-1%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "-0.25"],
                                [0.00000001, "-0.25"],
                                [0.00000002, "-0.25"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_scalar_expression_as_matrix() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=1%2B2&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "3"],
                                [0.00000001, "3"],
                                [0.00000002, "3"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_arithmetic_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%286%29%2Fvector%284%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "1.5"],
                                [0.00000001, "1.5"],
                                [0.00000002, "1.5"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_modulo_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%285%29%25vector%282%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "1"],
                                [0.00000001, "1"],
                                [0.00000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_parenthesized_vector_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%288%29%2F%28vector%281%29%2Bvector%283%29%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "2"],
                                [0.00000001, "2"],
                                [0.00000002, "2"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_literal_vector_arithmetic_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=2%2Avector%283%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "6"],
                                [0.00000001, "6"],
                                [0.00000002, "6"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_vector_bool_comparison_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%281%29%3Ebool%20vector%282%29&start=0&end=20&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {},
                            "values": [
                                [0, "0"],
                                [0.00000001, "0"],
                                [0.00000002, "0"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_metric_timestamps_as_unix_seconds_numbers() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            1_000_000_000,
            2_000_000_000,
            TimeRange::new(1_000_000_000, 2_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(api, 1_000_000_000, "api first", BTreeMap::new()),
            LogRow::new(api, 2_000_000_000, "api second", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B1ns%5D%29&start=1000000000&end=2000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown"
                            },
                            "values": [
                                [1, "1"],
                                [2, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/query_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_includes_loki_stats_object() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    let body = json_body(response).await;
    assert!(
        body.pointer("/data/stats")
            .and_then(Value::as_object)
            .is_some()
    );
}

#[tokio::test]
async fn query_range_endpoint_treats_integer_step_as_seconds() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=20&end=10000000020&step=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"],
                                [10.00000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_float_seconds_step_for_count_over_time_matrix_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=20&end=30&step=0.000000010")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"],
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_duration_step_for_count_over_time_matrix_json() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=20&end=10000000020&step=10s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"],
                                [10.00000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_compound_duration_step_for_grafana() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=20&end=90000000020&step=1m30s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_millisecond_duration_step_for_grafana() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29&start=20&end=1000000020&step=1000ms")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"],
                                [1.00000002, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_compound_duration_range_selector() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B1m30s%5D%29&start=20&end=30&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000002, "1"],
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 2, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_range_endpoint_accepts_trailing_vector_grouping() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=sum%28count_over_time%28%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30s%5D%29%29%20by%20%28env%29&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(1819, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_can_load_indexes_from_persisted_manifest() {
    let state = persisted_fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_manifest() {
    let state = tenant_object_store_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard() {
    let state = tenant_object_store_shard_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_load_tenant_index_from_object_store_shard_catalog() {
    let state = tenant_object_store_shard_catalog_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn query_endpoint_can_build_querier_from_object_store_shard_catalog_config() {
    let (state, _dir) = tenant_object_store_shard_catalog_config_fixture().await;
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn service_router_builds_querier_role_from_object_store_shard_catalog_config() {
    let (config, store, _dir) = tenant_object_store_shard_catalog_service_fixture().await;
    let app = build_service_router(&config, ServiceDependencies::default(), Some(&store))
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn service_router_applies_query_authorizer_dependency_to_querier_role() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_query_authorizer(DenyingQueryAuthorizer),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert_loki_error(
        &json_body(response).await,
        "forbidden",
        "tenant read ACL denied",
    );
}

#[tokio::test]
async fn service_router_builds_querier_role_with_hot_tail_dependency() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 19,
            line: "api error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: Some(WalPosition {
                partition: 0,
                offset: 42,
            }),
        })
        .await
        .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_hot_tail_frontier(hot_tail, CompactionFrontier::new(0)),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == expected_api_error_with_stats(expected_loki_ingester_stats_with(1))
    );
}

#[tokio::test]
async fn service_router_applies_configured_query_range_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: Some(20),
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "query range");
}

#[tokio::test]
async fn service_router_applies_configured_query_length_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: Some(10),
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "query length");
}

#[tokio::test]
async fn service_router_applies_configured_query_series_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: Some(1),
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Benv%3D%22prod%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "series");
}

#[tokio::test]
async fn service_router_applies_configured_query_bytes_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let mut block_index = BlockIndex::default();
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 10, "api ok", BTreeMap::new())],
    )
    .unwrap();
    block_index.insert(api_block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: Some(1),
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "bytes");
}

#[tokio::test]
async fn service_router_builds_querier_role_with_wal_consumer_hot_tail_poller() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    write_log_index_manifest(&dir, &label_index, &BlockIndex::default()).unwrap();

    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 19,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let consumer = RecordingWalConsumer::new(vec![vec![kafka_wal_record(&record, 0, 42)]]);
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root: dir,
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_consumer(consumer),
        None,
    )
    .await
    .unwrap();

    let body = timeout(Duration::from_millis(500), async {
        loop {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(
                            "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                        )
                        .header("X-Scope-OrgID", "tenant-a")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();

            assert!(response.status() == StatusCode::OK);
            let body = json_body(response).await;
            if body == expected_api_error_with_stats(expected_loki_ingester_stats_with(1)) {
                break body;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    assert!(body == expected_api_error_with_stats(expected_loki_ingester_stats_with(1)));
}

#[tokio::test]
async fn service_router_loads_persisted_frontier_for_configured_querier_hot_tail() {
    let (config, store, _dir) = tenant_object_store_shard_catalog_service_fixture().await;
    write_compaction_frontier_to_object_store(
        &store,
        &ObjectPath::from("indexes"),
        &CompactionFrontier::new(i64::MIN).with_partition_offset(0, 43),
    )
    .await
    .unwrap();
    let record = WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: labels([("app", "api"), ("env", "prod")]),
        timestamp_ns: 19,
        line: "api error".to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let poll_count = Arc::new(AtomicUsize::new(0));
    let consumer = RecordingWalConsumer::new(vec![vec![kafka_wal_record(&record, 0, 43)]])
        .with_poll_count(poll_count.clone());
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_wal_consumer(consumer),
        Some(&store),
    )
    .await
    .unwrap();

    timeout(Duration::from_millis(500), async {
        while poll_count.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn service_router_builds_configured_local_object_store_for_querier_role() {
    let (mut config, _store, dir) = tenant_object_store_shard_catalog_service_fixture().await;
    config.object_store_url = Some(format!("file://{}", dir.display()));
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=19",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == expected_api_error());
}

#[tokio::test]
async fn configured_object_store_query_returns_partial_warning_for_missing_block() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let readable_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .await
    .unwrap();
    let readable_block_bytes = readable_block.size_bytes;
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(readable_block_bytes, 1, 2)
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

#[tokio::test]
async fn configured_object_store_query_merges_hot_tail_with_source_split_stats() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let cold_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api cold error", BTreeMap::new())],
    )
    .await
    .unwrap();
    let cold_block_bytes = cold_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(cold_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("env", "prod")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-object-hot-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(
        &config,
        ServiceDependencies::default().with_hot_tail(hot_tail, 19),
        None,
    )
    .await
    .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&start=0&end=30&direction=forward",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                ["19", "api cold error"],
                                ["20", "api hot error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_mixed_stats_with(cold_block_bytes, 1, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_metric_query_returns_partial_warning_for_missing_block() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let readable_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let readable_block_bytes = readable_block.size_bytes;
    let missing_block = BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([api]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(readable_block);
    block_index.insert(missing_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query_range?query=count_over_time(%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22%20%5B30ns%5D)&start=30&end=30&step=1ns",
                )
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "matrix",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "prod"
                            },
                            "values": [
                                [0.00000003, "1"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(readable_block_bytes, 1, 2)
                },
                "warnings": [
                    "failed to read block tenant=tenant-a/partition=0/offsets=20-29/time=20-29.parquet"
                ]
            })
    );
}

#[tokio::test]
async fn query_endpoint_rejects_missing_tenant_header() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing X-Scope-OrgID header",
    );
}

#[tokio::test]
async fn query_endpoint_rejects_unauthorized_tenant_read() {
    let state = fixture().with_query_authorizer(DenyingQueryAuthorizer);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert_loki_error(
        &json_body(response).await,
        "forbidden",
        "tenant read ACL denied",
    );
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_invalid_logql() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn endpoints_return_loki_error_for_missing_query() {
    let state = fixture();
    let app = loki_router(state);

    for uri in [
        "/loki/api/v1/query",
        "/loki/api/v1/query_range?start=0&end=1",
        "/loki/api/v1/index/stats?start=0&end=1",
        "/loki/api/v1/index/volume?start=0&end=1",
        "/loki/api/v1/index/volume_range?start=0&end=1&step=1ns",
        "/loki/api/v1/detected_fields?start=0&end=1",
        "/loki/api/v1/detected_field/status/values?start=0&end=1",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(text_body(response).await == "parse error : syntax error: unexpected $end");
    }
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_invalid_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&limit=not-a-number")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "strconv.Atoi: parsing \"not-a-number\": invalid syntax");
}

#[tokio::test]
async fn query_endpoint_returns_loki_error_for_negative_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&limit=-1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "limit must be a positive value");
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_step() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29&start=0&end=30&step=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_step_duration() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=count_over_time%28%7Bapp%3D%22api%22%7D%5B30s%5D%29&start=0&end=30&step=not-a-number")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(text_body(response).await == "cannot parse \"not-a-number\" to a valid duration");
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_excessive_resolution() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%281%29&start=0&end=11001000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=vector%281%29&start=0&end=2595601000000000&step=1h")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn query_range_endpoint_returns_loki_error_for_invalid_start() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=not-a-number&end=1000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'start' parameter: strconv.ParseInt: parsing \"not-a-number\": invalid syntax"
    );
}

#[tokio::test]
async fn query_range_endpoint_rejects_ranges_over_configured_limit() {
    let state = fixture().with_max_query_range_ns(20);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query_range?query=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "query range");
}

#[tokio::test]
async fn query_endpoint_rejects_series_over_configured_limit() {
    let state = fixture().with_max_query_series(1);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Benv%3D%22prod%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "series");
}

#[tokio::test]
async fn query_endpoint_rejects_planned_block_bytes_over_configured_limit() {
    let state = fixture().with_max_query_bytes(1);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(&json_body(response).await, "bad_data", "bytes");
}

#[tokio::test]
async fn metadata_endpoints_return_loki_parse_error_text_for_invalid_matcher() {
    let paths = [
        "/loki/api/v1/labels?query=%7Bapp%3D",
        "/loki/api/v1/label/app/values?query=%7Bapp%3D",
        "/loki/api/v1/series?match[]=%7Bapp%3D",
    ];

    for path in paths {
        let state = fixture();
        let app = loki_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
        );
    }
}

#[tokio::test]
async fn series_endpoint_returns_loki_error_for_invalid_time_bound() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/series?start=not-a-number")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "could not parse 'start' parameter: strconv.ParseInt: parsing \"not-a-number\": invalid syntax"
    );
}

#[tokio::test]
async fn series_endpoint_allows_missing_matcher_parameter_like_loki() {
    for path in ["/loki/api/v1/series", "/api/prom/series"] {
        let dir = tempfile::tempdir().unwrap().keep();
        let state = QuerierState::new(&dir, LabelIndex::default(), BlockIndex::default());
        let app = loki_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{path}");
        assert!(
            json_body(response).await
                == json!({
                    "status": "success",
                    "data": []
                })
        );
    }
}

#[tokio::test]
async fn metadata_endpoints_reject_loki_query_ranges_over_limit() {
    let paths = [
        "/loki/api/v1/labels?start=0&end=2595601000000000",
        "/loki/api/v1/label/app/values?start=0&end=2595601000000000",
        "/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000",
    ];

    for path in paths {
        let state = fixture();
        let app = loki_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
        );
    }
}

#[tokio::test]
async fn labels_endpoint_returns_tenant_label_names() {
    let state = fixture();
    let app = loki_router(state);

    for path in ["/loki/api/v1/labels", "/loki/api/v1/label"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        assert!(
            json_body(response).await
                == json!({
                    "status": "success",
                    "data": ["app", "env"]
                })
        );
    }
}

#[tokio::test]
async fn empty_metadata_endpoints_return_loki_sparse_success_shapes() {
    let dir = tempfile::tempdir().unwrap().keep();
    let app = loki_router(QuerierState::new(
        dir,
        LabelIndex::default(),
        BlockIndex::default(),
    ));

    for path in [
        "/loki/api/v1/labels",
        "/loki/api/v1/label",
        "/loki/api/v1/label/app/values",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        assert!(json_body(response).await == json!({ "status": "success" }));
    }

    for path in [
        "/api/prom/label",
        "/api/prom/label/app/values",
        "/loki/api/v1/detected_labels?limit=10",
        "/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&limit=10",
        "/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&limit=10",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        assert!(json_body(response).await == json!({}));
    }
}

#[tokio::test]
async fn metadata_endpoints_hide_loki_detected_level_enrichment() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    label_index.insert_series(
        "tenant-a",
        labels([
            ("app", "api"),
            ("detected_level", "error"),
            ("env", "prod"),
            ("service_name", "api"),
        ]),
    );
    let app = loki_router(QuerierState::new(dir, label_index, BlockIndex::default()));

    let labels_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(labels_response.status() == StatusCode::OK);
    assert!(
        json_body(labels_response).await
            == json!({
                "status": "success",
                "data": ["app", "env", "service_name"]
            })
    );

    let series_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(series_response.status() == StatusCode::OK);
    assert!(
        json_body(series_response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "api",
                        "env": "prod",
                        "service_name": "api"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn labels_endpoint_includes_hot_wal_tail_label_names() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("level", "error")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["app", "env", "level"]
            })
    );
}

#[tokio::test]
async fn deprecated_api_prom_metadata_endpoints_return_loki_metadata() {
    let state = fixture();
    let app = loki_router(state);

    let label_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/label")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(label_response.status() == StatusCode::OK);
    assert!(
        json_body(label_response).await
            == json!({
                "values": ["app", "env"]
            })
    );

    let values_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/label/env/values")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(values_response.status() == StatusCode::OK);
    assert!(
        json_body(values_response).await
            == json!({
                "values": ["app", "env"]
            })
    );

    let series_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/prom/series?match%5B%5D=%7Bapp%3D%22api%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(series_response.status() == StatusCode::OK);
    assert!(
        json_body(series_response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "api",
                        "env": "prod"
                    }
                ]
            })
    );

    let series_post_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/prom/series")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "match%5B%5D=%7Bapp%3D%22api%22%7D&start=0&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(series_post_response.status() == StatusCode::OK);
    assert!(
        json_body(series_post_response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "api",
                        "env": "prod"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn labels_endpoint_applies_time_range() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker = label_index.insert_series(
        "tenant-a",
        labels([("app", "worker"), ("env", "prod"), ("zone", "east")]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([worker]),
    ));
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels?start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["app", "env"]
            })
    );
}

#[tokio::test]
async fn label_values_endpoint_applies_since_when_start_is_absent() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker = label_index.insert_series(
        "tenant-a",
        labels([("app", "worker"), ("env", "prod"), ("zone", "east")]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([worker]),
    ));
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/label/app/values?end=29&since=9ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["worker"]
            })
    );
}

#[tokio::test]
async fn labels_endpoint_applies_selector_query() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker = label_index.insert_series(
        "tenant-a",
        labels([("app", "worker"), ("env", "prod"), ("zone", "east")]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        BTreeSet::from([worker]),
    ));
    let app = loki_router(QuerierState::new(dir, label_index, block_index));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels?query=%7Bapp%3D%22api%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["app", "env"]
            })
    );
}

#[tokio::test]
async fn label_metadata_endpoints_accept_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let labels_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22api%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(labels_response.status() == StatusCode::OK);
    assert!(
        json_body(labels_response).await
            == json!({
                "status": "success",
                "data": ["app", "env"]
            })
    );

    let values_response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/label/app/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22worker%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(values_response.status() == StatusCode::OK);
    assert!(
        json_body(values_response).await
            == json!({
                "status": "success",
                "data": ["worker"]
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_grafana_loki_health_vector_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%281%29%2Bvector%281%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_function_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%281.5%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "1.5"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_scalar_arithmetic_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=1%2B1&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "scalar",
                    "result": [4, "2"],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_scientific_vector_function_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%28-2.5e-1%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "-0.25"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_arithmetic_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%285%29-vector%282%29%2Avector%281.5%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_power_and_modulo_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%282%29%5Evector%283%29%2Bvector%285%29%25vector%282%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "9"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_parenthesized_vector_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=%28vector%281%29%2Bvector%282%29%29%2Avector%283%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "9"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_literal_arithmetic_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%284%29%2B2&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "6"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_and_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%282%29%20and%20vector%281%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_or_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%282%29%20or%20vector%281%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_unless_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%282%29%20unless%20vector%281%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_arithmetic_on_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%286%29%20%2F%20on%28%29%20vector%283%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_bool_comparison_ignoring_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%281%29%20%3E%20bool%20ignoring%28app%29%20vector%282%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "0"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_group_left_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%286%29%20%2F%20on%28app%29%20group_left%28status%29%20vector%283%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "2"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_group_right_modifier() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%282%29%20%3E%20bool%20ignoring%28app%29%20group_right%28zone%29%20vector%281%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {},
                            "value": [4, "1"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_label_replace_vector_function() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=label_replace%28vector%281%29%2C%20%22service%22%2C%20%22api-%241%22%2C%20%22missing%22%2C%20%22%28.%2A%29%22%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "service": "api-"
                            },
                            "value": [4, "1"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_accepts_label_join_vector_function() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=label_join%28vector%281%29%2C%20%22joined%22%2C%20%22%2F%22%2C%20%22app%22%2C%20%22missing%22%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "joined": "/"
                            },
                            "value": [4, "1"]
                        }
                    ],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn query_endpoint_rejects_unsupported_scalar_vector_function_like_loki() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=abs%28vector%28-1.2%29%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status() == StatusCode::BAD_REQUEST,
        "unsupported scalar functions must stay aligned with Loki's parser"
    );
    assert!(
        text_body(response).await
            == "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER"
    );
}

#[tokio::test]
async fn query_endpoint_accepts_vector_filter_comparison_expression() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/query?query=vector%281%29%3Evector%282%29&time=4000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [],
                    "stats": expected_loki_stats()
                }
            })
    );
}

#[tokio::test]
async fn label_values_endpoint_returns_tenant_values() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/label/app/values")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["api", "worker"]
            })
    );
}

#[tokio::test]
async fn label_values_endpoint_includes_hot_wal_tail_values() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("level", "error")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/label/level/values")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["error"]
            })
    );
}

#[tokio::test]
async fn label_values_endpoint_applies_selector_query() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/label/app/values?query=%7Bapp%3D%22worker%22%7D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["worker"]
            })
    );
}

#[tokio::test]
async fn label_values_endpoint_applies_time_range() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/label/app/values?start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": ["api"]
            })
    );
}

#[tokio::test]
async fn series_endpoint_applies_matchers_time_range_and_tenant() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/series?match%5B%5D=%7Benv%3D%22prod%22%7D&start=20&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "worker",
                        "env": "prod"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn label_names_endpoint_rejects_unauthorized_tenant_read() {
    let state = fixture().with_query_authorizer(DenyingQueryAuthorizer);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::FORBIDDEN);
    assert_loki_error(
        &json_body(response).await,
        "forbidden",
        "tenant read ACL denied",
    );
}

#[tokio::test]
async fn series_endpoint_includes_matching_hot_wal_tail_series() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api"), ("level", "error")]),
            timestamp_ns: 20,
            line: "api hot error".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/series?match%5B%5D=%7Blevel%3D%22error%22%7D&start=0&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "api",
                        "level": "error"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn series_endpoint_accepts_form_encoded_post_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/series")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "match%5B%5D=%7Benv%3D%22prod%22%7D&start=20&end=30",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "worker",
                        "env": "prod"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn series_endpoint_accepts_post_query_parameters_when_body_is_empty() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/series?match%5B%5D=%7Bapp%3D%22worker%22%7D&start=20&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "worker",
                        "env": "prod"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn series_endpoint_merges_post_query_parameters_with_form_body() {
    let state = fixture();
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/series?start=20&end=30")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("match%5B%5D=%7Benv%3D%22prod%22%7D"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "worker",
                        "env": "prod"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn series_endpoint_accepts_form_post_matcher_with_raw_ampersand() {
    let hot_tail = InMemoryWalSink::default();
    hot_tail
        .append(WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: labels([("app", "api&edge"), ("level", "info")]),
            timestamp_ns: 20,
            line: "api edge hot".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        })
        .await
        .unwrap();
    let state = fixture().with_hot_tail(hot_tail, 19);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/series")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(r#"match[]={app="api&edge"}&start=0&end=30"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "app": "api&edge",
                        "level": "info"
                    }
                ]
            })
    );
}

#[tokio::test]
async fn index_stats_endpoint_returns_stream_chunk_entry_and_byte_counts() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn index_stats_endpoint_accepts_form_encoded_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/stats")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("query=%7Bapp%3D%22api%22%7D&start=10&end=19"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn index_volume_endpoint_returns_series_vector_bytes() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(worker, 19, "worker error", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "env": "prod"
                            },
                            "value": [19, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_vector_with_target_labels() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/volume_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=10&end=30&step=10ns&targetLabels=app",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api edge error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/index/volume_range")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=10&end=30&step=10ns"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api&edge"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_vector_without_target_labels() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=10&end=30&step=10ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "env": "prod"
                            },
                            "value": [30, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_start_to_recent_range() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&end=1000000000&step=1s"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK);
        assert!(
            json_body(response).await
                == json!({
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [],
                        "stats": expected_loki_stats()
                    }
                })
        );
    }
}

#[tokio::test]
async fn index_stats_endpoint_requires_start_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&end=1000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `start`",
    );
}

#[tokio::test]
async fn index_volume_endpoints_default_missing_end_to_current_time() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D%22api%22%7D&start=0"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response)
                .await
                .starts_with("the query time range exceeds the limit (query length: ")
        );
    }
}

#[tokio::test]
async fn index_stats_endpoint_requires_end_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert_loki_error(
        &json_body(response).await,
        "bad_data",
        "missing query parameter `end`",
    );
}

#[tokio::test]
async fn index_stats_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn index_volume_range_endpoint_returns_loki_error_for_zero_step() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume_range?query=%7Bapp%3D%22api%22%7D&start=0&end=1000000000&step=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(
        std::str::from_utf8(&body).unwrap()
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn index_volume_endpoint_returns_loki_error_for_invalid_aggregate_by() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=0&end=1000000000&aggregateBy=bogus")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    assert!(std::str::from_utf8(&body).unwrap() == "invalid aggregation option");
}

#[tokio::test]
async fn index_endpoints_return_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    for endpoint in ["index/stats", "index/volume", "index/volume_range"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/loki/api/v1/{endpoint}?query=%7Bapp%3D&start=0&end=1"
                    ))
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(
            text_body(response).await
                == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
        );
    }
}

#[tokio::test]
async fn index_volume_endpoint_supports_label_aggregation_and_limit() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(api, 19, "api error", BTreeMap::new())],
    )
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=10&end=19&aggregateBy=labels&targetLabels=app,env&limit=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": ""
                            },
                            "value": [19, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_groups_matching_logs_by_detected_pattern() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let worker = label_index.insert_series("tenant-a", labels([("app", "worker")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "ts=2024-03-30T23:03:40 caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=200ms msg=gRPC",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "ts=2024-03-30T23:03:41 caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=500ms msg=gRPC",
                BTreeMap::new(),
            ),
            LogRow::new(
                worker,
                1_100_000_000,
                "ts=2024-03-30T23:03:41 caller=worker.go:10 level=info duration=5ms msg=ignored",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "ts=<_> caller=grpc_logging.go:66 level=info method=/cortex.Ingester/Push duration=<_> msg=gRPC",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_excludes_entries_at_end_bound() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            1_000_000_000,
            2_000_000_000,
            TimeRange::new(1_000_000_000, 2_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                1_000_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                2_000_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_accepts_form_encoded_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/patterns")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                100_000_000,
                "status=500 user=100 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                1_100_000_000,
                "status=200 user=200 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/patterns")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=0&end=2000000000&step=1s"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_patterns_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    create_secret_delete_request(&delete_requests).await;

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                14_000_000_000,
                "status=500 user=100 secret",
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                17_000_000_000,
                "status=200 user=200 public",
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = test_service_config(Role::Querier, dir);
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000001&step=1s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> public",
                        "samples": [
                            [17, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn patterns_endpoint_returns_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D&start=0&end=1")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_discovers_json_logfmt_and_structured_metadata() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(
                api,
                10,
                r#"{"status":500,"ok":false,"path":"/checkout"}"#,
                BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            ),
            LogRow::new(
                api,
                11,
                "level=warn duration=12ms bytes=1.5MiB status=503",
                BTreeMap::new(),
            ),
            LogRow::new(
                worker,
                12,
                r#"{"status":200,"worker_field":"ignored"}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "fields": [
                    {
                        "label": "bytes",
                        "type": "bytes",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 2,
                        "parsers": null
                    },
                    {
                        "label": "duration",
                        "type": "duration",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["logfmt"]
                    },
                    {
                        "label": "ok",
                        "type": "boolean",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "path",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 2,
                        "parsers": ["json", "logfmt"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["structured_metadata"]
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_reports_stream_label_cardinality() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
            LogRow::new(worker, 12, "worker ignored", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 1
                    },
                    {
                        "label": "env",
                        "cardinality": 2
                    }
                ]
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_returns_empty_object_without_matches() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![LogRow::new(api, 10, "api prod", BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22missing%22%7D&start=10&end=20")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({}));
}

#[tokio::test]
async fn detected_labels_endpoint_defaults_missing_query_to_all_streams() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(worker, 11, "worker prod", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?start=10&end=20&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 2
                    },
                    {
                        "label": "env",
                        "cardinality": 1
                    }
                ]
            })
    );
}

#[tokio::test]
async fn detected_labels_endpoint_ignores_malformed_step_and_limit_like_loki() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api_prod = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_stage =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "stage")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api_prod, 10, "api prod", BTreeMap::new()),
            LogRow::new(api_stage, 11, "api stage", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?query=%7Bapp%3D%22api%22%7D&start=10&end=20&step=not-a-duration&limit=not-a-limit")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "detectedLabels": [
                    {
                        "label": "app",
                        "cardinality": 1
                    },
                    {
                        "label": "env",
                        "cardinality": 2
                    }
                ]
            })
    );
}

#[tokio::test]
async fn compactor_delete_requests_filter_querier_detected_fields_results() {
    let delete_requests = SharedLogDeleteRequests::default();
    create_secret_delete_request(&delete_requests).await;

    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let block = write_log_block(
        &dir,
        &BlockKey::new(
            "tenant-a",
            0,
            14_000_000_000,
            17_000_000_000,
            TimeRange::new(14_000_000_000, 17_000_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                api,
                14_000_000_000,
                r#"{"status":"500","secret_field":"hidden","msg":"secret"}"#,
                BTreeMap::new(),
            ),
            LogRow::new(
                api,
                17_000_000_000,
                r#"{"status":"200","visible_field":"kept","msg":"public"}"#,
                BTreeMap::new(),
            ),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();
    let querier_config = test_service_config(Role::Querier, dir);
    let querier_app = build_service_router(
        &querier_config,
        ServiceDependencies::default().with_delete_requests(delete_requests),
        None,
    )
    .await
    .unwrap();

    let fields_response = querier_app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(fields_response.status() == StatusCode::OK);
    assert!(
        json_body(fields_response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "msg",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "visible_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    }
                ],
                "limit": 10
            })
    );

    let values_response = querier_app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=14000000000&end=17000000000&limit=10")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(values_response.status() == StatusCode::OK);
    assert!(
        json_body(values_response).await
            == json!({
                "values": ["200"],
                "limit": 10
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_form_post_body() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"status":500}"#, BTreeMap::new()),
            LogRow::new(api, 11, "status=503", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/detected_field/status/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "query=%7Bapp%3D%22api%22%7D&start=10&end=20&limit=1",
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "values": ["500"],
                "limit": 1
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_form_post_query_with_raw_ampersand() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api&edge")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"status":500}"#, BTreeMap::new()),
            LogRow::new(api, 11, "status=503", BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/detected_field/status/values")
                .header("X-Scope-OrgID", "tenant-a")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    r#"query={app="api&edge"}&start=10&end=20&limit=1"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "values": ["500"],
                "limit": 1
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_derives_start_from_since_when_start_is_omitted() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![
            LogRow::new(api, 10, r#"{"old_field":"ignored"}"#, BTreeMap::new()),
            LogRow::new(api, 20, r#"{"new_field":"kept"}"#, BTreeMap::new()),
        ],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&end=20&since=5ns")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "new_field",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["json"]
                    }
                ],
                "limit": 1000
            })
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_accepts_step_duration_parameter() {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api")]));
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 20, TimeRange::new(10, 20).unwrap()),
        vec![LogRow::new(api, 20, r#"{"status":"200"}"#, BTreeMap::new())],
    )
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    let state = QuerierState::new(dir, label_index, block_index);
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&end=20&since=1m&step=30s")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "values": ["200"],
                "limit": 1000
            })
    );
}

#[tokio::test]
async fn detected_fields_endpoint_rejects_invalid_step_parameter() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=not-a-duration")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response)
            .await
            .contains("cannot parse \"not-a-duration\" to a valid duration")
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_zero_step() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&step=0")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "zero or negative query resolution step widths are not accepted. Try a positive integer"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_returns_loki_error_for_invalid_logql() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "parse error at line 1, col 6: syntax error: unexpected $end, expecting STRING"
    );
}

#[tokio::test]
async fn detected_fields_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_labels_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_labels?start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn detected_field_values_endpoint_rejects_loki_query_ranges_over_limit() {
    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    );
    let app = loki_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_field/status/values?query=%7Bapp%3D%22api%22%7D&start=0&end=2595601000000000")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::BAD_REQUEST);
    assert!(
        text_body(response).await
            == "the query time range exceeds the limit (query length: 721h0m1s, limit: 30d1h)"
    );
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_counts_entries_from_object_store_blocks() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let api_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let expected_block_bytes = api_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=10&end=19")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 2,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn configured_object_store_index_stats_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let expected_block_bytes = tenant_b_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/stats?query=%7Bapp%3D%22api%22%7D&start=20&end=29")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "streams": 1,
                "chunks": 1,
                "entries": 1,
                "bytes": expected_block_bytes,
            })
    );
}

#[tokio::test]
async fn configured_object_store_index_volume_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let expected_block_bytes = tenant_b_block.size_bytes;
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/index/volume?query=%7Bapp%3D%22api%22%7D&start=20&end=29")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {
                            "metric": {
                                "app": "api",
                                "env": "stage"
                            },
                            "value": [29, expected_block_bytes.to_string()]
                        }
                    ],
                    "stats": expected_loki_stats_with(expected_block_bytes, 0, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_patterns_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new(
            "tenant-b",
            0,
            100_000_000,
            1_100_000_000,
            TimeRange::new(100_000_000, 1_100_000_000).unwrap(),
        ),
        vec![
            LogRow::new(
                tenant_b_api,
                100_000_000,
                "status=500 user=123 route=/checkout",
                BTreeMap::new(),
            ),
            LogRow::new(
                tenant_b_api,
                1_100_000_000,
                "status=503 user=456 route=/checkout",
                BTreeMap::new(),
            ),
        ],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/patterns?query=%7Bapp%3D%22api%22%7D&start=0&end=2000000000&step=1s")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": [
                    {
                        "pattern": "status=<_> user=<_> route=/checkout",
                        "samples": [
                            [0, 1],
                            [1, 1]
                        ]
                    }
                ]
            })
    );
}

#[tokio::test]
async fn configured_object_store_detected_fields_endpoint_loads_request_tenant_manifest() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            r#"{"status":500}"#,
            BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/detected_fields?query=%7Bapp%3D%22api%22%7D&start=20&end=29&limit=10")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "fields": [
                    {
                        "label": "detected_level",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": null
                    },
                    {
                        "label": "status",
                        "type": "int",
                        "cardinality": 1,
                        "parsers": ["json"]
                    },
                    {
                        "label": "trace_id",
                        "type": "string",
                        "cardinality": 1,
                        "parsers": ["structured_metadata"]
                    }
                ],
                "limit": 10
            })
    );
}

#[tokio::test]
#[allow(clippy::similar_names, clippy::too_many_lines)]
async fn configured_object_store_querier_loads_manifest_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_a_api =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_a_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![LogRow::new(
            tenant_a_api,
            19,
            "tenant-a api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    let tenant_b_block_bytes = tenant_b_block.size_bytes;
    block_index.insert(tenant_a_block);
    block_index.insert(tenant_b_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(tenant_b_block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_labels_endpoint_loads_manifest_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &label_index,
        &BlockIndex::default(),
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreManifest,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"status": "success", "data": ["app", "env"]}));
}

#[tokio::test]
async fn configured_object_store_shard_catalog_querier_loads_shards_for_request_tenant_header() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    let tenant_b_block_bytes = tenant_b_block.size_bytes;
    block_index.insert(tenant_b_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &[TimeRange::new(20, 29).unwrap()],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri(
                    "/loki/api/v1/query?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22error%22&time=29",
                )
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(
        json_body(response).await
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "detected_level": "unknown",
                                "env": "stage"
                            },
                            "values": [
                                ["29", "tenant-b api error"]
                            ]
                        }
                    ],
                    "stats": expected_loki_stats_with(tenant_b_block_bytes, 1, 1)
                }
            })
    );
}

#[tokio::test]
async fn configured_object_store_shard_catalog_labels_endpoint_loads_request_tenant_shards() {
    let object_dir = tempfile::tempdir().unwrap().keep();
    let data_root = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&object_dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));
    let tenant_b_block = write_log_block_to_object_store(
        &store,
        &prefix,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .await
    .unwrap();
    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_b_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-b",
        &[TimeRange::new(20, 29).unwrap()],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: Some(format!("file://{}", object_dir.display())),
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-querier-tail".to_string(),
        data_root,
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: None,
        index_prefix: Some(prefix.to_string()),
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };
    let app = build_service_router(&config, ServiceDependencies::default(), None)
        .await
        .unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/loki/api/v1/labels")
                .header("X-Scope-OrgID", "tenant-b")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK);
    assert!(json_body(response).await == json!({"status": "success", "data": ["app", "env"]}));
}

fn fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let mut block_index = BlockIndex::default();
    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    block_index.insert(api_block);
    block_index.insert(worker_block);

    QuerierState::new(dir, label_index, block_index)
}

fn multi_tenant_fixture() -> (QuerierState, u64, u64) {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let tenant_a_api =
        label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let tenant_b_api =
        label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "stage")]));

    let tenant_a_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_a_api,
            29,
            "tenant-a api error",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let tenant_b_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-b", 0, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(
            tenant_b_api,
            29,
            "tenant-b api error",
            BTreeMap::new(),
        )],
    )
    .unwrap();
    let tenant_a_bytes = tenant_a_block.size_bytes;
    let tenant_b_bytes = tenant_b_block.size_bytes;

    let mut block_index = BlockIndex::default();
    block_index.insert(tenant_a_block);
    block_index.insert(tenant_b_block);

    (
        QuerierState::new(dir, label_index, block_index),
        tenant_a_bytes,
        tenant_b_bytes,
    )
}

fn persisted_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_log_index_manifest(&dir, &label_index, &block_index).unwrap();

    QuerierState::from_manifest(dir).unwrap()
}

async fn tenant_object_store_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_manifest_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store(dir, &store, &prefix, "tenant-a")
        .await
        .unwrap()
}

async fn tenant_object_store_shard_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let shard_range = TimeRange::new(0, 30).unwrap();
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    label_index.insert_series("tenant-b", labels([("app", "api"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    write_tenant_log_index_shard_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        shard_range,
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store_shard(dir, &store, &prefix, "tenant-a", shard_range)
        .await
        .unwrap()
}

async fn tenant_object_store_shard_catalog_fixture() -> QuerierState {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &api_block.key,
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &worker_block.key,
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(0, 19).unwrap(),
            TimeRange::new(20, 29).unwrap(),
        ],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    QuerierState::from_tenant_object_store_shards(
        dir,
        &store,
        &prefix,
        "tenant-a",
        TimeRange::new(0, 19).unwrap(),
    )
    .await
    .unwrap()
}

async fn tenant_object_store_shard_catalog_config_fixture() -> (QuerierState, std::path::PathBuf) {
    let (config, store, dir) = tenant_object_store_shard_catalog_service_fixture().await;
    let state = build_querier_state(&config, Some(&store)).await.unwrap();

    (state, dir)
}

async fn tenant_object_store_shard_catalog_service_fixture()
-> (ServiceConfig, LocalFileSystem, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap().keep();
    let store = LocalFileSystem::new_with_prefix(&dir).unwrap();
    let prefix = ObjectPath::from("indexes");
    let mut label_index = LabelIndex::default();
    let api = label_index.insert_series("tenant-a", labels([("app", "api"), ("env", "prod")]));
    let worker =
        label_index.insert_series("tenant-a", labels([("app", "worker"), ("env", "prod")]));

    let api_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 0, 10, 19, TimeRange::new(10, 19).unwrap()),
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &api_block.key,
        vec![
            LogRow::new(api, 10, "api ok", BTreeMap::new()),
            LogRow::new(api, 19, "api error", BTreeMap::new()),
        ],
    )
    .await
    .unwrap();
    let worker_block = write_log_block(
        &dir,
        &BlockKey::new("tenant-a", 1, 20, 29, TimeRange::new(20, 29).unwrap()),
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .unwrap();
    write_log_block_to_object_store(
        &store,
        &prefix,
        &worker_block.key,
        vec![LogRow::new(worker, 25, "worker error", BTreeMap::new())],
    )
    .await
    .unwrap();

    let mut block_index = BlockIndex::default();
    block_index.insert(api_block);
    block_index.insert(worker_block);
    write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        "tenant-a",
        &[
            TimeRange::new(0, 19).unwrap(),
            TimeRange::new(20, 29).unwrap(),
        ],
        &label_index,
        &block_index,
    )
    .await
    .unwrap();

    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-compactor".to_string(),
        data_root: dir.clone(),
        querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
        tenant: Some("tenant-a".to_string()),
        index_prefix: Some(prefix.to_string()),
        query_start_ns: Some(0),
        query_end_ns: Some(19),
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    };

    (config, store, dir)
}

fn proto_key_value(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        key_strindex: 0,
    }
}

fn proto_logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    proto_key_value(
                        "service.name",
                        any_value::Value::StringValue("checkout".into()),
                    ),
                    proto_key_value(
                        "deployment.environment",
                        any_value::Value::StringValue("prod".into()),
                    ),
                ],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "api".to_string(),
                    version: "1.2.3".to_string(),
                    attributes: vec![proto_key_value(
                        "instrumentation.scope",
                        any_value::Value::StringValue("api".into()),
                    )],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 19,
                    observed_time_unix_nano: 0,
                    severity_number: 0,
                    severity_text: String::new(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("api error".into())),
                    }),
                    attributes: vec![
                        proto_key_value("status", any_value::Value::IntValue(500)),
                        proto_key_value("trace_id", any_value::Value::StringValue("abc".into())),
                    ],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: vec![],
                    span_id: vec![],
                    event_name: String::new(),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

struct RecordingWalConsumer {
    batches: Vec<Vec<KafkaWalRecord>>,
    poll_count: Option<Arc<AtomicUsize>>,
}

impl RecordingWalConsumer {
    fn new(batches: Vec<Vec<KafkaWalRecord>>) -> Self {
        Self {
            batches,
            poll_count: None,
        }
    }

    fn with_poll_count(mut self, poll_count: Arc<AtomicUsize>) -> Self {
        self.poll_count = Some(poll_count);
        self
    }
}

#[async_trait]
impl LogWalConsumer for RecordingWalConsumer {
    async fn poll(&mut self, _timeout: Duration) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        if let Some(poll_count) = &self.poll_count {
            poll_count.fetch_add(1, Ordering::SeqCst);
        }
        if self.batches.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(self.batches.remove(0))
        }
    }

    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        Ok(())
    }
}

fn kafka_wal_record(record: &WalLogRecord, partition: i32, offset: i64) -> KafkaWalRecord {
    let producer_record =
        build_kafka_wal_record("__crabka_observability_logs_wal", record).expect("producer record");
    KafkaWalRecord {
        value: producer_record.value.expect("producer value").to_vec(),
        partition,
        offset,
        timestamp_ms: producer_record.timestamp_ms,
        headers: producer_record
            .headers
            .into_iter()
            .map(|header| KafkaWalHeader {
                key: header.key,
                value: header.value.map(|value| value.to_vec()),
            })
            .collect(),
    }
}

async fn json_body(response: axum::response::Response) -> Value {
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn text_body(response: axum::response::Response) -> String {
    let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}

fn current_unix_second_ns() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    i64::try_from(now).expect("unix seconds fit in i64") * 1_000_000_000
}

fn current_unix_epoch_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos()
}

fn test_service_config(target: Role, data_root: impl Into<std::path::PathBuf>) -> ServiceConfig {
    let index_prefix = if matches!(target, Role::Compactor) {
        Some("observability/logs".to_string())
    } else {
        None
    };
    ServiceConfig {
        target,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__crabka_observability_logs_wal".to_string(),
        wal_group_id: "crabka-observability-test".to_string(),
        data_root: data_root.into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range_ns: None,
        max_query_series: None,
        max_query_bytes: None,
        max_query_length: None,
        max_ingest_body_bytes: None,
        wal_append_timeout_ms: None,
    }
}

async fn create_secret_delete_request(delete_requests: &SharedLogDeleteRequests) {
    let compactor_config = test_service_config(Role::Compactor, ".");
    let compactor_app = build_service_router(
        &compactor_config,
        ServiceDependencies::default().with_delete_requests(delete_requests.clone()),
        None,
    )
    .await
    .unwrap();
    let delete_response = compactor_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/loki/api/v1/delete?query=%7Bapp%3D%22api%22%7D%20%7C%3D%20%22secret%22&start=14&end=16")
                .header("X-Scope-OrgID", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete_response.status() == StatusCode::NO_CONTENT);
}

fn assert_loki_error(body: &Value, error_type: &str, error_contains: &str) {
    assert!(body["status"] == "error");
    assert!(body["errorType"] == error_type);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|error| error.contains(error_contains))
    );
    assert!(body["data"].is_null());
}

fn expected_api_error() -> Value {
    expected_api_error_with_stats(expected_loki_stats_with(1819, 1, 1))
}

fn expected_api_error_with_stats(stats: Value) -> Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {
                        "app": "api",
                        "detected_level": "unknown",
                        "env": "prod"
                    },
                    "values": [
                        ["19", "api error"]
                    ]
                }
            ],
            "stats": stats
        }
    })
}

fn expected_loki_stats() -> Value {
    expected_loki_stats_with(0, 0, 0)
}

fn expected_loki_stats_with(bytes: u64, lines: u64, chunks: u64) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": 0,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": bytes,
            "decompressedBytes": bytes,
            "decompressedLines": lines,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": chunks,
            "totalChunksDownloaded": chunks,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": lines
        }
    })
}

fn expected_loki_ingester_stats_with(lines: u64) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": lines,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": lines,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": 0,
            "totalChunksDownloaded": 0,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": 0,
            "totalLinesProcessed": lines
        }
    })
}

fn expected_loki_mixed_stats_with(
    bytes: u64,
    store_lines: u64,
    ingester_lines: u64,
    chunks: u64,
) -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": ingester_lines,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": ingester_lines,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": bytes,
            "decompressedBytes": bytes,
            "decompressedLines": store_lines,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": chunks,
            "totalChunksDownloaded": chunks,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": bytes,
            "totalLinesProcessed": store_lines + ingester_lines
        }
    })
}
