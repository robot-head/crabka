//! Comprehensive Docker-backed Grafana end-to-end probe.
//!
//! This single `#[ignore]` test exercises EVERY traces integration point:
//!   * every distributor ingest door: OTLP HTTP, Tempo `/api/push`, Zipkin v2,
//!     Jaeger binary thrift, Jaeger compact thrift, OTLP gRPC, Jaeger gRPC;
//!   * every Tempo query endpoint, driven through a REAL Grafana container's
//!     Tempo-datasource proxy. This also covers the protobuf trace-by-id
//!     content negotiation, verified directly against the querier, as described
//!     below;
//!   * the full Service Graph loop: metrics-generator -> snappy/protobuf
//!     Prometheus `remote_write` -> real Prometheus -> Grafana Prometheus
//!     datasource proxy -> `PromQL` result.
//!
//! The test is ignored by default because it pulls and runs upstream Docker
//! images: `mirror.gcr.io/grafana/grafana` and `mirror.gcr.io/prom/prometheus`.
//! Run it explicitly with:
//!
//! `cargo test -p crabka-traces --test grafana_e2e -- --ignored --nocapture`
//!
//! ## Door coverage (stated honestly)
//!
//! The test drives doors D1..D7 below for real. It drives the HTTP doors with
//! `oneshot` through the production `distributor::router`, and the gRPC doors
//! through the production service structs' trait methods
//! (`OtlpGrpcService::export`, `JaegerGrpcService::post_spans`). That is the
//! same decode path a real tonic transport takes, and only the socket is
//! elided.
//!
//! The test exercises both Jaeger thrift decoders. D4 drives the **binary**
//! protocol (`application/vnd.apache.thrift.binary` ->
//! `decode_jaeger_binary_thrift`). D7 drives the **compact** protocol
//! (`application/x-thrift` -> `decode_jaeger_thrift`).
//!
//! The one entry point the test does NOT drive is the **Jaeger compact UDP**
//! receiver on port 6831. Its datagram handler decodes with the very same
//! compact `decode_jaeger_thrift` path that D7 drives over HTTP, so the test
//! does cover the decode logic itself. Only the live `UdpSocket` transport is
//! elided. Its handler is module-private, and it would add a flaky timing
//! dependency for no extra API-surface coverage. This is the sole intentional
//! omission.
//!
//! ## Service Graph faithfulness (stated honestly)
//!
//! The Service Graph leg closes the full production loop: a real `EdgeStore`
//! inside the real `MetricsGenService` -> real snappy/protobuf Prometheus
//! remote-write v1 (`PrometheusRemoteWriteSink`) -> a real Prometheus
//! remote-write receiver -> the Grafana Prometheus-datasource proxy -> a
//! `PromQL` result. The ONLY mock is `MockSpanSource`, which stands in for the
//! Kafka WAL consumer, because Kafka is out of scope for a Grafana E2E. This
//! leg is strictly stronger than the two-ends approximation in
//! `tempo_differential.rs` LEG 5.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use assert2::check;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::Engine as _;
use crabka_traceql::{
    AttrValue as TraceqlAttrValue, EngineOpts, InMemorySpanStore, InputSpan, TraceqlEngine,
};
use crabka_traces::{
    AttrValue, Span, SpanRecord, TracesError,
    distributor::{self, DistributorState, JaegerGrpcService, OtlpGrpcService, WalSink},
    metricsgen::{
        MetricsGenConfig, MetricsGenService, MockSpanSource, PrometheusRemoteWriteSink,
        SpanKind as MetricsSpanKind, SpanRecord as MetricsSpanRecord,
        StatusCode as MetricsStatusCode, SystemClock,
    },
    querier::http::{HttpConfig, router_with_config},
    wire::jaeger_grpc::api_v2::collector_service_server::CollectorService,
};
use crabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use http_body_util::BodyExt as _;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{ExportTraceServiceRequest, trace_service_server::TraceService},
    common::v1::{AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue, any_value::Value},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpStatus, TracesData},
};
use prost::Message as _;
use prost_types::{Duration as ProstDuration, Timestamp as ProstTimestamp};
use reqwest::StatusCode as ReqwestStatusCode;
use serde_json::{Value as JsonValue, json};
use testcontainers::{
    ContainerAsync, CopyDataSource, CopyTargetOptions, GenericImage, ImageExt,
    core::{Host, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tonic::Request as GrpcRequest;
use tower::ServiceExt as _;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// Mirror of Tempo's `TraceByIDResponse`, which is
/// `message TraceByIDResponse { Trace trace = 1 }`.
///
/// This type decodes the v2 trace-by-id protobuf body. The inner `Trace` is
/// wire-identical to OTLP `TracesData`, so this type models it as such.
#[derive(Clone, PartialEq, prost::Message)]
struct TraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    trace: Option<TracesData>,
}

const TENANT: &str = "tenant-a";
const DOCKER_HOST_ALIAS: &str = "host.testcontainers.internal";
/// Grafana HTTP port inside the container.
const GRAFANA_HTTP_PORT: u16 = 3000;
/// Prometheus HTTP port inside the container.
const PROM_HTTP_PORT: u16 = 9090;
/// Grafana admin username AND password. `GF_SECURITY_ADMIN_PASSWORD` sets it,
/// and every Grafana API call uses it as basic-auth.
const GRAFANA_ADMIN: &str = "admin";
const GRAFANA_TEMPO_DATASOURCE_UID: &str = "crabka-traces";
const GRAFANA_PROM_DATASOURCE_UID: &str = "crabka-service-graph";
/// `< 5` spans in the "big" trace -> forces a PARTIAL trace-by-id response.
const MAX_TRACE_SPANS: usize = 4;

const TRACE_A_HEX: &str = "11111111111111111111111111111111";
const TRACE_B_HEX: &str = "22222222222222222222222222222222";
const ROOT_SPAN_ID: [u8; 8] = [0x02; 8];
const ERROR_SPAN_ID: [u8; 8] = [0x04; 8];
const ERROR_SPAN_ID_HEX: &str = "0404040404040404";
const PROM_CONFIG: &str = "global:\n  scrape_interval: 15s\nscrape_configs: []\n";

// ---------------------------------------------------------------------------
// Recording sink (capture WAL appends in-process instead of going to Kafka).
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct CapturingSink {
    records: Arc<Mutex<Vec<SpanRecord>>>,
}

#[async_trait::async_trait]
impl WalSink for CapturingSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        self.records
            .lock()
            .map_err(|_| TracesError::Wal("capturing sink lock poisoned".into()))?
            .push(rec);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fixture traces.
// ---------------------------------------------------------------------------

/// A wall-clock-anchored timestamp 60s in the past, so the fixture falls inside
/// both the Prometheus ingestion window and the metrics query `start`/`end`.
fn now_minus_60s_ns() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    u64::try_from(now)
        .unwrap_or(u64::MAX)
        .saturating_sub(60_000_000_000)
}

/// Trace A: COMPLETE, 4 spans across two services.
///
/// This trace drives search, tags, values and metrics. The
/// `[0x0A] (client) -> [0x0B] (server)` pair across
/// `checkout-frontend -> cart-backend` is the service-graph request edge. The
/// error span `[0x04]` drives the `{ span:status = error }` search.
fn trace_a_otlp_bytes() -> Vec<u8> {
    let base = now_minus_60s_ns();
    let scope = InstrumentationScope {
        name: "crabka-e2e".into(),
        version: "1.0.0".into(),
        ..InstrumentationScope::default()
    };

    let frontend_spans = vec![
        OtlpSpan {
            trace_id: vec![0x11; 16],
            span_id: ROOT_SPAN_ID.to_vec(),
            name: "GET /checkout".into(),
            kind: 2, // SERVER
            start_time_unix_nano: base,
            end_time_unix_nano: base + 30_000_000,
            attributes: vec![string_kv("http.method", "GET")],
            status: Some(OtlpStatus {
                code: 1, // OK
                message: String::new(),
            }),
            ..OtlpSpan::default()
        },
        OtlpSpan {
            trace_id: vec![0x11; 16],
            span_id: vec![0x0A; 8],
            parent_span_id: ROOT_SPAN_ID.to_vec(),
            name: "call cart".into(),
            kind: 3, // CLIENT
            start_time_unix_nano: base + 2_000_000,
            end_time_unix_nano: base + 22_000_000,
            attributes: vec![string_kv("peer.service", "cart-backend")],
            status: Some(OtlpStatus {
                code: 1,
                message: String::new(),
            }),
            ..OtlpSpan::default()
        },
        OtlpSpan {
            trace_id: vec![0x11; 16],
            span_id: ERROR_SPAN_ID.to_vec(),
            parent_span_id: ROOT_SPAN_ID.to_vec(),
            name: "charge card".into(),
            kind: 3, // CLIENT
            start_time_unix_nano: base + 23_000_000,
            end_time_unix_nano: base + 33_000_000,
            attributes: vec![string_kv("http.method", "POST")],
            status: Some(OtlpStatus {
                code: 2, // ERROR
                message: "payment declined".into(),
            }),
            ..OtlpSpan::default()
        },
    ];

    let cart_spans = vec![OtlpSpan {
        trace_id: vec![0x11; 16],
        span_id: vec![0x0B; 8],
        parent_span_id: vec![0x0A; 8],
        name: "cart lookup".into(),
        kind: 2, // SERVER
        start_time_unix_nano: base + 4_000_000,
        end_time_unix_nano: base + 19_000_000,
        attributes: vec![string_kv("db.system", "postgresql")],
        status: Some(OtlpStatus {
            code: 1,
            message: String::new(),
        }),
        ..OtlpSpan::default()
    }];

    TracesData {
        resource_spans: vec![
            ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "checkout-frontend")],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope.clone()),
                    spans: frontend_spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            },
            ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_kv("service.name", "cart-backend")],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope),
                    spans: cart_spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            },
        ],
    }
    .encode_to_vec()
}

/// Trace B: PARTIAL, 5 spans, more than `MAX_TRACE_SPANS`.
fn trace_b_otlp_bytes() -> Vec<u8> {
    let base = now_minus_60s_ns();
    let mut spans = Vec::new();
    for (idx, id) in (0x31u8..=0x35u8).enumerate() {
        spans.push(OtlpSpan {
            trace_id: vec![0x22; 16],
            span_id: vec![id; 8],
            parent_span_id: if idx == 0 { Vec::new() } else { vec![0x31; 8] },
            name: format!("chunk {idx}"),
            kind: 1, // INTERNAL
            start_time_unix_nano: base,
            end_time_unix_nano: base + 1_000_000,
            ..OtlpSpan::default()
        });
    }
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "bulk-svc")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

/// Independently-assertable OTLP body for the OTLP/gRPC door (D5).
fn grpc_otlp_bytes() -> Vec<u8> {
    let base = now_minus_60s_ns();
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "grpc-otlp-svc")],
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![OtlpSpan {
                    trace_id: vec![0x55; 16],
                    span_id: vec![0x51; 8],
                    name: "otlp grpc op".into(),
                    kind: 2,
                    start_time_unix_nano: base,
                    end_time_unix_nano: base + 1_000_000,
                    ..OtlpSpan::default()
                }],
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
    .encode_to_vec()
}

fn string_kv(key: &str, value: &str) -> OtlpKeyValue {
    OtlpKeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
        ..OtlpKeyValue::default()
    }
}

/// Self-contained Jaeger binary-thrift batch, ported verbatim from
/// `distributor::tests::jaeger_binary_batch`.
///
/// It yields one span named `GET /binary`, with service `checkout` from its
/// embedded process.
fn jaeger_binary_batch() -> Vec<u8> {
    const T_STOP: u8 = 0;
    const T_BOOL: u8 = 2;
    const T_I32: u8 = 8;
    const T_I64: u8 = 10;
    const T_BINARY: u8 = 11;
    const T_STRUCT: u8 = 12;
    const T_LIST: u8 = 15;

    fn field(out: &mut Vec<u8>, type_: u8, id: i16) {
        out.push(type_);
        out.extend_from_slice(&id.to_be_bytes());
    }
    fn string(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
        field(out, T_BINARY, id);
        string(out, value);
    }
    fn i32_field(out: &mut Vec<u8>, id: i16, value: i32) {
        field(out, T_I32, id);
        out.extend_from_slice(&value.to_be_bytes());
    }
    fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
        field(out, T_I64, id);
        out.extend_from_slice(&value.to_be_bytes());
    }
    fn bool_field(out: &mut Vec<u8>, id: i16, value: bool) {
        field(out, T_BOOL, id);
        out.push(u8::from(value));
    }
    fn key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        string_field(out, 1, key);
        i32_field(out, 2, 0);
        string_field(out, 3, value);
        out.push(T_STOP);
    }
    fn key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        string_field(out, 1, key);
        i32_field(out, 2, 3);
        bool_field(out, 5, value);
        out.push(T_STOP);
    }

    let mut out = Vec::new();
    field(&mut out, T_STRUCT, 1);
    string_field(&mut out, 1, "checkout");
    field(&mut out, T_LIST, 2);
    out.push(T_STRUCT);
    out.extend_from_slice(&1_i32.to_be_bytes());
    key_value_string(&mut out, "process.tag", "present");
    out.push(T_STOP);

    field(&mut out, T_LIST, 2);
    out.push(T_STRUCT);
    out.extend_from_slice(&1_i32.to_be_bytes());
    i64_field(&mut out, 1, 2);
    i64_field(&mut out, 2, 1);
    i64_field(&mut out, 3, 3);
    i64_field(&mut out, 4, 0);
    string_field(&mut out, 5, "GET /binary");
    i64_field(&mut out, 8, 1_000);
    i64_field(&mut out, 9, 25);
    field(&mut out, T_LIST, 10);
    out.push(T_STRUCT);
    out.extend_from_slice(&3_i32.to_be_bytes());
    key_value_string(&mut out, "span.kind", "server");
    key_value_string(&mut out, "http.method", "GET");
    key_value_bool(&mut out, "error", true);
    out.push(T_STOP);
    out.push(T_STOP);
    out
}

/// Self-contained Jaeger **compact**-thrift batch.
///
/// The compact protocol uses field-delta headers and zig-zag varints. This
/// batch mirrors the in-crate `decode_jaeger_thrift` round-trip fixtures. It
/// drives the compact decoder through the `application/x-thrift` HTTP door
/// (D7), which is the same `decode_jaeger_thrift` path the compact UDP datagram
/// receiver uses.
///
/// It yields one span `compact thrift op`, service `compact-svc`, with the
/// `error` tag set so the decoded status is ERROR.
fn jaeger_compact_batch() -> Vec<u8> {
    fn write_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        out.push(u8::try_from(value).unwrap());
    }
    fn zigzag_i32(value: i32) -> u64 {
        u64::from(((value << 1) ^ (value >> 31)).cast_unsigned())
    }
    fn zigzag_i64(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)).cast_unsigned()
    }
    fn field_header(out: &mut Vec<u8>, type_id: u8, id: i16, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            out.push(type_id);
            write_varint(out, zigzag_i32(i32::from(id)));
        }
        *last = id;
    }
    fn list_header(out: &mut Vec<u8>, element_type: u8, size: usize) {
        if size < 15 {
            out.push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            out.push(0xF0 | element_type);
            write_varint(out, u64::try_from(size).unwrap());
        }
    }
    fn string_field(out: &mut Vec<u8>, id: i16, value: &str, last: &mut i16) {
        field_header(out, 8, id, last); // compact BINARY/STRING = 8
        write_varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }
    fn i32_field(out: &mut Vec<u8>, id: i16, value: i32, last: &mut i16) {
        field_header(out, 5, id, last); // compact I32 = 5
        write_varint(out, zigzag_i32(value));
    }
    fn i64_field(out: &mut Vec<u8>, id: i16, value: i64, last: &mut i16) {
        field_header(out, 6, id, last); // compact I64 = 6
        write_varint(out, zigzag_i64(value));
    }
    fn bool_field(out: &mut Vec<u8>, id: i16, value: bool, last: &mut i16) {
        // compact bool is encoded directly in the field header: TRUE=1, FALSE=2.
        field_header(out, if value { 1 } else { 2 }, id, last);
    }
    fn key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        let mut last = 0;
        string_field(out, 1, key, &mut last);
        i32_field(out, 2, 0, &mut last); // value_type = STRING
        string_field(out, 3, value, &mut last);
        out.push(0);
    }
    fn key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        let mut last = 0;
        string_field(out, 1, key, &mut last);
        i32_field(out, 2, 3, &mut last); // value_type = BOOL
        bool_field(out, 5, value, &mut last);
        out.push(0);
    }

    let mut out = Vec::new();
    // Batch.process (struct, field 1).
    field_header(&mut out, 12, 1, &mut 0);
    {
        let mut last = 0;
        string_field(&mut out, 1, "compact-svc", &mut last); // Process.service_name
        out.push(0);
    }
    // Batch.spans (list<struct>, field 2).
    field_header(&mut out, 9, 2, &mut 1);
    list_header(&mut out, 12, 1);
    {
        let mut last = 0;
        i64_field(&mut out, 1, 4, &mut last); // trace_id_low
        i64_field(&mut out, 2, 3, &mut last); // trace_id_high
        i64_field(&mut out, 3, 9, &mut last); // span_id
        i64_field(&mut out, 4, 0, &mut last); // parent_span_id
        string_field(&mut out, 5, "compact thrift op", &mut last); // operation_name
        i64_field(&mut out, 8, 1_000, &mut last); // start_time (micros)
        i64_field(&mut out, 9, 25, &mut last); // duration (micros)
        field_header(&mut out, 9, 10, &mut last); // tags (list<struct>)
        list_header(&mut out, 12, 2);
        key_value_string(&mut out, "span.kind", "server");
        key_value_bool(&mut out, "error", true);
        out.push(0); // end span struct
    }
    out.push(0); // end batch struct
    out
}

// ---------------------------------------------------------------------------
// Multi-door ingest harness.
// ---------------------------------------------------------------------------

/// Build one `DistributorState` over a recording sink, drive EVERY ingest door
/// against it, and return the captured `SpanRecord`s.
///
/// All six doors write to the same `CapturingSink`, so this function reads the
/// snapshot only after every door has run.
async fn ingest_all_doors() -> TestResult<Vec<SpanRecord>> {
    let sink = CapturingSink::default();
    let state = Arc::new(DistributorState::new(Arc::new(sink.clone())));

    // D1 — OTLP HTTP `POST /v1/traces` (Trace A).
    let resp = distributor::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(trace_a_otlp_bytes()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;

    // D2 — Tempo push `POST /api/push` (Trace B, the PARTIAL trace).
    let resp = distributor::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/push")
                .header("content-type", "application/x-protobuf")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(trace_b_otlp_bytes()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::OK);
    let _ = resp.into_body().collect().await?;

    // D3 — Zipkin v2 `POST /api/v2/spans`.
    let zipkin = r#"[{"traceId":"33333333333333333333333333333333","id":"0000000000000033",
        "name":"zipkin op","timestamp":1000,"duration":2000,"kind":"SERVER",
        "localEndpoint":{"serviceName":"zipkin-svc"},
        "tags":{"http.method":"GET","error":"boom"}}]"#;
    let resp = distributor::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v2/spans")
                .header("content-type", "application/json")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(zipkin))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::ACCEPTED);
    let _ = resp.into_body().collect().await?;

    // D4 — Jaeger binary thrift `POST /api/traces`.
    let resp = distributor::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/traces")
                .header("content-type", "application/vnd.apache.thrift.binary")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(jaeger_binary_batch()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::ACCEPTED);
    let _ = resp.into_body().collect().await?;

    // D5 — OTLP gRPC, in-process via the production service struct.
    let otlp_grpc = OtlpGrpcService::new(state.clone());
    let mut req = GrpcRequest::new(ExportTraceServiceRequest {
        resource_spans: TracesData::decode(grpc_otlp_bytes().as_slice())?.resource_spans,
    });
    req.metadata_mut().insert("x-scope-orgid", TENANT.parse()?);
    otlp_grpc
        .export(req)
        .await
        .map_err(|status| format!("OTLP gRPC export failed: {status}"))?;

    // D6 — Jaeger gRPC, in-process via the production service struct.
    let jaeger_grpc = JaegerGrpcService::new(state.clone());
    let mut req = GrpcRequest::new(crabka_traces::wire::jaeger_grpc::api_v2::PostSpansRequest {
        batch: Some(crabka_traces::wire::jaeger_grpc::api_v2::Batch {
            process: Some(crabka_traces::wire::jaeger_grpc::api_v2::Process {
                service_name: "jaeger-grpc-svc".into(),
                tags: Vec::new(),
            }),
            spans: vec![crabka_traces::wire::jaeger_grpc::api_v2::Span {
                trace_id: vec![0x66; 16],
                span_id: vec![0x61; 8],
                operation_name: "jaeger grpc op".into(),
                start_time: Some(ProstTimestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                duration: Some(ProstDuration {
                    seconds: 0,
                    nanos: 5_000_000,
                }),
                tags: vec![
                    crabka_traces::wire::jaeger_grpc::api_v2::KeyValue {
                        key: "span.kind".into(),
                        v_type: crabka_traces::wire::jaeger_grpc::api_v2::ValueType::String.into(),
                        v_str: "server".into(),
                        ..Default::default()
                    },
                    crabka_traces::wire::jaeger_grpc::api_v2::KeyValue {
                        key: "error".into(),
                        v_type: crabka_traces::wire::jaeger_grpc::api_v2::ValueType::Bool.into(),
                        v_bool: true,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        }),
    });
    req.metadata_mut().insert("x-scope-orgid", TENANT.parse()?);
    jaeger_grpc
        .post_spans(req)
        .await
        .map_err(|status| format!("Jaeger gRPC post_spans failed: {status}"))?;

    // D7 — Jaeger compact thrift `POST /api/traces` (content-type
    // `application/x-thrift` selects the compact decoder, distinct from D4's
    // binary decoder). This is the same `decode_jaeger_thrift` path the compact
    // UDP datagram receiver uses.
    let resp = distributor::router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/traces")
                .header("content-type", "application/x-thrift")
                .header("x-scope-orgid", TENANT)
                .body(Body::from(jaeger_compact_batch()))?,
        )
        .await?;
    assert2::assert!(resp.status() == StatusCode::ACCEPTED);
    let _ = resp.into_body().collect().await?;

    let records = sink
        .records
        .lock()
        .map_err(|_| "capturing sink lock poisoned")?
        .clone();
    Ok(records)
}

/// Assert that every door's contribution landed in the captured records.
///
/// The assertions check door-specific decode fidelity, not merely
/// presence-by-name.
fn assert_all_doors_present(records: &[SpanRecord]) {
    let names: Vec<&str> = records.iter().map(|r| r.span.name.as_str()).collect();
    // D1 (OTLP HTTP, Trace A root), D2 (Tempo push, Trace B), D3 (Zipkin),
    // D4 (Jaeger binary), D5 (OTLP gRPC), D6 (Jaeger gRPC), D7 (Jaeger compact).
    for expected in [
        "GET /checkout",     // D1
        "chunk 0",           // D2
        "zipkin op",         // D3
        "GET /binary",       // D4
        "otlp grpc op",      // D5
        "jaeger grpc op",    // D6
        "compact thrift op", // D7
    ] {
        assert2::assert!(names.contains(&expected));
    }
    // All doors used the same tenant header.
    assert2::assert!(records.iter().all(|r| r.tenant == TENANT));

    let by_name = |name: &str| -> &SpanRecord {
        records
            .iter()
            .find(|r| r.span.name == name)
            .unwrap_or_else(|| panic!("record {name:?} present"))
    };

    // D3 (Zipkin): serviceName -> resource, error tag -> ERROR status,
    // µs -> ns conversion, trace-id pass-through.
    let zipkin = by_name("zipkin op");
    check!(
        zipkin.span.trace_id == [0x33; 16],
        "zipkin decode fidelity (trace-id pass-through)"
    );
    check!(
        zipkin.span.start_ns == 1_000_000,
        "zipkin decode fidelity (start µs -> ns conversion)"
    );
    check!(
        zipkin.span.duration_ns == 2_000_000,
        "zipkin decode fidelity (duration µs -> ns conversion)"
    );
    check!(
        zipkin.span.status.as_i32() == 2,
        "zipkin decode fidelity (error tag -> ERROR status)"
    );
    check!(
        resource_attr(&zipkin.span, "service.name") == Some("zipkin-svc"),
        "zipkin decode fidelity (serviceName -> resource)"
    );

    // Per-door decode fidelity: process/resource service.name mapping, plus
    // (where expected) error tag/bool -> ERROR status (`Some(2)`).
    let cases = [
        // D4 (Jaeger binary thrift): process.service_name -> resource
        // service.name, error tag -> ERROR status (binary decode path).
        ("GET /binary", "checkout", Some(2)),
        // D5 (OTLP gRPC): resource service.name from the independent gRPC OTLP
        // body (no error expectation).
        ("otlp grpc op", "grpc-otlp-svc", None),
        // D6 (Jaeger gRPC): process.service_name -> resource, error bool -> ERROR.
        ("jaeger grpc op", "jaeger-grpc-svc", Some(2)),
        // D7 (Jaeger compact thrift): the compact decode path (shared with the
        // UDP datagram receiver). process.service_name -> resource, error -> ERROR.
        ("compact thrift op", "compact-svc", Some(2)),
    ];
    for (name, service, error_status) in cases {
        let record = by_name(name);
        assert2::assert!(resource_attr(&record.span, "service.name") == Some(service));
        if let Some(expected) = error_status {
            assert2::assert!(record.span.status.as_i32() == expected);
        }
    }
}

// ---------------------------------------------------------------------------
// Querier-building helpers (copied + extended from tempo_differential.rs).
// ---------------------------------------------------------------------------

fn span_store_from_records(records: &[SpanRecord]) -> InMemorySpanStore {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }

    let mut store = InMemorySpanStore::new();
    for ((tenant, _), spans) in grouped {
        let root = spans
            .iter()
            .find(|span| span.parent_span_id.is_none())
            .unwrap_or(&spans[0]);
        let root_service = resource_attr(root, "service.name")
            .unwrap_or("unknown")
            .to_string();
        let root_name = root.name.clone();
        store.push_trace(
            &tenant,
            &root_service,
            &root_name,
            spans.into_iter().map(input_span).collect(),
        );
    }
    store
}

fn input_span(span: Span) -> InputSpan {
    let mut attrs = span.resource_attrs;
    attrs.extend(span.span_attrs);
    InputSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        kind: span.kind.as_i32(),
        start_unix_nano: span.start_ns,
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message,
        instrumentation_name: span.instrumentation_scope,
        instrumentation_version: span.instrumentation_version,
        attrs: attrs
            .into_iter()
            .filter_map(|attr| Some((attr.key, traceql_attr(attr.value)?)))
            .collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}

fn traceql_attr(value: AttrValue) -> Option<TraceqlAttrValue> {
    match value {
        AttrValue::Str(value) => Some(TraceqlAttrValue::Str(value)),
        AttrValue::Int(value) => Some(TraceqlAttrValue::Int(value)),
        AttrValue::Double(value) => Some(TraceqlAttrValue::Float(value)),
        AttrValue::Bool(value) => Some(TraceqlAttrValue::Bool(value)),
        AttrValue::Bytes(_) => None,
    }
}

fn resource_attr<'a>(span: &'a Span, key: &str) -> Option<&'a str> {
    span.resource_attrs
        .iter()
        .find_map(|attr| match &attr.value {
            AttrValue::Str(value) if attr.key == key => Some(value.as_str()),
            _ => None,
        })
}

/// Metrics-generator span record helper, the service-graph loop input.
fn metrics_span(
    service: &str,
    span_id: [u8; 8],
    parent: [u8; 8],
    kind: MetricsSpanKind,
    status: MetricsStatusCode,
    duration_ns: i64,
) -> MetricsSpanRecord {
    MetricsSpanRecord {
        tenant: TENANT.into(),
        trace_id: [0x11; 16],
        span_id,
        parent_span_id: parent,
        name: "op".into(),
        kind,
        start_ns: 0,
        duration_ns,
        status,
        status_message: String::new(),
        service_name: service.into(),
        attributes: vec![],
        size: ByteSize::from_bytes(0),
    }
}

// ---------------------------------------------------------------------------
// Crabka querier pair (bound on 0.0.0.0, reachable from containers).
// ---------------------------------------------------------------------------

struct CrabkaPair {
    /// Reachable from containers such as Grafana, through the host-gateway
    /// alias.
    container_base_url: String,
    /// Reachable from this test process directly, over loopback. It serves the
    /// protobuf trace-by-id content negotiation that Grafana never requests.
    local_base_url: String,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

impl CrabkaPair {
    fn shutdown(self) {
        let _ = self.shutdown.send(());
    }
}

async fn start_crabka_querier(records: &[SpanRecord]) -> TestResult<CrabkaPair> {
    let engine = Arc::new(TraceqlEngine::new(
        Arc::new(span_store_from_records(records)),
        EngineOpts::default(),
    ));
    let app = router_with_config(
        engine,
        HttpConfig {
            max_trace_spans: MAX_TRACE_SPANS,
            ..HttpConfig::default()
        },
    );
    let listener = tokio::net::TcpListener::bind("0.0.0.0:0").await?;
    let port = listener.local_addr()?.port();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .await;
    });

    // Grafana reaches the querier through the proxy (container_base_url); the
    // test process can also hit it directly on loopback (local_base_url).
    Ok(CrabkaPair {
        container_base_url: format!("http://{DOCKER_HOST_ALIAS}:{port}"),
        local_base_url: format!("http://127.0.0.1:{port}"),
        shutdown: tx,
    })
}

// ---------------------------------------------------------------------------
// Container launchers + HTTP helpers (copied from tempo_differential.rs).
// ---------------------------------------------------------------------------

async fn start_grafana() -> TestResult<ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_GRAFANA_IMAGE_TAG").unwrap_or_else(|_| "11.5.2".into());
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/grafana/grafana".to_string(), tag)
            .with_exposed_port(GRAFANA_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::seconds(5))
            .with_env_var("GF_SECURITY_ADMIN_PASSWORD", GRAFANA_ADMIN)
            .with_host(DOCKER_HOST_ALIAS, Host::HostGateway)
            .start(),
    )
    .await??)
}

async fn start_prometheus() -> TestResult<ContainerAsync<GenericImage>> {
    let tag = std::env::var("CRABKA_PROM_IMAGE_TAG").unwrap_or_else(|_| "v3.1.0".into());
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/prom/prometheus".to_string(), tag)
            .with_exposed_port(PROM_HTTP_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "Server is ready to receive web requests",
            ))
            .with_copy_to(
                CopyTargetOptions::new("/etc/prometheus/prometheus.yml").with_mode(0o644),
                CopyDataSource::Data(PROM_CONFIG.as_bytes().to_vec()),
            )
            .with_cmd([
                "--web.enable-remote-write-receiver",
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.retention.time=1h",
            ])
            .start(),
    )
    .await??)
}

async fn mapped_base_url(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> TestResult<String> {
    let mapped = container.get_host_port_ipv4(port).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}

async fn wait_for_http_ok(client: &reqwest::Client, base: &str, paths: &[&str]) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(90);
    while Instant::now() < deadline {
        for path in paths {
            if client
                .get(format!("{base}{path}"))
                .send()
                .await
                .is_ok_and(|resp| resp.status().is_success())
            {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(format!("timed out waiting for {base}").into())
}

async fn get_json(client: &reqwest::Client, url: &str) -> TestResult<JsonValue> {
    let resp = client
        .get(url)
        .basic_auth(GRAFANA_ADMIN, Some(GRAFANA_ADMIN))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.bytes().await?;
    assert2::assert!(status == ReqwestStatusCode::OK);
    Ok(serde_json::from_slice(&body)?)
}

async fn get_text(client: &reqwest::Client, url: &str) -> TestResult<(ReqwestStatusCode, String)> {
    let resp = client
        .get(url)
        .basic_auth(GRAFANA_ADMIN, Some(GRAFANA_ADMIN))
        .send()
        .await?;
    let status = resp.status();
    let body = resp.text().await?;
    Ok((status, body))
}

async fn get_json_until_positive_metric_total(
    client: &reqwest::Client,
    url: &str,
) -> TestResult<JsonValue> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url).await?;
        if metric_points_total(&json) > 0.0 {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for positive metric total from {url}: {last}").into())
}

async fn get_json_until_prom_result_non_empty(
    client: &reqwest::Client,
    url: &str,
) -> TestResult<JsonValue> {
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut last = JsonValue::Null;
    while Instant::now() < deadline {
        let json = get_json(client, url).await?;
        if json["data"]["result"]
            .as_array()
            .is_some_and(|result| !result.is_empty())
        {
            return Ok(json);
        }
        last = json;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("timed out waiting for non-empty Prometheus result from {url}: {last}").into())
}

fn metric_points_total(value: &JsonValue) -> f64 {
    let points_total: f64 = value["series"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|series| series["points"].as_array().into_iter().flatten())
        .filter_map(|point| point.as_array().and_then(|items| items.get(1)))
        .filter_map(JsonValue::as_f64)
        .sum();
    let samples_total: f64 = value["series"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|series| series["samples"].as_array().into_iter().flatten())
        .filter_map(|sample| sample["value"].as_f64())
        .sum();
    points_total + samples_total
}

#[test]
fn metric_points_total_sums_tempo_samples() {
    let metrics = json!({
        "series": [
            {
                "samples": [
                    {"timestampMs": "1782676277000", "value": 0.133_333_333_333_333_33},
                    {"timestampMs": "1782676307000", "value": 0.0}
                ]
            }
        ]
    });

    assert2::assert!(metric_points_total(&metrics) > 0.0);
}

/// Percent-encode a `TraceQL` query so it survives the Grafana proxy query string.
fn enc(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn search_contains_span_id_hex(search: &JsonValue, span_id_hex: &str) -> bool {
    search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|trace| trace["spanSets"].as_array().into_iter().flatten())
        .flat_map(|span_set| span_set["spans"].as_array().into_iter().flatten())
        .any(|span| span["spanID"].as_str() == Some(span_id_hex))
}

// ---------------------------------------------------------------------------
// THE test.
// ---------------------------------------------------------------------------

struct QueryWindow {
    now_secs: u64,
    metric_start: u64,
    metric_end: u64,
    range: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (Grafana + Prometheus containers)"]
async fn grafana_e2e_full_surface() -> TestResult {
    let client = reqwest::Client::new();

    // ----- §2/§3: drive every ingest door, then stand up the querier. -----
    let records = ingest_all_doors().await?;
    assert_all_doors_present(&records);
    let crabka = start_crabka_querier(&records).await?;

    // Wide query window covering the (now - 60s) fixture timestamps.
    let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let metric_start = now_secs.saturating_sub(300);
    let metric_end = now_secs + 60;
    let range = format!("start=0&end={}", now_secs + 60);

    // ----- §4: Grafana + Tempo datasource + every Tempo endpoint. -----
    let grafana = start_grafana().await?;
    let grafana_base = mapped_base_url(&grafana, GRAFANA_HTTP_PORT).await?;
    wait_for_http_ok(&client, &grafana_base, &["/api/health"]).await?;

    let tempo_ds = json!({
        "name": "Crabka Traces",
        "uid": GRAFANA_TEMPO_DATASOURCE_UID,
        "type": "tempo",
        "access": "proxy",
        "url": crabka.container_base_url,
        "isDefault": true,
        "jsonData": { "httpMethod": "GET", "httpHeaderName1": "X-Scope-OrgID" },
        "secureJsonData": { "httpHeaderValue1": TENANT },
    });
    client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth(GRAFANA_ADMIN, Some(GRAFANA_ADMIN))
        .json(&tempo_ds)
        .send()
        .await?
        .error_for_status()?;
    let fetched = get_json(
        &client,
        &format!("{grafana_base}/api/datasources/uid/{GRAFANA_TEMPO_DATASOURCE_UID}"),
    )
    .await?;
    assert2::assert!(fetched["type"].as_str() == Some("tempo"));
    assert2::assert!(fetched["url"].as_str() == Some(crabka.container_base_url.as_str()));

    let proxy = |path: &str| {
        format!("{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/{path}")
    };

    // E1 — /api/echo; E2 — /ready; E3 — /status (alias of /ready).
    let cases = [
        ("api/echo", "echo"), // E1
        ("ready", "ready"),   // E2
        ("status", "ready"),  // E3
    ];
    for (path, expected_body) in cases {
        let (status, body) = get_text(&client, &proxy(path)).await?;
        assert2::assert!((status, body.as_str()) == (ReqwestStatusCode::OK, expected_body));
    }

    // E4 — trace-by-id, Trace A: COMPLETE, all 4 spans, root span present. The
    // in-process store groups a trace under its single root service, so the
    // querier returns one resourceSpan (the root service) carrying every span;
    // each span keeps its own service.name as a span attribute.
    let trace_a = get_json(
        &client,
        &proxy(&format!("api/v2/traces/{TRACE_A_HEX}?{range}")),
    )
    .await?;
    check!(
        trace_a["status"].as_str() == Some("COMPLETE"),
        "expected a COMPLETE trace: {trace_a}"
    );
    check!(
        trace_a["message"].as_str() == Some(""),
        "expected an empty message on the COMPLETE trace: {trace_a}"
    );
    check!(
        trace_a["trace"]["resourceSpans"].as_array().map(Vec::len) == Some(1),
        "expected one resourceSpan (root service): {trace_a}"
    );
    let trace_a_spans = trace_a["trace"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|rs| rs["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|ss| ss["spans"].as_array().into_iter().flatten())
        .count();
    assert2::assert!(trace_a_spans == 4);
    let root_b64 = b64(&ROOT_SPAN_ID);
    let has_root = trace_a["trace"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|rs| rs["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|ss| ss["spans"].as_array().into_iter().flatten())
        .any(|span| span["spanId"].as_str() == Some(root_b64.as_str()));
    assert2::assert!(has_root);

    // E5 — trace-by-id, Trace B: PARTIAL, truncated to MAX_TRACE_SPANS.
    let trace_b = get_json(
        &client,
        &proxy(&format!("api/v2/traces/{TRACE_B_HEX}?{range}")),
    )
    .await?;
    assert2::assert!(trace_b["status"].as_str() == Some("PARTIAL"));
    assert2::assert!(trace_b["message"].as_str() == Some("trace truncated after 4 spans"));
    let returned_spans: usize = trace_b["trace"]["resourceSpans"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|rs| rs["scopeSpans"].as_array().into_iter().flatten())
        .flat_map(|ss| ss["spans"].as_array().into_iter().flatten())
        .count();
    assert2::assert!(returned_spans == MAX_TRACE_SPANS);

    // E4b — trace-by-id PROTOBUF, the format Grafana's Tempo *backend* uses for
    // the trace-view: GET /api/v2/traces/{id} with Accept: application/protobuf
    // returns a Tempo `TraceByIDResponse` wrapping the OTLP trace. Verified
    // directly against the querier (this exact path is what makes the Grafana
    // waterfall render; a raw OTLP body trips "failed to convert ... unexpected
    // EOF" in the plugin).
    let pb = client
        .get(format!(
            "{}/api/v2/traces/{TRACE_A_HEX}?{range}",
            crabka.local_base_url
        ))
        .header("accept", "application/protobuf")
        .header("x-scope-orgid", TENANT)
        .send()
        .await?;
    assert2::assert!(pb.status() == ReqwestStatusCode::OK);
    assert2::assert!(
        pb.headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            == Some("application/protobuf")
    );
    let decoded = TraceByIdResponse::decode(pb.bytes().await?)?
        .trace
        .expect("TraceByIDResponse carries the trace");
    assert2::assert!(decoded.resource_spans.len() == 1);
    let pb_spans: usize = decoded
        .resource_spans
        .iter()
        .flat_map(|rs| rs.scope_spans.iter())
        .map(|ss| ss.spans.len())
        .sum();
    assert2::assert!(pb_spans == 4);

    // E6 — bad-length trace id -> 400.
    let (status, body) = get_text(&client, &proxy("api/v2/traces/abcd")).await?;
    assert2::assert!(status == ReqwestStatusCode::BAD_REQUEST);
    assert2::assert!(body.contains("trace id must be 32 hex chars"));

    // E7 — absent trace -> 404.
    let (status, body) = get_text(
        &client,
        &proxy(&format!(
            "api/v2/traces/00000000000000000000000000000000?{range}"
        )),
    )
    .await?;
    assert2::assert!(status == ReqwestStatusCode::NOT_FOUND);
    assert2::assert!(body.contains("trace not found"));

    grafana_e2e_search(
        client,
        crabka,
        grafana,
        grafana_base,
        QueryWindow {
            now_secs,
            metric_start,
            metric_end,
            range,
        },
    )
    .await
}

async fn grafana_e2e_search(
    client: reqwest::Client,
    crabka: CrabkaPair,
    grafana: ContainerAsync<GenericImage>,
    grafana_base: String,
    window: QueryWindow,
) -> TestResult {
    let QueryWindow {
        now_secs,
        metric_start,
        metric_end,
        range,
    } = window;
    let proxy = |path: &str| {
        format!("{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/{path}")
    };

    // E8 — TraceQL selector search.
    let q = enc("{ resource.service.name = \"checkout-frontend\" }");
    let search = get_json(&client, &proxy(&format!("api/search?q={q}&{range}"))).await?;
    assert2::assert!(search["traces"][0]["traceID"].as_str() == Some(TRACE_A_HEX));
    assert2::assert!(
        search["metrics"]["inspectedSpans"]
            .as_u64()
            .is_some_and(|n| n > 0)
    );

    // E9 — legacy logfmt `tags=` search (no `q`).
    let search = get_json(
        &client,
        &proxy(&format!("api/search?tags=http.method%3DGET&{range}")),
    )
    .await?;
    let found_a = search["traces"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|trace| trace["traceID"].as_str() == Some(TRACE_A_HEX));
    assert2::assert!(found_a);

    // E10 — duration-filtered search. Trace A (~33ms) is kept by [25ms,40ms].
    let q = enc("{ resource.service.name = \"checkout-frontend\" }");
    let search = get_json(
        &client,
        &proxy(&format!(
            "api/search?q={q}&minDuration=25ms&maxDuration=40ms&{range}"
        )),
    )
    .await?;
    assert2::assert!(
        search["traces"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|trace| trace["traceID"].as_str() == Some(TRACE_A_HEX))
    );
    // The same selector with a tight max excludes Trace A's ~33ms trace.
    let search = get_json(
        &client,
        &proxy(&format!("api/search?q={q}&maxDuration=5ms&{range}")),
    )
    .await?;
    assert2::assert!(search["traces"].as_array().is_some_and(Vec::is_empty));

    // E11 — limit/spss caps.
    let q = enc("{ resource.service.name = \"checkout-frontend\" }");
    let search = get_json(
        &client,
        &proxy(&format!("api/search?q={q}&{range}&limit=1&spss=1")),
    )
    .await?;
    let traces = search["traces"].as_array().expect("traces array");
    assert2::assert!(traces.len() <= 1);
    for trace in traces {
        for span_set in trace["spanSets"].as_array().into_iter().flatten() {
            assert2::assert!(
                span_set["spans"]
                    .as_array()
                    .is_some_and(|spans| spans.len() <= 1)
            );
        }
    }

    // E12 — error-status search resolves the error span.
    let q = enc("{ span:status = error }");
    let search = get_json(&client, &proxy(&format!("api/search?q={q}&{range}"))).await?;
    assert2::assert!(
        search["traces"]
            .as_array()
            .is_some_and(|traces| !traces.is_empty())
    );
    assert2::assert!(search_contains_span_id_hex(&search, ERROR_SPAN_ID_HEX));

    grafana_e2e_tags(
        client,
        crabka,
        grafana,
        grafana_base,
        QueryWindow {
            now_secs,
            metric_start,
            metric_end,
            range,
        },
    )
    .await
}

async fn grafana_e2e_tags(
    client: reqwest::Client,
    crabka: CrabkaPair,
    grafana: ContainerAsync<GenericImage>,
    grafana_base: String,
    window: QueryWindow,
) -> TestResult {
    let QueryWindow {
        now_secs,
        metric_start,
        metric_end,
        range,
    } = window;
    let proxy = |path: &str| {
        format!("{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/{path}")
    };

    // E13 — v2 tags: scopes present (resource w/ service.name; intrinsic, link/event/instrumentation).
    let tags = get_json(&client, &proxy(&format!("api/v2/search/tags?{range}"))).await?;
    let scope_names: Vec<&str> = tags["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|scope| scope["name"].as_str())
        .collect();
    for required in ["resource", "intrinsic", "link", "event", "instrumentation"] {
        assert2::assert!(scope_names.contains(&required));
    }
    let cases = [
        ("resource", "service.name"),
        ("intrinsic", "span:name"),
        ("intrinsic", "span:duration"),
    ];
    for (scope, tag) in cases {
        assert2::assert!(scope_tags(&tags, scope).contains(&tag.to_string()));
    }

    // E14 — v2 tags scoped to resource only.
    let tags = get_json(
        &client,
        &proxy(&format!("api/v2/search/tags?scope=resource&{range}")),
    )
    .await?;
    let scope_names: Vec<&str> = tags["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|scope| scope["name"].as_str())
        .collect();
    assert2::assert!(scope_names == vec!["resource"]);
    assert2::assert!(scope_tags(&tags, "resource").contains(&"service.name".to_string()));

    // E15 — invalid scope -> 400.
    let (status, body) = get_text(&client, &proxy("api/v2/search/tags?scope=bogus")).await?;
    assert2::assert!(status == ReqwestStatusCode::BAD_REQUEST);
    assert2::assert!(body.contains("invalid scope"));

    // E16 — v2 typed values for resource.service.name.
    let values = get_json(
        &client,
        &proxy(&format!(
            "api/v2/search/tag/resource.service.name/values?{range}"
        )),
    )
    .await?;
    let typed: Vec<(&str, &str)> = values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| Some((v["type"].as_str()?, v["value"].as_str()?)))
        .collect();
    // resource.service.name values are the per-trace ROOT services (the store
    // groups each trace under one root); checkout-frontend (Trace A) and
    // bulk-svc (Trace B) are both roots. cart-backend is a non-root service, so
    // it appears as a span attribute, not a resource value.
    assert2::assert!(typed.contains(&("string", "checkout-frontend")));
    assert2::assert!(typed.contains(&("string", "bulk-svc")));

    // E17 — v2 typed values for the intrinsic span:duration (type "duration").
    let values = get_json(
        &client,
        &proxy(&format!("api/v2/search/tag/span:duration/values?{range}")),
    )
    .await?;
    let all_durations = values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .all(|v| v["type"].as_str() == Some("duration"));
    assert2::assert!(
        values["tagValues"]
            .as_array()
            .is_some_and(|vs| !vs.is_empty())
            && all_durations
    );

    // E18 — legacy v1 flat tags.
    let tags = get_json(&client, &proxy(&format!("api/search/tags?{range}"))).await?;
    let names: Vec<&str> = tags["tagNames"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect();
    assert2::assert!(names.contains(&"service.name"));
    assert2::assert!(tags["metrics"]["inspectedBytes"].as_str() == Some("0"));

    // E19 — legacy v1 flat values (plain strings).
    let values = get_json(
        &client,
        &proxy(&format!(
            "api/search/tag/resource.service.name/values?{range}"
        )),
    )
    .await?;
    let plain: Vec<&str> = values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect();
    assert2::assert!(plain.contains(&"checkout-frontend"));

    grafana_e2e_metrics(
        client,
        crabka,
        grafana,
        grafana_base,
        QueryWindow {
            now_secs,
            metric_start,
            metric_end,
            range,
        },
    )
    .await
}

async fn grafana_e2e_metrics(
    client: reqwest::Client,
    crabka: CrabkaPair,
    grafana: ContainerAsync<GenericImage>,
    grafana_base: String,
    window: QueryWindow,
) -> TestResult {
    let QueryWindow {
        now_secs,
        metric_start,
        metric_end,
        range,
    } = window;
    let proxy = |path: &str| {
        format!("{grafana_base}/api/datasources/proxy/uid/{GRAFANA_TEMPO_DATASOURCE_UID}/{path}")
    };

    // E20 — TraceQL metrics query_range (poll until ingested samples appear).
    let mq = enc("{ resource.service.name = \"checkout-frontend\" } | rate()");
    let metrics = get_json_until_positive_metric_total(
        &client,
        &proxy(&format!(
            "api/metrics/query_range?q={mq}&start={metric_start}&end={metric_end}&step=30s"
        )),
    )
    .await?;
    assert2::assert!(
        metrics["series"]
            .as_array()
            .is_some_and(|series| !series.is_empty())
    );
    assert2::assert!(metric_points_total(&metrics) > 0.0);

    // E21 — query_range with exemplars disabled: every series has empty exemplars.
    let metrics = get_json(
        &client,
        &proxy(&format!(
            "api/metrics/query_range?q={mq}&start={metric_start}&end={metric_end}&step=30s&exemplars=0"
        )),
    )
    .await?;
    assert2::assert!(
        metrics["series"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|series| series["exemplars"].as_array().is_some_and(Vec::is_empty))
    );

    // E22 — instant metrics via `time` (each series collapsed to one point).
    let iq = enc("{ resource.service.name = \"checkout-frontend\" } | count_over_time()");
    let metrics = get_json(
        &client,
        &proxy(&format!("api/metrics/query?q={iq}&time={now_secs}")),
    )
    .await?;
    assert2::assert!(
        metrics["series"]
            .as_array()
            .is_some_and(|series| !series.is_empty())
    );
    assert2::assert!(
        metrics["series"]
            .as_array()
            .into_iter()
            .flatten()
            .all(|series| series["samples"]
                .as_array()
                .is_some_and(|samples| samples.len() == 1))
    );

    // E23 — instant metrics via start/end bounds (single sample at `end`).
    let metrics = get_json(
        &client,
        &proxy(&format!(
            "api/metrics/query?q={iq}&start={metric_start}&end={now_secs}"
        )),
    )
    .await?;
    let expected_end_ms = (now_secs * 1_000).to_string();
    let single_sample_at_end = metrics["series"]
        .as_array()
        .into_iter()
        .flatten()
        .all(|series| {
            series["samples"].as_array().is_some_and(|samples| {
                samples.len() == 1
                    && samples[0]["timestampMs"].as_str() == Some(expected_end_ms.as_str())
            })
        });
    assert2::assert!(single_sample_at_end);

    // E24 — q-derived tag discovery (matching_traces -> scoped_tags_from_traces),
    // distinct from the global-index path in E13/E18.
    let derived_q = enc("{ resource.service.name = \"checkout-frontend\" }");
    let tags = get_json(
        &client,
        &proxy(&format!(
            "api/search/tags?q={derived_q}&scope=resource&{range}"
        )),
    )
    .await?;
    let derived_names: Vec<&str> = tags["tagNames"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect();
    assert2::assert!(derived_names.contains(&"service.name"));

    // E25 — q-derived tag values (matching_traces -> tag_values_from_traces).
    let values = get_json(
        &client,
        &proxy(&format!(
            "api/search/tag/resource.service.name/values?q={derived_q}&{range}"
        )),
    )
    .await?;
    let derived_values: Vec<&str> = values["tagValues"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(JsonValue::as_str)
        .collect();
    assert2::assert!(derived_values.contains(&"checkout-frontend"));

    // E26 — metrics query_range defaults `step` when omitted.
    let metrics = get_json(
        &client,
        &proxy(&format!(
            "api/metrics/query_range?q={mq}&start={metric_start}&end={metric_end}"
        )),
    )
    .await?;
    assert2::assert!(
        metrics["series"]
            .as_array()
            .is_some_and(|series| !series.is_empty())
    );
    assert2::assert!(metric_points_total(&metrics) > 0.0);

    // E27-E28 — Grafana's Tempo datasource normalizes some malformed metric
    // requests before proxying them, so validate these Crabka API contracts
    // directly.
    let direct_metrics_url =
        |params: &str| format!("{}/api/metrics/query_range?{params}", crabka.local_base_url);

    // E27 — metrics query_range rejects a non-positive `step` -> 400.
    let (status, body) = get_text(
        &client,
        &direct_metrics_url(&format!(
            "q={mq}&start={metric_start}&end={metric_end}&step=0"
        )),
    )
    .await?;
    assert2::assert!(status == ReqwestStatusCode::BAD_REQUEST);
    assert2::assert!(body.contains("step must be positive"));

    // E28 — metrics query_range rejects end < start -> 400.
    let (status, body) = get_text(
        &client,
        &direct_metrics_url(&format!(
            "q={mq}&start={metric_end}&end={metric_start}&step=30s"
        )),
    )
    .await?;
    assert2::assert!(status == ReqwestStatusCode::BAD_REQUEST);
    assert2::assert!(body.contains("end must be >= start"));

    grafana_e2e_service_graph(client, crabka, grafana, grafana_base).await
}

async fn grafana_e2e_service_graph(
    client: reqwest::Client,
    crabka: CrabkaPair,
    grafana: ContainerAsync<GenericImage>,
    grafana_base: String,
) -> TestResult {
    // ----- §5: Service Graph full loop through real Prometheus. -----
    let prom = start_prometheus().await?;
    let prom_mapped = mapped_base_url(&prom, PROM_HTTP_PORT).await?;
    wait_for_http_ok(&client, &prom_mapped, &["/-/ready"]).await?;
    let prom_port = prom.get_host_port_ipv4(PROM_HTTP_PORT).await?;
    let rw_url = format!("{prom_mapped}/api/v1/write");

    // Drive the real metrics-generator chain: EdgeStore -> SeriesPayload ->
    // PrometheusRemoteWriteSink (snappy/protobuf PRW v1) -> real Prometheus.
    // Only the WAL consumer is mocked (MockSpanSource).
    let source = Arc::new(MockSpanSource::default());
    source.push_batch(vec![
        metrics_span(
            "checkout-frontend",
            [0x0A; 8],
            [0; 8],
            MetricsSpanKind::Client,
            MetricsStatusCode::Ok,
            20_000_000,
        ),
        metrics_span(
            "cart-backend",
            [0x0B; 8],
            [0x0A; 8],
            MetricsSpanKind::Server,
            MetricsStatusCode::Ok,
            15_000_000,
        ),
    ]);
    let sink = Arc::new(PrometheusRemoteWriteSink::new(rw_url));
    let svc = MetricsGenService::new(
        MetricsGenConfig::default(),
        Arc::new(SystemClock),
        source,
        sink,
    );
    assert2::assert!(svc.poll_once(usize::MAX).await? == 2);
    assert2::assert!(svc.collect_once().await? == 1);

    // Provision the Grafana Prometheus datasource (the Service-Graph backend).
    let prom_ds = json!({
        "name": "Crabka Service Graph",
        "uid": GRAFANA_PROM_DATASOURCE_UID,
        "type": "prometheus",
        "access": "proxy",
        "url": format!("http://{DOCKER_HOST_ALIAS}:{prom_port}"),
        "isDefault": false,
    });
    client
        .post(format!("{grafana_base}/api/datasources"))
        .basic_auth(GRAFANA_ADMIN, Some(GRAFANA_ADMIN))
        .json(&prom_ds)
        .send()
        .await?
        .error_for_status()?;
    let fetched = get_json(
        &client,
        &format!("{grafana_base}/api/datasources/uid/{GRAFANA_PROM_DATASOURCE_UID}"),
    )
    .await?;
    assert2::assert!(fetched["type"].as_str() == Some("prometheus"));

    let prom_proxy = |query: &str| {
        format!(
            "{grafana_base}/api/datasources/proxy/uid/{GRAFANA_PROM_DATASOURCE_UID}/api/v1/query?query={}",
            enc(query)
        )
    };

    // Close the loop: query the request-total edge through the Grafana proxy.
    let result = get_json_until_prom_result_non_empty(
        &client,
        &prom_proxy("traces_service_graph_request_total"),
    )
    .await?;
    let edge = &result["data"]["result"][0];
    check!(
        edge["value"][1].as_str() == Some("1"),
        "service-graph request_total edge total: {result}"
    );
    check!(
        edge["metric"]["client"].as_str() == Some("checkout-frontend"),
        "service-graph request_total edge client label: {result}"
    );
    check!(
        edge["metric"]["server"].as_str() == Some("cart-backend"),
        "service-graph request_total edge server label: {result}"
    );

    // The server-side latency histogram fan-out also reached Prometheus.
    let result = get_json_until_prom_result_non_empty(
        &client,
        &prom_proxy("traces_service_graph_request_server_seconds_count"),
    )
    .await?;
    assert2::assert!(result["data"]["result"][0]["value"][1].as_str() == Some("1"));

    crabka.shutdown();
    // Keep `grafana` and `prom` bindings alive until here; they stop on drop.
    drop(grafana);
    drop(prom);
    Ok(())
}

/// In-process sanity check for the ingest half. It runs WITHOUT Docker.
///
/// The test drives every distributor door and asserts each one's decode
/// fidelity. That validates the hand-rolled thrift, Zipkin and OTLP fixtures,
/// and the per-door decode mappings.
///
/// The Grafana/Prometheus query half and the service-graph half still need
/// containers. They live in the ignored `grafana_e2e_full_surface` above.
#[tokio::test]
async fn ingest_all_doors_decode_correctly() -> TestResult {
    let records = ingest_all_doors().await?;
    assert_all_doors_present(&records);
    Ok(())
}

/// Collect the tag list for a named scope from a v2 tags response.
fn scope_tags(tags: &JsonValue, scope_name: &str) -> Vec<String> {
    tags["scopes"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|scope| scope["name"].as_str() == Some(scope_name))
        .and_then(|scope| scope["tags"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|tag| tag.as_str().map(str::to_string))
        .collect()
}
