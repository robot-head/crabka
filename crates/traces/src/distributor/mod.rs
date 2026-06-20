//! Distributor role: push-door HTTP routes into the traces WAL.

use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use crabka_client_producer::{Producer, ProducerRecord};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message as _;
use tokio_util::sync::CancellationToken;

use crate::error::TracesError;
use crate::span::{AttrValue, KeyValue, Span};
use crate::wal::{SpanRecord, TRACES_WAL_TOPIC, partition_key};
use crate::wire::jaeger::decode_jaeger_thrift;
use crate::wire::otlp::decode_otlp;
use crate::wire::zipkin::decode_zipkin;

const TENANT_HEADER: &str = "x-scope-orgid";
const CONTENT_ENCODING: &str = "content-encoding";

/// Per-tenant distributor limits enforced before WAL append.
#[derive(Clone, Debug)]
pub struct TenantLimits {
    pub max_spans_per_request: usize,
    pub max_attr_value_len: usize,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_spans_per_request: 10_000,
            max_attr_value_len: 64 * 1024,
        }
    }
}

/// Append one already-encoded logical span record to the traces WAL.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError>;
}

/// Kafka-backed WAL sink.
pub struct KafkaSink {
    producer: Arc<Producer>,
}

impl KafkaSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>) -> Self {
        Self { producer }
    }
}

#[async_trait::async_trait]
impl WalSink for KafkaSink {
    async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
        let key = partition_key(&rec.span.trace_id);
        let value = Bytes::from(rec.encode()?);
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: TRACES_WAL_TOPIC.to_string(),
                key: Some(key),
                value: Some(value),
                ..ProducerRecord::default()
            })
            .await;
        ack.await
            .map_err(|err| TracesError::Produce(err.to_string()))?
            .map_err(|err| TracesError::Produce(err.to_string()))?;
        Ok(())
    }
}

/// Shared distributor state.
pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    pub limits: TenantLimits,
    pub max_decompressed: usize,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self {
            sink,
            limits: TenantLimits::default(),
            max_decompressed: 10 * 1024 * 1024,
        }
    }
}

/// Build the distributor HTTP router.
pub fn router(state: Arc<DistributorState>) -> Router {
    Router::new()
        .route("/v1/traces", post(otlp_push))
        .route("/api/push", post(otlp_push))
        .route("/api/v2/spans", post(zipkin_push))
        .route("/api/traces", post(jaeger_push))
        .with_state(state)
}

/// Serve the distributor until cancelled.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = router(state);
    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });
        if let Err(err) = server.await {
            tracing::warn!(error = %err, "traces distributor server error");
        }
    });
    Ok(bound)
}

async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match decode_body(&headers, &body, state.max_decompressed)
        .and_then(|body| {
            TracesData::decode(body.as_slice()).map_err(|err| TracesError::Decode(err.to_string()))
        })
        .and_then(|data| decode_otlp(&data))
    {
        Ok(spans) => append_decoded(&state, &headers, spans, StatusCode::OK).await,
        Err(err) => error_response(&err),
    }
}

async fn zipkin_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match decode_body(&headers, &body, state.max_decompressed).and_then(|body| decode_zipkin(&body))
    {
        Ok(spans) => append_decoded(&state, &headers, spans, StatusCode::ACCEPTED).await,
        Err(err) => error_response(&err),
    }
}

async fn jaeger_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    match decode_body(&headers, &body, state.max_decompressed)
        .and_then(|body| decode_jaeger_thrift(&body))
    {
        Ok(spans) => append_decoded(&state, &headers, spans, StatusCode::ACCEPTED).await,
        Err(err) => error_response(&err),
    }
}

async fn append_decoded(
    state: &DistributorState,
    headers: &HeaderMap,
    spans: Vec<Span>,
    success: StatusCode,
) -> Response {
    let tenant = tenant(headers);
    if let Err(err) = validate(&spans, &state.limits) {
        return error_response(&err);
    }
    match produce_spans(state.sink.as_ref(), &tenant, spans).await {
        Ok(()) => success.into_response(),
        Err(err) => error_response(&err),
    }
}

fn decode_body(
    headers: &HeaderMap,
    body: &[u8],
    max_decompressed: usize,
) -> Result<Vec<u8>, TracesError> {
    let encoding = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("identity");
    let decoded = match encoding {
        "identity" => body.to_vec(),
        "gzip" => {
            let mut out = Vec::new();
            GzDecoder::new(body)
                .take(u64::try_from(max_decompressed).unwrap_or(u64::MAX) + 1)
                .read_to_end(&mut out)
                .map_err(|err| TracesError::Decode(err.to_string()))?;
            out
        }
        other => return Err(TracesError::UnsupportedContentType(other.to_string())),
    };
    if decoded.len() > max_decompressed {
        return Err(TracesError::TooLarge {
            limit: max_decompressed,
        });
    }
    Ok(decoded)
}

fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

/// Validate decoded spans against per-tenant structural limits.
pub fn validate(spans: &[Span], limits: &TenantLimits) -> Result<(), TracesError> {
    if spans.len() > limits.max_spans_per_request {
        return Err(TracesError::Invalid(format!(
            "span count {} exceeds limit {}",
            spans.len(),
            limits.max_spans_per_request
        )));
    }
    for span in spans {
        validate_attrs(&span.resource_attrs, limits)?;
        validate_attrs(&span.span_attrs, limits)?;
        for event in &span.events {
            validate_attrs(&event.attrs, limits)?;
        }
        for link in &span.links {
            validate_attrs(&link.attrs, limits)?;
        }
    }
    Ok(())
}

fn validate_attrs(attrs: &[KeyValue], limits: &TenantLimits) -> Result<(), TracesError> {
    for attr in attrs {
        let len = match &attr.value {
            AttrValue::Str(value) => value.len(),
            AttrValue::Bytes(value) => value.len(),
            AttrValue::Int(_) | AttrValue::Double(_) | AttrValue::Bool(_) => 0,
        };
        if len > limits.max_attr_value_len {
            return Err(TracesError::Invalid(format!(
                "attribute `{}` exceeds limit {}",
                attr.key, limits.max_attr_value_len
            )));
        }
    }
    Ok(())
}

/// Append decoded spans to the WAL sink.
pub async fn produce_spans(
    sink: &dyn WalSink,
    tenant: &str,
    spans: Vec<Span>,
) -> Result<(), TracesError> {
    for span in spans {
        sink.append(SpanRecord {
            tenant: tenant.to_string(),
            span,
        })
        .await?;
    }
    Ok(())
}

fn error_response(err: &TracesError) -> Response {
    (
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        err.to_string(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::assert;
    use axum::body::Body;
    use axum::http::Request;
    use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData,
    };
    use prost::Message as _;
    use tower::ServiceExt as _;

    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        records: Mutex<Vec<SpanRecord>>,
    }

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
            self.records.lock().unwrap().push(rec);
            Ok(())
        }
    }

    impl RecordingSink {
        fn count(&self) -> usize {
            self.records.lock().unwrap().len()
        }

        fn tenant(&self, idx: usize) -> String {
            self.records.lock().unwrap()[idx].tenant.clone()
        }

        fn span_name(&self, idx: usize) -> String {
            self.records.lock().unwrap()[idx].span.name.clone()
        }
    }

    fn test_state() -> (Arc<DistributorState>, Arc<RecordingSink>) {
        test_state_with_limits(TenantLimits::default())
    }

    fn test_state_with_limits(limits: TenantLimits) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        (
            Arc::new(DistributorState {
                sink: sink.clone(),
                limits,
                max_decompressed: 1024 * 1024,
            }),
            sink,
        )
    }

    fn otlp_body() -> Vec<u8> {
        TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /".into(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_500,
                        ..OtlpSpan::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn otlp_push_returns_200_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        assert!(sink.count() == 1);
        assert!(sink.tenant(0) == "tenant-a");
    }

    #[tokio::test]
    async fn tempo_push_uses_anonymous_tenant_when_header_absent() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/push")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
        assert!(sink.tenant(0) == "anonymous");
    }

    #[tokio::test]
    async fn zipkin_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let body = r#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"x","timestamp":1,"duration":1}]"#;
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v2/spans")
                    .header("content-type", "application/json")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::ACCEPTED);
        assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn jaeger_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/traces")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(
                        crate::wire::jaeger::test_support::encode_sample_batch(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::ACCEPTED);
        assert!(sink.count() == 1);
        assert!(sink.span_name(0) == "GET /");
    }

    #[tokio::test]
    async fn over_span_limit_is_400() {
        let limits = TenantLimits {
            max_spans_per_request: 0,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::BAD_REQUEST);
        assert!(sink.count() == 0);
    }

    #[test]
    fn validate_rejects_large_attribute_values() {
        let limits = TenantLimits {
            max_attr_value_len: 2,
            ..TenantLimits::default()
        };
        let span = Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "x".into(),
            kind: crate::span::SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: crate::span::StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };
        assert!(validate(&[span], &limits).is_err());
    }
}
