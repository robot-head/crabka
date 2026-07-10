//! Distributor role: push-door HTTP routes into the traces WAL.

use std::{collections::BTreeMap, io::Read, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use crabka_client_producer::{Header, Producer, ProducerRecord};
use flate2::read::GzDecoder;
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
    trace::v1::TracesData,
};
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::{
    Request as GrpcRequest, Response as GrpcResponse, Status as GrpcStatus, metadata::MetadataMap,
    transport::Server as GrpcServer,
};
use tracing::Instrument as _;

use crate::{
    error::{TracesError, tempo_error_response},
    limits::{IngestEnforcer, LimitError, Limits, OverridesProvider},
    metrics::ServiceMetrics,
    span::{AttrValue, KeyValue, Span},
    wal::{SpanRecord, TRACES_WAL_TOPIC, partition_key},
    wire::{
        jaeger::{decode_jaeger_binary_thrift, decode_jaeger_thrift},
        jaeger_grpc::{
            api_v2::{
                PostSpansRequest, PostSpansResponse,
                collector_service_server::{CollectorService, CollectorServiceServer},
            },
            decode_jaeger_grpc_batch,
        },
        otlp::decode_otlp,
        zipkin::decode_zipkin,
    },
};

const TENANT_HEADER: &str = "x-scope-orgid";
const CONTENT_ENCODING: &str = "content-encoding";

/// Per-tenant distributor limits enforced before WAL append.
#[derive(Clone, Debug)]
pub struct TenantLimits {
    pub max_spans_per_request: usize,
    pub max_spans_per_trace: usize,
    pub max_ingest_spans_per_second: usize,
    pub ingest_rate_burst: usize,
    pub max_attr_value_len: usize,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_spans_per_request: 10_000,
            max_spans_per_trace: usize::MAX,
            max_ingest_spans_per_second: usize::MAX,
            ingest_rate_burst: usize::MAX,
            max_attr_value_len: 64 * 1024,
        }
    }
}

impl TenantLimits {
    #[must_use]
    pub fn to_shared_limits(&self) -> Limits {
        Limits {
            ingestion_rate_spans_per_sec: f64_limit_from_usize(self.max_ingest_spans_per_second),
            ingestion_burst_spans: u64_limit_from_usize(self.ingest_rate_burst),
            max_traces_per_search: Limits::default().max_traces_per_search,
            max_spans_per_trace: u64_limit_from_usize(self.max_spans_per_trace),
            max_attribute_bytes: u64_limit_from_usize(self.max_attr_value_len),
            max_search_duration_secs: Limits::default().max_search_duration_secs,
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
        // Inject the current ingest span's W3C trace context onto the WAL record
        // so the block-builder (WAL consumer) can continue the same distributed
        // trace. Empty when there is no active/sampled span, so this is additive.
        let headers = crabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|(key, value)| Header {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            })
            .collect();
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: TRACES_WAL_TOPIC.to_string(),
                key: Some(key),
                value: Some(value),
                headers,
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
    pub shared_limits: Limits,
    pub overrides: Option<OverridesProvider>,
    pub ingest_enforcer: IngestEnforcer,
    pub max_decompressed: usize,
    pub metrics: ServiceMetrics,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self::with_metrics(sink, ServiceMetrics::new())
    }

    #[must_use]
    pub fn with_metrics(sink: Arc<dyn WalSink>, metrics: ServiceMetrics) -> Self {
        Self {
            sink,
            limits: TenantLimits::default(),
            shared_limits: TenantLimits::default().to_shared_limits(),
            overrides: None,
            ingest_enforcer: IngestEnforcer::new(),
            max_decompressed: 10 * 1024 * 1024,
            metrics,
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

/// Serve the OTLP/gRPC trace receiver until cancelled.
pub async fn serve_otlp_grpc(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    GrpcServer::builder()
        .add_service(TraceServiceServer::new(OtlpGrpcService::new(state)))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
        })
        .await
}

/// Serve the Jaeger API v2 gRPC trace receiver until cancelled.
pub async fn serve_jaeger_grpc(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    GrpcServer::builder()
        .add_service(CollectorServiceServer::new(JaegerGrpcService::new(state)))
        .serve_with_shutdown(addr, async move {
            shutdown.cancelled().await;
        })
        .await
}

/// Serve the Jaeger compact-Thrift UDP receiver until cancelled.
pub async fn serve_jaeger_compact_udp(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> std::io::Result<SocketAddr> {
    let socket = tokio::net::UdpSocket::bind(addr).await?;
    let bound = socket.local_addr()?;
    tokio::spawn(async move {
        let mut buf = vec![0_u8; 65_535];
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                received = socket.recv_from(&mut buf) => {
                    match received {
                        Ok((len, peer)) => {
                            if let Err(err) =
                                handle_jaeger_compact_datagram(&state, "anonymous", &buf[..len]).await
                            {
                                tracing::warn!(%peer, error = %err, "jaeger compact datagram rejected");
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "jaeger compact UDP receive error");
                        }
                    }
                }
            }
        }
    });
    Ok(bound)
}

/// OTLP/gRPC trace export service backed by the traces WAL.
pub struct OtlpGrpcService {
    state: Arc<DistributorState>,
}

impl OtlpGrpcService {
    #[must_use]
    pub fn new(state: Arc<DistributorState>) -> Self {
        Self { state }
    }
}

/// Jaeger API v2 gRPC collector backed by the traces WAL.
pub struct JaegerGrpcService {
    state: Arc<DistributorState>,
}

impl JaegerGrpcService {
    #[must_use]
    pub fn new(state: Arc<DistributorState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl CollectorService for JaegerGrpcService {
    async fn post_spans(
        &self,
        request: GrpcRequest<PostSpansRequest>,
    ) -> Result<GrpcResponse<PostSpansResponse>, GrpcStatus> {
        let metadata = request.metadata().clone();
        let batch = request
            .into_inner()
            .batch
            .ok_or_else(|| GrpcStatus::invalid_argument("missing jaeger batch"))?;
        let spans = decode_jaeger_grpc_batch(batch)
            .map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        let tenant = tenant_metadata(&metadata);
        self.state
            .enforce_ingest(&tenant, &spans)
            .map_err(|err| grpc_status_from_error(&err))?;
        produce_spans(self.state.sink.as_ref(), &tenant, spans)
            .await
            .map_err(|err| GrpcStatus::internal(err.to_string()))?;
        Ok(GrpcResponse::new(PostSpansResponse {}))
    }
}

#[async_trait::async_trait]
impl TraceService for OtlpGrpcService {
    async fn export(
        &self,
        request: GrpcRequest<ExportTraceServiceRequest>,
    ) -> Result<GrpcResponse<ExportTraceServiceResponse>, GrpcStatus> {
        let metadata = request.metadata().clone();
        let data = TracesData {
            resource_spans: request.into_inner().resource_spans,
        };
        let spans =
            decode_otlp(&data).map_err(|err| GrpcStatus::invalid_argument(err.to_string()))?;
        let tenant = tenant_metadata(&metadata);
        self.state
            .enforce_ingest(&tenant, &spans)
            .map_err(|err| grpc_status_from_error(&err))?;
        produce_spans(self.state.sink.as_ref(), &tenant, spans)
            .await
            .map_err(|err| GrpcStatus::internal(err.to_string()))?;
        Ok(GrpcResponse::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    // One ingest span per request (NOT per span-record). The accepted-span count
    // is only known after decode, so it is declared `Empty` and recorded below;
    // this span becomes the local parent whose context is injected onto each WAL
    // record in `KafkaSink::append`, continuing the trace into the block-builder.
    let span = tracing::info_span!(
        "traces_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = TRACES_WAL_TOPIC,
        crabka.tenant = %tenant(&headers),
        crabka.ingest.spans = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    async move {
        if let Err(err) = require_content_type(
            &headers,
            &["application/x-protobuf", "application/protobuf"],
        ) {
            return record_ingest_response(&state, error_response(&err), bytes, 0, start);
        }
        match decode_body(&headers, &body, state.max_decompressed)
            .and_then(|body| {
                TracesData::decode(body.as_slice())
                    .map_err(|err| TracesError::Decode(err.to_string()))
            })
            .and_then(|data| decode_otlp(&data))
        {
            Ok(spans) => {
                let items = spans.len() as u64;
                tracing::Span::current().record("crabka.ingest.spans", items);
                let resp =
                    append_decoded_response(&state, &headers, spans, otlp_success_response()).await;
                record_ingest_response(&state, resp, bytes, items, start)
            }
            Err(err) => record_ingest_response(&state, error_response(&err), bytes, 0, start),
        }
    }
    .instrument(span)
    .await
}

async fn zipkin_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    if let Err(err) = require_content_type(&headers, &["application/json"]) {
        return record_ingest_response(&state, error_response(&err), bytes, 0, start);
    }
    match decode_body(&headers, &body, state.max_decompressed).and_then(|body| decode_zipkin(&body))
    {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(&state, &headers, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(&state, resp, bytes, items, start)
        }
        Err(err) => record_ingest_response(&state, error_response(&err), bytes, 0, start),
    }
}

async fn jaeger_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    if let Err(err) = require_content_type(
        &headers,
        &[
            "application/x-thrift",
            "application/octet-stream",
            "application/vnd.apache.thrift.binary",
        ],
    ) {
        return record_ingest_response(&state, error_response(&err), bytes, 0, start);
    }
    match decode_body(&headers, &body, state.max_decompressed).and_then(|body| {
        if is_jaeger_binary_thrift(&headers) {
            decode_jaeger_binary_thrift(&body)
        } else {
            decode_jaeger_thrift(&body)
        }
    }) {
        Ok(spans) => {
            let items = spans.len() as u64;
            let resp = append_decoded(&state, &headers, spans, StatusCode::ACCEPTED).await;
            record_ingest_response(&state, resp, bytes, items, start)
        }
        Err(err) => record_ingest_response(&state, error_response(&err), bytes, 0, start),
    }
}

/// Record one push-handler ingest outcome from the response status and return
/// the response unchanged. `ok` is true for any 2xx; the WAL/produce failure
/// counter is bumped separately at the [`produce_spans`] error site, so a 4xx
/// validation/rate-limit reject here does not inflate it.
fn record_ingest_response(
    state: &DistributorState,
    resp: Response,
    bytes: u64,
    items: u64,
    start: std::time::Instant,
) -> Response {
    let ok = resp.status().is_success();
    state
        .metrics
        .record_ingest(ok, bytes, items, start.elapsed().as_secs_f64());
    resp
}

async fn handle_jaeger_compact_datagram(
    state: &DistributorState,
    tenant: &str,
    body: &[u8],
) -> Result<(), TracesError> {
    let spans = decode_jaeger_thrift(body)?;
    state.enforce_ingest(tenant, &spans)?;
    produce_spans(state.sink.as_ref(), tenant, spans).await
}

async fn append_decoded(
    state: &DistributorState,
    headers: &HeaderMap,
    spans: Vec<Span>,
    success: StatusCode,
) -> Response {
    append_decoded_response(state, headers, spans, success.into_response()).await
}

async fn append_decoded_response(
    state: &DistributorState,
    headers: &HeaderMap,
    spans: Vec<Span>,
    success: Response,
) -> Response {
    let tenant = tenant(headers);
    if let Err(err) = state.enforce_ingest(&tenant, &spans) {
        return error_response(&err);
    }
    let accepted = spans.len() as u64;
    match produce_spans(state.sink.as_ref(), &tenant, spans).await {
        Ok(()) => {
            // Attribute accepted spans to the tenant once per request (batch
            // size), not per span-record, keeping cardinality bounded.
            state.metrics.record_ingest_spans(&tenant, accepted);
            success.into_response()
        }
        Err(err) => {
            // A produce failure is an actual WAL-append error (distinct from a
            // 4xx validation/rate-limit reject handled above).
            state.metrics.record_wal_append_failure();
            error_response(&err)
        }
    }
}

fn otlp_success_response() -> Response {
    let body = ExportTraceServiceResponse {
        partial_success: None,
    }
    .encode_to_vec();
    ([(header::CONTENT_TYPE, "application/x-protobuf")], body).into_response()
}

fn require_content_type(headers: &HeaderMap, allowed: &[&str]) -> Result<(), TracesError> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Ok(());
    };
    let declared = value
        .to_str()
        .map_err(|err| TracesError::UnsupportedContentType(err.to_string()))?;
    let media_type = declared
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if allowed
        .iter()
        .any(|allowed| media_type == allowed.to_ascii_lowercase())
    {
        Ok(())
    } else {
        Err(TracesError::UnsupportedContentType(declared.to_string()))
    }
}

fn is_jaeger_binary_thrift(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|declared| declared.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("application/vnd.apache.thrift.binary")
        })
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
    let decoded = if encoding.eq_ignore_ascii_case("identity") {
        body.to_vec()
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut out = Vec::new();
        GzDecoder::new(body)
            .take(u64::try_from(max_decompressed).unwrap_or(u64::MAX) + 1)
            .read_to_end(&mut out)
            .map_err(|err| TracesError::Decode(err.to_string()))?;
        out
    } else {
        return Err(TracesError::UnsupportedContentType(encoding.to_string()));
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

fn tenant_metadata(metadata: &MetadataMap) -> String {
    metadata
        .get(TENANT_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

impl DistributorState {
    fn enforce_ingest(&self, tenant: &str, spans: &[Span]) -> Result<(), TracesError> {
        validate(spans, &self.limits)?;
        let limits = self.ingest_limits_for_tenant(tenant);
        validate_shared(spans, limits)?;
        self.ingest_enforcer
            .check_span_rate(
                limits,
                tenant,
                u64::try_from(spans.len()).unwrap_or(u64::MAX),
            )
            .map_err(|err| limit_error_to_traces_error(&err))
    }

    fn ingest_limits_for_tenant(&self, tenant: &str) -> &Limits {
        self.overrides
            .as_ref()
            .map_or(&self.shared_limits, |overrides| {
                overrides.for_tenant(tenant)
            })
    }
}

/// Validate decoded spans against per-tenant structural limits.
pub fn validate(spans: &[Span], limits: &TenantLimits) -> Result<(), TracesError> {
    if spans.len() > limits.max_spans_per_request {
        return Err(TracesError::Limit(format!(
            "span count {} exceeds limit {}",
            spans.len(),
            limits.max_spans_per_request
        )));
    }
    let mut spans_per_trace = BTreeMap::new();
    for span in spans {
        let count = spans_per_trace
            .entry(span.trace_id)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        if *count > limits.max_spans_per_trace {
            return Err(TracesError::Limit(format!(
                "trace span count {} exceeds limit {}",
                count, limits.max_spans_per_trace
            )));
        }
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

fn validate_shared(spans: &[Span], limits: &Limits) -> Result<(), TracesError> {
    let mut spans_per_trace = BTreeMap::new();
    for span in spans {
        let count = spans_per_trace
            .entry(span.trace_id)
            .and_modify(|count| *count += 1_u64)
            .or_insert(1_u64);
        IngestEnforcer::check_trace_size(limits, *count)
            .map_err(|err| limit_error_to_traces_error(&err))?;
        check_shared_attrs(limits, &span.resource_attrs)?;
        check_shared_attrs(limits, &span.span_attrs)?;
        for event in &span.events {
            check_shared_attrs(limits, &event.attrs)?;
        }
        for link in &span.links {
            check_shared_attrs(limits, &link.attrs)?;
        }
    }
    Ok(())
}

fn check_shared_attrs(limits: &Limits, attrs: &[KeyValue]) -> Result<(), TracesError> {
    let flattened = attrs.iter().map(shared_attr_measured).collect::<Vec<_>>();
    IngestEnforcer::check_attributes(limits, &flattened)
        .map_err(|err| limit_error_to_traces_error(&err))
}

/// Pair an attribute key with the TRUE encoded byte length of its value.
///
/// Measuring the real byte length (rather than stringifying) ensures the
/// `max_attribute_bytes` cap sees the actual size of `Bytes`/`Int`/`Double`
/// values, which a `String` conversion would mis-report (e.g. a large byte
/// blob would otherwise read as length 0 and bypass the limit).
fn shared_attr_measured(attr: &KeyValue) -> (String, u64) {
    let value_bytes = match &attr.value {
        AttrValue::Str(value) => value.len(),
        AttrValue::Bytes(value) => value.len(),
        AttrValue::Int(value) => value.to_le_bytes().len(),
        AttrValue::Double(value) => value.to_le_bytes().len(),
        AttrValue::Bool(_) => 1,
    };
    (attr.key.clone(), value_bytes as u64)
}

fn limit_error_to_traces_error(err: &LimitError) -> TracesError {
    match err {
        LimitError::IngestionRateExceeded { .. } => TracesError::RateLimit(err.message()),
        LimitError::MaxSpansPerTrace { .. }
        | LimitError::AttributeTooLong { .. }
        | LimitError::TracesPerSearchExceeded { .. }
        | LimitError::SearchDurationExceeded { .. } => TracesError::Limit(err.message()),
    }
}

fn u64_limit_from_usize(value: usize) -> u64 {
    if value == usize::MAX {
        0
    } else {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}

fn f64_limit_from_usize(value: usize) -> f64 {
    if value == usize::MAX {
        0.0
    } else {
        value.to_string().parse().unwrap_or(f64::INFINITY)
    }
}

fn validate_attrs(attrs: &[KeyValue], limits: &TenantLimits) -> Result<(), TracesError> {
    for attr in attrs {
        let len = attr.key.len()
            + match &attr.value {
                AttrValue::Str(value) => value.len(),
                AttrValue::Bytes(value) => value.len(),
                AttrValue::Int(_) | AttrValue::Double(_) | AttrValue::Bool(_) => 0,
            };
        if len > limits.max_attr_value_len {
            return Err(TracesError::Limit(format!(
                "attribute `{}` exceeds limit {}",
                attr.key, limits.max_attr_value_len
            )));
        }
    }
    Ok(())
}

fn grpc_status_from_error(err: &TracesError) -> GrpcStatus {
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            GrpcStatus::resource_exhausted(err.to_string())
        }
        TracesError::Invalid(_) | TracesError::Decode(_) | TracesError::TooLarge { .. } => {
            GrpcStatus::invalid_argument(err.to_string())
        }
        TracesError::UnsupportedContentType(_) => GrpcStatus::unimplemented(err.to_string()),
        TracesError::Wal(_) | TracesError::Produce(_) | TracesError::Block(_) => {
            GrpcStatus::internal(err.to_string())
        }
    }
}

/// Append decoded spans to the WAL sink.
///
/// All spans in one request are appended concurrently: each `append` enqueues
/// its record into the producer's per-partition accumulator (a fast, non-broker
/// hop) and then awaits the broker ack. Awaiting them sequentially would force N
/// serial produce+ack round-trips — on a single-partition WAL with
/// `max.in.flight=1` that serialized a few-hundred-span batch into seconds,
/// overrunning the OTLP client's deadline. Firing them together lets the
/// producer coalesce them into a handful of batches drained in ~one round-trip.
/// Per-partition ordering and idempotent sequencing are unaffected (the sender
/// still drains each partition in order with one batch in flight); traces carry
/// no cross-span WAL-order dependency (the block-builder regroups by `trace_id`).
pub async fn produce_spans(
    sink: &dyn WalSink,
    tenant: &str,
    spans: Vec<Span>,
) -> Result<(), TracesError> {
    let appends = spans.into_iter().map(|span| {
        sink.append(SpanRecord {
            tenant: tenant.to_string(),
            span,
        })
    });
    futures::future::try_join_all(appends).await?;
    Ok(())
}

fn error_response(err: &TracesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        TracesError::Limit(_) | TracesError::RateLimit(_) => {
            tempo_error_response(status, err.to_string())
        }
        _ => (status, err.to_string()).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write as _, sync::Mutex};

    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use flate2::{Compression, write::GzEncoder};
    use http_body_util::BodyExt as _;
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData},
    };
    use prost::Message as _;
    use tonic::Request as GrpcRequest;
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
        let mut state = DistributorState::new(sink.clone());
        state.shared_limits = limits.to_shared_limits();
        state.limits = limits;
        state.max_decompressed = 1024 * 1024;
        (Arc::new(state), sink)
    }

    fn test_state_with_shared_limits(
        limits: crate::limits::Limits,
    ) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let mut state = DistributorState::new(sink.clone());
        state.shared_limits = limits;
        state.max_decompressed = 1024 * 1024;
        (Arc::new(state), sink)
    }

    fn test_state_with_overrides(
        overrides: crate::limits::OverridesProvider,
    ) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let mut state = DistributorState::new(sink.clone());
        state.overrides = Some(overrides);
        state.max_decompressed = 1024 * 1024;
        (Arc::new(state), sink)
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

    fn otlp_body_with_spans(n: u8) -> Vec<u8> {
        let spans = (0..n)
            .map(|idx| OtlpSpan {
                trace_id: vec![1; 16],
                span_id: vec![idx.saturating_add(1); 8],
                name: format!("span-{idx}"),
                start_time_unix_nano: 1_000 + u64::from(idx),
                end_time_unix_nano: 1_500 + u64::from(idx),
                ..OtlpSpan::default()
            })
            .collect();
        TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
        .encode_to_vec()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
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
        check!(resp.status() == StatusCode::OK);
        assert_eq!(sink.count(), 1);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
    }

    #[tokio::test]
    async fn otlp_push_returns_export_response_protobuf() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/x-protobuf")
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let response = ExportTraceServiceResponse::decode(body.as_ref()).unwrap();
        assert!(response.partial_success.is_none());
        assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_accepts_application_protobuf_content_type() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/protobuf")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.status() == StatusCode::OK);
        assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_accepts_case_insensitive_gzip_encoding() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-encoding", "GZip")
                    .body(Body::from(gzip(&otlp_body())))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.status() == StatusCode::OK);
        assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_rejects_declared_non_protobuf_content_type() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "text/plain")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.count() == 0);
    }

    #[tokio::test]
    async fn otlp_grpc_export_appends_and_returns_success() {
        let (state, sink) = test_state();
        let service = OtlpGrpcService::new(state);
        let mut req = GrpcRequest::new(ExportTraceServiceRequest {
            resource_spans: TracesData::decode(otlp_body().as_slice())
                .unwrap()
                .resource_spans,
        });
        req.metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let resp = service.export(req).await.unwrap();

        check!(resp.into_inner().partial_success.is_none());
        assert_eq!(sink.count(), 1);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
    }

    #[tokio::test]
    async fn jaeger_grpc_post_spans_appends_and_returns_success() {
        let (state, sink) = test_state();
        let service = JaegerGrpcService::new(state);
        let mut req = GrpcRequest::new(crate::wire::jaeger_grpc::api_v2::PostSpansRequest {
            batch: Some(crate::wire::jaeger_grpc::api_v2::Batch {
                process: Some(crate::wire::jaeger_grpc::api_v2::Process {
                    service_name: "checkout".into(),
                    tags: Vec::new(),
                }),
                spans: vec![crate::wire::jaeger_grpc::api_v2::Span {
                    trace_id: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                    span_id: vec![0, 0, 0, 0, 0, 0, 0, 3],
                    operation_name: "GET /grpc".into(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 2_000,
                    }),
                    duration: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 25_000,
                    }),
                    tags: vec![
                        crate::wire::jaeger_grpc::api_v2::KeyValue {
                            key: "span.kind".into(),
                            v_type: crate::wire::jaeger_grpc::api_v2::ValueType::String.into(),
                            v_str: "server".into(),
                            ..Default::default()
                        },
                        crate::wire::jaeger_grpc::api_v2::KeyValue {
                            key: "error".into(),
                            v_type: crate::wire::jaeger_grpc::api_v2::ValueType::Bool.into(),
                            v_bool: true,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
            }),
        });
        req.metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let resp = service.post_spans(req).await.unwrap();

        assert_eq!(sink.count(), 1);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
        assert_eq!(sink.span_name(0), "GET /grpc".to_string());
        check!(resp.into_inner() == crate::wire::jaeger_grpc::api_v2::PostSpansResponse {});
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
        check!(resp.status() == StatusCode::ACCEPTED);
        assert_eq!(sink.count(), 1);
        assert_eq!(sink.span_name(0), "GET /".to_string());
    }

    #[tokio::test]
    async fn jaeger_binary_thrift_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/traces")
                    .header("content-type", "application/vnd.apache.thrift.binary")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(jaeger_binary_batch()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(resp.status() == StatusCode::ACCEPTED);
        assert_eq!(sink.count(), 1);
        assert_eq!(sink.span_name(0), "GET /binary".to_string());
    }

    #[tokio::test]
    async fn jaeger_compact_datagram_appends() {
        let (state, sink) = test_state();

        handle_jaeger_compact_datagram(
            &state,
            "tenant-a",
            &crate::wire::jaeger::test_support::encode_sample_batch(),
        )
        .await
        .unwrap();

        assert_eq!(sink.count(), 1);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
        assert_eq!(sink.span_name(0), "GET /".to_string());
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

    #[tokio::test]
    async fn oversized_trace_limit_is_400() {
        let limits = TenantLimits {
            max_spans_per_trace: 0,
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

    #[tokio::test]
    async fn shared_trace_span_limit_is_enforced_before_append() {
        let limits = crate::limits::Limits {
            max_spans_per_trace: 1,
            ..crate::limits::Limits::default()
        };
        let (state, sink) = test_state_with_shared_limits(limits);

        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.status() == StatusCode::BAD_REQUEST);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "error");
        check!(
            json["error"]
                .as_str()
                .is_some_and(|message| message.contains("max spans per trace"))
        );
        check!(sink.count() == 0);
    }

    #[tokio::test]
    async fn tenant_override_trace_span_limit_is_enforced_before_append() {
        let overrides = crate::limits::OverridesProvider::from_yaml(
            r"
overrides:
  tenant-tight:
    max_spans_per_trace: 1
",
        )
        .unwrap();
        let (state, sink) = test_state_with_overrides(overrides);
        let app = router(state);

        let tight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-tight")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-loose")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(tight.status() == StatusCode::BAD_REQUEST);
        check!(loose.status() == StatusCode::OK);
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.tenant(0), "tenant-loose".to_string());
        assert_eq!(sink.tenant(1), "tenant-loose".to_string());
    }

    #[tokio::test]
    async fn shared_ingest_rate_limit_is_per_tenant() {
        let limits = crate::limits::Limits {
            ingestion_rate_spans_per_sec: 1.0,
            ingestion_burst_spans: 1,
            ..crate::limits::Limits::default()
        };
        let (state, sink) = test_state_with_shared_limits(limits);
        let app = router(state);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let other_tenant = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-b")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(first.status() == StatusCode::OK);
        assert!(second.status() == StatusCode::TOO_MANY_REQUESTS);
        let body = second.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "error");
        check!(
            json["error"]
                .as_str()
                .is_some_and(|message| message.contains("ingestion rate"))
        );
        check!(other_tenant.status() == StatusCode::OK);
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
        assert_eq!(sink.tenant(1), "tenant-b".to_string());
    }

    #[tokio::test]
    async fn ingest_rate_limit_is_per_tenant() {
        let limits = TenantLimits {
            max_ingest_spans_per_second: 1,
            ingest_rate_burst: 1,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let app = router(state);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let other_tenant = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-b")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(first.status() == StatusCode::OK);
        check!(second.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(other_tenant.status() == StatusCode::OK);
        assert_eq!(sink.count(), 2);
        assert_eq!(sink.tenant(0), "tenant-a".to_string());
        assert_eq!(sink.tenant(1), "tenant-b".to_string());
    }

    #[tokio::test]
    async fn otlp_grpc_limit_errors_are_resource_exhausted() {
        let limits = TenantLimits {
            max_spans_per_request: 0,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let service = OtlpGrpcService::new(state);
        let req = GrpcRequest::new(ExportTraceServiceRequest {
            resource_spans: TracesData::decode(otlp_body().as_slice())
                .unwrap()
                .resource_spans,
        });

        let err = service.export(req).await.unwrap_err();

        assert!(err.code() == tonic::Code::ResourceExhausted);
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

    #[test]
    fn validate_rejects_large_attribute_keys() {
        let limits = TenantLimits {
            max_attr_value_len: 4,
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
            resource_attrs: Vec::new(),
            span_attrs: vec![KeyValue {
                key: "too-large".into(),
                value: AttrValue::Bool(true),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };

        assert!(validate(&[span], &limits).is_err());
    }

    #[test]
    fn validate_rejects_traces_over_span_limit() {
        let limits = TenantLimits {
            max_spans_per_trace: 1,
            ..TenantLimits::default()
        };
        let first = Span {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: "root".into(),
            kind: crate::span::SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: crate::span::StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: Vec::new(),
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };
        let second = Span {
            span_id: [2; 8],
            ..first.clone()
        };
        let other_trace = Span {
            trace_id: [2; 16],
            ..first.clone()
        };

        assert!(validate(&[first.clone(), other_trace], &limits).is_ok());
        assert!(validate(&[first, second], &limits).is_err());
    }

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

    #[test]
    fn oversized_bytes_attr_is_rejected_by_attribute_size_cap() {
        let limits = crate::limits::Limits {
            max_attribute_bytes: 4,
            ..crate::limits::Limits::default()
        };
        // A `Bytes` value of 8 bytes exceeds the 4-byte cap. The old stringless
        // path measured it as length 0 and let it through.
        let attrs = vec![KeyValue {
            key: "blob".into(),
            value: AttrValue::Bytes(vec![0u8; 8]),
        }];

        let err = check_shared_attrs(&limits, &attrs).unwrap_err();
        assert!(matches!(err, TracesError::Limit(_)));

        // A small `Bytes` value within the cap is accepted.
        let small = vec![KeyValue {
            key: "b".into(),
            value: AttrValue::Bytes(vec![0u8; 2]),
        }];
        assert!(check_shared_attrs(&limits, &small).is_ok());
    }
}
