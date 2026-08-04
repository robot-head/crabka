//! An in-test OTLP collector: a real gRPC `TraceService` the spawned
//! `crabka-gres` processes export to.
//!
//! This exists because the cross-process propagation claim cannot be checked
//! from inside one process. Every other layer of the tracing suite installs a
//! subscriber and inspects `SpanData` in memory; here the spans have to make a
//! round trip through each node's batch exporter, over OTLP/gRPC, and be
//! decoded from the wire — which is also the only way the `service.instance.id`
//! resource attribute (the thing that says *which process* emitted a span)
//! becomes observable at all.
//!
//! `LogsService` is implemented alongside `TraceService` because
//! `crabka_telemetry::init` always builds a log exporter next to the span
//! exporter. Leaving logs unimplemented would make every log batch fail, and
//! the SDK reports export failures through `tracing` — which feeds the log
//! bridge, which fails again. Accepting and discarding them cuts that loop.

use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{
            ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
        },
        trace::v1::{
            ExportTraceServiceRequest, ExportTraceServiceResponse,
            trace_service_server::{TraceService, TraceServiceServer},
        },
    },
    common::v1::{AnyValue, KeyValue, any_value::Value},
    trace::v1::{ResourceSpans, Span, span::SpanKind},
};

/// The `service.instance.id` resource attribute, which is what distinguishes
/// one gres process's spans from another's.
const SERVICE_INSTANCE_ID: &str = "service.instance.id";

/// One exported span, flattened out of its resource and scope envelopes with
/// ids rendered as lowercase hex (the form a `traceparent` carries, so the
/// client's tag can be compared directly against what came back).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatSpan {
    /// `service.instance.id` of the process that emitted the span.
    pub instance: String,
    /// Exported span name. Note that `otel.name`, when recorded, *replaces*
    /// this — a `gres.range_serve` span arrives named after its RPC method and
    /// a `gres.statement` span after its query summary, so tests must key off
    /// attributes and span kind rather than this field.
    pub name: String,
    /// OTLP span kind.
    pub kind: SpanKind,
    /// 32-hex trace id.
    pub trace_id: String,
    /// 16-hex span id.
    pub span_id: String,
    /// 16-hex parent span id; empty for a root span.
    pub parent_span_id: String,
    /// Span attributes, rendered as strings.
    pub attributes: BTreeMap<String, String>,
}

impl FlatSpan {
    /// The value of `key`, if the span carries it.
    #[must_use]
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(String::as_str)
    }

    /// Whether `key` is present with exactly `value`.
    #[must_use]
    pub fn has_attribute(&self, key: &str, value: &str) -> bool {
        self.attribute(key) == Some(value)
    }

    /// Whether this span is one end of a range RPC — the pair that carries the
    /// trace across the process boundary.
    #[must_use]
    pub fn is_range_rpc(&self, kind: SpanKind) -> bool {
        self.kind == kind && self.has_attribute("rpc.system", RANGE_RPC_SYSTEM)
    }
}

/// `rpc.system` on both ends of the cross-node range RPC.
pub const RANGE_RPC_SYSTEM: &str = "crabka.range";

/// A running OTLP/gRPC collector bound to an ephemeral loopback port.
#[derive(Debug)]
pub struct OtlpCollector {
    endpoint: String,
    spans: SpanSink,
}

impl OtlpCollector {
    /// Binds a loopback port and serves OTLP traces and logs on it until the
    /// test process exits.
    ///
    /// # Errors
    ///
    /// Returns an error if the port cannot be bound.
    pub async fn start() -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let spans = SpanSink::default();
        let service = CollectorService {
            spans: spans.clone(),
        };
        let incoming = futures::stream::unfold(listener, |listener| async move {
            let accepted = listener.accept().await.map(|(stream, _)| stream);
            Some((accepted, listener))
        });
        let serving = service.clone();
        tokio::spawn(async move {
            let result = tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(serving.clone()))
                .add_service(
                    opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer::new(serving),
                )
                .serve_with_incoming(incoming)
                .await;
            if let Err(error) = result {
                eprintln!("in-test OTLP collector stopped: {error}");
            }
        });
        Ok(Self {
            endpoint: format!("http://{addr}"),
            spans,
        })
    }

    /// The `CRABKA_OTLP_ENDPOINT` value pointing at this collector.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Removes and returns everything received so far.
    #[must_use]
    pub fn drain(&self) -> Vec<FlatSpan> {
        std::mem::take(&mut *self.spans.lock())
    }
}

/// Shared buffer of decoded spans.
#[derive(Debug, Clone, Default)]
struct SpanSink(Arc<Mutex<Vec<FlatSpan>>>);

impl SpanSink {
    fn lock(&self) -> MutexGuard<'_, Vec<FlatSpan>> {
        // A poisoned lock only means a panicking test thread; the buffer is
        // still coherent, so keep serving it.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The gRPC service backing [`OtlpCollector`].
#[derive(Debug, Clone)]
struct CollectorService {
    spans: SpanSink,
}

#[async_trait::async_trait]
impl TraceService for CollectorService {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let decoded: Vec<FlatSpan> = request
            .into_inner()
            .resource_spans
            .iter()
            .flat_map(flatten_resource_spans)
            .collect();
        self.spans.lock().extend(decoded);
        Ok(tonic::Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[async_trait::async_trait]
impl LogsService for CollectorService {
    async fn export(
        &self,
        _request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        Ok(tonic::Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

/// Flattens one `ResourceSpans` into [`FlatSpan`]s, stamping each with the
/// resource's `service.instance.id`.
fn flatten_resource_spans(resource_spans: &ResourceSpans) -> Vec<FlatSpan> {
    let instance = resource_spans
        .resource
        .as_ref()
        .and_then(|resource| attribute(&resource.attributes, SERVICE_INSTANCE_ID))
        .unwrap_or_default();
    resource_spans
        .scope_spans
        .iter()
        .flat_map(|scope| scope.spans.iter())
        .map(|span| flatten_span(&instance, span))
        .collect()
}

/// Flattens one OTLP span.
fn flatten_span(instance: &str, span: &Span) -> FlatSpan {
    FlatSpan {
        instance: instance.to_owned(),
        name: span.name.clone(),
        kind: SpanKind::try_from(span.kind).unwrap_or(SpanKind::Unspecified),
        trace_id: hex(&span.trace_id),
        span_id: hex(&span.span_id),
        parent_span_id: hex(&span.parent_span_id),
        attributes: span
            .attributes
            .iter()
            .filter_map(|kv| render(kv).map(|value| (kv.key.clone(), value)))
            .collect(),
    }
}

/// The string form of `key` in an attribute list.
fn attribute(attributes: &[KeyValue], key: &str) -> Option<String> {
    attributes.iter().find(|kv| kv.key == key).and_then(render)
}

/// Renders an attribute value as a string. Composite values (arrays, nested
/// key-value lists, opaque bytes) are dropped: no span the gres query path
/// emits uses one, so a test that needed one would be testing something else.
fn render(kv: &KeyValue) -> Option<String> {
    match kv
        .value
        .as_ref()
        .and_then(|any: &AnyValue| any.value.clone())?
    {
        Value::StringValue(value) => Some(value),
        Value::BoolValue(value) => Some(value.to_string()),
        Value::IntValue(value) => Some(value.to_string()),
        Value::DoubleValue(value) => Some(value.to_string()),
        Value::ArrayValue(_)
        | Value::KvlistValue(_)
        | Value::BytesValue(_)
        | Value::StringValueStrindex(_) => None,
    }
}

/// Lowercase hex, the encoding a W3C `traceparent` uses for both ids.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}
