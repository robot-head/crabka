//! Metrics distributor role: validate, HA-dedup, and append to the WAL.

pub mod ha;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::Bytes as BodyBytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use bytes::Bytes;
use crabka_blockstore::SeriesFingerprint;
use crabka_client_consumer::{Consumer, ConsumerRecord};
use crabka_client_producer::{Header as ProducerHeader, Producer, ProducerRecord};
use crabka_ids::{Offset, PartitionIndex};
use crabka_telemetry::propagation::current_trace_headers;
pub use ha::{
    HA_TRACKER_TOPIC, HaDecision, HaElection, HaElectionRecord, HaTracker, ha_decision,
    ha_election, strip_replica_label,
};
use opentelemetry_proto::tonic::{
    collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    metrics::v1::MetricsData,
};
use tokio::net::TcpListener;
use tonic::{Request as TonicRequest, Response as TonicResponse, Status};
use tracing::Instrument as _;

use crate::{
    IngestEnforcer, LimitError, Limits, OverridesProvider,
    metrics::ServiceMetrics,
    otlp::{
        DeltaAccumulator, OtlpError, TranslationStrategy, decode_otlp_stateful,
        decode_otlp_stateful_bytes,
    },
    validate_tenant,
    wal::{SamplePayload, WAL_TOPIC, WalExemplar, WalRecord, partition_key},
    wire::{
        DecodedExemplar, DecodedSeries, WireError, WireFormat, WrittenCounts, decode_v1, decode_v2,
        negotiate,
    },
};

const MAX_EXEMPLAR_LABEL_CODEPOINTS: usize = 128;

/// Structural per-request limits enforced before WAL append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantLimits {
    pub max_label_name_len: usize,
    pub max_label_value_len: usize,
    pub max_samples_per_series: usize,
    pub max_series_per_request: usize,
    pub ingestion_rate_samples_per_second: usize,
    pub ingestion_burst_size: usize,
    pub out_of_order_time_window_ms: i64,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name_len: 2048,
            max_label_value_len: 2048,
            max_samples_per_series: 10_000,
            max_series_per_request: 100_000,
            ingestion_rate_samples_per_second: 1_000_000,
            ingestion_burst_size: 1_000_000,
            out_of_order_time_window_ms: 0,
        }
    }
}

/// Errors from appending to the metrics WAL.
#[derive(Debug, thiserror::Error)]
pub enum ProduceError {
    #[error("wal append failed: {0}")]
    Append(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionReplayError {
    #[error("HA election record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionConsumerError {
    #[error("HA election consumer poll failed: {0}")]
    Poll(String),

    #[error(transparent)]
    Replay(#[from] HaElectionReplayError),

    #[error("HA election consumer commit failed: {0}")]
    Commit(String),
}

/// Testable sink for metrics WAL records.
#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError>;
}

/// Producer-backed metrics WAL sink.
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
    async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        // Inject the current ingest span's W3C trace context into the WAL record
        // headers so the downstream compactor can stitch its `metrics_compaction`
        // span onto this producer's trace. Additive: it only appends the
        // traceparent/tracestate headers, and is an empty `Vec` (no-op) when no
        // span is active or OTLP is disabled.
        let headers = current_trace_headers()
            .into_iter()
            .map(|(key, value)| ProducerHeader {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            })
            .collect::<Vec<_>>();
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: WAL_TOPIC.to_string(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                headers,
                ..Default::default()
            })
            .await;
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}

#[must_use]
pub fn ha_election_compaction_key(record: &HaElectionRecord) -> Bytes {
    Bytes::from(format!("{}\0{}", record.tenant, record.cluster))
}

/// Testable sink for compacted HA election records.
#[async_trait::async_trait]
pub trait HaElectionSink: Send + Sync {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError>;
}

/// Producer-backed compacted HA election sink.
pub struct KafkaHaElectionSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaHaElectionSink {
    #[must_use]
    pub fn new(producer: Arc<Producer>, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }
}

#[async_trait::async_trait]
impl HaElectionSink for KafkaHaElectionSink {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
        let key = ha_election_compaction_key(&record);
        let value = record
            .encode()
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        ack.await
            .map_err(|error| ProduceError::Append(error.to_string()))?
            .map_err(|error| ProduceError::Append(error.to_string()))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset: the next offset after the last replayed record.
    pub offset: Offset,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

pub fn replay_ha_election_records(
    tracker: &HaTracker,
    ha_topic: &str,
    records: &[HaElectionConsumerRecord],
) -> Result<HaElectionReplayResult, HaElectionReplayError> {
    let mut committed_offsets = BTreeMap::<PartitionIndex, Offset>::new();
    let mut replayed_records = 0;
    for record in records {
        if record.topic != ha_topic {
            continue;
        }
        let value = record
            .value
            .as_deref()
            .ok_or(HaElectionReplayError::MissingValue {
                partition: record.partition,
                offset: record.offset,
            })?;
        let election_record = HaElectionRecord::decode(value)
            .map_err(|error| HaElectionReplayError::Decode(error.to_string()))?;
        tracker.persist_elected(&election_record);
        replayed_records += 1;
        committed_offsets
            .entry(record.partition)
            .and_modify(|offset| *offset = (*offset).max(record.offset + 1))
            .or_insert(record.offset + 1);
    }

    Ok(HaElectionReplayResult {
        polled_records: records.len(),
        replayed_records,
        committed_offsets: committed_offsets
            .into_iter()
            .map(|(partition, offset)| HaElectionPartitionOffset { partition, offset })
            .collect(),
    })
}

#[async_trait::async_trait]
pub trait HaElectionConsumerPoll: Send {
    async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError>;
}

#[async_trait::async_trait]
pub trait HaElectionConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError>;
}

#[async_trait::async_trait]
impl HaElectionConsumerPoll for Consumer {
    async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
        Consumer::poll(self, timeout)
            .await
            .map_err(|error| HaElectionConsumerError::Poll(error.to_string()))
    }
}

#[async_trait::async_trait]
impl HaElectionConsumerCommit for Consumer {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
        Consumer::commit_sync(self)
            .await
            .map_err(|error| HaElectionConsumerError::Commit(error.to_string()))
    }
}

pub async fn poll_ha_election_consumer_once<C>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Duration,
) -> Result<HaElectionReplayResult, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
{
    let records = consumer.poll(timeout).await?;
    let replay_records = records
        .into_iter()
        .map(|record| HaElectionConsumerRecord {
            topic: record.topic,
            partition: PartitionIndex(record.partition),
            offset: Offset(record.offset),
            value: record.value.map(|value| value.to_vec()),
        })
        .collect::<Vec<_>>();
    let result = replay_ha_election_records(tracker, ha_topic, &replay_records)?;
    if result.replayed_records > 0 {
        consumer.commit_sync().await?;
    }
    Ok(result)
}

pub async fn run_ha_election_consumer_loop<C, Stop>(
    consumer: &mut C,
    tracker: &HaTracker,
    ha_topic: &str,
    timeout: Duration,
    mut should_stop: Stop,
) -> Result<HaElectionConsumerLoopSummary, HaElectionConsumerError>
where
    C: HaElectionConsumerPoll + HaElectionConsumerCommit + ?Sized,
    Stop: FnMut(&HaElectionConsumerLoopSummary) -> bool,
{
    let mut summary = HaElectionConsumerLoopSummary::default();
    loop {
        let result = poll_ha_election_consumer_once(consumer, tracker, ha_topic, timeout).await?;
        summary.polls += 1;
        summary.polled_records += result.polled_records;
        summary.replayed_records += result.replayed_records;
        summary.committed_offsets.extend(result.committed_offsets);

        if should_stop(&summary) {
            break;
        }
    }
    Ok(summary)
}

/// Shared distributor handler state.
pub struct DistributorState {
    sink: Arc<dyn WalSink>,
    ha_election_sink: Option<Arc<dyn HaElectionSink>>,
    tracker: HaTracker,
    otlp_delta_accumulator: Mutex<DeltaAccumulator>,
    ingest_enforcer: IngestEnforcer,
    overrides: Option<OverridesProvider>,
    active_series: Mutex<BTreeMap<String, BTreeSet<SeriesFingerprint>>>,
    latest_timestamps: Mutex<BTreeMap<(String, SeriesFingerprint), i64>>,
    limits: TenantLimits,
    max_decompressed: usize,
    metrics: Option<ServiceMetrics>,
}

impl DistributorState {
    #[must_use]
    pub fn new(sink: Arc<dyn WalSink>) -> Self {
        Self {
            sink,
            ha_election_sink: None,
            tracker: HaTracker::default(),
            otlp_delta_accumulator: Mutex::new(DeltaAccumulator::default()),
            ingest_enforcer: IngestEnforcer::new(),
            overrides: None,
            active_series: Mutex::new(BTreeMap::new()),
            latest_timestamps: Mutex::new(BTreeMap::new()),
            limits: TenantLimits::default(),
            max_decompressed: 32 * 1024 * 1024,
            metrics: None,
        }
    }

    #[must_use]
    pub fn with_limits(mut self, limits: TenantLimits) -> Self {
        self.limits = limits;
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_overrides(mut self, overrides: OverridesProvider) -> Self {
        self.overrides = Some(overrides);
        self
    }

    #[must_use]
    pub fn with_max_decompressed(mut self, max_decompressed: usize) -> Self {
        self.max_decompressed = max_decompressed;
        self
    }

    #[must_use]
    pub fn with_ha_election_sink(mut self, sink: Arc<dyn HaElectionSink>) -> Self {
        self.ha_election_sink = Some(sink);
        self
    }

    #[must_use]
    pub fn tracker(&self) -> &HaTracker {
        &self.tracker
    }
}

/// Build the distributor HTTP router.
pub fn router(state: Arc<DistributorState>) -> Router {
    let grpc_service = otlp_metrics_service_server(Arc::clone(&state));
    // Cap the (compressed) push body explicitly rather than relying on axum's
    // implicit 2 MiB default. A snappy body cannot usefully exceed the
    // decompressed cap, so `max_decompressed` is a sound, configurable ceiling
    // — applied per-route so the tonic gRPC `route_service` keeps its own limit.
    let max_body = state.max_decompressed;
    Router::new()
        .route(
            "/api/v1/push",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/api/v1/write",
            post(push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route(
            "/otlp/v1/metrics",
            post(otlp_push).layer(DefaultBodyLimit::max(max_body)),
        )
        .route_service(
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
            grpc_service,
        )
        .with_state(state)
}

/// Build the OTLP gRPC metrics service implementation.
#[must_use]
pub fn otlp_metrics_service(state: Arc<DistributorState>) -> OtlpMetricsService {
    OtlpMetricsService { state }
}

/// Build a tonic server for OTLP metrics export.
#[must_use]
pub fn otlp_metrics_service_server(
    state: Arc<DistributorState>,
) -> MetricsServiceServer<OtlpMetricsService> {
    MetricsServiceServer::new(otlp_metrics_service(state))
}

/// OTLP `MetricsService` implementation backed by the distributor WAL pipeline.
#[derive(Clone)]
pub struct OtlpMetricsService {
    state: Arc<DistributorState>,
}

#[tonic::async_trait]
impl MetricsService for OtlpMetricsService {
    async fn export(
        &self,
        request: TonicRequest<ExportMetricsServiceRequest>,
    ) -> Result<TonicResponse<ExportMetricsServiceResponse>, Status> {
        let started = std::time::Instant::now();
        let result = otlp_grpc_export_inner(&self.state, request).await;
        if let Some(metrics) = &self.state.metrics {
            let secs = started.elapsed().as_secs_f64();
            match &result {
                Ok(items) => metrics.record_ingest(true, 0, *items, secs),
                Err(_) => metrics.record_ingest(false, 0, 0, secs),
            }
        }
        match result {
            Ok(_) => Ok(TonicResponse::new(ExportMetricsServiceResponse {
                partial_success: None,
            })),
            Err(error) => Err(status_from_push_error(&error)),
        }
    }
}

/// Bind and serve the metrics distributor until `shutdown` resolves.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, "metrics distributor server stopped with error");
        }
    });
    Ok(bound)
}

async fn push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let bytes = body.len() as u64;
    // ONE ingest span per request (not per series/sample). `crabka.ingest.series`
    // starts empty and is recorded from inside `push_inner` once the body is
    // decoded; the WAL producer injects this span's trace context into the record
    // headers so the compactor's span joins the same distributed trace.
    let span = ingest_span(&headers, bytes);
    let result = push_inner(&state, &headers, &body).instrument(span).await;
    record_ingest_outcome(&state, &result, bytes, started.elapsed().as_secs_f64());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}

/// Build the per-request ingest span. `crabka.ingest.series` is declared empty
/// here and recorded once the request body is decoded (see `push_inner`).
fn ingest_span(headers: &HeaderMap, bytes: u64) -> tracing::Span {
    let tenant = tenant_for_span(headers);
    tracing::info_span!(
        "metrics_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = WAL_TOPIC,
        crabka.tenant = %tenant,
        crabka.ingest.series = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    )
}

/// Tenant label for the ingest span, falling back to `"unknown"` when the
/// `X-Scope-OrgID` header is absent or non-ASCII. This is span-only labelling and
/// never rejects the request — validation stays in `tenant_from_headers`.
fn tenant_for_span(headers: &HeaderMap) -> String {
    headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

async fn push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    let format = negotiate(content_type)?;
    require_snappy_encoding(headers)?;

    let (mut series, counts) = match format {
        WireFormat::RemoteWriteV1 => (decode_v1(body, state.max_decompressed)?, None),
        WireFormat::RemoteWriteV2 => {
            let (series, counts) = decode_v2(body, state.max_decompressed)?;
            (series, Some(counts))
        }
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("crabka.ingest.series", items);

    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((
            PushSuccess::Accepted {
                counts: counts.map(|_| WrittenCounts::default()),
            },
            items,
        ));
    }

    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::NoContent { counts }, items))
}

fn require_snappy_encoding(headers: &HeaderMap) -> Result<(), WireError> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !header_list_includes(encoding, "snappy") {
        return Err(WireError::UnsupportedContentEncoding(encoding.to_string()));
    }
    Ok(())
}

fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}

async fn otlp_push(
    State(state): State<Arc<DistributorState>>,
    headers: HeaderMap,
    body: BodyBytes,
) -> Response {
    let started = std::time::Instant::now();
    let bytes = body.len() as u64;
    // ONE ingest span per OTLP HTTP push request; series recorded post-decode.
    let span = ingest_span(&headers, bytes);
    let result = otlp_push_inner(&state, &headers, &body)
        .instrument(span)
        .await;
    record_ingest_outcome(&state, &result, bytes, started.elapsed().as_secs_f64());
    match result {
        Ok((success, _items)) => success.into_response(),
        Err(error) => error.into_response(),
    }
}

async fn otlp_push_inner(
    state: &DistributorState,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(PushSuccess, u64), PushError> {
    let tenant = tenant_from_headers(headers)?;
    require_otlp_protobuf_content_type(headers)?;
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful_bytes(body, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    // Backfill the decoded series count onto the enclosing `metrics_ingest` span.
    tracing::Span::current().record("crabka.ingest.series", items);
    if !append_decoded_series(state, tenant, &mut series).await? {
        return Ok((PushSuccess::Accepted { counts: None }, items));
    }
    if let Some(metrics) = &state.metrics {
        metrics.record_ingest_series(tenant, items);
    }
    Ok((PushSuccess::Ok, items))
}

/// Record an ingest request outcome on the distributor metrics bundle, if one
/// is configured. `bytes` is the (compressed) request-body length; `items` is
/// the decoded series count on success and `0` on error.
fn record_ingest_outcome(
    state: &DistributorState,
    result: &Result<(PushSuccess, u64), PushError>,
    bytes: u64,
    secs: f64,
) {
    let Some(metrics) = &state.metrics else {
        return;
    };
    match result {
        Ok((_, items)) => metrics.record_ingest(true, bytes, *items, secs),
        Err(_) => metrics.record_ingest(false, bytes, 0, secs),
    }
}

/// Decode and append an OTLP gRPC export. Returns the decoded series count
/// (the ingest `items` measure) on success.
async fn otlp_grpc_export_inner(
    state: &DistributorState,
    request: TonicRequest<ExportMetricsServiceRequest>,
) -> Result<u64, PushError> {
    let tenant = tenant_from_metadata(request.metadata())?.to_string();
    let data = MetricsData {
        resource_metrics: request.into_inner().resource_metrics,
    };
    let mut series = {
        let mut accumulator = state
            .otlp_delta_accumulator
            .lock()
            .expect("otlp delta accumulator poisoned");
        decode_otlp_stateful(&data, TranslationStrategy::default(), &mut accumulator)?
    };
    let items = series.len() as u64;
    if append_decoded_series(state, &tenant, &mut series).await?
        && let Some(metrics) = &state.metrics
    {
        metrics.record_ingest_series(&tenant, items);
    }
    Ok(items)
}

fn require_otlp_protobuf_content_type(headers: &HeaderMap) -> Result<(), WireError> {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let base = content_type.split(';').next().unwrap_or_default().trim();
    if !base.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(WireError::UnsupportedContentType(base.to_string()));
    }
    Ok(())
}

async fn append_decoded_series(
    state: &DistributorState,
    tenant: &str,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    validate(series, &state.limits)?;
    let limits = state.limits_for_tenant(tenant);
    enforce_label_limits(&limits, series)?;
    // Decide-and-commit the in-memory HA winner atomically so a racing replica
    // cannot also win the same (tenant, cluster); only the durable Kafka persist
    // is left async, after the in-memory winner is already fixed.
    match state.tracker.elect_now(tenant, series) {
        HaElection::Accept => {}
        HaElection::Drop => return Ok(false),
        HaElection::Elect(record) | HaElection::Update(record) => {
            // The in-memory winner is already committed under the tracker lock;
            // only the durable Kafka persist remains and may proceed async.
            if let Some(sink) = &state.ha_election_sink {
                sink.persist_election(record.clone()).await?;
            }
        }
    }

    strip_replica_label(series);
    enforce_and_record_active_series(state, &limits, tenant, series)?;
    enforce_ingestion_rate(state, &limits, tenant, series)?;
    enforce_out_of_order_window(state, &limits, tenant, series)?;
    for record in wal_records_from_series(tenant, series) {
        let key = partition_key(tenant, record.series_fingerprint());
        if let Err(error) = state.sink.append(key, record).await {
            // The actual WAL/produce error site — count it distinctly from
            // 4xx client/validation rejects so operators can alert on durable
            // append failures via rate(wal_append_failures_total).
            if let Some(metrics) = &state.metrics {
                metrics.wal_append_failures.inc();
            }
            return Err(error.into());
        }
    }
    Ok(true)
}

impl DistributorState {
    fn limits_for_tenant(&self, tenant: &str) -> Limits {
        self.overrides.as_ref().map_or_else(
            || tenant_limits_to_limits(&self.limits),
            |overrides| overrides.for_tenant(tenant).clone(),
        )
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "Legacy distributor limits are integer sample counts; the Mimir limit model stores ingestion rate as f64 samples/sec."
)]
fn tenant_limits_to_limits(limits: &TenantLimits) -> Limits {
    Limits {
        ingestion_rate: limits.ingestion_rate_samples_per_second as f64,
        ingestion_burst_size: u64::try_from(limits.ingestion_burst_size).unwrap_or(u64::MAX),
        max_label_name_length: u64::try_from(limits.max_label_name_len).unwrap_or(u64::MAX),
        max_label_value_length: u64::try_from(limits.max_label_value_len).unwrap_or(u64::MAX),
        out_of_order_time_window_ms: limits.out_of_order_time_window_ms,
        ..Limits::default()
    }
}

fn enforce_label_limits(limits: &Limits, series: &[DecodedSeries]) -> Result<(), LimitError> {
    for series in series {
        IngestEnforcer::check_labels(limits, &series.labels)?;
    }
    Ok(())
}

/// Enforce the per-user active-series limit and record the new series under a
/// single lock acquisition. Holding the lock across the check AND the insert
/// closes the active-series TOCTOU: two concurrent pushes can no longer both
/// observe the same pre-insert count and overshoot `max_global_series_per_user`.
fn enforce_and_record_active_series(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), LimitError> {
    let mut active = state
        .active_series
        .lock()
        .expect("active series tracker poisoned");

    if limits.max_global_series_per_user != 0 {
        let existing = active.get(tenant);
        let current = existing.map_or(0, BTreeSet::len);
        let would_add = series
            .iter()
            .map(|series| series.labels.fingerprint())
            .filter(|fingerprint| existing.is_none_or(|set| !set.contains(fingerprint)))
            .collect::<BTreeSet<_>>()
            .len();

        state.ingest_enforcer.check_active_series(
            limits,
            tenant,
            u64::try_from(would_add).unwrap_or(u64::MAX),
            u64::try_from(current).unwrap_or(u64::MAX),
        )?;
    }

    let tenant_active = active.entry(tenant.to_string()).or_default();
    for series in series {
        tenant_active.insert(series.labels.fingerprint());
    }
    Ok(())
}

fn enforce_ingestion_rate(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    let sample_count = decoded_sample_count(series);
    if sample_count == 0 {
        return Ok(());
    }

    state
        .ingest_enforcer
        .check_sample_rate(
            limits,
            tenant,
            u64::try_from(sample_count).unwrap_or(u64::MAX),
        )
        .map_err(PushError::from)
}

fn decoded_sample_count(series: &[DecodedSeries]) -> usize {
    series
        .iter()
        .map(|series| series.samples.len() + series.histograms.len() + series.exemplars.len())
        .sum()
}

fn enforce_out_of_order_window(
    state: &DistributorState,
    limits: &Limits,
    tenant: &str,
    series: &[DecodedSeries],
) -> Result<(), PushError> {
    if limits.out_of_order_time_window_ms < 0 {
        return Ok(());
    }

    let mut latest = state
        .latest_timestamps
        .lock()
        .expect("latest timestamp tracker poisoned");
    let mut updates = Vec::new();
    for series in series {
        let Some((min_timestamp, max_timestamp)) = sample_timestamp_bounds(series) else {
            continue;
        };
        let fingerprint = series.labels.fingerprint();
        let key = (tenant.to_string(), fingerprint);
        if let Some(previous_latest) = latest.get(&key).copied() {
            let oldest_allowed = previous_latest - limits.out_of_order_time_window_ms;
            if min_timestamp < oldest_allowed {
                return Err(PushError::TooOldSample {
                    timestamp_ms: min_timestamp,
                    oldest_allowed_ms: oldest_allowed,
                });
            }
        }
        updates.push((key, max_timestamp));
    }

    for (key, max_timestamp) in updates {
        latest
            .entry(key)
            .and_modify(|previous| *previous = (*previous).max(max_timestamp))
            .or_insert(max_timestamp);
    }
    Ok(())
}

fn sample_timestamp_bounds(series: &DecodedSeries) -> Option<(i64, i64)> {
    series
        .samples
        .iter()
        .map(|sample| sample.timestamp_ms)
        .chain(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, _)| *timestamp_ms),
        )
        .chain(
            series
                .exemplars
                .iter()
                .map(|exemplar| exemplar.timestamp_ms),
        )
        .fold(None, |bounds, timestamp| match bounds {
            None => Some((timestamp, timestamp)),
            Some((min_timestamp, max_timestamp)) => {
                Some((min_timestamp.min(timestamp), max_timestamp.max(timestamp)))
            }
        })
}

// cargo-mutants: covered through HTTP push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
fn tenant_from_headers(headers: &HeaderMap) -> Result<&str, PushError> {
    headers
        .get("X-Scope-OrgID")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}

// cargo-mutants: covered through OTLP gRPC push-path tenant validation tests.
#[cfg_attr(test, mutants::skip)]
fn tenant_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Result<&str, PushError> {
    metadata
        .get("x-scope-orgid")
        .ok_or(PushError::MissingTenant)?
        .to_str()
        .map_err(|_| PushError::MissingTenant)
        .and_then(validate_request_tenant)
}

// cargo-mutants: shared tenant validation glue is covered by HTTP and gRPC callers.
#[cfg_attr(test, mutants::skip)]
fn validate_request_tenant(tenant: &str) -> Result<&str, PushError> {
    if tenant.is_empty() {
        Err(PushError::MissingTenant)
    } else {
        validate_tenant(tenant).map_err(PushError::InvalidTenant)?;
        Ok(tenant)
    }
}

/// Validate decoded series against structural limits.
pub fn validate(series: &[DecodedSeries], limits: &TenantLimits) -> Result<(), WireError> {
    if series.len() > limits.max_series_per_request {
        return Err(WireError::Invalid(format!(
            "series per request {} exceeds limit {}",
            series.len(),
            limits.max_series_per_request
        )));
    }

    for series in series {
        let sample_count = series.samples.len() + series.histograms.len() + series.exemplars.len();
        if sample_count > limits.max_samples_per_series {
            return Err(WireError::Invalid(format!(
                "samples per series {sample_count} exceeds limit {}",
                limits.max_samples_per_series
            )));
        }
        for (name, value) in series.labels.iter() {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!("invalid label name `{name}`")));
            }
            if name.len() > limits.max_label_name_len {
                return Err(WireError::Invalid(format!(
                    "label name length {} exceeds limit {}",
                    name.len(),
                    limits.max_label_name_len
                )));
            }
            if value.len() > limits.max_label_value_len {
                return Err(WireError::Invalid(format!(
                    "label value length {} exceeds limit {}",
                    value.len(),
                    limits.max_label_value_len
                )));
            }
        }
        for exemplar in &series.exemplars {
            validate_exemplar_labels(exemplar)?;
        }
    }

    Ok(())
}

fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn validate_exemplar_labels(exemplar: &DecodedExemplar) -> Result<(), WireError> {
    let codepoints = exemplar
        .labels
        .iter()
        .try_fold(0usize, |codepoints, (name, value)| {
            if !is_valid_label_name(name) {
                return Err(WireError::Invalid(format!(
                    "invalid exemplar label name `{name}`"
                )));
            }
            Ok(codepoints + name.chars().count() + value.chars().count())
        })?;
    if codepoints > MAX_EXEMPLAR_LABEL_CODEPOINTS {
        return Err(WireError::Invalid(format!(
            "exemplar label set has {codepoints} codepoints, exceeding limit {MAX_EXEMPLAR_LABEL_CODEPOINTS}"
        )));
    }
    Ok(())
}

enum PushSuccess {
    Ok,
    Accepted { counts: Option<WrittenCounts> },
    NoContent { counts: Option<WrittenCounts> },
}

impl IntoResponse for PushSuccess {
    fn into_response(self) -> Response {
        match self {
            Self::Ok => StatusCode::OK.into_response(),
            Self::Accepted { counts: None } => StatusCode::ACCEPTED.into_response(),
            Self::Accepted {
                counts: Some(counts),
            } => written_counts_response(StatusCode::ACCEPTED, counts),
            Self::NoContent { counts: None } => StatusCode::NO_CONTENT.into_response(),
            Self::NoContent {
                counts: Some(counts),
            } => written_counts_response(StatusCode::NO_CONTENT, counts),
        }
    }
}

fn written_counts_response(status: StatusCode, counts: WrittenCounts) -> Response {
    let mut response = status.into_response();
    let headers = response.headers_mut();
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Samples-Written",
        counts.samples,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Histograms-Written",
        counts.histograms,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Exemplars-Written",
        counts.exemplars,
    );
    response
}

fn insert_written_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    headers.insert(
        name,
        HeaderValue::from_str(&value.to_string()).expect("u64 header value"),
    );
}

#[derive(Debug, thiserror::Error)]
enum PushError {
    #[error("missing X-Scope-OrgID tenant header")]
    MissingTenant,
    #[error("invalid tenant: {0}")]
    InvalidTenant(String),
    #[error(
        "too-old-sample: timestamp {timestamp_ms} is older than oldest allowed {oldest_allowed_ms}"
    )]
    TooOldSample {
        timestamp_ms: i64,
        oldest_allowed_ms: i64,
    },
    #[error(transparent)]
    Limit(#[from] LimitError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Otlp(#[from] OtlpError),
    #[error(transparent)]
    Produce(#[from] ProduceError),
}

impl IntoResponse for PushError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Limit(error) => {
                StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::MissingTenant | Self::InvalidTenant(_) | Self::TooOldSample { .. } => {
                StatusCode::BAD_REQUEST
            }
            Self::Wire(error) => StatusCode::from_u16(error.status_code())
                .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Self::Otlp(error) => {
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST)
            }
            Self::Produce(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

fn status_from_push_error(error: &PushError) -> Status {
    match error {
        PushError::Limit(limit)
            if limit.http_status() == StatusCode::TOO_MANY_REQUESTS.as_u16() =>
        {
            Status::resource_exhausted(error.to_string())
        }
        PushError::Produce(_) => Status::internal(error.to_string()),
        PushError::Wire(wire) if wire.status_code() == StatusCode::TOO_MANY_REQUESTS.as_u16() => {
            Status::resource_exhausted(error.to_string())
        }
        PushError::Wire(wire)
            if wire.status_code() == StatusCode::INTERNAL_SERVER_ERROR.as_u16() =>
        {
            Status::internal(error.to_string())
        }
        PushError::Otlp(otlp)
            if otlp.status_code() == StatusCode::INTERNAL_SERVER_ERROR.as_u16() =>
        {
            Status::internal(error.to_string())
        }
        PushError::MissingTenant
        | PushError::InvalidTenant(_)
        | PushError::TooOldSample { .. }
        | PushError::Limit(_)
        | PushError::Wire(_)
        | PushError::Otlp(_) => Status::invalid_argument(error.to_string()),
    }
}

/// Fan decoded series into one WAL record per float or native-histogram sample.
#[must_use]
pub fn wal_records_from_series(tenant: &str, series: &[DecodedSeries]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for series in series {
        let labels = label_pairs(series);
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| WalExemplar {
                labels: exemplar
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                value: exemplar.value,
                timestamp_ms: exemplar.timestamp_ms,
            })
            .collect::<Vec<_>>();

        out.extend(series.samples.iter().map(|sample| WalRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            payload: SamplePayload::Float {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: sample.start_timestamp_ms,
            },
            exemplars: Vec::new(),
        }));
        out.extend(
            series
                .histograms
                .iter()
                .map(|(timestamp_ms, hist)| WalRecord {
                    tenant: tenant.to_string(),
                    labels: labels.clone(),
                    payload: SamplePayload::Hist {
                        timestamp_ms: *timestamp_ms,
                        hist: hist.clone(),
                    },
                    exemplars: Vec::new(),
                }),
        );
        if let Some(metadata) = &series.metadata {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels: labels.clone(),
                payload: SamplePayload::Metadata {
                    metric_family_name: metadata.metric_family_name.clone(),
                    metric_type: metadata.metric_type.clone(),
                    help: metadata.help.clone(),
                    unit: metadata.unit.clone(),
                },
                exemplars: Vec::new(),
            });
        }
        if !exemplars.is_empty() {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels,
                payload: SamplePayload::Exemplars,
                exemplars,
            });
        }
    }
    out
}

fn label_pairs(series: &DecodedSeries) -> Vec<(String, String)> {
    series
        .labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::{assert, check};
    use axum::{body::Body, http::Request};
    use crabka_blockstore::Labels;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::{
            ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
            metrics_service_server::MetricsService,
        },
        common::v1::{AnyValue, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Sum, metric, number_data_point,
        },
        resource::v1::Resource,
    };
    use prost::Message;
    use tower::ServiceExt as _;

    use super::*;
    use crate::wire::DecodedSample;

    /// Pins `tenant_for_span`'s span-label logic: a present non-empty header is
    /// echoed verbatim, while a missing OR empty `X-Scope-OrgID` falls back to
    /// `"unknown"`. Kills the whole-fn replacement mutants (`"xyzzy"` /
    /// `String::new()`) and the `delete !` mutant on `!value.is_empty()` — the
    /// empty-string case only maps to `"unknown"` while the negation stands.
    #[test]
    fn tenant_for_span_labels_present_and_falls_back_on_missing_or_empty() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert!(tenant_for_span(&present) == "acme");

        let missing = HeaderMap::new();
        assert!(tenant_for_span(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert!(tenant_for_span(&empty) == "unknown");
    }

    #[derive(Default)]
    struct RecordingSink {
        appends: Mutex<Vec<(Bytes, WalRecord)>>,
    }

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, key: Bytes, record: WalRecord) -> Result<(), ProduceError> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .push((key, record));
            Ok(())
        }
    }

    impl RecordingSink {
        fn records(&self) -> Vec<WalRecord> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(_, record)| record.clone())
                .collect()
        }

        fn append_keys(&self) -> Vec<Bytes> {
            self.appends
                .lock()
                .expect("recording sink poisoned")
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }
    }

    #[derive(Default)]
    struct RecordingHaElectionSink {
        elections: Mutex<Vec<HaElectionRecord>>,
    }

    #[async_trait::async_trait]
    impl HaElectionSink for RecordingHaElectionSink {
        async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .push(record);
            Ok(())
        }
    }

    impl RecordingHaElectionSink {
        fn elections(&self) -> Vec<HaElectionRecord> {
            self.elections
                .lock()
                .expect("ha election sink poisoned")
                .clone()
        }
    }

    struct RecordingHaElectionConsumer {
        batches: Vec<Vec<ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerPoll for RecordingHaElectionConsumer {
        async fn poll(
            &mut self,
            _timeout: Duration,
        ) -> Result<Vec<ConsumerRecord>, HaElectionConsumerError> {
            Ok(self.batches.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl HaElectionConsumerCommit for RecordingHaElectionConsumer {
        async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    fn consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: Option<Vec<u8>>,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: value.map(Bytes::from),
            headers: Vec::new(),
        }
    }

    struct FailingHaElectionSink;

    #[async_trait::async_trait]
    impl HaElectionSink for FailingHaElectionSink {
        async fn persist_election(&self, _record: HaElectionRecord) -> Result<(), ProduceError> {
            Err(ProduceError::Append("ha election unavailable".to_string()))
        }
    }

    fn test_state() -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        (Arc::new(DistributorState::new(sink.clone())), sink)
    }

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).unwrap()
    }

    fn v1_body(labels: Vec<crate::wire::pb::v1::Label>) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels,
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_samples(sample_count: usize) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: (0..sample_count)
                    .map(|index| crate::wire::pb::v1::Sample {
                        value: 1.0,
                        timestamp: i64::try_from(index).expect("test sample index fits in i64"),
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_sample_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            metadata: vec![crate::wire::pb::v1::MetricMetadata {
                r#type: crate::wire::pb::v1::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "http_requests_total".into(),
                help: "Total HTTP requests.".into(),
                unit: "requests".into(),
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_label_value(value: &str) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "http_requests_total")],
                samples: vec![crate::wire::pb::v1::Sample {
                    value: 1.0,
                    timestamp: 100,
                }],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", value)],
                    value: 1.0,
                    timestamp: 100,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v1_body_with_exemplar_timestamp(timestamp: i64) -> Vec<u8> {
        let req = crate::wire::pb::v1::WriteRequest {
            timeseries: vec![crate::wire::pb::v1::TimeSeries {
                labels: vec![label("__name__", "up")],
                exemplars: vec![crate::wire::pb::v1::Exemplar {
                    labels: vec![label("trace_id", "abc123")],
                    value: 1.0,
                    timestamp,
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_metadata() -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "http_requests_total".into(),
                "Total HTTP requests.".into(),
                "requests".into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                metadata: Some(crate::wire::pb::v2::Metadata {
                    r#type: crate::wire::pb::v2::metadata::MetricType::Counter as i32,
                    help_ref: 3,
                    unit_ref: 4,
                }),
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn v2_body_with_ha_replica(replica: &str) -> Vec<u8> {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![
                String::new(),
                "__name__".into(),
                "up".into(),
                "cluster".into(),
                "c1".into(),
                "__replica__".into(),
                replica.into(),
            ],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 0,
                }],
                ..Default::default()
            }],
        };
        snappy(&req.encode_to_vec())
    }

    fn otlp_body() -> Vec<u8> {
        otlp_gauge_body()
    }

    fn otlp_sum_body(value: f64, timestamp: u64, monotonic: bool, temporality: i32) -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: timestamp,
                                value: Some(number_data_point::Value::AsDouble(value)),
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality,
                            is_monotonic: monotonic,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_gauge_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        description: "CPU utilization ratio.".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![KeyValue {
                                    key: "host.name".into(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue("api-1".into())),
                                    }),
                                    key_strindex: 0,
                                }],
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn otlp_resource_body() -> Vec<u8> {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("checkout".into())),
                        }),
                        key_strindex: 0,
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "system.cpu.utilization".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000,
                                value: Some(number_data_point::Value::AsDouble(0.5)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    fn label(name: &str, value: &str) -> crate::wire::pb::v1::Label {
        crate::wire::pb::v1::Label {
            name: name.into(),
            value: value.into(),
        }
    }

    #[tokio::test]
    async fn push_v1_returns_204_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert_eq!(
            records,
            vec![WalRecord {
                tenant: "tenant-a".to_string(),
                labels: vec![("__name__".to_string(), "up".to_string())],
                payload: SamplePayload::Float {
                    timestamp_ms: 100,
                    value: 1.0,
                    start_timestamp_ms: None,
                },
                exemplars: Vec::new(),
            }]
        );
    }

    #[tokio::test]
    async fn push_v1_accepts_listed_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "identity, snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_v1_accepts_prometheus_remote_write_receiver_path() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/write")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_keys_wal_append_by_tenant_and_series_fingerprint() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert_eq!(
            (records.len(), sink.append_keys(),),
            (
                1,
                vec![crate::wal::partition_key(
                    "tenant-a",
                    records[0].series_fingerprint()
                )],
            )
        );
    }

    #[tokio::test]
    async fn push_v2_sets_written_headers() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            (
                response.status(),
                response
                    .headers()
                    .get("X-Prometheus-Remote-Write-Samples-Written")
                    .and_then(|value| value.to_str().ok()),
                sink.records().len(),
            ),
            (StatusCode::NO_CONTENT, Some("1"), 1)
        );
    }

    #[tokio::test]
    async fn push_v2_preserves_sample_start_timestamp_in_wal() {
        let req = crate::wire::pb::v2::Request {
            symbols: vec![String::new(), "__name__".into(), "up".into()],
            timeseries: vec![crate::wire::pb::v2::TimeSeries {
                labels_refs: vec![1, 2],
                samples: vec![crate::wire::pb::v2::Sample {
                    value: 3.0,
                    timestamp: 7,
                    start_timestamp: 5,
                }],
                ..Default::default()
            }],
        };
        let (state, sink) = test_state();

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(snappy(&req.encode_to_vec())))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        let records = sink.records();
        assert!(records.len() == 1);
        let SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms,
        } = records[0].payload
        else {
            panic!("expected float payload");
        };
        check!(timestamp_ms == 7);
        check!((value - 3.0).abs() < f64::EPSILON);
        check!(start_timestamp_ms == Some(5));
    }

    #[tokio::test]
    async fn push_v1_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn push_v2_appends_metric_metadata_record() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_metadata()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(records.len() == 2);
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn oversized_exemplar_labels_are_rejected() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_label_value(
                        &"x".repeat(129),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn oversized_label_names_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_label_name_len: 7,
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn runtime_overrides_apply_label_limits_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r"
overrides:
  tenant-tight:
    max_label_value_length: 2
",
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let tight_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-tight")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("job", "api"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(tight_response.status() == StatusCode::BAD_REQUEST);
        check!(loose_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn oversized_sample_sets_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            }),
        );
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_samples(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn validation_counts_exemplars_toward_samples_per_series_limit() {
        let mut labels = Labels::new();
        labels.insert("__name__", "http_requests_total");
        let mut exemplar_labels = Labels::new();
        exemplar_labels.insert("trace_id", "abc");
        let series = [DecodedSeries {
            labels,
            samples: Vec::new(),
            histograms: Vec::new(),
            exemplars: vec![
                DecodedExemplar {
                    labels: exemplar_labels.clone(),
                    timestamp_ms: 1000,
                    value: 1.0,
                },
                DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 2000,
                    value: 2.0,
                },
            ],
            metadata: None,
        }];

        let err = validate(
            &series,
            &TenantLimits {
                max_samples_per_series: 1,
                ..TenantLimits::default()
            },
        )
        .unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("samples per series 2 exceeds limit 1"));
    }

    #[test]
    fn validation_rejects_invalid_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid label name"));
        }
    }

    #[test]
    fn validation_rejects_invalid_exemplar_label_names() {
        for label_name in ["", "9bad", "bad-label"] {
            let mut labels = Labels::new();
            labels.insert("__name__", "up");
            let mut exemplar_labels = Labels::new();
            exemplar_labels.insert(label_name, "value");
            let series = [DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(1000, 1.0)],
                histograms: Vec::new(),
                exemplars: vec![DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 1000,
                    value: 1.0,
                }],
                metadata: None,
            }];

            let err = validate(&series, &TenantLimits::default()).unwrap_err();

            assert!(matches!(err, WireError::Invalid(_)));
            assert!(format!("{err}").contains("invalid exemplar label name"));
        }
    }

    #[tokio::test]
    async fn ingestion_rate_limit_returns_429_without_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate_samples_per_second: 1,
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(first_response.status() == StatusCode::NO_CONTENT);
        check!(second_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn concurrent_pushes_cannot_overshoot_active_series_limit() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r"
defaults:
  max_global_series_per_user: 1
",
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        // Two distinct series pushed concurrently; the check-and-insert is a
        // single locked critical section, so exactly one is admitted and the
        // other is rejected rather than both passing the pre-insert count.
        let request = |name: &str| {
            app.clone().oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", name)])))
                    .unwrap(),
            )
        };
        let (first, second) = tokio::join!(request("series_a"), request("series_b"));
        let statuses = [first.unwrap().status(), second.unwrap().status()];

        let admitted = statuses
            .iter()
            .filter(|status| **status == StatusCode::NO_CONTENT)
            .count();
        let rejected = statuses
            .iter()
            .filter(|status| **status == StatusCode::BAD_REQUEST)
            .count();
        check!(admitted == 1);
        check!(rejected == 1);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn ingestion_rate_limit_counts_exemplar_only_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                ingestion_rate_samples_per_second: 1,
                ingestion_burst_size: 1,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let exemplar_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let sample_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_001)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(exemplar_response.status() == StatusCode::NO_CONTENT);
        check!(sample_response.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn too_old_samples_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window_ms: 100,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let within_window_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(within_window_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn runtime_overrides_apply_out_of_order_window_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_overrides(
                crate::OverridesProvider::from_yaml(
                    r"
defaults:
  out_of_order_time_window_ms: 0
overrides:
  tenant-loose:
    out_of_order_time_window_ms: 100
",
                )
                .unwrap(),
            ),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let overridden_window_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-loose")
                    .body(Body::from(v1_body_with_sample_timestamp(950)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(overridden_window_response.status() == StatusCode::NO_CONTENT);
        check!(sink.records().len() == 2);
    }

    #[tokio::test]
    async fn too_old_exemplar_only_series_beyond_out_of_order_window_are_rejected() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_limits(TenantLimits {
                out_of_order_time_window_ms: 100,
                ..TenantLimits::default()
            }),
        );
        let app = router(state);

        let newest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_sample_timestamp(1_000)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let too_old_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body_with_exemplar_timestamp(899)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(newest_response.status() == StatusCode::NO_CONTENT);
        check!(too_old_response.status() == StatusCode::BAD_REQUEST);
        check!(sink.records().len() == 1);
    }

    #[tokio::test]
    async fn push_rejects_invalid_tenant_with_400() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "bad tenant")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_requires_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn push_rejects_non_snappy_content_encoding() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "gzip")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![label("__name__", "up")])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn unsupported_content_type_is_415() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(vec![1, 2, 3]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_metrics_returns_200_and_appends() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        assert_eq!(
            (&sample.tenant, &sample.labels, &sample.payload),
            (
                &"tenant-a".to_string(),
                &vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ],
                &SamplePayload::Float {
                    timestamp_ms: 1,
                    value: 0.5,
                    start_timestamp_ms: None,
                },
            )
        );
        let metadata = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Metadata { .. }))
            .expect("metadata wal record");
        assert!(
            metadata.payload
                == SamplePayload::Metadata {
                    metric_family_name: "system_cpu_utilization".to_string(),
                    metric_type: "gauge".to_string(),
                    help: "CPU utilization ratio.".to_string(),
                    unit: "1".to_string(),
                }
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_appends() {
        let (state, sink) = test_state();
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let service = otlp_metrics_service(state);
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = service.export(request).await.expect("otlp grpc export");

        let records = sink.records();
        assert!(response.into_inner().partial_success.is_none());
        assert!(records.len() == 2);
        let sample = records
            .iter()
            .find(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .expect("float wal record");
        assert_eq!(
            (sample.tenant.as_str(), &sample.labels),
            (
                "tenant-a",
                &vec![
                    ("__name__".to_string(), "system_cpu_utilization".to_string()),
                    ("host_name".to_string(), "api-1".to_string())
                ],
            )
        );
    }

    #[tokio::test]
    async fn otlp_grpc_metrics_export_round_trips_over_bound_server() {
        let (state, sink) = test_state();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async {
            let _ = shutdown_rx.await;
        })
        .await
        .expect("serve distributor");
        let data = MetricsData::decode(otlp_body().as_slice()).expect("otlp metrics data");
        let mut client = MetricsServiceClient::connect(format!("http://{bound}"))
            .await
            .expect("connect otlp grpc client");
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: data.resource_metrics,
        });
        request
            .metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let response = client.export(request).await.expect("otlp grpc export");
        let _ = shutdown_tx.send(());

        let records = sink.records();
        check!(response.into_inner().partial_success.is_none());
        check!(records.len() == 2);
        check!(records.iter().any(|record| record.tenant == "tenant-a"));
    }

    #[tokio::test]
    async fn otlp_metrics_rejects_non_protobuf_content_type() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/json")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn otlp_delta_sum_accumulates_across_pushes() {
        let (state, sink) = test_state();
        let app = router(state);

        let first_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        7.0,
                        2_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_sum_body(
                        5.0,
                        3_000_000,
                        true,
                        AggregationTemporality::Delta as i32,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(first_response.status() == StatusCode::OK);
        assert!(second_response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert_eq!(
            (
                float_records.len(),
                matches!(
                    float_records[0].payload,
                    SamplePayload::Float {
                        timestamp_ms: 2,
                        value: 7.0,
                        ..
                    }
                ),
                matches!(
                    float_records[1].payload,
                    SamplePayload::Float {
                        timestamp_ms: 3,
                        value: 12.0,
                        ..
                    }
                ),
            ),
            (2, true, true)
        );
    }

    #[tokio::test]
    async fn otlp_resource_attributes_append_target_info() {
        let (state, sink) = test_state();
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/otlp/v1/metrics")
                    .header("Content-Type", "application/x-protobuf")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(otlp_resource_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        let records = sink.records();
        assert!(response.status() == StatusCode::OK);
        let float_records = records
            .iter()
            .filter(|record| matches!(record.payload, SamplePayload::Float { .. }))
            .collect::<Vec<_>>();
        assert!(float_records.len() == 2);
        let target = records
            .iter()
            .find(|record| {
                matches!(record.payload, SamplePayload::Float { .. })
                    && record
                        .labels
                        .iter()
                        .any(|(name, value)| name == "__name__" && value == "target_info")
            })
            .expect("target_info wal record");
        assert_eq!(
            (
                &target.labels,
                matches!(
                    target.payload,
                    SamplePayload::Float {
                        timestamp_ms: 1,
                        value: 1.0,
                        ..
                    }
                ),
            ),
            (
                &vec![
                    ("__name__".to_string(), "target_info".to_string()),
                    ("service_name".to_string(), "checkout".to_string()),
                ],
                true,
            )
        );
    }

    #[tokio::test]
    async fn non_elected_replica_returns_202_without_append() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r2"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::ACCEPTED);
        assert!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn non_elected_v2_replica_returns_zero_written_headers() {
        let (state, sink) = test_state();
        state.tracker().set_elected("tenant-a", "c1", "r1");
        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header(
                        "Content-Type",
                        "application/x-protobuf; proto=io.prometheus.write.v2.Request",
                    )
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v2_body_with_ha_replica("r2")))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(response.status() == StatusCode::ACCEPTED);
        for header in [
            "X-Prometheus-Remote-Write-Samples-Written",
            "X-Prometheus-Remote-Write-Histograms-Written",
            "X-Prometheus-Remote-Write-Exemplars-Written",
        ] {
            check!(
                response
                    .headers()
                    .get(header)
                    .and_then(|value| value.to_str().ok())
                    == Some("0"),
                "header {header}",
            );
        }
        check!(sink.records().is_empty());
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persists_election_before_append() {
        let sink = Arc::new(RecordingSink::default());
        let election_sink = Arc::new(RecordingHaElectionSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone()).with_ha_election_sink(election_sink.clone()),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::NO_CONTENT);
        assert!(sink.records().len() == 1);
        let elections = election_sink.elections();
        assert!(elections.len() == 1);
        check!(elections[0].tenant == "tenant-a");
        check!(elections[0].cluster == "c1");
        check!(elections[0].replica == "r1");
        check!(elections[0].lease_timestamp_ms > 0);
    }

    #[tokio::test]
    async fn first_seen_ha_replica_persistence_failure_prevents_append() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(
            DistributorState::new(sink.clone())
                .with_ha_election_sink(Arc::new(FailingHaElectionSink)),
        );

        let response = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/push")
                    .header("Content-Type", "application/x-protobuf")
                    .header("Content-Encoding", "snappy")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::from(v1_body(vec![
                        label("__name__", "up"),
                        label("cluster", "c1"),
                        label("__replica__", "r1"),
                    ])))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status() == StatusCode::INTERNAL_SERVER_ERROR);
        assert!(sink.records().is_empty());
    }

    #[test]
    fn ha_election_records_round_trip_with_compacted_key() {
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };

        let encoded = record.encode().unwrap();

        assert!(HaElectionRecord::decode(&encoded).unwrap() == record);
        assert!(ha_election_compaction_key(&record) == Bytes::from_static(b"tenant-a\0c1"));
    }

    #[test]
    fn replay_ha_election_records_applies_tracker_and_reports_commit_offsets() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let records = vec![
            HaElectionConsumerRecord {
                topic: "ignored".to_string(),
                partition: PartitionIndex(0),
                offset: Offset(10),
                value: Some(record.encode().unwrap()),
            },
            HaElectionConsumerRecord {
                topic: HA_TRACKER_TOPIC.to_string(),
                partition: PartitionIndex(2),
                offset: Offset(20),
                value: Some(record.encode().unwrap()),
            },
        ];

        let result = replay_ha_election_records(&tracker, HA_TRACKER_TOPIC, &records).unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 2,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(2),
                        offset: Offset(21),
                    }],
                }
        );
        assert!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    #[tokio::test]
    async fn poll_ha_election_consumer_once_replays_records_and_commits_on_progress() {
        let tracker = HaTracker::default();
        let record = HaElectionRecord {
            tenant: "tenant-a".to_string(),
            cluster: "c1".to_string(),
            replica: "r1".to_string(),
            lease_timestamp_ms: 42_000,
        };
        let mut consumer = RecordingHaElectionConsumer {
            batches: vec![vec![consumer_record(
                HA_TRACKER_TOPIC,
                1,
                7,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result = poll_ha_election_consumer_once(
            &mut consumer,
            &tracker,
            HA_TRACKER_TOPIC,
            Duration::from_millis(1),
        )
        .await
        .unwrap();

        assert!(
            result
                == HaElectionReplayResult {
                    polled_records: 1,
                    replayed_records: 1,
                    committed_offsets: vec![HaElectionPartitionOffset {
                        partition: PartitionIndex(1),
                        offset: Offset(8),
                    }],
                }
        );
        check!(consumer.commit_calls == 1);
        check!(tracker.elected_replica("tenant-a", "c1") == Some("r1".to_string()));
    }

    #[test]
    fn wal_records_from_series_fans_out_float_samples() {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let series = [DecodedSeries {
            labels,
            samples: vec![DecodedSample::new(10, 1.0), DecodedSample::new(20, 2.0)],
            histograms: Vec::new(),
            exemplars: Vec::new(),
            metadata: None,
        }];

        let records = wal_records_from_series("tenant-a", &series);

        assert_eq!(
            (
                records.len(),
                records[0].tenant.as_str(),
                &records[0].labels,
                &records[1].labels,
                matches!(
                    records[0].payload,
                    SamplePayload::Float {
                        timestamp_ms: 10,
                        value: 1.0,
                        ..
                    }
                ),
                matches!(
                    records[1].payload,
                    SamplePayload::Float {
                        timestamp_ms: 20,
                        value: 2.0,
                        ..
                    }
                ),
            ),
            (
                2,
                "tenant-a",
                &records[1].labels,
                &records[1].labels,
                true,
                true
            )
        );
    }
}
