//! Distributor role: decode ingress doors, split profiles, and append WAL records.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, RawQuery};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Router, routing::post};
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use connectrpc_axum::{MakeServiceBuilder, MessageLimits};
use crabka_broker::throttle::TokenBucket;
use crabka_client_producer::{Header, Producer, ProducerRecord};
use crabka_pprof::PprofProfile;
use prost::Message;
use tokio::net::TcpListener;
use tracing::Instrument as _;

use crate::error::ProfilesError;
use crate::ingest::{
    RelabelConfig, TenantLimitConfig, apply_relabel, cap_session_id, decode_ingest_body,
    decode_otlp, decode_push, enforce_limits, parse_ingest_query, require_service_name,
    split_sample_types,
};
use crate::limits::{Limits, OverridesProvider};
use crate::metrics::ServiceMetrics;
use crate::wal::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
    WalSymbolSet, partition_key,
};
use crate::wire::pb;

/// Maximum decompressed/decoded request body the distributor will accept, in
/// bytes (16 MiB). This is wired into both the axum `DefaultBodyLimit` for the
/// raw `/ingest` + OTLP-HTTP doors and the Connect receive limit, and is kept
/// in lockstep with the per-request `max_decompressed` gunzip cap so a single
/// request can never balloon past this bound at any stage.
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Hard cap on the number of distinct tenants tracked in the per-tenant
/// `active_series` and `ingestion_buckets` maps. These maps are otherwise
/// unbounded: a caller minting fresh tenant ids on every request could grow
/// them without limit (a memory `DoS`). Once the cap is reached we evict an
/// arbitrary existing tenant before inserting a new one, so total memory stays
/// bounded. Combined with `X-Scope-OrgID` validation (which rejects malformed
/// ids) this closes the "mint unlimited tenants" vector. The cap is generous
/// (a few thousand) so legitimate multi-tenant deployments are unaffected;
/// evicting a real tenant merely resets its cardinality/rate accounting, which
/// is self-healing on the next request.
const MAX_TENANTS: usize = 4096;

#[async_trait::async_trait]
pub trait WalSink: Send + Sync {
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError>;
}

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
    async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
        let key = partition_key(&rec.tenant, rec.series_fingerprint());
        let value = rec.encode()?;
        // Inject the current span's W3C trace context (traceparent/tracestate)
        // as Kafka record headers so the block-builder consumer can re-parent
        // its block-build span onto this ingest span, stitching one distributed
        // trace across the WAL. Additive: empty when no active/sampled span.
        let headers = crabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|(k, v)| Header {
                key: k,
                value: Some(Bytes::from(v.into_bytes())),
            })
            .collect();
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: PROFILES_WAL_TOPIC.to_string(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
                headers,
                ..Default::default()
            })
            .await;
        ack.await
            .map_err(|err| ProfilesError::Produce(err.to_string()))?
            .map_err(|err| ProfilesError::Produce(err.to_string()))?;
        Ok(())
    }
}

pub struct DistributorState {
    pub sink: Arc<dyn WalSink>,
    pub limits: TenantLimitConfig,
    pub profile_overrides: OverridesProvider,
    pub active_series: Mutex<HashMap<String, BTreeSet<u64>>>,
    pub ingestion_buckets: Mutex<HashMap<String, Arc<TokenBucket>>>,
    pub relabel: Vec<RelabelConfig>,
    pub max_decompressed: usize,
    /// Prometheus metrics bundle. `record_ingest` is called at each ingest
    /// handler boundary; `record_wal_append_failure` fires at the WAL-append
    /// error site inside [`process_raw`].
    pub metrics: ServiceMetrics,
}

pub async fn process_raw(
    state: &DistributorState,
    tenant: &str,
    raws: Vec<crate::ingest::RawProfile>,
) -> Result<(), ProfilesError> {
    let mut pending = Vec::new();
    for mut raw in raws {
        if !apply_relabel(&mut raw.labels, &state.relabel) {
            continue;
        }
        require_service_name(&mut raw.labels);
        let limits = ingest_limits_for_tenant(state, tenant);
        cap_session_id(&mut raw.labels, limits.session_id_buckets);

        let symbols = extract_symbols(&raw.profile)?;
        for profile in split_sample_types(&raw)? {
            enforce_limits(&profile.labels, &limits)?;
            let rec = ProfileRecord {
                tenant: tenant.to_string(),
                labels: profile
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                profile_type: profile.profile_type,
                samples: profile
                    .samples
                    .into_iter()
                    .map(|sample| WalSample {
                        stacktrace_location_refs: sample.stacktrace_location_refs,
                        value: sample.value,
                        timestamp_ns: sample.timestamp_ns,
                        span_id: sample.span_id,
                        trace_id: sample.trace_id,
                    })
                    .collect(),
                symbols: symbols.clone(),
            };
            pending.push(rec);
        }
    }

    // Atomically check the max-series limit AND reserve the new fingerprints
    // under a single lock hold (see `enforce_and_reserve_max_series`). The
    // returned set lists fingerprints that were newly inserted by this call and
    // must be rolled back if the subsequent WAL append fails, so a rejected or
    // failed write never permanently inflates the tenant's series count.
    let reserved = enforce_and_reserve_max_series(state, tenant, &pending)?;
    if let Err(err) = enforce_ingestion_rate(state, tenant, pending.len()) {
        rollback_reserved_series(state, tenant, &reserved);
        return Err(err);
    }

    for rec in pending {
        if let Err(err) = state.sink.append(rec).await {
            // The WAL append failed: count it as a WAL/produce failure (distinct
            // from a 4xx client/validation rejection) and undo the series
            // reservation so a transient produce error doesn't leak into the
            // tenant's max-series budget.
            state.metrics.record_wal_append_failure();
            rollback_reserved_series(state, tenant, &reserved);
            return Err(err);
        }
    }

    Ok(())
}

fn enforce_ingestion_rate(
    state: &DistributorState,
    tenant: &str,
    profile_count: usize,
) -> Result<(), ProfilesError> {
    if profile_count == 0 || !state.profile_overrides.has_tenant_override(tenant) {
        return Ok(());
    }
    let limits = state.profile_overrides.for_tenant(tenant);
    if limits.ingestion_rate_profiles_per_sec <= 0.0 {
        return Ok(());
    }
    let requested = u64::try_from(profile_count).unwrap_or(u64::MAX);
    if limits.ingestion_burst_profiles > 0 && requested > limits.ingestion_burst_profiles {
        return Err(crate::limits::LimitError::IngestionRateExceeded {
            rate: limits.ingestion_rate_profiles_per_sec,
            observed: requested as f64,
        }
        .into());
    }

    let configured_rate = rate_tokens_per_sec(limits);
    let bucket = ingestion_bucket_for_tenant(state, tenant, configured_rate)?;
    let granted = bucket.try_consume(requested);
    if granted < requested {
        return Err(crate::limits::LimitError::IngestionRateExceeded {
            rate: limits.ingestion_rate_profiles_per_sec,
            observed: requested as f64,
        }
        .into());
    }
    Ok(())
}

fn rate_tokens_per_sec(limits: &Limits) -> u64 {
    let rate = limits.ingestion_rate_profiles_per_sec.ceil().max(1.0) as u64;
    if limits.ingestion_burst_profiles > 0 {
        rate.min(limits.ingestion_burst_profiles)
    } else {
        rate
    }
}

fn ingestion_bucket_for_tenant(
    state: &DistributorState,
    tenant: &str,
    rate: u64,
) -> Result<Arc<TokenBucket>, ProfilesError> {
    let mut buckets = state
        .ingestion_buckets
        .lock()
        .map_err(|_| ProfilesError::Internal("ingestion bucket lock poisoned".to_string()))?;
    // Bound per-tenant map growth (see `MAX_TENANTS`): evict an arbitrary
    // existing tenant before admitting a brand-new one once the cap is hit.
    if !buckets.contains_key(tenant) && buckets.len() >= MAX_TENANTS {
        evict_one_tenant(&mut buckets);
    }
    let bucket = buckets
        .entry(tenant.to_string())
        .or_insert_with(|| Arc::new(TokenBucket::new()))
        .clone();
    if bucket.rate() != rate {
        bucket.set_rate(rate);
    }
    Ok(bucket)
}

fn ingest_limits_for_tenant(state: &DistributorState, tenant: &str) -> crate::ingest::TenantLimits {
    let base = state.limits.for_tenant(tenant);
    if !state.profile_overrides.has_tenant_override(tenant) {
        return base.clone();
    }
    let overrides = state.profile_overrides.for_tenant(tenant);
    merge_ingest_limits(base, overrides)
}

fn merge_ingest_limits(
    base: &crate::ingest::TenantLimits,
    overrides: &Limits,
) -> crate::ingest::TenantLimits {
    crate::ingest::TenantLimits {
        max_label_name_len: usize::try_from(overrides.max_label_name_length)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(base.max_label_name_len),
        max_label_names_per_series: usize::try_from(overrides.max_label_names_per_series)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(base.max_label_names_per_series),
        max_label_value_len: usize::try_from(overrides.max_label_value_length)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(base.max_label_value_len),
        session_id_buckets: if overrides.max_session_id_cardinality > 0 {
            overrides.max_session_id_cardinality
        } else {
            base.session_id_buckets
        },
    }
}

/// Atomically test the per-tenant max-series limit and reserve the new
/// fingerprints under a single `active_series` lock hold.
///
/// The previous implementation cloned the tenant's set, dropped the lock,
/// tested the limit, and only inserted later under a *separate* lock — a
/// check-then-act race where two concurrent requests could each pass the test
/// and jointly exceed `max_series`. Holding the lock across both the test and
/// the insertion makes the operation atomic.
///
/// On success returns the subset of `records`' fingerprints that this call
/// actually inserted (i.e. were not already present). The caller must pass this
/// set to [`rollback_reserved_series`] if the subsequent WAL append fails, so a
/// rejected/failed write never permanently inflates the tenant's series count.
/// When `max_series` is unlimited (`0`) no reservation is made and an empty set
/// is returned (cardinality is not tracked, matching prior behaviour).
fn enforce_and_reserve_max_series(
    state: &DistributorState,
    tenant: &str,
    records: &[ProfileRecord],
) -> Result<Vec<u64>, ProfilesError> {
    let limit = state.profile_overrides.for_tenant(tenant).max_series;
    if limit == 0 {
        return Ok(Vec::new());
    }

    let mut active = state
        .active_series
        .lock()
        .map_err(|_| ProfilesError::Internal("active series lock poisoned".to_string()))?;

    // Bound per-tenant map growth: evict an arbitrary existing tenant before
    // admitting a brand-new one once the cap is hit (see `MAX_TENANTS`).
    if !active.contains_key(tenant) && active.len() >= MAX_TENANTS {
        evict_one_tenant(&mut active);
    }
    let entry = active.entry(tenant.to_string()).or_default();

    // Compute the DISTINCT fingerprints this request would newly add, without
    // mutating `entry` yet, so a rejection leaves the set untouched (no partial
    // writes on limit failure). Deduping here means a request that repeats the
    // same new fingerprint counts it once.
    let mut to_add: BTreeSet<u64> = BTreeSet::new();
    for rec in records {
        let fingerprint = rec.series_fingerprint();
        if !entry.contains(&fingerprint) {
            to_add.insert(fingerprint);
        }
    }
    let projected = entry.len() + to_add.len();
    if u64::try_from(projected).unwrap_or(u64::MAX) > limit {
        return Err(crate::limits::LimitError::MaxSeries {
            limit,
            observed: u64::try_from(projected).unwrap_or(u64::MAX),
        }
        .into());
    }

    // Within budget: reserve the new fingerprints and report exactly which ones
    // were inserted so the caller can roll them back on a later failure.
    for fingerprint in &to_add {
        entry.insert(*fingerprint);
    }
    Ok(to_add.into_iter().collect())
}

/// Undo a max-series reservation made by [`enforce_and_reserve_max_series`].
///
/// Only removes fingerprints this request inserted (tracked in `reserved`), so
/// concurrent requests that legitimately share a series are unaffected. A
/// poisoned lock here is best-effort: we cannot recover the set, so we log and
/// move on rather than panic.
fn rollback_reserved_series(state: &DistributorState, tenant: &str, reserved: &[u64]) {
    if reserved.is_empty() {
        return;
    }
    let Ok(mut active) = state.active_series.lock() else {
        tracing::error!(tenant, "active series lock poisoned during rollback");
        return;
    };
    if let Some(entry) = active.get_mut(tenant) {
        for fingerprint in reserved {
            entry.remove(fingerprint);
        }
        if entry.is_empty() {
            active.remove(tenant);
        }
    }
}

/// Evict one arbitrary tenant from a per-tenant map to keep its size bounded.
fn evict_one_tenant<V>(map: &mut HashMap<String, V>) {
    if let Some(victim) = map.keys().next().cloned() {
        map.remove(&victim);
    }
}

pub fn router(state: Arc<DistributorState>) -> Router {
    // Connect routes are built through `MakeServiceBuilder` (not the convenience
    // `build_connect()`) so we can attach a receive-size limit while still
    // applying the same defaults `build_connect()` would: the `ConnectLayer`
    // (protocol detection + per-request `ConnectContext`, without which proto
    // Connect clients like Alloy's `pyroscope.write` / OTLP exporters get
    // `application/json` responses and reject them) plus default gzip
    // decompression. The `receive_max_bytes` cap rejects oversized Connect
    // bodies (via `Content-Length`) before decompression, mirroring the raw
    // doors' body limit. See the matching fix in the querier router.
    let connect_limits = MessageLimits::new().receive_max_bytes(MAX_REQUEST_BODY_BYTES);
    let push_router = pb::push::v1::pusher_service_connect::PusherServiceBuilder::<()>::new()
        .push(push_handler)
        .build();
    let push = MakeServiceBuilder::new()
        .message_limits(connect_limits)
        .add_router(push_router)
        .build();
    let otlp_router =
        pb::otlp_profiles::profiles_service_connect::ProfilesServiceBuilder::<()>::new()
            .export(export_handler)
            .build();
    let otlp = MakeServiceBuilder::new()
        .message_limits(connect_limits)
        .add_router(otlp_router)
        .build();

    Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/v1development/profiles", post(otlp_http_handler))
        // Cap the raw `/ingest` + OTLP-HTTP request bodies at `MAX_REQUEST_BODY_BYTES`
        // (16 MiB). This bounds memory before the body is buffered and is kept
        // consistent with the per-request gunzip cap (`max_decompressed`).
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .merge(push)
        .merge(otlp)
        .layer(Extension(state))
}

pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%err, "profiles distributor server stopped with error");
        }
    });
    Ok(bound)
}

async fn push_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::push::v1::PushRequest>,
) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError> {
    let start = std::time::Instant::now();
    // No raw body is exposed by the Connect codec; the decoded message size is a
    // faithful proxy for the request payload bytes.
    let bytes = req.0.encoded_len() as u64;
    // ONE server span per ingest request (not per sample). `crabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        crabka.tenant = %ingest_span_tenant(&headers),
        crabka.ingest.samples = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let raws = decode_push(&req.0, state.max_decompressed)?;
        let items = raws.len() as u64;
        process_raw(&state, &tenant, raws).await?;
        Ok::<u64, ProfilesError>(items)
    }
    .instrument(ingest_span.clone())
    .await;
    let items = *result.as_ref().unwrap_or(&0);
    ingest_span.record("crabka.ingest.samples", items);
    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, items);
    }
    state
        .metrics
        .record_ingest(result.is_ok(), bytes, items, start.elapsed().as_secs_f64());
    result.map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}

async fn export_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::otlp_profiles::ExportProfilesServiceRequest>,
) -> Result<ConnectResponse<pb::otlp_profiles::ExportProfilesServiceResponse>, ConnectError> {
    let start = std::time::Instant::now();
    // No raw body is exposed by the Connect codec; the decoded message size is a
    // faithful proxy for the request payload bytes.
    let bytes = req.0.encoded_len() as u64;
    // ONE server span per ingest request (not per sample). `crabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        crabka.tenant = %ingest_span_tenant(&headers),
        crabka.ingest.samples = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let raws = decode_otlp(&req.0)?;
        let items = raws.len() as u64;
        process_raw(&state, &tenant, raws).await?;
        Ok::<u64, ProfilesError>(items)
    }
    .instrument(ingest_span.clone())
    .await;
    let items = *result.as_ref().unwrap_or(&0);
    ingest_span.record("crabka.ingest.samples", items);
    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, items);
    }
    state
        .metrics
        .record_ingest(result.is_ok(), bytes, items, start.elapsed().as_secs_f64());
    result.map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        },
    ))
}

async fn otlp_http_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    let mut items: u64 = 0;
    // ONE server span per ingest request (not per sample). `crabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        crabka.tenant = %ingest_span_tenant(&headers),
        crabka.ingest.samples = tracing::field::Empty,
        crabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let req = pb::otlp_profiles::ExportProfilesServiceRequest::decode(body)
            .map_err(|err| ProfilesError::Decode(format!("OTLP profiles decode: {err}")))?;
        let raws = decode_otlp(&req)?;
        items = raws.len() as u64;
        process_raw(&state, &tenant, raws).await?;
        Ok::<_, ProfilesError>(
            pb::otlp_profiles::ExportProfilesServiceResponse {
                partial_success: None,
            }
            .encode_to_vec(),
        )
    }
    .instrument(ingest_span.clone())
    .await;

    ingest_span.record("crabka.ingest.samples", items);
    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, items);
    }
    state
        .metrics
        .record_ingest(result.is_ok(), bytes, items, start.elapsed().as_secs_f64());
    match result {
        Ok(body) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/x-protobuf")],
            Bytes::from(body),
        )
            .into_response(),
        Err(err) => profiles_error_response(err),
    }
}

async fn ingest_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    // ONE server span per ingest request. The `/ingest` door carries exactly one
    // profile per request, so `crabka.ingest.samples` is fixed at 1.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        crabka.tenant = %ingest_span_tenant(&headers),
        crabka.ingest.samples = 1_u64,
        crabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant_from_headers(&headers)?;
        let query = parse_ingest_query(query.as_deref().unwrap_or(""))?;
        let content_type = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let raw = decode_ingest_body(&query, content_type, body, state.max_decompressed).await?;
        process_raw(&state, &tenant, vec![raw]).await
    }
    .instrument(ingest_span)
    .await;

    if let Ok(tenant) = tenant_from_headers(&headers) {
        state.metrics.record_ingest_samples(&tenant, 1);
    }
    // The `/ingest` door carries exactly one profile per request.
    state
        .metrics
        .record_ingest(result.is_ok(), bytes, 1, start.elapsed().as_secs_f64());
    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => profiles_error_response(err),
    }
}

/// Resolve and VALIDATE the tenant from the `X-Scope-OrgID` header.
///
/// Absent, empty, or non-UTF-8 headers default to `"anonymous"` (via
/// [`crate::tenant::tenant_from_header`]); a present, non-empty value is
/// validated against the Mimir/Pyroscope charset and rejected with
/// [`ProfilesError::Invalid`] (→ 400 / `invalid_argument`) if malformed. This
/// is what stops a caller from minting path-unsafe or unlimited junk tenant ids
/// at the ingest door.
fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ProfilesError> {
    let value = headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok());
    crate::tenant::tenant_from_header(value)
}

/// Best-effort tenant label for the ingest tracing span, read straight from the
/// `X-Scope-OrgID` header without validation (an unvalidated/absent header falls
/// back to `"unknown"`). This is only used to tag the span; the actual tenant
/// used for storage is resolved and validated separately by
/// [`tenant_from_headers`].
fn ingest_span_tenant(headers: &HeaderMap) -> String {
    headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

/// Generic, non-leaky message returned to clients for any server-side (5xx)
/// fault. The detailed error is logged server-side via `tracing` instead of
/// being echoed in the response body, so internal details (lock-poisoning,
/// WAL/produce/block internals) never reach an untrusted caller.
const INTERNAL_ERROR_MESSAGE: &str = "internal server error";

/// Returns the client-facing message for `err`.
///
/// For genuine client-input faults (4xx: bad format, decode/gunzip failures,
/// invalid requests, oversized payloads) the specific message is safe and
/// useful, so it is returned verbatim. For any 5xx/internal fault the detailed
/// error is logged server-side and a generic message is returned. `LimitError`
/// is handled by its own already-shaped projection at the call sites.
fn client_facing_message(err: &ProfilesError) -> String {
    if err.status_code() >= 500 {
        tracing::error!(error = %err, "profiles distributor internal error");
        INTERNAL_ERROR_MESSAGE.to_string()
    } else {
        err.to_string()
    }
}

fn connect_error(err: ProfilesError) -> ConnectError {
    if let ProfilesError::Limit(limit) = &err {
        return ConnectError::new(limit_connect_code(limit), err.to_string());
    }
    let code = match err.status_code() {
        400 | 415 => Code::InvalidArgument,
        _ => Code::Internal,
    };
    ConnectError::new(code, client_facing_message(&err))
}

fn profiles_error_response(err: ProfilesError) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    match err {
        ProfilesError::Limit(limit) => (
            status,
            axum::Json(serde_json::json!({
                "code": limit.connect_code(),
                "message": limit.message(),
            })),
        )
            .into_response(),
        other => {
            let message = client_facing_message(&other);
            (status, message).into_response()
        }
    }
}

fn limit_connect_code(err: &crate::limits::LimitError) -> Code {
    match err.connect_code() {
        "resource_exhausted" => Code::ResourceExhausted,
        "invalid_argument" => Code::InvalidArgument,
        _ => Code::Internal,
    }
}

fn extract_symbols(profile: &PprofProfile) -> Result<WalSymbolSet, ProfilesError> {
    let inner = profile.inner();
    let function_refs = inner
        .function
        .iter()
        .enumerate()
        .map(|(idx, function)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("function index does not fit u32: {err}"))
            })?;
            Ok((function.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    let mapping_refs = inner
        .mapping
        .iter()
        .enumerate()
        .map(|(idx, mapping)| {
            let idx = u32::try_from(idx).map_err(|err| {
                ProfilesError::Decode(format!("mapping index does not fit u32: {err}"))
            })?;
            Ok((mapping.id, idx))
        })
        .collect::<Result<HashMap<_, _>, ProfilesError>>()?;
    Ok(WalSymbolSet {
        strings: inner.string_table.clone(),
        functions: inner
            .function
            .iter()
            .map(|function| {
                Ok(WalFunction {
                    name: u32_from_i64(function.name, "function.name")?,
                    system_name: u32_from_i64(function.system_name, "function.system_name")?,
                    filename: u32_from_i64(function.filename, "function.filename")?,
                    start_line: function.start_line,
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
        locations: inner
            .location
            .iter()
            .map(|location| {
                Ok(WalLocation {
                    address: location.address,
                    mapping_id: normalize_optional_pprof_id(
                        location.mapping_id,
                        &mapping_refs,
                        "location.mapping_id",
                    )?,
                    lines: location
                        .line
                        .iter()
                        .map(|line| {
                            Ok((
                                normalize_required_pprof_id(
                                    line.function_id,
                                    &function_refs,
                                    "line.function_id",
                                )?,
                                line.line,
                            ))
                        })
                        .collect::<Result<Vec<_>, ProfilesError>>()?,
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
        mappings: inner
            .mapping
            .iter()
            .map(|mapping| {
                Ok(WalMapping {
                    memory_start: mapping.memory_start,
                    memory_limit: mapping.memory_limit,
                    file_offset: mapping.file_offset,
                    filename: u32_from_i64(mapping.filename, "mapping.filename")?,
                    build_id: u32_from_i64(mapping.build_id, "mapping.build_id")?,
                    // Carry each pprof symbolization flag through independently;
                    // they are distinct signals (functions vs filenames vs line
                    // numbers vs inline frames) and must not be collapsed.
                    has_functions: mapping.has_functions,
                    has_filenames: mapping.has_filenames,
                    has_line_numbers: mapping.has_line_numbers,
                    has_inline_frames: mapping.has_inline_frames,
                })
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?,
    })
}

fn normalize_required_pprof_id(
    id: u64,
    refs: &HashMap<u64, u32>,
    field: &str,
) -> Result<u32, ProfilesError> {
    refs.get(&id)
        .copied()
        .ok_or_else(|| ProfilesError::Decode(format!("{field} references missing id {id}")))
}

fn normalize_optional_pprof_id(
    id: u64,
    refs: &HashMap<u64, u32>,
    field: &str,
) -> Result<u32, ProfilesError> {
    if id == 0 {
        return Ok(0);
    }
    normalize_required_pprof_id(id, refs, field)
}

fn u32_from_i64(value: i64, field: &str) -> Result<u32, ProfilesError> {
    u32::try_from(value)
        .map_err(|err| ProfilesError::Decode(format!("{field} does not fit u32: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use prost::Message;

    use super::*;

    /// `ingest_span_tenant` reads the `X-Scope-OrgID` header, returning its
    /// value verbatim when present and non-empty, and `"unknown"` when the
    /// header is missing or empty.
    #[test]
    fn ingest_span_tenant_reads_scope_orgid_header() {
        let mut present = HeaderMap::new();
        present.insert("x-scope-orgid", "acme".parse().unwrap());
        assert!(ingest_span_tenant(&present) == "acme");

        let missing = HeaderMap::new();
        assert!(ingest_span_tenant(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("x-scope-orgid", "".parse().unwrap());
        assert!(ingest_span_tenant(&empty) == "unknown");
    }

    use crate::error::ProfilesError;
    use crate::ingest::{RelabelAction, RelabelConfig, TenantLimitConfig, TenantLimits};
    use crate::limits::OverridesProvider;
    use crate::wal::ProfileRecord;

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<ProfileRecord>>);

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
            self.0.lock().unwrap().push(rec);
            Ok(())
        }
    }

    /// A sink whose `append` always fails, to exercise the WAL-failure
    /// reservation-rollback path.
    struct FailingSink;

    #[async_trait::async_trait]
    impl WalSink for FailingSink {
        async fn append(&self, _rec: ProfileRecord) -> Result<(), ProfilesError> {
            Err(ProfilesError::Produce(
                "simulated produce failure".to_string(),
            ))
        }
    }

    fn state_with(sink: Arc<RecordingSink>) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(Default::default()),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        })
    }

    fn otlp_export_request() -> pb::otlp_profiles::ExportProfilesServiceRequest {
        use pb::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value::Value};
        use pb::opentelemetry::proto::resource::v1::Resource;
        use pb::otlp_profiles::{
            Function, Line, Location, Profile, ProfilesDictionary, ResourceProfiles, Sample,
            ScopeProfiles, Stack, ValueType,
        };

        let dictionary = ProfilesDictionary {
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "main".to_string(),
            ],
            function_table: vec![Function {
                name_strindex: 3,
                ..Default::default()
            }],
            location_table: vec![Location {
                address: 0x40,
                lines: vec![Line {
                    function_index: 0,
                    line: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            stack_table: vec![Stack {
                location_indices: vec![0],
            }],
            ..Default::default()
        };
        let profile = Profile {
            sample_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            period_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            samples: vec![Sample {
                stack_index: 0,
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_000],
                ..Default::default()
            }],
            time_unix_nano: 1_700_000_000_000_000_000,
            ..Default::default()
        };

        pb::otlp_profiles::ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("api".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![profile],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            dictionary: Some(dictionary),
        }
    }

    #[tokio::test]
    async fn push_splits_and_appends_one_record_per_sample_type() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let raws = vec![crate::wire::test_fixtures::raw_profile_2types()];

        process_raw(&state, "tenant-a", raws).await.unwrap();

        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 2);
        assert!(recs.iter().all(|rec| rec.tenant == "tenant-a"));
        assert!(
            recs.iter()
                .all(|rec| rec.labels.iter().any(|(name, _)| name == "service_name"))
        );
    }

    #[tokio::test]
    async fn push_normalizes_pprof_symbol_ids_to_wal_indices() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");
        let profile = PprofProfile::from(crabka_pprof::proto::Profile {
            sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![crabka_pprof::proto::Sample {
                location_id: vec![2],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![
                crabka_pprof::proto::Location {
                    id: 1,
                    line: vec![crabka_pprof::proto::Line {
                        function_id: 1,
                        line: 10,
                        column: 0,
                    }],
                    ..Default::default()
                },
                crabka_pprof::proto::Location {
                    id: 2,
                    line: vec![crabka_pprof::proto::Line {
                        function_id: 2,
                        line: 20,
                        column: 0,
                    }],
                    ..Default::default()
                },
            ],
            function: vec![
                crabka_pprof::proto::Function {
                    id: 1,
                    name: 3,
                    system_name: 3,
                    filename: 5,
                    start_line: 1,
                },
                crabka_pprof::proto::Function {
                    id: 2,
                    name: 4,
                    system_name: 4,
                    filename: 5,
                    start_line: 2,
                },
            ],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "first".to_string(),
                "second".to_string(),
                "main.go".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
            ..Default::default()
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::ingest::RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            }],
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        assert!(recs[0].samples[0].stacktrace_location_refs == vec![1]);
        assert!(recs[0].symbols.locations[1].lines[0].0 == 1);
    }

    #[tokio::test]
    async fn relabel_drop_skips_the_series() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(Default::default()),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![RelabelConfig {
                source_labels: vec!["__name__".to_string()],
                regex: "process_cpu".to_string(),
                target_label: String::new(),
                replacement: String::new(),
                action: RelabelAction::Drop,
            }],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });
        let raws = vec![crate::wire::test_fixtures::raw_profile_cpu()];

        process_raw(&state, "t", raws).await.unwrap();

        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tenant_specific_limits_are_enforced() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default().with_tenant_limits(
                "tenant-a",
                TenantLimits {
                    max_label_value_len: 3,
                    ..Default::default()
                },
            ),
            profile_overrides: OverridesProvider::new(Default::default()),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("value exceeds"));
    }

    #[tokio::test]
    async fn label_count_limit_is_enforced_after_profile_type_split() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default().with_tenant_limits(
                "tenant-a",
                TenantLimits {
                    max_label_names_per_series: 5,
                    ..Default::default()
                },
            ),
            profile_overrides: OverridesProvider::new(Default::default()),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("too many label names"));
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_drive_ingest_label_limits() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    max_label_value_length: 3
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("value exceeds"));
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_max_series_without_partial_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    max_series: 1
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("max series exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_ingestion_burst_without_partial_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 100
    ingestion_burst_profiles: 1
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("ingestion rate exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_ingestion_rate_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 1
    ingestion_burst_profiles: 1
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();
        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("ingestion rate exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().len() == 2);
    }

    #[test]
    fn limit_errors_map_to_resource_exhausted_connect_code() {
        let err = connect_error(
            crate::limits::LimitError::MaxSeries {
                limit: 1,
                observed: 2,
            }
            .into(),
        );

        assert!(err.code() == Code::ResourceExhausted);
        assert!(
            err.message()
                .is_some_and(|message| message.contains("max series exceeded"))
        );
    }

    #[tokio::test]
    async fn otlp_http_profiles_path_appends_records() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let body = otlp_export_request().encode_to_vec();

        let response = reqwest::Client::new()
            .post(format!("http://{bound}/v1development/profiles"))
            .header("content-type", "application/x-protobuf")
            .header("x-scope-orgid", "tenant-a")
            .body(body)
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        assert!(recs[0].tenant == "tenant-a");
        assert!(recs[0].labels.iter().any(|(name, value)| {
            name == "__profile_type__" && value == "samples:samples:count:samples:count"
        }));
    }

    #[tokio::test]
    async fn legacy_ingest_accepts_plain_folded_groups_body() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .header("x-scope-orgid", "tenant-a")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        assert!(recs[0].tenant == "tenant-a");
        assert!(recs[0].labels.iter().any(|(name, value)| {
            name == "__profile_type__" && value == "myapp:samples:samples:samples:samples"
        }));
        assert!(
            recs[0]
                .labels
                .iter()
                .any(|(name, value)| name == "service_name" && value == "api")
        );
        assert!(recs[0].samples.len() == 1);
        assert!(recs[0].samples[0].value == 3);
        assert!(recs[0].samples[0].timestamp_ns == 1_700_000_000_000_000_000);
    }

    #[tokio::test]
    async fn legacy_ingest_limit_errors_return_connect_shaped_json() {
        let response = profiles_error_response(
            crate::limits::LimitError::MaxSeries {
                limit: 1,
                observed: 2,
            }
            .into(),
        );
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert!(status == StatusCode::TOO_MANY_REQUESTS);
        assert!(json.get("code").and_then(serde_json::Value::as_str) == Some("resource_exhausted"));
        assert!(
            json.get("message")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| message.contains("max series exceeded"))
        );
    }

    // C1: the ingest door must validate the `X-Scope-OrgID` tenant.
    #[tokio::test]
    async fn ingest_rejects_path_unsafe_tenant_with_400() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .header("x-scope-orgid", "../escape")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST, "{response:?}");
        // A rejected tenant must not produce any WAL records.
        assert!(sink.0.lock().unwrap().is_empty());
    }

    // C1: an absent header still defaults to the anonymous tenant.
    #[tokio::test]
    async fn ingest_without_tenant_header_defaults_to_anonymous() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        assert!(recs[0].tenant == "anonymous");
    }

    #[test]
    fn tenant_from_headers_validates_and_defaults() {
        use axum::http::HeaderValue;

        let mut headers = HeaderMap::new();
        assert!(tenant_from_headers(&headers).unwrap() == "anonymous");

        headers.insert("x-scope-orgid", HeaderValue::from_static("tenant-a"));
        assert!(tenant_from_headers(&headers).unwrap() == "tenant-a");

        headers.insert("x-scope-orgid", HeaderValue::from_static("a/b"));
        assert!(tenant_from_headers(&headers).is_err());
    }

    // #3: when the WAL append fails, the max-series reservation is rolled back
    // so a failed write does not permanently consume the tenant's budget.
    #[tokio::test]
    async fn wal_append_failure_rolls_back_max_series_reservation() {
        let state = Arc::new(DistributorState {
            sink: Arc::new(FailingSink),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    max_series: 100
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ProfilesError::Produce(_)), "{err}");

        // The reservation must have been rolled back: no leftover fingerprints.
        let active = state.active_series.lock().unwrap();
        assert!(
            active
                .get("tenant-a")
                .map_or(0, std::collections::BTreeSet::len)
                == 0
        );
    }

    // #3: a max-series rejection leaves the tracked set untouched (no partial
    // reservation), so a subsequent within-budget write still succeeds.
    #[tokio::test]
    async fn max_series_rejection_does_not_reserve() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r#"
overrides:
  tenant-a:
    max_series: 1
"#,
            )
            .unwrap(),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        // Two distinct series in one request exceed the cap of 1 and are rejected.
        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("max series exceeded"), "{err}");

        // Nothing was reserved, so a single-series write afterwards succeeds.
        process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();
        assert!(sink.0.lock().unwrap().len() == 1);
    }

    // #4: the per-tenant maps are bounded; admitting tenant N+1 past the cap
    // evicts an existing tenant rather than growing without limit.
    #[test]
    fn evict_one_tenant_bounds_map_growth() {
        let mut map: HashMap<String, usize> = HashMap::new();
        for idx in 0..MAX_TENANTS {
            map.insert(format!("tenant-{idx}"), idx);
        }
        assert!(map.len() == MAX_TENANTS);

        // Simulate the admission guard: evict before inserting a new tenant.
        if !map.contains_key("tenant-new") && map.len() >= MAX_TENANTS {
            evict_one_tenant(&mut map);
        }
        map.insert("tenant-new".to_string(), 0);

        assert!(map.len() == MAX_TENANTS);
        assert!(map.contains_key("tenant-new"));
    }

    #[tokio::test]
    async fn ingestion_buckets_map_is_capped() {
        let sink = Arc::new(RecordingSink::default());
        // Build an overrides provider that gives EVERY tenant a finite rate, so
        // each distinct tenant allocates a bucket. We assert the map never grows
        // past `MAX_TENANTS`.
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(crate::limits::Limits {
                ingestion_rate_profiles_per_sec: 1000.0,
                ingestion_burst_profiles: 1000,
                ..Default::default()
            }),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
            metrics: ServiceMetrics::new(),
        });

        for idx in 0..(MAX_TENANTS + 50) {
            // `has_tenant_override` is false for the default provider, so the
            // rate path is skipped; allocate buckets directly to exercise the cap.
            let _ = ingestion_bucket_for_tenant(&state, &format!("tenant-{idx}"), 10);
        }

        let buckets = state.ingestion_buckets.lock().unwrap();
        assert!(buckets.len() <= MAX_TENANTS, "{}", buckets.len());
    }

    // #7: a 5xx/internal error returns a GENERIC body, not the detailed text.
    #[tokio::test]
    async fn internal_error_response_is_generic() {
        let response = profiles_error_response(ProfilesError::Produce(
            "kafka broker is on fire".to_string(),
        ));
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);

        assert!(status == StatusCode::INTERNAL_SERVER_ERROR);
        assert!(text == INTERNAL_ERROR_MESSAGE, "leaked detail: {text}");
        assert!(!text.contains("kafka"), "leaked detail: {text}");
    }

    // #7: a 4xx client-input error keeps its specific, useful message.
    #[test]
    fn client_input_error_keeps_specific_message() {
        let message = client_facing_message(&ProfilesError::Invalid("bad query param".to_string()));
        assert!(message.contains("bad query param"), "{message}");
    }

    // #7: a poisoned lock is now an Internal/500 with a generic message, not a 400.
    #[test]
    fn poisoned_lock_maps_to_internal_500() {
        let err = ProfilesError::Internal("active series lock poisoned".to_string());
        assert!(err.status_code() == 500);

        let connect = connect_error(ProfilesError::Internal("secret detail".to_string()));
        assert!(connect.code() == Code::Internal);
        assert!(
            connect
                .message()
                .is_some_and(|message| message == INTERNAL_ERROR_MESSAGE)
        );
    }

    // #11: mapping symbolization flags flow through independently.
    #[tokio::test]
    async fn mapping_symbolization_flags_are_populated_independently() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut labels = crabka_blockstore::Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");
        let profile = PprofProfile::from(crabka_pprof::proto::Profile {
            sample_type: vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![crabka_pprof::proto::Sample {
                location_id: vec![1],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![crabka_pprof::proto::Location {
                id: 1,
                mapping_id: 1,
                line: vec![crabka_pprof::proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                ..Default::default()
            }],
            function: vec![crabka_pprof::proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                filename: 4,
                start_line: 1,
            }],
            mapping: vec![crabka_pprof::proto::Mapping {
                id: 1,
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0,
                filename: 5,
                build_id: 0,
                // Deliberately mixed: functions+line numbers symbolized, but no
                // filenames and no inline frames. A correct mapping must NOT
                // collapse these onto `has_functions`.
                has_functions: true,
                has_filenames: false,
                has_line_numbers: true,
                has_inline_frames: false,
            }],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "main".to_string(),
                "main.go".to_string(),
                "bin".to_string(),
            ],
            period_type: Some(crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
            ..Default::default()
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::ingest::RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            }],
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        let mapping = &recs[0].symbols.mappings[0];
        assert!(mapping.has_functions);
        assert!(!mapping.has_filenames);
        assert!(mapping.has_line_numbers);
        assert!(!mapping.has_inline_frames);
    }
}
