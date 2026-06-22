//! Distributor role: decode ingress doors, split profiles, and append WAL records.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Router, routing::post};
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use crabka_broker::throttle::TokenBucket;
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_pprof::PprofProfile;
use prost::Message;
use tokio::net::TcpListener;

use crate::error::ProfilesError;
use crate::ingest::{
    RelabelConfig, TenantLimitConfig, apply_relabel, cap_session_id, decode_ingest_body,
    decode_otlp, decode_push, enforce_limits, parse_ingest_query, require_service_name,
    split_sample_types,
};
use crate::limits::{Limits, OverridesProvider};
use crate::wal::{
    PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
    WalSymbolSet, partition_key,
};
use crate::wire::pb;

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
        let ack = self
            .producer
            .send(ProducerRecord {
                topic: PROFILES_WAL_TOPIC.to_string(),
                partition: None,
                key: Some(key),
                value: Some(Bytes::from(value)),
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
                    .map(|(name, value)| (name.to_string(), value.to_string()))
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

    enforce_max_series(state, tenant, &pending)?;
    enforce_ingestion_rate(state, tenant, pending.len())?;
    for rec in pending {
        let fingerprint = rec.series_fingerprint();
        state.sink.append(rec).await?;
        record_active_series(state, tenant, fingerprint)?;
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
        .map_err(|_| ProfilesError::Invalid("ingestion bucket lock poisoned".to_string()))?;
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

fn enforce_max_series(
    state: &DistributorState,
    tenant: &str,
    records: &[ProfileRecord],
) -> Result<(), ProfilesError> {
    let limit = state.profile_overrides.for_tenant(tenant).max_series;
    if limit == 0 {
        return Ok(());
    }
    let mut observed = {
        let active = state
            .active_series
            .lock()
            .map_err(|_| ProfilesError::Invalid("active series lock poisoned".to_string()))?;
        active.get(tenant).cloned().unwrap_or_default()
    };
    for rec in records {
        observed.insert(rec.series_fingerprint());
    }
    if u64::try_from(observed.len()).unwrap_or(u64::MAX) > limit {
        return Err(crate::limits::LimitError::MaxSeries {
            limit,
            observed: u64::try_from(observed.len()).unwrap_or(u64::MAX),
        }
        .into());
    }
    Ok(())
}

fn record_active_series(
    state: &DistributorState,
    tenant: &str,
    fingerprint: u64,
) -> Result<(), ProfilesError> {
    let mut active = state
        .active_series
        .lock()
        .map_err(|_| ProfilesError::Invalid("active series lock poisoned".to_string()))?;
    active
        .entry(tenant.to_string())
        .or_default()
        .insert(fingerprint);
    Ok(())
}

pub fn router(state: Arc<DistributorState>) -> Router {
    // `build_connect()` applies the `ConnectLayer` (protocol detection + per-request
    // `ConnectContext`); plain `.build()` omits it, so proto Connect clients (Alloy's
    // `pyroscope.write`, OTLP exporters) would receive `application/json` responses and reject
    // them. See the matching fix in the querier router.
    let push = pb::push::v1::pusher_service_connect::PusherServiceBuilder::<()>::new()
        .push(push_handler)
        .build_connect();
    let otlp = pb::otlp_profiles::profiles_service_connect::ProfilesServiceBuilder::<()>::new()
        .export(export_handler)
        .build_connect();

    Router::new()
        .route("/ingest", post(ingest_handler))
        .route("/v1development/profiles", post(otlp_http_handler))
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
    let raws = decode_push(&req.0, state.max_decompressed).map_err(connect_error)?;
    let tenant = tenant_from_headers(&headers);
    process_raw(&state, &tenant, raws)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}

async fn export_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    headers: HeaderMap,
    req: ConnectRequest<pb::otlp_profiles::ExportProfilesServiceRequest>,
) -> Result<ConnectResponse<pb::otlp_profiles::ExportProfilesServiceResponse>, ConnectError> {
    let raws = decode_otlp(&req.0).map_err(connect_error)?;
    let tenant = tenant_from_headers(&headers);
    process_raw(&state, &tenant, raws)
        .await
        .map_err(connect_error)?;
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
    let tenant = tenant_from_headers(&headers);
    let result = async {
        let req = pb::otlp_profiles::ExportProfilesServiceRequest::decode(body)
            .map_err(|err| ProfilesError::Decode(format!("OTLP profiles decode: {err}")))?;
        let raws = decode_otlp(&req)?;
        process_raw(&state, &tenant, raws).await?;
        Ok::<_, ProfilesError>(
            pb::otlp_profiles::ExportProfilesServiceResponse {
                partial_success: None,
            }
            .encode_to_vec(),
        )
    }
    .await;

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
    let tenant = tenant_from_headers(&headers);
    let result = async {
        let query = parse_ingest_query(query.as_deref().unwrap_or(""))?;
        let content_type = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok());
        let raw = decode_ingest_body(&query, content_type, body, state.max_decompressed).await?;
        process_raw(&state, &tenant, vec![raw]).await
    }
    .await;

    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => profiles_error_response(err),
    }
}

fn tenant_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-scope-orgid")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

fn connect_error(err: ProfilesError) -> ConnectError {
    let code = match &err {
        ProfilesError::Limit(limit) => limit_connect_code(limit),
        _ => match err.status_code() {
            400 | 415 => Code::InvalidArgument,
            _ => Code::Internal,
        },
    };
    ConnectError::new(code, err.to_string())
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
        other => (status, other.to_string()).into_response(),
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
                    has_functions: mapping.has_functions,
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

    fn state_with(sink: Arc<RecordingSink>) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(Default::default()),
            active_series: Default::default(),
            ingestion_buckets: Default::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
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
        let mut labels = crabka_blockstore::SeriesLabels::new();
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
}
