//! Distributor role: decode ingress doors, split profiles, and append WAL records.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Router, routing::post};
use connectrpc_axum::message::{Code, ConnectError, ConnectRequest, ConnectResponse};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_pprof::PprofProfile;
use tokio::net::TcpListener;

use crate::error::ProfilesError;
use crate::ingest::{
    RelabelConfig, TenantLimits, apply_relabel, cap_session_id, decode_ingest_multipart,
    decode_otlp, decode_push, enforce_limits, parse_ingest_query, require_service_name,
    split_sample_types,
};
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
    pub limits: TenantLimits,
    pub relabel: Vec<RelabelConfig>,
    pub max_decompressed: usize,
}

pub async fn process_raw(
    state: &DistributorState,
    tenant: &str,
    raws: Vec<crate::ingest::RawProfile>,
) -> Result<(), ProfilesError> {
    for mut raw in raws {
        if !apply_relabel(&mut raw.labels, &state.relabel) {
            continue;
        }
        require_service_name(&mut raw.labels);
        cap_session_id(&mut raw.labels, state.limits.session_id_buckets);
        enforce_limits(&raw.labels, &state.limits)?;

        let symbols = extract_symbols(&raw.profile)?;
        for profile in split_sample_types(&raw)? {
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
            state.sink.append(rec).await?;
        }
    }

    Ok(())
}

pub fn router(state: Arc<DistributorState>) -> Router {
    let push = pb::push::v1::pusher_service_connect::PusherServiceBuilder::<()>::new()
        .push(push_handler)
        .build();
    let otlp = pb::otlp_profiles::profiles_service_connect::ProfilesServiceBuilder::<()>::new()
        .export(export_handler)
        .build();

    Router::new()
        .route("/ingest", post(ingest_handler))
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
    req: ConnectRequest<pb::push::v1::PushRequest>,
) -> Result<ConnectResponse<pb::push::v1::PushResponse>, ConnectError> {
    let raws = decode_push(&req.0, state.max_decompressed).map_err(connect_error)?;
    // TODO(slice4-tenant-required): this ConnectRequest wrapper does not expose
    // headers; add middleware to carry X-Scope-OrgID into extensions.
    process_raw(&state, "anonymous", raws)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(pb::push::v1::PushResponse {}))
}

async fn export_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    req: ConnectRequest<pb::otlp_profiles::ExportProfilesServiceRequest>,
) -> Result<ConnectResponse<pb::otlp_profiles::ExportProfilesServiceResponse>, ConnectError> {
    let raws = decode_otlp(&req.0).map_err(connect_error)?;
    // TODO(slice4-tenant-required): thread X-Scope-OrgID through Connect.
    process_raw(&state, "anonymous", raws)
        .await
        .map_err(connect_error)?;
    Ok(ConnectResponse::new(
        pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        },
    ))
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
            .and_then(|value| value.to_str().ok())
            .ok_or_else(|| ProfilesError::Invalid("missing content-type".to_string()))?;
        let raw =
            decode_ingest_multipart(&query, content_type, body, state.max_decompressed).await?;
        process_raw(&state, &tenant, vec![raw]).await
    }
    .await;

    match result {
        Ok(()) => StatusCode::OK.into_response(),
        Err(err) => (
            StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            err.to_string(),
        )
            .into_response(),
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
    let code = match err.status_code() {
        400 | 415 => Code::InvalidArgument,
        _ => Code::Internal,
    };
    ConnectError::new(code, err.to_string())
}

fn extract_symbols(profile: &PprofProfile) -> Result<WalSymbolSet, ProfilesError> {
    let inner = profile.inner();
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
                    mapping_id: u32_from_u64(location.mapping_id, "location.mapping_id")?,
                    lines: location
                        .line
                        .iter()
                        .map(|line| {
                            Ok((
                                u32_from_u64(line.function_id, "line.function_id")?,
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

fn u32_from_i64(value: i64, field: &str) -> Result<u32, ProfilesError> {
    u32::try_from(value)
        .map_err(|err| ProfilesError::Decode(format!("{field} does not fit u32: {err}")))
}

fn u32_from_u64(value: u64, field: &str) -> Result<u32, ProfilesError> {
    u32::try_from(value)
        .map_err(|err| ProfilesError::Decode(format!("{field} does not fit u32: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;

    use super::*;
    use crate::error::ProfilesError;
    use crate::ingest::{RelabelAction, RelabelConfig, TenantLimits};
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
            limits: TenantLimits::default(),
            relabel: vec![],
            max_decompressed: 1 << 24,
        })
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
    async fn relabel_drop_skips_the_series() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimits::default(),
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
}
