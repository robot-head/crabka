//! Prometheus/Mimir-compatible HTTP query API adapter.

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use crabka_metrics::{LimitError, OverridesProvider, wire::WireError};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    EngineOpts, MetricStore, PromqlEngine, PromqlError,
    metrics::ServiceMetrics,
    query_frontend::{QueryFrontendCache, QueryFrontendOptions, RangeQueryCache},
    ruler::{RulerAlertStateRecord, RulerGroupState, RulerGroupStateRecord},
};

mod alert_templates;
mod cardinality;
mod discovery;
mod metadata;
mod parse;
mod query;
mod remote_read;
mod request;
mod response;
mod rules;
mod status;

pub(crate) use alert_templates::expand_alert_template;
use cardinality::{
    cardinality_active_series, cardinality_active_series_post, cardinality_label_names,
    cardinality_label_names_post, cardinality_label_values, cardinality_label_values_post,
};
use discovery::{label_values, label_values_post, labels, labels_post, series, series_post};
use metadata::{metadata, target_metadata};
use parse::{format_query, format_query_post, parse_query, parse_query_post};
use query::{
    query, query_exemplars, query_exemplars_post, query_post, query_range, query_range_post,
};
use remote_read::remote_read;
use request::{
    CardinalityParams, DiscoveryParams, apply_limit, apply_result_limit, check_range_resolution,
    discovery_matchers, discovery_window, duration_ms, enforce_sample_count,
    enforce_selected_series_limit, optional_timestamp_ms, parse_cardinality_form,
    parse_cardinality_params, parse_discovery_form, parse_discovery_params, parse_limit_parameter,
    required_form_param, selector_matchers, tenant_from_headers, timestamp_ms, unix_now_ms,
    validate_timestamp_range,
};
pub(crate) use response::format_sample_value;
use response::{
    active_series_response, cardinality_label_names_response, cardinality_label_values_response,
    exemplar_key, exemplars_json, labels_json, labels_key, sample_string, success_data_response,
    success_response,
};
use rules::{
    alerts, delete_ruler_config_group, delete_ruler_config_namespace, ruler_config_group,
    ruler_config_namespace, ruler_config_rules, rules, set_ruler_config_group,
};
use status::{
    alertmanagers, build_info, runtime_info, scrape_pools, status_config, status_flags, targets,
    tsdb_blocks, tsdb_status, wal_replay_status,
};

/// Shared state for the Prometheus HTTP query API.
pub struct PrometheusApiState<S: MetricStore> {
    engine: PromqlEngine<S>,
    engine_opts: EngineOpts,
    store: Arc<S>,
    ruler_rules: RwLock<RulerRuleStore>,
    ruler_alerts: RwLock<RulerAlertStateStore>,
    ruler_group_state: RwLock<RulerGroupState>,
    ruler_evaluation_time_ms: RwLock<i64>,
    query_frontend: Option<QueryFrontendState>,
    query_limits: Option<OverridesProvider>,
    query_gate: Option<Arc<Semaphore>>,
    metrics: Option<ServiceMetrics>,
    start_time: SystemTime,
}

struct QueryFrontendState {
    opts: QueryFrontendOptions,
    cache: Arc<dyn RangeQueryCache>,
}

/// RAII guard that keeps `active_queries` incremented while an in-flight query
/// is executing, decrementing it on drop (covering early returns and panics).
/// A no-op when no metrics bundle is configured.
struct ActiveQueryGuard {
    metrics: Option<ServiceMetrics>,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.query_finished();
        }
    }
}

type RulerRuleStore = BTreeMap<String, BTreeMap<String, BTreeMap<String, serde_yaml::Value>>>;
type RulerAlertStateStore = BTreeMap<AlertStateKey, i64>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AlertStateKey {
    tenant: String,
    rule_id: String,
    labels: BTreeMap<String, String>,
}

impl<S: MetricStore> PrometheusApiState<S> {
    #[must_use]
    pub fn new(store: Arc<S>, opts: EngineOpts) -> Self {
        Self {
            engine: PromqlEngine::new(Arc::clone(&store), opts),
            engine_opts: opts,
            store,
            ruler_rules: RwLock::new(BTreeMap::new()),
            ruler_alerts: RwLock::new(BTreeMap::new()),
            ruler_group_state: RwLock::new(RulerGroupState::default()),
            ruler_evaluation_time_ms: RwLock::new(0),
            query_frontend: None,
            query_limits: None,
            query_gate: None,
            metrics: None,
            start_time: SystemTime::now(),
        }
    }

    #[must_use]
    pub fn with_query_limits(mut self, limits: OverridesProvider) -> Self {
        self.query_limits = Some(limits);
        self
    }

    #[must_use]
    pub fn with_max_concurrent_queries(mut self, max_concurrent_queries: usize) -> Self {
        self.query_gate = Some(Arc::new(Semaphore::new(max_concurrent_queries.max(1))));
        self
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: ServiceMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Record one query request outcome on `route`, if a metrics bundle is
    /// configured. No-op otherwise.
    fn record_query(&self, route: &str, ok: bool, secs: f64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_query(route, ok, secs);
        }
    }

    /// Record one `PromQL` engine evaluation (`query_type` = `"instant"` /
    /// `"range"`) — its latency and, when `!ok`, an error increment. No-op when
    /// no metrics bundle is configured.
    fn record_eval(&self, query_type: &str, ok: bool, secs: f64) {
        if let Some(metrics) = &self.metrics {
            metrics.record_eval(query_type, ok, secs);
        }
    }

    /// Bump the in-flight-query gauge for the lifetime of `guard`, decrementing
    /// on drop. Returns a guard that is a no-op when no metrics bundle is
    /// configured.
    fn active_query_guard(&self) -> ActiveQueryGuard {
        if let Some(metrics) = &self.metrics {
            metrics.query_started();
            ActiveQueryGuard {
                metrics: Some(metrics.clone()),
            }
        } else {
            ActiveQueryGuard { metrics: None }
        }
    }

    #[must_use]
    pub fn with_query_frontend(mut self, opts: QueryFrontendOptions) -> Self {
        self.query_frontend = Some(QueryFrontendState {
            opts,
            cache: Arc::new(QueryFrontendCache::default()),
        });
        self
    }

    #[must_use]
    pub fn with_query_frontend_cache(
        mut self,
        opts: QueryFrontendOptions,
        cache: Arc<dyn RangeQueryCache>,
    ) -> Self {
        self.query_frontend = Some(QueryFrontendState { opts, cache });
        self
    }

    /// Return the `PromQL` engine backing this HTTP API state.
    #[must_use]
    pub fn engine(&self) -> &PromqlEngine<S> {
        &self.engine
    }

    #[must_use]
    pub fn engine_for_tenant(&self, tenant: &str) -> PromqlEngine<S> {
        let mut opts = self.engine_opts;
        if let Some(limits) = &self.query_limits {
            let max_samples = limits.for_tenant(tenant).max_samples_per_query;
            if max_samples != 0 {
                opts.max_samples = usize::try_from(max_samples).unwrap_or(usize::MAX);
            }
        }
        PromqlEngine::new(Arc::clone(&self.store), opts)
    }

    /// Snapshot ruler rules for one tenant.
    #[must_use]
    pub fn ruler_rule_set(
        &self,
        tenant: &str,
    ) -> BTreeMap<String, BTreeMap<String, serde_yaml::Value>> {
        self.ruler_rules
            .read()
            .ok()
            .and_then(|rules| rules.get(tenant).cloned())
            .unwrap_or_default()
    }

    /// Apply replayed ruler group state for HTTP rule rendering.
    pub fn apply_ruler_group_state(&self, record: RulerGroupStateRecord) {
        if let Ok(mut group_state) = self.ruler_group_state.write() {
            group_state.apply_record(record);
        }
    }

    /// Apply replayed ruler alert state for HTTP alert rendering.
    pub fn apply_ruler_alert_state(&self, record: RulerAlertStateRecord) {
        if let Ok(mut alert_states) = self.ruler_alerts.write() {
            let key = AlertStateKey {
                tenant: record.tenant,
                rule_id: record.rule_id,
                labels: record.labels,
            };
            match record.active_since_ms {
                Some(active_since_ms) => {
                    alert_states.insert(key, active_since_ms);
                }
                None => {
                    alert_states.remove(&key);
                }
            }
        }
    }

    /// Set the timestamp used when rendering ruler evaluations through the HTTP API.
    ///
    /// A production ruler loop will advance this from its injected clock; tests use it
    /// to exercise `for:` alert state transitions deterministically.
    pub fn set_ruler_evaluation_time_ms(&self, time_ms: i64) {
        if let Ok(mut eval_time) = self.ruler_evaluation_time_ms.write() {
            *eval_time = time_ms;
        }
    }

    fn ruler_evaluation_time_ms(&self) -> i64 {
        self.ruler_evaluation_time_ms
            .read()
            .map_or(0, |eval_time| *eval_time)
    }

    fn ruler_group_last_eval_ms(&self, tenant: &str, namespace: &str, group: &str) -> Option<i64> {
        self.ruler_group_state
            .read()
            .ok()
            .and_then(|group_state| group_state.last_eval_ms(tenant, namespace, group))
    }
}

async fn acquire_query_permit<S: MetricStore>(
    state: &PrometheusApiState<S>,
) -> Option<OwnedSemaphorePermit> {
    match &state.query_gate {
        Some(gate) => Some(
            Arc::clone(gate)
                .acquire_owned()
                .await
                .expect("query semaphore is never closed"),
        ),
        None => None,
    }
}

/// Maximum accepted `remote_read` request body, in bytes.
///
/// Bounds the snappy-compressed protobuf payload before it is buffered, so a
/// client cannot force an unbounded allocation by streaming a huge body. Matches
/// the 64 MiB decompressed-size cap [`remote_read`] already passes to
/// `snappy_block_decode`, since there is no configured `max_decompressed`
/// override to reuse.
const REMOTE_READ_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Build routes for the Prometheus API and Mimir's `/prometheus` prefix.
pub fn prometheus_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> Router {
    Router::new()
        .route("/api/v1/query", get(query::<S>).post(query_post::<S>))
        .route(
            "/api/v1/query_range",
            get(query_range::<S>).post(query_range_post::<S>),
        )
        .route(
            "/api/v1/query_exemplars",
            get(query_exemplars::<S>).post(query_exemplars_post::<S>),
        )
        .route(
            "/api/v1/read",
            post(remote_read::<S>).layer(DefaultBodyLimit::max(REMOTE_READ_MAX_BODY_BYTES)),
        )
        .route(
            "/api/v1/cardinality/label_names",
            get(cardinality_label_names::<S>).post(cardinality_label_names_post::<S>),
        )
        .route(
            "/api/v1/cardinality/label_values",
            get(cardinality_label_values::<S>).post(cardinality_label_values_post::<S>),
        )
        .route(
            "/api/v1/cardinality/active_series",
            get(cardinality_active_series::<S>).post(cardinality_active_series_post::<S>),
        )
        .route("/api/v1/series", get(series::<S>).post(series_post::<S>))
        .route("/api/v1/labels", get(labels::<S>).post(labels_post::<S>))
        .route(
            "/api/v1/label/{name}/values",
            get(label_values::<S>).post(label_values_post::<S>),
        )
        .route("/api/v1/metadata", get(metadata::<S>))
        .route("/api/v1/rules", get(rules::<S>))
        .route("/api/v1/alerts", get(alerts::<S>))
        .route("/api/v1/alertmanagers", get(alertmanagers))
        .route("/api/v1/targets", get(targets))
        .route("/api/v1/targets/metadata", get(target_metadata::<S>))
        .route("/api/v1/scrape_pools", get(scrape_pools))
        .route(
            "/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/api/v1/parse_query",
            get(parse_query).post(parse_query_post),
        )
        .route("/api/v1/status/buildinfo", get(build_info))
        .route("/api/v1/status/config", get(status_config))
        .route("/api/v1/status/flags", get(status_flags))
        .route("/api/v1/status/runtimeinfo", get(runtime_info::<S>))
        .route("/api/v1/status/tsdb", get(tsdb_status::<S>))
        .route("/api/v1/status/tsdb/blocks", get(tsdb_blocks::<S>))
        .route("/api/v1/status/walreplay", get(wal_replay_status))
        .route(
            "/prometheus/api/v1/query",
            get(query::<S>).post(query_post::<S>),
        )
        .route(
            "/prometheus/api/v1/query_range",
            get(query_range::<S>).post(query_range_post::<S>),
        )
        .route(
            "/prometheus/api/v1/query_exemplars",
            get(query_exemplars::<S>).post(query_exemplars_post::<S>),
        )
        .route(
            "/prometheus/api/v1/read",
            post(remote_read::<S>).layer(DefaultBodyLimit::max(REMOTE_READ_MAX_BODY_BYTES)),
        )
        .route(
            "/prometheus/api/v1/cardinality/label_names",
            get(cardinality_label_names::<S>).post(cardinality_label_names_post::<S>),
        )
        .route(
            "/prometheus/api/v1/cardinality/label_values",
            get(cardinality_label_values::<S>).post(cardinality_label_values_post::<S>),
        )
        .route(
            "/prometheus/api/v1/cardinality/active_series",
            get(cardinality_active_series::<S>).post(cardinality_active_series_post::<S>),
        )
        .route(
            "/prometheus/api/v1/series",
            get(series::<S>).post(series_post::<S>),
        )
        .route(
            "/prometheus/api/v1/labels",
            get(labels::<S>).post(labels_post::<S>),
        )
        .route(
            "/prometheus/api/v1/label/{name}/values",
            get(label_values::<S>).post(label_values_post::<S>),
        )
        .route("/prometheus/api/v1/metadata", get(metadata::<S>))
        .route("/prometheus/api/v1/rules", get(rules::<S>))
        .route("/prometheus/api/v1/alerts", get(alerts::<S>))
        .route("/prometheus/api/v1/alertmanagers", get(alertmanagers))
        .route("/prometheus/api/v1/targets", get(targets))
        .route(
            "/prometheus/api/v1/targets/metadata",
            get(target_metadata::<S>),
        )
        .route("/prometheus/api/v1/scrape_pools", get(scrape_pools))
        .route("/prometheus/config/v1/rules", get(ruler_config_rules::<S>))
        .route(
            "/prometheus/config/v1/rules/{namespace}",
            get(ruler_config_namespace::<S>)
                .post(set_ruler_config_group::<S>)
                .delete(delete_ruler_config_namespace::<S>),
        )
        .route(
            "/prometheus/config/v1/rules/{namespace}/{group_name}",
            get(ruler_config_group::<S>).delete(delete_ruler_config_group::<S>),
        )
        .route(
            "/prometheus/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/prometheus/api/v1/parse_query",
            get(parse_query).post(parse_query_post),
        )
        .route("/prometheus/api/v1/status/buildinfo", get(build_info))
        .route("/prometheus/api/v1/status/config", get(status_config))
        .route("/prometheus/api/v1/status/flags", get(status_flags))
        .route(
            "/prometheus/api/v1/status/runtimeinfo",
            get(runtime_info::<S>),
        )
        .route("/prometheus/api/v1/status/tsdb", get(tsdb_status::<S>))
        .route(
            "/prometheus/api/v1/status/tsdb/blocks",
            get(tsdb_blocks::<S>),
        )
        .route(
            "/prometheus/api/v1/status/walreplay",
            get(wal_replay_status),
        )
        .with_state(state)
}

#[derive(Debug, Default, Deserialize)]
struct RulesParams {
    #[serde(rename = "type")]
    rule_type: Option<String>,
    exclude_alerts: Option<bool>,
}

/// Record a query handler outcome from its final response status. A response
/// with a client/server error status (`>= 400`) counts as `status="error"`.
fn record_query_response<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    route: &str,
    response: &Response,
    started: std::time::Instant,
) {
    let ok = !response.status().is_client_error() && !response.status().is_server_error();
    state.record_query(route, ok, started.elapsed().as_secs_f64());
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    error_type: &'static str,
    message: String,
}

impl ApiError {
    fn bad_data(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error_type: "not_found",
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error_type: "execution",
            message: message.into(),
        }
    }
}

impl From<WireError> for ApiError {
    fn from(error: WireError) -> Self {
        let status = StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST);
        Self {
            status,
            error_type: "bad_data",
            message: error.to_string(),
        }
    }
}

impl From<LimitError> for ApiError {
    fn from(error: LimitError) -> Self {
        let status = StatusCode::from_u16(error.http_status()).unwrap_or(StatusCode::BAD_REQUEST);
        Self {
            status,
            error_type: error.error_type(),
            message: error.message(),
        }
    }
}

impl From<PromqlError> for ApiError {
    fn from(error: PromqlError) -> Self {
        let (status, error_type) = match &error {
            PromqlError::Parse(_) | PromqlError::Plan(_) => (StatusCode::BAD_REQUEST, "bad_data"),
            PromqlError::Unsupported(_) => (StatusCode::UNPROCESSABLE_ENTITY, "execution"),
            PromqlError::Exec(message) if message.starts_with("query exceeds max_samples=") => {
                (StatusCode::UNPROCESSABLE_ENTITY, "execution")
            }
            PromqlError::Exec(_) | PromqlError::Store(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "execution")
            }
        };
        Self {
            status,
            error_type,
            message: error.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "status": "error",
                "errorType": self.error_type,
                "error": self.message,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests;
