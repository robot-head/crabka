//! Prometheus/Mimir-compatible HTTP query API adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::AsArray;
use arrow::datatypes::{Float64Type, Int64Type, UInt64Type};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use crabka_blockstore::{LabelMatcher, Labels, MatchOp, SeriesFingerprint};
use crabka_metrics::{
    BucketSpan, LimitError, NativeHistogram, OverridesProvider, QueryEnforcer, ResetHint,
    decode_native_histograms, validate_tenant,
    wire::{WireError, pb, snappy_block_decode},
};
use promql_parser::parser::Expr;
use prost::Message;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::form_urlencoded;

use crate::{
    EngineOpts, MetricStore, PromqlEngine, PromqlError, QueryResult, RangeSeries, SampleValue,
    engine::{MAX_RESOLUTION_POINTS, label_matcher_sets},
    parse_promql,
    query_frontend::{
        FrontendRangeRequest, QueryFrontendCache, QueryFrontendOptions, RangeQueryCache,
        execute_range_query_frontend,
    },
    ruler::{RulerAlertStateRecord, RulerGroupState, RulerGroupStateRecord},
    store::{ExemplarRecord, MetadataRecord, NamedTsdbStat, ScanResult, TsdbBlock, TsdbStats},
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
    start_time: SystemTime,
}

struct QueryFrontendState {
    opts: QueryFrontendOptions,
    cache: Arc<dyn RangeQueryCache>,
}

type RulerRuleStore = BTreeMap<String, BTreeMap<String, BTreeMap<String, serde_yaml::Value>>>;
type RulerAlertStateStore = BTreeMap<AlertStateKey, i64>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AlertStateKey {
    tenant: String,
    rule_id: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuleTypeFilter {
    Any,
    Alert,
    Record,
}

impl RuleTypeFilter {
    fn from_param(value: Option<&str>) -> Self {
        match value {
            Some("alert") => Self::Alert,
            Some("record") => Self::Record,
            _ => Self::Any,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RuleRenderOptions {
    type_filter: RuleTypeFilter,
    exclude_alerts: bool,
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
            start_time: SystemTime::now(),
        }
    }

    #[must_use]
    pub fn with_query_limits(mut self, limits: OverridesProvider) -> Self {
        self.query_limits = Some(limits);
        self
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

/// Maximum accepted `remote_read` request body, in bytes.
///
/// Bounds the snappy-compressed protobuf payload before it is buffered, so a
/// client cannot force an unbounded allocation by streaming a huge body. Matches
/// the 64 MiB decompressed-size cap [`remote_read`] already passes to
/// `snappy_block_decode`, since there is no configured `max_decompressed`
/// override to reuse.
const REMOTE_READ_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Build routes for the Prometheus API and Mimir's `/prometheus` prefix.
#[allow(
    clippy::too_many_lines,
    reason = "The route table intentionally keeps the Prometheus and Mimir HTTP surfaces visible in one place."
)]
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

#[derive(Debug, Deserialize)]
struct InstantQueryParams {
    query: String,
    time: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct RangeQueryParams {
    query: String,
    start: String,
    end: String,
    step: String,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ExemplarsQueryParams {
    query: String,
    start: String,
    end: String,
}

#[derive(Debug, Deserialize)]
struct ParseQueryParams {
    query: String,
}

#[derive(Debug, Default, Deserialize)]
struct MetadataParams {
    metric: Option<String>,
    limit: Option<usize>,
    limit_per_metric: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct RulesParams {
    #[serde(rename = "type")]
    rule_type: Option<String>,
    exclude_alerts: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct TsdbStatusParams {
    limit: Option<usize>,
}

#[derive(Debug, Default)]
struct DiscoveryParams {
    matches: Vec<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default)]
struct CardinalityParams {
    selector: Option<String>,
    label_names: Vec<String>,
    limit: Option<usize>,
}

async fn query<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match instant_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_inner(state, headers, params).await
}

async fn query_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match instant_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_inner(state, headers, params).await
}

async fn query_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: InstantQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let time_ms = match optional_timestamp_ms(params.time.as_deref()) {
        Ok(time_ms) => time_ms,
        Err(error) => return error.into_response(),
    };

    let engine = state.engine_for_tenant(&tenant);
    match engine.query_instant(&tenant, &params.query, time_ms).await {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn query_range<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match range_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_range_inner(state, headers, params).await
}

async fn query_range_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match range_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_range_inner(state, headers, params).await
}

async fn query_range_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: RangeQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }
    let step_ms = match duration_ms(&params.step) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = check_range_resolution(start_ms, end_ms, step_ms) {
        return error.into_response();
    }
    if let Some(limits) = &state.query_limits {
        let now_ms = match unix_now_ms() {
            Ok(now_ms) => now_ms,
            Err(error) => return error.into_response(),
        };
        if let Err(error) =
            QueryEnforcer::check_range(limits.for_tenant(&tenant), start_ms, end_ms, now_ms)
        {
            return ApiError::from(error).into_response();
        }
    }

    let result = if let Some(frontend) = &state.query_frontend {
        let engine = state.engine_for_tenant(&tenant);
        execute_range_query_frontend(
            &engine,
            frontend.cache.as_ref(),
            &FrontendRangeRequest {
                tenant: tenant.clone(),
                query: params.query.clone(),
                start_ms,
                end_ms,
                step_ms,
                opts: frontend.opts,
            },
        )
        .await
    } else {
        state
            .engine_for_tenant(&tenant)
            .query_range(&tenant, &params.query, start_ms, end_ms, step_ms)
            .await
    };

    match result {
        Ok(mut result) => {
            apply_result_limit(&mut result, params.limit);
            success_response(result)
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn query_exemplars<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match exemplars_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes())
        {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_exemplars_inner(state, headers, params).await
}

async fn query_exemplars_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match exemplars_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_exemplars_inner(state, headers, params).await
}

async fn query_exemplars_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: ExemplarsQueryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match selector_matchers(&params.query) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let start_ms = match timestamp_ms(&params.start) {
        Ok(start_ms) => start_ms,
        Err(error) => return error.into_response(),
    };
    let end_ms = match timestamp_ms(&params.end) {
        Ok(end_ms) => end_ms,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_timestamp_range(start_ms, end_ms) {
        return error.into_response();
    }

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .exemplars(&tenant, &matchers, start_ms, end_ms)
            .await
        {
            Ok(exemplars) => {
                for exemplar in exemplars {
                    by_key.insert(exemplar_key(&exemplar), exemplar);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    success_data_response(exemplars_json(by_key.into_values().collect()))
}

async fn remote_read<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_remote_read_headers(&headers) {
        return error.into_response();
    }

    let decompressed = match snappy_block_decode(&body, 64 * 1024 * 1024) {
        Ok(decompressed) => decompressed,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let request = match pb::v1::ReadRequest::decode(decompressed.as_slice()) {
        Ok(request) => request,
        Err(error) => {
            return ApiError::bad_data(format!("protobuf decode failed: {error}")).into_response();
        }
    };
    if let Err(error) = require_remote_read_samples_response(&request) {
        return error.into_response();
    }

    let response = match remote_read_response(state.as_ref(), &tenant, request).await {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    let encoded = response.encode_to_vec();
    let compressed = match snap::raw::Encoder::new().compress_vec(&encoded) {
        Ok(compressed) => compressed,
        Err(error) => {
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_type: "execution",
                message: format!("snappy encode failed: {error}"),
            }
            .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-protobuf"),
            (header::CONTENT_ENCODING, "snappy"),
        ],
        compressed,
    )
        .into_response()
}

async fn remote_read_response<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    request: pb::v1::ReadRequest,
) -> Result<pb::v1::ReadResponse, ApiError> {
    let mut results = Vec::with_capacity(request.queries.len());
    for query in request.queries {
        validate_timestamp_range(query.start_timestamp_ms, query.end_timestamp_ms)?;
        let matchers = remote_read_matchers(&query.matchers)?;
        let labels = state
            .store
            .series(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;
        enforce_selected_series_limit(state, tenant, labels.len())?;
        let mut labels_by_fp = labels
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<SeriesFingerprint, Labels>>();
        let scan = state
            .store
            .scan(
                tenant,
                &matchers,
                query.start_timestamp_ms,
                query.end_timestamp_ms,
            )
            .await
            .map_err(ApiError::from)?;

        let mut by_fp = BTreeMap::<SeriesFingerprint, pb::v1::TimeSeries>::new();
        let mut returned_samples = 0_u64;

        if let Some(float_table) = scan.float_table.clone() {
            append_remote_read_float_samples(
                state,
                tenant,
                &scan,
                &float_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        if let Some(histogram_table) = scan.histogram_table.clone() {
            append_remote_read_histogram_samples(
                state,
                tenant,
                &scan,
                &histogram_table,
                &labels_by_fp,
                &mut by_fp,
                &mut returned_samples,
            )
            .await?;
        }

        append_remote_read_exemplars(
            state.store.as_ref(),
            tenant,
            &matchers,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
            &mut labels_by_fp,
            &mut by_fp,
        )
        .await?;

        results.push(pb::v1::QueryResult {
            timeseries: by_fp.into_values().collect(),
        });
    }
    Ok(pb::v1::ReadResponse { results })
}

async fn append_remote_read_float_samples<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    scan: &ScanResult,
    table: &str,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    returned_samples: &mut u64,
) -> Result<(), ApiError> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT series_fingerprint, timestamp, value FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    let batches = dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;

    for batch in batches {
        let fps = batch.column(0).as_primitive::<UInt64Type>();
        let timestamps = batch.column(1).as_primitive::<Int64Type>();
        let values = batch.column(2).as_primitive::<Float64Type>();
        for row in 0..batch.num_rows() {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fps.value(row))?;
            series.samples.push(pb::v1::Sample {
                timestamp: timestamps.value(row),
                value: values.value(row),
            });
        }
    }
    Ok(())
}

async fn append_remote_read_histogram_samples<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    scan: &ScanResult,
    table: &str,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    returned_samples: &mut u64,
) -> Result<(), ApiError> {
    let dataframe = scan
        .ctx
        .sql(&format!(
            "SELECT * FROM {table} ORDER BY series_fingerprint, timestamp"
        ))
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;
    let batches = dataframe
        .collect()
        .await
        .map_err(PromqlError::from)
        .map_err(ApiError::from)?;

    for batch in batches {
        for (fp, timestamp, hist) in decode_native_histograms(&batch)
            .map_err(|error| ApiError::internal(error.to_string()))?
        {
            *returned_samples = returned_samples.saturating_add(1);
            enforce_sample_count(state, tenant, *returned_samples)?;
            let series = remote_read_series(by_fp, labels_by_fp, fp)?;
            series
                .histograms
                .push(remote_read_histogram(timestamp, &hist));
        }
    }
    Ok(())
}

async fn append_remote_read_exemplars<S: MetricStore>(
    store: &S,
    tenant: &str,
    matchers: &[LabelMatcher],
    start_ms: i64,
    end_ms: i64,
    labels_by_fp: &mut BTreeMap<SeriesFingerprint, Labels>,
    by_fp: &mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
) -> Result<(), ApiError> {
    for exemplar in store
        .exemplars(tenant, matchers, start_ms, end_ms)
        .await
        .map_err(ApiError::from)?
    {
        let fp = exemplar.series_labels.fingerprint();
        labels_by_fp
            .entry(fp)
            .or_insert_with(|| exemplar.series_labels.clone());
        let series = remote_read_series(by_fp, labels_by_fp, fp)?;
        series.exemplars.push(remote_read_exemplar(&exemplar));
    }
    Ok(())
}

fn remote_read_matchers(matchers: &[pb::v1::LabelMatcher]) -> Result<Vec<LabelMatcher>, ApiError> {
    matchers
        .iter()
        .map(|matcher| {
            let op = match matcher.r#type {
                0 => MatchOp::Eq,
                1 => MatchOp::Neq,
                2 => MatchOp::Re,
                3 => MatchOp::Nre,
                other => {
                    return Err(ApiError::bad_data(format!(
                        "unknown remote_read matcher type {other}"
                    )));
                }
            };
            Ok(LabelMatcher::new(&matcher.name, op, &matcher.value))
        })
        .collect()
}

fn remote_read_labels(labels: &Labels) -> Vec<pb::v1::Label> {
    labels
        .iter()
        .map(|(name, value)| pb::v1::Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

fn remote_read_exemplar(exemplar: &ExemplarRecord) -> pb::v1::Exemplar {
    pb::v1::Exemplar {
        labels: remote_read_labels(&exemplar.labels),
        value: exemplar.value,
        timestamp: exemplar.ts_ms,
    }
}

fn remote_read_series<'a>(
    by_fp: &'a mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    fp: SeriesFingerprint,
) -> Result<&'a mut pb::v1::TimeSeries, ApiError> {
    let labels = labels_by_fp
        .get(&fp)
        .ok_or_else(|| ApiError::bad_data("remote_read series labels not found"))?;
    Ok(by_fp.entry(fp).or_insert_with(|| pb::v1::TimeSeries {
        labels: remote_read_labels(labels),
        samples: Vec::new(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }))
}

fn remote_read_histogram(timestamp: i64, hist: &NativeHistogram) -> pb::v1::Histogram {
    pb::v1::Histogram {
        count: Some(remote_read_histogram_count(hist)),
        sum: hist.sum,
        schema: i32::from(hist.schema),
        zero_threshold: hist.zero_threshold,
        zero_count: Some(remote_read_histogram_zero_count(hist)),
        negative_spans: remote_read_bucket_spans(&hist.negative_spans),
        negative_deltas: remote_read_histogram_deltas(hist.is_float, &hist.negative_counts),
        negative_counts: if hist.is_float {
            hist.negative_counts.clone()
        } else {
            Vec::new()
        },
        positive_spans: remote_read_bucket_spans(&hist.positive_spans),
        positive_deltas: remote_read_histogram_deltas(hist.is_float, &hist.positive_counts),
        positive_counts: if hist.is_float {
            hist.positive_counts.clone()
        } else {
            Vec::new()
        },
        reset_hint: remote_read_reset_hint(hist.reset_hint),
        timestamp,
        custom_values: hist.custom_values.clone().unwrap_or_default(),
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Prometheus integer histograms require integer protobuf fields; Crabka stores native histogram counts in f64 for query math."
)]
fn remote_read_histogram_count(hist: &NativeHistogram) -> pb::v1::histogram::Count {
    if hist.is_float {
        pb::v1::histogram::Count::CountFloat(hist.count)
    } else {
        pb::v1::histogram::Count::CountInt(hist.count as u64)
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Prometheus integer histograms require integer protobuf fields; Crabka stores native histogram counts in f64 for query math."
)]
fn remote_read_histogram_zero_count(hist: &NativeHistogram) -> pb::v1::histogram::ZeroCount {
    if hist.is_float {
        pb::v1::histogram::ZeroCount::ZeroCountFloat(hist.zero_count)
    } else {
        pb::v1::histogram::ZeroCount::ZeroCountInt(hist.zero_count as u64)
    }
}

fn remote_read_bucket_spans(spans: &[BucketSpan]) -> Vec<pb::v1::BucketSpan> {
    spans
        .iter()
        .map(|span| pb::v1::BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "Prometheus integer histogram bucket deltas are integer protobuf fields; Crabka stores absolute counts in f64 for query math."
)]
fn remote_read_histogram_deltas(is_float: bool, counts: &[f64]) -> Vec<i64> {
    if is_float {
        return Vec::new();
    }
    let mut previous = 0.0;
    counts
        .iter()
        .map(|count| {
            let delta = *count - previous;
            previous = *count;
            delta as i64
        })
        .collect()
}

fn remote_read_reset_hint(reset_hint: ResetHint) -> i32 {
    match reset_hint {
        ResetHint::Unknown => pb::v1::histogram::ResetHint::Unknown as i32,
        ResetHint::Yes => pb::v1::histogram::ResetHint::Yes as i32,
        ResetHint::No => pb::v1::histogram::ResetHint::No as i32,
        ResetHint::Gauge => pb::v1::histogram::ResetHint::Gauge as i32,
    }
}

fn require_remote_read_samples_response(request: &pb::v1::ReadRequest) -> Result<(), ApiError> {
    if request.accepted_response_types.is_empty()
        || request
            .accepted_response_types
            .contains(&(pb::v1::ResponseType::Samples as i32))
    {
        return Ok(());
    }
    Err(ApiError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        error_type: "execution",
        message: "remote_read only supports samples responses".into(),
    })
}

async fn cardinality_label_names<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_names_inner(state, headers, params).await
}

async fn cardinality_label_names_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_names_inner(state, headers, params).await
}

async fn cardinality_label_names_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, &tenant, &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    Json(cardinality_label_names_response(&series, params.limit)).into_response()
}

async fn cardinality_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}

async fn cardinality_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_label_values_inner(state, headers, params).await
}

async fn cardinality_label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = match cardinality_series(&state, &tenant, &params).await {
        Ok(series) => series,
        Err(error) => return error.into_response(),
    };
    Json(cardinality_label_values_response(
        &series,
        &params.label_names,
        params.limit,
    ))
    .into_response()
}

/// Resolve the series set a cardinality request operates on: the selector match
/// when a `selector` is provided, otherwise every active series for the tenant.
async fn cardinality_series<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    if params.selector.is_some() {
        cardinality_series_for_params(state, tenant, params).await
    } else {
        state
            .store
            .cardinality_active_series(tenant)
            .await
            .map_err(ApiError::from)
    }
}

async fn cardinality_series_for_params<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    let selector = params.selector.as_deref().unwrap_or_default();
    let matcher_sets = selector_matchers(selector).map_err(ApiError::from)?;
    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        let series = state
            .store
            .series(tenant, &matchers, i64::MIN, i64::MAX)
            .await
            .map_err(ApiError::from)?;
        for labels in series {
            by_key.insert(labels_key(&labels), labels);
        }
    }
    Ok(by_key.into_values().collect())
}

async fn cardinality_active_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_cardinality_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, params).await
}

async fn cardinality_active_series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_cardinality_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    cardinality_active_series_inner(state, headers, params).await
}

async fn cardinality_active_series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: CardinalityParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = cardinality_series(&state, &tenant, &params).await;
    match series {
        Ok(mut series) => {
            if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
                return error.into_response();
            }
            apply_limit(&mut series, params.limit);
            Json(active_series_response(series)).into_response()
        }
        Err(error) => error.into_response(),
    }
}

async fn format_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

async fn format_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => format_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

fn format_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => success_data_response(expr.to_string()),
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn parse_query(RawQuery(raw_query): RawQuery) -> Response {
    match parse_query_params(raw_query.as_deref().unwrap_or_default().as_bytes()) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

async fn parse_query_post(body: Bytes) -> Response {
    match parse_query_params(&body) {
        Ok(params) => parse_query_inner(&params),
        Err(error) => error.into_response(),
    }
}

fn parse_query_inner(params: &ParseQueryParams) -> Response {
    match parse_promql(&params.query) {
        Ok(expr) => match serde_json::to_value(expr) {
            Ok(value) => success_data_response(value),
            Err(error) => ApiError::internal(format!("PromQL AST serialization failed: {error}"))
                .into_response(),
        },
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn build_info() -> Response {
    success_data_response(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": "",
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": "",
    }))
}

async fn status_config() -> Response {
    success_data_response(json!({
        "yaml": "global:\n  scrape_interval: 1m\n",
    }))
}

async fn status_flags() -> Response {
    success_data_response(json!({
        "log.level": "info",
        "query.lookback-delta": "5m",
        "query.max-concurrency": "20",
        "storage.tsdb.retention.time": "15d",
    }))
}

async fn runtime_info<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let tsdb_stats = match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb_stats) => tsdb_stats,
        Err(error) => return ApiError::from(error).into_response(),
    };

    success_data_response(json!({
        "startTime": unix_time_string(state.start_time),
        "CWD": std::env::current_dir()
            .ok()
            .and_then(|path| path.into_os_string().into_string().ok())
            .unwrap_or_default(),
        "hostname": "",
        "serverTime": unix_time_string(SystemTime::now()),
        "reloadConfigSuccess": true,
        "lastConfigTime": unix_time_string(state.start_time),
        "timeSeriesCount": tsdb_stats.head_stats.num_series,
        "corruptionCount": 0,
        "goroutineCount": 0,
        "GOMAXPROCS": 0,
        "GOGC": "",
        "GODEBUG": "",
        "storageRetention": "",
    }))
}

async fn tsdb_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_tsdb_status_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_stats(&tenant).await {
        Ok(tsdb) => success_data_response(tsdb_status_json(tsdb, params.limit)),
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_blocks(&tenant).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn wal_replay_status() -> Response {
    success_data_response(json!({
        "min": 0,
        "max": 0,
        "current": 0,
        "state": "done",
    }))
}

async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, params).await
}

async fn series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    series_inner(state, headers, params).await
}

async fn series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .series(&tenant, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(series) => {
                for labels in series {
                    by_key.insert(labels_key(&labels), labels);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut series = by_key
        .into_values()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
        return error.into_response();
    }
    apply_limit(&mut series, params.limit);
    success_data_response(series)
}

async fn labels<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}

async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    labels_inner(state, headers, params).await
}

async fn labels_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut names = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_names(&tenant, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(label_names) => {
                for name in label_names {
                    names.insert(name.clone(), name);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut names = names.into_values().collect::<Vec<_>>();
    apply_limit(&mut names, params.limit);
    success_data_response(names)
}

async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_discovery_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}

async fn label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(state, headers, name, params).await
}

async fn label_values_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    name: String,
    params: DiscoveryParams,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let window = match discovery_window(&params) {
        Ok(window) => window,
        Err(error) => return error.into_response(),
    };
    let matcher_sets = match discovery_matchers(&params) {
        Ok(matcher_sets) => matcher_sets,
        Err(error) => return error.into_response(),
    };

    let mut values = BTreeMap::new();
    for matchers in matcher_sets {
        match state
            .store
            .label_values(&tenant, &name, &matchers, window.start_ms, window.end_ms)
            .await
        {
            Ok(label_values) => {
                for value in label_values {
                    values.insert(value.clone(), value);
                }
            }
            Err(error) => return ApiError::from(error).into_response(),
        }
    }
    let mut values = values.into_values().collect::<Vec<_>>();
    apply_limit(&mut values, params.limit);
    success_data_response(values)
}

async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(&tenant, params.metric.as_deref())
        .await
    {
        Ok(mut metadata) => {
            apply_limit(&mut metadata, params.limit);
            success_data_response(metadata_json(metadata, params.limit_per_metric))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_rules_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let groups = match prometheus_rule_groups_json(
        &state,
        &tenant,
        rules,
        RuleRenderOptions {
            type_filter: RuleTypeFilter::from_param(params.rule_type.as_deref()),
            exclude_alerts: params.exclude_alerts.unwrap_or(false),
        },
    )
    .await
    {
        Ok(groups) => groups,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "groups": groups,
    }))
}

async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let alerts = match prometheus_alerts_json(&state, &tenant, rules).await {
        Ok(alerts) => alerts,
        Err(error) => return ApiError::from(error).into_response(),
    };
    success_data_response(json!({
        "alerts": alerts,
    }))
}

async fn alertmanagers() -> Response {
    success_data_response(json!({
        "activeAlertmanagers": [],
        "droppedAlertmanagers": [],
    }))
}

async fn targets() -> Response {
    success_data_response(json!({
        "activeTargets": [],
        "droppedTargets": [],
        "droppedTargetCounts": {},
    }))
}

async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_metadata_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state
        .store
        .metadata(&tenant, params.metric.as_deref())
        .await
    {
        Ok(mut metadata) => {
            apply_limit(&mut metadata, params.limit);
            success_data_response(target_metadata_json(metadata))
        }
        Err(error) => ApiError::from(error).into_response(),
    }
}

async fn scrape_pools() -> Response {
    success_data_response(json!([]))
}

async fn ruler_config_rules<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = match state.ruler_rules.read() {
        Ok(rules) => rules.get(&tenant).cloned().unwrap_or_default(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    let rules = rules
        .into_iter()
        .map(|(namespace, groups)| (namespace, groups.into_values().collect::<Vec<_>>()))
        .collect::<BTreeMap<_, _>>();
    yaml_response(StatusCode::OK, &rules)
}

async fn ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let groups = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match groups {
        Some(groups) => yaml_response(StatusCode::OK, &groups.into_values().collect::<Vec<_>>()),
        None => ApiError::not_found("rule namespace not found").into_response(),
    }
}

async fn ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let group = match state.ruler_rules.read() {
        Ok(rules) => rules
            .get(&tenant)
            .and_then(|namespaces| namespaces.get(&namespace))
            .and_then(|groups| groups.get(&group_name))
            .cloned(),
        Err(_) => return ApiError::internal("ruler rules lock poisoned").into_response(),
    };
    match group {
        Some(group) => yaml_response(StatusCode::OK, &group),
        None => ApiError::not_found("rule group not found").into_response(),
    }
}

async fn set_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_yaml_content_type(&headers) {
        return error.into_response();
    }
    let group: serde_yaml::Value = match serde_yaml::from_slice(&body) {
        Ok(group) => group,
        Err(error) => {
            return ApiError::bad_data(format!("rule group YAML decode failed: {error}"))
                .into_response();
        }
    };
    let group_name = match rule_group_name(&group) {
        Ok(name) => name,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = validate_rule_group(&group) {
        return error.into_response();
    }

    match state.ruler_rules.write() {
        Ok(mut rules) => {
            rules
                .entry(tenant)
                .or_default()
                .entry(namespace)
                .or_default()
                .insert(group_name, group);
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

async fn delete_ruler_config_group<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path((namespace, group_name)): Path<(String, String)>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant)
                && let Some(groups) = namespaces.get_mut(&namespace)
            {
                groups.remove(&group_name);
                if groups.is_empty() {
                    namespaces.remove(&namespace);
                }
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

async fn delete_ruler_config_namespace<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    Path(namespace): Path<String>,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.ruler_rules.write() {
        Ok(mut rules) => {
            if let Some(namespaces) = rules.get_mut(&tenant) {
                namespaces.remove(&namespace);
            }
            StatusCode::ACCEPTED.into_response()
        }
        Err(_) => ApiError::internal("ruler rules lock poisoned").into_response(),
    }
}

fn tenant_from_headers(headers: &HeaderMap) -> Result<String, ApiError> {
    let tenant = headers
        .get("X-Scope-OrgID")
        .and_then(|value| value.to_str().ok())
        .filter(|tenant| !tenant.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("missing X-Scope-OrgID tenant header"))?;
    validate_tenant(&tenant).map_err(ApiError::bad_data)?;
    Ok(tenant)
}

fn require_remote_read_headers(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    if !content_type.eq_ignore_ascii_case("application/x-protobuf") {
        return Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "remote_read requires application/x-protobuf".into(),
        });
    }

    let content_encoding = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .trim();
    if !header_list_includes(content_encoding, "snappy") {
        return Err(ApiError::bad_data(
            "remote_read requires snappy content encoding",
        ));
    }
    Ok(())
}

fn header_list_includes(value: &str, expected: &str) -> bool {
    value
        .split(',')
        .any(|item| item.trim().eq_ignore_ascii_case(expected))
}

fn require_yaml_content_type(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    match content_type {
        "application/yaml" | "application/x-yaml" | "text/yaml" => Ok(()),
        _ => Err(ApiError {
            status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
            error_type: "bad_data",
            message: "ruler config requires application/yaml".into(),
        }),
    }
}

fn rule_group_name(group: &serde_yaml::Value) -> Result<String, ApiError> {
    group
        .get("name")
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain a non-empty name"))
}

fn validate_rule_group(group: &serde_yaml::Value) -> Result<(), ApiError> {
    let rules = group
        .get("rules")
        .and_then(serde_yaml::Value::as_sequence)
        .filter(|rules| !rules.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain at least one rule"))?;
    for rule in rules {
        validate_rule(rule)?;
    }
    Ok(())
}

fn validate_rule(rule: &serde_yaml::Value) -> Result<(), ApiError> {
    let has_record = yaml_optional_string(rule, "record").is_some();
    let has_alert = yaml_optional_string(rule, "alert").is_some();
    match (has_record, has_alert) {
        (true, true) | (false, false) => {
            return Err(ApiError::bad_data(
                "rule must contain exactly one of record or alert",
            ));
        }
        _ => {}
    }
    let expr = yaml_optional_string(rule, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule must contain a non-empty expr"))?;
    parse_promql(&expr)
        .map(|_| ())
        .map_err(|error| ApiError::bad_data(format!("rule PromQL expression is invalid: {error}")))
}

fn yaml_response(status: StatusCode, value: &impl serde::Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(yaml) => (status, [(header::CONTENT_TYPE, "application/yaml")], yaml).into_response(),
        Err(error) => ApiError::internal(format!("YAML encode failed: {error}")).into_response(),
    }
}

fn optional_timestamp_ms(value: Option<&str>) -> Result<i64, ApiError> {
    value.map_or(Ok(0), timestamp_ms)
}

fn unix_now_ms() -> Result<i64, ApiError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ApiError::internal(format!("system time before Unix epoch: {error}")))?;
    i64::try_from(duration.as_millis())
        .map_err(|_| ApiError::internal("system time exceeds supported timestamp range"))
}

fn timestamp_ms(value: &str) -> Result<i64, ApiError> {
    seconds_to_ms(value)
        .or_else(|()| rfc3339_to_ms(value))
        .map_err(|()| ApiError::bad_data("invalid timestamp"))
}

fn duration_ms(value: &str) -> Result<i64, ApiError> {
    let step_ms = seconds_to_ms(value)
        .or_else(|()| prometheus_duration_ms(value).ok_or(()))
        .map_err(|()| ApiError::bad_data("invalid duration"))?;
    if step_ms <= 0 {
        return Err(ApiError::bad_data("duration must be positive"));
    }
    Ok(step_ms)
}

fn validate_timestamp_range(start_ms: i64, end_ms: i64) -> Result<(), ApiError> {
    if end_ms < start_ms {
        return Err(ApiError::bad_data(
            "end timestamp must not be before start time",
        ));
    }
    Ok(())
}

/// Reject a range query whose resolution exceeds the per-timeseries point cap.
///
/// Prometheus enforces this unconditionally (independent of any configured
/// per-tenant limit) in `web/api/v1/api.go`: it rejects when
/// `(end - start) / step > maxResolution` (integer division, where
/// `maxResolution` is [`MAX_RESOLUTION_POINTS`]). The error message and the
/// comma-formatted bound are matched byte-for-byte so Prometheus/Grafana clients
/// that string-match on it behave identically. `step_ms` is already validated
/// positive by [`duration_ms`].
fn check_range_resolution(start_ms: i64, end_ms: i64, step_ms: i64) -> Result<(), ApiError> {
    if step_ms <= 0 {
        return Ok(());
    }
    let intervals = end_ms.saturating_sub(start_ms) / step_ms;
    if intervals > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX) {
        return Err(ApiError::bad_data(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)",
        ));
    }
    Ok(())
}

fn enforce_selected_series_limit<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    selected: usize,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_series_count(
        limits.for_tenant(tenant),
        u64::try_from(selected).unwrap_or(u64::MAX),
    )
    .map_err(ApiError::from)
}

fn enforce_sample_count<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    processed: u64,
) -> Result<(), ApiError> {
    let Some(limits) = &state.query_limits else {
        return Ok(());
    };
    QueryEnforcer::check_sample_count(limits.for_tenant(tenant), processed).map_err(ApiError::from)
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "Prometheus HTTP timestamps are decimal Unix seconds; internal evaluation uses millisecond integers."
)]
fn seconds_to_ms(value: &str) -> Result<i64, ()> {
    let seconds = value.parse::<f64>().map_err(|_| ())?;
    Ok((seconds * 1000.0).round() as i64)
}

fn prometheus_duration_ms(value: &str) -> Option<i64> {
    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = value.as_bytes();

    while index < bytes.len() {
        let amount_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if amount_start == index {
            return None;
        }
        let amount = value[amount_start..index].parse::<i64>().ok()?;

        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &value[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        total_ms = total_ms.checked_add(amount.checked_mul(multiplier)?)?;
    }

    Some(total_ms)
}

fn rfc3339_to_ms(value: &str) -> Result<i64, ()> {
    let time = OffsetDateTime::parse(value, &Rfc3339).map_err(|_| ())?;
    i64::try_from(time.unix_timestamp_nanos() / 1_000_000).map_err(|_| ())
}

fn unix_time_string(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_string(),
        |duration| duration.as_secs().to_string(),
    )
}

fn parse_discovery_params(raw_query: Option<&str>) -> Result<DiscoveryParams, ApiError> {
    let mut params = DiscoveryParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "match[]" => params.matches.push(value.into_owned()),
            "start" => params.start = Some(value.into_owned()),
            "end" => params.end = Some(value.into_owned()),
            "limit" => {
                params.limit = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid limit parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}

fn parse_discovery_form(body: &[u8]) -> Result<DiscoveryParams, ApiError> {
    parse_discovery_params(std::str::from_utf8(body).ok())
}

fn instant_query_params_from_form(body: &[u8]) -> Result<InstantQueryParams, ApiError> {
    let mut query = None;
    let mut time = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "time" => time = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(InstantQueryParams {
        query: required_form_param(query, "query")?,
        time,
        limit,
    })
}

fn parse_limit_parameter(value: &str) -> Result<usize, ApiError> {
    value
        .parse()
        .map_err(|_| ApiError::bad_data("invalid limit parameter"))
}

fn parse_metadata_params(raw_query: Option<&str>) -> Result<MetadataParams, ApiError> {
    let mut params = MetadataParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "metric" => params.metric = Some(value.into_owned()),
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            "limit_per_metric" => params.limit_per_metric = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}

fn parse_cardinality_params(raw_query: Option<&str>) -> Result<CardinalityParams, ApiError> {
    let mut params = CardinalityParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "selector" => params.selector = Some(value.into_owned()),
            "label_names[]" => params.label_names.push(value.into_owned()),
            "count_method" => match value.as_ref() {
                "inmemory" | "active" => {}
                _ => return Err(ApiError::bad_data("invalid count_method parameter")),
            },
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(params)
}

fn parse_cardinality_form(body: &[u8]) -> Result<CardinalityParams, ApiError> {
    parse_cardinality_params(std::str::from_utf8(body).ok())
}

fn parse_tsdb_status_params(raw_query: Option<&str>) -> Result<TsdbStatusParams, ApiError> {
    let mut params = TsdbStatusParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        if name == "limit" {
            params.limit = Some(parse_limit_parameter(&value)?);
        }
    }
    Ok(params)
}

fn parse_rules_params(raw_query: Option<&str>) -> Result<RulesParams, ApiError> {
    let mut params = RulesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "type" => match value.as_ref() {
                "alert" | "record" => params.rule_type = Some(value.into_owned()),
                _ => return Err(ApiError::bad_data("invalid type parameter")),
            },
            "exclude_alerts" => {
                params.exclude_alerts = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid exclude_alerts parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}

fn range_query_params_from_form(body: &[u8]) -> Result<RangeQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    let mut step = None;
    let mut limit = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            "step" => step = Some(value.into_owned()),
            "limit" => limit = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
    }
    Ok(RangeQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
        step: required_form_param(step, "step")?,
        limit,
    })
}

fn exemplars_query_params_from_form(body: &[u8]) -> Result<ExemplarsQueryParams, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    for (name, value) in form_urlencoded::parse(body) {
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            _ => {}
        }
    }
    Ok(ExemplarsQueryParams {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
    })
}

fn parse_query_params(body: &[u8]) -> Result<ParseQueryParams, ApiError> {
    let mut query = None;
    for (name, value) in form_urlencoded::parse(body) {
        if name == "query" {
            query = Some(value.into_owned());
        }
    }
    Ok(ParseQueryParams {
        query: required_form_param(query, "query")?,
    })
}

fn required_form_param(value: Option<String>, name: &str) -> Result<String, ApiError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_data(format!("missing {name} parameter")))
}

fn apply_limit<T>(values: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit.filter(|limit| *limit > 0) {
        values.truncate(limit);
    }
}

fn apply_result_limit(result: &mut QueryResult, limit: Option<usize>) {
    match result {
        QueryResult::InstantVector(samples) => apply_limit(samples, limit),
        QueryResult::RangeMatrix(series) => apply_limit(series, limit),
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => {}
    }
}

struct DiscoveryWindow {
    start_ms: i64,
    end_ms: i64,
}

fn discovery_window(params: &DiscoveryParams) -> Result<DiscoveryWindow, ApiError> {
    let start_ms = match params.start.as_deref() {
        Some(start) => timestamp_ms(start)?,
        None => 0,
    };
    let end_ms = match params.end.as_deref() {
        Some(end) => timestamp_ms(end)?,
        None => i64::MAX,
    };
    validate_timestamp_range(start_ms, end_ms)?;
    Ok(DiscoveryWindow { start_ms, end_ms })
}

fn discovery_matchers(params: &DiscoveryParams) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    if params.matches.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    let mut out = Vec::new();
    for selector in &params.matches {
        out.extend(selector_matchers(selector).map_err(ApiError::from)?);
    }
    Ok(out)
}

fn selector_matchers(selector: &str) -> Result<Vec<Vec<LabelMatcher>>, PromqlError> {
    match parse_promql(selector)? {
        Expr::VectorSelector(selector) => Ok(label_matcher_sets(&selector)),
        Expr::MatrixSelector(selector) => Ok(label_matcher_sets(&selector.vs)),
        other => Err(PromqlError::Plan(format!(
            "metadata matcher must be a vector selector, got {other}"
        ))),
    }
}

fn success_response(result: QueryResult) -> Response {
    Json(json!({
        "status": "success",
        "data": result_json(result),
    }))
    .into_response()
}

fn success_data_response(data: impl serde::Serialize) -> Response {
    Json(json!({
        "status": "success",
        "data": data,
    }))
    .into_response()
}

fn result_json(result: QueryResult) -> Value {
    match result {
        QueryResult::Scalar { ts_ms, value } => json!({
            "resultType": "scalar",
            "result": [timestamp_seconds(ts_ms), sample_string(value)],
        }),
        QueryResult::InstantVector(samples) => {
            let result = samples
                .into_iter()
                .map(|sample| match sample.value {
                    SampleValue::Float(value) => json!({
                        "metric": labels_json(&sample.labels),
                        "value": [timestamp_seconds(sample.ts_ms), sample_string(value)],
                    }),
                    SampleValue::Histogram(histogram) => json!({
                        "metric": labels_json(&sample.labels),
                        "histogram": [timestamp_seconds(sample.ts_ms), native_histogram_json(&histogram)],
                    }),
                })
                .collect::<Vec<_>>();
            json!({
                "resultType": "vector",
                "result": result,
            })
        }
        QueryResult::RangeMatrix(series) => json!({
            "resultType": "matrix",
            "result": range_matrix_json(series),
        }),
        QueryResult::Str { ts_ms, value } => json!({
            "resultType": "string",
            "result": [timestamp_seconds(ts_ms), value],
        }),
    }
}

fn range_matrix_json(series: Vec<RangeSeries>) -> Vec<Value> {
    series
        .into_iter()
        .map(|series| {
            let mut values = Vec::new();
            let mut histograms = Vec::new();
            for (ts_ms, sample) in series.samples {
                match sample {
                    SampleValue::Float(value) => {
                        values.push(json!([timestamp_seconds(ts_ms), sample_string(value)]));
                    }
                    SampleValue::Histogram(histogram) => {
                        histograms.push(json!([
                            timestamp_seconds(ts_ms),
                            native_histogram_json(&histogram)
                        ]));
                    }
                }
            }
            let mut object = Map::new();
            object.insert("metric".to_string(), labels_json(&series.labels));
            if !values.is_empty() {
                object.insert("values".to_string(), Value::Array(values));
            }
            if !histograms.is_empty() {
                object.insert("histograms".to_string(), Value::Array(histograms));
            }
            Value::Object(object)
        })
        .collect()
}

fn native_histogram_json(histogram: &NativeHistogram) -> Value {
    json!({
        "count": sample_string(histogram.count),
        "sum": sample_string(histogram.sum),
        "buckets": native_histogram_buckets_json(histogram),
    })
}

fn native_histogram_buckets_json(histogram: &NativeHistogram) -> Vec<Value> {
    let mut buckets = Vec::new();
    if histogram.is_nhcb() {
        append_custom_histogram_buckets(&mut buckets, histogram);
    } else {
        append_standard_histogram_buckets(&mut buckets, histogram);
    }
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    buckets
        .into_iter()
        .map(|bucket| {
            json!([
                bucket.boundary_rule,
                sample_string(bucket.lower),
                sample_string(bucket.upper),
                sample_string(bucket.count),
            ])
        })
        .collect()
}

fn append_standard_histogram_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    hist: &NativeHistogram,
) {
    append_spanned_buckets(
        buckets,
        &hist.negative_spans,
        &hist.negative_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_RIGHT,
            lower: -standard_histogram_bound(index, hist.schema),
            upper: -standard_histogram_bound(index - 1, hist.schema),
            count: 0.0,
        },
    );
    if hist.zero_count != 0.0 {
        buckets.push(HistogramBucketJson {
            boundary_rule: BOUNDARY_CLOSED_BOTH,
            lower: -hist.zero_threshold,
            upper: hist.zero_threshold,
            count: hist.zero_count,
        });
    }
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: standard_histogram_bound(index - 1, hist.schema),
            upper: standard_histogram_bound(index, hist.schema),
            count: 0.0,
        },
    );
}

fn append_custom_histogram_buckets(buckets: &mut Vec<HistogramBucketJson>, hist: &NativeHistogram) {
    let custom_values = hist.custom_values.as_deref().unwrap_or_default();
    append_spanned_buckets(
        buckets,
        &hist.positive_spans,
        &hist.positive_counts,
        |index| HistogramBucketJson {
            boundary_rule: BOUNDARY_OPEN_LEFT,
            lower: custom_histogram_bound(index - 1, custom_values),
            upper: custom_histogram_bound(index, custom_values),
            count: 0.0,
        },
    );
}

fn append_spanned_buckets(
    buckets: &mut Vec<HistogramBucketJson>,
    spans: &[BucketSpan],
    counts: &[f64],
    mut bucket_for_index: impl FnMut(i32) -> HistogramBucketJson,
) {
    let mut index = 0;
    let mut count_index = 0;
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                return;
            };
            let mut bucket = bucket_for_index(index);
            bucket.count = count;
            buckets.push(bucket);
            index += 1;
            count_index += 1;
        }
    }
}

fn standard_histogram_bound(index: i32, schema: i8) -> f64 {
    2_f64.powf(f64::from(index) * 2_f64.powi(-i32::from(schema)))
}

fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

const BOUNDARY_OPEN_LEFT: u8 = 0;
const BOUNDARY_OPEN_RIGHT: u8 = 1;
const BOUNDARY_CLOSED_BOTH: u8 = 3;

struct HistogramBucketJson {
    boundary_rule: u8,
    lower: f64,
    upper: f64,
    count: f64,
}

fn exemplars_json(exemplars: Vec<ExemplarRecord>) -> Vec<Value> {
    let mut groups = BTreeMap::<String, (Labels, Vec<Value>)>::new();
    for exemplar in exemplars {
        let key = labels_key(&exemplar.series_labels);
        let labels_json = labels_json(&exemplar.labels);
        let value = json!({
            "labels": labels_json,
            "value": sample_string(exemplar.value),
            "timestamp": timestamp_seconds(exemplar.ts_ms),
        });
        groups
            .entry(key)
            .or_insert_with(|| (exemplar.series_labels, Vec::new()))
            .1
            .push(value);
    }

    groups
        .into_values()
        .map(|(series_labels, exemplars)| {
            json!({
                "seriesLabels": labels_json(&series_labels),
                "exemplars": exemplars,
            })
        })
        .collect()
}

fn metadata_json(
    metadata: Vec<MetadataRecord>,
    limit_per_metric: Option<usize>,
) -> Map<String, Value> {
    let mut by_metric = BTreeMap::<String, Vec<Value>>::new();
    for record in metadata {
        let entries = by_metric.entry(record.metric_family_name).or_default();
        if limit_per_metric == Some(0) || limit_per_metric.is_none_or(|limit| entries.len() < limit)
        {
            entries.push(json!({
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            }));
        }
    }
    by_metric
        .into_iter()
        .map(|(name, entries)| (name, Value::Array(entries)))
        .collect()
}

fn target_metadata_json(metadata: Vec<MetadataRecord>) -> Vec<Value> {
    metadata
        .into_iter()
        .map(|record| {
            json!({
                "target": {},
                "metric": record.metric_family_name,
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            })
        })
        .collect()
}

async fn prometheus_rule_groups_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let mut groups = Vec::new();
    for (namespace, namespace_groups) in rules {
        for group in namespace_groups.into_values() {
            let rules = prometheus_rules_json(state, tenant, &group, options).await?;
            if rules.is_empty() {
                continue;
            }
            let group_name = yaml_string(&group, "name");
            let last_evaluation = state
                .ruler_group_last_eval_ms(tenant, &namespace, &group_name)
                .map_or_else(|| zero_evaluation_time().to_string(), rfc3339_time_string);
            groups.push(json!({
                "name": group_name,
                "file": namespace,
                "interval": duration_seconds_from_yaml(&group, "interval"),
                "lastEvaluation": last_evaluation,
                "evaluationTime": 0.0,
                "lastError": "",
                "limit": 0,
                "rules": rules,
            }));
        }
    }
    Ok(groups)
}

async fn prometheus_rules_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    group: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Vec<Value>, PromqlError> {
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for rule in rules {
        if let Some(rule_json) = prometheus_rule_json(state, tenant, rule, options).await? {
            out.push(rule_json);
        }
    }
    Ok(out)
}

async fn prometheus_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rule: &serde_yaml::Value,
    options: RuleRenderOptions,
) -> Result<Option<Value>, PromqlError> {
    if let Some(name) = yaml_optional_string(rule, "record") {
        if options.type_filter == RuleTypeFilter::Alert {
            return Ok(None);
        }
        return Ok(Some(json!({
            "evaluationTime": 0.0,
            "health": "ok",
            "lastError": "",
            "lastEvaluation": zero_evaluation_time(),
            "name": name,
            "query": yaml_string(rule, "expr"),
            "type": "recording",
        })));
    }
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(None);
    };
    if options.type_filter == RuleTypeFilter::Record {
        return Ok(None);
    }
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let alert_eval = prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await;
    let (health, last_error, alerts) = match alert_eval {
        Ok(alerts) => ("ok", String::new(), alerts),
        Err(error) => ("err", error.to_string(), Vec::new()),
    };
    let mut rule_json = json!({
        "annotations": yaml_mapping_json(rule, "annotations"),
        "duration": duration_seconds_from_yaml(rule, "for"),
        "evaluationTime": 0.0,
        "health": health,
        "lastError": last_error,
        "lastEvaluation": rfc3339_time_string(eval_time_ms),
        "labels": yaml_mapping_json(rule, "labels"),
        "name": name,
        "query": yaml_string(rule, "expr"),
        "type": "alerting",
    });
    if !options.exclude_alerts {
        rule_json["alerts"] = json!(alerts);
    }
    Ok(Some(rule_json))
}

async fn prometheus_alerts_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rules: BTreeMap<String, BTreeMap<String, serde_yaml::Value>>,
) -> Result<Vec<Value>, PromqlError> {
    let eval_time_ms = state.ruler_evaluation_time_ms();
    let mut alerts = Vec::new();
    for namespace_groups in rules.into_values() {
        for group in namespace_groups.into_values() {
            if let Some(group_rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) {
                for rule in group_rules {
                    alerts.extend(
                        prometheus_alerts_for_rule_json(state, tenant, rule, eval_time_ms).await?,
                    );
                }
            }
        }
    }
    Ok(alerts)
}

async fn prometheus_alerts_for_rule_json<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    rule: &serde_yaml::Value,
    eval_time_ms: i64,
) -> Result<Vec<Value>, PromqlError> {
    let Some(name) = yaml_optional_string(rule, "alert") else {
        return Ok(Vec::new());
    };
    let query = yaml_string(rule, "expr");
    let result = state
        .engine
        .query_instant(tenant, &query, eval_time_ms)
        .await?;
    let QueryResult::InstantVector(samples) = result else {
        return Ok(Vec::new());
    };
    let duration_seconds = duration_seconds_from_yaml(rule, "for");
    let duration_ms = duration_seconds.saturating_mul(1000);
    let rule_id = format!("{name}\n{query}");
    let mut evaluated = Vec::new();
    let mut active_keys = BTreeSet::new();
    for sample in samples {
        let SampleValue::Float(value) = sample.value else {
            continue;
        };
        let labels = alert_labels_map(&sample.labels, rule, &name);
        let key = AlertStateKey {
            tenant: tenant.to_string(),
            rule_id: rule_id.clone(),
            labels: labels.clone(),
        };
        active_keys.insert(key.clone());
        evaluated.push((key, labels, value));
    }

    let mut alert_states = state
        .ruler_alerts
        .write()
        .map_err(|_| PromqlError::Exec("ruler alert state lock poisoned".into()))?;
    alert_states.retain(|key, _| {
        (key.tenant != tenant || key.rule_id != rule_id) || active_keys.contains(key)
    });

    let mut alerts = Vec::new();
    for (key, labels, value) in evaluated {
        let active_at_ms = *alert_states.entry(key).or_insert(eval_time_ms);
        let alert_state = if duration_ms == 0
            || u64::try_from(eval_time_ms.saturating_sub(active_at_ms))
                .is_ok_and(|active_ms| active_ms >= duration_ms)
        {
            "firing"
        } else {
            "pending"
        };
        let template_labels = labels_from_map(&labels);
        let annotations = expand_alert_mapping_json(
            &yaml_mapping_json(rule, "annotations"),
            value,
            &template_labels,
        );
        let expanded_labels = labels
            .into_iter()
            .map(|(name, label_value)| {
                let expanded = expand_alert_template(&label_value, value, &template_labels);
                (name, expanded)
            })
            .collect::<BTreeMap<_, _>>();
        alerts.push(json!({
            "activeAt": rfc3339_time_string(active_at_ms),
            "annotations": annotations,
            "duration": duration_seconds,
            "labels": labels_map_json(expanded_labels),
            "name": name,
            "query": query,
            "state": alert_state,
            "value": sample_string(value),
        }));
    }
    Ok(alerts)
}

fn zero_evaluation_time() -> &'static str {
    "0001-01-01T00:00:00Z"
}

fn rfc3339_time_string(ts_ms: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(ts_ms) * 1_000_000).map_or_else(
        |_| zero_evaluation_time().to_string(),
        |time| {
            time.format(&Rfc3339)
                .unwrap_or_else(|_| zero_evaluation_time().to_string())
        },
    )
}

fn alert_labels_map(
    sample_labels: &Labels,
    rule: &serde_yaml::Value,
    alert_name: &str,
) -> BTreeMap<String, String> {
    let mut labels = sample_labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("alertname".to_string(), alert_name.to_string());
    if let Value::Object(rule_labels) = yaml_mapping_json(rule, "labels") {
        labels.extend(
            rule_labels
                .into_iter()
                .filter_map(|(name, value)| Some((name, value.as_str()?.to_string()))),
        );
    }
    labels
}

fn labels_map_json(labels: BTreeMap<String, String>) -> Value {
    Value::Object(
        labels
            .into_iter()
            .map(|(name, value)| (name, Value::String(value)))
            .collect(),
    )
}

fn yaml_string(value: &serde_yaml::Value, key: &str) -> String {
    yaml_optional_string(value, key).unwrap_or_default()
}

fn yaml_optional_string(value: &serde_yaml::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
}

fn yaml_mapping_json(value: &serde_yaml::Value, key: &str) -> Value {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_mapping)
        .map_or_else(
            || json!({}),
            |mapping| {
                let object = mapping
                    .iter()
                    .filter_map(|(name, value)| {
                        Some((
                            name.as_str()?.to_string(),
                            Value::String(value.as_str().unwrap_or_default().to_string()),
                        ))
                    })
                    .collect::<Map<_, _>>();
                Value::Object(object)
            },
        )
}

fn duration_seconds_from_yaml(value: &serde_yaml::Value, key: &str) -> u64 {
    value
        .get(key)
        .and_then(serde_yaml::Value::as_str)
        .and_then(parse_duration_seconds)
        .unwrap_or(0)
}

fn parse_duration_seconds(value: &str) -> Option<u64> {
    let value = value.trim();
    if let Some(seconds) = value.strip_suffix('s') {
        return seconds.parse().ok();
    }
    if let Some(minutes) = value.strip_suffix('m') {
        return minutes.parse::<u64>().ok().map(|minutes| minutes * 60);
    }
    if let Some(hours) = value.strip_suffix('h') {
        return hours.parse::<u64>().ok().map(|hours| hours * 60 * 60);
    }
    value.parse().ok()
}

/// Build Grafana Mimir's `/cardinality/label_names` response from a series set.
///
/// Shape: `{ "label_values_count_total": N, "label_names_count": M,
/// "cardinality": [{ "label_name": .., "label_values_count": k }, ..] }`.
/// The `cardinality` array is sorted by `label_values_count` DESC then
/// `label_name` ASC, and `limit` (when > 0) truncates that array. The two
/// totals are computed over the full, unlimited series set.
fn cardinality_label_names_response(series: &[Labels], limit: Option<usize>) -> Value {
    let mut values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in series {
        for (name, value) in labels.iter() {
            values_by_name
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    let label_names_count = values_by_name.len();
    let label_values_count_total: usize = values_by_name.values().map(BTreeSet::len).sum();

    let mut cardinality = values_by_name
        .into_iter()
        .map(|(name, values)| (name, values.len()))
        .collect::<Vec<_>>();
    cardinality.sort_by(|(left_name, left_count), (right_name, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_name.cmp(right_name))
    });
    apply_limit(&mut cardinality, limit);

    let entries = cardinality
        .into_iter()
        .map(|(label_name, label_values_count)| {
            json!({
                "label_name": label_name,
                "label_values_count": label_values_count,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "label_values_count_total": label_values_count_total,
        "label_names_count": label_names_count,
        "cardinality": entries,
    })
}

/// Build Grafana Mimir's `/cardinality/label_values` response from a series set.
///
/// Shape: `{ "series_count_total": N, "labels": [{ "label_name": ..,
/// "label_values_count": k, "series_count": s, "cardinality": [{
/// "label_value": .., "series_count": c }, ..] }, ..] }`. `labels` is sorted by
/// `series_count` DESC then `label_name` ASC; each nested `cardinality` is
/// sorted by `series_count` DESC then `label_value` ASC. `limit` (when > 0)
/// truncates each nested `cardinality` array, matching Mimir's per-label limit.
fn cardinality_label_values_response(
    series: &[Labels],
    label_names: &[String],
    limit: Option<usize>,
) -> Value {
    // For each (label_name, label_value), the distinct series carrying it.
    let mut series_by_value =
        BTreeMap::<String, BTreeMap<String, BTreeSet<SeriesFingerprint>>>::new();
    let mut total_series = BTreeSet::<SeriesFingerprint>::new();
    for labels in series {
        let fp = labels.fingerprint();
        total_series.insert(fp);
        for (name, value) in labels.iter() {
            if !label_names.is_empty() && !label_names.iter().any(|wanted| wanted == name) {
                continue;
            }
            series_by_value
                .entry(name.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(fp);
        }
    }

    let mut labels_out = series_by_value
        .into_iter()
        .map(|(label_name, values)| {
            let label_values_count = values.len();
            let series_count: usize = values.values().flatten().collect::<BTreeSet<_>>().len();
            let mut value_cardinality = values
                .into_iter()
                .map(|(label_value, fingerprints)| (label_value, fingerprints.len()))
                .collect::<Vec<_>>();
            value_cardinality.sort_by(|(left_value, left_count), (right_value, right_count)| {
                right_count
                    .cmp(left_count)
                    .then_with(|| left_value.cmp(right_value))
            });
            apply_limit(&mut value_cardinality, limit);
            (
                label_name,
                label_values_count,
                series_count,
                value_cardinality,
            )
        })
        .collect::<Vec<_>>();
    labels_out.sort_by(
        |(left_name, _, left_series, _), (right_name, _, right_series, _)| {
            right_series
                .cmp(left_series)
                .then_with(|| left_name.cmp(right_name))
        },
    );

    let labels_json = labels_out
        .into_iter()
        .map(
            |(label_name, label_values_count, series_count, value_cardinality)| {
                let cardinality = value_cardinality
                    .into_iter()
                    .map(|(label_value, count)| {
                        json!({
                            "label_value": label_value,
                            "series_count": count,
                        })
                    })
                    .collect::<Vec<_>>();
                json!({
                    "label_name": label_name,
                    "label_values_count": label_values_count,
                    "series_count": series_count,
                    "cardinality": cardinality,
                })
            },
        )
        .collect::<Vec<_>>();

    json!({
        "series_count_total": total_series.len(),
        "labels": labels_json,
    })
}

/// Build Grafana Mimir's `/cardinality/active_series` response: a bare object
/// with a single `data` array of flat label maps (no `status` envelope, no
/// `seriesLabels`/`metric` wrapper).
fn active_series_response(series: Vec<Labels>) -> Value {
    let data = series
        .into_iter()
        .map(|labels| labels_json(&labels))
        .collect::<Vec<_>>();
    json!({ "data": data })
}

fn tsdb_status_json(stats: TsdbStats, limit: Option<usize>) -> Value {
    json!({
        "headStats": {
            "numSeries": stats.head_stats.num_series,
            "chunkCount": stats.head_stats.num_chunks,
            "numSamples": stats.head_stats.num_samples,
            "minTime": stats.head_stats.min_time,
            "maxTime": stats.head_stats.max_time,
        },
        "seriesCountByMetricName": named_tsdb_stats_json(stats.series_count_by_metric_name, limit),
        "labelValueCountByLabelName": named_tsdb_stats_json(stats.label_value_count_by_label_name, limit),
        "memoryInBytesByLabelName": named_tsdb_stats_json(stats.memory_in_bytes_by_label_name, limit),
        "seriesCountByLabelValuePair": named_tsdb_stats_json(stats.series_count_by_label_value_pair, limit),
    })
}

fn tsdb_blocks_json(mut blocks: Vec<TsdbBlock>) -> Vec<Value> {
    blocks.sort_by(|left, right| {
        left.min_time
            .cmp(&right.min_time)
            .then_with(|| left.max_time.cmp(&right.max_time))
            .then_with(|| left.id.cmp(&right.id))
    });
    blocks
        .into_iter()
        .map(|block| {
            json!({
                "ulid": block.id,
                "minTime": block.min_time,
                "maxTime": block.max_time,
                "stats": {
                    "numSamples": block.num_samples,
                    "numSeries": block.num_series,
                    "numChunks": block.num_series,
                },
            })
        })
        .collect()
}

fn named_tsdb_stats_json(mut stats: Vec<NamedTsdbStat>, limit: Option<usize>) -> Vec<Value> {
    apply_limit(&mut stats, limit);
    stats
        .into_iter()
        .map(|stat| {
            json!({
                "name": stat.name,
                "value": stat.value,
            })
        })
        .collect()
}

fn labels_json(labels: &Labels) -> Value {
    let pairs = labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect::<BTreeMap<_, _>>();
    Value::Object(Map::from_iter(pairs))
}

fn labels_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, (name, value)| {
        let _ = writeln!(out, "{name}={value}");
        out
    })
}

fn exemplar_key(exemplar: &ExemplarRecord) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        labels_key(&exemplar.series_labels),
        labels_key(&exemplar.labels),
        exemplar.ts_ms,
        exemplar.value.to_bits()
    )
}

/// Encode a millisecond timestamp as the JSON number Prometheus emits from
/// `jsonutil.MarshalTimestamp`: a bare integer for whole seconds, otherwise a
/// trailing-zero-trimmed fraction (e.g. `10`, `1435781430.781`, `-0.5`).
///
/// `serde_json` renders an `f64` of `10` as `10.0`, so the value is carried as
/// a pre-formatted [`RawValue`](serde_json::value::RawValue) number token to
/// keep the output byte-exact.
fn timestamp_seconds(ts_ms: i64) -> Box<serde_json::value::RawValue> {
    let token = format_timestamp_token(ts_ms);
    serde_json::value::RawValue::from_string(token)
        .expect("timestamp token is always valid JSON number syntax")
}

/// Build the JSON number token for a millisecond timestamp, mirroring
/// Prometheus `MarshalTimestamp`: write the sign, then the absolute integer
/// seconds, then a trailing-zero-trimmed millisecond fraction when non-zero.
fn format_timestamp_token(ts_ms: i64) -> String {
    let mut out = String::new();
    if ts_ms < 0 {
        out.push('-');
    }
    let magnitude = ts_ms.unsigned_abs();
    let seconds = magnitude / 1000;
    let fraction = magnitude % 1000;
    out.push_str(&seconds.to_string());
    if fraction != 0 {
        out.push('.');
        let padded = format!("{fraction:03}");
        out.push_str(padded.trim_end_matches('0'));
    }
    out
}

fn sample_string(value: f64) -> String {
    format_sample_value(value)
}

/// Format a float exactly like Prometheus `jsonutil.MarshalFloat`, which calls
/// Go's `strconv.AppendFloat(f, fmt, -1, 64)`.
///
/// Go picks `'e'` (scientific) notation when the magnitude is `< 1e-6` or
/// `>= 1e21`, and `'f'` (plain decimal) otherwise. Precision `-1` means the
/// shortest representation that round-trips back to the same `f64`.
pub(crate) fn format_sample_value(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f == f64::INFINITY {
        return "+Inf".to_string();
    }
    if f == f64::NEG_INFINITY {
        return "-Inf".to_string();
    }

    let abs = f.abs();
    if abs != 0.0 && !(1e-6..1e21).contains(&abs) {
        format_float_exponent(f)
    } else {
        // Rust's `Display` for `f64` already emits the shortest round-tripping
        // plain-decimal form (no exponent), matching Go's `'f'` form: `3.0` ->
        // "3", `1e20` -> "100000000000000000000", `0.000001` -> "0.000001".
        format!("{f}")
    }
}

/// Render `f` in Go's `'e'` form (e.g. `1e+21`, `9.999e-07`, `-1.5e-07`).
///
/// Rust's `{:e}` produces the same shortest mantissa but a bare exponent
/// (`1e21`, `9.999e-7`); Go always writes a sign and at least two exponent
/// digits, so we re-assemble the exponent suffix here.
fn format_float_exponent(f: f64) -> String {
    let rust = format!("{f:e}");
    let (mantissa, exponent) = rust
        .split_once('e')
        .expect("Rust {:e} formatting always contains an exponent marker");
    let exponent: i32 = exponent
        .parse()
        .expect("Rust {:e} exponent is always a valid integer");
    let sign = if exponent < 0 { '-' } else { '+' };
    format!("{mantissa}e{sign}{:02}", exponent.abs())
}

/// Expand the minimal Prometheus alert-template subset used for annotation and
/// label values.
///
/// Supported actions (whitespace inside the braces is ignored):
/// - `{{ $value }}` -> the firing sample value via [`format_sample_value`].
/// - `{{ $labels.NAME }}` / `{{ $labels."NAME" }}` -> the series label `NAME`
///   ("" if absent).
///
/// Any other `{{ ... }}` action is passed through verbatim (Prometheus's
/// `humanize` and friends are out of scope).
pub(crate) fn expand_alert_template(tmpl: &str, value: f64, labels: &Labels) -> String {
    let mut out = String::with_capacity(tmpl.len());
    let mut rest = tmpl;
    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        let Some(close) = after_open.find("}}") else {
            // No closing braces: emit the remainder verbatim.
            out.push_str(&rest[open..]);
            rest = "";
            break;
        };
        let action = after_open[..close].trim();
        let full = &rest[open..open + 2 + close + 2];
        match expand_alert_action(action, value, labels) {
            Some(expanded) => out.push_str(&expanded),
            None => out.push_str(full),
        }
        rest = &after_open[close + 2..];
    }
    out.push_str(rest);
    out
}

/// Build a [`Labels`] set from an alert label map for template `$labels.NAME`
/// lookups.
fn labels_from_map(map: &BTreeMap<String, String>) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in map {
        labels.insert(name, value);
    }
    labels
}

/// Apply [`expand_alert_template`] to every string value of a JSON object,
/// leaving keys and non-string values untouched. Used for alert annotation
/// maps.
fn expand_alert_mapping_json(mapping: &Value, value: f64, labels: &Labels) -> Value {
    let Value::Object(object) = mapping else {
        return mapping.clone();
    };
    let expanded = object
        .iter()
        .map(|(key, entry)| {
            let expanded = entry.as_str().map_or_else(
                || entry.clone(),
                |text| Value::String(expand_alert_template(text, value, labels)),
            );
            (key.clone(), expanded)
        })
        .collect::<Map<_, _>>();
    Value::Object(expanded)
}

fn expand_alert_action(action: &str, value: f64, labels: &Labels) -> Option<String> {
    if action == "$value" {
        return Some(format_sample_value(value));
    }
    if let Some(label_ref) = action.strip_prefix("$labels.") {
        let name = label_ref.trim();
        let name = name
            .strip_prefix('"')
            .and_then(|inner| inner.strip_suffix('"'))
            .unwrap_or(name);
        let resolved = labels
            .iter()
            .find(|(label, _)| label.as_str() == name)
            .map(|(_, label_value)| label_value.clone())
            .unwrap_or_default();
        return Some(resolved);
    }
    None
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
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use crabka_metrics::{Limits, OverridesProvider};
    use tower::ServiceExt;

    use super::*;
    use crate::InMemoryMetricStore;

    #[test]
    fn float_formatting_matches_go() {
        // Matches Go's strconv.AppendFloat(f, fmt, -1, 64) selection used by
        // Prometheus jsonutil.MarshalFloat.
        assert_eq!(format_sample_value(1.0), "1");
        assert_eq!(format_sample_value(1.5), "1.5");
        assert_eq!(format_sample_value(0.0), "0");
        assert_eq!(format_sample_value(-0.0), "-0");
        assert_eq!(format_sample_value(3.0), "3");
        assert_eq!(format_sample_value(0.5), "0.5");
        // 1e20 stays in 'f' form (abs < 1e21).
        assert_eq!(format_sample_value(1e20), "100000000000000000000");
        // 1e21 is the boundary where 'e' form kicks in.
        assert_eq!(format_sample_value(1e21), "1e+21");
        // 1e-6 is NOT < 1e-6, so it stays in 'f' form.
        assert_eq!(format_sample_value(1e-6), "0.000001");
        // Just below 1e-6 switches to 'e' form.
        assert_eq!(format_sample_value(9.999e-7), "9.999e-07");
        assert_eq!(format_sample_value(1.5e-7), "1.5e-07");
        assert_eq!(format_sample_value(f64::NAN), "NaN");
        assert_eq!(format_sample_value(f64::INFINITY), "+Inf");
        assert_eq!(format_sample_value(f64::NEG_INFINITY), "-Inf");
        // Very long decimal: shortest round-trip representation.
        assert_eq!(format_sample_value(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(format_sample_value(-1234.5678), "-1234.5678");
        // Negative exponent boundary and large magnitudes.
        assert_eq!(format_sample_value(-1e21), "-1e+21");
        assert_eq!(format_sample_value(1.234e30), "1.234e+30");
    }

    #[test]
    fn expand_alert_template_substitutions() {
        let mut labels = Labels::new();
        labels.insert("job", "api");
        labels.insert("instance", "host-1");

        assert_eq!(
            expand_alert_template("value is {{ $value }}", 42.5, &labels),
            "value is 42.5"
        );
        assert_eq!(
            expand_alert_template("job={{ $labels.job }}", 1.0, &labels),
            "job=api"
        );
        assert_eq!(
            expand_alert_template("job={{ $labels.\"job\" }}", 1.0, &labels),
            "job=api"
        );
        // Absent label expands to empty string.
        assert_eq!(
            expand_alert_template("x={{ $labels.missing }}", 1.0, &labels),
            "x="
        );
        // Unknown actions pass through verbatim.
        assert_eq!(
            expand_alert_template("{{ humanize $value }}", 1.0, &labels),
            "{{ humanize $value }}"
        );
        // No-whitespace variants still expand.
        assert_eq!(
            expand_alert_template("{{$value}} {{$labels.job}}", 7.0, &labels),
            "7 api"
        );
    }

    #[tokio::test]
    async fn query_range_rejects_ranges_over_tenant_limit() {
        let limits = Limits {
            max_query_length_secs: 60,
            ..Limits::default()
        };
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
                .with_query_limits(OverridesProvider::new(limits)),
        );

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query_range?query=up&start=0&end=120&step=60")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "execution");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("query range too long")
        );
    }

    #[tokio::test]
    async fn query_range_rejects_resolution_over_point_cap_without_limits() {
        // No per-tenant query_limits configured: Prometheus enforces the
        // 11000-point resolution cap unconditionally. start=0 end=20000 step=1s
        // => 20000 intervals > 11000.
        let state = Arc::new(PrometheusApiState::new(
            Arc::new(InMemoryMetricStore::new()),
            EngineOpts::default(),
        ));

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query_range?query=up&start=0&end=20000&step=1")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");
        assert_eq!(
            body["error"],
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
        );
    }

    #[tokio::test]
    async fn rejects_tenant_id_with_unsupported_character() {
        // dskit ValidTenantID rejects characters outside [a-zA-Z0-9] and the
        // allowed punctuation set; '/' is forbidden.
        let state = Arc::new(PrometheusApiState::new(
            Arc::new(InMemoryMetricStore::new()),
            EngineOpts::default(),
        ));

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up")
                    .header("x-scope-orgid", "tenant/a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");
        // The reason comes from the shared `crabka_metrics::validate_tenant`.
        assert_eq!(
            body["error"],
            "tenant ID contains unsupported character `/`"
        );
    }

    #[tokio::test]
    async fn series_rejects_selected_series_over_tenant_limit() {
        let mut store = InMemoryMetricStore::new();
        let mut api_labels = Labels::new();
        api_labels.insert("__name__", "up");
        api_labels.insert("job", "api");
        store.push_float("tenant-a", api_labels, 0, 1.0);
        let mut worker_labels = Labels::new();
        worker_labels.insert("__name__", "up");
        worker_labels.insert("job", "worker");
        store.push_float("tenant-a", worker_labels, 0, 1.0);

        let limits = Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        };
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(store), EngineOpts::default())
                .with_query_limits(OverridesProvider::new(limits)),
        );

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/series?match[]=up&start=0&end=1")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "execution");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("series per query exceeded")
        );
    }

    #[tokio::test]
    async fn cardinality_active_series_rejects_over_tenant_limit() {
        let mut store = InMemoryMetricStore::new();
        let mut api_labels = Labels::new();
        api_labels.insert("__name__", "up");
        api_labels.insert("job", "api");
        store.push_float("tenant-a", api_labels, 0, 1.0);
        let mut worker_labels = Labels::new();
        worker_labels.insert("__name__", "up");
        worker_labels.insert("job", "worker");
        store.push_float("tenant-a", worker_labels, 0, 1.0);

        let limits = Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        };
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(store), EngineOpts::default())
                .with_query_limits(OverridesProvider::new(limits)),
        );

        let response = prometheus_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/cardinality/active_series")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "execution");
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("series per query exceeded")
        );
    }
}
