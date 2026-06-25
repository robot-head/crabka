use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine;
use crabka_traceql::{
    AttrValue, ScanJob, ScanOptions, ScopedTag, SearchOptions, SearchResponse, SpanRef, SpanStore,
    TagScope, TraceMetricsResponse, TraceSpans, TraceqlEngine, TraceqlError, TypedValue,
};
use opentelemetry_proto::tonic::common::v1::{
    AnyValue as OtlpAnyValue, ArrayValue as OtlpArrayValue, InstrumentationScope,
    KeyValue as OtlpKeyValue, any_value::Value as OtlpValue,
};
use opentelemetry_proto::tonic::resource::v1::Resource as OtlpResource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans as OtlpResourceSpans, ScopeSpans as OtlpScopeSpans, Span as OtlpSpan,
    Status as OtlpStatus,
    span::{Event as OtlpEvent, Link as OtlpLink},
};
use prost::Message as _;
use serde_json::{Map, Value, json};

use crate::error::tempo_limit_error_response;
use crate::limits::{LimitError, Limits, OverridesProvider, QueryEnforcer};

const TENANT_HEADER: &str = "x-scope-orgid";
const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:parentID",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];
const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];
const LINK_TAGS: &[&str] = &["link:spanID", "link:traceID"];
const INSTRUMENTATION_TAGS: &[&str] = &["instrumentation:name", "instrumentation:version"];

struct AppState<S: SpanStore> {
    engine: Arc<TraceqlEngine<S>>,
    cfg: HttpConfig,
}

impl<S: SpanStore> Clone for AppState<S> {
    fn clone(&self) -> Self {
        Self {
            engine: Arc::clone(&self.engine),
            cfg: self.cfg.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HttpConfig {
    pub max_trace_spans: usize,
    pub limits: Limits,
    pub overrides: Option<OverridesProvider>,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            max_trace_spans: usize::MAX,
            limits: Limits::default(),
            overrides: None,
        }
    }
}

impl HttpConfig {
    fn limits_for_tenant(&self, tenant: &str) -> &Limits {
        self.overrides
            .as_ref()
            .map_or(&self.limits, |overrides| overrides.for_tenant(tenant))
    }
}

pub fn router<S>(engine: Arc<TraceqlEngine<S>>) -> Router
where
    S: SpanStore + 'static,
{
    router_with_config(engine, HttpConfig::default())
}

pub fn router_with_config<S>(engine: Arc<TraceqlEngine<S>>, cfg: HttpConfig) -> Router
where
    S: SpanStore + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/status/buildinfo", get(buildinfo))
        .route("/api/search", get(search::<S>))
        .route("/api/search/tags", get(search_tags::<S>))
        .route("/api/v2/search/tags", get(search_tags_v2::<S>))
        .route("/api/search/tag/{tag}/values", get(search_tag_values::<S>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<S>),
        )
        .route("/api/metrics/query_range", get(query_range::<S>))
        .route("/api/metrics/query", get(query_instant::<S>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<S>))
        .route("/api/traces/{trace_id}", get(trace_by_id_v1::<S>))
        .with_state(AppState { engine, cfg })
}

async fn echo() -> &'static str {
    "echo"
}

async fn ready() -> &'static str {
    "ready"
}

/// Tempo-compatible build info. Grafana's Tempo datasource probes this on every
/// query to detect the backend version; without it Grafana treats the backend as
/// a legacy Tempo and falls back to endpoints we do not serve (breaking the
/// trace-by-id view). The Prometheus-style `{status, data:{version,...}}` shape
/// matches Tempo's `/api/status/buildinfo`.
async fn buildinfo() -> Response {
    Json(json!({
        "status": "success",
        "data": {
            "version": "2.6.0",
            "revision": "crabka",
            "branch": "main",
            "buildUser": "crabka",
            "buildDate": "",
            "goVersion": "",
        },
    }))
    .into_response()
}

async fn search<S>(State(state): State<AppState<S>>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let query = match search_query(&uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
        }
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end_ns < start_ns {
        return (StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let limit = match optional_usize_param(&uri, "limit") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) =
        QueryEnforcer::check_search_limit(limits, u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return limit_error_response(&err);
    }
    if let Err(err) = QueryEnforcer::check_search_duration(limits, start_ns, end_ns) {
        return limit_error_response(&err);
    }
    if limit > state.engine.max_traces() {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "max traces per search exceeded",
        )
            .into_response();
    }
    let spss = match optional_usize_param(&uri, "spss") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let min_duration_ns = match duration_param(&uri, "minDuration") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let max_duration_ns = match duration_param(&uri, "maxDuration") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    let duration_filtered = min_duration_ns.is_some() || max_duration_ns.is_some();
    let search_limit = duration_filtered.then_some(usize::MAX);

    match state
        .engine
        .search_with_options(
            &tenant,
            &query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit,
                scan_options,
            },
        )
        .await
    {
        Ok(resp) => {
            let resp = if duration_filtered {
                filter_search_duration(
                    resp,
                    min_duration_ns,
                    max_duration_ns,
                    state.engine.effective_search_limit(limit),
                )
            } else {
                resp
            };
            Json(search_json(resp)).into_response()
        }
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

async fn search_tags<S>(State(state): State<AppState<S>>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q") {
        let scan_options = match scan_options_param(&uri) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match matching_traces(
            state.engine.as_ref(),
            &tenant,
            &query,
            start_ns,
            end_ns,
            scan_options,
        )
        .await
        {
            Ok(traces) => {
                Json(search_tags_json(&scoped_tags_from_traces(&traces, scope))).into_response()
            }
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_names(&tenant, scope, start_ns, end_ns)
            .await
        {
            Ok(tags) => Json(search_tags_json(&tags)).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}

async fn search_tags_v2<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let scope = match scope_param(&uri) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q") {
        let scan_options = match scan_options_param(&uri) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match matching_traces(
            state.engine.as_ref(),
            &tenant,
            &query,
            start_ns,
            end_ns,
            scan_options,
        )
        .await
        {
            Ok(traces) => Json(search_tags_v2_json(&add_intrinsic_tags(
                scoped_tags_from_traces(&traces, scope),
                scope,
            )))
            .into_response(),
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_names(&tenant, scope, start_ns, end_ns)
            .await
        {
            Ok(tags) => Json(search_tags_v2_json(&add_intrinsic_tags(tags, scope))).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}

async fn search_tag_values<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q") {
        let scan_options = match scan_options_param(&uri) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match matching_traces(
            state.engine.as_ref(),
            &tenant,
            &query,
            start_ns,
            end_ns,
            scan_options,
        )
        .await
        {
            Ok(traces) => Json(search_tag_values_json(&tag_values_from_traces(
                &traces, &tag,
            )))
            .into_response(),
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_values(&tenant, &tag, start_ns, end_ns)
            .await
        {
            Ok(values) => Json(search_tag_values_json(&values)).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}

async fn search_tag_values_v2<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if let Some(query) = query_param(&uri, "q") {
        let scan_options = match scan_options_param(&uri) {
            Ok(value) => value,
            Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
        };
        match matching_traces(
            state.engine.as_ref(),
            &tenant,
            &query,
            start_ns,
            end_ns,
            scan_options,
        )
        .await
        {
            Ok(traces) => Json(search_tag_values_v2_json(&tag_values_from_traces(
                &traces, &tag,
            )))
            .into_response(),
            Err(err) => traceql_query_error_response(&err),
        }
    } else {
        match state
            .engine
            .tag_values(&tenant, &tag, start_ns, end_ns)
            .await
        {
            Ok(values) => Json(search_tag_values_v2_json(&values)).into_response(),
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        }
    }
}

async fn query_range<S>(State(state): State<AppState<S>>, headers: HeaderMap, uri: Uri) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = query_param(&uri, "q") else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    if end_ns < start_ns {
        return (StatusCode::BAD_REQUEST, "end must be >= start").into_response();
    }
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) = QueryEnforcer::check_search_duration(limits, start_ns, end_ns) {
        return limit_error_response(&err);
    }
    let step_ns = match step_param(&uri, start_ns, end_ns) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_selection = exemplar_selection(&uri);
    let scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .query_range_with_options(&tenant, &query, start_ns, end_ns, step_ns, scan_options)
        .await
    {
        Ok(resp) => Json(trace_metrics_json(&filter_metrics_exemplars(
            resp,
            exemplar_selection,
        )))
        .into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

async fn query_instant<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = query_param(&uri, "q") else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns, step_ns, point_ns) = match instant_metric_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_selection = exemplar_selection(&uri);
    let scan_options = match scan_options_param(&uri) {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .query_range_with_options(&tenant, &query, start_ns, end_ns, step_ns, scan_options)
        .await
    {
        Ok(resp) => Json(trace_metrics_json(&filter_metrics_exemplars(
            instant_metrics_response(resp, point_ns),
            exemplar_selection,
        )))
        .into_response(),
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

async fn trace_by_id<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let Ok(trace_id) = decode_trace_id(&trace_id) else {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    };
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .trace_by_id_within(&tenant, &trace_id, start_ns, end_ns)
        .await
    {
        Ok(Some(trace)) => {
            if wants_protobuf(&headers) {
                match trace_by_id_response_protobuf(&trace, state.cfg.max_trace_spans) {
                    Ok(bytes) => {
                        ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response()
                    }
                    Err(err) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }
                }
            } else {
                Json(trace_json(&trace, state.cfg.max_trace_spans)).into_response()
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, "trace not found").into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

/// Tempo v1 trace-by-id (`/api/traces/{id}`). Grafana's Tempo *backend*
/// datasource fetches the trace-view here with `Accept: application/protobuf`
/// and proto-decodes the body as OTLP, so we default to OTLP `TracesData`
/// protobuf (Tempo's v1 default), falling back to the wrapped JSON for humans.
async fn trace_by_id_v1<S>(
    State(state): State<AppState<S>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    S: SpanStore + 'static,
{
    let Ok(trace_id) = decode_trace_id(&trace_id) else {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    };
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    match state
        .engine
        .trace_by_id_within(&tenant, &trace_id, start_ns, end_ns)
        .await
    {
        Ok(Some(trace)) => {
            if wants_json(&headers) {
                Json(trace_json(&trace, state.cfg.max_trace_spans)).into_response()
            } else {
                match trace_protobuf(&trace, state.cfg.max_trace_spans) {
                    Ok(bytes) => {
                        ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response()
                    }
                    Err(err) => {
                        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                    }
                }
            }
        }
        Ok(None) => (StatusCode::NOT_FOUND, "trace not found").into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

fn wants_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                let media_type = part.split(';').next().unwrap_or_default().trim();
                media_type.eq_ignore_ascii_case("application/protobuf")
                    || media_type.eq_ignore_ascii_case("application/x-protobuf")
            })
        })
}

fn wants_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                part.split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .eq_ignore_ascii_case("application/json")
            })
        })
}

fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(query) = query_param(uri, "q") {
        return Ok(Some(query));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}

fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|(key, value)| {
            // The key becomes an unquoted TraceQL attribute reference, so a key
            // carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys
            // rather than interpolating their raw bytes.
            key_is_safe_attribute(&key).then(|| {
                format!(
                    "{} = \"{}\"",
                    traceql_tag_field(&key),
                    value.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
        })
        .collect::<Option<Vec<_>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}

/// A legacy `tags=` key is a safe `TraceQL` attribute reference only if it is
/// made of identifier characters (alphanumerics plus `._:-`). Anything else
/// (`{`, `}`, `"`, `\`, `|`, `&`, `=`, whitespace, …) could inject query
/// structure once interpolated unquoted into the generated `TraceQL`.
fn key_is_safe_attribute(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}

fn parse_logfmt_tags(tags: &str) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = tags.trim_start();
    while !rest.is_empty() {
        let key_end = rest.find('=')?;
        let key = &rest[..key_end];
        if key.is_empty() || key.chars().any(char::is_whitespace) {
            return None;
        }
        rest = &rest[key_end + 1..];
        let (value, consumed) = parse_logfmt_value(rest)?;
        out.push((key.to_string(), value));
        rest = rest[consumed..].trim_start();
    }
    Some(out)
}

fn parse_logfmt_value(input: &str) -> Option<(String, usize)> {
    if let Some(input) = input.strip_prefix('"') {
        let mut value = String::new();
        let mut escaped = false;
        for (idx, ch) in input.char_indices() {
            if escaped {
                value.push(match ch {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                });
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Some((value, idx + 2));
            } else {
                value.push(ch);
            }
        }
        return None;
    }

    let end = input
        .char_indices()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx))
        .unwrap_or(input.len());
    Some((input[..end].to_string(), end))
}

fn traceql_tag_field(key: &str) -> String {
    if key.contains(':') {
        key.to_string()
    } else {
        format!(".{}", key.strip_prefix('.').unwrap_or(key))
    }
}

fn optional_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = optional_seconds_param(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds_param(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn instant_metric_bounds(uri: &Uri) -> Result<(i64, i64, i64, i64), String> {
    if query_param(uri, "start").is_some() || query_param(uri, "end").is_some() {
        let start_ns = required_seconds_param(uri, "start")?;
        let end_ns = required_seconds_param(uri, "end")?;
        let step_ns = end_ns
            .checked_sub(start_ns)
            .and_then(|width| width.checked_add(1))
            .filter(|step| *step > 0)
            .ok_or_else(|| "end must be >= start".to_string())?;
        return Ok((start_ns, end_ns, step_ns, end_ns));
    }

    let ts_ns = optional_seconds_param(uri, "time")?.unwrap_or(0);
    Ok((ts_ns, ts_ns, 1_000_000_000, ts_ns))
}

fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (whole, fraction) = value.split_once('.').map_or((value, ""), |parts| parts);
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 9
    {
        return None;
    }

    let whole_ns = whole.parse::<i64>().ok()?.checked_mul(1_000_000_000)?;
    let fraction_ns = if fraction.is_empty() {
        0
    } else {
        let padded = format!("{fraction:0<9}");
        padded.parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}

fn required_seconds_param(uri: &Uri, key: &'static str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

fn optional_seconds_param(uri: &Uri, key: &'static str) -> Result<Option<i64>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
        })
        .transpose()
}

fn optional_usize_param(uri: &Uri, key: &'static str) -> Result<Option<usize>, String> {
    query_param(uri, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid query parameter {key}"))
        })
        .transpose()
}

fn scan_options_param(uri: &Uri) -> Result<ScanOptions, String> {
    let block = query_param(uri, "block");
    let row_group_start = optional_usize_param(uri, "rowGroupStart")?;
    let row_group_end = optional_usize_param(uri, "rowGroupEnd")?;
    if block.is_none() && row_group_start.is_none() && row_group_end.is_none() {
        return Ok(ScanOptions::default());
    }
    let Some(object_key) = block else {
        return Err("missing query parameter block".into());
    };
    let Some(row_group_start) = row_group_start else {
        return Err("missing query parameter rowGroupStart".into());
    };
    let Some(row_group_end) = row_group_end else {
        return Err("missing query parameter rowGroupEnd".into());
    };
    if row_group_end <= row_group_start {
        return Err("rowGroupEnd must be > rowGroupStart".into());
    }
    Ok(ScanOptions {
        job: Some(ScanJob {
            object_key,
            row_group_start,
            row_group_end,
        }),
        ..ScanOptions::default()
    })
}

fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}

fn step_param(uri: &Uri, start_ns: i64, end_ns: i64) -> Result<i64, &'static str> {
    let Some(step) = query_param(uri, "step") else {
        // Tempo computes a default step when the client omits it; Grafana's
        // Traces Drilldown breakdown queries send no `step`. Match that instead
        // of rejecting the query.
        return Ok(default_query_range_step_ns(start_ns, end_ns));
    };
    let Some(step_ns) = parse_step_to_ns(&step) else {
        return Err("invalid step");
    };
    if step_ns <= 0 {
        return Err("step must be positive");
    }
    Ok(step_ns)
}

/// Default query-range step when none is supplied: aim for ~100 buckets over the
/// range, rounded up to a whole second, with a 1s floor (mirrors Tempo's
/// `DefaultQueryRangeStep` closely enough for a usable series).
fn default_query_range_step_ns(start_ns: i64, end_ns: i64) -> i64 {
    const SECOND_NS: i64 = 1_000_000_000;
    let delta = end_ns.saturating_sub(start_ns).max(0);
    let raw = delta / 100;
    let rounded = raw.saturating_add(SECOND_NS - 1) / SECOND_NS * SECOND_NS;
    rounded.max(SECOND_NS)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExemplarSelection {
    All,
    Limit(usize),
    None,
}

fn exemplar_selection(uri: &Uri) -> ExemplarSelection {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => ExemplarSelection::None,
        Some(value) => value
            .parse::<usize>()
            .map_or(ExemplarSelection::All, ExemplarSelection::Limit),
        None => ExemplarSelection::All,
    }
}

fn duration_param(uri: &Uri, key: &str) -> Result<Option<u64>, String> {
    query_param(uri, key)
        .map(|value| parse_go_duration_ns(&value).map_err(|err| format!("invalid {key}: {err}")))
        .transpose()
}

fn parse_go_duration_ns(value: &str) -> Result<u64, String> {
    if value.is_empty() {
        return Err("empty duration".into());
    }

    let mut total = 0_u128;
    let mut rest = value;
    while !rest.is_empty() {
        let number_len = rest
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected number in {value:?}"))?;
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_alphabetic() || *c == 'µ')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected unit after {number:?}"))?;
        let (unit, next) = tail.split_at(unit_len);
        let multiplier = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(format!("unsupported unit {unit:?}")),
        };
        total = total
            .checked_add(parse_duration_component_ns(number, multiplier)?)
            .ok_or_else(|| "duration out of range".to_string())?;
        rest = next;
    }

    u64::try_from(total).map_err(|_| "duration out of range".into())
}

fn parse_duration_component_ns(number: &str, multiplier: u128) -> Result<u128, String> {
    let (whole, fraction) = number.split_once('.').map_or((number, ""), |parts| parts);
    if whole.is_empty() && fraction.is_empty() {
        return Err(format!("invalid number {number:?}"));
    }
    if fraction.contains('.') {
        return Err(format!("invalid number {number:?}"));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<u128>()
            .map_err(|_| format!("invalid number {number:?}"))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction = fraction
        .parse::<u128>()
        .map_err(|_| format!("invalid number {number:?}"))?;
    let scale = (0..number.rsplit_once('.').map_or(0, |(_, frac)| frac.len()))
        .try_fold(1_u128, |acc, _| acc.checked_mul(10))
        .ok_or_else(|| "duration out of range".to_string())?;
    let fraction_ns = fraction
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?
        / scale;
    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| "duration out of range".to_string())
}

fn parse_tag_scope(scope: &str) -> Option<TagScope> {
    match scope {
        "resource" => Some(TagScope::Resource),
        "span" => Some(TagScope::Span),
        "intrinsic" => Some(TagScope::Intrinsic),
        "event" => Some(TagScope::Event),
        "link" => Some(TagScope::Link),
        "instrumentation" => Some(TagScope::Instrumentation),
        _ => None,
    }
}

fn scope_param(uri: &Uri) -> Result<Option<TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|scope| parse_tag_scope(&scope).ok_or("invalid scope"))
        .transpose()
}

fn decode_trace_id(trace_id: &str) -> Result<[u8; 16], hex::FromHexError> {
    let mut out = [0; 16];
    hex::decode_to_slice(trace_id, &mut out)?;
    Ok(out)
}

fn search_json(resp: SearchResponse) -> Value {
    let inspected_traces = resp.inspected_traces;
    let inspected_bytes = resp.inspected_bytes;
    // Spans this response scanned/returned: the distinct matched spans across
    // every returned trace's spanSets. The frontend folds this per-job sum into
    // the merged `metrics.inspectedSpans`.
    let inspected_spans: usize = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .map(|set| set.spans.len())
        .sum();
    json!({
        "traces": resp.traces.into_iter().map(|trace| {
            json!({
                "traceID": hex::encode(trace.trace_id),
                "rootServiceName": trace.root_service_name,
                "rootTraceName": trace.root_trace_name,
                "startTimeUnixNano": trace.start_time_unix_nano.to_string(),
                "durationMs": trace.duration_ms,
                "spanSets": trace.span_sets.into_iter().map(|set| {
                    json!({
                        "spans": set.spans.iter().map(search_span_json).collect::<Vec<_>>(),
                        "matched": set.matched,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        // Per-response job accounting the frontend folds (`metrics.add`): this
        // search ran as one completed job. `inspectedBytes` is the decoded size of
        // the cold+live data the scan inspected (threaded up from the SpanStore).
        "metrics": {
            "completedJobs": 1,
            "totalBlocks": 0,
            "inspectedTraces": inspected_traces,
            "inspectedSpans": inspected_spans,
            "inspectedBytes": inspected_bytes,
        },
    })
}

fn filter_search_duration(
    mut resp: SearchResponse,
    min_duration_ns: Option<u64>,
    max_duration_ns: Option<u64>,
    limit: usize,
) -> SearchResponse {
    if min_duration_ns.is_none() && max_duration_ns.is_none() {
        return resp;
    }
    resp.traces.retain(|trace| {
        min_duration_ns.is_none_or(|min| trace.duration_nanos >= min)
            && max_duration_ns.is_none_or(|max| trace.duration_nanos <= max)
    });
    resp.inspected_traces = resp.traces.len();
    resp.traces.truncate(limit);
    resp
}

fn search_tags_json(tags: &[ScopedTag]) -> Value {
    json!({
        "tagNames": tags.iter().flat_map(|scope| scope.tags.iter()).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

fn search_tags_v2_json(tags: &[ScopedTag]) -> Value {
    json!({
        "scopes": tags.iter().map(|scope| {
            json!({
                "name": tag_scope_name(scope.scope),
                "tags": &scope.tags,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

fn search_tag_values_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| &value.value).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

fn search_tag_values_v2_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| {
            json!({
                "type": &value.type_,
                "value": &value.value,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

fn traceql_query_error_response(err: &TraceqlError) -> Response {
    let status = if matches!(
        &err,
        TraceqlError::Parse(_) | TraceqlError::Plan(_) | TraceqlError::Unsupported(_)
    ) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    (status, err.to_string()).into_response()
}

fn limit_error_response(err: &LimitError) -> Response {
    tempo_limit_error_response(err)
}

fn add_intrinsic_tags(mut tags: Vec<ScopedTag>, scope: Option<TagScope>) -> Vec<ScopedTag> {
    if matches!(scope, None | Some(TagScope::Intrinsic)) {
        merge_static_scope(&mut tags, TagScope::Intrinsic, INTRINSIC_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Event)) {
        merge_static_scope(&mut tags, TagScope::Event, EVENT_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Link)) {
        merge_static_scope(&mut tags, TagScope::Link, LINK_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        merge_static_scope(&mut tags, TagScope::Instrumentation, INSTRUMENTATION_TAGS);
    }
    tags
}

fn merge_static_scope(tags: &mut Vec<ScopedTag>, scope: TagScope, static_tags: &[&str]) {
    let existing = tags
        .iter()
        .position(|scoped| scoped.scope == scope)
        .map(|idx| tags.remove(idx).tags)
        .unwrap_or_default();
    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for tag in static_tags {
        if seen.insert((*tag).to_string()) {
            merged.push((*tag).to_string());
        }
    }
    for tag in existing {
        if seen.insert(tag.clone()) {
            merged.push(tag);
        }
    }
    tags.push(ScopedTag {
        scope,
        tags: merged,
    });
}

async fn matching_traces<S>(
    engine: &TraceqlEngine<S>,
    tenant: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    scan_options: ScanOptions,
) -> Result<Vec<TraceSpans>, TraceqlError>
where
    S: SpanStore + 'static,
{
    let resp = engine
        .search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit: usize::MAX,
                spss: 0,
                search_limit: Some(usize::MAX),
                scan_options,
            },
        )
        .await?;
    let mut seen = BTreeSet::new();
    let mut traces = Vec::new();
    for trace in resp.traces {
        if seen.insert(trace.trace_id)
            && let Some(trace) = engine
                .trace_by_id_within(tenant, &trace.trace_id, start_ns, end_ns)
                .await?
        {
            traces.push(trace);
        }
    }
    Ok(traces)
}

fn scoped_tags_from_traces(traces: &[TraceSpans], scope: Option<TagScope>) -> Vec<ScopedTag> {
    let mut out = Vec::new();

    if matches!(scope, None | Some(TagScope::Resource)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            tags.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .map(|(key, _)| key),
            );
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Resource,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Span)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                tags.extend(span.attributes.iter().map(|(key, _)| key.clone()));
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Span,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Event)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for event in &span.events {
                    tags.extend(event.attributes.iter().map(|(key, _)| key.clone()));
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Event,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Link)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for link in &span.links {
                    tags.extend(link.attributes.iter().map(|(key, _)| key.clone()));
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Link,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                if !span.instrumentation_name.is_empty() {
                    tags.insert("instrumentation:name".to_string());
                }
                if !span.instrumentation_version.is_empty() {
                    tags.insert("instrumentation:version".to_string());
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Instrumentation,
                tags: tags.into_iter().collect(),
            });
        }
    }

    out
}

fn tag_values_from_traces(traces: &[TraceSpans], tag: &str) -> Vec<TypedValue> {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    let (attr_tag, attr_scope) = scoped_attribute_tag(tag);
    let mut values = BTreeSet::new();
    for trace in traces {
        collect_trace_intrinsic_values(trace, tag, &mut values);
        if matches!(attr_scope, None | Some(TagScope::Resource)) {
            values.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .filter(|(key, _)| key == attr_tag)
                    .map(|(_, value)| typed_value_parts(&value)),
            );
        }
        for span in &trace.spans {
            collect_span_intrinsic_values(span, &trace.spans, tag, &mut values);
            collect_event_values(span, tag, &mut values);
            collect_link_values(span, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Span)) {
                values.extend(
                    span.attributes
                        .iter()
                        .filter(|(key, _)| key == attr_tag)
                        .map(|(_, value)| typed_value_parts(value)),
                );
            }
        }
    }
    values
        .into_iter()
        .map(|(type_, value)| TypedValue { type_, value })
        .collect()
}

fn scoped_attribute_tag(tag: &str) -> (&str, Option<TagScope>) {
    if let Some(tag) = tag.strip_prefix("resource.") {
        (tag, Some(TagScope::Resource))
    } else if let Some(tag) = tag.strip_prefix("span.") {
        (tag, Some(TagScope::Span))
    } else {
        (tag, None)
    }
}

fn collect_trace_intrinsic_values(
    trace: &TraceSpans,
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "trace:duration" => {
            if let Some(duration) = trace_duration_nanos(trace) {
                values.insert(("duration".to_string(), duration.to_string()));
            }
        }
        "trace:id" => {
            values.insert(("string".to_string(), hex::encode(trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(("string".to_string(), trace.root_trace_name.clone()));
        }
        "trace:rootService" => {
            values.insert(("string".to_string(), trace.root_service_name.clone()));
        }
        _ => {}
    }
}

fn trace_duration_nanos(trace: &TraceSpans) -> Option<u64> {
    let start = trace
        .spans
        .iter()
        .map(|span| span.start_time_unix_nano)
        .min()?;
    let end = trace
        .spans
        .iter()
        .map(|span| {
            span.start_time_unix_nano
                .saturating_add(span.duration_nanos)
        })
        .max()?;
    Some(end.saturating_sub(start))
}

fn collect_span_intrinsic_values(
    span: &SpanRef,
    spans: &[SpanRef],
    tag: &str,
    values: &mut BTreeSet<(String, String)>,
) {
    match tag {
        "span:childCount" => {
            let count = spans
                .iter()
                .filter(|other| other.nested_set_parent == span.nested_set_left)
                .count();
            values.insert(("int".to_string(), count.to_string()));
        }
        "span:duration" => {
            values.insert(("duration".to_string(), span.duration_nanos.to_string()));
        }
        "span:id" => {
            values.insert(("string".to_string(), hex::encode(span.span_id)));
        }
        "span:kind" => {
            values.insert(("int".to_string(), span.kind.to_string()));
        }
        "span:name" => {
            values.insert(("string".to_string(), span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(("string".to_string(), hex::encode(parent_id)));
            }
        }
        "span:nestedSetLeft" => {
            values.insert(("int".to_string(), span.nested_set_left.to_string()));
        }
        "span:nestedSetParent" | "span:Parent" => {
            values.insert(("int".to_string(), span.nested_set_parent.to_string()));
        }
        "span:nestedSetRight" => {
            values.insert(("int".to_string(), span.nested_set_right.to_string()));
        }
        "span:status" => {
            values.insert(("int".to_string(), span.status_code.to_string()));
        }
        "span:statusMessage" if !span.status_message.is_empty() => {
            values.insert(("string".to_string(), span.status_message.clone()));
        }
        "instrumentation:name" if !span.instrumentation_name.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_name.clone()));
        }
        "instrumentation:version" if !span.instrumentation_version.is_empty() => {
            values.insert(("string".to_string(), span.instrumentation_version.clone()));
        }
        _ => {}
    }
}

fn collect_event_values(span: &SpanRef, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(("string".to_string(), event.name.clone()));
            }
            "event:timeSinceStart" => {
                values.insert((
                    "duration".to_string(),
                    event.time_since_start_nano.to_string(),
                ));
            }
            _ => {}
        }
        values.extend(
            event
                .attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "event."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}

fn collect_link_values(span: &SpanRef, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(("string".to_string(), hex::encode(link.trace_id)));
            }
            "link:spanID" => {
                values.insert(("string".to_string(), hex::encode(link.span_id)));
            }
            _ => {}
        }
        values.extend(
            link.attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "link."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}

fn nested_attribute_key_matches(key: &str, tag: &str, scope_prefix: &str) -> bool {
    key == tag || tag.strip_prefix(scope_prefix).is_some_and(|tag| key == tag)
}

fn typed_value_parts(value: &AttrValue) -> (String, String) {
    match value {
        AttrValue::Str(value) => ("string".into(), value.clone()),
        AttrValue::Int(value) => ("int".into(), value.to_string()),
        AttrValue::Float(value) => ("float".into(), value.to_string()),
        AttrValue::Bool(value) => ("bool".into(), value.to_string()),
    }
}

/// One TraceQL-metrics label as Tempo's protojson `commonv1.KeyValue`
/// (`{"key": k, "value": {"stringValue": v}}`). Grafana's Tempo backend parses
/// the `labels` field as a JSON array, so a map object fails to unmarshal
/// (`cannot unmarshal object into Go value of type []json.RawMessage`).
fn metric_label_json(key: &str, value: &str) -> Value {
    json!({ "key": key, "value": { "stringValue": value } })
}

/// Prometheus-style label string for `TimeSeries.promLabels` (Grafana's legend),
/// e.g. `{resource_service_name="api"}`. Empty label set renders as `{}`.
fn metric_prom_labels(labels: &[(String, String)]) -> String {
    let inner = labels
        .iter()
        .map(|(key, value)| {
            format!(
                "{}=\"{}\"",
                key.replace('.', "_"),
                value.replace('"', "\\\"")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{inner}}}")
}

fn trace_metrics_json(resp: &TraceMetricsResponse) -> Value {
    // Tempo `tempopb.QueryRangeResponse` protojson shape, which Grafana's Tempo
    // backend unmarshals: `series[].labels` is an ARRAY of KeyValue, samples use
    // `timestampMs` (milliseconds; int64 rendered as a string to match protojson)
    // and `value`. Crabka's internal point timestamps are nanoseconds.
    json!({
        "series": resp.series.iter().map(|series| {
            json!({
                "labels": series.labels.iter()
                    .map(|(key, value)| metric_label_json(key, value))
                    .collect::<Vec<_>>(),
                "promLabels": metric_prom_labels(&series.labels),
                "samples": series.points.iter()
                    .map(|(ts_ns, value)| json!({
                        "timestampMs": (ts_ns / 1_000_000).to_string(),
                        "value": *value,
                    }))
                    .collect::<Vec<_>>(),
                "exemplars": series.exemplars.iter()
                    .map(|exemplar| {
                        json!({
                            "labels": exemplar.labels.iter()
                                .map(|(key, value)| metric_label_json(key, value))
                                .collect::<Vec<_>>(),
                            "value": exemplar.value,
                            "timestampMs": (exemplar.timestamp_ns / 1_000_000).to_string(),
                        })
                    })
                    .collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

fn filter_metrics_exemplars(
    mut resp: TraceMetricsResponse,
    selection: ExemplarSelection,
) -> TraceMetricsResponse {
    match selection {
        ExemplarSelection::All => {}
        ExemplarSelection::Limit(max) => {
            for series in &mut resp.series {
                series.exemplars.truncate(max);
            }
        }
        ExemplarSelection::None => {
            for series in &mut resp.series {
                series.exemplars.clear();
            }
        }
    }
    resp
}

fn instant_metrics_response(mut resp: TraceMetricsResponse, point_ns: i64) -> TraceMetricsResponse {
    for series in &mut resp.series {
        let value = series
            .points
            .last()
            .map(|(_, value)| *value)
            .unwrap_or_default();
        series.points = vec![(point_ns, value)];
    }
    resp
}

fn tag_scope_name(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn search_span_json(span: &SpanRef) -> Value {
    json!({
        "spanID": hex::encode(span.span_id),
        "startTimeUnixNano": span.start_time_unix_nano.to_string(),
        "durationNanos": span.duration_nanos.to_string(),
        "attributes": attrs_json(&span.attributes),
    })
}

fn trace_resource_attributes(trace: &TraceSpans) -> Vec<(String, AttrValue)> {
    dedup_attrs(&trace.resource_attributes, trace.root_service_name.as_str())
}

fn span_resource_attributes(trace: &TraceSpans, span: &SpanRef) -> Vec<(String, AttrValue)> {
    if span.resource_attributes.is_empty() {
        trace_resource_attributes(trace)
    } else {
        dedup_attrs(&span.resource_attributes, "")
    }
}

fn dedup_attrs(
    attrs_in: &[(String, AttrValue)],
    fallback_service_name: &str,
) -> Vec<(String, AttrValue)> {
    let mut seen = BTreeSet::new();
    let mut attrs = Vec::new();
    for (key, value) in attrs_in {
        seen.insert(key.clone());
        attrs.push((key.clone(), value.clone()));
    }
    if !fallback_service_name.is_empty() && seen.insert("service.name".into()) {
        attrs.push((
            "service.name".into(),
            AttrValue::Str(fallback_service_name.to_string()),
        ));
    }
    attrs
}

type ResourceAttrs = Vec<(String, AttrValue)>;
type ResourceSpanGroup<'a> = (ResourceAttrs, Vec<&'a SpanRef>);

fn resource_span_groups(trace: &TraceSpans, returned_spans: usize) -> Vec<ResourceSpanGroup<'_>> {
    let mut groups: Vec<ResourceSpanGroup<'_>> = Vec::new();
    for span in trace.spans.iter().take(returned_spans) {
        let attrs = span_resource_attributes(trace, span);
        if let Some((_, spans)) = groups
            .iter_mut()
            .find(|(existing_attrs, _)| existing_attrs == &attrs)
        {
            spans.push(span);
        } else {
            groups.push((attrs, vec![span]));
        }
    }
    groups
}

fn trace_json(trace: &TraceSpans, max_trace_spans: usize) -> Value {
    let total_spans = trace.spans.len();
    let returned_spans = total_spans.min(max_trace_spans);
    let status = if returned_spans < total_spans {
        "PARTIAL"
    } else {
        "COMPLETE"
    };
    let message = if returned_spans < total_spans {
        format!("trace truncated after {returned_spans} spans")
    } else {
        String::new()
    };

    json!({
        "trace": {
            "resourceSpans": resource_span_groups(trace, returned_spans)
                .into_iter()
                .map(|(attrs, spans)| {
                    json!({
                        "resource": {
                            "attributes": attrs_json(&attrs),
                        },
                        "scopeSpans": scope_spans_json(trace.trace_id, spans),
                    })
                })
                .collect::<Vec<_>>(),
        },
        "status": status,
        "message": message,
    })
}

fn trace_traces_data(trace: &TraceSpans, max_trace_spans: usize) -> OtlpTracesData {
    OtlpTracesData {
        resource_spans: resource_span_groups(trace, trace.spans.len().min(max_trace_spans))
            .into_iter()
            .map(|(attrs, spans)| OtlpResourceSpans {
                resource: Some(OtlpResource {
                    attributes: otlp_attrs(&attrs),
                    ..OtlpResource::default()
                }),
                scope_spans: otlp_scope_spans(trace.trace_id, spans),
                ..OtlpResourceSpans::default()
            })
            .collect(),
    }
}

/// OTLP `TracesData` protobuf — the Tempo v1 `/api/traces/{id}` body, which
/// Grafana's Tempo datasource decodes as `tempopb.Trace` (wire-identical to
/// `TracesData`: both are field 1 = repeated `ResourceSpans`).
fn trace_protobuf(
    trace: &TraceSpans,
    max_trace_spans: usize,
) -> Result<Vec<u8>, prost::EncodeError> {
    let data = trace_traces_data(trace, max_trace_spans);
    let mut bytes = Vec::with_capacity(data.encoded_len());
    data.encode(&mut bytes)?;
    Ok(bytes)
}

/// Tempo `TraceByIDResponse` (`message TraceByIDResponse { Trace trace = 1; }`),
/// the `/api/v2/traces/{id}` protobuf body. Grafana's Tempo datasource decodes
/// the v2 trace-by-id response into this message; the inner `Trace` is
/// wire-identical to OTLP `TracesData`, so we model the field as `TracesData`.
#[derive(Clone, PartialEq, ::prost::Message)]
struct TraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    trace: Option<OtlpTracesData>,
}

fn trace_by_id_response_protobuf(
    trace: &TraceSpans,
    max_trace_spans: usize,
) -> Result<Vec<u8>, prost::EncodeError> {
    let response = TraceByIdResponse {
        trace: Some(trace_traces_data(trace, max_trace_spans)),
    };
    let mut bytes = Vec::with_capacity(response.encoded_len());
    response.encode(&mut bytes)?;
    Ok(bytes)
}

type OtlpTracesData = opentelemetry_proto::tonic::trace::v1::TracesData;

fn otlp_scope_spans(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Vec<OtlpScopeSpans> {
    let mut groups: Vec<((String, String), Vec<&SpanRef>)> = Vec::new();
    for span in input_spans {
        let key = (
            span.instrumentation_name.clone(),
            span.instrumentation_version.clone(),
        );
        if let Some((_, spans)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            spans.push(span);
        } else {
            groups.push((key, vec![span]));
        }
    }

    groups
        .into_iter()
        .map(|((name, version), spans)| OtlpScopeSpans {
            scope: (!name.is_empty() || !version.is_empty()).then_some(InstrumentationScope {
                name,
                version,
                ..InstrumentationScope::default()
            }),
            spans: spans
                .into_iter()
                .map(|span| otlp_span(trace_id, span))
                .collect(),
            ..OtlpScopeSpans::default()
        })
        .collect()
}

fn otlp_span(trace_id: [u8; 16], span: &SpanRef) -> OtlpSpan {
    OtlpSpan {
        trace_id: trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        parent_span_id: span
            .parent_span_id
            .map(|parent| parent.to_vec())
            .unwrap_or_default(),
        name: span.name.clone(),
        kind: span.kind,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span
            .start_time_unix_nano
            .saturating_add(span.duration_nanos),
        attributes: otlp_attrs(&span.attributes),
        events: span
            .events
            .iter()
            .map(|event| otlp_event(span, event))
            .collect(),
        links: span.links.iter().map(otlp_link).collect(),
        status: Some(otlp_status(span.status_code, &span.status_message)),
        ..OtlpSpan::default()
    }
}

fn otlp_event(span: &SpanRef, event: &crabka_traceql::EventRef) -> OtlpEvent {
    OtlpEvent {
        time_unix_nano: span
            .start_time_unix_nano
            .saturating_add(event.time_since_start_nano),
        name: event.name.clone(),
        attributes: otlp_attrs(&event.attributes),
        ..OtlpEvent::default()
    }
}

fn otlp_link(link: &crabka_traceql::LinkRef) -> OtlpLink {
    OtlpLink {
        trace_id: link.trace_id.to_vec(),
        span_id: link.span_id.to_vec(),
        attributes: otlp_attrs(&link.attributes),
        ..OtlpLink::default()
    }
}

fn otlp_status(code: i32, message: &str) -> OtlpStatus {
    // Tempo constructs a Status for every span, and Grafana's Tempo backend
    // dereferences `span.Status` when transforming the protobuf trace
    // (trace_transform.go) — an absent/nil status is a nil pointer dereference
    // that 500s the trace view. STATUS_CODE_UNSET (0) is a valid, present status,
    // so this is emitted unconditionally (wrapped in `Some` at the call site).
    OtlpStatus {
        code,
        message: message.to_string(),
    }
}

fn otlp_attrs(attrs: &[(String, AttrValue)]) -> Vec<OtlpKeyValue> {
    group_attrs(attrs)
        .into_iter()
        .map(|(key, values)| OtlpKeyValue {
            key: key.to_string(),
            value: Some(otlp_values(&values)),
            ..OtlpKeyValue::default()
        })
        .collect()
}

fn otlp_values(values: &[&AttrValue]) -> OtlpAnyValue {
    if let [value] = values {
        return otlp_value(value);
    }
    OtlpAnyValue {
        value: Some(OtlpValue::ArrayValue(OtlpArrayValue {
            values: values.iter().map(|value| otlp_value(value)).collect(),
        })),
    }
}

fn otlp_value(value: &AttrValue) -> OtlpAnyValue {
    OtlpAnyValue {
        value: Some(match value {
            AttrValue::Str(value) => OtlpValue::StringValue(value.clone()),
            AttrValue::Int(value) => OtlpValue::IntValue(*value),
            AttrValue::Float(value) => OtlpValue::DoubleValue(*value),
            AttrValue::Bool(value) => OtlpValue::BoolValue(*value),
        }),
    }
}

fn scope_spans_json(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Value {
    let mut groups: Vec<((String, String), Vec<&SpanRef>)> = Vec::new();
    for span in input_spans {
        let key = (
            span.instrumentation_name.clone(),
            span.instrumentation_version.clone(),
        );
        if let Some((_, spans)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            spans.push(span);
        } else {
            groups.push((key, vec![span]));
        }
    }

    Value::Array(
        groups
            .into_iter()
            .map(|((name, version), spans)| {
                json!({
                    "scope": instrumentation_scope_json(&name, &version),
                    "spans": spans
                        .into_iter()
                        .map(|span| trace_span_json(trace_id, span))
                        .collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

fn instrumentation_scope_json(name: &str, version: &str) -> Value {
    let mut scope = Map::new();
    if !name.is_empty() {
        scope.insert("name".into(), json!(name));
    }
    if !version.is_empty() {
        scope.insert("version".into(), json!(version));
    }
    Value::Object(scope)
}

fn trace_span_json(trace_id: [u8; 16], span: &SpanRef) -> Value {
    let mut obj = Map::new();
    obj.insert("traceId".into(), json!(base64(trace_id)));
    obj.insert("spanId".into(), json!(base64(span.span_id)));
    if let Some(parent_span_id) = span.parent_span_id {
        obj.insert("parentSpanId".into(), json!(base64(parent_span_id)));
    }
    obj.insert("name".into(), json!(span.name));
    if let Some(kind) = span_kind_json(span.kind) {
        obj.insert("kind".into(), json!(kind));
    }
    obj.insert(
        "startTimeUnixNano".into(),
        json!(span.start_time_unix_nano.to_string()),
    );
    obj.insert(
        "endTimeUnixNano".into(),
        json!((span.start_time_unix_nano + span.duration_nanos).to_string()),
    );
    obj.insert(
        "status".into(),
        span_status_json(span.status_code, &span.status_message),
    );
    obj.insert("attributes".into(), attrs_json(&span.attributes));
    if !span.events.is_empty() {
        obj.insert("events".into(), events_json(span));
    }
    if !span.links.is_empty() {
        obj.insert("links".into(), links_json(&span.links));
    }
    Value::Object(obj)
}

fn events_json(span: &SpanRef) -> Value {
    Value::Array(
        span.events
            .iter()
            .map(|event| {
                json!({
                    "timeUnixNano": span
                        .start_time_unix_nano
                        .saturating_add(event.time_since_start_nano)
                        .to_string(),
                    "name": event.name,
                    "attributes": attrs_json(&event.attributes),
                })
            })
            .collect(),
    )
}

fn links_json(links: &[crabka_traceql::LinkRef]) -> Value {
    Value::Array(
        links
            .iter()
            .map(|link| {
                json!({
                    "traceId": base64(link.trace_id),
                    "spanId": base64(link.span_id),
                    "attributes": attrs_json(&link.attributes),
                })
            })
            .collect(),
    )
}

fn span_kind_json(kind: i32) -> Option<&'static str> {
    match kind {
        1 => Some("SPAN_KIND_INTERNAL"),
        2 => Some("SPAN_KIND_SERVER"),
        3 => Some("SPAN_KIND_CLIENT"),
        4 => Some("SPAN_KIND_PRODUCER"),
        5 => Some("SPAN_KIND_CONSUMER"),
        _ => None,
    }
}

fn span_status_json(code: i32, message: &str) -> Value {
    // Always emit a status object so the field is never missing (Tempo always
    // sets a Status; Grafana dereferences it). STATUS_CODE_UNSET (0) is the
    // protojson default and is omitted (rendered as `{}`); OK/ERROR are explicit.
    let mut status = Map::new();
    match code {
        1 => {
            status.insert("code".into(), json!("STATUS_CODE_OK"));
        }
        2 => {
            status.insert("code".into(), json!("STATUS_CODE_ERROR"));
        }
        _ => {}
    }
    if !message.is_empty() {
        status.insert("message".into(), json!(message));
    }
    Value::Object(status)
}

fn attrs_json(attrs: &[(String, AttrValue)]) -> Value {
    Value::Array(
        group_attrs(attrs)
            .into_iter()
            .map(|(key, values)| {
                json!({
                    "key": key,
                    "value": attr_values_json(&values),
                })
            })
            .collect(),
    )
}

fn group_attrs(attrs: &[(String, AttrValue)]) -> Vec<(&str, Vec<&AttrValue>)> {
    let mut grouped: Vec<(&str, Vec<&AttrValue>)> = Vec::new();
    for (key, value) in attrs {
        if let Some((_, values)) = grouped
            .iter_mut()
            .find(|(existing_key, _)| existing_key == key)
        {
            values.push(value);
        } else {
            grouped.push((key.as_str(), vec![value]));
        }
    }
    grouped
}

fn attr_values_json(values: &[&AttrValue]) -> Value {
    if let [value] = values {
        return attr_value_json(value);
    }
    json!({
        "arrayValue": {
            "values": values.iter().map(|value| attr_value_json(value)).collect::<Vec<_>>(),
        }
    })
}

fn attr_value_json(value: &AttrValue) -> Value {
    match value {
        AttrValue::Str(v) => json!({"stringValue": v}),
        AttrValue::Int(v) => json!({"intValue": v.to_string()}),
        AttrValue::Float(v) => json!({"doubleValue": v}),
        AttrValue::Bool(v) => json!({"boolValue": v}),
    }
}

fn base64<const N: usize>(bytes: [u8; N]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use crabka_blockstore::{
        AttrValue as BlockAttrValue, BlockStore, NestedSet as BlockNestedSet, ShardedTraceBloom,
        SpanAttr, SpanKind as BlockSpanKind, SpanRow, StatusCode as BlockStatusCode,
        TraceBlockStats, TraceIndex, encode_span_rows, span_block_schema,
    };
    use crabka_traceql::{
        AttrValue, EngineOpts, EventRef, InMemorySpanStore, InputSpan, LinkRef, TraceqlEngine,
    };
    use http_body_util::BodyExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use opentelemetry_proto::tonic::trace::v1::TracesData;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::arrow::async_writer::ParquetObjectWriter;
    use parquet::file::properties::WriterProperties;
    use prost::Message as _;
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use url::Url;

    use arc_swap::ArcSwap;

    use super::*;
    use crate::querier::store::{CrabkaSpanStore, SharedTraceIndex};

    fn shared_index(index: TraceIndex) -> SharedTraceIndex {
        Arc::new(ArcSwap::from_pointee(index))
    }

    fn span(trace: u8, span: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        span_at(trace, span, parent, svc, 1_000 + i64::from(span))
    }

    fn span_at(trace: u8, span: u8, parent: Option<u8>, svc: &str, start_ns: i64) -> InputSpan {
        span_at_with_attrs(trace, span, parent, svc, start_ns, Vec::new())
    }

    fn span_at_with_attrs(
        trace: u8,
        span: u8,
        parent: Option<u8>,
        svc: &str,
        start_ns: i64,
        attrs: Vec<(String, AttrValue)>,
    ) -> InputSpan {
        let mut all_attrs = vec![("svc".into(), AttrValue::Str(svc.into()))];
        all_attrs.extend(attrs);
        InputSpan {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: "span".into(),
            kind: 0,
            start_unix_nano: start_ns,
            duration_nanos: 200,
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            attrs: all_attrs,
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn app() -> axum::Router {
        app_with_opts(EngineOpts {
            max_exemplars: 1,
            ..EngineOpts::default()
        })
    }

    fn app_with_opts(opts: EngineOpts) -> axum::Router {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![span(9, 1, None, "a"), span(9, 2, Some(1), "b")],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), opts));
        router(engine)
    }

    fn app_with_http_config(cfg: HttpConfig) -> axum::Router {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![span(9, 1, None, "a"), span(9, 2, Some(1), "b")],
        );
        let engine = Arc::new(TraceqlEngine::new(
            Arc::new(store),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        ));
        router_with_config(engine, cfg)
    }

    async fn get_json(uri: &str) -> (StatusCode, Value) {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    async fn get_text(uri: &str) -> (StatusCode, String) {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    async fn get_text_with_app(app: axum::Router, uri: &str) -> (StatusCode, String) {
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(bytes.to_vec()).unwrap())
    }

    async fn get_json_with_app(app: axum::Router, uri: &str) -> (StatusCode, Value) {
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    fn block_span_row(trace: u8, span: u8, name: &str) -> SpanRow {
        SpanRow {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: None,
            nested_set: BlockNestedSet {
                nested_set_left: 1,
                nested_set_right: 2,
                parent_id: 0,
            },
            child_count: 0,
            root_service_name: Some("api".into()),
            root_span_name: Some(name.into()),
            trace_start_unix_nano: 1_000,
            trace_duration_nanos: 500,
            name: Some(name.into()),
            kind: BlockSpanKind::Server,
            start_unix_nano: 1_000,
            duration_nanos: 500,
            status_code: BlockStatusCode::Ok,
            status_message: None,
            instrumentation_name: Some("otel-rust".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "svc".into(),
                is_array: false,
                value: BlockAttrValue::Str(vec!["api".into()]),
            }],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    async fn row_group_job_app() -> axum::Router {
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        let first = encode_span_rows(&[block_span_row(1, 1, "first-rg")]).unwrap();
        let second = encode_span_rows(&[block_span_row(2, 2, "second-rg")]).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .set_write_batch_size(1)
            .build();
        let object_writer = ParquetObjectWriter::new(
            object_store.clone(),
            Path::from("blocks/row-groups.parquet"),
        );
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), Some(props)).unwrap();
        writer.write(&first).await.unwrap();
        writer.write(&second).await.unwrap();
        writer.close().await.unwrap();

        let mut trace_index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        bloom.insert(&[2; 16]);
        trace_index.add_trace_block(
            "tenant-a",
            TraceBlockStats {
                object_key: "blocks/row-groups.parquet".into(),
                min_ts: 0,
                max_ts: 10,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared_index(trace_index), None);
        router(Arc::new(TraceqlEngine::new(
            Arc::new(store),
            EngineOpts::default(),
        )))
    }

    #[tokio::test]
    async fn operational_probes_return_plain_text() {
        let (status, body) = get_text("/api/echo").await;
        assert!(status == StatusCode::OK);
        assert!(body == "echo");

        let (status, body) = get_text("/ready").await;
        assert!(status == StatusCode::OK);
        assert!(body == "ready");

        let (status, body) = get_text("/status").await;
        assert!(status == StatusCode::OK);
        assert!(body == "ready");
    }

    #[tokio::test]
    async fn metrics_routes_return_traceql_metrics_json() {
        let (status, body) = get_json(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20rate()&start=0&end=1&step=1",
        )
        .await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "series": [{
                    "labels": [],
                    "promLabels": "{}",
                    "samples": [
                        {"timestampMs": "0", "value": 2.0},
                        {"timestampMs": "1000", "value": 0.0}
                    ],
                    "exemplars": [{
                        "labels": [
                            {"key": "trace_id", "value": {"stringValue": "09090909090909090909090909090909"}},
                            {"key": "span_id", "value": {"stringValue": "0101010101010101"}}
                        ],
                        "value": 1.0,
                        "timestampMs": "0"
                    }]
                }]
            })
        );

        let (status, body) = get_json(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=1",
        )
        .await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "series": [{
                    "labels": [],
                    "promLabels": "{}",
                    "samples": [
                        {"timestampMs": "0", "value": 2.0},
                        {"timestampMs": "1000", "value": 0.0}
                    ],
                    "exemplars": [{
                        "labels": [
                            {"key": "trace_id", "value": {"stringValue": "09090909090909090909090909090909"}},
                            {"key": "span_id", "value": {"stringValue": "0101010101010101"}}
                        ],
                        "value": 1.0,
                        "timestampMs": "0"
                    }]
                }]
            })
        );
    }

    #[tokio::test]
    async fn metrics_query_range_honors_backend_row_group_job_params() {
        let app = row_group_job_app().await;
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=1&block=blocks%2Frow-groups.parquet&rowGroupStart=1&rowGroupEnd=2",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "series": [{
                    "labels": [],
                    "promLabels": "{}",
                    "samples": [
                        {"timestampMs": "0", "value": 1.0},
                        {"timestampMs": "1000", "value": 0.0}
                    ],
                    "exemplars": []
                }]
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_filter_honors_backend_row_group_job_params() {
        let app = row_group_job_app().await;
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:name/values?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&block=blocks%2Frow-groups.parquet&rowGroupStart=1&rowGroupEnd=2",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "second-rg"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn metrics_query_range_can_disable_exemplars() {
        let (status, body) = get_json(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=1&exemplars=false",
        )
        .await;

        assert!(status == StatusCode::OK);
        assert!(body["series"][0]["exemplars"] == json!([]));
    }

    #[tokio::test]
    async fn metrics_query_range_limits_exemplars_from_numeric_param() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at(9, 1, None, "a", 0),
                span_at(9, 2, None, "b", 1_000_000_000),
            ],
        );
        let engine = Arc::new(TraceqlEngine::new(
            Arc::new(store),
            EngineOpts {
                max_exemplars: 2,
                ..EngineOpts::default()
            },
        ));
        let resp = router(engine)
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=1&exemplars=1",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["series"][0]["exemplars"].as_array().unwrap().len() == 1);
    }

    #[tokio::test]
    async fn metrics_query_range_accepts_duration_step() {
        let (status, body) = get_json(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=500ms&exemplars=false",
        )
        .await;

        assert!(status == StatusCode::OK);
        assert!(
            body["series"][0]["samples"]
                == json!([
                    {"timestampMs": "0", "value": 2.0},
                    {"timestampMs": "500", "value": 0.0},
                    {"timestampMs": "1000", "value": 0.0}
                ])
        );
    }

    #[tokio::test]
    async fn metrics_query_uses_start_end_range_without_step() {
        let (status, body) = get_json(
            "/api/metrics/query?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&exemplars=false",
        )
        .await;

        assert!(status == StatusCode::OK);
        assert!(body["series"][0]["samples"] == json!([{"timestampMs": "1000", "value": 2.0}]));
        assert!(body["series"][0]["exemplars"] == json!([]));
    }

    #[tokio::test]
    async fn metrics_query_rejects_invalid_time() {
        let (status, body) = get_text(
            "/api/metrics/query?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&time=bogus",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter time");
    }

    #[tokio::test]
    async fn metrics_query_rejects_end_before_start() {
        let (status, body) = get_text(
            "/api/metrics/query?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=2&end=1",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "end must be >= start");
    }

    #[tokio::test]
    async fn metrics_query_range_rejects_zero_step() {
        let (status, body) = get_text(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1&step=0",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "step must be positive");
    }

    #[tokio::test]
    async fn metrics_query_range_defaults_step_when_omitted() {
        // Grafana's Traces Drilldown breakdown queries omit `step`; Tempo defaults
        // it rather than rejecting (was a 400 "missing query parameter step").
        let (status, body) = get_json(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=1",
        )
        .await;

        assert!(status == StatusCode::OK);
        assert!(!body["series"][0]["samples"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn metrics_query_range_requires_start() {
        let (status, body) = get_text(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&end=1&step=1",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "missing query parameter start");
    }

    #[tokio::test]
    async fn metrics_query_range_requires_end() {
        let (status, body) = get_text(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&step=1",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "missing query parameter end");
    }

    #[tokio::test]
    async fn metrics_query_range_rejects_end_before_start() {
        let (status, body) = get_text(
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=2&end=1&step=1",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "end must be >= start");
    }

    #[tokio::test]
    async fn search_requires_start_and_end() {
        let (status, body) = get_text("/api/search?q=%7B%20.svc%20%3D%20%22b%22%20%7D").await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "missing query parameter start");

        let (status, body) =
            get_text("/api/search?q=%7B%20.svc%20%3D%20%22b%22%20%7D&start=0").await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "missing query parameter end");
    }

    #[tokio::test]
    async fn search_accepts_legacy_tags_parameter() {
        let (status, body) = get_json("/api/search?tags=svc%3Db&start=0&end=10").await;
        assert!(status == StatusCode::OK);
        assert!(body["traces"][0]["traceID"] == "09090909090909090909090909090909");
        assert!(body["traces"][0]["spanSets"][0]["matched"] == 1);
    }

    #[tokio::test]
    async fn search_accepts_quoted_legacy_tags_parameter() {
        let (status, body) = get_json("/api/search?tags=svc%3D%22b%22&start=0&end=10").await;

        assert!(status == StatusCode::OK);
        assert!(body["traces"][0]["traceID"] == "09090909090909090909090909090909");
        assert!(body["traces"][0]["spanSets"][0]["matched"] == 1);
    }

    #[tokio::test]
    async fn search_rejects_invalid_legacy_tags_parameter() {
        let (status, body) = get_text("/api/search?tags=svc&start=0&end=10").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter tags");
    }

    #[tokio::test]
    async fn search_rejects_legacy_tags_key_with_traceql_metacharacters() {
        // A key carrying TraceQL-significant characters (e.g. `}` and `=`) must
        // not be interpolated into the generated query — it would inject
        // structure. Rejected as an invalid tags param, not silently executed.
        // Raw key: `a"} || true {.b`  value: `c`.
        let malicious = "a%22%7D%20%7C%7C%20true%20%7B.b=c";
        let (status, body) =
            get_text(&format!("/api/search?tags={malicious}&start=0&end=10")).await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter tags");

        // The unit-level converter rejects a key with a metacharacter (no
        // structure leaks); only the key is unquoted, so the value side is safe.
        assert!(tags_to_traceql("a}=c").is_none());
        assert!(tags_to_traceql("a\"b=c").is_none());
        // A benign key is still converted to a properly-quoted attribute match,
        // and a value containing metacharacters stays safely quoted.
        assert!(tags_to_traceql("svc=b") == Some("{ .svc = \"b\" }".to_string()));
        assert!(tags_to_traceql("svc=a\"}||x") == Some("{ .svc = \"a\\\"}||x\" }".to_string()));
    }

    #[tokio::test]
    async fn search_parses_start_end_as_epoch_seconds() {
        let (status, body) =
            get_json("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1").await;
        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
    }

    #[tokio::test]
    async fn search_honors_backend_row_group_job_params() {
        let (status, body) = get_json_with_app(
            row_group_job_app().await,
            "/api/search?q=%7B%20.svc%20%3D%20%22api%22%20%7D&start=0&end=10&block=blocks%2Frow-groups.parquet&rowGroupStart=1&rowGroupEnd=2",
        )
        .await;

        assert!(status == StatusCode::OK);
        let traces = body["traces"].as_array().unwrap();
        assert!(traces.len() == 1);
        assert!(traces[0]["traceID"] == "02020202020202020202020202020202");
        assert!(traces[0]["rootTraceName"] == "second-rg");
    }

    #[tokio::test]
    async fn search_rejects_end_before_start() {
        let (status, body) =
            get_text("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=2&end=1").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "end must be >= start");
    }

    #[tokio::test]
    async fn search_rejects_invalid_numeric_parameters() {
        let (status, body) =
            get_text("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&limit=bogus")
                .await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter limit");

        let (status, body) =
            get_text("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&spss=bogus")
                .await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter spss");
    }

    #[tokio::test]
    async fn search_rejects_limit_above_max_traces() {
        let app = app_with_opts(EngineOpts {
            max_traces: 1,
            ..EngineOpts::default()
        });
        let (status, body) = get_text_with_app(
            app,
            "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&limit=2",
        )
        .await;

        assert!(status == StatusCode::TOO_MANY_REQUESTS);
        assert!(body == "max traces per search exceeded");
    }

    #[tokio::test]
    async fn search_rejects_limit_above_http_limits() {
        let app = app_with_http_config(HttpConfig {
            max_trace_spans: usize::MAX,
            limits: crate::limits::Limits {
                max_traces_per_search: 1,
                ..crate::limits::Limits::default()
            },
            overrides: None,
        });
        let (status, body) = get_json_with_app(
            app,
            "/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&limit=2",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body["status"] == "error");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("max traces per search"))
        );
    }

    #[tokio::test]
    async fn search_applies_tenant_limit_overrides() {
        let app = app_with_http_config(HttpConfig {
            max_trace_spans: usize::MAX,
            limits: crate::limits::Limits::default(),
            overrides: Some(
                crate::limits::OverridesProvider::from_yaml(
                    r"
overrides:
  tenant-tight:
    max_traces_per_search: 1
",
                )
                .unwrap(),
            ),
        });
        let tight = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&limit=2")
                    .header("x-scope-orgid", "tenant-tight")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=0&end=1&limit=2")
                    .header("x-scope-orgid", "tenant-loose")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(tight.status() == StatusCode::BAD_REQUEST);
        let body = tight.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("max traces per search"))
        );
        assert!(loose.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_query_range_rejects_duration_above_http_limits() {
        let app = app_with_http_config(HttpConfig {
            max_trace_spans: usize::MAX,
            limits: crate::limits::Limits {
                max_search_duration_secs: 1,
                ..crate::limits::Limits::default()
            },
            overrides: None,
        });
        let (status, body) = get_json_with_app(
            app,
            "/api/metrics/query_range?q=%7B%20.svc%20%21%3D%20nil%20%7D%20%7C%20count_over_time()&start=0&end=3&step=1",
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body["status"] == "error");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|message| message.contains("max search duration"))
        );
    }

    #[tokio::test]
    async fn search_accepts_fractional_epoch_seconds() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "inside",
            vec![span_at(1, 1, None, "a", 1_500_000_000)],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "outside",
            vec![span_at(2, 1, None, "b", 2_000_000_000)],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=1.4&end=1.6")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();

        assert!(
            status == StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["rootTraceName"] == "inside");
    }

    #[tokio::test]
    async fn search_defaults_missing_tenant_to_anonymous() {
        let mut store = InMemorySpanStore::new();
        store.push_trace("anonymous", "svc-a", "root-a", vec![span(9, 1, None, "a")]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%3D%20%22a%22%20%7D&start=0&end=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(
            status == StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["traceID"] == "09090909090909090909090909090909");
    }

    #[tokio::test]
    async fn search_honors_spss_parameter() {
        let (status, body) =
            get_json("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&spss=1&start=0&end=10").await;
        let spans = body["traces"][0]["spanSets"][0]["spans"]
            .as_array()
            .unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"][0]["spanSets"][0]["matched"] == 2);
        assert!(spans.len() == 1);
    }

    #[tokio::test]
    async fn search_honors_min_duration_parameter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "short",
            vec![span_at(1, 1, None, "a", 1_000_000_000)],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "long",
            vec![
                span_at(2, 1, None, "b", 1_000_000_000),
                span_at(2, 2, Some(1), "b", 4_000_000_000),
            ],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&minDuration=2s&start=0&end=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["rootTraceName"] == "long");
    }

    #[tokio::test]
    async fn search_honors_nanosecond_precision_min_duration_parameter() {
        let mut short = span_at(1, 1, None, "a", 1_000_000_000);
        short.duration_nanos = 1_000_000;
        let mut long = span_at(2, 1, None, "b", 1_000_000_000);
        long.duration_nanos = 1_000_001;

        let mut store = InMemorySpanStore::new();
        store.push_trace("tenant-a", "svc-a", "short", vec![short]);
        store.push_trace("tenant-a", "svc-b", "long", vec![long]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&minDuration=1000001ns&start=0&end=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["rootTraceName"] == "long");
        assert!(body["traces"][0]["durationMs"] == 1);
    }

    #[tokio::test]
    async fn search_honors_max_duration_parameter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "short",
            vec![span_at(1, 1, None, "a", 1_000_000_000)],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "long",
            vec![
                span_at(2, 1, None, "b", 1_000_000_000),
                span_at(2, 2, Some(1), "b", 4_000_000_000),
            ],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&maxDuration=2s&start=0&end=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["rootTraceName"] == "short");
    }

    #[tokio::test]
    async fn search_applies_duration_filter_before_limit() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "short",
            vec![span_at(1, 1, None, "a", 1_000_000_000)],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "long",
            vec![
                span_at(2, 1, None, "b", 2_000_000_000),
                span_at(2, 2, Some(1), "b", 5_000_000_000),
            ],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&minDuration=2s&limit=1&start=0&end=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["traces"][0]["rootTraceName"] == "long");
    }

    #[tokio::test]
    async fn search_metrics_report_inspected_traces_before_limit() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "first",
            vec![span_at(1, 1, None, "a", 1_000)],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "second",
            vec![span_at(2, 1, None, "b", 2_000)],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&limit=1&start=0&end=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["traces"].as_array().unwrap().len() == 1);
        assert!(body["metrics"]["inspectedTraces"] == 2);
    }

    #[tokio::test]
    async fn search_returns_tempo_search_shape() {
        let (status, body) =
            get_json("/api/search?q=%7B%20.svc%20%3D%20%22b%22%20%7D&start=0&end=10").await;
        assert!(status == StatusCode::OK);
        assert!(
            body["traces"]
                == json!([{
                    "traceID": "09090909090909090909090909090909",
                    "rootServiceName": "svc-a",
                    "rootTraceName": "root-a",
                    "startTimeUnixNano": "1001",
                    "durationMs": 0,
                    "spanSets": [{
                        "spans": [{
                            "spanID": "0202020202020202",
                            "startTimeUnixNano": "1002",
                            "durationNanos": "200",
                            "attributes": [{"key": "svc", "value": {"stringValue": "b"}}]
                        }],
                        "matched": 1
                    }]
                }])
        );
        let metrics = &body["metrics"];
        assert!(metrics["completedJobs"] == 1);
        assert!(metrics["totalBlocks"] == 0);
        assert!(metrics["inspectedTraces"] == 1);
        assert!(metrics["inspectedSpans"] == 1);
        // inspectedBytes = decoded size of the scanned data: non-zero, but not
        // pinned to a brittle exact byte count.
        assert!(metrics["inspectedBytes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn search_metrics_block_reports_real_per_response_accounting() {
        // A successful search must emit real per-job accounting the frontend
        // folds: `completedJobs >= 1` and non-zero inspected traces/spans (not
        // an all-zero block, which would make the merged frontend metrics read 0
        // even on a successful multi-job search).
        let (status, body) =
            get_json("/api/search?q=%7B%20.svc%20%3D%20%22b%22%20%7D&start=0&end=10").await;
        assert!(status == StatusCode::OK);
        let metrics = &body["metrics"];
        assert!(metrics["completedJobs"].as_u64().unwrap() >= 1);
        assert!(metrics["inspectedTraces"].as_u64().unwrap() >= 1);
        assert!(metrics["inspectedSpans"].as_u64().unwrap() >= 1);
        assert!(metrics["inspectedBytes"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn by_id_returns_tempo_trace_shape() {
        let (status, body) = get_json("/api/v2/traces/09090909090909090909090909090909").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "trace": {
                    "resourceSpans": [{
                        "resource": {
                            "attributes": [{
                                "key": "service.name",
                                "value": {"stringValue": "svc-a"}
                            }]
                        },
                        "scopeSpans": [{
                            "scope": {},
                            "spans": [
                                {
                                    "traceId": "CQkJCQkJCQkJCQkJCQkJCQ==",
                                    "spanId": "AQEBAQEBAQE=",
                                    "name": "span",
                                    "startTimeUnixNano": "1001",
                                    "endTimeUnixNano": "1201",
                                    "status": {},
                                    "attributes": [{"key": "svc", "value": {"stringValue": "a"}}]
                                },
                                {
                                    "traceId": "CQkJCQkJCQkJCQkJCQkJCQ==",
                                    "spanId": "AgICAgICAgI=",
                                    "parentSpanId": "AQEBAQEBAQE=",
                                    "name": "span",
                                    "startTimeUnixNano": "1002",
                                    "endTimeUnixNano": "1202",
                                    "status": {},
                                    "attributes": [{"key": "svc", "value": {"stringValue": "b"}}]
                                }
                            ]
                        }]
                    }]
                },
                "status": "COMPLETE",
                "message": ""
            })
        );
    }

    #[test]
    fn span_status_is_always_present_even_when_unset() {
        // Grafana's Tempo backend dereferences span.Status when transforming the
        // protobuf trace; an absent (nil) status panics it and 500s the trace
        // view. Every span must carry a Status, including STATUS_CODE_UNSET (0).
        for code in [0, 1, 2] {
            assert!(
                otlp_status(code, "").code == code,
                "code {code} must emit status"
            );
        }
        let unset = otlp_status(0, "");
        assert!(unset.code == 0 && unset.message.is_empty());
        // JSON mirrors the protojson shape: UNSET is present-but-empty, OK/ERROR
        // carry the explicit code.
        assert!(span_status_json(0, "") == json!({}));
        assert!(span_status_json(1, "") == json!({"code": "STATUS_CODE_OK"}));
        assert!(
            span_status_json(2, "boom") == json!({"code": "STATUS_CODE_ERROR", "message": "boom"})
        );
    }

    #[test]
    fn trace_json_includes_resource_attributes() {
        let trace = TraceSpans {
            trace_id: [9; 16],
            root_service_name: "svc-a".into(),
            root_trace_name: "root-a".into(),
            resource_attributes: vec![
                ("service.name".into(), AttrValue::Str("svc-a".into())),
                ("cloud.region".into(), AttrValue::Str("us-east-1".into())),
            ],
            spans: vec![SpanRef {
                span_id: [1; 8],
                parent_span_id: None,
                name: "root-a".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 1_001,
                duration_nanos: 200,
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                resource_attributes: Vec::new(),
                attributes: Vec::new(),
                events: Vec::new(),
                links: Vec::new(),
            }],
        };

        let body = trace_json(&trace, 10);

        assert!(
            body["trace"]["resourceSpans"][0]["resource"]["attributes"]
                .as_array()
                .unwrap()
                .contains(&json!({
                    "key": "cloud.region",
                    "value": {"stringValue": "us-east-1"}
                }))
        );
    }

    #[test]
    fn trace_json_projects_repeated_resource_attributes_as_arrays() {
        let trace = TraceSpans {
            trace_id: [9; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            resource_attributes: vec![
                ("deployment.zone".into(), AttrValue::Str("a".into())),
                ("deployment.zone".into(), AttrValue::Str("b".into())),
            ],
            spans: vec![SpanRef {
                span_id: [1; 8],
                parent_span_id: None,
                name: "api".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 1_001,
                duration_nanos: 200,
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                resource_attributes: Vec::new(),
                attributes: Vec::new(),
                events: Vec::new(),
                links: Vec::new(),
            }],
        };

        let body = trace_json(&trace, 10);

        assert!(
            body["trace"]["resourceSpans"][0]["resource"]["attributes"]
                == json!([
                    {
                        "key": "deployment.zone",
                        "value": {
                            "arrayValue": {
                                "values": [
                                    {"stringValue": "a"},
                                    {"stringValue": "b"}
                                ]
                            }
                        }
                    },
                    {
                        "key": "service.name",
                        "value": {"stringValue": "api"}
                    }
                ])
        );
    }

    #[test]
    fn trace_json_groups_spans_by_resource_attributes() {
        let trace = TraceSpans {
            trace_id: [9; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            resource_attributes: vec![("service.name".into(), AttrValue::Str("api".into()))],
            spans: vec![
                SpanRef {
                    span_id: [1; 8],
                    parent_span_id: None,
                    name: "api".into(),
                    kind: 0,
                    nested_set_left: 1,
                    nested_set_right: 4,
                    nested_set_parent: 0,
                    start_time_unix_nano: 1_001,
                    duration_nanos: 200,
                    status_code: 0,
                    status_message: String::new(),
                    instrumentation_name: String::new(),
                    instrumentation_version: String::new(),
                    resource_attributes: vec![(
                        "service.name".into(),
                        AttrValue::Str("api".into()),
                    )],
                    attributes: Vec::new(),
                    events: Vec::new(),
                    links: Vec::new(),
                },
                SpanRef {
                    span_id: [2; 8],
                    parent_span_id: Some([1; 8]),
                    name: "db".into(),
                    kind: 0,
                    nested_set_left: 2,
                    nested_set_right: 3,
                    nested_set_parent: 1,
                    start_time_unix_nano: 1_002,
                    duration_nanos: 100,
                    status_code: 0,
                    status_message: String::new(),
                    instrumentation_name: String::new(),
                    instrumentation_version: String::new(),
                    resource_attributes: vec![("service.name".into(), AttrValue::Str("db".into()))],
                    attributes: Vec::new(),
                    events: Vec::new(),
                    links: Vec::new(),
                },
            ],
        };

        let body = trace_json(&trace, 10);
        let resource_spans = body["trace"]["resourceSpans"].as_array().unwrap();

        assert!(resource_spans.len() == 2);
        assert!(
            resource_spans[0]["resource"]["attributes"]
                .as_array()
                .unwrap()
                .contains(&json!({
                    "key": "service.name",
                    "value": {"stringValue": "api"}
                }))
        );
        assert!(
            resource_spans[1]["resource"]["attributes"]
                .as_array()
                .unwrap()
                .contains(&json!({
                    "key": "service.name",
                    "value": {"stringValue": "db"}
                }))
        );
    }

    #[test]
    fn attrs_json_projects_repeated_keys_as_otlp_array_values() {
        let attrs = vec![
            ("http.method".into(), AttrValue::Str("GET".into())),
            ("http.method".into(), AttrValue::Str("POST".into())),
            ("attempt".into(), AttrValue::Int(1)),
        ];

        let body = attrs_json(&attrs);

        assert!(
            body == json!([
                {
                    "key": "http.method",
                    "value": {
                        "arrayValue": {
                            "values": [
                                {"stringValue": "GET"},
                                {"stringValue": "POST"}
                            ]
                        }
                    }
                },
                {
                    "key": "attempt",
                    "value": {"intValue": "1"}
                }
            ])
        );
    }

    #[test]
    fn trace_protobuf_projects_repeated_resource_attributes_as_arrays() {
        let trace = TraceSpans {
            trace_id: [9; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            resource_attributes: vec![
                ("deployment.zone".into(), AttrValue::Str("a".into())),
                ("deployment.zone".into(), AttrValue::Str("b".into())),
            ],
            spans: vec![SpanRef {
                span_id: [1; 8],
                parent_span_id: None,
                name: "api".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 1_001,
                duration_nanos: 200,
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                resource_attributes: Vec::new(),
                attributes: Vec::new(),
                events: Vec::new(),
                links: Vec::new(),
            }],
        };

        let bytes = trace_protobuf(&trace, 10).unwrap();
        let data = TracesData::decode(bytes.as_slice()).unwrap();
        let attrs = &data.resource_spans[0].resource.as_ref().unwrap().attributes;

        assert!(attrs.len() == 2);
        assert!(attrs[0].key == "deployment.zone");
        let Some(OtlpValue::ArrayValue(array)) = attrs[0]
            .value
            .as_ref()
            .and_then(|value| value.value.as_ref())
        else {
            panic!("expected deployment.zone array value");
        };
        assert!(
            array
                .values
                .iter()
                .map(|value| value.value.as_ref())
                .collect::<Vec<_>>()
                == vec![
                    Some(&OtlpValue::StringValue("a".into())),
                    Some(&OtlpValue::StringValue("b".into())),
                ]
        );
        assert!(attrs[1].key == "service.name");
    }

    #[tokio::test]
    async fn by_id_honors_protobuf_accept_header() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .header("accept", "application/protobuf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        // v2 returns a Tempo TraceByIDResponse wrapping the OTLP trace.
        let data = TraceByIdResponse::decode(bytes).unwrap().trace.unwrap();

        assert!(status == StatusCode::OK);
        assert!(content_type.as_deref() == Some("application/protobuf"));
        assert!(data.resource_spans.len() == 1);
        assert!(data.resource_spans[0].scope_spans[0].spans.len() == 2);
        assert!(data.resource_spans[0].scope_spans[0].spans[0].trace_id == vec![9; 16]);
    }

    #[tokio::test]
    async fn by_id_accept_header_matches_media_type_case_insensitively() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .header("accept", "Application/Protobuf; q=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();

        assert!(status == StatusCode::OK);
        assert!(content_type.as_deref() == Some("application/protobuf"));
        assert!(TraceByIdResponse::decode(bytes).unwrap().trace.is_some());
    }

    #[tokio::test]
    async fn buildinfo_reports_tempo_version() {
        // Grafana's Tempo datasource probes this to detect backend capabilities.
        let (status, body) = get_json("/api/status/buildinfo").await;
        assert!(status == StatusCode::OK);
        assert!(body["status"] == "success");
        assert!(body["data"]["version"].as_str() == Some("2.6.0"));
    }

    #[tokio::test]
    async fn trace_by_id_v1_returns_bare_otlp_protobuf() {
        // Grafana's backend falls back to the v1 endpoint and decodes it as
        // `tempopb.Trace` (wire-identical to a bare OTLP `TracesData`).
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .header("accept", "application/protobuf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let data = TracesData::decode(bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(content_type.as_deref() == Some("application/protobuf"));
        assert!(data.resource_spans[0].scope_spans[0].spans.len() == 2);
        assert!(data.resource_spans[0].scope_spans[0].spans[0].trace_id == vec![9; 16]);
    }

    #[tokio::test]
    async fn trace_by_id_v1_defaults_to_protobuf_without_accept() {
        // Tempo's v1 endpoint defaults to protobuf when no Accept is given.
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();

        assert!(content_type.as_deref() == Some("application/protobuf"));
        assert!(TracesData::decode(bytes).is_ok());
    }

    #[tokio::test]
    async fn trace_by_id_v1_honors_json_accept() {
        let resp = app()
            .oneshot(
                Request::builder()
                    .uri("/api/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .header("accept", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["status"] == "COMPLETE");
        assert!(body["trace"]["resourceSpans"].as_array().is_some());
    }

    #[tokio::test]
    async fn by_id_projects_instrumentation_scope() {
        let mut store = InMemorySpanStore::new();
        let mut span = span_at(9, 1, None, "a", 1_000);
        span.instrumentation_name = "tracer".into();
        span.instrumentation_version = "1.2.3".into();
        store.push_trace("tenant-a", "svc-a", "root-a", vec![span]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body["trace"]["resourceSpans"][0]["scopeSpans"][0]["scope"]
                == json!({
                    "name": "tracer",
                    "version": "1.2.3"
                })
        );
    }

    #[tokio::test]
    async fn by_id_projects_span_kind_and_status() {
        let mut store = InMemorySpanStore::new();
        let mut span = span_at(9, 1, None, "a", 1_000);
        span.kind = 2;
        span.status_code = 2;
        span.status_message = "boom".into();
        store.push_trace("tenant-a", "svc-a", "root-a", vec![span]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let span = &body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];

        assert!(status == StatusCode::OK);
        assert!(span["kind"] == "SPAN_KIND_SERVER");
        assert!(
            span["status"]
                == json!({
                    "code": "STATUS_CODE_ERROR",
                    "message": "boom",
                })
        );
    }

    #[tokio::test]
    async fn by_id_projects_events_and_links() {
        let mut store = InMemorySpanStore::new();
        let mut span = span_at(9, 1, None, "a", 1_000);
        span.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "exception".into(),
            attributes: vec![("exception.type".into(), AttrValue::Str("timeout".into()))],
        }];
        span.links = vec![LinkRef {
            trace_id: [7; 16],
            span_id: [8; 8],
            attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
        }];
        store.push_trace("tenant-a", "svc-a", "root-a", vec![span]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let span = &body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0];

        assert!(status == StatusCode::OK);
        assert!(
            span["events"]
                == json!([{
                    "timeUnixNano": "1050",
                    "name": "exception",
                    "attributes": [{
                        "key": "exception.type",
                        "value": {"stringValue": "timeout"}
                    }]
                }])
        );
        assert!(
            span["links"]
                == json!([{
                    "traceId": "BwcHBwcHBwcHBwcHBwcHBw==",
                    "spanId": "CAgICAgICAg=",
                    "attributes": [{
                        "key": "link.kind",
                        "value": {"stringValue": "retry"}
                    }]
                }])
        );
    }

    #[tokio::test]
    async fn by_id_returns_whole_trace_for_narrow_window_and_marks_complete() {
        // A by-id `start`/`end` is a candidate-selection HINT, not a hard
        // span-level filter (real Tempo returns the whole trace for a by-id
        // lookup). A trace whose spans straddle the window edge must come back
        // intact, COMPLETE — not clipped to only the in-window spans.
        let object_store = Arc::new(InMemory::new());
        let blocks = Arc::new(BlockStore::new(
            object_store.clone(),
            Url::parse("memory:///").unwrap(),
        ));
        // Two spans of trace `09..` in one block: span 1 starts at 1s (before the
        // window), span 2 at 5s (inside the window).
        let mut early = block_span_row(9, 1, "root");
        early.trace_start_unix_nano = 1_000_000_000;
        early.start_unix_nano = 1_000_000_000;
        let mut late = block_span_row(9, 2, "child");
        late.parent_span_id = Some([1; 8]);
        late.trace_start_unix_nano = 1_000_000_000;
        late.start_unix_nano = 5_000_000_000;
        let batch = encode_span_rows(&[early, late]).unwrap();
        let object_writer =
            ParquetObjectWriter::new(object_store.clone(), Path::from("blocks/straddle.parquet"));
        let mut writer =
            AsyncArrowWriter::try_new(object_writer, span_block_schema(), None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let mut trace_index = TraceIndex::new();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[9; 16]);
        trace_index.add_trace_block(
            "tenant-a",
            TraceBlockStats {
                object_key: "blocks/straddle.parquet".into(),
                min_ts: 1_000_000_000,
                max_ts: 5_000_000_000,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
            },
        );
        let store = CrabkaSpanStore::new(blocks, shared_index(trace_index), None);
        let app = router(Arc::new(TraceqlEngine::new(
            Arc::new(store),
            EngineOpts::default(),
        )));

        // Window [4s, 6s] covers only the late span by start, yet the by-id
        // lookup must return both spans with status COMPLETE.
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909?start=4&end=6")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let spans = body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .unwrap();

        assert!(status == StatusCode::OK);
        assert!(spans.len() == 2);
        assert!(body["status"] == "COMPLETE");
    }

    #[tokio::test]
    async fn by_id_rejects_invalid_start_and_end() {
        let (status, body) =
            get_text("/api/v2/traces/09090909090909090909090909090909?start=bogus").await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter start");

        let (status, body) =
            get_text("/api/v2/traces/09090909090909090909090909090909?end=bogus").await;
        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter end");
    }

    #[tokio::test]
    async fn by_id_rejects_end_before_start() {
        let (status, body) =
            get_text("/api/v2/traces/09090909090909090909090909090909?start=2&end=1").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "end must be >= start");
    }

    #[tokio::test]
    async fn by_id_marks_trace_partial_when_span_limit_truncates() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![span(9, 1, None, "a"), span(9, 2, Some(1), "b")],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router_with_config(
            engine,
            HttpConfig {
                max_trace_spans: 1,
                ..HttpConfig::default()
            },
        );
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/traces/09090909090909090909090909090909")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(body["status"] == "PARTIAL");
        assert!(body["message"] == "trace truncated after 1 spans");
        assert!(
            body["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
                .as_array()
                .unwrap()
                .len()
                == 1
        );
    }

    #[tokio::test]
    async fn search_tags_returns_legacy_tempo_shape() {
        let (status, body) = get_json("/api/search/tags?scope=span").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagNames": ["svc"],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_legacy_respects_query_filter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at_with_attrs(
                    1,
                    1,
                    None,
                    "a",
                    1_000,
                    vec![("env".into(), AttrValue::Str("prod".into()))],
                ),
                span_at_with_attrs(
                    1,
                    2,
                    Some(1),
                    "b",
                    2_000,
                    vec![("target".into(), AttrValue::Str("kept".into()))],
                ),
            ],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "c",
                3_000,
                vec![
                    ("env".into(), AttrValue::Str("dev".into())),
                    ("noise".into(), AttrValue::Str("dropped".into())),
                ],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search/tags?q=%7B%20.env%20%3D%20%22prod%22%20%7D&scope=span")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagNames": ["env", "svc", "target"],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_legacy_query_filter_returns_instrumentation_scope() {
        let mut store = InMemorySpanStore::new();
        let mut root = span_at_with_attrs(
            1,
            1,
            None,
            "root-svc",
            1_000,
            vec![("env".into(), AttrValue::Str("prod".into()))],
        );
        root.instrumentation_name = "tracer".into();
        root.instrumentation_version = "1.2.3".into();
        store.push_trace("tenant-a", "svc-a", "root-a", vec![root]);
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "dropped-svc",
                3_000,
                vec![("env".into(), AttrValue::Str("dev".into()))],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search/tags?q=%7B%20.env%20%3D%20%22prod%22%20%7D&scope=instrumentation")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagNames": ["instrumentation:name", "instrumentation:version"],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_rejects_invalid_scope_parameter() {
        let (status, body) = get_text("/api/search/tags?scope=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid scope");
    }

    #[tokio::test]
    async fn search_tags_rejects_invalid_time_bounds() {
        let (status, body) = get_text("/api/search/tags?start=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter start");
    }

    #[tokio::test]
    async fn search_tags_v2_returns_scoped_tempo_shape() {
        let (status, body) = get_json("/api/v2/search/tags").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [
                    {
                        "name": "resource",
                        "tags": ["service.name"]
                    },
                    {
                        "name": "span",
                        "tags": ["svc"]
                    },
                    {
                        "name": "intrinsic",
                        "tags": INTRINSIC_TAGS
                    },
                    {
                        "name": "event",
                        "tags": ["event:name", "event:timeSinceStart"]
                    },
                    {
                        "name": "link",
                        "tags": ["link:spanID", "link:traceID"]
                    },
                    {
                        "name": "instrumentation",
                        "tags": ["instrumentation:name", "instrumentation:version"]
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_respects_scope_parameter() {
        let (status, body) = get_json("/api/v2/search/tags?scope=span").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "span",
                    "tags": ["svc"]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_returns_intrinsic_scope() {
        let (status, body) = get_json("/api/v2/search/tags?scope=intrinsic").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "intrinsic",
                    "tags": [
                        "span:childCount",
                        "span:duration",
                        "span:id",
                        "span:kind",
                        "span:name",
                        "span:Parent",
                        "span:nestedSetLeft",
                        "span:nestedSetParent",
                        "span:nestedSetRight",
                        "span:parentID",
                        "span:status",
                        "span:statusMessage",
                        "trace:duration",
                        "trace:id",
                        "trace:rootName",
                        "trace:rootService"
                    ]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_returns_event_scope() {
        let (status, body) = get_json("/api/v2/search/tags?scope=event").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "event",
                    "tags": ["event:name", "event:timeSinceStart"]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_returns_link_scope() {
        let (status, body) = get_json("/api/v2/search/tags?scope=link").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "link",
                    "tags": ["link:spanID", "link:traceID"]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_returns_instrumentation_scope() {
        let (status, body) = get_json("/api/v2/search/tags?scope=instrumentation").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "instrumentation",
                    "tags": ["instrumentation:name", "instrumentation:version"]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_rejects_invalid_scope_parameter() {
        let (status, body) = get_text("/api/v2/search/tags?scope=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid scope");
    }

    #[tokio::test]
    async fn search_tags_v2_rejects_invalid_time_bounds() {
        let (status, body) = get_text("/api/v2/search/tags?start=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter start");
    }

    #[tokio::test]
    async fn search_tags_v2_respects_query_filter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at_with_attrs(
                    1,
                    1,
                    None,
                    "a",
                    1_000,
                    vec![("env".into(), AttrValue::Str("prod".into()))],
                ),
                span_at_with_attrs(
                    1,
                    2,
                    Some(1),
                    "b",
                    2_000,
                    vec![("target".into(), AttrValue::Str("kept".into()))],
                ),
            ],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "c",
                3_000,
                vec![
                    ("env".into(), AttrValue::Str("dev".into())),
                    ("noise".into(), AttrValue::Str("dropped".into())),
                ],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/search/tags?q=%7B%20.env%20%3D%20%22prod%22%20%7D&scope=span")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [{
                    "name": "span",
                    "tags": ["env", "svc", "target"]
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn search_tags_v2_query_filter_returns_event_and_link_scopes() {
        let mut store = InMemorySpanStore::new();
        let mut root = span_at_with_attrs(
            1,
            1,
            None,
            "root-svc",
            1_000,
            vec![("env".into(), AttrValue::Str("prod".into()))],
        );
        root.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "exception".into(),
            attributes: vec![("exception.type".into(), AttrValue::Str("timeout".into()))],
        }];
        root.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
        }];
        store.push_trace("tenant-a", "svc-a", "root-a", vec![root]);
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "c",
                3_000,
                vec![("env".into(), AttrValue::Str("dev".into()))],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/search/tags?q=%7B%20.env%20%3D%20%22prod%22%20%7D")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "scopes": [
                    {
                        "name": "resource",
                        "tags": ["service.name"]
                    },
                    {
                        "name": "span",
                        "tags": ["env", "svc"]
                    },
                    {
                        "name": "intrinsic",
                        "tags": [
                            "span:childCount",
                            "span:duration",
                            "span:id",
                            "span:kind",
                            "span:name",
                            "span:Parent",
                            "span:nestedSetLeft",
                            "span:nestedSetParent",
                            "span:nestedSetRight",
                            "span:parentID",
                            "span:status",
                            "span:statusMessage",
                            "trace:duration",
                            "trace:id",
                            "trace:rootName",
                            "trace:rootService"
                        ]
                    },
                    {
                        "name": "event",
                        "tags": ["event:name", "event:timeSinceStart", "exception.type"]
                    },
                    {
                        "name": "link",
                        "tags": ["link:spanID", "link:traceID", "link.kind"]
                    },
                    {
                        "name": "instrumentation",
                        "tags": ["instrumentation:name", "instrumentation:version"]
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tags_v2_rejects_invalid_query_filter_as_bad_request() {
        let (status, body) = get_text("/api/v2/search/tags?q=%7B").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body.contains("parse error"));
    }

    #[tokio::test]
    async fn search_tag_values_returns_legacy_tempo_shape() {
        let (status, body) = get_json("/api/search/tag/service.name/values").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": ["svc-a"],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_legacy_respects_query_filter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at_with_attrs(
                    1,
                    1,
                    None,
                    "a",
                    1_000,
                    vec![
                        ("env".into(), AttrValue::Str("prod".into())),
                        ("target".into(), AttrValue::Str("kept".into())),
                    ],
                ),
                span_at_with_attrs(
                    1,
                    2,
                    Some(1),
                    "b",
                    2_000,
                    vec![("target".into(), AttrValue::Str("also-kept".into()))],
                ),
            ],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "c",
                3_000,
                vec![
                    ("env".into(), AttrValue::Str("dev".into())),
                    ("target".into(), AttrValue::Str("dropped".into())),
                ],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search/tag/target/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": ["also-kept", "kept"],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_rejects_invalid_time_bounds() {
        let (status, body) = get_text("/api/search/tag/service.name/values?end=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter end");
    }

    #[tokio::test]
    async fn search_tag_values_v2_returns_typed_tempo_shape() {
        let (status, body) = get_json("/api/v2/search/tag/.svc/values").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "string",
                        "value": "a"
                    },
                    {
                        "type": "string",
                        "value": "b"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_accepts_resource_scope_prefix() {
        let (status, body) = get_json("/api/v2/search/tag/resource.service.name/values").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "svc-a"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_returns_intrinsic_values() {
        let (status, body) = get_json("/api/v2/search/tag/span:name/values").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "span"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let (status, body) = get_json("/api/v2/search/tag/trace:rootService/values").await;
        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "svc-a"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_rejects_invalid_query_filter_as_bad_request() {
        let (status, body) = get_text("/api/v2/search/tag/.svc/values?q=%7B").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body.contains("parse error"));
    }

    #[tokio::test]
    async fn search_tag_values_v2_rejects_invalid_time_bounds() {
        let (status, body) = get_text("/api/v2/search/tag/.svc/values?end=bogus").await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body == "invalid query parameter end");
    }

    #[tokio::test]
    async fn search_tag_values_v2_respects_query_filter() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at_with_attrs(
                    1,
                    1,
                    None,
                    "a",
                    1_000,
                    vec![("env".into(), AttrValue::Str("prod".into()))],
                ),
                span_at_with_attrs(
                    1,
                    2,
                    Some(1),
                    "b",
                    2_000,
                    vec![("target".into(), AttrValue::Str("kept".into()))],
                ),
            ],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "b",
                2_000,
                vec![("env".into(), AttrValue::Str("dev".into()))],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v2/search/tag/.svc/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "string",
                        "value": "a"
                    },
                    {
                        "type": "string",
                        "value": "b"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/search/tag/.target/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "kept"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_query_filter_accepts_resource_scope_prefix() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![span_at_with_attrs(
                1,
                1,
                None,
                "span-svc",
                1_000,
                vec![("env".into(), AttrValue::Str("prod".into()))],
            )],
        );
        store.push_trace(
            "tenant-a",
            "svc-b",
            "root-b",
            vec![span_at_with_attrs(
                2,
                1,
                None,
                "dropped-svc",
                2_000,
                vec![("env".into(), AttrValue::Str("dev".into()))],
            )],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/resource.service.name/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "svc-a"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_query_filter_honors_time_bounds() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "tenant-a",
            "svc-a",
            "root-a",
            vec![
                span_at_with_attrs(
                    1,
                    1,
                    None,
                    "a",
                    1_000_000_000,
                    vec![
                        ("env".into(), AttrValue::Str("prod".into())),
                        ("target".into(), AttrValue::Str("inside".into())),
                    ],
                ),
                span_at_with_attrs(
                    1,
                    2,
                    Some(1),
                    "b",
                    5_000_000_000,
                    vec![("target".into(), AttrValue::Str("outside".into()))],
                ),
            ],
        );
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/.target/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D&start=0&end=2",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "inside"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn search_tag_values_v2_query_filter_returns_intrinsic_values() {
        let mut store = InMemorySpanStore::new();
        let mut root = span_at_with_attrs(
            1,
            1,
            None,
            "root-svc",
            1_000,
            vec![("env".into(), AttrValue::Str("prod".into()))],
        );
        root.name = "root".into();
        let mut child = span_at_with_attrs(1, 2, Some(1), "child-svc", 2_000, Vec::new());
        child.name = "child".into();
        child.instrumentation_name = "tracer".into();
        store.push_trace("tenant-a", "svc-a", "root-a", vec![root, child]);
        let mut dropped = span_at_with_attrs(
            2,
            1,
            None,
            "dropped-svc",
            3_000,
            vec![("env".into(), AttrValue::Str("dev".into()))],
        );
        dropped.name = "dropped".into();
        store.push_trace("tenant-a", "svc-b", "root-b", vec![dropped]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:name/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "string",
                        "value": "child"
                    },
                    {
                        "type": "string",
                        "value": "root"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/trace:duration/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "duration",
                    "value": "1200"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:status/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "int",
                    "value": "0"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:nestedSetLeft/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "int",
                        "value": "1"
                    },
                    {
                        "type": "int",
                        "value": "2"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:Parent/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "int",
                        "value": "-1"
                    },
                    {
                        "type": "int",
                        "value": "1"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/span:childCount/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "int",
                        "value": "0"
                    },
                    {
                        "type": "int",
                        "value": "1"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/instrumentation:name/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "tracer"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_query_filter_returns_event_and_link_values() {
        let mut store = InMemorySpanStore::new();
        let mut root = span_at_with_attrs(
            1,
            1,
            None,
            "root-svc",
            1_000,
            vec![("env".into(), AttrValue::Str("prod".into()))],
        );
        root.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "exception".into(),
            attributes: Vec::new(),
        }];
        root.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: Vec::new(),
        }];
        store.push_trace("tenant-a", "svc-a", "root-a", vec![root]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/event:name/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "exception"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/link:traceID/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "09090909090909090909090909090909"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn search_tag_values_v2_query_filter_returns_event_and_link_attribute_values() {
        let mut store = InMemorySpanStore::new();
        let mut root = span_at_with_attrs(
            1,
            1,
            None,
            "root-svc",
            1_000,
            vec![("env".into(), AttrValue::Str("prod".into()))],
        );
        root.events = vec![EventRef {
            time_since_start_nano: 50,
            name: "exception".into(),
            attributes: vec![("cache.key".into(), AttrValue::Str("users".into()))],
        }];
        root.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: vec![("link.kind".into(), AttrValue::Str("retry".into()))],
        }];
        store.push_trace("tenant-a", "svc-a", "root-a", vec![root]);
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/cache.key/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "users"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/event.cache.key/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "users"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/link.kind/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "retry"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );

        let resp = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/v2/search/tag/link.link.kind/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D",
                    )
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [{
                    "type": "string",
                    "value": "retry"
                }],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }

    #[tokio::test]
    async fn search_tag_values_v2_query_filter_scans_past_default_search_limit() {
        let mut store = InMemorySpanStore::new();
        for trace in 1..=21 {
            let target = if trace == 21 { "late" } else { "early" };
            store.push_trace(
                "tenant-a",
                "svc-a",
                &format!("root-{trace}"),
                vec![span_at_with_attrs(
                    trace,
                    1,
                    None,
                    "a",
                    i64::from(trace),
                    vec![
                        ("env".into(), AttrValue::Str("prod".into())),
                        ("target".into(), AttrValue::Str(target.into())),
                    ],
                )],
            );
        }
        let engine = Arc::new(TraceqlEngine::new(Arc::new(store), EngineOpts::default()));
        let app = router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v2/search/tag/.target/values?q=%7B%20.env%20%3D%20%22prod%22%20%7D")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(status == StatusCode::OK);
        assert!(
            body == json!({
                "tagValues": [
                    {
                        "type": "string",
                        "value": "early"
                    },
                    {
                        "type": "string",
                        "value": "late"
                    }
                ],
                "metrics": {
                    "inspectedBytes": "0"
                }
            })
        );
    }
}
