//! axum HTTP surface for the query-frontend: the Tempo query endpoints, tenant
//! extraction, the v2 by-id `status`/`message` envelope, and time-param parsing
//! that matches the querier's contract (`start`/`end` are epoch **seconds**,
//! fractional allowed).
//!
//! The router is generic over the backend/catalog pair so tests drive
//! `MockQuerier`+`MockCatalog` and production binds `HttpQuerier`+
//! `TraceIndexCatalog`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::json;

use crate::frontend::QueryFrontend;
use crate::frontend::backend::{BackendError, QuerierBackend};
use crate::frontend::job::BlockCatalog;
use crate::frontend::merge::TraceStatus;
use crate::frontend::wire::parse_hex16;

const TENANT_HEADER: &str = "x-scope-orgid";

/// Render a propagated backend failure as the client response, preserving the
/// upstream querier's status code and error text (so an invalid `TraceQL` query
/// surfaces as the querier's `4xx` body rather than a silent empty `200`).
fn backend_error_response(err: &BackendError) -> Response {
    let (status, body) = err.to_http();
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
    (code, body).into_response()
}

/// Build the query-frontend router for any backend/catalog pair.
pub fn router_with_backend<B, C>(qf: Arc<QueryFrontend<B, C>>) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/search", get(search::<B, C>))
        .route("/api/v2/traces/{trace_id}", get(trace_by_id::<B, C>))
        .route("/api/v2/search/tags", get(search_tags_v2::<B, C>))
        .route(
            "/api/v2/search/tag/{tag}/values",
            get(search_tag_values_v2::<B, C>),
        )
        .route("/api/metrics/query_range", get(query_range::<B, C>))
        .route("/api/metrics/query", get(query_instant::<B, C>))
        .with_state(qf)
}

async fn echo() -> &'static str {
    "echo"
}

async fn ready() -> &'static str {
    "ready"
}

async fn search<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let query = match search_query(&uri) {
        Ok(Some(q)) => q,
        Ok(None) => return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response(),
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let limit = bounded_count(&uri, "limit", qf.default_limit());
    let spss = bounded_count(&uri, "spss", qf.default_spss());

    match qf
        .search(&tenant, &query, start_ns, end_ns, limit, spss)
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

async fn trace_by_id<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    }
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let tid = parse_hex16(&trace_id);
    let (trace, _metrics, status) = match qf.trace_by_id(&tenant, tid, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };

    let Some(trace) = trace else {
        return (StatusCode::NOT_FOUND, "trace not found").into_response();
    };
    // v2 envelope: { trace, status, message }. Per the querier's contract the
    // by-id endpoint does NOT carry a metrics block.
    let message = match status {
        TraceStatus::Partial => "trace exceeds max size; returned partially".to_string(),
        TraceStatus::Complete => String::new(),
    };
    Json(json!({
        "trace": trace.trace,
        "status": status.as_str(),
        "message": message,
    }))
    .into_response()
}

async fn search_tags_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
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
    let (tags, _metrics) = match qf.tag_names(&tenant, scope, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let scopes: Vec<_> = tags
        .iter()
        .map(|st| json!({ "name": scope_name(st.scope), "tags": &st.tags }))
        .collect();
    Json(json!({ "scopes": scopes, "metrics": { "inspectedBytes": "0" } })).into_response()
}

async fn search_tag_values_v2<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    Path(tag): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let (values, _metrics) = match qf.tag_values(&tenant, &tag, start_ns, end_ns).await {
        Ok(out) => out,
        Err(err) => return backend_error_response(&err),
    };
    let tag_values: Vec<_> = values
        .iter()
        .map(|v| json!({ "type": &v.type_, "value": &v.value }))
        .collect();
    Json(json!({ "tagValues": tag_values, "metrics": { "inspectedBytes": "0" } })).into_response()
}

async fn query_range<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let step_ns = match required_step(&uri) {
        Ok(step) => step,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            start_ns,
            end_ns,
            step_ns,
            false,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

async fn query_instant<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    headers: HeaderMap,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let tenant = tenant(&headers);
    let Some(query) = metrics_query_param(&uri) else {
        return (StatusCode::BAD_REQUEST, "missing query parameter q").into_response();
    };
    // Instant query: a window via start/end, else a single `time` point.
    let (start_ns, end_ns) =
        if query_param(&uri, "start").is_some() || query_param(&uri, "end").is_some() {
            match required_time_bounds(&uri) {
                Ok(bounds) => bounds,
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            }
        } else {
            let ts = match optional_seconds(&uri, "time") {
                Ok(value) => value.unwrap_or(0),
                Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
            };
            (ts, ts)
        };
    let step_ns = end_ns.saturating_sub(start_ns).max(1);
    let exemplar_limit = exemplar_limit(&uri);
    match qf
        .metrics_query(
            &tenant,
            &query,
            start_ns,
            end_ns,
            step_ns,
            true,
            exemplar_limit,
        )
        .await
    {
        Ok(resp) => Json(resp).into_response(),
        Err(err) => backend_error_response(&err),
    }
}

// --- param helpers (mirror the querier's contract) --------------------------

fn tenant(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or("anonymous")
        .to_string()
}

fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

/// The `TraceQL` metrics query string. Tempo accepts both `q` and `query` on
/// the metrics endpoints: the Explore `TraceQL` editor and the HTTP API send
/// `q`, while the Grafana Tempo datasource powering the Traces Drilldown app
/// sends `query`. Accept either, preferring `q`.
fn metrics_query_param(uri: &Uri) -> Option<String> {
    query_param(uri, "q").or_else(|| query_param(uri, "query"))
}

/// `q` (`TraceQL`) or the legacy `tags` logfmt form.
fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(q) = query_param(uri, "q") {
        return Ok(Some(q));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}

fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts: Vec<String> = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|(key, value)| {
            // The key is interpolated unquoted as a TraceQL attribute reference,
            // so a key carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys.
            key_is_safe_attribute(&key).then(|| {
                let field = if key.contains(':') {
                    key
                } else {
                    format!(".{}", key.strip_prefix('.').unwrap_or(&key))
                };
                let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("{field} = \"{escaped}\"")
            })
        })
        .collect::<Option<Vec<String>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}

/// A legacy `tags=` key is a safe `TraceQL` attribute reference only if it is
/// made of identifier characters (alphanumerics plus `._:-`). Anything else
/// (`{`, `}`, `"`, `\`, `|`, `&`, `=`, whitespace, …) could inject query
/// structure once interpolated unquoted into the generated `TraceQL`. Kept in
/// sync with the querier's `key_is_safe_attribute`.
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

fn bounded_count(uri: &Uri, key: &str, default: usize) -> usize {
    query_param(uri, key)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn required_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = required_seconds(uri, "start")?;
    let end_ns = required_seconds(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn optional_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = optional_seconds(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn required_seconds(uri: &Uri, key: &str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

fn optional_seconds(uri: &Uri, key: &str) -> Result<Option<i64>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
        })
        .transpose()
}

fn required_step(uri: &Uri) -> Result<i64, String> {
    let Some(value) = query_param(uri, "step") else {
        return Err("missing query parameter step".to_string());
    };
    let step = parse_step_to_ns(&value).ok_or("invalid step")?;
    if step <= 0 {
        return Err("step must be positive".to_string());
    }
    Ok(step)
}

/// `step` may be bare epoch-seconds OR a Go-duration like `30s`/`5m`/`100ms`
/// (Grafana's Tempo datasource sends the duration form). Mirrors the querier's
/// `parse_step_to_ns` so the frontend accepts exactly what the querier accepts —
/// without it the frontend would `400` a query the querier handles.
fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}

/// Parse a Go-style duration (`1h`, `5m`, `30s`, `100ms`, `1m30s`, fractional
/// like `1.5s`) to nanoseconds. Kept in sync with the querier's parser.
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
        let multiplier: u128 = match unit {
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
    let fraction_value = fraction
        .parse::<u128>()
        .map_err(|_| format!("invalid number {number:?}"))?;
    let scale = (0..fraction.len())
        .try_fold(1_u128, |acc, _| acc.checked_mul(10))
        .ok_or_else(|| "duration out of range".to_string())?;
    let fraction_ns = fraction_value
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?
        / scale;
    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| "duration out of range".to_string())
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
        format!("{fraction:0<9}").parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}

fn exemplar_limit(uri: &Uri) -> Option<usize> {
    match query_param(uri, "exemplars").as_deref() {
        Some("false" | "0") => Some(0),
        Some("true") | None => None,
        Some(value) => value.parse().ok().or(None),
    }
}

fn scope_param(uri: &Uri) -> Result<Option<crabka_traceql::TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|s| parse_scope(&s).ok_or("invalid scope"))
        .transpose()
}

fn parse_scope(name: &str) -> Option<crabka_traceql::TagScope> {
    Some(match name {
        "resource" => crabka_traceql::TagScope::Resource,
        "span" => crabka_traceql::TagScope::Span,
        "intrinsic" => crabka_traceql::TagScope::Intrinsic,
        "event" => crabka_traceql::TagScope::Event,
        "link" => crabka_traceql::TagScope::Link,
        "instrumentation" => crabka_traceql::TagScope::Instrumentation,
        _ => return None,
    })
}

fn scope_name(scope: crabka_traceql::TagScope) -> &'static str {
    match scope {
        crabka_traceql::TagScope::Resource => "resource",
        crabka_traceql::TagScope::Span => "span",
        crabka_traceql::TagScope::Intrinsic => "intrinsic",
        crabka_traceql::TagScope::Event => "event",
        crabka_traceql::TagScope::Link => "link",
        crabka_traceql::TagScope::Instrumentation => "instrumentation",
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn step_accepts_seconds_and_go_durations() {
        for (input, want) in [
            // Bare epoch-seconds (what the frontend already accepted).
            ("30", Some(30_000_000_000)),
            // Go-duration forms Grafana's Tempo datasource actually sends.
            ("30s", Some(30_000_000_000)),
            ("5m", Some(300_000_000_000)),
            ("1h", Some(3_600_000_000_000)),
            ("100ms", Some(100_000_000)),
            ("1m30s", Some(90_000_000_000)),
            // Garbage is still rejected.
            ("nonsense", None),
            ("30q", None),
        ] {
            check!(parse_step_to_ns(input) == want);
        }
    }

    #[test]
    fn tags_to_traceql_rejects_keys_with_metacharacters() {
        // Benign keys convert to a properly-quoted attribute match.
        assert!(tags_to_traceql("svc=b") == Some("{ .svc = \"b\" }".to_string()));
        assert!(tags_to_traceql("span:name=op") == Some("{ span:name = \"op\" }".to_string()));
        // A key carrying TraceQL-significant characters injects structure when
        // interpolated unquoted, so it is rejected.
        assert!(tags_to_traceql("a}=c").is_none());
        assert!(tags_to_traceql("a\"b=c").is_none());
        // The value side stays safely quoted even with metacharacters.
        assert!(tags_to_traceql("svc=a\"}||x") == Some("{ .svc = \"a\\\"}||x\" }".to_string()));
    }
}
