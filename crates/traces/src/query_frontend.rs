//! Query-frontend role: queue and shard Tempo search requests across queriers.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use reqwest::Url;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryTier {
    Backend,
    Live,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryShard {
    pub tier: QueryTier,
    pub start_ns: i64,
    pub end_ns: i64,
}

#[derive(Clone, Debug)]
pub struct QueryFrontendConfig {
    pub querier_url: Url,
    pub live_frontier_ns: Option<i64>,
    pub max_queue_depth: usize,
}

impl QueryFrontendConfig {
    pub fn new(querier_url: &str) -> Result<Self, url::ParseError> {
        Ok(Self {
            querier_url: Url::parse(querier_url)?,
            live_frontier_ns: None,
            max_queue_depth: 128,
        })
    }
}

#[derive(Clone)]
struct AppState {
    cfg: QueryFrontendConfig,
    http: reqwest::Client,
    permits: Arc<Semaphore>,
}

pub fn router(cfg: QueryFrontendConfig) -> Router {
    let max_queue_depth = cfg.max_queue_depth.max(1);
    Router::new()
        .route("/api/echo", get(echo))
        .route("/ready", get(ready))
        .route("/status", get(ready))
        .route("/api/search", get(search))
        .route("/api/search/tags", get(proxy))
        .route("/api/v2/search/tags", get(proxy))
        .route("/api/search/tag/{tag}/values", get(proxy))
        .route("/api/v2/search/tag/{tag}/values", get(proxy))
        .route("/api/metrics/query_range", get(query_range))
        .route("/api/metrics/query", get(proxy))
        .route("/api/v2/traces/{trace_id}", get(proxy))
        .with_state(AppState {
            cfg,
            http: reqwest::Client::new(),
            permits: Arc::new(Semaphore::new(max_queue_depth)),
        })
}

#[must_use]
pub fn plan_time_shards(
    start_ns: i64,
    end_ns: i64,
    live_frontier_ns: Option<i64>,
) -> Vec<QueryShard> {
    if end_ns < start_ns {
        return Vec::new();
    }

    let Some(frontier) = live_frontier_ns else {
        return vec![QueryShard {
            tier: QueryTier::Backend,
            start_ns,
            end_ns,
        }];
    };

    if end_ns < frontier {
        return vec![QueryShard {
            tier: QueryTier::Backend,
            start_ns,
            end_ns,
        }];
    }
    if start_ns >= frontier {
        return vec![QueryShard {
            tier: QueryTier::Live,
            start_ns,
            end_ns,
        }];
    }

    vec![
        QueryShard {
            tier: QueryTier::Backend,
            start_ns,
            end_ns: frontier.saturating_sub(1),
        },
        QueryShard {
            tier: QueryTier::Live,
            start_ns: frontier,
            end_ns,
        },
    ]
}

async fn echo() -> &'static str {
    "echo"
}

async fn ready() -> &'static str {
    "ready"
}

async fn search(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut merged_traces = Vec::new();
    let mut metrics = json!({
        "totalBlocks": 0,
        "inspectedTraces": 0,
        "inspectedBytes": 0,
    });

    for shard in plan_time_shards(start_ns, end_ns, state.cfg.live_frontier_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, shard)
            .send()
            .await
        else {
            return (StatusCode::BAD_GATEWAY, "querier request failed").into_response();
        };
        let status = resp.status();
        let Ok(bytes) = resp.bytes().await else {
            return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
        };
        if !status.is_success() {
            let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            return (status, bytes).into_response();
        }
        let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
            return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
        };
        if let Some(traces) = body.get("traces").and_then(Value::as_array) {
            for trace in traces {
                merge_trace(&mut merged_traces, trace.clone());
            }
        }
        merge_metrics(&mut metrics, body.get("metrics"));
    }

    axum::Json(json!({
        "traces": merged_traces,
        "metrics": metrics,
    }))
    .into_response()
}

async fn query_range(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let start_ns = match required_seconds_param(&uri, "start") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let end_ns = match required_seconds_param(&uri, "end") {
        Ok(value) => value,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut merged_series = Vec::new();

    for shard in plan_time_shards(start_ns, end_ns, state.cfg.live_frontier_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, shard)
            .send()
            .await
        else {
            return (StatusCode::BAD_GATEWAY, "querier request failed").into_response();
        };
        let status = resp.status();
        let Ok(bytes) = resp.bytes().await else {
            return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
        };
        if !status.is_success() {
            let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            return (status, bytes).into_response();
        }
        let Ok(body) = serde_json::from_slice::<Value>(&bytes) else {
            return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
        };
        if let Some(series) = body.get("series").and_then(Value::as_array) {
            for next in series {
                merge_metric_series(&mut merged_series, next.clone());
            }
        }
    }

    axum::Json(json!({ "series": merged_series })).into_response()
}

async fn proxy(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let Ok(resp) = build_plain_querier_request(&state, &headers, &uri)
        .send()
        .await
    else {
        return (StatusCode::BAD_GATEWAY, "querier request failed").into_response();
    };
    let status = resp.status();
    let content_type = resp.headers().get(header::CONTENT_TYPE).cloned();
    let Ok(body) = resp.bytes().await else {
        return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
    };
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut response = (status, body).into_response();
    if let Some(content_type) = content_type {
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
    }
    response
}

fn merge_metric_series(series: &mut Vec<Value>, next: Value) {
    let Some(labels) = next.get("labels") else {
        series.push(next);
        return;
    };
    let Some(existing) = series
        .iter_mut()
        .find(|existing| existing.get("labels") == Some(labels))
    else {
        series.push(next);
        return;
    };

    if let Some(next_points) = next.get("points").and_then(Value::as_array)
        && let Some(existing_points) = existing.get_mut("points").and_then(Value::as_array_mut)
    {
        existing_points.extend(next_points.iter().cloned());
    }
    if let Some(next_exemplars) = next.get("exemplars").and_then(Value::as_array)
        && let Some(existing_exemplars) =
            existing.get_mut("exemplars").and_then(Value::as_array_mut)
    {
        existing_exemplars.extend(next_exemplars.iter().cloned());
    }
}

fn merge_trace(traces: &mut Vec<Value>, trace: Value) {
    let Some(trace_id) = trace.get("traceID").and_then(Value::as_str) else {
        traces.push(trace);
        return;
    };
    let Some(existing) = traces
        .iter_mut()
        .find(|existing| existing.get("traceID").and_then(Value::as_str) == Some(trace_id))
    else {
        traces.push(trace);
        return;
    };

    let Some(new_span_sets) = trace.get("spanSets").and_then(Value::as_array) else {
        return;
    };
    if let Some(existing_span_sets) = existing.get_mut("spanSets").and_then(Value::as_array_mut) {
        merge_span_sets(existing_span_sets, new_span_sets);
    }
}

fn merge_span_sets(existing_span_sets: &mut Vec<Value>, new_span_sets: &[Value]) {
    for span_set in new_span_sets {
        let Some(new_spans) = span_set.get("spans").and_then(Value::as_array) else {
            existing_span_sets.push(span_set.clone());
            continue;
        };
        if new_spans
            .iter()
            .all(|span| span_id_seen(existing_span_sets, span))
        {
            continue;
        }

        let Some(existing_span_set) = existing_span_sets.first_mut() else {
            let mut span_set = span_set.clone();
            ensure_matched_count(&mut span_set);
            existing_span_sets.push(span_set);
            continue;
        };
        add_matched_count(existing_span_set, span_set);
        let Some(existing_spans) = existing_span_set
            .get_mut("spans")
            .and_then(Value::as_array_mut)
        else {
            existing_span_sets.push(span_set.clone());
            continue;
        };
        for span in new_spans {
            if !existing_spans
                .iter()
                .any(|existing| same_span_id(existing, span))
            {
                existing_spans.push(span.clone());
            }
        }
        ensure_matched_count(existing_span_set);
    }
}

fn span_id_seen(span_sets: &[Value], span: &Value) -> bool {
    span_sets.iter().any(|span_set| {
        span_set
            .get("spans")
            .and_then(Value::as_array)
            .is_some_and(|spans| spans.iter().any(|existing| same_span_id(existing, span)))
    })
}

fn same_span_id(lhs: &Value, rhs: &Value) -> bool {
    let Some(lhs) = lhs.get("spanID").and_then(Value::as_str) else {
        return false;
    };
    rhs.get("spanID").and_then(Value::as_str) == Some(lhs)
}

fn add_matched_count(existing: &mut Value, incoming: &Value) {
    let lhs = span_set_matched(existing).unwrap_or_else(|| span_count(existing));
    let rhs = span_set_matched(incoming).unwrap_or_else(|| span_count(incoming));
    existing["matched"] = json!(lhs.saturating_add(rhs));
}

fn ensure_matched_count(span_set: &mut Value) {
    if span_set_matched(span_set).is_none() {
        span_set["matched"] = json!(span_count(span_set));
    }
}

fn span_set_matched(span_set: &Value) -> Option<u64> {
    span_set.get("matched").and_then(Value::as_u64)
}

fn span_count(span_set: &Value) -> u64 {
    let Some(count) = span_set
        .get("spans")
        .and_then(Value::as_array)
        .map(Vec::len)
    else {
        return 0;
    };
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn build_querier_request(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
    shard: QueryShard,
) -> reqwest::RequestBuilder {
    let mut url = state.cfg.querier_url.clone();
    url.set_path(uri.path());
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        {
            if key != "start" && key != "end" {
                pairs.append_pair(&key, &value);
            }
        }
        pairs.append_pair("start", &format_ns_as_seconds(shard.start_ns));
        pairs.append_pair("end", &format_ns_as_seconds(shard.end_ns));
    }

    let mut req = state.http.get(url);
    if let Some(tenant) = headers.get("x-scope-orgid") {
        req = req.header("x-scope-orgid", tenant.clone());
    }
    req = forward_accept(req, headers);
    req.header(
        "x-crabka-query-tier",
        match shard.tier {
            QueryTier::Backend => "backend",
            QueryTier::Live => "live",
        },
    )
}

fn build_plain_querier_request(
    state: &AppState,
    headers: &HeaderMap,
    uri: &Uri,
) -> reqwest::RequestBuilder {
    let mut url = state.cfg.querier_url.clone();
    url.set_path(uri.path());
    url.set_query(uri.query());

    let mut req = state.http.get(url);
    if let Some(tenant) = headers.get("x-scope-orgid") {
        req = req.header("x-scope-orgid", tenant.clone());
    }
    forward_accept(req, headers)
}

fn merge_metrics(metrics: &mut Value, next: Option<&Value>) {
    let Some(next) = next.and_then(Value::as_object) else {
        return;
    };
    let Some(current) = metrics.as_object_mut() else {
        return;
    };
    for key in ["totalBlocks", "inspectedTraces", "inspectedBytes"] {
        let lhs = current.get(key).and_then(Value::as_u64).unwrap_or(0);
        let rhs = next.get(key).and_then(Value::as_u64).unwrap_or(0);
        current.insert(key.to_string(), json!(lhs + rhs));
    }
}

fn forward_accept(req: reqwest::RequestBuilder, headers: &HeaderMap) -> reqwest::RequestBuilder {
    if let Some(accept) = headers.get(header::ACCEPT) {
        req.header(header::ACCEPT, accept.clone())
    } else {
        req
    }
}

fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

fn required_seconds_param(uri: &Uri, key: &'static str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    if let Ok(seconds) = value.parse::<i64>() {
        return seconds.checked_mul(1_000_000_000);
    }

    let (whole, frac) = value.split_once('.')?;
    if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let whole = whole.parse::<i64>().ok()?;
    if whole < 0 {
        return None;
    }
    let mut nanos = frac.parse::<i64>().ok()?;
    for _ in frac.len()..9 {
        nanos = nanos.checked_mul(10)?;
    }
    whole.checked_mul(1_000_000_000)?.checked_add(nanos)
}

fn format_ns_as_seconds(ns: i64) -> String {
    let seconds = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    if nanos == 0 {
        return seconds.to_string();
    }
    let mut frac = format!("{nanos:09}");
    while frac.ends_with('0') {
        frac.pop();
    }
    format!("{seconds}.{frac}")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn merge_span_sets_preserves_total_matched_count_across_shards() {
        let mut existing = vec![json!({
            "spans": [{ "spanID": "01" }],
            "matched": 4,
        })];
        let incoming = vec![json!({
            "spans": [{ "spanID": "02" }],
            "matched": 3,
        })];

        merge_span_sets(&mut existing, &incoming);

        assert!(existing[0]["matched"] == 7);
        assert!(
            existing[0]["spans"]
                == json!([
                    { "spanID": "01" },
                    { "spanID": "02" },
                ])
        );
    }

    #[test]
    fn parses_fractional_epoch_seconds_to_nanoseconds() {
        assert!(parse_seconds_to_ns("1.4") == Some(1_400_000_000));
        assert!(parse_seconds_to_ns("1.000000001") == Some(1_000_000_001));
    }
}
