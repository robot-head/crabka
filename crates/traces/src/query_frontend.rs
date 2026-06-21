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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendRowGroup {
    pub index: u32,
    pub compressed_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendBlock {
    pub object_key: String,
    pub min_time_ns: i64,
    pub max_time_ns: i64,
    pub row_groups: Vec<BackendRowGroup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendJob {
    pub object_key: String,
    pub row_group_start: u32,
    pub row_group_end: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryShard {
    pub tier: QueryTier,
    pub start_ns: i64,
    pub end_ns: i64,
    pub backend_job: Option<BackendJob>,
}

#[derive(Clone, Debug)]
pub struct QueryFrontendConfig {
    pub querier_url: Url,
    pub live_frontier_ns: Option<i64>,
    pub max_queue_depth: usize,
    pub target_bytes_per_job: u64,
    pub backend_blocks: Vec<BackendBlock>,
}

impl QueryFrontendConfig {
    pub fn new(querier_url: &str) -> Result<Self, url::ParseError> {
        Ok(Self {
            querier_url: Url::parse(querier_url)?,
            live_frontier_ns: None,
            max_queue_depth: 128,
            target_bytes_per_job: 0,
            backend_blocks: Vec::new(),
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
        .route("/api/v2/search/tags", get(search_tags_v2))
        .route("/api/search/tag/{tag}/values", get(proxy))
        .route("/api/v2/search/tag/{tag}/values", get(search_tag_values_v2))
        .route("/api/metrics/query_range", get(query_range))
        .route("/api/metrics/query", get(query_instant))
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
            backend_job: None,
        }];
    };

    if end_ns < frontier {
        return vec![QueryShard {
            tier: QueryTier::Backend,
            start_ns,
            end_ns,
            backend_job: None,
        }];
    }
    if start_ns >= frontier {
        return vec![QueryShard {
            tier: QueryTier::Live,
            start_ns,
            end_ns,
            backend_job: None,
        }];
    }

    vec![
        QueryShard {
            tier: QueryTier::Backend,
            start_ns,
            end_ns: frontier.saturating_sub(1),
            backend_job: None,
        },
        QueryShard {
            tier: QueryTier::Live,
            start_ns: frontier,
            end_ns,
            backend_job: None,
        },
    ]
}

#[must_use]
pub fn plan_query_shards(
    start_ns: i64,
    end_ns: i64,
    live_frontier_ns: Option<i64>,
    target_bytes_per_job: u64,
    backend_blocks: &[BackendBlock],
) -> Vec<QueryShard> {
    let time_shards = plan_time_shards(start_ns, end_ns, live_frontier_ns);
    if target_bytes_per_job == 0 || backend_blocks.is_empty() {
        return time_shards;
    }

    let mut out = Vec::new();
    for shard in time_shards {
        if shard.tier != QueryTier::Backend {
            out.push(shard);
            continue;
        }

        let before = out.len();
        for block in backend_blocks {
            if block.max_time_ns < shard.start_ns || block.min_time_ns > shard.end_ns {
                continue;
            }
            out.extend(plan_block_jobs(&shard, block, target_bytes_per_job));
        }
        if out.len() == before {
            out.push(shard);
        }
    }
    out
}

fn plan_block_jobs(
    shard: &QueryShard,
    block: &BackendBlock,
    target_bytes_per_job: u64,
) -> Vec<QueryShard> {
    let mut jobs = Vec::new();
    let mut row_group_start = None;
    let mut row_group_end = 0;
    let mut bytes = 0_u64;

    for row_group in &block.row_groups {
        row_group_start.get_or_insert(row_group.index);
        row_group_end = row_group.index.saturating_add(1);
        bytes = bytes.saturating_add(row_group.compressed_bytes);
        if bytes >= target_bytes_per_job {
            jobs.push(backend_job_shard(
                shard,
                block,
                row_group_start.unwrap(),
                row_group_end,
            ));
            row_group_start = None;
            bytes = 0;
        }
    }

    if let Some(start) = row_group_start {
        jobs.push(backend_job_shard(shard, block, start, row_group_end));
    }

    jobs
}

fn backend_job_shard(
    shard: &QueryShard,
    block: &BackendBlock,
    row_group_start: u32,
    row_group_end: u32,
) -> QueryShard {
    QueryShard {
        tier: QueryTier::Backend,
        start_ns: shard.start_ns,
        end_ns: shard.end_ns,
        backend_job: Some(BackendJob {
            object_key: block.object_key.clone(),
            row_group_start,
            row_group_end,
        }),
    }
}

async fn echo() -> &'static str {
    "echo"
}

async fn ready() -> &'static str {
    "ready"
}

fn planned_shards(state: &AppState, start_ns: i64, end_ns: i64) -> Vec<QueryShard> {
    plan_query_shards(
        start_ns,
        end_ns,
        state.cfg.live_frontier_ns,
        state.cfg.target_bytes_per_job,
        &state.cfg.backend_blocks,
    )
}

async fn search(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut merged_traces = Vec::new();
    let mut metrics = json!({
        "totalBlocks": 0,
        "inspectedTraces": 0,
        "inspectedBytes": 0,
    });

    for shard in planned_shards(&state, start_ns, end_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, &shard)
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

    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let exemplar_limit = exemplar_limit_param(&uri);
    let mut merged_series = Vec::new();

    for shard in planned_shards(&state, start_ns, end_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, &shard)
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

    limit_metric_exemplars(&mut merged_series, exemplar_limit);
    axum::Json(json!({ "series": merged_series })).into_response()
}

async fn query_instant(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    if query_param(&uri, "start").is_none() && query_param(&uri, "end").is_none() {
        return proxy_querier_response(&state, &headers, &uri).await;
    }

    let (start_ns, end_ns) = match required_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };

    merged_metric_query_response(state, headers, uri, start_ns, end_ns).await
}

async fn merged_metric_query_response(
    state: AppState,
    headers: HeaderMap,
    uri: Uri,
    start_ns: i64,
    end_ns: i64,
) -> Response {
    let exemplar_limit = exemplar_limit_param(&uri);
    let mut merged_series = Vec::new();

    for shard in planned_shards(&state, start_ns, end_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, &shard)
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

    limit_metric_exemplars(&mut merged_series, exemplar_limit);
    axum::Json(json!({ "series": merged_series })).into_response()
}

async fn search_tags_v2(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut merged_scopes = Vec::new();
    let mut metrics = json!({
        "totalBlocks": 0,
        "inspectedTraces": 0,
        "inspectedBytes": 0,
    });

    for shard in planned_shards(&state, start_ns, end_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, &shard)
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
        if let Some(scopes) = body.get("scopes").and_then(Value::as_array) {
            merge_scopes(&mut merged_scopes, scopes);
        }
        merge_metrics(&mut metrics, body.get("metrics"));
    }

    axum::Json(json!({
        "scopes": merged_scopes,
        "metrics": metrics,
    }))
    .into_response()
}

async fn search_tag_values_v2(
    State(state): State<AppState>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    let mut merged_values = Vec::new();
    let mut metrics = json!({
        "totalBlocks": 0,
        "inspectedTraces": 0,
        "inspectedBytes": 0,
    });

    for shard in planned_shards(&state, start_ns, end_ns) {
        let Ok(resp) = build_querier_request(&state, &headers, &uri, &shard)
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
        if let Some(values) = body.get("tagValues").and_then(Value::as_array) {
            merge_tag_values(&mut merged_values, values);
        }
        merge_metrics(&mut metrics, body.get("metrics"));
    }

    axum::Json(json!({
        "tagValues": merged_values,
        "metrics": metrics,
    }))
    .into_response()
}

async fn proxy(State(state): State<AppState>, headers: HeaderMap, uri: Uri) -> Response {
    let Ok(_permit) = state.permits.clone().try_acquire_owned() else {
        return (StatusCode::TOO_MANY_REQUESTS, "query frontend queue full").into_response();
    };

    proxy_querier_response(&state, &headers, &uri).await
}

async fn proxy_querier_response(state: &AppState, headers: &HeaderMap, uri: &Uri) -> Response {
    let Ok(resp) = build_plain_querier_request(state, headers, uri)
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

fn merge_scopes(merged_scopes: &mut Vec<Value>, incoming_scopes: &[Value]) {
    for incoming_scope in incoming_scopes {
        let Some(scope_name) = incoming_scope.get("name").and_then(Value::as_str) else {
            merged_scopes.push(incoming_scope.clone());
            continue;
        };
        let Some(incoming_tags) = incoming_scope.get("tags").and_then(Value::as_array) else {
            merged_scopes.push(incoming_scope.clone());
            continue;
        };
        let Some(existing_scope) = merged_scopes
            .iter_mut()
            .find(|scope| scope.get("name").and_then(Value::as_str) == Some(scope_name))
        else {
            merged_scopes.push(incoming_scope.clone());
            continue;
        };
        let Some(existing_tags) = existing_scope.get_mut("tags").and_then(Value::as_array_mut)
        else {
            continue;
        };
        for tag in incoming_tags {
            if !existing_tags.iter().any(|existing| existing == tag) {
                existing_tags.push(tag.clone());
            }
        }
        existing_tags.sort_by(tag_value_cmp);
    }
}

fn merge_tag_values(merged_values: &mut Vec<Value>, incoming_values: &[Value]) {
    for value in incoming_values {
        if !merged_values.iter().any(|existing| existing == value) {
            merged_values.push(value.clone());
        }
    }
    merged_values.sort_by(tag_value_cmp);
}

fn tag_value_cmp(lhs: &Value, rhs: &Value) -> std::cmp::Ordering {
    tag_value_sort_key(lhs).cmp(&tag_value_sort_key(rhs))
}

fn tag_value_sort_key(value: &Value) -> (String, String) {
    if let Some(value) = value.as_str() {
        return (String::new(), value.to_string());
    }
    let type_ = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let value = value
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    (type_, value)
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
    shard: &QueryShard,
) -> reqwest::RequestBuilder {
    let mut url = state.cfg.querier_url.clone();
    url.set_path(uri.path());
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        {
            if key != "start"
                && key != "end"
                && key != "block"
                && key != "rowGroupStart"
                && key != "rowGroupEnd"
            {
                pairs.append_pair(&key, &value);
            }
        }
        pairs.append_pair("start", &format_ns_as_seconds(shard.start_ns));
        pairs.append_pair("end", &format_ns_as_seconds(shard.end_ns));
        if let Some(job) = &shard.backend_job {
            pairs.append_pair("block", &job.object_key);
            pairs.append_pair("rowGroupStart", &job.row_group_start.to_string());
            pairs.append_pair("rowGroupEnd", &job.row_group_end.to_string());
        }
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

fn exemplar_limit_param(uri: &Uri) -> Option<usize> {
    query_param(uri, "exemplars").and_then(|value| {
        if value.eq_ignore_ascii_case("false") {
            Some(0)
        } else if value.eq_ignore_ascii_case("true") {
            None
        } else {
            value.parse().ok()
        }
    })
}

fn limit_metric_exemplars(series: &mut [Value], limit: Option<usize>) {
    let Some(limit) = limit else {
        return;
    };
    for series in series {
        if let Some(exemplars) = series.get_mut("exemplars").and_then(Value::as_array_mut) {
            exemplars.truncate(limit);
        }
    }
}

fn required_seconds_param(uri: &Uri, key: &'static str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

fn required_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = required_seconds_param(uri, "start")?;
    let end_ns = required_seconds_param(uri, "end")?;
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn optional_time_bounds(uri: &Uri) -> Result<(i64, i64), String> {
    let start_ns = optional_seconds_param(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds_param(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok((start_ns, end_ns))
}

fn optional_seconds_param(uri: &Uri, key: &'static str) -> Result<Option<i64>, String> {
    let Some(value) = query_param(uri, key) else {
        return Ok(None);
    };
    parse_seconds_to_ns(&value)
        .map(Some)
        .ok_or_else(|| format!("invalid query parameter {key}"))
}

fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    if let Ok(seconds) = value.parse::<i64>() {
        return seconds.checked_mul(1_000_000_000);
    }

    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (whole, frac) = value.split_once('.')?;
    if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let whole = whole.parse::<i64>().ok()?;
    let mut nanos = frac.parse::<i64>().ok()?;
    for _ in frac.len()..9 {
        nanos = nanos.checked_mul(10)?;
    }
    let ns = whole.checked_mul(1_000_000_000)?.checked_add(nanos)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}

fn format_ns_as_seconds(ns: i64) -> String {
    let negative = ns < 0;
    let ns = ns.unsigned_abs();
    let seconds = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    if nanos == 0 {
        return if negative {
            format!("-{seconds}")
        } else {
            seconds.to_string()
        };
    }
    let mut frac = format!("{nanos:09}");
    while frac.ends_with('0') {
        frac.pop();
    }
    if negative {
        format!("-{seconds}.{frac}")
    } else {
        format!("{seconds}.{frac}")
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt as _;
    use serde_json::json;
    use tower::ServiceExt as _;

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
    fn merge_scopes_keeps_tags_sorted_after_deduping_shards() {
        let mut scopes = vec![json!({
            "name": "span",
            "tags": ["zeta", "svc"],
        })];
        let incoming = vec![json!({
            "name": "span",
            "tags": ["alpha", "svc"],
        })];

        merge_scopes(&mut scopes, &incoming);

        assert!(scopes[0]["tags"] == json!(["alpha", "svc", "zeta"]));
    }

    #[test]
    fn merge_tag_values_keeps_values_sorted_after_deduping_shards() {
        let mut values = vec![
            json!({"type": "string", "value": "zeta"}),
            json!({"type": "int", "value": "9"}),
        ];
        let incoming = vec![
            json!({"type": "string", "value": "alpha"}),
            json!({"type": "int", "value": "9"}),
        ];

        merge_tag_values(&mut values, &incoming);

        assert!(
            values
                == vec![
                    json!({"type": "int", "value": "9"}),
                    json!({"type": "string", "value": "alpha"}),
                    json!({"type": "string", "value": "zeta"}),
                ]
        );
    }

    #[test]
    fn parses_fractional_epoch_seconds_to_nanoseconds() {
        assert!(parse_seconds_to_ns("1.4") == Some(1_400_000_000));
        assert!(parse_seconds_to_ns("1.000000001") == Some(1_000_000_001));
    }

    #[test]
    fn parses_negative_fractional_epoch_seconds_to_nanoseconds() {
        assert!(parse_seconds_to_ns("-1.4") == Some(-1_400_000_000));
        assert!(parse_seconds_to_ns("-0.5") == Some(-500_000_000));
    }

    #[test]
    fn formats_negative_epoch_nanoseconds_as_seconds() {
        assert!(format_ns_as_seconds(-1_400_000_000) == "-1.4");
        assert!(format_ns_as_seconds(-500_000_000) == "-0.5");
    }

    #[tokio::test]
    async fn search_rejects_end_before_start_without_querying_backend() {
        let app = router(QueryFrontendConfig::new("http://127.0.0.1:9").unwrap());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/search?q=%7B%20.svc%20%21%3D%20nil%20%7D&start=2&end=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = resp.into_body().collect().await.unwrap().to_bytes();

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body.as_ref() == b"end must be >= start");
    }
}
