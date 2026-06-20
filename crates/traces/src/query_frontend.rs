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
        .route("/api/metrics/query_range", get(proxy))
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

    let start_ns = query_param(&uri, "start")
        .and_then(|v| parse_seconds_to_ns(&v))
        .unwrap_or(0);
    let end_ns = query_param(&uri, "end")
        .and_then(|v| parse_seconds_to_ns(&v))
        .unwrap_or(i64::MAX);
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
        let Ok(body) = resp.json::<Value>().await else {
            return (StatusCode::BAD_GATEWAY, "querier response decode failed").into_response();
        };
        if !status.is_success() {
            let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            return (status, body.to_string()).into_response();
        }
        if let Some(traces) = body.get("traces").and_then(Value::as_array) {
            merged_traces.extend(traces.iter().cloned());
        }
        merge_metrics(&mut metrics, body.get("metrics"));
    }

    axum::Json(json!({
        "traces": merged_traces,
        "metrics": metrics,
    }))
    .into_response()
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
        pairs.append_pair("start", &(shard.start_ns / 1_000_000_000).to_string());
        pairs.append_pair("end", &(shard.end_ns / 1_000_000_000).to_string());
    }

    let mut req = state.http.get(url);
    if let Some(tenant) = headers.get("x-scope-orgid") {
        req = req.header("x-scope-orgid", tenant.clone());
    }
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
    req
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

fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|(k, v)| (k == key).then(|| v.into_owned()))
}

fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    value.parse::<i64>().ok()?.checked_mul(1_000_000_000)
}
