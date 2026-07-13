//! The real querier fan-out backend: a reqwest client over a configurable set
//! of querier addresses, speaking the Tempo HTTP API at the per-job grain (one
//! HTTP call per planned shard).
//!
//! The shard restriction uses the querier's real `scan_options` contract
//! (`querier/http::scan_options_param`): a cold block + row-group range is
//! `block=<object_key>&rowGroupStart=<n>&rowGroupEnd=<m>`; the live shard sends
//! no scan params (the querier's hot/cold union scan). There is no `shard=live`
//! param. By-id has no block scoping — it targets one querier by index and
//! unions across the pool. `start`/`end` are epoch **seconds** on every endpoint.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::frontend::{
    backend::{
        BackendError, MetricsJobRequest, MetricsPartial, QuerierBackend, SearchJobRequest,
        SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
        TraceByIdJobRequest, TracePartial,
    },
    config::FrontendConfig,
    job::{JobShard, TraceIndexCatalog},
    metrics_merge::MetricsResponseJson,
    wire::{SearchResponseJson, TraceByIdResponseJson},
};

const TENANT_HEADER: &str = "X-Scope-OrgID";

/// HTTP querier pool. Round-robins `addrs` for search/tag jobs; targets a
/// specific querier by index for by-id fan-out. Each request carries the tenant
/// in `X-Scope-OrgID` and a per-request timeout.
pub struct HttpQuerier {
    http: reqwest::Client,
    addrs: Vec<String>,
    next: AtomicUsize,
}

impl HttpQuerier {
    /// Build the pool. `addrs` are `host:port` (no scheme; `http://` is assumed).
    ///
    /// # Errors
    /// Returns `BackendError::Transport` if `addrs` is empty or the client
    /// cannot be built.
    pub fn new(addrs: Vec<String>, timeout: Duration) -> Result<Self, BackendError> {
        if addrs.is_empty() {
            return Err(BackendError::Transport("no querier addresses".to_string()));
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| BackendError::Transport(e.to_string()))?;
        Ok(Self {
            http,
            addrs,
            next: AtomicUsize::new(0),
        })
    }

    fn next_addr(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        &self.addrs[i]
    }

    fn addr_at(&self, idx: usize) -> &str {
        &self.addrs[idx % self.addrs.len()]
    }

    fn map_send_err(e: &reqwest::Error) -> BackendError {
        if e.is_timeout() {
            BackendError::Timeout
        } else {
            BackendError::Transport(e.to_string())
        }
    }
}

/// Epoch nanos -> epoch seconds string (the querier parses `start`/`end` as
/// seconds, fractional allowed).
fn ns_to_seconds(ns: i64) -> String {
    let negative = ns < 0;
    let ns = ns.unsigned_abs();
    let seconds = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    let s = if nanos == 0 {
        seconds.to_string()
    } else {
        let mut frac = format!("{nanos:09}");
        while frac.ends_with('0') {
            frac.pop();
        }
        format!("{seconds}.{frac}")
    };
    if negative { format!("-{s}") } else { s }
}

/// Push the querier's scan-job params for a cold-block shard. The live shard
/// sends none.
fn push_shard_params(params: &mut Vec<(&'static str, String)>, shard: &JobShard) {
    if let JobShard::Block {
        block_id,
        row_group_start,
        row_group_end,
    } = shard
    {
        params.push(("block", block_id.clone()));
        params.push(("rowGroupStart", row_group_start.to_string()));
        params.push(("rowGroupEnd", row_group_end.to_string()));
    }
}

/// Build a `host/path?query` URL with the given params. The crate's `reqwest` is
/// built without the `query` feature, so query strings are encoded via `url`
/// (the same approach the legacy query-frontend uses).
fn build_url(base: &str, params: &[(&str, String)]) -> Result<reqwest::Url, BackendError> {
    let mut url = reqwest::Url::parse(base)
        .map_err(|e| BackendError::Transport(format!("invalid url {base}: {e}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in params {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}

#[async_trait]
impl QuerierBackend for HttpQuerier {
    fn querier_count(&self) -> usize {
        self.addrs.len()
    }

    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        let url = format!("http://{}/api/search", self.next_addr());
        let mut params: Vec<(&str, String)> = vec![
            ("q", req.query.clone()),
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
            ("limit", req.limit.to_string()),
            ("spss", req.spss.to_string()),
        ];
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, &req.tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: SearchResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode search body: {e}")))?;
        Ok(SearchPartial {
            traces: body.traces,
            metrics: body.metrics,
        })
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        let hex = crate::frontend::wire::hex16(&req.trace_id);
        let addr = req
            .querier
            .map_or_else(|| self.next_addr(), |i| self.addr_at(i));
        let url = format!("http://{addr}/api/v2/traces/{hex}");
        let params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, &req.tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        // The querier returns 404 when it does not hold the trace; treat that as
        // an empty partial rather than an error (another querier may have it).
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(TracePartial::default());
        }
        let resp = error_for_status(resp).await?;
        let body: TraceByIdResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode trace body: {e}")))?;
        Ok(TracePartial {
            trace: body,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        let url = format!("http://{}/api/v2/search/tags", self.next_addr());
        let mut params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        if let Some(scope) = req.scope {
            params.push(("scope", scope_param(scope).to_string()));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, &req.tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: TagsBody = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tags body: {e}")))?;
        Ok(TagNamesPartial {
            tags: body.scoped_tags(),
            metrics: body.metrics,
        })
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        // The tag is a client-supplied path segment (e.g. `span:name`,
        // `resource.service.name`); build it via `path_segments_mut` so any
        // special chars (`/`, `?`, `#`, space) are percent-encoded into a single
        // segment rather than corrupting the path/query when re-parsed.
        let mut url = reqwest::Url::parse(&format!("http://{}", self.next_addr()))
            .map_err(|e| BackendError::Transport(format!("invalid querier addr: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| BackendError::Transport("querier url cannot be a base".to_string()))?
            .extend(["api", "v2", "search", "tag", req.tag.as_str(), "values"]);
        let mut params: Vec<(&str, String)> = vec![
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        push_shard_params(&mut params, &req.shard);
        {
            let mut pairs = url.query_pairs_mut();
            for (key, value) in &params {
                pairs.append_pair(key, value);
            }
        }
        let resp = self
            .http
            .get(url)
            .header(TENANT_HEADER, &req.tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let body: TagValuesBody = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode tag-values body: {e}")))?;
        let metrics = body.metrics;
        Ok(TagValuesPartial {
            values: body.into_typed_values(),
            metrics,
        })
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        let path = if req.instant { "query" } else { "query_range" };
        let url = format!("http://{}/api/metrics/{path}", self.next_addr());
        let mut params: Vec<(&str, String)> = vec![
            ("q", req.query.clone()),
            ("start", ns_to_seconds(req.start_ns)),
            ("end", ns_to_seconds(req.end_ns)),
        ];
        if !req.instant {
            params.push(("step", ns_to_seconds(req.step_ns)));
        }
        push_shard_params(&mut params, &req.shard);
        let resp = self
            .http
            .get(build_url(&url, &params)?)
            .header(TENANT_HEADER, &req.tenant)
            .send()
            .await
            .map_err(|e| Self::map_send_err(&e))?;
        let resp = error_for_status(resp).await?;
        let response: MetricsResponseJson = resp
            .json()
            .await
            .map_err(|e| BackendError::Transport(format!("decode metrics body: {e}")))?;
        Ok(MetricsPartial {
            response,
            metrics: crate::frontend::wire::Metrics::default(),
        })
    }
}

async fn error_for_status(resp: reqwest::Response) -> Result<reqwest::Response, BackendError> {
    let status = resp.status();
    if status.is_success() {
        return Ok(resp);
    }
    let message = resp.text().await.unwrap_or_default();
    Err(BackendError::Backend {
        status: status.as_u16().to_string(),
        message,
    })
}

// --- Tag body parsing (the querier's v2 tag shapes) -------------------------

/// The `/api/v2/search/tags` body: `{ scopes: [{ name, tags }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
struct TagsBody {
    #[serde(default)]
    scopes: Vec<ScopeTagsJson>,
    #[serde(default)]
    metrics: crate::frontend::wire::Metrics,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct ScopeTagsJson {
    #[serde(default)]
    name: String,
    #[serde(default)]
    tags: Vec<String>,
}

impl TagsBody {
    fn scoped_tags(&self) -> Vec<crabka_traceql::ScopedTag> {
        self.scopes
            .iter()
            .map(|s| crabka_traceql::ScopedTag {
                scope: parse_scope(&s.name),
                tags: s.tags.clone(),
            })
            .collect()
    }
}

/// The `/api/v2/search/tag/{tag}/values` body: `{ tagValues: [{ type, value }], metrics }`.
#[derive(Clone, Debug, serde::Deserialize)]
struct TagValuesBody {
    #[serde(rename = "tagValues", default)]
    tag_values: Vec<TypedValueJson>,
    #[serde(default)]
    metrics: crate::frontend::wire::Metrics,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct TypedValueJson {
    #[serde(rename = "type", default)]
    type_: String,
    #[serde(default)]
    value: String,
}

impl TagValuesBody {
    fn into_typed_values(self) -> Vec<crabka_traceql::TypedValue> {
        self.tag_values
            .into_iter()
            .map(|v| crabka_traceql::TypedValue {
                type_: v.type_,
                value: v.value,
            })
            .collect()
    }
}

fn scope_param(scope: crabka_traceql::TagScope) -> &'static str {
    match scope {
        crabka_traceql::TagScope::Resource => "resource",
        crabka_traceql::TagScope::Span => "span",
        crabka_traceql::TagScope::Intrinsic => "intrinsic",
        crabka_traceql::TagScope::Event => "event",
        crabka_traceql::TagScope::Link => "link",
        crabka_traceql::TagScope::Instrumentation => "instrumentation",
    }
}

fn parse_scope(name: &str) -> crabka_traceql::TagScope {
    match name {
        "resource" => crabka_traceql::TagScope::Resource,
        "intrinsic" => crabka_traceql::TagScope::Intrinsic,
        "event" => crabka_traceql::TagScope::Event,
        "link" => crabka_traceql::TagScope::Link,
        "instrumentation" => crabka_traceql::TagScope::Instrumentation,
        _ => crabka_traceql::TagScope::Span,
    }
}

/// Boot the query-frontend role: build the HTTP querier pool + a block catalog,
/// then serve the router on `cfg.listen_addr` until `shutdown` fires.
///
/// `catalog` is the production [`TraceIndexCatalog`] (or any compatible block catalog).
///
/// # Errors
/// Propagates bind/serve `std::io` errors and backend-construction failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    catalog: TraceIndexCatalog,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let backend = HttpQuerier::new(cfg.querier_addrs.clone(), cfg.request_timeout)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let listen_addr = cfg.listen_addr;
    let qf = Arc::new(crate::frontend::QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let app = crate::frontend::server::router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn ns_to_seconds_round_trips_whole_and_fractional() {
        for (ns, want) in [
            (0, "0"),
            (1_000_000_000, "1"),
            (1_400_000_000, "1.4"),
            (-500_000_000, "-0.5"),
        ] {
            check!(ns_to_seconds(ns) == want);
        }
    }

    #[test]
    fn tag_values_body_projects_typed_values() {
        let body = TagValuesBody {
            tag_values: vec![TypedValueJson {
                type_: "string".into(),
                value: "GET".into(),
            }],
            metrics: crate::frontend::wire::Metrics::default(),
        };
        let values = body.into_typed_values();
        assert2::assert!(values.len() == 1);
        assert2::assert!(values[0].value.as_str() == "GET");
    }
}
