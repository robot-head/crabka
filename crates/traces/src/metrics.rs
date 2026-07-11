//! Traces-service Prometheus metrics.
//!
//! Shared metric spec uniform across the LGTM observability services: a
//! `prometheus-client` [`Registry`] (prefix `crabka_traces`) wrapped in
//! `Arc<Mutex<…>>` so the `/metrics` exporter can lock it while the cheaply
//! cloneable [`ServiceMetrics`] hands out counter / histogram handles that the
//! ingest (distributor) and query (querier) handlers increment directly.
//!
//! `prometheus-client` auto-appends `_total` to counters at encode time, so
//! counter names are registered WITHOUT the suffix.

use std::sync::Arc;

use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Shared registry owning every metric this service emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry` to
/// register and the exporter takes a read lock at scrape time.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Ingest request outcome label (`status="ok"|"error"`).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}

/// Query route + outcome label (`route="search", status="ok"|"error"`).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}

/// Per-tenant ingest label (`tenant="anonymous"`), paired with the spans-accepted
/// counter family so accepted-span volume can be attributed per tenant.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}

/// Query route label (`route="search"`), paired with the per-route latency
/// histogram family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}

/// Cheaply-clonable bundle of metric handles plus the shared registry.
/// Construct once in the binary's `run()` before role dispatch; clone into the
/// distributor and querier state structs (each clone is a handful of
/// `Arc::clone`s).
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor role).
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    /// Spans accepted on the ingest path, attributed per tenant.
    pub ingest_spans: Family<TenantLabel, Counter>,
    // BLOCK-BUILDER (WAL-consumer role).
    /// WAL span blocks durably written by the block-builder.
    pub blocks_flushed: Counter,
    // QUERY (querier role).
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_traces");

        let ingest_requests = Family::<StatusLabel, Counter>::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_spans = Family::<TenantLabel, Counter>::default();
        let blocks_flushed = Counter::default();
        let query_requests = Family::<RouteStatusLabel, Counter>::default();
        let query_duration = Family::<RouteLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Trace-ingest (push) requests by outcome (status=ok|error)",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative request-body bytes accepted on the trace-ingest path",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative spans accepted on the trace-ingest path",
            ingest_items.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Trace-ingest push-handler latency in seconds",
            ingest_duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            "Cumulative trace-WAL (produce) append failures",
            wal_append_failures.clone(),
        );
        registry.register(
            "ingest_spans",
            "Cumulative spans accepted on the trace-ingest path, by tenant",
            ingest_spans.clone(),
        );
        registry.register(
            "blocks_flushed",
            "Cumulative trace-WAL span blocks durably written by the block-builder",
            blocks_flushed.clone(),
        );
        registry.register(
            "query_requests",
            "Querier requests by route and outcome (route, status=ok|error)",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Querier handler latency in seconds, by route",
            query_duration.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_spans,
            blocks_flushed,
            query_requests,
            query_duration,
        }
    }

    /// Record one trace-ingest request outcome: bumps the per-status request
    /// counter, accumulates bytes / spans, and observes the handler latency.
    /// `ok=false` covers any 4xx/5xx (validation, rate-limit, decode, or
    /// produce failure); the WAL/produce-specific failure counter is bumped
    /// separately via [`Self::record_wal_append_failure`] only at the actual
    /// produce error site so a 4xx client error does not inflate it.
    pub fn record_ingest(&self, ok: bool, bytes: u64, items: u64, secs: f64) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        self.ingest_bytes.inc_by(bytes);
        self.ingest_items.inc_by(items);
        self.ingest_duration.observe(secs);
    }

    /// Bump the WAL/produce append-failure counter. Called only when the
    /// failure was an actual WAL (Kafka produce) error, not a client/validation
    /// 4xx.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
    }

    /// Attribute `count` accepted spans to `tenant` on the ingest path. Called
    /// once per successful push request (not per span-record) with the batch
    /// size, so per-tenant span volume is visible without a high-cardinality
    /// per-record hop.
    pub fn record_ingest_spans(&self, tenant: &str, count: u64) {
        if count == 0 {
            return;
        }
        self.ingest_spans
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(count);
    }

    /// Bump the block-builder flushed-block counter once per span block durably
    /// written to object storage.
    pub fn record_block_flushed(&self) {
        self.blocks_flushed.inc();
    }

    /// Record one querier request: bumps the per-(route, status) request
    /// counter and observes the per-route handler latency.
    pub fn record_query(&self, route: &str, ok: bool, secs: f64) {
        let status = if ok { "ok" } else { "error" };
        self.query_requests
            .get_or_create(&RouteStatusLabel {
                route: route.into(),
                status: status.into(),
            })
            .inc();
        self.query_duration
            .get_or_create(&RouteLabel {
                route: route.into(),
            })
            .observe(secs);
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// `/metrics` router serving the `OpenMetrics` text encoding of `registry`.
/// Merged onto the admin port by `serve_admin_from_env_with`; does NOT include
/// the pprof routes (those are added by `serve_admin`).
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}

async fn export(
    axum::extract::State(reg): axum::extract::State<SharedRegistry>,
) -> axum::response::Response {
    use axum::response::IntoResponse as _;

    let mut buf = String::new();
    let r = reg.lock().await;
    if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &r) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("encode: {e}"),
        )
            .into_response();
    }
    (
        axum::http::StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_traces_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, 1_024, 7, 0.01);
        m.record_ingest(false, 0, 0, 0.002);
        m.record_wal_append_failure();
        m.record_ingest_spans("tenant-a", 7);
        m.record_block_flushed();
        m.record_query("search", true, 0.05);
        m.record_query("trace_by_id", false, 0.2);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();

        for needle in [
            "crabka_traces_ingest_requests_total",
            "crabka_traces_ingest_bytes_total",
            "crabka_traces_ingest_items_total",
            "crabka_traces_ingest_duration_seconds",
            "crabka_traces_wal_append_failures_total",
            "crabka_traces_ingest_spans_total",
            "crabka_traces_blocks_flushed_total",
            "crabka_traces_query_requests_total",
            "crabka_traces_query_duration_seconds",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"search\"",
            "tenant=\"tenant-a\"",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[test]
    fn ingest_counters_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, 100, 3, 0.01);
        m.record_ingest(true, 50, 2, 0.01);
        assert2::assert!(m.ingest_bytes.get() == 150);
        assert2::assert!(m.ingest_items.get() == 5);
        assert2::assert!(
            m.ingest_requests
                .get_or_create(&StatusLabel {
                    status: "ok".into()
                })
                .get()
                == 2
        );
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // A 4xx client error: error outcome, but NOT a WAL failure.
        m.record_ingest(false, 0, 0, 0.001);
        assert2::assert!(m.wal_append_failures.get() == 0);
        // A produce failure: bump explicitly at the WAL error site.
        m.record_wal_append_failure();
        assert2::assert!(m.wal_append_failures.get() == 1);
    }

    #[test]
    fn ingest_spans_split_by_tenant_and_blocks_flushed_accumulate() {
        let m = ServiceMetrics::new();
        m.record_ingest_spans("tenant-a", 3);
        m.record_ingest_spans("tenant-a", 2);
        m.record_ingest_spans("tenant-b", 4);
        // A zero-span request must not create a tenant series.
        m.record_ingest_spans("tenant-c", 0);
        m.record_block_flushed();
        m.record_block_flushed();

        assert2::assert!(
            m.ingest_spans
                .get_or_create(&TenantLabel {
                    tenant: "tenant-a".into()
                })
                .get()
                == 5
        );
        assert2::assert!(
            m.ingest_spans
                .get_or_create(&TenantLabel {
                    tenant: "tenant-b".into()
                })
                .get()
                == 4
        );
        assert2::assert!(m.blocks_flushed.get() == 2);
    }

    #[test]
    fn query_counters_split_by_route_and_status() {
        let m = ServiceMetrics::new();
        m.record_query("search", true, 0.01);
        m.record_query("search", true, 0.02);
        m.record_query("search", false, 0.03);
        m.record_query("tags", true, 0.01);
        for (route, status, want) in [
            ("search", "ok", 2),
            ("search", "error", 1),
            ("tags", "ok", 1),
        ] {
            assert2::assert!(
                m.query_requests
                    .get_or_create(&RouteStatusLabel {
                        route: route.into(),
                        status: status.into()
                    })
                    .get()
                    == want
            );
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, 42, 1, 0.01);
        let app = metrics_router(m.registry);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(resp.status() == StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert2::assert!(ct.starts_with("application/openmetrics-text"));
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert2::assert!(s.contains("crabka_traces_ingest_requests_total"));
        assert2::assert!(s.contains("# EOF"));
    }
}
