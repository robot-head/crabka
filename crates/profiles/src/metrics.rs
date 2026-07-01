//! Prometheus metrics for the profiles subsystem.
//!
//! Uses the `OpenMetrics` `prometheus-client` crate. A cheaply-clonable
//! [`ServiceMetrics`] bundle is constructed once in the binary `main`, threaded
//! into the distributor and querier state structs, and incremented from the
//! ingest / query handler boundaries via the ergonomic [`ServiceMetrics::record_ingest`]
//! / [`ServiceMetrics::record_query`] helpers. The exporter emits the
//! `OpenMetrics` text format.
//!
//! Counters are registered WITHOUT a `_total` suffix — `prometheus-client`
//! appends it automatically at encode time. The registry prefix is
//! `crabka_profiles`, so e.g. the `ingest_requests` counter renders on the wire
//! as `crabka_profiles_ingest_requests_total{status="ok"}`.

use std::sync::Arc;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::{counter::Counter, family::Family, histogram::Histogram};
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

/// Shared registry owning every metric the service emits. `Arc<Mutex<…>>`
/// because `prometheus-client` requires `&mut Registry` to register and the
/// `/metrics` exporter needs shared read access.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// `status="ok" | "error"` label for the ingest-request counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}

/// `route` + `status` label set for the query-request counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}

/// `route` label set for the per-route query-duration histogram family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}

/// `tenant` label set for the per-tenant ingested-samples counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}

/// Cheaply-clonable bundle of metric handles plus the shared registry.
/// Construct once via [`ServiceMetrics::new`]; clone freely (each clone is a
/// handful of `Arc::clone`s).
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    /// Ingest requests, labelled by outcome. Renders as
    /// `crabka_profiles_ingest_requests_total{status}`.
    pub ingest_requests: Family<StatusLabel, Counter>,
    /// Cumulative ingest body bytes accepted. Renders as
    /// `crabka_profiles_ingest_bytes_total`.
    pub ingest_bytes: Counter,
    /// Cumulative profile/sample items ingested. Renders as
    /// `crabka_profiles_ingest_items_total`.
    pub ingest_items: Counter,
    /// Ingest handler latency in seconds.
    pub ingest_duration: Histogram,
    /// Cumulative WAL/produce append failures. Renders as
    /// `crabka_profiles_wal_append_failures_total`.
    pub wal_append_failures: Counter,
    /// Cumulative profile samples accepted, labelled by tenant. Renders as
    /// `crabka_profiles_ingest_samples_total{tenant}`. Bumped once per ingest
    /// request with the number of WAL samples that request produced.
    pub ingest_samples: Family<TenantLabel, Counter>,
    /// Cumulative profile sample blocks flushed to object storage by the
    /// block-builder. Renders as `crabka_profiles_blocks_built_total`.
    pub blocks_built: Counter,
    /// Query requests, labelled by route + outcome. Renders as
    /// `crabka_profiles_query_requests_total{route,status}`.
    pub query_requests: Family<RouteStatusLabel, Counter>,
    /// Per-route query handler latency in seconds.
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_profiles");

        let ingest_requests = Family::<StatusLabel, Counter>::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_samples = Family::<TenantLabel, Counter>::default();
        let blocks_built = Counter::default();
        let query_requests = Family::<RouteStatusLabel, Counter>::default();
        let query_duration = Family::<RouteLabel, Histogram>::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Ingest requests handled, labelled by outcome (ok/error).",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative ingest request body bytes accepted.",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative profiles/samples ingested.",
            ingest_items.clone(),
        );
        registry.register(
            "ingest_duration_seconds",
            "Ingest handler latency in seconds.",
            ingest_duration.clone(),
        );
        registry.register(
            "wal_append_failures",
            "Cumulative WAL/produce append failures on the ingest path.",
            wal_append_failures.clone(),
        );
        registry.register(
            "ingest_samples",
            "Cumulative profile samples accepted, labelled by tenant.",
            ingest_samples.clone(),
        );
        registry.register(
            "blocks_built",
            "Cumulative profile sample blocks flushed to object storage by the block-builder.",
            blocks_built.clone(),
        );
        registry.register(
            "query_requests",
            "Query requests handled, labelled by route and outcome (ok/error).",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Per-route query handler latency in seconds.",
            query_duration.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_samples,
            blocks_built,
            query_requests,
            query_duration,
        }
    }

    /// Record one ingest request outcome: bump the per-status request counter,
    /// add to the cumulative bytes/items counters, and observe the latency.
    ///
    /// This does NOT touch `wal_append_failures` — increment that separately at
    /// the actual WAL/produce error site (a 4xx client/validation error is an
    /// `ok=false` request but not a WAL failure).
    pub fn record_ingest(&self, ok: bool, bytes: u64, items: u64, secs: f64) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        if bytes > 0 {
            self.ingest_bytes.inc_by(bytes);
        }
        if items > 0 {
            self.ingest_items.inc_by(items);
        }
        self.ingest_duration.observe(secs);
    }

    /// Record one WAL/produce append failure (the durable write to the profiles
    /// WAL topic failed). Distinct from a 4xx client/validation rejection.
    pub fn record_wal_append_failure(&self) {
        self.wal_append_failures.inc();
    }

    /// Add `samples` to the per-tenant cumulative ingested-samples counter.
    /// Called once per ingest request with the number of WAL samples that
    /// request produced (a no-op when `samples == 0`).
    pub fn record_ingest_samples(&self, tenant: &str, samples: u64) {
        if samples == 0 {
            return;
        }
        self.ingest_samples
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(samples);
    }

    /// Add `blocks` to the cumulative block-builder blocks-flushed counter.
    /// Called once per block-build poll batch with the number of blocks the
    /// flush wrote to object storage (a no-op when `blocks == 0`).
    pub fn record_blocks_built(&self, blocks: u64) {
        if blocks == 0 {
            return;
        }
        self.blocks_built.inc_by(blocks);
    }

    /// Record one query request outcome on `route`: bump the per-route+status
    /// request counter and observe the per-route latency.
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

/// Build the `/metrics` router serving the `OpenMetrics` text exposition of
/// `registry`. The pprof routes are merged separately by `serve_admin`.
pub fn metrics_router(registry: SharedRegistry) -> axum::Router {
    axum::Router::new()
        .route("/metrics", axum::routing::get(export))
        .with_state(registry)
}

async fn export(
    axum::extract::State(reg): axum::extract::State<SharedRegistry>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
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
    use super::*;
    use assert2::assert;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    #[tokio::test]
    async fn registry_has_profiles_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, 1024, 3, 0.012);
        m.record_ingest(false, 0, 0, 0.001);
        m.record_wal_append_failure();
        m.record_ingest_samples("tenant-a", 3);
        m.record_blocks_built(2);
        m.record_query("select_series", true, 0.5);
        m.record_query("render", false, 0.1);

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "crabka_profiles_ingest_requests_total",
            "crabka_profiles_ingest_bytes_total",
            "crabka_profiles_ingest_items_total",
            "crabka_profiles_ingest_duration_seconds",
            "crabka_profiles_wal_append_failures_total",
            "crabka_profiles_ingest_samples_total",
            "crabka_profiles_blocks_built_total",
            "crabka_profiles_query_requests_total",
            "crabka_profiles_query_duration_seconds",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
        assert!(
            buf.contains("tenant=\"tenant-a\""),
            "tenant label missing in:\n{buf}"
        );
        assert!(buf.contains("status=\"ok\""), "ok status label missing");
        assert!(
            buf.contains("status=\"error\""),
            "error status label missing"
        );
        assert!(
            buf.contains("route=\"select_series\""),
            "route label missing"
        );
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
        assert!(resp.status() == StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/openmetrics-text"), "ct={ct}");
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("crabka_profiles_ingest_requests_total"), "{s}");
        assert!(s.contains("# EOF"), "{s}");
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // An ok=false request alone must NOT bump wal_append_failures.
        m.record_ingest(false, 0, 0, 0.001);
        assert!(m.wal_append_failures.get() == 0);
        // Only the explicit WAL-failure call does.
        m.record_wal_append_failure();
        assert!(m.wal_append_failures.get() == 1);
    }
}
