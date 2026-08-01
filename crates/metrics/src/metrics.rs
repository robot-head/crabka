//! Prometheus metrics for the metrics-subsystem ingest (distributor) role.
//!
//! Mirrors the broker's `metrics` pattern: a shared `Registry` wrapped in
//! `Arc<Mutex<…>>` plus a cheaply-`Clone` bundle of metric handles that the
//! ingest handlers clone and increment directly. Registry prefix is
//! `crabka_metrics`; `prometheus-client` auto-appends `_total` to counters at
//! encode time, so counter names are registered without the suffix.

use std::sync::Arc;

use crabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Shared registry owning every metric this process emits. Wrapped in
/// `Arc<Mutex<…>>` because `prometheus-client` requires `&mut Registry` to
/// register and the exporter needs shared read access at scrape time.
pub type SharedRegistry = Arc<Mutex<Registry>>;

/// Request-outcome label: `status="ok"` or `status="error"`.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StatusLabel {
    pub status: String,
}

/// Per-query-route outcome label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}

/// Per-query-route label (latency histogram family).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}

/// Per-tenant label for the accepted-series counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}

/// Cheaply-clonable bundle of metric handles. Construct once with
/// [`ServiceMetrics::new`]; hand out clones (each a single `Arc::clone`) to the
/// handlers that emit.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor) role.
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    /// Accepted series counted per tenant on the ingest path.
    pub ingest_series: Family<TenantLabel, Counter>,
    // COMPACTOR role.
    /// Metric blocks written to object storage by the compactor.
    pub blocks_compacted: Counter,
    // QUERY (querier) role.
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
}

impl ServiceMetrics {
    /// Build a fresh registry, register every metric, and return the bundle.
    #[must_use]
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("crabka_metrics");

        let ingest_requests: Family<StatusLabel, Counter> = Family::default();
        let ingest_bytes = Counter::default();
        let ingest_items = Counter::default();
        let ingest_duration = Histogram::new([
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]);
        let wal_append_failures = Counter::default();
        let ingest_series: Family<TenantLabel, Counter> = Family::default();

        let blocks_compacted = Counter::default();

        let query_requests: Family<RouteStatusLabel, Counter> = Family::default();
        let query_duration: Family<RouteLabel, Histogram> = Family::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });

        registry.register(
            "ingest_requests",
            "Ingest (push) requests handled, labelled by outcome status.",
            ingest_requests.clone(),
        );
        registry.register(
            "ingest_bytes",
            "Cumulative request-body bytes accepted on the ingest path.",
            ingest_bytes.clone(),
        );
        registry.register(
            "ingest_items",
            "Cumulative items (series/samples) accepted on the ingest path.",
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
            "ingest_series",
            "Accepted series on the ingest path, labelled by tenant.",
            ingest_series.clone(),
        );
        registry.register(
            "blocks_compacted",
            "Metric blocks written to object storage by the compactor.",
            blocks_compacted.clone(),
        );
        registry.register(
            "query_requests",
            "Query requests handled, labelled by route and outcome status.",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Query handler latency in seconds, labelled by route.",
            query_duration.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            ingest_series,
            blocks_compacted,
            query_requests,
            query_duration,
        }
    }

    /// Record one ingest request outcome. `wal_append_failures` is NOT touched
    /// here — increment it separately at the actual WAL/produce error site so a
    /// 4xx client/validation error does not inflate the WAL-failure counter.
    ///
    /// `body` is the request-body size and `elapsed` the handler latency; both
    /// are converted to the raw units the Prometheus instruments hold here, so
    /// callers never spell out `_bytes`/`_secs` themselves.
    pub fn record_ingest(&self, ok: bool, body: ByteSize, items: u64, elapsed: Time) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        self.ingest_bytes.inc_by(body.bytes_u64());
        self.ingest_items.inc_by(items);
        self.ingest_duration.observe(elapsed.secs_f64());
    }

    /// Record `series` accepted series for `tenant` on the ingest path. Called
    /// once per accepted push request, after the body decodes to a series count.
    pub fn record_ingest_series(&self, tenant: &str, series: u64) {
        if series == 0 {
            return;
        }
        self.ingest_series
            .get_or_create(&TenantLabel {
                tenant: tenant.into(),
            })
            .inc_by(series);
    }

    /// Record `blocks` metric blocks written by the compactor in one flush.
    pub fn record_blocks_compacted(&self, blocks: u64) {
        if blocks == 0 {
            return;
        }
        self.blocks_compacted.inc_by(blocks);
    }

    /// Record one query request outcome on `route` with its latency.
    pub fn record_query(&self, route: &str, ok: bool, elapsed: Time) {
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
            .observe(elapsed.secs_f64());
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Build the `/metrics` exporter router. The admin server merges this with the
/// pprof routes via `serve_admin_from_env_with`; do not merge `pprof_router`
/// here.
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
    use assert2::assert;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, kibibytes(1), 5, millis(12));
        m.record_ingest(false, ByteSize::ZERO, 0, millis(1));
        m.wal_append_failures.inc();
        m.record_ingest_series("tenant-a", 5);
        m.record_blocks_compacted(3);
        m.record_query("query", true, millis(50));
        m.record_query("query_range", false, millis(1500));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "crabka_metrics_ingest_requests_total",
            "crabka_metrics_ingest_bytes_total",
            "crabka_metrics_ingest_items_total",
            "crabka_metrics_ingest_duration_seconds",
            "crabka_metrics_wal_append_failures_total",
            "crabka_metrics_ingest_series_total",
            "crabka_metrics_blocks_compacted_total",
            "crabka_metrics_query_requests_total",
            "crabka_metrics_query_duration_seconds",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"query\"",
            "tenant=\"tenant-a\"",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[test]
    fn record_ingest_does_not_touch_wal_failures() {
        let m = ServiceMetrics::new();
        // An error outcome must NOT bump wal_append_failures — that is reserved
        // for actual WAL/produce errors, incremented at the append site.
        m.record_ingest(false, ByteSize::ZERO, 0, Time::ZERO);
        assert!(m.wal_append_failures.get() == 0);
    }

    #[tokio::test]
    async fn dimensioned_arguments_export_in_prometheus_base_units() {
        // The instruments hold raw bytes and raw seconds; the quantity seam must
        // scale a `ByteSize`/`Time` into exactly those units, not pass the
        // caller's magnitude through unscaled.
        let m = ServiceMetrics::new();
        m.record_ingest(true, mebibytes(2), 1, millis(250));
        m.record_query("query", true, secs(2));

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "crabka_metrics_ingest_bytes_total 2097152",
            "crabka_metrics_ingest_duration_seconds_sum 0.25",
            "crabka_metrics_query_duration_seconds_sum{route=\"query\"} 2.0",
        ] {
            assert!(buf.contains(needle), "missing {needle} in:\n{buf}");
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(true, bytes(42), 1, millis(10));
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
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("crabka_metrics_ingest_bytes_total"), "{s}");
        assert!(s.contains("# EOF"), "{s}");
    }
}
