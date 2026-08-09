//! Prometheus metrics for the metrics-subsystem query (querier) role.
//!
//! This module mirrors the `metrics` pattern of the broker: a shared `Registry`
//! in an `Arc<Mutex<…>>`, and a bundle of metric handles that is cheap to
//! `Clone`. The query handlers clone the bundle and increment the handles
//! directly. The registry prefix is `crabka_metrics`. `prometheus-client`
//! appends `_total` to counters at encode time, so this module registers counter
//! names without the suffix.
//!
//! This bundle has the same shape as the bundle of the ingest crate
//! (`crabka_metrics::metrics`). Both processes export under the same
//! `crabka_metrics` prefix, but they run in separate binaries.

use std::sync::Arc;

use crabka_units::prelude::*;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge, histogram::Histogram},
    registry::Registry,
};
use tokio::sync::Mutex;

/// Shared registry that owns every metric this process emits. It is in an
/// `Arc<Mutex<…>>`, because `prometheus-client` needs `&mut Registry` to
/// register a metric and the exporter needs shared read access at scrape time.
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

/// Query-shape label: `type="instant"` or `type="range"`.
///
/// The label separates the engine-eval latency histogram and the eval-error
/// counter by the kind of `PromQL` query, and not by the HTTP route. For
/// example, `query` and a remote-read fanned instant query both have
/// `type="instant"`.
///
/// The field is the raw identifier `r#type`. The `EncodeLabelSet` derive maps a
/// keyword-raw ident back to its bare form, so the field encodes as the label
/// key `type`. The derive of this crate supports only `flatten`, not a `rename`
/// attribute.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct QueryTypeLabel {
    pub r#type: String,
}

/// Bundle of metric handles that is cheap to clone. Build it one time with
/// [`ServiceMetrics::new`]. Give a clone, one `Arc::clone` each, to every
/// handler that emits a metric.
#[derive(Clone)]
pub struct ServiceMetrics {
    pub registry: SharedRegistry,
    // INGEST (distributor) role.
    pub ingest_requests: Family<StatusLabel, Counter>,
    pub ingest_bytes: Counter,
    pub ingest_items: Counter,
    pub ingest_duration: Histogram,
    pub wal_append_failures: Counter,
    // QUERY (querier) role.
    pub query_requests: Family<RouteStatusLabel, Counter>,
    pub query_duration: Family<RouteLabel, Histogram>,
    /// PromQL-engine evaluation latency (parse + plan + execute), labelled by
    /// query `type` (`instant`|`range`). This scope is narrower than
    /// `query_duration`, which covers the whole HTTP handler: param decode,
    /// permit wait, and encode.
    pub query_eval_duration: Family<QueryTypeLabel, Histogram>,
    /// Cumulative engine-eval failures, labelled by query `type`.
    pub query_errors: Family<QueryTypeLabel, Counter>,
    /// In-flight `PromQL` queries currently executing in the engine.
    pub active_queries: Gauge,
}

impl ServiceMetrics {
    /// Builds a new registry, registers every metric, and returns the bundle.
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

        let query_requests: Family<RouteStatusLabel, Counter> = Family::default();
        let query_duration: Family<RouteLabel, Histogram> = Family::new_with_constructor(|| {
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
        });
        let query_eval_duration: Family<QueryTypeLabel, Histogram> =
            Family::new_with_constructor(|| {
                Histogram::new([0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
            });
        let query_errors: Family<QueryTypeLabel, Counter> = Family::default();
        let active_queries = Gauge::default();

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
            "query_requests",
            "Query requests handled, labelled by route and outcome status.",
            query_requests.clone(),
        );
        registry.register(
            "query_duration_seconds",
            "Query handler latency in seconds, labelled by route.",
            query_duration.clone(),
        );
        registry.register(
            "query_eval_duration_seconds",
            "PromQL engine evaluation latency in seconds (parse+plan+execute), labelled by query type.",
            query_eval_duration.clone(),
        );
        registry.register(
            "query_errors",
            "Cumulative PromQL engine evaluation failures, labelled by query type.",
            query_errors.clone(),
        );
        registry.register(
            "active_queries",
            "PromQL queries currently executing in the engine.",
            active_queries.clone(),
        );

        Self {
            registry: Arc::new(Mutex::new(registry)),
            ingest_requests,
            ingest_bytes,
            ingest_items,
            ingest_duration,
            wal_append_failures,
            query_requests,
            query_duration,
            query_eval_duration,
            query_errors,
            active_queries,
        }
    }

    /// Records one ingest request outcome.
    ///
    /// This method does NOT touch `wal_append_failures`. Increment that counter
    /// at the WAL or produce error site, so that a 4xx client or validation
    /// error does not inflate the WAL-failure counter.
    pub fn record_ingest(&self, ok: bool, size: ByteSize, items: u64, latency: Time) {
        let status = if ok { "ok" } else { "error" };
        self.ingest_requests
            .get_or_create(&StatusLabel {
                status: status.into(),
            })
            .inc();
        self.ingest_bytes.inc_by(size.bytes_u64());
        self.ingest_items.inc_by(items);
        // Prometheus histograms are in base units, so the latency lands in
        // seconds no matter what unit the caller measured it in.
        self.ingest_duration.observe(latency.secs_f64());
    }

    /// Records one query request outcome on `route` with its latency.
    pub fn record_query(&self, route: &str, ok: bool, latency: Time) {
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
            .observe(latency.secs_f64());
    }

    /// Records one `PromQL` engine evaluation.
    ///
    /// This method observes `latency` under `query_eval_duration{type}`. When
    /// `ok` is false, it also increments `query_errors{type}`. `query_type` is
    /// `"instant"` or `"range"`.
    pub fn record_eval(&self, query_type: &str, ok: bool, latency: Time) {
        self.query_eval_duration
            .get_or_create(&QueryTypeLabel {
                r#type: query_type.into(),
            })
            .observe(latency.secs_f64());
        if !ok {
            self.query_errors
                .get_or_create(&QueryTypeLabel {
                    r#type: query_type.into(),
                })
                .inc();
        }
    }

    /// Increments `active_queries` at query entry, without an RAII guard.
    ///
    /// Pair every call with [`Self::query_finished`].
    pub fn query_started(&self) {
        self.active_queries.inc();
    }

    /// Decrements `active_queries` at query exit. Pairs with
    /// [`Self::query_started`].
    pub fn query_finished(&self) {
        self.active_queries.dec();
    }
}

impl Default for ServiceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Builds the `/metrics` exporter router.
///
/// The admin server merges this router with the pprof routes through
/// `serve_admin_from_env_with`. Do not merge `pprof_router` here.
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
    use assert2::check;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn registry_has_metrics_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        // Exercise the ingest helpers too so every counter family materializes
        // a sample line (an empty Family emits only # HELP/# TYPE metadata,
        // which carry the name WITHOUT the `_total` suffix).
        m.record_ingest(true, kibibytes(1), 5, millis(12));
        m.wal_append_failures.inc();
        m.record_query("query", true, millis(50));
        m.record_query("query_range", false, millis(1500));
        m.record_query("series", true, millis(200));
        m.record_query("labels", true, millis(100));
        m.record_query("label_values", true, millis(100));
        // Engine-eval metrics: an instant success, a range failure, and some
        // in-flight tracking so every new metric materializes a sample line.
        m.record_eval("instant", true, millis(20));
        m.record_eval("range", false, millis(1200));
        m.query_started();
        m.query_started();
        m.query_finished();

        let mut buf = String::new();
        let r = m.registry.lock().await;
        prometheus_client::encoding::text::encode(&mut buf, &r).unwrap();
        for needle in [
            "crabka_metrics_ingest_requests_total",
            "crabka_metrics_ingest_bytes_total",
            "crabka_metrics_ingest_items_total",
            "crabka_metrics_ingest_duration_seconds",
            "crabka_metrics_wal_append_failures_total",
            "crabka_metrics_query_requests_total",
            "crabka_metrics_query_duration_seconds",
            "crabka_metrics_query_eval_duration_seconds",
            "crabka_metrics_query_errors_total",
            "crabka_metrics_active_queries",
            "route=\"query\"",
            "route=\"query_range\"",
            "status=\"error\"",
            // The `r#type` field must encode as the bare `type` label key.
            "type=\"instant\"",
            "type=\"range\"",
            // One `query_started` is still outstanding (2 inc, 1 dec) → gauge == 1.
            "crabka_metrics_active_queries 1",
        ] {
            assert2::assert!(buf.contains(needle));
        }
    }

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = ServiceMetrics::new();
        m.record_query("query", true, millis(10));
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
        check!(resp.status() == StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), kibibytes(64).bytes_usize())
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        check!(s.contains("crabka_metrics_query_requests_total"), "{s}");
        check!(s.contains("# EOF"), "{s}");
    }
}
