//! Prometheus `/metrics` HTTP server. Mirrors the operator
//! crate's `health` pattern: a small axum app exposing one route.
//! Returns `OpenMetrics` text on success; 500 on encoder failure.
//!
//! The server is spawned by [`crate::Broker::start`] when
//! `BrokerConfig::metrics_listen_addr` is `Some`. Cancelled via the
//! broker's supervisor shutdown token.

use std::net::SocketAddr;

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio_util::sync::CancellationToken;

use crate::metrics::SharedRegistry;

/// Build the router: `/metrics` (Prometheus) plus `/debug/pprof/{profile,heap}`
/// (CPU always on Unix; heap when built with `--features heap-profiling`).
pub fn router(
    registry: SharedRegistry,
    profiling: crabka_telemetry::profiling::ProfilingConfig,
) -> Result<Router, crabka_telemetry::profiling::ProfilingError> {
    Ok(Router::new()
        .route("/metrics", get(metrics))
        .with_state(registry)
        .merge(crabka_telemetry::profiling::pprof_router_with_config(
            profiling,
        )?))
}

/// Bind and serve until `shutdown` fires. Returns the bound address so
/// integration tests can scrape `127.0.0.1:0`-style configs without
/// guessing ports. On any axum error (typically socket close), logs and
/// returns.
pub(crate) async fn run(
    addr: SocketAddr,
    registry: SharedRegistry,
    profiling: crabka_telemetry::profiling::ProfilingConfig,
    shutdown: CancellationToken,
) -> Result<SocketAddr, crabka_telemetry::profiling::ProfilingError> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tracing::info!(%bound, "metrics server listening");
    let app = router(registry, profiling)?;
    tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });
        if let Err(e) = server.await {
            tracing::warn!(error = %e, "metrics server error");
        }
    });
    Ok(bound)
}

async fn metrics(State(registry): State<SharedRegistry>) -> impl IntoResponse {
    let mut buf = String::new();
    let r = registry.lock().await;
    if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &r) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
    }
    (
        StatusCode::OK,
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
    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;

    use super::*;
    use crate::metrics::BrokerMetrics;

    #[tokio::test]
    async fn metrics_route_returns_openmetrics() {
        let m = BrokerMetrics::new();
        m.record_produce("t", 42);
        let app = router(
            m.registry,
            crabka_telemetry::profiling::ProfilingConfig::default(),
        )
        .unwrap();
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
        for needle in ["crabka_broker_topic_bytes_in_total", "42", "# EOF"] {
            assert!(s.contains(needle), "missing {needle:?} in {s}");
        }
    }

    #[test]
    fn router_rejects_invalid_profiling_policy() {
        let m = BrokerMetrics::new();
        let profiling = crabka_telemetry::profiling::ProfilingConfig {
            profiling_cpu_default_duration: crabka_units::secs(61),
            ..crabka_telemetry::profiling::ProfilingConfig::default()
        };
        assert!(router(m.registry, profiling).is_err());
    }
}
