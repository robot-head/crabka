//! Prometheus `/metrics` HTTP server.
//!
//! It mirrors the `health` pattern in the operator crate: a small axum app
//! that exposes one route. It returns `OpenMetrics` text on success, and 500
//! on an encoder failure.
//!
//! [`crate::Broker::start`] spawns the server when
//! `BrokerConfig::metrics_listen_addr` is `Some`. The broker's supervisor
//! shutdown token cancels it.

use std::net::SocketAddr;

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio_util::sync::CancellationToken;

use crate::metrics::SharedRegistry;

/// Builds the router. It serves `/metrics` for Prometheus, and
/// `/debug/pprof/{profile,heap}`. The CPU route is always present on Unix. The
/// heap route needs a build with `--features heap-profiling`.
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

/// Binds and serves until `shutdown` fires. It returns the bound address, so
/// an integration test can scrape a `127.0.0.1:0` config without a guess at
/// the port. On any axum error, which is normally a socket close, it logs the
/// error and returns.
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
