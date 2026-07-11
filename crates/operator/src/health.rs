use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};

use crate::telemetry::SharedRegistry;

#[derive(Clone)]
pub struct HealthState {
    /// Shared metrics registry. Cloned by controllers that need to
    /// register metrics; read by the `/metrics` handler.
    pub registry: SharedRegistry,
    ready: Arc<AtomicBool>,
}

impl HealthState {
    #[must_use]
    pub fn new(registry: SharedRegistry) -> Self {
        Self {
            registry,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Mark the operator as ready to serve. Reflected in `/readyz`.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }
}

pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Bind and serve forever. Returns only on socket error or shutdown signal.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn serve(addr: SocketAddr, state: HealthState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "health server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(s): State<HealthState>) -> impl IntoResponse {
    if s.ready.load(Ordering::Acquire) {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metrics(State(s): State<HealthState>) -> impl IntoResponse {
    let mut buf = String::new();
    let r = s.registry.lock().await;
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
    use http::StatusCode as Code;
    use tokio::sync::Mutex;
    use tower::ServiceExt as _;

    use super::*;

    fn fixture() -> HealthState {
        HealthState::new(Arc::new(Mutex::new(crate::telemetry::new_registry())))
    }

    #[tokio::test]
    async fn healthz_ok() {
        let app = router(fixture());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == Code::OK);
    }

    #[tokio::test]
    async fn readyz_503_until_marked() {
        let state = fixture();
        let app = router(state.clone());
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == Code::SERVICE_UNAVAILABLE);

        state.mark_ready();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == Code::OK);
    }

    #[tokio::test]
    async fn metrics_returns_openmetrics() {
        let app = router(fixture());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == Code::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/openmetrics-text"));

        // Body should be valid OpenMetrics text: encoder always emits `# EOF`
        // as the terminator. Catches a future regression where the route is
        // wired but the encoder isn't actually run.
        let body_bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("# EOF"), "metrics body missing # EOF: {body}");
    }
}
