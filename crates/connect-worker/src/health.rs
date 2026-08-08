//! HTTP liveness, readiness, and Prometheus endpoints.

use std::net::SocketAddr;

use anyhow::Context as _;
use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::metrics::WorkerMetrics;

pub(crate) fn router(metrics: WorkerMetrics) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/metrics", get(prometheus))
        .with_state(metrics)
}

pub(crate) async fn start(
    listen: SocketAddr,
    metrics: WorkerMetrics,
    shutdown: CancellationToken,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind health listener {listen}"))?;
    let bound = listener
        .local_addr()
        .context("read health listener address")?;
    tracing::info!(%bound, "connector health server listening");
    let app = router(metrics);
    Ok(tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
            .context("serve connector health endpoints")
    }))
}

async fn live(State(metrics): State<WorkerMetrics>) -> StatusCode {
    if metrics.is_live() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn ready(State(metrics): State<WorkerMetrics>) -> StatusCode {
    if metrics.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn prometheus(State(metrics): State<WorkerMetrics>) -> impl IntoResponse {
    let mut body = String::new();
    let registry = metrics.registry.lock().await;
    match prometheus_client::encoding::text::encode(&mut body, &registry) {
        Ok(()) => (
            StatusCode::OK,
            [(
                "content-type",
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )],
            body,
        )
            .into_response(),
        Err(error) => {
            metrics.record_error();
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use axum::{body::Body, http::Request};
    use tower::ServiceExt as _;

    use super::*;

    #[tokio::test]
    async fn health_and_metrics_follow_worker_state() {
        let metrics = WorkerMetrics::new();
        metrics.set_live(true);
        let app = router(metrics.clone());

        let live = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/live")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let ready = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(live.status() == StatusCode::OK);
        assert!(ready.status() == StatusCode::SERVICE_UNAVAILABLE);

        metrics.set_ready(true);
        let ready = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(ready.status() == StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body");
        let text = std::str::from_utf8(&body).expect("OpenMetrics is UTF-8");
        assert!(text.contains("crabka_connect_worker_live 1"));
        assert!(text.contains("crabka_connect_worker_ready 1"));
    }
}
