//! Plain axum routes for `/healthz`, `/readyz`, `/metrics`. Mounted
//! alongside the Connect-RPC router by the binary entry.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use crate::ingest::SharedSnapshot;
use crate::state_topic::StateBackend;

#[derive(Clone)]
pub struct HealthState {
    pub snapshot: SharedSnapshot,
    pub registry: Arc<Mutex<Registry>>,
    /// Gate `/readyz` on the state topic being fully loaded.
    pub state_topic: Arc<dyn StateBackend>,
}

pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(s): State<HealthState>) -> impl IntoResponse {
    let g = s.snapshot.load();
    if (*g).is_none() {
        return (StatusCode::SERVICE_UNAVAILABLE, "no snapshot yet");
    }
    if !s.state_topic.is_loaded() {
        return (StatusCode::SERVICE_UNAVAILABLE, "state topic loading");
    }
    (StatusCode::OK, "ready")
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

#[must_use]
pub fn new_registry() -> Registry {
    Registry::with_prefix("crabka_rebalancer")
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::{assert, check};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn fixture() -> HealthState {
        HealthState {
            snapshot: crate::ingest::new_shared_snapshot(),
            registry: Arc::new(Mutex::new(new_registry())),
            state_topic: Arc::new(crate::state_topic::fake::InMemoryBackend::new_loaded()),
        }
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
        assert!(resp.status() == StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_503_before_snapshot() {
        let app = router(fixture());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_200_after_snapshot() {
        use crate::model::ClusterState;
        let s = fixture();
        s.snapshot.store(std::sync::Arc::new(Some(ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![],
            partitions: vec![],
            in_flight_reassignments: vec![],
        })));
        let app = router(s);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == StatusCode::OK);
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
        check!(resp.status() == StatusCode::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        check!(ct.starts_with("application/openmetrics-text"));
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        check!(s.contains("# EOF"));
    }
}
