//! Liveness and readiness endpoints.
//!
//! `/healthz` is always 200 once the gateway serves. `/readyz` returns 503
//! until the dedup store has warmed up, so a load balancer does not route
//! dedup'd traffic to a cold replica.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::{Router, extract::State, http::StatusCode, routing::get};

/// Shared readiness flag. It turns `true` once the dedup store is warm and the
/// gateway can serve dedup'd traffic correctly. It is cheap to clone.
#[derive(Clone, Default)]
pub struct Readiness(pub Arc<AtomicBool>);

impl Readiness {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn set_ready(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn set_not_ready(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Build the health and readiness router, with the readiness flag as state.
pub fn router(readiness: Readiness) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(|State(r): State<Readiness>| async move {
                if r.is_ready() {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }),
        )
        .with_state(readiness)
}
