//! Liveness/readiness endpoints. `/healthz` is always 200 once serving;
//! `/readyz` returns 503 until the dedup store has warmed up, so load
//! balancers don't route dedup'd traffic to a cold replica.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use axum::{Router, extract::State, http::StatusCode, routing::get};

/// Shared readiness flag, flipped to `true` once the gateway can serve
/// dedup'd traffic correctly (dedup store warmed). Cheaply cloneable.
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

    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Build the health/readiness router, with the readiness flag as state.
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
