//! HTTP server helpers for the admin UI.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

use crate::config::AdminUiConfig;
use crate::session::SessionStore;

#[derive(Debug, Clone)]
pub struct AppState {
    pub cfg: Arc<AdminUiConfig>,
    pub sessions: Arc<SessionStore>,
}

impl AppState {
    #[must_use]
    pub fn new(cfg: AdminUiConfig) -> Self {
        let session_ttl = Duration::from_secs(cfg.session_ttl_seconds);

        Self {
            cfg: Arc::new(cfg),
            sessions: Arc::new(SessionStore::new(session_ttl)),
        }
    }

    #[must_use]
    pub const fn from_parts(cfg: Arc<AdminUiConfig>, sessions: Arc<SessionStore>) -> Self {
        Self { cfg, sessions }
    }

    #[must_use]
    pub fn sessions_ttl_seconds(&self) -> u64 {
        self.sessions.ttl().as_secs()
    }
}

pub fn health_router() -> Router {
    Router::new().route("/healthz", get(|| async { StatusCode::OK }))
}
