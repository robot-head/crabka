//! HTTP server helpers for the admin UI.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;

use crate::config::AdminUiConfig;
use crate::session::SessionStore;

const ROOT_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Crabka Admin</title>
  </head>
  <body>
    <main class="operations-shell">
      <h1>Crabka Admin</h1>
      <p>Standalone broker administration UI checkpoint.</p>
      <nav aria-label="Admin sections">
        <a href="/login">Login</a>
        <a href="/topics">Topics</a>
        <a href="/groups">Consumer Groups</a>
        <a href="/acls">ACLs</a>
        <a href="/users">SCRAM Users</a>
        <a href="/quotas">Quotas</a>
        <a href="/log-dirs">Log Dirs</a>
      </nav>
    </main>
  </body>
</html>"#;

const LOGIN_HTML: &str = r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Sign in to Crabka</title>
  </head>
  <body>
    <main class="login-shell">
      <h1>Sign in to Crabka</h1>
      <p>Authentication is required before broker operations are shown.</p>
    </main>
  </body>
</html>"#;

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
    Router::new().route("/healthz", get(healthz))
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(root))
        .route("/login", get(login))
        .with_state(state)
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

async fn root() -> Html<&'static str> {
    Html(ROOT_HTML)
}

async fn login() -> Html<&'static str> {
    Html(LOGIN_HTML)
}
