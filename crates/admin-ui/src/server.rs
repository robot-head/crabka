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

const TOPICS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Topics</title></head><body><main><h1>Topics</h1><p>No topics loaded yet.</p></main></body></html>"#;
const GROUPS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Consumer Groups</title></head><body><main><h1>Consumer Groups</h1><p>No consumer groups loaded yet.</p></main></body></html>"#;
const ACLS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ACLs</title></head><body><main><h1>ACLs</h1><p>No ACL operation selected.</p></main></body></html>"#;
const USERS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>SCRAM Users</title></head><body><main><h1>SCRAM Users</h1><p>No user operation selected.</p></main></body></html>"#;
const QUOTAS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Quotas</title></head><body><main><h1>Quotas</h1><p>No quota data loaded yet.</p></main></body></html>"#;
const LOG_DIRS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Log Dirs</title></head><body><main><h1>Log Dirs</h1><p>No log-dir data loaded yet.</p></main></body></html>"#;

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
        .route("/topics", get(topics))
        .route("/groups", get(groups))
        .route("/acls", get(acls))
        .route("/users", get(users))
        .route("/quotas", get(quotas))
        .route("/log-dirs", get(log_dirs))
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

async fn topics() -> Html<&'static str> {
    Html(TOPICS_HTML)
}

async fn groups() -> Html<&'static str> {
    Html(GROUPS_HTML)
}

async fn acls() -> Html<&'static str> {
    Html(ACLS_HTML)
}

async fn users() -> Html<&'static str> {
    Html(USERS_HTML)
}

async fn quotas() -> Html<&'static str> {
    Html(QUOTAS_HTML)
}

async fn log_dirs() -> Html<&'static str> {
    Html(LOG_DIRS_HTML)
}
