//! HTTP server helpers for the admin UI.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Html;
use axum::routing::get;

use crate::config::AdminUiConfig;
use crate::dto::{GroupRow, LogDirRow, TopicRow};
use crate::error::UiError;
use crate::server_fns::{self, AdminSeamFactory, BrokerAdminSeamFactory, ServerFunctionContext};
use crate::session::SessionStore;

pub const SESSION_COOKIE_NAME: &str = "crabka_admin_session";

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

const ACLS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ACLs</title></head><body><main><h1>ACLs</h1><p>No ACL operation selected.</p></main></body></html>"#;
const USERS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>SCRAM Users</title></head><body><main><h1>SCRAM Users</h1><p>No user operation selected.</p></main></body></html>"#;
const QUOTAS_HTML: &str = r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Quotas</title></head><body><main><h1>Quotas</h1><p>No quota data loaded yet.</p></main></body></html>"#;

#[derive(Debug, Clone)]
pub struct AppState {
    pub cfg: Arc<AdminUiConfig>,
    pub sessions: Arc<SessionStore>,
}

#[derive(Clone)]
pub struct AdminRouterState<F> {
    app: AppState,
    seam_factory: F,
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
    router_with_factory(state, BrokerAdminSeamFactory)
}

pub fn router_with_factory<F>(state: AppState, seam_factory: F) -> Router
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
{
    let router_state = AdminRouterState {
        app: state,
        seam_factory,
    };

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
        .with_state(router_state)
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

async fn topics<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_page("Topics", "<p>Authentication required.</p>"));
    };

    Html(match server_fns::list_topics_with_context(&context).await {
        Ok(rows) => render_topics(rows),
        Err(UiError::NotAuthenticated) => render_page("Topics", "<p>Authentication required.</p>"),
        Err(_) => render_page("Topics", "<p>Unable to load topics.</p>"),
    })
}

async fn groups<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_page(
            "Consumer Groups",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_groups_with_context(&context).await {
        Ok(rows) => render_groups(rows),
        Err(UiError::NotAuthenticated) => {
            render_page("Consumer Groups", "<p>Authentication required.</p>")
        }
        Err(_) => render_page("Consumer Groups", "<p>Unable to load consumer groups.</p>"),
    })
}

async fn acls<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
{
    protected_static_page(&state, &headers, "ACLs", ACLS_HTML)
}

async fn users<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
{
    protected_static_page(&state, &headers, "SCRAM Users", USERS_HTML)
}

async fn quotas<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
{
    protected_static_page(&state, &headers, "Quotas", QUOTAS_HTML)
}

async fn log_dirs<F>(State(state): State<AdminRouterState<F>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_page("Log Dirs", "<p>Authentication required.</p>"));
    };

    Html(
        match server_fns::list_log_dirs_with_context(&context).await {
            Ok(rows) => render_log_dirs(rows),
            Err(UiError::NotAuthenticated) => {
                render_page("Log Dirs", "<p>Authentication required.</p>")
            }
            Err(_) => render_page("Log Dirs", "<p>Unable to load log-dir data.</p>"),
        },
    )
}

fn context_from_headers<'a, F>(
    state: &'a AdminRouterState<F>,
    headers: &'a HeaderMap,
) -> Option<ServerFunctionContext<'a, F>> {
    let raw_session_id = session_cookie(headers)?;

    Some(ServerFunctionContext::new(
        &state.app.cfg,
        &state.app.sessions,
        Some(raw_session_id),
        &state.seam_factory,
    ))
}

fn protected_static_page<F>(
    state: &AdminRouterState<F>,
    headers: &HeaderMap,
    title: &str,
    authenticated_html: &str,
) -> Html<String> {
    let Some(context) = context_from_headers(state, headers) else {
        return Html(render_page(title, "<p>Authentication required.</p>"));
    };

    match server_fns::current_session_with_context(&context) {
        Ok(_) => Html(authenticated_html.to_string()),
        Err(UiError::NotAuthenticated) => {
            Html(render_page(title, "<p>Authentication required.</p>"))
        }
        Err(_) => Html(render_page(title, "<p>Unable to load page.</p>")),
    }
}

fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;

    cookie_header.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == SESSION_COOKIE_NAME).then_some(value)
    })
}

fn render_topics(rows: Vec<TopicRow>) -> String {
    if rows.is_empty() {
        return render_page("Topics", "<p>No topics loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_name = escape_html(&row.name);
        write!(rendered_rows, "<li>{escaped_name}</li>").expect("writing to String cannot fail");
    }

    render_page("Topics", &format!("<ul>{rendered_rows}</ul>"))
}

fn render_groups(rows: Vec<GroupRow>) -> String {
    if rows.is_empty() {
        return render_page("Consumer Groups", "<p>No consumer groups loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_group_id = escape_html(&row.group_id);
        write!(rendered_rows, "<li>{escaped_group_id}</li>")
            .expect("writing to String cannot fail");
    }

    render_page("Consumer Groups", &format!("<ul>{rendered_rows}</ul>"))
}

fn render_log_dirs(rows: Vec<LogDirRow>) -> String {
    if rows.is_empty() {
        return render_page("Log Dirs", "<p>No log-dir data loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_log_dir = escape_html(&row.log_dir);
        let escaped_topic = escape_html(&row.topic);
        write!(
            rendered_rows,
            "<li>{escaped_log_dir} {escaped_topic}/{}-{}</li>",
            row.partition, row.partition_size
        )
        .expect("writing to String cannot fail");
    }

    render_page("Log Dirs", &format!("<ul>{rendered_rows}</ul>"))
}

fn render_page(title: &str, body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{}</title></head><body><main><h1>{}</h1>{}</main></body></html>"#,
        escape_html(title),
        escape_html(title),
        body
    )
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
