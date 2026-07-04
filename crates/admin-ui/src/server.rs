//! HTTP server helpers for the admin UI.

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Form, Router};

use crate::auth::{AdminClientLoginBroker, LoginBroker, LoginRequest};
use crate::config::AdminUiConfig;
use crate::dto::{GroupRow, LogDirRow, TopicRow};
use crate::error::UiError;
use crate::server_fns::{
    self, AclRow, AdminSeamFactory, BrokerAdminSeamFactory, QuotaRow, ServerFunctionContext,
    UserRow,
};
use crate::session::SessionStore;
use crate::views::Route;

pub const SESSION_COOKIE_NAME: &str = "crabka_admin_session";

#[derive(Debug, Clone)]
pub struct AppState {
    pub cfg: Arc<AdminUiConfig>,
    pub sessions: Arc<SessionStore>,
}

#[derive(Clone)]
pub struct AdminRouterState<F, B = AdminClientLoginBroker> {
    app: AppState,
    seam_factory: F,
    login_broker: B,
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
    router_with_factory_and_login_broker(state, BrokerAdminSeamFactory, AdminClientLoginBroker)
}

pub fn router_with_factory<F>(state: AppState, seam_factory: F) -> Router
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
{
    router_with_factory_and_login_broker(state, seam_factory, AdminClientLoginBroker)
}

pub fn router_with_factory_and_login_broker<F, B>(
    state: AppState,
    seam_factory: F,
    login_broker: B,
) -> Router
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let router_state = AdminRouterState {
        app: state,
        seam_factory,
        login_broker,
    };

    Router::new()
        .route("/healthz", get(healthz))
        .route("/", get(root))
        .route("/login", get(login).post(post_login::<F, B>))
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

async fn root() -> Html<String> {
    Html(render_route_page(Route::Overview, "Crabka Admin", ""))
}

async fn login() -> Html<String> {
    Html(render_route_page(
        Route::Login,
        "Sign in to Crabka",
        login_form(),
    ))
}

async fn post_login<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    Form(request): Form<LoginRequest>,
) -> impl IntoResponse
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let result = server_fns::login_with_context(
        &state.app.cfg,
        &state.app.sessions,
        &state.login_broker,
        request,
    )
    .await;

    let Ok(success) = result else {
        return (
            StatusCode::UNAUTHORIZED,
            Html(render_route_page(
                Route::Login,
                "Sign in to Crabka",
                "<p>Authentication failed.</p>",
            )),
        )
            .into_response();
    };

    let cookie = format!(
        "{SESSION_COOKIE_NAME}={}; HttpOnly; SameSite=Lax; Path=/",
        success.session_id
    );

    (
        [(header::SET_COOKIE, cookie)],
        Html(render_route_page(
            Route::Overview,
            "Crabka Admin",
            "<p>Signed in.</p>",
        )),
    )
        .into_response()
}

async fn topics<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::Topics,
            "Topics",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_topics_with_context(&context).await {
        Ok(rows) => render_topics(rows),
        Err(UiError::NotAuthenticated) => {
            render_route_page(Route::Topics, "Topics", "<p>Authentication required.</p>")
        }
        Err(_) => render_route_page(Route::Topics, "Topics", "<p>Unable to load topics.</p>"),
    })
}

async fn groups<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::Groups,
            "Consumer Groups",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_groups_with_context(&context).await {
        Ok(rows) => render_groups(rows),
        Err(UiError::NotAuthenticated) => render_route_page(
            Route::Groups,
            "Consumer Groups",
            "<p>Authentication required.</p>",
        ),
        Err(_) => render_route_page(
            Route::Groups,
            "Consumer Groups",
            "<p>Unable to load consumer groups.</p>",
        ),
    })
}

async fn acls<F, B>(State(state): State<AdminRouterState<F, B>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::Acls,
            "ACLs",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_acls(&context).await {
        Ok(rows) => render_acls(rows),
        Err(UiError::NotAuthenticated) => {
            render_route_page(Route::Acls, "ACLs", "<p>Authentication required.</p>")
        }
        Err(_) => render_route_page(Route::Acls, "ACLs", "<p>Unable to load ACLs.</p>"),
    })
}

async fn users<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::Users,
            "SCRAM Users",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_users(&context).await {
        Ok(rows) => render_users(rows),
        Err(UiError::NotAuthenticated) => render_route_page(
            Route::Users,
            "SCRAM Users",
            "<p>Authentication required.</p>",
        ),
        Err(_) => render_route_page(
            Route::Users,
            "SCRAM Users",
            "<p>Unable to load SCRAM users.</p>",
        ),
    })
}

async fn quotas<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::Quotas,
            "Quotas",
            "<p>Authentication required.</p>",
        ));
    };

    Html(match server_fns::list_quotas(&context).await {
        Ok(rows) => render_quotas(rows),
        Err(UiError::NotAuthenticated) => {
            render_route_page(Route::Quotas, "Quotas", "<p>Authentication required.</p>")
        }
        Err(_) => render_route_page(Route::Quotas, "Quotas", "<p>Unable to load quotas.</p>"),
    })
}

async fn log_dirs<F, B>(
    State(state): State<AdminRouterState<F, B>>,
    headers: HeaderMap,
) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_route_page(
            Route::LogDirs,
            "Log Dirs",
            "<p>Authentication required.</p>",
        ));
    };

    Html(
        match server_fns::list_log_dirs_with_context(&context).await {
            Ok(rows) => render_log_dirs(rows),
            Err(UiError::NotAuthenticated) => render_route_page(
                Route::LogDirs,
                "Log Dirs",
                "<p>Authentication required.</p>",
            ),
            Err(_) => render_route_page(
                Route::LogDirs,
                "Log Dirs",
                "<p>Unable to load log-dir data.</p>",
            ),
        },
    )
}

fn context_from_headers<'a, F, B>(
    state: &'a AdminRouterState<F, B>,
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

fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;

    cookie_header.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == SESSION_COOKIE_NAME).then_some(value)
    })
}

fn render_topics(rows: Vec<TopicRow>) -> String {
    if rows.is_empty() {
        return render_route_page(Route::Topics, "Topics", "<p>No topics loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_name = escape_html(&row.name);
        write!(rendered_rows, "<li>{escaped_name}</li>").expect("writing to String cannot fail");
    }

    render_route_page(
        Route::Topics,
        "Topics",
        &format!("<ul>{rendered_rows}</ul>"),
    )
}

fn render_groups(rows: Vec<GroupRow>) -> String {
    if rows.is_empty() {
        return render_route_page(
            Route::Groups,
            "Consumer Groups",
            "<p>No consumer groups loaded yet.</p>",
        );
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_group_id = escape_html(&row.group_id);
        write!(rendered_rows, "<li>{escaped_group_id}</li>")
            .expect("writing to String cannot fail");
    }

    render_route_page(
        Route::Groups,
        "Consumer Groups",
        &format!("<ul>{rendered_rows}</ul>"),
    )
}

fn render_acls(rows: Vec<AclRow>) -> String {
    if rows.is_empty() {
        return render_route_page(Route::Acls, "ACLs", "<p>No ACLs loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_resource = escape_html(&row.resource);
        let escaped_principal = escape_html(&row.principal);
        let escaped_operation = escape_html(&row.operation);
        let escaped_permission = escape_html(&row.permission);
        write!(
            rendered_rows,
            "<li>{escaped_resource} {escaped_principal} {escaped_operation} {escaped_permission}</li>"
        )
        .expect("writing to String cannot fail");
    }

    render_route_page(Route::Acls, "ACLs", &format!("<ul>{rendered_rows}</ul>"))
}

fn render_users(rows: Vec<UserRow>) -> String {
    if rows.is_empty() {
        return render_route_page(
            Route::Users,
            "SCRAM Users",
            "<p>No SCRAM users loaded yet.</p>",
        );
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_username = escape_html(&row.username);
        let escaped_principal = escape_html(&row.principal);
        write!(
            rendered_rows,
            "<li>{escaped_username} {escaped_principal}</li>"
        )
        .expect("writing to String cannot fail");
    }

    render_route_page(
        Route::Users,
        "SCRAM Users",
        &format!("<ul>{rendered_rows}</ul>"),
    )
}

fn render_quotas(rows: Vec<QuotaRow>) -> String {
    if rows.is_empty() {
        return render_route_page(Route::Quotas, "Quotas", "<p>No quotas loaded yet.</p>");
    }

    let mut rendered_rows = String::new();
    for row in rows {
        let escaped_entity = escape_html(&row.entity);
        let escaped_quota_type = escape_html(&row.quota_type);
        let escaped_value = escape_html(&row.value);
        write!(
            rendered_rows,
            "<li>{escaped_entity} {escaped_quota_type} {escaped_value}</li>"
        )
        .expect("writing to String cannot fail");
    }

    render_route_page(
        Route::Quotas,
        "Quotas",
        &format!("<ul>{rendered_rows}</ul>"),
    )
}

fn render_log_dirs(rows: Vec<LogDirRow>) -> String {
    if rows.is_empty() {
        return render_route_page(
            Route::LogDirs,
            "Log Dirs",
            "<p>No log-dir data loaded yet.</p>",
        );
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

    render_route_page(
        Route::LogDirs,
        "Log Dirs",
        &format!("<ul>{rendered_rows}</ul>"),
    )
}

fn render_route_page(route: Route, title: &str, body: &str) -> String {
    let route_body = dioxus_ssr::render_element(crate::views::render_route(route));

    format!(
        r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{}</title></head><body>{}<main><h1>{}</h1>{}</main></body></html>"#,
        escape_html(title),
        route_body,
        escape_html(title),
        body
    )
}

fn login_form() -> &'static str {
    r#"<form method="post" action="/login"><label>Username <input name="username" autocomplete="username"></label><label>Password <input name="password" type="password" autocomplete="current-password"></label><button type="submit">Sign in</button></form>"#
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
