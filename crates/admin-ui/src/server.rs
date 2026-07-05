//! HTTP server helpers for the admin UI.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Form, Router};

use crate::auth::{AdminClientLoginBroker, LoginBroker, LoginRequest};
use crate::config::AdminUiConfig;
use crate::error::UiError;
use crate::server_fns::{self, AdminSeamFactory, BrokerAdminSeamFactory, ServerFunctionContext};
use crate::session::SessionStore;
use crate::views::{ReadRouteState, Route, RoutePage, render_page};

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
        .route("/", get(root::<F, B>))
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

async fn root<F, B>(State(state): State<AdminRouterState<F, B>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let page = if is_authenticated(&state, &headers) {
        RoutePage::for_authenticated_route(Route::Overview)
    } else {
        RoutePage::for_unauthenticated_route(Route::Overview)
    };

    Html(render_page(&page))
}

async fn login() -> Html<String> {
    Html(render_page(&RoutePage::login()))
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
            Html(render_page(&RoutePage::login_failed())),
        )
            .into_response();
    };

    let cookie = format!(
        "{SESSION_COOKIE_NAME}={}; HttpOnly; SameSite=Lax; Path=/",
        success.session_id
    );

    (
        [(header::SET_COOKIE, cookie)],
        Html(render_page(&RoutePage::signed_in())),
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
        return Html(render_page(&RoutePage::topics(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(match server_fns::list_topics_with_context(&context).await {
        Ok(rows) => render_page(&RoutePage::topics(ReadRouteState::Rows(rows))),
        Err(UiError::NotAuthenticated) => {
            render_page(&RoutePage::topics(ReadRouteState::AuthenticationRequired))
        }
        Err(_) => render_page(&RoutePage::topics(ReadRouteState::LoadFailed)),
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
        return Html(render_page(&RoutePage::groups(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(match server_fns::list_groups_with_context(&context).await {
        Ok(rows) => render_page(&RoutePage::groups(ReadRouteState::Rows(rows))),
        Err(UiError::NotAuthenticated) => {
            render_page(&RoutePage::groups(ReadRouteState::AuthenticationRequired))
        }
        Err(_) => render_page(&RoutePage::groups(ReadRouteState::LoadFailed)),
    })
}

async fn acls<F, B>(State(state): State<AdminRouterState<F, B>>, headers: HeaderMap) -> Html<String>
where
    F: AdminSeamFactory + Clone + Send + Sync + 'static,
    for<'a> F::Reader<'a>: Send,
    B: LoginBroker + Clone + Send + Sync + 'static,
{
    let Some(context) = context_from_headers(&state, &headers) else {
        return Html(render_page(&RoutePage::acls(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(match server_fns::list_acls(&context).await {
        Ok(rows) => render_page(&RoutePage::acls(ReadRouteState::Rows(rows))),
        Err(UiError::NotAuthenticated) => {
            render_page(&RoutePage::acls(ReadRouteState::AuthenticationRequired))
        }
        Err(_) => render_page(&RoutePage::acls(ReadRouteState::LoadFailed)),
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
        return Html(render_page(&RoutePage::users(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(match server_fns::list_users(&context).await {
        Ok(rows) => render_page(&RoutePage::users(ReadRouteState::Rows(rows))),
        Err(UiError::NotAuthenticated) => {
            render_page(&RoutePage::users(ReadRouteState::AuthenticationRequired))
        }
        Err(_) => render_page(&RoutePage::users(ReadRouteState::LoadFailed)),
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
        return Html(render_page(&RoutePage::quotas(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(match server_fns::list_quotas(&context).await {
        Ok(rows) => render_page(&RoutePage::quotas(ReadRouteState::Rows(rows))),
        Err(UiError::NotAuthenticated) => {
            render_page(&RoutePage::quotas(ReadRouteState::AuthenticationRequired))
        }
        Err(_) => render_page(&RoutePage::quotas(ReadRouteState::LoadFailed)),
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
        return Html(render_page(&RoutePage::log_dirs(
            ReadRouteState::AuthenticationRequired,
        )));
    };

    Html(
        match server_fns::list_log_dirs_with_context(&context).await {
            Ok(rows) => render_page(&RoutePage::log_dirs(ReadRouteState::Rows(rows))),
            Err(UiError::NotAuthenticated) => {
                render_page(&RoutePage::log_dirs(ReadRouteState::AuthenticationRequired))
            }
            Err(_) => render_page(&RoutePage::log_dirs(ReadRouteState::LoadFailed)),
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

fn is_authenticated<F, B>(state: &AdminRouterState<F, B>, headers: &HeaderMap) -> bool {
    let Some(raw_session_id) = session_cookie(headers) else {
        return false;
    };

    server_fns::current_session_with_store(&state.app.sessions, Some(raw_session_id)).is_ok()
}

fn session_cookie(headers: &HeaderMap) -> Option<&str> {
    let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;

    cookie_header.split(';').find_map(|cookie| {
        let (name, value) = cookie.trim().split_once('=')?;
        (name == SESSION_COOKIE_NAME).then_some(value)
    })
}
