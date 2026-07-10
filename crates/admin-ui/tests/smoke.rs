use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode, header},
};
use crabka_admin_ui::{
    auth::LoginBroker,
    config::AdminUiConfig,
    dto::{
        AclRequestDto, AlterConfigRequestDto, CreatePartitionsRequestDto, CreateTopicRequestDto,
        DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow, QuotaDeleteDto,
        QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto, TopicRow,
    },
    error::UiError,
    server::{AppState, SESSION_COOKIE_NAME, router, router_with_factory},
    server_fns::{AclRow, AdminMutationSeam, AdminReadSeam, AdminSeamFactory, QuotaRow, UserRow},
    session::{SessionRecord, SessionStore, SessionUser},
    views::{ReadRouteState, Route, RoutePage, render_page, render_route_html},
};
use tower::ServiceExt as _;

fn smoke_app() -> axum::Router {
    let cfg = AdminUiConfig {
        bootstrap_addrs: vec!["127.0.0.1:9092".to_string()],
        ..AdminUiConfig::default()
    };

    router(AppState::new(cfg))
}

async fn get(path: &str) -> axum::response::Response {
    smoke_app()
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds")
}

async fn get_from(
    app: axum::Router,
    path: &str,
    cookie: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder().uri(path);
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }

    app.oneshot(request.body(Body::empty()).expect("request builds"))
        .await
        .expect("router responds")
}

async fn post_form_from(
    app: axum::Router,
    path: &str,
    form_body: &'static str,
) -> axum::response::Response {
    app.oneshot(
        Request::builder()
            .method(Method::POST)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(form_body))
            .expect("request builds"),
    )
    .await
    .expect("router responds")
}

async fn post_json_from(
    app: axum::Router,
    path: &str,
    json_body: impl Into<Body>,
    cookie: Option<String>,
) -> axum::response::Response {
    let mut request = Request::builder()
        .method(Method::POST)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }

    app.oneshot(request.body(json_body.into()).expect("request builds"))
        .await
        .expect("router responds")
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("body can be collected");

    String::from_utf8(bytes.to_vec()).expect("HTML body is UTF-8")
}

fn sample_topic_row() -> TopicRow {
    TopicRow {
        name: "orders".to_string(),
        topic_id: None,
        partition_count: 3,
        replication_factor: 1,
        error: None,
    }
}

fn sample_acl_row() -> AclRow {
    AclRow {
        resource: "Topic:orders".to_string(),
        principal: "User:alice".to_string(),
        operation: "Read".to_string(),
        permission: "Allow".to_string(),
    }
}

#[tokio::test]
async fn healthz_returns_ok() {
    let response = get("/healthz").await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn unauthenticated_login_page_cases() {
    for (name, path, required, forbidden) in [
        (
            "protected root",
            "/",
            vec!["<!doctype html>", "Sign in to Crabka Admin"],
            vec!["operations-shell", "Crabka Operations"],
        ),
        (
            "login route",
            "/login",
            vec![
                "Sign in to Crabka",
                "method=\"post\"",
                "name=\"username\"",
                "name=\"password\"",
            ],
            vec![],
        ),
    ] {
        let response = get(path).await;
        assert_eq!(
            (
                response.status(),
                response.headers().get(header::CONTENT_TYPE),
            ),
            (
                StatusCode::OK,
                Some(&header::HeaderValue::from_static(
                    "text/html; charset=utf-8"
                )),
            ),
            "case {name}"
        );
        let body = response_text(response).await;
        assert!(
            required.iter().all(|needle| body.contains(needle))
                && forbidden.iter().all(|needle| !body.contains(needle)),
            "case {name}: {body}"
        );
    }
}

#[tokio::test]
async fn root_with_valid_cookie_renders_overview_shell() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    let response = get_from(app, "/", cookie).await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert_eq!(body, render_page(&RoutePage::overview()));
    assert_eq!(factory.read_seam_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn posting_login_sets_session_cookie_and_cookie_authenticates_protected_route() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let login_broker = RecordingLoginBroker::default();
    let app = crabka_admin_ui::server::router_with_factory_and_login_broker(
        state,
        factory.clone(),
        login_broker.clone(),
    );
    let password_sentinel = "login-route-password-sentinel";

    let login_response = post_form_from(
        app.clone(),
        "/login",
        "username=alice&password=login-route-password-sentinel",
    )
    .await;

    assert_eq!(login_response.status(), StatusCode::OK);
    let set_cookie = login_response
        .headers()
        .get(header::SET_COOKIE)
        .expect("login sets a session cookie")
        .to_str()
        .expect("cookie is ASCII")
        .to_string();
    assert!(set_cookie.starts_with(&format!("{SESSION_COOKIE_NAME}=")));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("SameSite=Lax"));
    assert!(set_cookie.contains("Path=/"));
    assert_eq!(login_broker.calls.load(Ordering::SeqCst), 1);

    let login_body = response_text(login_response).await;
    assert!(!login_body.contains(password_sentinel));

    let protected_response = get_from(app, "/topics", Some(set_cookie)).await;

    assert_eq!(protected_response.status(), StatusCode::OK);
    let protected_body = response_text(protected_response).await;
    assert!(protected_body.contains("orders"));
}

#[tokio::test]
async fn authenticated_post_mutation_routes_call_admin_mutation_seam() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    for (path, body, expected_resource) in [
        (
            "/topics/create",
            r#"{"name":"orders","partitions":3,"replicas":1,"configs":[]}"#,
            "orders",
        ),
        ("/topics/delete", r#"{"name":"orders"}"#, "orders"),
        (
            "/topics/partitions",
            r#"{"topic":"orders","total_count":6}"#,
            "orders",
        ),
        (
            "/topics/configs",
            r#"{"resource_type":"topic","resource_name":"orders","configs":[{"name":"cleanup.policy","value":"compact"}]}"#,
            "orders",
        ),
        (
            "/acls/create",
            r#"{"resource_type":"topic","resource_name":"orders","principal":"User:alice","operation":"Read","permission":"Allow","host":"*"}"#,
            "User:alice",
        ),
        (
            "/acls/delete",
            r#"{"resource_type":"topic","resource_name":"orders","principal":"User:alice","operation":"Read","permission":"Allow","host":"*"}"#,
            "User:alice",
        ),
        (
            "/users/scram/upsert",
            r#"{"username":"alice","password":"secret","iterations":4096}"#,
            "alice",
        ),
        ("/users/scram/delete", r#"{"username":"alice"}"#, "alice"),
        (
            "/quotas/upsert",
            r#"{"entity":"user=alice","quota_type":"producer_byte_rate","value":1024.0}"#,
            "user=alice",
        ),
        (
            "/quotas/delete",
            r#"{"entity":"user=alice","quota_type":"producer_byte_rate"}"#,
            "user=alice",
        ),
        (
            "/log-dirs/move",
            r#"{"topic":"orders","partition":0,"destination_log_dir":"/var/lib/crabka-1"}"#,
            "orders",
        ),
    ] {
        let response = post_json_from(app.clone(), path, body, Some(cookie.clone())).await;

        assert_eq!(response.status(), StatusCode::OK, "{path} should succeed");
        let text = response_text(response).await;
        assert!(text.contains("status=ok"), "{path} returned {text}");
        assert!(text.contains(expected_resource), "{path} returned {text}");
    }

    assert_eq!(
        factory.mutation_counts(),
        [11, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    );
}

#[tokio::test]
async fn post_mutation_routes_authenticate_before_decoding_request_body() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());

    let response = post_json_from(app, "/topics/create", "not-json", None).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        (
            factory.mutation_seam_calls.load(Ordering::SeqCst),
            factory.total_mutation_calls(),
        ),
        (0, 0)
    );
    let text = response_text(response).await;
    assert!(text.contains("not authenticated"));
}

#[tokio::test]
async fn post_mutation_routes_reject_stale_cookie_before_decoding_request_body() {
    let state = AppState::new(AdminUiConfig::default());
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}=stale-session");

    let response = post_json_from(app, "/topics/create", "not-json", Some(cookie)).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        (
            factory.mutation_seam_calls.load(Ordering::SeqCst),
            factory.total_mutation_calls(),
        ),
        (0, 0)
    );
    let text = response_text(response).await;
    assert!(text.contains("not authenticated"));
}

#[tokio::test]
async fn post_mutation_routes_return_bad_request_for_authenticated_malformed_json() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());

    let response = post_json_from(app, "/topics/create", "not-json", Some(cookie)).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        (
            factory.mutation_seam_calls.load(Ordering::SeqCst),
            factory.total_mutation_calls(),
        ),
        (0, 0)
    );
    let text = response_text(response).await;
    assert!(text.contains("invalid JSON request"));
}

#[tokio::test]
async fn authenticated_post_mutation_routes_reject_oversized_body_before_deserializing() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = format!("{SESSION_COOKIE_NAME}={}", session_id.expose_for_cookie());
    let oversized_topic_name = "orders".repeat(180_000);
    let oversized_body =
        format!(r#"{{"name":"{oversized_topic_name}","partitions":3,"replicas":1,"configs":[]}}"#);

    let response = post_json_from(app, "/topics/create", oversized_body, Some(cookie)).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        (
            factory.mutation_seam_calls.load(Ordering::SeqCst),
            factory.total_mutation_calls(),
        ),
        (0, 0)
    );
    let text = response_text(response).await;
    assert!(text.contains("request body too large"));
}

#[tokio::test]
async fn protected_http_routes_without_cookie_render_guarded_login_page() {
    for (path, route) in [
        ("/topics", Route::Topics),
        ("/groups", Route::Groups),
        ("/acls", Route::Acls),
        ("/users", Route::Users),
        ("/quotas", Route::Quotas),
        ("/log-dirs", Route::LogDirs),
    ] {
        let response = get(path).await;

        assert_eq!(response.status(), StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert_eq!(body, render_route_html(route), "{path} guarded route HTML");
        assert_eq!(body, render_page(&RoutePage::login()), "{path} login HTML");
        assert!(
            !body.contains("operations-shell"),
            "{path} operations shell"
        );
        assert!(
            !body.contains("Authentication required."),
            "{path} auth shell copy"
        );
    }
}

#[tokio::test]
async fn authenticated_read_routes_call_injected_seams_and_render_rows() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory.clone());
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    for (path, expected) in [
        ("/topics", "orders"),
        ("/groups", "consumer-a"),
        ("/acls", "User:alice"),
        ("/users", "scram-alice"),
        ("/quotas", "producer_byte_rate"),
        ("/log-dirs", "/var/lib/crabka"),
    ] {
        let response = get_from(app.clone(), path, cookie.clone()).await;

        assert_eq!(response.status(), StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert!(body.contains(expected), "{path} row content");
    }

    assert_eq!(factory.read_counts(), [1; 6]);
}

#[tokio::test]
async fn dynamic_read_routes_match_shared_page_renderer() {
    let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
    let session_id = sessions.create_user("alice", "User:alice");
    let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
    let factory = RecordingAdminSeamFactory::default();
    let app = router_with_factory(state, factory);
    let cookie = Some(format!(
        "{SESSION_COOKIE_NAME}={}",
        session_id.expose_for_cookie()
    ));

    let cases = [
        (
            "/topics",
            render_page(&RoutePage::topics(ReadRouteState::Rows(vec![
                sample_topic_row(),
            ]))),
        ),
        (
            "/groups",
            render_page(&RoutePage::groups(ReadRouteState::Rows(vec![GroupRow {
                group_id: "consumer-a".to_string(),
            }]))),
        ),
        (
            "/acls",
            render_page(&RoutePage::acls(ReadRouteState::Rows(vec![
                sample_acl_row(),
            ]))),
        ),
        (
            "/users",
            render_page(&RoutePage::users(ReadRouteState::Rows(vec![UserRow {
                username: "scram-alice".to_string(),
                principal: "User:scram-alice".to_string(),
            }]))),
        ),
        (
            "/quotas",
            render_page(&RoutePage::quotas(ReadRouteState::Rows(vec![QuotaRow {
                entity: "User:alice".to_string(),
                quota_type: "producer_byte_rate".to_string(),
                value: "1024".to_string(),
            }]))),
        ),
        (
            "/log-dirs",
            render_page(&RoutePage::log_dirs(ReadRouteState::Rows(vec![
                LogDirRow {
                    log_dir: "/var/lib/crabka".to_string(),
                    topic: "orders".to_string(),
                    partition: 0,
                    partition_size: 10,
                    offset_lag: 0,
                    is_future_key: false,
                    error: None,
                },
            ]))),
        ),
    ];

    for (path, expected_body) in cases {
        let body = response_text(get_from(app.clone(), path, cookie.clone()).await).await;

        assert_eq!(body, expected_body, "{path} shared renderer output");
    }
}

#[tokio::test]
async fn missing_or_invalid_cookie_does_not_call_injected_seams() {
    for cookie in [None, Some(format!("{SESSION_COOKIE_NAME}=not-a-session"))] {
        let sessions = Arc::new(SessionStore::new(Duration::from_mins(1)));
        let state = AppState::from_parts(Arc::new(AdminUiConfig::default()), sessions);
        let factory = RecordingAdminSeamFactory::default();
        let app = router_with_factory(state, factory.clone());

        for path in [
            "/topics",
            "/groups",
            "/log-dirs",
            "/acls",
            "/users",
            "/quotas",
        ] {
            let response = get_from(app.clone(), path, cookie.clone()).await;

            assert_eq!(response.status(), StatusCode::OK, "{path} status");
            let body = response_text(response).await;
            assert_eq!(body, render_page(&RoutePage::login()), "{path} login page");
        }

        assert_eq!(factory.read_counts_with_seam(), [0; 7]);
    }
}

#[derive(Clone, Default)]
struct RecordingLoginBroker {
    calls: Arc<AtomicUsize>,
}

impl LoginBroker for RecordingLoginBroker {
    fn check_login<'a>(
        &'a self,
        _cfg: &'a AdminUiConfig,
        username: &'a str,
        password: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), UiError>> + Send + 'a>> {
        Box::pin(async move {
            assert_eq!(
                (username, password),
                ("alice", "login-route-password-sentinel")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

#[derive(Clone, Default)]
struct RecordingAdminSeamFactory {
    read_seam_calls: Arc<AtomicUsize>,
    topics: Arc<AtomicUsize>,
    groups: Arc<AtomicUsize>,
    acls: Arc<AtomicUsize>,
    users: Arc<AtomicUsize>,
    quotas: Arc<AtomicUsize>,
    log_dirs: Arc<AtomicUsize>,
    mutation_seam_calls: Arc<AtomicUsize>,
    create_topic: Arc<AtomicUsize>,
    delete_topic: Arc<AtomicUsize>,
    create_partitions: Arc<AtomicUsize>,
    alter_configs: Arc<AtomicUsize>,
    create_acl: Arc<AtomicUsize>,
    delete_acl: Arc<AtomicUsize>,
    upsert_scram: Arc<AtomicUsize>,
    delete_scram: Arc<AtomicUsize>,
    upsert_quota: Arc<AtomicUsize>,
    delete_quota: Arc<AtomicUsize>,
    move_log_dir: Arc<AtomicUsize>,
}

impl RecordingAdminSeamFactory {
    fn read_counts(&self) -> [usize; 6] {
        [
            self.topics.load(Ordering::SeqCst),
            self.groups.load(Ordering::SeqCst),
            self.acls.load(Ordering::SeqCst),
            self.users.load(Ordering::SeqCst),
            self.quotas.load(Ordering::SeqCst),
            self.log_dirs.load(Ordering::SeqCst),
        ]
    }

    fn read_counts_with_seam(&self) -> [usize; 7] {
        let [topics, groups, acls, users, quotas, log_dirs] = self.read_counts();
        [
            self.read_seam_calls.load(Ordering::SeqCst),
            topics,
            groups,
            acls,
            users,
            quotas,
            log_dirs,
        ]
    }

    fn mutation_counts(&self) -> [usize; 12] {
        [
            self.mutation_seam_calls.load(Ordering::SeqCst),
            self.create_topic.load(Ordering::SeqCst),
            self.delete_topic.load(Ordering::SeqCst),
            self.create_partitions.load(Ordering::SeqCst),
            self.alter_configs.load(Ordering::SeqCst),
            self.create_acl.load(Ordering::SeqCst),
            self.delete_acl.load(Ordering::SeqCst),
            self.upsert_scram.load(Ordering::SeqCst),
            self.delete_scram.load(Ordering::SeqCst),
            self.upsert_quota.load(Ordering::SeqCst),
            self.delete_quota.load(Ordering::SeqCst),
            self.move_log_dir.load(Ordering::SeqCst),
        ]
    }

    fn total_mutation_calls(&self) -> usize {
        self.create_topic.load(Ordering::SeqCst)
            + self.delete_topic.load(Ordering::SeqCst)
            + self.create_partitions.load(Ordering::SeqCst)
            + self.alter_configs.load(Ordering::SeqCst)
            + self.create_acl.load(Ordering::SeqCst)
            + self.delete_acl.load(Ordering::SeqCst)
            + self.upsert_scram.load(Ordering::SeqCst)
            + self.delete_scram.load(Ordering::SeqCst)
            + self.upsert_quota.load(Ordering::SeqCst)
            + self.delete_quota.load(Ordering::SeqCst)
            + self.move_log_dir.load(Ordering::SeqCst)
    }
}

impl AdminSeamFactory for RecordingAdminSeamFactory {
    type Reader<'a> = Self;
    type Mutations<'a> = Self;

    fn read_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError> {
        assert_eq!(
            record.user,
            SessionUser {
                username: "alice".to_string(),
                principal: "User:alice".to_string(),
            }
        );
        self.read_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.clone())
    }

    fn mutation_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError> {
        assert_eq!(
            record.user,
            SessionUser {
                username: "alice".to_string(),
                principal: "User:alice".to_string(),
            }
        );
        self.mutation_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.clone())
    }
}

impl AdminReadSeam for RecordingAdminSeamFactory {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.topics.fetch_add(1, Ordering::SeqCst);
            Ok(vec![sample_topic_row()])
        })
    }

    fn groups<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<GroupRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.groups.fetch_add(1, Ordering::SeqCst);
            Ok(vec![GroupRow {
                group_id: "consumer-a".to_string(),
            }])
        })
    }

    fn acls<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AclRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.acls.fetch_add(1, Ordering::SeqCst);
            Ok(vec![sample_acl_row()])
        })
    }

    fn users<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<UserRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.users.fetch_add(1, Ordering::SeqCst);
            Ok(vec![UserRow {
                username: "scram-alice".to_string(),
                principal: "User:scram-alice".to_string(),
            }])
        })
    }

    fn quotas<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<QuotaRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.quotas.fetch_add(1, Ordering::SeqCst);
            Ok(vec![QuotaRow {
                entity: "User:alice".to_string(),
                quota_type: "producer_byte_rate".to_string(),
                value: "1024".to_string(),
            }])
        })
    }

    fn log_dirs<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogDirRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.log_dirs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![LogDirRow {
                log_dir: "/var/lib/crabka".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                partition_size: 10,
                offset_lag: 0,
                is_future_key: false,
                error: None,
            }])
        })
    }
}

impl AdminMutationSeam for RecordingAdminSeamFactory {
    fn create_topic<'a>(
        &'a self,
        request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn delete_topic<'a>(
        &'a self,
        request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_topic.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.name)])
        })
    }

    fn create_partitions<'a>(
        &'a self,
        request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_partitions.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }

    fn alter_configs<'a>(
        &'a self,
        request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.alter_configs.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.resource_name)])
        })
    }

    fn create_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.create_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn delete_acl<'a>(
        &'a self,
        request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_acl.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.principal)])
        })
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn delete_scram_user<'a>(
        &'a self,
        request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_scram.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.username)])
        })
    }

    fn upsert_quota<'a>(
        &'a self,
        request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.upsert_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn delete_quota<'a>(
        &'a self,
        request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.delete_quota.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.entity)])
        })
    }

    fn move_log_dir<'a>(
        &'a self,
        request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.move_log_dir.fetch_add(1, Ordering::SeqCst);
            Ok(vec![ResourceOutcome::ok(request.topic)])
        })
    }
}
