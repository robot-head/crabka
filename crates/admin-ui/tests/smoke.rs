use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use crabka_admin_ui::config::AdminUiConfig;
use crabka_admin_ui::dto::{
    AclRequestDto, AlterConfigRequestDto, CreatePartitionsRequestDto, CreateTopicRequestDto,
    DeleteTopicRequestDto, GroupRow, LogDirMoveRequestDto, LogDirRow, QuotaDeleteDto,
    QuotaUpsertDto, ResourceOutcome, ScramUserDeleteDto, ScramUserUpsertDto, TopicRow,
};
use crabka_admin_ui::error::UiError;
use crabka_admin_ui::server::{AppState, SESSION_COOKIE_NAME, router, router_with_factory};
use crabka_admin_ui::server_fns::{AdminMutationSeam, AdminReadSeam, AdminSeamFactory};
use crabka_admin_ui::session::{SessionRecord, SessionStore};
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

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), 64 * 1024)
        .await
        .expect("body can be collected");

    String::from_utf8(bytes.to_vec()).expect("HTML body is UTF-8")
}

#[tokio::test]
async fn healthz_returns_ok() {
    let response = get("/healthz").await;

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn root_returns_html() {
    let response = get("/").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&header::HeaderValue::from_static(
            "text/html; charset=utf-8"
        ))
    );

    let body = response_text(response).await;
    assert!(body.contains("<!doctype html>"));
    assert!(body.contains("Crabka Admin"));
}

#[tokio::test]
async fn login_returns_html_with_login_prompt() {
    let response = get("/login").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE),
        Some(&header::HeaderValue::from_static(
            "text/html; charset=utf-8"
        ))
    );

    let body = response_text(response).await;
    assert!(body.contains("Sign in to Crabka"));
}

#[tokio::test]
async fn linked_admin_pages_return_static_html() {
    for (path, heading, empty_state) in [
        ("/topics", "Topics", "Authentication required."),
        ("/groups", "Consumer Groups", "Authentication required."),
        ("/acls", "ACLs", "Authentication required."),
        ("/users", "SCRAM Users", "Authentication required."),
        ("/quotas", "Quotas", "Authentication required."),
        ("/log-dirs", "Log Dirs", "Authentication required."),
    ] {
        let response = get(path).await;

        assert_eq!(response.status(), StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert!(body.contains(heading), "{path} heading");
        assert!(body.contains(empty_state), "{path} empty state");
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
        ("/log-dirs", "/var/lib/crabka"),
    ] {
        let response = get_from(app.clone(), path, cookie.clone()).await;

        assert_eq!(response.status(), StatusCode::OK, "{path} status");
        let body = response_text(response).await;
        assert!(body.contains(expected), "{path} row content");
    }

    assert_eq!(factory.topics.load(Ordering::SeqCst), 1);
    assert_eq!(factory.groups.load(Ordering::SeqCst), 1);
    assert_eq!(factory.log_dirs.load(Ordering::SeqCst), 1);
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
            assert!(
                body.contains("Authentication required."),
                "{path} auth state"
            );
        }

        assert_eq!(factory.read_seam_calls.load(Ordering::SeqCst), 0);
        assert_eq!(factory.topics.load(Ordering::SeqCst), 0);
        assert_eq!(factory.groups.load(Ordering::SeqCst), 0);
        assert_eq!(factory.log_dirs.load(Ordering::SeqCst), 0);
    }
}

#[derive(Clone, Default)]
struct RecordingAdminSeamFactory {
    read_seam_calls: Arc<AtomicUsize>,
    topics: Arc<AtomicUsize>,
    groups: Arc<AtomicUsize>,
    log_dirs: Arc<AtomicUsize>,
}

impl AdminSeamFactory for RecordingAdminSeamFactory {
    type Reader<'a> = Self;
    type Mutations<'a> = Self;

    fn read_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        record: &SessionRecord,
    ) -> Result<Self::Reader<'a>, UiError> {
        assert_eq!(record.user.username, "alice");
        self.read_seam_calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.clone())
    }

    fn mutation_seam<'a>(
        &'a self,
        _cfg: &AdminUiConfig,
        _record: &SessionRecord,
    ) -> Result<Self::Mutations<'a>, UiError> {
        Ok(self.clone())
    }
}

impl AdminReadSeam for RecordingAdminSeamFactory {
    fn topics<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<TopicRow>, UiError>> + Send + 'a>> {
        Box::pin(async move {
            self.topics.fetch_add(1, Ordering::SeqCst);
            Ok(vec![TopicRow {
                name: "orders".to_string(),
                topic_id: None,
                partition_count: 3,
                replication_factor: 1,
                error: None,
            }])
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
        _request: CreateTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete_topic<'a>(
        &'a self,
        _request: DeleteTopicRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn create_partitions<'a>(
        &'a self,
        _request: CreatePartitionsRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn alter_configs<'a>(
        &'a self,
        _request: AlterConfigRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn create_acl<'a>(
        &'a self,
        _request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete_acl<'a>(
        &'a self,
        _request: AclRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn upsert_scram_sha512_user<'a>(
        &'a self,
        _request: ScramUserUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete_scram_user<'a>(
        &'a self,
        _request: ScramUserDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn upsert_quota<'a>(
        &'a self,
        _request: QuotaUpsertDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn delete_quota<'a>(
        &'a self,
        _request: QuotaDeleteDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn move_log_dir<'a>(
        &'a self,
        _request: LogDirMoveRequestDto,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResourceOutcome>, UiError>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}
