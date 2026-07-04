use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode, header};
use crabka_admin_ui::config::AdminUiConfig;
use crabka_admin_ui::server::{AppState, router};
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
