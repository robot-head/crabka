use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt as _;

#[tokio::test]
async fn healthz_returns_ok() {
    let app = crabka_admin_ui::server::health_router();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("request builds"),
        )
        .await
        .expect("router responds");

    assert_eq!(response.status(), StatusCode::OK);
}
