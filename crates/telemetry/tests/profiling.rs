#[cfg(unix)]
mod unix_only {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt; // for `oneshot`

    #[tokio::test]
    async fn cpu_profile_endpoint_returns_pprof_bytes() {
        let app = crabka_telemetry::profiling::pprof_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/profile?seconds=1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        // A pprof CPU profile is a non-empty gzip/protobuf blob.
        assert!(!body.is_empty(), "expected a non-empty pprof profile");
    }
}
