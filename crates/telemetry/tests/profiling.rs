#[cfg(unix)]
mod unix_only {
    use std::{io::Read as _, net::SocketAddr};

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
        assert!(body.starts_with(&[0x1f, 0x8b]), "expected gzip header");

        let mut decoder = flate2::read::GzDecoder::new(&body[..]);
        let mut raw = Vec::new();
        decoder
            .read_to_end(&mut raw)
            .expect("profile body should be valid gzip");
        assert!(!raw.is_empty(), "expected gzip to contain pprof bytes");
    }

    async fn unused_loopback_addr() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        addr
    }

    async fn http_get(addr: SocketAddr, path: &str) -> String {
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

        for _ in 0..50 {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(mut stream) => {
                    stream.write_all(request.as_bytes()).await.unwrap();
                    let mut response = Vec::new();
                    stream.read_to_end(&mut response).await.unwrap();
                    return String::from_utf8_lossy(&response).into_owned();
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
            }
        }

        panic!("admin server at {addr} did not accept a connection");
    }

    #[tokio::test]
    async fn serve_admin_binds_and_serves_extra_route() {
        let addr = unused_loopback_addr().await;
        crabka_telemetry::profiling::serve_admin(
            addr,
            Router::new().route("/ready", get(|| async { "ready" })),
        )
        .await
        .unwrap();

        let response = http_get(addr, "/ready").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\r\n\r\nready"), "{response}");
    }

    #[tokio::test]
    async fn serve_admin_from_env_uses_default_addr() {
        if std::env::var_os("CRABKA_ADMIN_LISTEN_ADDR").is_some() {
            return;
        }

        let addr = unused_loopback_addr().await;
        crabka_telemetry::profiling::serve_admin_from_env(&addr.to_string())
            .await
            .unwrap();

        let response = http_get(addr, "/__missing").await;
        assert!(response.starts_with("HTTP/1.1 404 Not Found"), "{response}");
    }

    #[tokio::test]
    async fn serve_admin_from_env_with_uses_default_addr_and_extra_routes() {
        if std::env::var_os("CRABKA_ADMIN_LISTEN_ADDR").is_some() {
            return;
        }

        let addr = unused_loopback_addr().await;
        crabka_telemetry::profiling::serve_admin_from_env_with(
            &addr.to_string(),
            Router::new().route("/ready-env", get(|| async { "ready-env" })),
        )
        .await
        .unwrap();

        let response = http_get(addr, "/ready-env").await;
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        assert!(response.contains("\r\n\r\nready-env"), "{response}");
    }
}
