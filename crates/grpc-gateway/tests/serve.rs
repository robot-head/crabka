//! Coverage for `serve::serve`, over the plaintext arm and the TLS
//! handshake-reject branch, with a minimal health-only app.

use std::time::Duration;

use crabka_grpc_gateway::{
    health::{self, Readiness},
    serve,
};
use crabka_units::prelude::*;
use tokio_util::sync::CancellationToken;

fn install_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Covers `serve()`'s `None` arm, which is plaintext. Bind a listener, serve
/// the health router over plain HTTP, verify that `/healthz` returns 200, then
/// shut down with the cancellation token.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plaintext_serve_serves_healthz() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let token = CancellationToken::new();
    let app = health::router(Readiness::new());
    let t = token.clone();
    let h = tokio::spawn(async move {
        let _ = serve::serve(listener, app, None, t).await;
    });

    let client = reqwest::Client::new();
    let mut ok = false;
    for _ in 0..50 {
        if let Ok(r) = client.get(format!("http://{addr}/healthz")).send().await
            && r.status() == reqwest::StatusCode::OK
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert2::assert!(ok);
    token.cancel();
    let _ = h.await;
}

/// Covers `serve_tls`'s handshake-failure branch. Start the server over TLS,
/// then send a PLAINTEXT http request to the TLS port. The client sends a raw
/// HTTP request and not a TLS `ClientHello`, so the rustls handshake fails on
/// the server side and the reqwest client errors out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_rejects_plaintext_connection() {
    use std::net::{IpAddr, Ipv4Addr};

    use crabka_grpc_gateway::config::{ClientAuthMode, TlsSettings};
    use crabka_security::ca::{SubjectAltName, generate_clients_ca, issue_broker_cert};

    install_provider();
    let dir = tempfile::TempDir::new().unwrap();
    let ca = generate_clients_ca("p4-serve-ca", 365).unwrap();
    let sans = vec![SubjectAltName::Ip(IpAddr::V4(Ipv4Addr::LOCALHOST))];
    let gw = issue_broker_cert(&ca.cert_pem, &ca.key_pem, "gw", &sans, &[], 365).unwrap();
    let cert = dir.path().join("c.pem");
    std::fs::write(&cert, &gw.cert_pem).unwrap();
    let key = dir.path().join("k.pem");
    std::fs::write(&key, &gw.key_pem).unwrap();

    let settings = TlsSettings {
        cert_chain_path: cert,
        private_key_path: key,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
        reload_interval: hours(1),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let token = CancellationToken::new();
    let dynamic =
        serve::build_and_watch_tls(settings.to_security(), hours(1), token.clone()).unwrap();
    let app = health::router(Readiness::new());
    let t = token.clone();
    let h = tokio::spawn(async move {
        let _ = serve::serve(listener, app, Some(dynamic), t).await;
    });
    // real-time wait (not a progress poll): waits on a real TLS handshake to reject a plaintext client; settle-then-assert-failure over a network round-trip.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Plain HTTP (no TLS) to the TLS port ⇒ rustls handshake fails server-side
    // ⇒ client errors (connection reset / EOF / protocol error).
    let res = reqwest::Client::new()
        .get(format!("http://{addr}/healthz"))
        .send()
        .await;
    assert2::assert!(res.is_err());
    token.cancel();
    let _ = h.await;
}
