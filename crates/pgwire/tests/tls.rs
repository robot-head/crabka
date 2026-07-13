use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use crabka_pgwire::{session::SessionConfig, stub::StubEngine};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn frame_len(len: usize) -> i32 {
    i32::try_from(len).expect("test frame length fits in i32") + 4
}

fn server_tls() -> TlsAcceptor {
    let certs = CertificateDer::pem_slice_iter(include_bytes!("fixtures/test-server.pem"))
        .collect::<Result<Vec<_>, _>>()
        .expect("certs");
    let key = PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/test-server-key.pem"))
        .expect("read key");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("cert");
    TlsAcceptor::from(Arc::new(config))
}

fn client_tls() -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(include_bytes!("fixtures/test-ca.pem")) {
        roots.add(cert.expect("ca cert")).expect("add root");
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

#[tokio::test]
async fn ssl_request_upgrades_to_tls_and_session_works() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve_tls(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
        Some(server_tls()),
    ));

    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");

    // SSLRequest: length 8, code 80877103.
    let mut ssl_request = BytesMut::new();
    ssl_request.put_i32(8);
    ssl_request.put_i32(80_877_103);
    tcp.write_all(&ssl_request).await.expect("write");

    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await.expect("read");
    assert_eq!(answer[0], b'S', "server must accept TLS");

    let domain = rustls::pki_types::ServerName::try_from("localhost").expect("name");
    let mut tls = client_tls().connect(domain, tcp).await.expect("handshake");

    // StartupMessage over TLS: protocol 3.0, user/database params.
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.put_slice(b"user\0crab\0database\0crab\0\0");
    let mut startup = BytesMut::new();
    startup.put_i32(frame_len(body.len()));
    startup.put_slice(&body);
    tls.write_all(&startup).await.expect("startup");

    // Read until ReadyForQuery ('Z'); must see AuthenticationOk ('R') first.
    let mut seen = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = tls.read(&mut buf).await.expect("read");
        assert!(n > 0, "server closed before ReadyForQuery");
        seen.extend_from_slice(&buf[..n]);
        if seen.contains(&b'Z') && seen.first() == Some(&b'R') {
            break;
        }
    }
}

#[tokio::test]
async fn ssl_request_without_tls_config_gets_n() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve_tls(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
        None,
    ));

    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");
    let mut ssl_request = BytesMut::new();
    ssl_request.put_i32(8);
    ssl_request.put_i32(80_877_103);
    tcp.write_all(&ssl_request).await.expect("write");
    let mut answer = [0u8; 1];
    tcp.read_exact(&mut answer).await.expect("read");
    assert_eq!(answer[0], b'N');
}

#[tokio::test]
async fn pipelined_bytes_after_ssl_request_are_rejected() {
    // CVE-2021-23222 class: plaintext startup pipelined with SSLRequest must
    // NOT be processed as if it arrived over TLS.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();
    tokio::spawn(crabka_pgwire::server::serve_tls(
        listener,
        Arc::new(StubEngine::new()),
        Arc::new(SessionConfig::trust()),
        Some(server_tls()),
    ));

    let mut tcp = TcpStream::connect(("127.0.0.1", port)).await.expect("tcp");

    // SSLRequest immediately followed by a plaintext StartupMessage in one write.
    let mut evil = BytesMut::new();
    evil.put_i32(8);
    evil.put_i32(80_877_103);
    let mut body = BytesMut::new();
    body.put_i32(196_608);
    body.put_slice(b"user\0mallory\0\0");
    evil.put_i32(frame_len(body.len()));
    evil.put_slice(&body);
    tcp.write_all(&evil).await.expect("write");

    // Server must close without sending 'S' (or close immediately after);
    // it must NOT complete a TLS session that honors the injected startup.
    let mut answer = [0u8; 1];
    if tcp.read_exact(&mut answer).await.is_ok() {
        // If a byte arrived it must not be 'S'-then-working-session;
        // connection must be closed right after.
        // (With the fix the server returns Err BEFORE writing 'S', so the
        // error path above is the expected path.)
        let mut rest = [0u8; 16];
        let n = tokio::time::timeout(std::time::Duration::from_secs(2), tcp.read(&mut rest))
            .await
            .expect("server must close promptly")
            .expect("read");
        assert_eq!(n, 0, "server must close the connection, got more bytes");
    }
}
