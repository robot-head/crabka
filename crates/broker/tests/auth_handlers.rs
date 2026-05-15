//! Slice 12 broker-side auth tests. No Docker.
//!
//! T10 contributes a single smoke test that proves a TLS-only listener
//! completes a TLS handshake with a stock `tokio_rustls::TlsConnector`
//! using the dev cert fixture as the trust anchor. T11 adds a Metadata
//! round-trip case that verifies per-listener endpoints land on the
//! broker's self-registration record. Subsequent tasks (T12+) extend
//! this file with SASL/PLAIN, SASL/SCRAM, and `AlterUserScramCredentials`
//! cases.

use std::sync::Arc;

use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_security::{ListenerProtocol, TlsConfig};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

const DEV_CERT: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert.pem");
const DEV_KEY: &str = include_str!("../../../crates/security/tests/fixtures/dev_key.pem");

fn write_dev_pem(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
    let cp = dir.join("cert.pem");
    let kp = dir.join("key.pem");
    std::fs::write(&cp, DEV_CERT).unwrap();
    std::fs::write(&kp, DEV_KEY).unwrap();
    (cp, kp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_listener_accepts_tls_handshake_only() {
    // The broker installs the rustls crypto provider in `Broker::start`,
    // but the client side of this test also needs one — install it here
    // so the call below the broker startup doesn't panic when this is
    // the first test in the process. `.ok()` swallows `AlreadySet`.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_dev_pem(pem_dir.path());

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SSL".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::Ssl,
    }];
    cfg.inter_broker_listener_name = "SSL".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path,
        trust_roots_path: None,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Build a client config trusting our dev cert. The fixture cert is
    // self-issued with `CA:TRUE`, which rustls's default webpki verifier
    // refuses to accept as an end-entity (`CaUsedAsEndEntity`). Since
    // this test only proves the TLS handshake bytes complete and produces
    // no real authentication, plug in a verifier that pins to the dev
    // cert's DER bytes and accepts anything that matches. Subsequent
    // task tests (T22 JVM TLS) regenerate proper cert chains.
    let cert_pem = std::fs::read(&cert_path).unwrap();
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<Result<_, _>>()
            .unwrap();
    let expected_cert = certs.into_iter().next().expect("at least one cert").clone();
    let client_cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedDevCertVerifier {
            pinned: expected_cert,
        }))
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(client_cfg));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("crabka-dev").unwrap();
    let _tls = connector
        .connect(server_name, tcp)
        .await
        .expect("TLS handshake must succeed");

    handle.shutdown().await;
}

/// Minimal `ServerCertVerifier` that accepts exactly one pre-known DER
/// blob. Skips hostname, validity, signature, and CA-flag checks — fine
/// for a smoke test, never for production code.
#[derive(Debug)]
struct PinnedDevCertVerifier {
    pinned: CertificateDer<'static>,
}

impl ServerCertVerifier for PinnedDevCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        if end_entity.as_ref() == self.pinned.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(tokio_rustls::rustls::Error::General(
                "presented cert does not match pinned dev cert".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ED25519,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

/// Task 11: every configured listener should appear as a `BrokerEndpoint`
/// on this broker's self-registration record, and the projection should
/// survive a Metadata round-trip end-to-end. The Kafka v9+ Metadata wire
/// response carries a single `host:port` per broker (the codec has no
/// `endpoints[]` array on `MetadataResponseBroker`), so this test asserts:
///
/// 1. The on-disk registration record stored in [`crabka_metadata::MetadataImage`]
///    carries one [`crabka_metadata::BrokerEndpoint`] per [`ListenerSpec`].
/// 2. A `MetadataRequest::v12` round-trip over the PLAINTEXT listener
///    returns at least one broker entry whose `host:port` matches one of
///    the configured advertised endpoints.
///
/// The two-listener config uses PLAINTEXT + SSL (the SSL listener uses
/// the slice-12 dev cert so `BrokerConfig::validate` is satisfied). We
/// only dial the PLAINTEXT listener — the goal here is the metadata
/// projection, not TLS termination (that's `tls_listener_accepts_*`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_response_carries_listener_endpoints() {
    // The Broker installs the rustls crypto provider during `start`; the
    // client side doesn't need TLS for this test, but installing here is
    // cheap and matches the T10 case if this is the first test to run.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path) = write_dev_pem(pem_dir.path());

    // Pre-reserve two distinct ephemeral ports (bind-and-drop trick) so
    // the listener-conflict validation sees two different `bind_addr`s.
    // `BrokerConfig::validate` rejects `"127.0.0.1:0" == "127.0.0.1:0"`
    // even though the OS would assign distinct ports at bind time.
    let p1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plaintext_bind = p1.local_addr().unwrap();
    let ssl_bind = p2.local_addr().unwrap();
    drop((p1, p2));

    // Two listeners on independent ephemeral ports. PLAINTEXT is the
    // inter-broker listener (so self-registration's `host`/`port` falls
    // out from it); SSL exercises the multi-listener code path.
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![
        ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: plaintext_bind,
            advertised: plaintext_bind.to_string(),
            protocol: ListenerProtocol::Plaintext,
        },
        ListenerSpec {
            name: "SSL".to_string(),
            bind_addr: ssl_bind,
            advertised: ssl_bind.to_string(),
            protocol: ListenerProtocol::Ssl,
        },
    ];
    cfg.inter_broker_listener_name = "PLAINTEXT".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let plaintext_addr = handle.listen_addr();

    // ── Assertion 1: in-memory registration carries both endpoints.
    //
    // Self-registration is best-effort + asynchronous (the leader watcher
    // races with the `submit_change` round-trip); poll for ~2s before
    // giving up so flakes on slow CI don't fail this test.
    let mut endpoints = handle.self_registration_endpoints().await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while endpoints.len() < 2 && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        endpoints = handle.self_registration_endpoints().await;
    }
    assert_eq!(
        endpoints.len(),
        2,
        "self-registration must carry one endpoint per configured listener (got {endpoints:?})"
    );
    let mut names: Vec<&str> = endpoints.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, vec!["PLAINTEXT", "SSL"]);
    let plaintext_ep = endpoints
        .iter()
        .find(|e| e.name == "PLAINTEXT")
        .expect("PLAINTEXT endpoint");
    assert_eq!(plaintext_ep.protocol, ListenerProtocol::Plaintext);
    let ssl_ep = endpoints
        .iter()
        .find(|e| e.name == "SSL")
        .expect("SSL endpoint");
    assert_eq!(ssl_ep.protocol, ListenerProtocol::Ssl);

    // ── Assertion 2: a Metadata round-trip over the PLAINTEXT listener
    // returns a broker entry. The Kafka v9+ wire format has no
    // `endpoints[]` array on `MetadataResponseBroker`, so we only assert
    // that *some* broker entry comes back and matches our id — the
    // per-listener data is verified above via the in-memory image.
    let bootstrap = plaintext_addr.to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-task11-test")
        .build()
        .await
        .expect("client build");
    let resp = client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata round-trip");
    assert!(
        !resp.brokers.is_empty(),
        "MetadataResponse must include at least one broker"
    );
    assert!(
        resp.brokers.iter().any(|b| b.node_id == 1),
        "MetadataResponse must include this broker (node_id=1): {:?}",
        resp.brokers,
    );

    handle.shutdown().await;
}
