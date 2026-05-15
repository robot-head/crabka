//! Slice 12 broker-side auth tests. No Docker.
//!
//! T10 contributes a single smoke test that proves a TLS-only listener
//! completes a TLS handshake with a stock `tokio_rustls::TlsConnector`
//! using the dev cert fixture as the trust anchor. Subsequent tasks
//! (T12+) extend this file with SASL/PLAIN, SASL/SCRAM, and
//! `AlterUserScramCredentials` cases.

use std::sync::Arc;

use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig};
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
