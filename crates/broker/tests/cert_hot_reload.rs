// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files
// (same upstream bug as auth_handlers.rs / mtls.rs).
#![allow(clippy::pedantic)]

//! TLS hot-reload.
//!
//! Starts a broker with cert A, completes a handshake against it,
//! overwrites the cert files with cert B, triggers
//! [`crabka_broker::BrokerHandle::reload_tls`], and verifies the next
//! handshake serves cert B. The client pins exact DER blobs so the
//! assertion is "the server presented cert X" — there's no way to
//! satisfy a B-pinned verifier with the A cert (and vice-versa).

use std::{io, path::PathBuf, sync::Arc};

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use crabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig};
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, DigitallySignedStruct, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
    },
};

const DEV_CERT_A: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert.pem");
const DEV_KEY_A: &str = include_str!("../../../crates/security/tests/fixtures/dev_key.pem");
const DEV_CERT_B: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert_alt.pem");
const DEV_KEY_B: &str = include_str!("../../../crates/security/tests/fixtures/dev_key_alt.pem");

fn write(dir: &std::path::Path, name: &str, body: &str) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, body).unwrap();
    p
}

/// Pin a single end-entity cert by DER bytes. Mirrors the helpers in
/// `mtls.rs` / `auth_handlers.rs`. Skips hostname / CA / validity
/// checks — fine for fixture-pinned tests.
#[derive(Debug)]
struct PinnedServerVerifier {
    pinned: CertificateDer<'static>,
}

impl ServerCertVerifier for PinnedServerVerifier {
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
                "presented server cert does not match pinned cert".into(),
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
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
        ]
    }
}

fn pinned_client_config(pinned: CertificateDer<'static>) -> Arc<ClientConfig> {
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier { pinned }))
        .with_no_client_auth();
    Arc::new(cfg)
}

fn cert_der_from_pem(pem: &str) -> CertificateDer<'static> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .next()
        .expect("at least one cert in fixture")
        .expect("cert parses")
        .clone()
}

/// Try a TLS handshake against `addr`, verifying the server cert
/// against `pinned`. Returns `Ok(())` on a successful handshake, `Err`
/// otherwise. We only care that the negotiation completed — the
/// connection is dropped immediately after.
async fn handshake_against(
    addr: std::net::SocketAddr,
    pinned: CertificateDer<'static>,
) -> Result<(), io::Error> {
    let client_cfg = pinned_client_config(pinned);
    let connector = TlsConnector::from(client_cfg);
    let tcp = TcpStream::connect(addr).await?;
    let server_name = ServerName::try_from("crabka-dev").unwrap();
    connector
        .connect(server_name, tcp)
        .await
        .map(|_| ())
        .map_err(io::Error::other)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reload_tls_swaps_served_cert() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let cert_path = write(pem_dir.path(), "cert.pem", DEV_CERT_A);
    let key_path = write(pem_dir.path(), "key.pem", DEV_KEY_A);

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SSL".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Ssl,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SSL".into();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path.clone(),
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });
    // Disable the periodic watcher; this test drives reloads via the
    // explicit `BrokerHandle::reload_tls()` so it doesn't depend on
    // poll-tick timing.
    cfg.tls_reload_interval = std::time::Duration::ZERO;

    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    let cert_a = cert_der_from_pem(DEV_CERT_A);
    let cert_b = cert_der_from_pem(DEV_CERT_B);

    // Before reload: cert A is served. Cert-A verifier accepts;
    // cert-B verifier rejects.
    handshake_against(addr, cert_a.clone())
        .await
        .expect("pre-reload handshake against cert A must succeed");
    let pre_b = handshake_against(addr, cert_b.clone()).await;
    assert!(
        pre_b.is_err(),
        "pre-reload handshake against cert B must fail: got {pre_b:?}",
    );

    // Overwrite the cert files with cert B and trigger an immediate
    // reload. The periodic watcher is disabled, so without this call
    // the broker would keep serving cert A indefinitely.
    std::fs::write(&cert_path, DEV_CERT_B).unwrap();
    std::fs::write(&key_path, DEV_KEY_B).unwrap();
    handle.reload_tls().expect("reload_tls must succeed");

    // After reload: cert B is served. Now the verifiers flip.
    handshake_against(addr, cert_b.clone())
        .await
        .expect("post-reload handshake against cert B must succeed");
    let post_a = handshake_against(addr, cert_a).await;
    assert!(
        post_a.is_err(),
        "post-reload handshake against cert A must fail: got {post_a:?}",
    );

    handle.shutdown().await;
}

/// The periodic watcher reloads when files mtime-bump on disk —
/// proves the mtime-polling path (not just the explicit `reload_tls`
/// trigger). Uses a 100ms tick to keep the test fast.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_watcher_reloads_on_mtime_change() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let cert_path = write(pem_dir.path(), "cert.pem", DEV_CERT_A);
    let key_path = write(pem_dir.path(), "key.pem", DEV_KEY_A);

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SSL".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Ssl,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SSL".into();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path.clone(),
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: ClientAuthMode::Disabled,
    });
    cfg.tls_reload_interval = std::time::Duration::from_millis(100);

    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    let cert_b = cert_der_from_pem(DEV_CERT_B);

    // Sleep > 1s before rewriting so the new mtime is guaranteed to
    // differ from the original (mtime resolution on some filesystems
    // is 1s).
    // intentional: mtime-resolution deadline is the behavior under test; no broker
    // image/metric signal reflects on-disk mtime.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    std::fs::write(&cert_path, DEV_CERT_B).unwrap();
    std::fs::write(&key_path, DEV_KEY_B).unwrap();

    // Poll up to 5s for the watcher to pick up the change. Each
    // attempt is a fresh TCP+TLS handshake — keep going until cert B
    // is served.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if handshake_against(addr, cert_b.clone()).await.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "watcher never reloaded within 5s",
        );
        // intentional: the periodic mtime-polling reload is the behavior under test;
        // "cert B is served" is observable only via a live handshake, not any broker
        // metadata-image or metric, so keep the bounded handshake-retry poll.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    handle.shutdown().await;
}
