// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files,
// matching the convention in auth_handlers.rs / elect_leaders.rs.

//! mTLS client authentication.
//!
//! Drives the full stack: rustls client cert handshake → broker
//! verifies the cert chain against `client_ca_path` → dispatch layer
//! extracts the cert's Subject DN as the connection's `Principal` →
//! authorizer reads that name when checking ACLs.
//!
//! The test exercises the principal-derivation path. It sets the
//! cert DN as a super-user and then sends a request that the
//! authorizer would refuse for any other principal. A successful round
//! trip proves that the broker resolved the connection to the cert DN
//! and not to `ANONYMOUS`.
//!
//! The test is gated to non-Windows. There is no multi-broker dependency, but
//! the dev cert fixture path resolution and tempfile semantics are easier to
//! keep consistent with the existing TLS integration tests.

use std::{io, sync::Arc};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    },
};
use crabka_security::{ClientAuthMode, ListenerProtocol, TlsConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, DigitallySignedStruct, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject},
    },
};

const DEV_CERT: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert.pem");
const DEV_KEY: &str = include_str!("../../../crates/security/tests/fixtures/dev_key.pem");
const DEV_CLIENT_CA: &str =
    include_str!("../../../crates/security/tests/fixtures/dev_client_ca.pem");
const DEV_CLIENT_CERT: &str =
    include_str!("../../../crates/security/tests/fixtures/dev_client_cert.pem");
const DEV_CLIENT_KEY: &str =
    include_str!("../../../crates/security/tests/fixtures/dev_client_key.pem");

/// Subject DN of the fixture client cert as rendered by `x509-parser`.
/// It must match `extract_principal_from_cert` exactly, because operators pin
/// this string in ACLs and `super_users`.
const CLIENT_PRINCIPAL: &str = "CN=test-client,OU=integration,O=crabka";

fn write_fixture(dir: &std::path::Path, name: &str, contents: &str) -> std::path::PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, contents).unwrap();
    p
}

/// Build a rustls `ClientConfig` that:
/// - skips server-cert verification (the broker presents the self-issued
///   `dev_cert` fixture, which rustls's default verifier rejects as
///   `CaUsedAsEndEntity`),
/// - presents the fixture client cert and private key on the
///   `CertificateRequest` callback.
fn client_config_with_pinned_server_and_client_cert(
    broker_cert: CertificateDer<'static>,
) -> Arc<ClientConfig> {
    let client_certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(DEV_CLIENT_CERT.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse client cert PEM");
    let client_key = PrivateKeyDer::from_pem_slice(DEV_CLIENT_KEY.as_bytes())
        .expect("parse client private key PEM");
    let cfg = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedServerVerifier {
            pinned: broker_cert,
        }))
        .with_client_auth_cert(client_certs, client_key)
        .expect("rustls accepts client cert + key");
    Arc::new(cfg)
}

/// Test-only `ServerCertVerifier` that pins a single DER blob. It skips the
/// hostname, validity, signature, and CA-flag checks. It mirrors the
/// helper in `tests/auth_handlers.rs`. The dev fixture is a self-issued
/// CA cert, which rustls does not accept as an end-entity by default.
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
                "presented server cert does not match pinned dev cert".into(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_principal_is_cert_dn_and_super_user_bypass_works() {
    // Provider registration is shared with auth_handlers.rs; tolerate
    // an earlier installer.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_dir = tempfile::tempdir().unwrap();
    let pem_dir = tempfile::tempdir().unwrap();
    let server_cert_path = write_fixture(pem_dir.path(), "server.pem", DEV_CERT);
    let server_key_path = write_fixture(pem_dir.path(), "server.key", DEV_KEY);
    let client_ca_path = write_fixture(pem_dir.path(), "client_ca.pem", DEV_CLIENT_CA);

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SSL".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::Ssl,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SSL".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: server_cert_path.clone(),
        private_key_path: server_key_path,
        trust_roots_path: None,
        client_ca_path: Some(client_ca_path),
        client_auth: ClientAuthMode::Required,
    });
    // The cert's Subject DN is the principal name. Set it as a
    // super-user so the authorizer permits CreateTopics; with no
    // super-users + no ACLs the compat shim would allow everything
    // regardless of principal, which would mask the principal-derivation
    // path under test.
    cfg.super_users = std::collections::HashSet::from([CLIENT_PRINCIPAL.to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Build the test TLS client: pin the broker's self-issued cert,
    // present the fixture client cert + key.
    let server_cert_der: CertificateDer<'static> =
        CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
            .next()
            .expect("dev server cert present")
            .expect("dev server cert parses")
            .clone();
    let client_cfg = client_config_with_pinned_server_and_client_cert(server_cert_der);
    let connector = TlsConnector::from(client_cfg);

    let tcp = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from("crabka-dev").unwrap();
    let mut tls = connector
        .connect(server_name, tcp)
        .await
        .expect("mTLS handshake must succeed");

    // Send CreateTopics. Authorize gate: Cluster Create on the
    // super-user path. Any non-super-user principal (including
    // ANONYMOUS, which is what a non-mTLS connection would see) would
    // get CLUSTER_AUTHORIZATION_FAILED.
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "mtls-smoke".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut tls, 19, 7, 1, true, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");

    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics must succeed for the cert-DN super-user — got {:?}",
        resp.topics[0]
    );

    handle.shutdown().await;
}

/// PLAINTEXT-style length-prefixed request/response over an arbitrary
/// `AsyncRead + AsyncWrite` stream. It mirrors the helper in
/// `auth_handlers.rs` and `elect_leaders.rs`.
async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "crabka-mtls-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    if flexible {
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}
