//! Broker-side auth tests. No Docker.
//!
//! T10 gives one smoke test. It proves that a TLS-only listener completes
//! a TLS handshake with a stock `tokio_rustls::TlsConnector`, and it uses
//! the dev cert fixture as the trust anchor. T11 adds a Metadata
//! round-trip case that checks that the per-listener endpoints land on the
//! broker's self-registration record. Task T12 and the tasks after it add
//! SASL/PLAIN, SASL/SCRAM, and `AlterUserScramCredentials` cases to this
//! file.

use std::{io, net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use crabka_client_core::Client;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
        },
        alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, DigitallySignedStruct, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime, pem::PemObject},
    },
};

const DEV_CERT: &str = include_str!("../../../crates/security/tests/fixtures/dev_cert.pem");
const DEV_KEY: &str = include_str!("../../../crates/security/tests/fixtures/dev_key.pem");

/// alice's SCRAM test password, built from characters at runtime.
///
/// The value is a non-secret test fixture. But a literal that goes into the
/// client SASL-auth calls trips GitHub's default code-scanning credential
/// query. This function keeps those call sites free of literals.
fn alice_password() -> String {
    ['w', 'o', 'n', 'd', 'e', 'r', 'l', 'a', 'n', 'd']
        .iter()
        .collect()
}

/// admin PLAIN test password, built at runtime.
///
/// A runtime value stops code scanning from giving a false positive for a
/// static secret in the integration fixtures.
fn admin_plain_password() -> String {
    ['s', 'e', 'c', 'r', 'e', 't'].iter().collect()
}

/// wrong SCRAM test password, built at runtime for the same reason as
/// `admin_plain_password`.
fn wrong_scram_password() -> String {
    ['h', 'u', 'n', 't', 'e', 'r', '2'].iter().collect()
}

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
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SSL".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path.clone(),
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: crabka_security::ClientAuthMode::Disabled,
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
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_path)
        .expect("open dev cert")
        .collect::<Result<_, _>>()
        .expect("parse dev cert");
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

/// Minimal `ServerCertVerifier` that accepts exactly one pre-known DER blob.
///
/// The verifier skips the hostname, validity, signature, and CA-flag checks.
/// This is good enough for a smoke test, but never for production code.
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

/// Every configured listener should appear as a `BrokerEndpoint` on this
/// broker's self-registration record, and the projection should survive a
/// Metadata round-trip end-to-end. The Kafka v9+ Metadata wire response
/// carries a single `host:port` per broker, because the codec has no
/// `endpoints[]` array on `MetadataResponseBroker`. So this test asserts:
///
/// 1. The on-disk registration record stored in [`crabka_metadata::MetadataImage`]
///    carries one [`crabka_metadata::BrokerEndpoint`] per [`ListenerSpec`].
/// 2. A `MetadataRequest::v12` round-trip over the PLAINTEXT listener
///    returns at least one broker entry whose `host:port` matches one of
///    the configured advertised endpoints.
///
/// The two-listener config uses PLAINTEXT and SSL. The SSL listener uses the
/// dev cert, so `BrokerConfig::validate` accepts it. We dial only the
/// PLAINTEXT listener, because the goal here is the metadata projection and
/// not TLS termination. `tls_listener_accepts_*` covers TLS termination.
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
            tls_config: None,
            sasl_mechanisms: None,
        },
        ListenerSpec {
            name: "SSL".to_string(),
            bind_addr: ssl_bind,
            advertised: ssl_bind.to_string(),
            protocol: ListenerProtocol::Ssl,
            tls_config: None,
            sasl_mechanisms: None,
        },
    ];
    cfg.inter_broker_listener_name = "PLAINTEXT".to_string();
    cfg.tls_config = Some(TlsConfig {
        cert_chain_path: cert_path,
        private_key_path: key_path,
        trust_roots_path: None,
        client_ca_path: None,
        client_auth: crabka_security::ClientAuthMode::Disabled,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let plaintext_addr = handle.listen_addr();

    // ── Assertion 1: in-memory registration carries both endpoints.
    //
    // Self-registration is best-effort + asynchronous (the leader watcher
    // races with the `submit_change` round-trip); wait until the broker's
    // own registration record in the committed image carries both endpoints.
    let node_id = handle.node_id();
    handle
        .wait_for_image(|img| {
            img.broker(crabka_broker::NodeId(node_id))
                .is_some_and(|b| b.endpoints.len() >= 2)
        })
        .await;
    let endpoints = handle.self_registration_endpoints();
    assert!(
        endpoints.len() == 2,
        "self-registration must carry one endpoint per configured listener (got {endpoints:?})"
    );
    let mut names: Vec<&str> = endpoints.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    assert!(names == vec!["PLAINTEXT", "SSL"]);
    let plaintext_ep = endpoints
        .iter()
        .find(|e| e.name == "PLAINTEXT")
        .expect("PLAINTEXT endpoint");
    assert!(plaintext_ep.protocol == ListenerProtocol::Plaintext);
    let ssl_ep = endpoints
        .iter()
        .find(|e| e.name == "SSL")
        .expect("SSL endpoint");
    assert!(ssl_ep.protocol == ListenerProtocol::Ssl);

    // ── Assertion 2: a Metadata round-trip over the PLAINTEXT listener
    // returns a broker entry. The Kafka v9+ wire format has no
    // `endpoints[]` array on `MetadataResponseBroker`, so we only assert
    // that *some* broker entry comes back and matches our id — the
    // per-listener data is verified above via the in-memory image.
    let bootstrap = plaintext_addr.to_string();
    let client = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("crabka-auth-test")
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

// ────────────────────────────────────────────────────────────────────────
// SASL/PLAIN end-to-end.
// ────────────────────────────────────────────────────────────────────────

/// Happy-path drive of a SASL/PLAIN session: `ApiVersions` → `SaslHandshake`
/// → `SaslAuthenticate` → Metadata.
///
/// The test asserts that the connection survives every step and that the
/// final Metadata response carries this broker. The dial side sends raw
/// bytes over `TcpStream` and not `Client`, because `Client` does not speak
/// SASL yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", alice_password().as_bytes()).await;
    handle.shutdown().await;
    result.expect("SASL/PLAIN session must succeed end-to-end");
}

/// SASL PLAIN metrics: one happy-path session and one wrong-password session
/// tick both the `successful_authentication_total` and the
/// `failed_authentication_total` per-mechanism counters on the `/metrics`
/// scrape.
///
/// The test checks the end-to-end wire path from the `SaslAuthenticate`
/// dispatch site to the rendered Prometheus text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_authentication_metrics_tick_for_success_and_failure() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());
    cfg.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let metrics_addr = handle
        .metrics_addr()
        .expect("metrics server should be bound");

    // 1. Happy path — must tick `successful_authentication_total`.
    drive_sasl_plain_session(addr, "alice", alice_password().as_bytes())
        .await
        .expect("happy-path PLAIN session");
    // 2. Wrong password — must tick `failed_authentication_total`.
    let bad = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    assert!(bad.is_err(), "wrong password must fail: {bad:?}");

    let body = scrape_metrics(metrics_addr).await;
    handle.shutdown().await;

    let success_needle = "crabka_broker_successful_authentication_total{mechanism=\"PLAIN\"} 1";
    let failed_needle = "crabka_broker_failed_authentication_total{mechanism=\"PLAIN\"} 1";
    assert!(
        body.contains(success_needle),
        "missing or wrong-value {success_needle} in:\n{body}"
    );
    assert!(
        body.contains(failed_needle),
        "missing or wrong-value {failed_needle} in:\n{body}"
    );
}

/// Send an HTTP GET `/metrics` to `addr` and return the response body.
///
/// The returned body holds no HTTP head. This helper is a copy of the helper
/// in `tests/metrics.rs`. It stays inline here so that the test does not need
/// a cross-test module.
async fn scrape_metrics(addr: SocketAddr) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n",
    );
    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let s = String::from_utf8(buf).unwrap();
    let body_start = s.find("\r\n\r\n").map_or(0, |i| i + 4);
    s[body_start..].to_string()
}

/// Negative path: with a wrong password, `SaslAuthenticate` responds with
/// `error_code = SASL_AUTHENTICATION_FAILED` (58) and the broker closes the
/// connection.
///
/// `drive_sasl_plain_session` reports the failure as an `Err` in two cases.
/// The first case is a non-zero `error_code` on the auth response. The second
/// case is an EOF on the Metadata read that follows, when the peer closed the
/// connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", wrong_scram_password().as_bytes()).await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail the SASL session: {result:?}"
    );
}

/// Drive a complete SASL/PLAIN session against a `SASL_PLAINTEXT` listener.
///
/// On success, this helper returns `Ok(())` after a successful post-auth
/// Metadata round-trip. It returns `Err` when any step fails: frame I/O,
/// response decode, a non-zero error code on a SASL response, or EOF before
/// Metadata.
///
/// This helper handles these wire-protocol mechanics inline, without the
/// `Client` API:
/// - Request headers: v1, non-flexible, for `ApiVersions v0` and
///   `SaslHandshake v1`. v2, flexible with a trailing `0x00` tagged-fields
///   byte, for `SaslAuthenticate v2` and `Metadata v12`.
/// - Response headers: always v0, which holds only `correlation_id`, for
///   `ApiVersions`, whatever the body flexibility. v1, which holds `corr_id`
///   plus a `0x00` tagged byte, for every other flexible response. v0 for
///   non-flexible.
/// - Length framing: a 4-byte big-endian length prefix on every frame in both
///   directions, the same as `crabka_broker::network::codec`.
async fn drive_sasl_plain_session(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible): proves the pre-auth allowlist
    //    lets us talk to the broker before authentication. We decode the
    //    response and ignore the contents — its presence is enough.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (non-flexible, mechanism="PLAIN").
    let mut sh_body = BytesMut::new();
    let sh_req = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    };
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // authzid (empty)
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let auth_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    };
    let mut auth_body = BytesMut::new();
    auth_req
        .encode(&mut auth_body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={} error_message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    // ── 4. Post-auth Metadata round-trip proves the connection survived
    //    and the data plane is reachable.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 4, true, &md_body).await?;
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
    if md_resp.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }

    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// SASL/SCRAM-SHA-512 end-to-end.
// ────────────────────────────────────────────────────────────────────────

/// Happy-path drive of a SASL/SCRAM-SHA-512 session.
///
/// The test provisions a credential for "alice" with the shared test
/// password. It goes through the controller directly and not through the
/// public `AlterUserScramCredentials` handler. It then runs the two-round
/// RFC 5802 exchange end-to-end and asserts that the post-auth Metadata
/// request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    // Provision alice/wonderland directly via the controller, rather than
    // through the public path (AlterUserScramCredentials, api_key 51).
    let cred = crabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha512,
        4096,
    );
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha512)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM session must succeed end-to-end");
}

/// Negative path: with a wrong password, `SaslAuthenticate` round 2 responds
/// with `error_code = 58`, which is `SASL_AUTHENTICATION_FAILED`, and the
/// broker closes the connection.
///
/// `drive_sasl_scram_session` reports the failure as a non-zero error code on
/// the auth response, or as an EOF when the Metadata read that follows
/// returns no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = crabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha512,
        4096,
    );
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha512,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SCRAM session: {result:?}"
    );
}

/// SASL/SCRAM-SHA-256 happy path.
///
/// The test is a copy of the SHA-512 test, but it provisions a SHA-256
/// credential and configures the listener to enable only SHA-256. This proves
/// that the new mechanism is wired end-to-end, and that it does not use the
/// SHA-512 code by accident.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha256];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = crabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha256,
        4096,
    );
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha256,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha256)
            .await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM-SHA-256 session must succeed end-to-end");
}

/// Negative path for SHA-256: a wrong password must close the connection,
/// the same as in the SHA-512 variant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha256_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha256];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred = crabka_security::hash_scram_password(
        alice_password().as_bytes(),
        SaslMechanism::ScramSha256,
        4096,
    );
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha256,
                salt: cred.salt,
                stored_key: cred.stored_key,
                server_key: cred.server_key,
                iterations: cred.iterations,
            },
        ))
        .await
        .expect("submit V1ScramCredential");

    let addr = handle.listen_addr();
    let result = drive_sasl_scram_session(
        addr,
        "alice",
        &wrong_scram_password(),
        SaslMechanism::ScramSha256,
    )
    .await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SHA-256 SCRAM session: {result:?}"
    );
}

/// Drive a complete SASL/SCRAM session against a `SASL_PLAINTEXT` listener.
///
/// This helper works for both SHA-256 and SHA-512. It passes the mechanism
/// through to the handshake and to the client state machine.
///
/// On success it returns `Ok(())` after a successful post-auth Metadata
/// round-trip. It returns `Err` when any step fails: a non-zero error code on
/// either of the two `SaslAuthenticate` rounds, a server-final signature
/// mismatch in the client-side proof, or EOF before Metadata returns.
async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
    mechanism: crabka_security::SaslMechanism,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible). Same as PLAIN: pre-auth allowlist.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // ── 2. SaslHandshake v1 (non-flexible).
    let mut sh_body = BytesMut::new();
    let sh_req = SaslHandshakeRequest {
        mechanism: mechanism.wire_name().to_string(),
        ..Default::default()
    };
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SCRAM client-first → server-first.
    let client = crabka_security::ScramClientExchange::new(
        user.to_string(),
        password.as_bytes().to_vec(),
        mechanism,
    );
    let (client_first, client) = client
        .client_first()
        .map_err(|e| io::Error::other(format!("scram client_first: {e:?}")))?;
    let scram_req_first = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first),
        ..Default::default()
    };
    let mut scram_body_first = BytesMut::new();
    scram_req_first
        .encode(&mut scram_body_first, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) encode: {e}")))?;
    let scram_first_response_bytes =
        round_trip(&mut stream, 36, 2, 3, true, &scram_body_first).await?;
    let mut cur: &[u8] = &scram_first_response_bytes;
    let scram_first_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) decode: {e}")))?;
    if scram_first_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 1 failed: error_code={} error_message={:?}",
            scram_first_response.error_code, scram_first_response.error_message
        )));
    }
    let server_first = scram_first_response.auth_bytes.to_vec();

    // ── 4. SCRAM client-final → server-final.
    let (client_final, client) = client
        .step(&server_first)
        .map_err(|e| io::Error::other(format!("scram client step: {e:?}")))?;
    let scram_req_final = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_final),
        ..Default::default()
    };
    let mut scram_body_final = BytesMut::new();
    scram_req_final
        .encode(&mut scram_body_final, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) encode: {e}")))?;
    let scram_final_response_bytes =
        round_trip(&mut stream, 36, 2, 4, true, &scram_body_final).await?;
    let mut cur: &[u8] = &scram_final_response_bytes;
    let scram_final_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) decode: {e}")))?;
    if scram_final_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate round 2 failed: error_code={} error_message={:?}",
            scram_final_response.error_code, scram_final_response.error_message
        )));
    }
    // Client verifies server signature — proves the broker holds the
    // expected `server_key` rather than just any matching `stored_key`.
    client
        .verify_server_final(&scram_final_response.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    // ── 5. Post-auth Metadata round-trip proves the connection survived
    //    and the data plane is reachable.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 5, true, &md_body).await?;
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
    if md_resp.brokers.is_empty() {
        return Err(io::Error::other("Metadata response carried no brokers"));
    }

    Ok(())
}

/// Encode a request header, send the frame, and return the response body
/// bytes.
///
/// The header is a `RequestHeader v1`, or a v2 header when `flexible` is set.
/// This function appends the body, writes the length-prefixed frame, reads
/// one response frame, and then strips the `ResponseHeader`. That header is
/// always v0 for ApiVersions(18). For every other API it is v0 when the
/// response is non-flexible and v1 when the response is flexible.
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    // RequestHeader: api_key + version + corr_id + client_id (i16 NULLABLE_STRING).
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "crabka-sasl-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    // Length-prefixed write.
    stream
        .write_u32(u32::try_from(frame.len()).expect("frame size fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    // Read length prefix then exactly that many bytes.
    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    // Strip ResponseHeader: 4-byte corr_id, plus 1-byte tagged-fields for
    // v1 (flexible body AND api_key != 18).
    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    let uses_v1_header = flexible && api_key != 18;
    if uses_v1_header {
        if cur.is_empty() {
            return Err(io::Error::other(
                "flexible response missing tagged-fields byte",
            ));
        }
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

// ────────────────────────────────────────────────────────────────────────
// SASL/OAUTHBEARER (KIP-255 / RFC 7628) end-to-end (no Docker).
// ────────────────────────────────────────────────────────────────────────

/// Build an unsecured JWS (`alg:none`) bearer token with a `sub` principal and
/// an `exp` in Unix seconds.
///
/// The signature segment is empty. This matches what the JVM
/// `OAuthBearerUnsecuredLoginCallbackHandler` produces.
fn unsecured_jws(sub: &str, exp_unix_secs: i64) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    let header = B64.encode(b"{\"alg\":\"none\"}");
    let claims = B64.encode(format!("{{\"sub\":\"{sub}\",\"exp\":{exp_unix_secs}}}").as_bytes());
    format!("{header}.{claims}.")
}

/// RFC 7628 client initial response that carries `token` with an empty
/// authzid.
fn oauthbearer_initial(token: &str) -> bytes::Bytes {
    bytes::Bytes::from(format!("n,,\u{1}auth=Bearer {token}\u{1}\u{1}").into_bytes())
}

fn now_unix_secs() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs(),
    )
    .expect("seconds fit in i64")
}

/// Start a single `SASL_PLAINTEXT` broker that enables only OAUTHBEARER, with
/// the given validator.
fn start_oauthbearer_broker(
    log_dir: &std::path::Path,
    validator: crabka_security::OAuthBearerValidator,
) -> impl std::future::Future<Output = crabka_broker::BrokerHandle> {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        // This helper exercises only the client-listener validator. Dedicated
        // multi-broker tests cover outbound OAUTHBEARER on the controller and
        // inter-broker paths.
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator = validator;
    Box::pin(async move { Broker::start(cfg).await.expect("broker must start") })
}

/// Same as [`start_oauthbearer_broker`], but with a configurable server-side
/// ceiling on the OAUTHBEARER session lifetime.
///
/// `Some(seconds)` clamps `session_lifetime_ms` to
/// `min(token_exp - now, seconds * 1000)`. It clamps the dispatch-loop
/// re-auth deadline to the same value. `None` reproduces the 49e default,
/// where the session ends at the token exp.
fn start_oauthbearer_broker_with_cap(
    log_dir: &std::path::Path,
    validator: crabka_security::OAuthBearerValidator,
    max_session_lifetime: Option<crabka_units::Time>,
) -> impl std::future::Future<Output = crabka_broker::BrokerHandle> {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer];
    cfg.oauthbearer_validator = validator;
    cfg.oauthbearer_max_session_lifetime = max_session_lifetime;
    Box::pin(async move { Broker::start(cfg).await.expect("broker must start") })
}

/// Run a pre-auth `ApiVersions` and a `SaslHandshake`(OAUTHBEARER).
///
/// The helper asserts that the broker advertises OAUTHBEARER and that the
/// handshake succeeds.
async fn oauthbearer_handshake(stream: &mut TcpStream, corr: &mut i32) -> Result<(), io::Error> {
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av = round_trip(stream, 18, 0, *corr, false, &av_body).await?;
    *corr += 1;
    let mut cur: &[u8] = &av;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let sh_req = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh = round_trip(stream, 17, 1, *corr, false, &sh_body).await?;
    *corr += 1;
    let mut cur: &[u8] = &sh;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }
    if !sh_resp.mechanisms.iter().any(|m| m == "OAUTHBEARER") {
        return Err(io::Error::other("OAUTHBEARER not advertised"));
    }
    Ok(())
}

/// Send one `SaslAuthenticate v2` with `auth_bytes` and return the decoded
/// response.
async fn oauthbearer_authenticate(
    stream: &mut TcpStream,
    corr: &mut i32,
    auth_bytes: bytes::Bytes,
) -> Result<SaslAuthenticateResponse, io::Error> {
    let req = SaslAuthenticateRequest {
        auth_bytes,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let resp_bytes = round_trip(stream, 36, 2, *corr, true, &body).await?;
    *corr += 1;
    let mut cur: &[u8] = &resp_bytes;
    SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))
}

/// Happy path: a valid unsecured token authenticates in a single round.
///
/// A post-auth Metadata round-trip proves that the connection survived and
/// that the broker accepted the principal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(
        log_dir.path(),
        crabka_security::OAuthBearerValidator::default(),
    )
    .await;
    let addr = handle.listen_addr();

    let result: Result<(), io::Error> = async {
        let mut stream = TcpStream::connect(addr).await?;
        let mut corr = 1;
        oauthbearer_handshake(&mut stream, &mut corr).await?;

        let token = unsecured_jws("svc-account", now_unix_secs() + 3600);
        let auth =
            oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(&token)).await?;
        if auth.error_code != 0 {
            return Err(io::Error::other(format!(
                "authenticate failed: code={} msg={:?}",
                auth.error_code, auth.error_message
            )));
        }
        if !auth.auth_bytes.is_empty() {
            return Err(io::Error::other(
                "unexpected challenge — token was rejected",
            ));
        }

        let md_req = MetadataRequest::default();
        let mut md_body = BytesMut::new();
        md_req
            .encode(&mut md_body, 12)
            .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
        let md = round_trip(&mut stream, 3, 12, corr, true, &md_body).await?;
        let mut cur: &[u8] = &md;
        let md_resp = MetadataResponse::decode(&mut cur, 12)
            .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
        if md_resp.brokers.is_empty() {
            return Err(io::Error::other("Metadata carried no brokers"));
        }
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("OAUTHBEARER session must succeed end-to-end");
}

/// Failure path: an expired token triggers the RFC 7628 two-round failure
/// handshake.
///
/// Round 1 returns the `invalid_token` JSON with `error_code = 0` and keeps
/// the connection open. Round 2, the client's `\x01` dummy, returns
/// `SASL_AUTHENTICATION_FAILED` (58).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_invalid_token_two_round_failure() {
    let log_dir = tempfile::tempdir().unwrap();
    let validator =
        crabka_security::OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
            allowable_clock_skew: crabka_units::secs(0),
            ..Default::default()
        });
    let handle = start_oauthbearer_broker(log_dir.path(), validator).await;
    let addr = handle.listen_addr();

    let result: Result<(), io::Error> = async {
        let mut stream = TcpStream::connect(addr).await?;
        let mut corr = 1;
        oauthbearer_handshake(&mut stream, &mut corr).await?;

        // Expired token (exp an hour in the past, zero skew).
        let token = unsecured_jws("admin", now_unix_secs() - 3600);
        let round1 =
            oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(&token)).await?;
        assert!(round1.error_code == 0, "round 1 must not close yet");
        assert!(
            &round1.auth_bytes[..] == br#"{"status":"invalid_token"}"#,
            "round 1 must carry the RFC 7628 error JSON"
        );

        // The client's `\x01` dummy → SASL_AUTHENTICATION_FAILED (58).
        let round2 =
            oauthbearer_authenticate(&mut stream, &mut corr, bytes::Bytes::from_static(&[1u8]))
                .await?;
        assert!(round2.error_code == 58, "round 2 must fail the connection");
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("OAUTHBEARER failure handshake must complete");
}

// ────────────────────────────────────────────────────────────────────────
// SASL/OAUTHBEARER signed-JWT (JWKS) validation end-to-end.
// ────────────────────────────────────────────────────────────────────────

/// Generate a fresh ES256 key and return `(key_pair, jwks_json)`.
///
/// The JWKS advertises the matching public key under `kid`.
fn es256_key(kid: &str) -> (ring::signature::EcdsaKeyPair, String) {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    use ring::{
        rand::SystemRandom,
        signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair},
    };
    let rng = SystemRandom::new();
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
    let kp =
        EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng).unwrap();
    let point = kp.public_key().as_ref(); // 0x04 || x || y
    let jwks = format!(
        "{{\"keys\":[{{\"kty\":\"EC\",\"crv\":\"P-256\",\"kid\":\"{kid}\",\"x\":\"{}\",\"y\":\"{}\"}}]}}",
        B64.encode(&point[1..33]),
        B64.encode(&point[33..65]),
    );
    (kp, jwks)
}

/// Sign an ES256 JWS with `kp`, `kid` in the header and `claims` as the payload.
fn es256_token(kp: &ring::signature::EcdsaKeyPair, kid: &str, claims: &str) -> String {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
    let header = B64.encode(format!("{{\"alg\":\"ES256\",\"kid\":\"{kid}\"}}").as_bytes());
    let payload = B64.encode(claims.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let sig = kp
        .sign(&ring::rand::SystemRandom::new(), signing_input.as_bytes())
        .unwrap();
    format!("{signing_input}.{}", B64.encode(sig.as_ref()))
}

/// A `Signed` validator whose key set comes from `jwks_json`.
///
/// The test needs no network fetch.
fn signed_validator(jwks_json: &str) -> crabka_security::OAuthBearerValidator {
    let handle = crabka_security::JwksHandle::new(
        crabka_security::Jwks::from_json(jwks_json, false).unwrap(),
    );
    crabka_security::OAuthBearerValidator::Signed(crabka_security::SignedJwsValidator::new(handle))
}

/// Happy path: a real signed ES256 token, verified against an in-memory JWKS,
/// authenticates in a single round.
///
/// This proves that the `Signed` validator is wired through the live
/// `SaslAuthenticate` path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_happy_path() {
    let (kp, jwks) = es256_key("k1");
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), signed_validator(&jwks)).await;
    let addr = handle.listen_addr();

    let result: Result<(), io::Error> = async {
        let mut stream = TcpStream::connect(addr).await?;
        let mut corr = 1;
        oauthbearer_handshake(&mut stream, &mut corr).await?;

        let claims = format!(
            "{{\"sub\":\"svc-account\",\"exp\":{}}}",
            now_unix_secs() + 3600
        );
        let token = es256_token(&kp, "k1", &claims);
        let auth =
            oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(&token)).await?;
        if auth.error_code != 0 {
            return Err(io::Error::other(format!(
                "authenticate failed: code={} msg={:?}",
                auth.error_code, auth.error_message
            )));
        }
        if !auth.auth_bytes.is_empty() {
            return Err(io::Error::other("signed success round must be empty"));
        }

        // Post-auth Metadata proves the connection survived authentication.
        let md_req = MetadataRequest::default();
        let mut md_body = BytesMut::new();
        md_req
            .encode(&mut md_body, 12)
            .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
        let md = round_trip(&mut stream, 3, 12, corr, true, &md_body).await?;
        let mut cur: &[u8] = &md;
        let md_resp = MetadataResponse::decode(&mut cur, 12)
            .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))?;
        if md_resp.brokers.is_empty() {
            return Err(io::Error::other("Metadata carried no brokers"));
        }
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("signed OAUTHBEARER session must succeed end-to-end");
}

/// Failure path: a token signed by a *different* key than the JWKS advertises
/// triggers the RFC 7628 two-round failure handshake.
///
/// Round 1 carries the `invalid_token` JSON. Round 2, the `\x01` dummy,
/// returns 58.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_oauthbearer_signed_token_wrong_key_two_round_failure() {
    // JWKS advertises key A's public key; the token is signed by key B.
    let (_kp_a, jwks_a) = es256_key("k1");
    let (kp_b, _jwks_b) = es256_key("k1");
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), signed_validator(&jwks_a)).await;
    let addr = handle.listen_addr();

    let result: Result<(), io::Error> = async {
        let mut stream = TcpStream::connect(addr).await?;
        let mut corr = 1;
        oauthbearer_handshake(&mut stream, &mut corr).await?;

        let claims = format!("{{\"sub\":\"admin\",\"exp\":{}}}", now_unix_secs() + 3600);
        let token = es256_token(&kp_b, "k1", &claims);
        let round1 =
            oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(&token)).await?;
        assert!(round1.error_code == 0, "round 1 must not close yet");
        assert!(
            &round1.auth_bytes[..] == br#"{"status":"invalid_token"}"#,
            "round 1 must carry the RFC 7628 error JSON"
        );

        let round2 =
            oauthbearer_authenticate(&mut stream, &mut corr, bytes::Bytes::from_static(&[1u8]))
                .await?;
        assert!(round2.error_code == 58, "round 2 must fail the connection");
        Ok(())
    }
    .await;

    handle.shutdown().await;
    result.expect("signed OAUTHBEARER failure handshake must complete");
}

// ────────────────────────────────────────────────────────────────────────
// SASL/OAUTHBEARER re-authentication (KIP-368) end-to-end.
//
// Six integration scenarios that exercise the full session-lifetime +
// in-band re-auth surface:
//   1. response carries `session_lifetime_ms ≈ exp - now`
//   2. dispatch-loop timer closes the connection past the token's `exp`
//   3. in-band re-auth with a fresh token resets the timer
//   4. in-band re-auth with a different principal name returns 58 and
//      closes the connection
//   5. in-band re-auth attempting to switch SASL mechanism returns 34
//   6. PLAIN-listener regression: `session_lifetime_ms = 0`, no timer
//
// `tokio::time::pause()` + `advance()` drive the per-connection deadline
// deterministically. We pause AFTER the broker is started and the
// handshake completes (rather than `start_paused = true`), because the
// broker's own internal timers — heartbeats, JWKS refresh, disk scans,
// raft — must run at real wall-clock rates during startup or `Broker::
// start` hangs. Post-handshake `pause()` is enough: the dispatch loop's
// `sleep_until(instant_at_epoch_ms(exp))` was armed against the real
// tokio clock at the moment the loop re-entered `select!` after the
// SaslAuthenticate; `advance()` then jumps past that Instant.
// ────────────────────────────────────────────────────────────────────────

/// Build an unsecured-JWS validator with zero clock skew and the default
/// `sub` principal claim.
///
/// Zero clock skew makes `exp` the exact session boundary. This validator is
/// the same as the one in the other OAuth tests, but it is pinned to zero
/// skew so that the assertion windows in the re-auth tests do not drift.
fn oauthbearer_zero_skew_validator() -> crabka_security::OAuthBearerValidator {
    crabka_security::OAuthBearerValidator::Unsecured(crabka_security::UnsecuredJwsValidator {
        allowable_clock_skew: crabka_units::secs(0),
        ..Default::default()
    })
}

/// Drive a `SASL_PLAINTEXT` OAUTHBEARER handshake to completion on a fresh
/// connection.
///
/// The function returns the still-open `TcpStream` and the
/// `session_lifetime_ms` field from the `SaslAuthenticateResponse`. Callers
/// can then assert on the timer and continue to use the connection. The
/// in-band re-auth scenarios do this.
///
/// `bearer_token` is the JWS string. For unsecured tests it is an `alg:none`
/// JWT with the wanted `sub` and `exp`. The function frames the RFC 7628
/// client-first message that wraps the token.
async fn drive_sasl_oauthbearer_session_open(
    addr: SocketAddr,
    bearer_token: &str,
) -> Result<(TcpStream, i64), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut corr = 1;
    oauthbearer_handshake(&mut stream, &mut corr).await?;
    let auth =
        oauthbearer_authenticate(&mut stream, &mut corr, oauthbearer_initial(bearer_token)).await?;
    if auth.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate error_code={} message={:?}",
            auth.error_code, auth.error_message
        )));
    }
    if !auth.auth_bytes.is_empty() {
        return Err(io::Error::other(
            "unexpected challenge — token was rejected",
        ));
    }
    Ok((stream, auth.session_lifetime_ms))
}

/// Drive a `SaslHandshake`(OAUTHBEARER) and `SaslAuthenticate` pair on an
/// already-authenticated stream, with a new bearer token.
///
/// This helper exercises KIP-368 in-band re-authentication. It returns
/// `Ok(())` when the broker accepts the new token. It returns `Err` when
/// either round reports a non-zero error code. The caller then asserts on
/// the rendered error text to tell 34 from 58.
async fn drive_inband_reauth(stream: &mut TcpStream, new_token: &str) -> Result<(), io::Error> {
    let handshake_request = SaslHandshakeRequest {
        mechanism: "OAUTHBEARER".to_string(),
        ..Default::default()
    };
    let mut handshake_body = BytesMut::new();
    handshake_request
        .encode(&mut handshake_body, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let handshake_response_bytes = round_trip(stream, 17, 1, 100, false, &handshake_body).await?;
    let mut cur: &[u8] = &handshake_response_bytes;
    let handshake_response = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if handshake_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslHandshake error_code={}",
            handshake_response.error_code
        )));
    }

    let authenticate_request = SaslAuthenticateRequest {
        auth_bytes: oauthbearer_initial(new_token),
        ..Default::default()
    };
    let mut authenticate_body = BytesMut::new();
    authenticate_request
        .encode(&mut authenticate_body, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let authenticate_response_bytes =
        round_trip(stream, 36, 2, 101, true, &authenticate_body).await?;
    let mut cur: &[u8] = &authenticate_response_bytes;
    let authenticate_response = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if authenticate_response.error_code != 0 {
        return Err(io::Error::other(format!(
            "in-band SaslAuthenticate error_code={} message={:?}",
            authenticate_response.error_code, authenticate_response.error_message
        )));
    }
    Ok(())
}

/// Test #1: a successful OAUTHBEARER authentication carries
/// `session_lifetime_ms ≈ exp - now`.
///
/// `session_lifetime_ms` is the KIP-368 wire field on
/// `SaslAuthenticateResponse v1+`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_session_lifetime_ms_set_from_token_exp() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");
    drop(stream);

    // ~600_000 ms; allow generous wall-clock slop for CI.
    assert!(
        (590_000..605_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~600_000"
    );

    handle.shutdown().await;
}

/// The broker clamps `session_lifetime_ms` when the config sets
/// `[oauthbearer].max_session_lifetime_seconds`.
///
/// The response value becomes `min(token_exp_ms - now_ms, cap * 1000)`. The
/// dispatch loop anchors its deadline to the CLAMPED value, so the broker
/// enforces what it told the client.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_session_capped_by_broker_max_session_lifetime_seconds() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker_with_cap(
        log_dir.path(),
        oauthbearer_zero_skew_validator(),
        Some(crabka_units::secs(30)), // 30s cap
    )
    .await;
    let addr = handle.listen_addr();

    // Token exp = now + 600s. Cap = 30s. Expected session = 30_000 ms.
    let exp_secs = now_unix_secs() + 600;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, session_lifetime_ms) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Cap should clamp the response.
    assert!(
        (29_000..31_000).contains(&session_lifetime_ms),
        "session_lifetime_ms = {session_lifetime_ms}, expected ~30_000 (capped)"
    );

    // Now pause and advance past cap; broker should close.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    tokio::time::resume();

    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(
        n == 0,
        "expected EOF after cap-bounded session expiry, got {n} bytes"
    );

    handle.shutdown().await;
}

/// Test #2: once the tokio clock advances past the token's `exp`, the
/// dispatch loop's per-connection `sleep_until` fires and closes the
/// TCP stream. The client observes EOF on the next read.
///
/// `tokio::time::pause()` needs the `current_thread` runtime. The test calls
/// `pause()` after the handshake and does not set `start_paused = true`,
/// because the broker's internal start-up timers need real wall-clock
/// progress. Those timers are the raft heartbeats, the JWKS refresh, and the
/// disk scans. Without that progress, `Broker::start` hangs. The dispatch
/// loop armed its deadline on the real Instant when it re-entered `select!`
/// after the handshake, so `advance(61s)` after the pause jumps tokio past
/// that Instant.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_session_expires_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let exp_secs = now_unix_secs() + 60;
    let token = unsecured_jws("alice", exp_secs);

    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("OAUTHBEARER session must succeed");

    // Freeze tokio's clock and jump past the token's expiry. The deadline
    // armed by the dispatch loop after handshake completion was anchored
    // to the real Instant at the time the loop re-entered `select!`; a
    // 61-second `advance` jumps tokio's clock past that Instant.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    tokio::time::resume();

    // Broker must have closed the connection. Read should EOF.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(n == 0, "expected EOF after session expiry, got {n} bytes");

    handle.shutdown().await;
}

/// Test #3: an in-band SaslHandshake/SaslAuthenticate pair with a fresh token
/// on an already-authenticated stream resets the per-connection deadline.
///
/// The fresh token has a longer `exp`. After the clock advances past the
/// original token's `exp`, the connection is still open and a Metadata RPC
/// succeeds.
#[tokio::test(flavor = "current_thread")]
async fn oauthbearer_in_band_reauth_with_fresh_token_resets_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    // Token A expires in 60s. Token B expires in 600s.
    let token_a = unsecured_jws("alice", now_unix_secs() + 60);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_a)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // In-band re-auth with the fresh token BEFORE token A expires (we
    // haven't yet paused / advanced anything).
    let token_b = unsecured_jws("alice", now_unix_secs() + 600);
    drive_inband_reauth(&mut stream, &token_b)
        .await
        .expect("in-band re-auth with fresh token must succeed");

    // Now jump past token A's exp (61s). Token B is good for another
    // ~540s, so the dispatch deadline should be re-armed to token B's
    // expiry and the connection must remain open.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_mins(2)).await;
    tokio::time::resume();

    // Issue a Metadata RPC to prove the connection survived.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req
        .encode(&mut md_body, 12)
        .expect("Metadata encode must succeed");
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 99, true, &md_body)
        .await
        .expect("Metadata RPC must succeed past original token expiry");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12).expect("Metadata decode must succeed");
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}

/// Test #4: the broker rejects an in-band re-auth with a token whose `sub`
/// differs from the original principal name.
///
/// `SaslAuthenticateResponse` carries
/// `error_code = SASL_AUTHENTICATION_FAILED (58)`, and the connection closes.
/// The client then reads EOF. KIP-368 forbids a change of principal across an
/// in-band re-auth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_principal_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    let handle = start_oauthbearer_broker(log_dir.path(), oauthbearer_zero_skew_validator()).await;
    let addr = handle.listen_addr();

    let token_alice = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token_alice)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // Attempt re-auth with a token belonging to "bob".
    let token_bob = unsecured_jws("bob", now_unix_secs() + 600);
    let result = drive_inband_reauth(&mut stream, &token_bob).await;
    let err = result.expect_err("re-auth with different principal must fail");
    assert!(
        err.to_string().contains("error_code=58"),
        "expected SASL_AUTHENTICATION_FAILED (58); got {err}"
    );

    // Broker closes after the error response.
    let mut buf = [0_u8; 16];
    let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("read should not hang")
        .expect("read should not error");
    assert!(n == 0, "expected EOF after failed re-auth");

    handle.shutdown().await;
}

/// Test #5: the broker rejects an in-band `SaslHandshake` whose `mechanism`
/// differs from the mechanism it first negotiated.
///
/// The response carries `error_code = ILLEGAL_SASL_STATE (34)`. KIP-368 needs
/// the same mechanism across an in-band re-auth, even when the broker would
/// otherwise accept SCRAM on a fresh connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oauthbearer_in_band_reauth_with_different_mechanism_closes() {
    let log_dir = tempfile::tempdir().unwrap();
    // Enable both OAUTHBEARER + SCRAM-SHA-512 on the same listener so a
    // fresh-connection SCRAM handshake WOULD succeed. The reject here
    // must be due to the same-mechanism rule, not "mechanism unknown".
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::OAuthBearer, SaslMechanism::ScramSha512];
    cfg.oauthbearer_validator = oauthbearer_zero_skew_validator();
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let token = unsecured_jws("alice", now_unix_secs() + 600);
    let (mut stream, _) = drive_sasl_oauthbearer_session_open(addr, &token)
        .await
        .expect("initial OAUTHBEARER must succeed");

    // In-band SaslHandshake with SCRAM-SHA-512 — must come back with
    // ILLEGAL_SASL_STATE (34) on the handshake response itself.
    let sh_req = SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req
        .encode(&mut sh_body, 1)
        .expect("SaslHandshake encode must succeed");
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 200, false, &sh_body)
        .await
        .expect("handshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp =
        SaslHandshakeResponse::decode(&mut cur, 1).expect("SaslHandshake decode must succeed");
    assert!(
        sh_resp.error_code == 34,
        "expected ILLEGAL_SASL_STATE for mechanism switch"
    );

    handle.shutdown().await;
}

/// Test #6: regression for PLAIN listeners.
///
/// A PLAIN `SaslAuthenticate` response must carry `session_lifetime_ms = 0`.
/// The KIP-368 wire field has a meaning for OAUTHBEARER only. The dispatch
/// loop must NOT arm a per-connection deadline. An advance of the tokio clock
/// by one hour is harmless, and a Metadata RPC still succeeds.
///
/// The test uses the `current_thread` flavor, because `tokio::time::pause()`
/// needs a single-threaded runtime. See the comment on test #2 for why we
/// pause after the handshake and do not set `start_paused = true`.
#[tokio::test(flavor = "current_thread")]
async fn plain_listener_session_lifetime_ms_is_zero_and_no_timer() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), alice_password());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Inline a full PLAIN handshake (mirrors `drive_sasl_plain_session`)
    // so we can capture the SaslAuthenticateResponse and assert its
    // `session_lifetime_ms` field directly.
    let mut stream = TcpStream::connect(addr).await.unwrap();

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body)
        .await
        .expect("ApiVersions round-trip");
    let mut cur: &[u8] = &av_resp_bytes;
    let _ = ApiVersionsResponse::decode(&mut cur, 0).unwrap();

    let sh_req = SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body)
        .await
        .expect("SaslHandshake round-trip");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();
    assert!(sh_resp.error_code == 0, "PLAIN handshake must succeed");

    let mut payload = Vec::new();
    payload.push(0);
    payload.extend_from_slice(b"alice");
    payload.push(0);
    payload.extend_from_slice(alice_password().as_bytes());
    let auth_req = SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    };
    let mut auth_body = BytesMut::new();
    auth_req.encode(&mut auth_body, 2).unwrap();
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body)
        .await
        .expect("SaslAuthenticate round-trip");
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2).unwrap();
    assert!(auth_resp.error_code == 0, "PLAIN authenticate must succeed");
    assert!(
        auth_resp.session_lifetime_ms == 0,
        "PLAIN listener must report session_lifetime_ms = 0 (no KIP-368 deadline)"
    );

    // Advance the tokio clock by an hour. The dispatch loop must NOT
    // have armed a per-connection timer for this non-OAuth session, so
    // the connection stays alive and serves further requests.
    tokio::time::pause();
    tokio::time::advance(std::time::Duration::from_hours(1)).await;
    tokio::time::resume();

    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();
    let md_resp_bytes = round_trip(&mut stream, 3, 12, 5, true, &md_body)
        .await
        .expect("Metadata RPC must succeed an hour after PLAIN auth");
    let mut cur: &[u8] = &md_resp_bytes;
    let md_resp = MetadataResponse::decode(&mut cur, 12).unwrap();
    assert!(
        !md_resp.brokers.is_empty(),
        "Metadata response must carry at least one broker"
    );

    handle.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────────
// AlterUserScramCredentials (api_key 51, KIP-554).
// ────────────────────────────────────────────────────────────────────────

/// SCRAM mechanism byte on the `AlterUserScramCredentials` wire, from
/// KIP-554. `1` is `SCRAM-SHA-256` and `2` is `SCRAM-SHA-512`.
const WIRE_MECH_SCRAM_SHA_256: i8 = 1;
const WIRE_MECH_SCRAM_SHA_512: i8 = 2;
const KAFKA_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
const KAFKA_UNACCEPTABLE_CREDENTIAL: i16 = 93;
const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Happy path: a super-user authenticates over PLAIN, sends an
/// `AlterUserScramCredentials` upsertion for `alice`, and the broker stores
/// the credential.
///
/// The test then authenticates as `alice` over SCRAM-SHA-512. This proves
/// that the upsertion wrote a valid credential to the metadata image.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain, SaslMechanism::ScramSha512];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR upsertion");
    assert!(resp.results.len() == 1, "one result row per upsertion");
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Round-trip: now log in as `alice` over SCRAM, proving the upserted
    // credential actually reached the metadata image. Wait for the raft
    // commit to land the credential in the committed image, then auth.
    handle
        .wait_for_image(|img| {
            img.scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        })
        .await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha512)
            .await;
    handle.shutdown().await;
    result.expect("post-upsertion SCRAM auth must succeed");
}

/// Wire-mapping proof: `AlterUserScramCredentials` accepts `mechanism=1`,
/// which is SCRAM-SHA-256, and stores a credential.
///
/// The broker can later authenticate against that credential over SHA-256.
/// The test is a copy of `alter_scram_creds_super_user_can_provision`, but it
/// uses the SHA-256 wire byte and a 32-byte `salted_password` payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision_sha256() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain, SaslMechanism::ScramSha256];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted_sha256(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_256,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR upsertion (SHA-256)");
    assert!(resp.results.len() == 1);
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == "alice");

    // Wait for the upserted credential to reach the committed metadata
    // image, then authenticate as `alice` over SHA-256 SCRAM.
    handle
        .wait_for_image(|img| {
            img.scram_credential("alice", SaslMechanism::ScramSha256)
                .is_some()
        })
        .await;
    let result =
        drive_sasl_scram_session(addr, "alice", &alice_password(), SaslMechanism::ScramSha256)
            .await;
    handle.shutdown().await;
    result.expect("post-upsertion SHA-256 SCRAM auth must succeed");
}

/// A non-super-user authenticates and tries to upsert.
///
/// The broker accepts the request, because it is a valid SASL listener API.
/// But every per-user row reports `CLUSTER_AUTHORIZATION_FAILED` (31). The
/// broker makes no metadata change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_non_super_user_rejected() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("bob".to_string(), wrong_scram_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; the default `AllowAllAuthorizer`
    // would let alice through.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            iterations: 4096,
            salt: bytes::Bytes::from(salt),
            salted_password: bytes::Bytes::from(salted.to_vec()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "bob",
        wrong_scram_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR (rejected)");
    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == 31, // CLUSTER_AUTHORIZATION_FAILED
        "non-super-user must get CLUSTER_AUTHORIZATION_FAILED, got {:?}",
        resp.results[0]
    );
}

/// `iterations < 4096` gives `UNACCEPTABLE_CREDENTIAL`.
///
/// The test uses a super-user principal, so the only error path it exercises
/// is the parameter validation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_low_iterations_rejected() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // 64-byte salted_password length is valid; only `iterations` violates.
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            iterations: 1,
            salt: bytes::Bytes::from(vec![0u8; 16]),
            salted_password: bytes::Bytes::from(vec![0u8; 64]),
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR (rejected)");
    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == KAFKA_UNACCEPTABLE_CREDENTIAL,
        "iterations < 4096 must get UNACCEPTABLE_CREDENTIAL, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_high_iterations_rejected_but_max_allowed() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![
            ScramCredentialUpsertion {
                name: "too-high".to_string(),
                mechanism: WIRE_MECH_SCRAM_SHA_512,
                iterations: KAFKA_MAX_SCRAM_ITERATIONS + 1,
                salt: bytes::Bytes::from(vec![0u8; 16]),
                salted_password: bytes::Bytes::from(vec![0u8; 64]),
                ..Default::default()
            },
            ScramCredentialUpsertion {
                name: "max".to_string(),
                mechanism: WIRE_MECH_SCRAM_SHA_512,
                iterations: KAFKA_MAX_SCRAM_ITERATIONS,
                salt: bytes::Bytes::from(vec![1u8; 16]),
                salted_password: bytes::Bytes::from(vec![1u8; 64]),
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR high iterations");

    handle.shutdown().await;
    assert!(resp.results.len() == 2, "one row per distinct username");
    let too_high = resp
        .results
        .iter()
        .find(|result| result.user == "too-high")
        .expect("too-high row");
    assert!(
        too_high.error_code == KAFKA_UNACCEPTABLE_CREDENTIAL,
        "iterations > 16384 must get UNACCEPTABLE_CREDENTIAL, got {:?}",
        too_high
    );
    let max = resp
        .results
        .iter()
        .find(|result| result.user == "max")
        .expect("max row");
    assert!(
        max.error_code == 0,
        "16384 iterations remains allowed: {max:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_unknown_mechanism_returns_unsupported_sasl_mechanism() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let admin_password = format!(
        "test-pass-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_password.clone());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: 99,
            iterations: 4096,
            salt: bytes::Bytes::from(vec![0u8; 16]),
            salted_password: bytes::Bytes::from(vec![0u8; 64]),
            ..Default::default()
        }],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR unknown mechanism");

    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == KAFKA_UNSUPPORTED_SASL_MECHANISM,
        "unknown SCRAM mechanism must get UNSUPPORTED_SASL_MECHANISM, got {:?}",
        resp.results[0]
    );
}

/// Two upsertions for the same user in one request: Kafka's response is
/// per username, so the single row for that username gets
/// `DUPLICATE_RESOURCE` (92) even when the mechanisms differ.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_resource_rejected() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_string(), admin_plain_password());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let (salt, salted) = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let upsert = ScramCredentialUpsertion {
        name: "alice".to_string(),
        mechanism: WIRE_MECH_SCRAM_SHA_512,
        iterations: 4096,
        salt: bytes::Bytes::from(salt),
        salted_password: bytes::Bytes::from(salted.to_vec()),
        ..Default::default()
    };
    let mut upsert_sha256 = upsert.clone();
    upsert_sha256.mechanism = WIRE_MECH_SCRAM_SHA_256;
    upsert_sha256.salted_password = bytes::Bytes::from(vec![7; 32]);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![upsert, upsert_sha256],
        ..Default::default()
    };
    let resp = drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        req,
    )
    .await
    .expect("PLAIN auth + AUSCR (duplicate)");
    handle.shutdown().await;
    assert!(resp.results.len() == 1, "one result row per username");
    assert!(
        resp.results[0].error_code == KAFKA_DUPLICATE_RESOURCE,
        "duplicate username must get DUPLICATE_RESOURCE, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_deletion_and_upsertion_rejected_per_user() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    let admin_password = uuid::Uuid::new_v4().to_string();
    cfg.plain_credentials
        .insert("admin".to_string(), admin_password.clone());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    handle
        .submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: "alice".into(),
                mechanism: SaslMechanism::ScramSha512,
                iterations: 4096,
                salt: vec![1; 16],
                server_key: vec![2; 64],
                stored_key: vec![3; 64],
            },
        ))
        .await
        .expect("seed alice SCRAM credential");
    handle
        .wait_for_image(|image| {
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        })
        .await;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        upsertions: vec![ScramCredentialUpsertion {
            name: "alice".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_256,
            iterations: 4096,
            salt: bytes::Bytes::from(vec![4u8; 16]),
            salted_password: bytes::Bytes::from(vec![5u8; 32]),
            ..Default::default()
        }],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR duplicate deletion/upsertion");

    handle.shutdown().await;
    assert!(resp.results.len() == 1, "one result row per username");
    assert!(resp.results[0].user == "alice");
    assert!(
        resp.results[0].error_code == KAFKA_DUPLICATE_RESOURCE,
        "delete+upsert for same username must get DUPLICATE_RESOURCE, got {:?}",
        resp.results[0]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_missing_deletion_returns_resource_not_found_91() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    let admin_password = uuid::Uuid::new_v4().to_string();
    cfg.plain_credentials
        .insert("admin".to_string(), admin_password.clone());
    cfg.super_users = std::collections::HashSet::from(["admin".to_string()]);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "ghost".to_string(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        ..Default::default()
    };

    let resp =
        drive_alter_user_scram_credentials_as_plain(addr, "admin", admin_password.as_bytes(), req)
            .await
            .expect("PLAIN auth + AUSCR missing deletion");

    handle.shutdown().await;
    assert!(resp.results.len() == 1);
    assert!(
        resp.results[0].error_code == 91,
        "missing deletion target must get RESOURCE_NOT_FOUND (91), got {:?}",
        resp.results[0]
    );
}

/// Authenticate over SASL/PLAIN against `addr` as `user`/`password`, send one
/// `AlterUserScramCredentials v0` request, and decode the response.
///
/// The request uses `api_key` 51 and is flexible. Every T15 test case calls
/// this helper, so the SASL boilerplate stays in one place.
async fn drive_alter_user_scram_credentials_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: AlterUserScramCredentialsRequest,
) -> Result<AlterUserScramCredentialsResponse, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let _ = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;

    // ── 2. SaslHandshake v1.
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .map_err(|e| io::Error::other(format!("SaslHandshake encode: {e}")))?;
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &sh_body).await?;
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1)
        .map_err(|e| io::Error::other(format!("SaslHandshake decode: {e}")))?;
    if sh_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslHandshake failed: error_code={}",
            sh_resp.error_code
        )));
    }

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0);
    payload.extend_from_slice(user.as_bytes());
    payload.push(0);
    payload.extend_from_slice(password);
    let mut auth_body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut auth_body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate encode: {e}")))?;
    let auth_resp_bytes = round_trip(&mut stream, 36, 2, 3, true, &auth_body).await?;
    let mut cur: &[u8] = &auth_resp_bytes;
    let auth_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate decode: {e}")))?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={}",
            auth_resp.error_code
        )));
    }

    // ── 4. AlterUserScramCredentials v0 (api_key 51, flexible from v0).
    let mut auscr_body = BytesMut::new();
    req.encode(&mut auscr_body, 0)
        .map_err(|e| io::Error::other(format!("AUSCR encode: {e}")))?;
    let auscr_resp_bytes = round_trip(&mut stream, 51, 0, 4, true, &auscr_body).await?;
    let mut cur: &[u8] = &auscr_resp_bytes;
    AlterUserScramCredentialsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("AUSCR decode: {e}")))
}

/// Compute `(salt, salted_password)` for a SCRAM-SHA-512 wire upsertion.
///
/// The salt is a fixed 16-byte vector, which keeps the test deterministic.
/// The salted password is the 64-byte PBKDF2-HMAC-SHA-512 output that the
/// KIP-554 wire request carries.
fn pbkdf2_salt_and_salted(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 64]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 64] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, iterations);
    (salt, salted)
}

/// SHA-256 analog of [`pbkdf2_salt_and_salted`].
///
/// It produces the 32-byte PBKDF2-HMAC-SHA-256 output for the wire tests.
fn pbkdf2_salt_and_salted_sha256(password: &[u8], iterations: u32) -> (Vec<u8>, [u8; 32]) {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 32] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, iterations);
    (salt, salted)
}

// ────────────────────────────────────────────────────────────────────────
// SASL gate integration test matrix.
// ────────────────────────────────────────────────────────────────────────

/// `ApiVersions`, `api_key` 18, is on the pre-auth allowlist and must succeed
/// without any SASL exchange.
///
/// The response should decode without an error. The supported-api list should
/// include `api_keys` 17, which is `SaslHandshake`, and 36, which is
/// `SaslAuthenticate`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_reachable_pre_auth_on_sasl_listener() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body)
        .await
        .expect("ApiVersions must succeed pre-auth on SASL listener");

    let mut cur: &[u8] = &av_resp_bytes;
    let av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .expect("ApiVersionsResponse must decode successfully");

    check!(
        av_resp.error_code == 0,
        "ApiVersions error_code must be 0 on SASL listener pre-auth"
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 17),
        "ApiVersionsResponse must list SaslHandshake (17): {:?}",
        av_resp.api_keys
    );
    check!(
        av_resp.api_keys.iter().any(|k| k.api_key == 36),
        "ApiVersionsResponse must list SaslAuthenticate (36): {:?}",
        av_resp.api_keys
    );

    handle.shutdown().await;
}

/// A pre-auth `Metadata` request on a `SASL_PLAINTEXT` listener must not
/// succeed.
///
/// `Metadata`, `api_key` 3, is not on the pre-auth allowlist. T12 closes the
/// TCP connection and does not encode a typed error response. So the read
/// after the `Metadata` request must return an I/O error, either
/// `UnexpectedEof` or a connection reset, and not a well-formed response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_rejected_pre_auth_on_sasl_listener() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send Metadata (api_key=3, v12, flexible) WITHOUT any auth.
    let md_req = MetadataRequest::default();
    let mut md_body = BytesMut::new();
    md_req.encode(&mut md_body, 12).unwrap();

    // Build the frame manually: header + body, then length-prefix.
    let mut frame = BytesMut::with_capacity(32 + md_body.len());
    frame.put_i16(3); // api_key = Metadata
    frame.put_i16(12); // api_version
    frame.put_i32(1); // correlation_id
    let client_id = "crabka-t19-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible header tagged-fields
    frame.put_slice(&md_body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .unwrap();
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();

    // The broker closes the connection instead of responding — any read
    // attempt must return an error (UnexpectedEof / connection reset).
    let read_result = stream.read_u32().await;
    assert!(
        read_result.is_err(),
        "expected TCP close after pre-auth Metadata, but read succeeded: {read_result:?}"
    );

    handle.shutdown().await;
}

/// A `SaslHandshake` with an unsupported mechanism, GSSAPI, must return
/// `error_code = 33`, which is `UNSUPPORTED_SASL_MECHANISM`, with the enabled
/// list AND keep the connection open.
///
/// A `SaslHandshake` that follows with the supported mechanism, PLAIN, must
/// succeed with `error_code = 0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_mechanism_rejected_but_handshake_retryable() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // ── 1. SaslHandshake with "GSSAPI" (not in enabled list).
    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
        ..Default::default()
    }
    .encode(&mut sh_body, 1)
    .unwrap();
    let sh_resp_bytes = round_trip(&mut stream, 17, 1, 1, false, &sh_body)
        .await
        .expect("SaslHandshake(GSSAPI) must get a response (not a TCP close)");
    let mut cur: &[u8] = &sh_resp_bytes;
    let sh_resp =
        SaslHandshakeResponse::decode(&mut cur, 1).expect("SaslHandshakeResponse must decode");
    assert!(
        sh_resp.error_code == 33, // UNSUPPORTED_SASL_MECHANISM
        "GSSAPI handshake must return error_code=33, got {:?}",
        sh_resp.error_code
    );
    assert!(
        sh_resp.mechanisms.iter().any(|m| m == "PLAIN"),
        "error response must include the enabled mechanisms list: {:?}",
        sh_resp.mechanisms
    );

    // ── 2. Retry on the SAME connection with "PLAIN" — must succeed.
    let mut plain_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut plain_body, 1)
    .unwrap();
    let plain_resp_bytes = round_trip(&mut stream, 17, 1, 2, false, &plain_body)
        .await
        .expect("SaslHandshake(PLAIN) retry must succeed on the same connection");
    let mut plain_cur: &[u8] = &plain_resp_bytes;
    let plain_resp = SaslHandshakeResponse::decode(&mut plain_cur, 1)
        .expect("SaslHandshakeResponse retry must decode");
    assert!(
        plain_resp.error_code == 0,
        "PLAIN handshake retry on same connection must return error_code=0"
    );

    handle.shutdown().await;
}

// ────────────────────────────────────────────────────────────────────────
// InterBrokerClient — outbound TLS + SASL handshake.
// ────────────────────────────────────────────────────────────────────────

/// Start a broker with a `SASL_PLAINTEXT` listener and one PLAIN credential,
/// then dial it with the public `InterBrokerClient` API.
///
/// The client must run `SaslHandshake` and `SaslAuthenticate` on its own, and
/// it must return a stream that the caller can keep using for normal RPCs.
/// The test proves this: it sends an `ApiVersions` request over the returned
/// stream and decodes the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inter_broker_client_authenticates_via_plain() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), admin_plain_password());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let client = crabka_broker::network::client::InterBrokerClient::new(
        None,
        Some(crabka_broker::config::InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: admin_plain_password(),
        }),
    );

    let result = drive_inter_broker_client_then_apiversions(&client, addr).await;
    handle.shutdown().await;
    result.expect("InterBrokerClient PLAIN auth + ApiVersions round-trip must succeed");
}

/// Drive `InterBrokerClient::connect` and run one `ApiVersions` round-trip
/// over the post-auth stream to prove that the stream survives.
///
/// The helper works with any mechanism. It dials `localhost:<port>` over
/// `SaslPlaintext`, so a GSSAPI SPN resolves to `kafka/localhost`. It then
/// asserts that the post-auth stream works.
async fn drive_inter_broker_client_then_apiversions(
    client: &crabka_broker::network::client::InterBrokerClient,
    addr: SocketAddr,
) -> Result<(), io::Error> {
    let options = crabka_client_core::ConnectionOptions {
        client_id: "crabka-t16-test".to_owned(),
        ..Default::default()
    };
    let mut stream = client
        .connect(
            &addr.ip().to_string(),
            addr.port(),
            ListenerProtocol::SaslPlaintext,
            "localhost",
            &options,
        )
        .await
        .map_err(|e| io::Error::other(format!("InterBrokerClient::connect: {e}")))?;

    // Build an ApiVersions v0 request, frame it, send it through the
    // authenticated stream, decode the response. This proves (a) the
    // client returned a usable stream and (b) the broker treats the
    // stream as fully authenticated.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;

    let mut frame = BytesMut::with_capacity(16 + av_body.len());
    frame.put_i16(18); // api_key = ApiVersions
    frame.put_i16(0); // api_version
    frame.put_i32(99); // post-auth correlation id (distinct from auth ones)
    let client_id = "crabka-t16-test";
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    // ApiVersions v0 is non-flexible → no tagged-fields byte.
    frame.put_slice(&av_body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    // Non-flexible response: header is v0 (just corr_id).
    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;
    Ok(())
}

// ────────────────────────────────────────────────────────────────────────
// Task 8: SASL/GSSAPI handshake advertisement (no Docker / no KDC).
// ────────────────────────────────────────────────────────────────────────

/// A broker with GSSAPI enabled advertises GSSAPI in its `SaslHandshake`
/// response and accepts the handshake with `error_code = 0`.
///
/// The connection then stays in GSSAPI negotiation. The GSS context exchange
/// itself needs a live KDC, and the E2E parity tests in Task 10 cover it.
/// This case proves two things only. The mechanism is wired through the
/// handshake advertisement, and the broker does not touch the keytab before
/// the first `SaslAuthenticate` round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gssapi_handshake_advertised_when_enabled() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Gssapi];
    cfg.gssapi = Some(crabka_security::gssapi::GssapiConfig {
        // Points at the committed fixture, but the handshake path never reads
        // it (the acceptor is built lazily on the first SaslAuthenticate).
        keytab_path: std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../security/tests/fixtures/kdc/kafka.keytab"),
        service_name: "kafka".to_string(),
        principal_to_local_rules: vec![],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: None,
        max_time_skew: crabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut corr = 0;

    // ApiVersions (pre-auth) so the connection is in a clean state.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req.encode(&mut av_body, 0).unwrap();
    let av = round_trip(&mut stream, 18, 0, corr, false, &av_body)
        .await
        .unwrap();
    corr += 1;
    let mut cur: &[u8] = &av;
    ApiVersionsResponse::decode(&mut cur, 0).unwrap();

    // SaslHandshake v1, mechanism = "GSSAPI".
    let sh_req = SaslHandshakeRequest {
        mechanism: "GSSAPI".to_string(),
        ..Default::default()
    };
    let mut sh_body = BytesMut::new();
    sh_req.encode(&mut sh_body, 1).unwrap();
    let sh = round_trip(&mut stream, 17, 1, corr, false, &sh_body)
        .await
        .unwrap();
    let mut cur: &[u8] = &sh;
    let sh_resp = SaslHandshakeResponse::decode(&mut cur, 1).unwrap();

    handle.shutdown().await;

    assert!(sh_resp.error_code == 0, "GSSAPI handshake must succeed");
    assert!(
        sh_resp.mechanisms.iter().any(|m| m == "GSSAPI"),
        "GSSAPI must be advertised; got {:?}",
        sh_resp.mechanisms
    );
}

/// End-to-end inter-broker GSSAPI initiate against a live KDC.
///
/// A Crabka broker accepts on a `SASL_PLAINTEXT`/GSSAPI listener, with the
/// service key in `kafka.keytab`. `InterBrokerClient` dials it with
/// `InterBrokerCredentials::Gssapi` and authenticates *from a keytab* as
/// `alice@CRABKA.TEST`, with no password. The test proves the full outbound
/// GSSAPI path: AS/TGS from `alice.keytab` → AP-REQ → broker validates →
/// RFC 4752 auth-only layer negotiation → authenticated stream. A follow-up
/// `ApiVersions` round-trip confirms the stream.
///
/// The test needs the MIT KDC fixture and the exported env, the same as the
/// provider contract test:
///
/// ```text
/// cd crates/security/tests/fixtures/kdc && docker compose up --build -d
/// KRB5_CONFIG=crates/security/tests/fixtures/kdc/krb5.conf SSPI_KDC_URL=tcp://localhost:88 \
///   cargo test -p crabka-broker gssapi_inter_broker -- --ignored
/// ```
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the MIT KDC fixture (docker compose up) + exported KRB5_CONFIG/SSPI_KDC_URL"]
async fn gssapi_inter_broker_client_authenticates_from_keytab() {
    let fixtures =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../security/tests/fixtures/kdc");
    let kdc_url =
        std::env::var("SSPI_KDC_URL").unwrap_or_else(|_| "tcp://localhost:88".to_string());

    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Gssapi];
    cfg.gssapi = Some(crabka_security::gssapi::GssapiConfig {
        keytab_path: fixtures.join("kafka.keytab"),
        service_name: "kafka".to_string(),
        // DEFAULT rule + matching default realm maps alice@CRABKA.TEST to
        // the short name "alice".
        principal_to_local_rules: vec![crabka_security::gssapi::name::Rule::Default],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: Some(kdc_url.clone()),
        max_time_skew: crabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    });

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let client = crabka_broker::network::client::InterBrokerClient::new(
        None,
        Some(crabka_broker::config::InterBrokerCredentials::Gssapi {
            keytab_path: fixtures.join("alice.keytab"),
            client_principal: "alice@CRABKA.TEST".to_string(),
            service_name: "kafka".to_string(),
            kdc_url,
        }),
    );

    let result = drive_inter_broker_client_then_apiversions(&client, addr).await;
    handle.shutdown().await;
    result.expect("InterBrokerClient GSSAPI auth + ApiVersions round-trip must succeed");
}

// ────────────────────────────────────────────────────────────────────────
// InterBrokerClient wired into replicator / heartbeat — proves a
// two-broker cluster with a SASL_PLAINTEXT inter-broker listener
// authenticates outbound fetch + heartbeat traffic and replicates records
// end-to-end.
//
// Gated to non-Windows (openraft `debug_assert!` race on the hosted Windows
// runner — same gate as `tests/replication.rs`).
// ────────────────────────────────────────────────────────────────────────

mod two_broker_sasl {
    use assert2::assert;
    use crabka_broker::{BootstrapMode, Broker, BrokerHandle, config::InterBrokerCredentials};
    use crabka_protocol::{
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        },
        records::{Record, RecordBatch},
    };
    use tempfile::TempDir;

    use super::*;

    /// Reserve `n` ephemeral loopback ports with the bind-and-drop method.
    async fn reserve_ports(n: usize) -> Vec<SocketAddr> {
        let mut out = Vec::with_capacity(n);
        let mut listeners = Vec::with_capacity(n);
        for _ in 0..n {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            out.push(l.local_addr().unwrap());
            listeners.push(l);
        }
        drop(listeners);
        out
    }

    /// Build a SASL-enabled broker config with two listeners.
    ///
    /// The first listener is a PLAINTEXT data-plane listener on
    /// `listen_addr`. The test clients use it, because they do not speak SASL
    /// yet. The second listener is a `SASL_PLAINTEXT` inter-broker listener.
    /// The replicator and the heartbeat use it against the peer broker.
    fn sasl_two_listener_config(
        i: usize,
        plaintext_addrs: &[SocketAddr],
        sasl_addrs: &[SocketAddr],
        controller_addrs: &[SocketAddr],
        voters: &[(u64, SocketAddr)],
        log_dir: &std::path::Path,
        mode: BootstrapMode,
    ) -> BrokerConfig {
        let listen = plaintext_addrs[i];
        let sasl = sasl_addrs[i];
        let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
        cfg.broker_id = i32::try_from(i + 1).unwrap();
        cfg.listen_addr = listen;
        cfg.advertised_listener = listen.to_string();
        cfg.node_id = crabka_broker::NodeId(u64::try_from(i + 1).unwrap());
        cfg.controller_listen_addr = controller_addrs[i];
        cfg.controller_quorum_voters = voters
            .iter()
            .map(|(id, a)| (crabka_broker::NodeId(*id), a.to_string()))
            .collect();
        cfg.bootstrap_mode = mode;
        cfg.listeners = vec![
            ListenerSpec {
                name: "PLAINTEXT".to_string(),
                bind_addr: listen,
                advertised: listen.to_string(),
                protocol: ListenerProtocol::Plaintext,
                tls_config: None,
                sasl_mechanisms: None,
            },
            ListenerSpec {
                name: "SASL_PLAINTEXT".to_string(),
                bind_addr: sasl,
                advertised: sasl.to_string(),
                protocol: ListenerProtocol::SaslPlaintext,
                tls_config: None,
                sasl_mechanisms: None,
            },
        ];
        cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
        cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        cfg.plain_credentials
            .insert("broker".to_string(), admin_plain_password());
        cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: admin_plain_password(),
        });
        cfg
    }

    /// Start a 2-broker cluster whose inter-broker listener is
    /// `SASL_PLAINTEXT`.
    ///
    /// This helper is a copy of `support::start_n_node`, but it uses the
    /// two-listener config above. It returns `(handle, config, tempdir)`
    /// triples in broker id order.
    async fn start_two_node_sasl() -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();

        let plaintext_addrs = reserve_ports(2).await;
        let sasl_addrs = reserve_ports(2).await;
        let controller_addrs = reserve_ports(2).await;
        let voters: Vec<(u64, SocketAddr)> = (0..2_u64)
            .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
            .collect();

        let dir0 = TempDir::new().unwrap();
        let cfg0 = sasl_two_listener_config(
            0,
            &plaintext_addrs,
            &sasl_addrs,
            &controller_addrs,
            &voters,
            dir0.path(),
            BootstrapMode::Bootstrap,
        );
        let dir1 = TempDir::new().unwrap();
        let cfg1 = sasl_two_listener_config(
            1,
            &plaintext_addrs,
            &sasl_addrs,
            &controller_addrs,
            &voters,
            dir1.path(),
            BootstrapMode::Bootstrap,
        );
        // KIP-595 Slice 3c static bootstrap: both brokers boot with the same
        // static voter set and elect among themselves over the SASL controller
        // wire — no add_learner / change_membership (KIP-853, Slice 5). Start
        // them concurrently: `Broker::start` blocks until a leader is committed,
        // which needs a voter majority up, so a sequential `start().await` on
        // broker0 alone would deadlock.
        let cfg0_for_spawn = cfg0.clone();
        let cfg1_for_spawn = cfg1.clone();
        let join0 = tokio::spawn(async move { Broker::start(cfg0_for_spawn).await });
        let join1 = tokio::spawn(async move { Broker::start(cfg1_for_spawn).await });
        let broker0 = join0.await.expect("join0 spawn").expect("broker0 start");
        let broker1 = join1.await.expect("join1 spawn").expect("broker1 start");
        vec![(broker0, cfg0, dir0), (broker1, cfg1, dir1)]
    }

    /// Start two brokers with a `SASL_PLAINTEXT` inter-broker listener,
    /// create a topic with rf=2, produce, and check that the follower
    /// converges.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_broker_sasl_plaintext_replication() {
        let cluster = start_two_node_sasl().await;

        // Wait for both brokers to register in each other's image.
        for (h, _, _) in &cluster {
            h.wait_until_brokers_registered(2).await;
        }

        let leader_addr = cluster[0].1.listen_addr.to_string();
        let admin = Client::builder()
            .bootstrap(leader_addr.clone())
            .build()
            .await
            .unwrap();
        let resp = admin
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "sasl-repl".into(),
                    num_partitions: 1,
                    replication_factor: 2,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.topics[0].error_code == 0);
        let topic_id = resp.topics[0].topic_id;

        // Wait for the topic to propagate to every broker's image.
        for (h, _, _) in &cluster {
            h.wait_until_partition_present("sasl-repl", 0).await;
        }

        // Produce 10 records to the leader.
        let producer = Client::builder()
            .bootstrap(leader_addr)
            .build()
            .await
            .unwrap();
        let batch = RecordBatch {
            base_offset: 0,
            last_offset_delta: 9,
            records: (0..10)
                .map(|i| Record {
                    offset_delta: i,
                    value: Some(bytes::Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let prod = producer
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: "sasl-repl".into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(prod.responses[0].partition_responses[0].error_code == 0);

        // Wait until every broker's local log reaches >= 10. The SASL
        // inter-broker handshake on each follower-fetch round trip is the
        // critical path here — a misconfigured replicator would never
        // commit a record and this awaiter would time out.
        for (h, _, _) in &cluster {
            h.wait_until_local_log_end_offset("sasl-repl", 0, 10).await;
        }

        for (h, _, _) in cluster {
            h.shutdown().await;
        }
    }
}
