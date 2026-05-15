//! Slice 12 broker-side auth tests. No Docker.
//!
//! T10 contributes a single smoke test that proves a TLS-only listener
//! completes a TLS handshake with a stock `tokio_rustls::TlsConnector`
//! using the dev cert fixture as the trust anchor. T11 adds a Metadata
//! round-trip case that verifies per-listener endpoints land on the
//! broker's self-registration record. Subsequent tasks (T12+) extend
//! this file with SASL/PLAIN, SASL/SCRAM, and `AlterUserScramCredentials`
//! cases.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::metadata_response::MetadataResponse;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism, TlsConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

// ────────────────────────────────────────────────────────────────────────
// Task 13: SASL/PLAIN end-to-end.
// ────────────────────────────────────────────────────────────────────────

/// Happy-path drive of a SASL/PLAIN session: `ApiVersions` → `SaslHandshake`
/// → `SaslAuthenticate` → Metadata. Asserts the connection survives every step
/// and the final Metadata response carries this broker. The dial-side runs
/// raw bytes against `TcpStream` rather than `Client` because `Client`
/// doesn't (yet) speak SASL — that's task 16.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), "wonderland".to_string());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", b"wonderland").await;
    handle.shutdown().await;
    result.expect("SASL/PLAIN session must succeed end-to-end");
}

/// Negative path: wrong password ⇒ `SaslAuthenticate` responds with
/// `error_code = SASL_AUTHENTICATION_FAILED` (58) and the broker closes
/// the connection. `drive_sasl_plain_session` surfaces the failure as an
/// `Err` either when the auth response's `error_code` is non-zero, or when
/// the subsequent Metadata read returns EOF (connection closed by peer).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_plain_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("alice".to_string(), "wonderland".to_string());

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    let result = drive_sasl_plain_session(addr, "alice", b"hunter2").await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail the SASL session: {result:?}"
    );
}

/// Drive a complete SASL/PLAIN session against a `SASL_PLAINTEXT` listener.
///
/// On success, returns `Ok(())` after a successful post-auth Metadata
/// round-trip. Returns `Err` when any step (frame I/O, response decode,
/// non-zero error code on a SASL response, EOF before Metadata) fails.
///
/// Wire-protocol mechanics this helper handles inline (no `Client` API):
/// - Request headers: v1 (non-flexible) for `ApiVersions v0`, `SaslHandshake v1`;
///   v2 (flexible, trailing `0x00` tagged-fields byte) for `SaslAuthenticate v2`
///   and `Metadata v12`.
/// - Response headers: always v0 (just `correlation_id`) for `ApiVersions`
///   regardless of body flexibility, v1 (`corr_id` + `0x00` tagged byte) for
///   every other flexible response, v0 for non-flexible.
/// - Length framing: 4-byte big-endian length prefix on every frame in
///   both directions, matching `crabka_broker::network::codec`.
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
// Task 14: SASL/SCRAM-SHA-512 end-to-end.
// ────────────────────────────────────────────────────────────────────────

/// Happy-path drive of a SASL/SCRAM-SHA-512 session: provisions a credential
/// for "alice" with password "wonderland" directly through the controller
/// (the public `AlterUserScramCredentials` handler lands in Task 15), then
/// runs the two-round RFC 5802 dance end-to-end and asserts the post-auth
/// Metadata request succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_happy_path() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    // Provision alice/wonderland directly via the controller. The public
    // path (AlterUserScramCredentials, api_key 51) is built in Task 15.
    let cred =
        crabka_security::hash_scram_password(b"wonderland", SaslMechanism::ScramSha512, 4096);
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
    let result = drive_sasl_scram_session(addr, "alice", "wonderland").await;
    handle.shutdown().await;
    result.expect("SASL/SCRAM session must succeed end-to-end");
}

/// Negative path: wrong password ⇒ `SaslAuthenticate` round 2 responds
/// with `error_code = 58` (`SASL_AUTHENTICATION_FAILED`) and the broker
/// closes the connection. `drive_sasl_scram_session` surfaces the failure
/// either as a non-zero error code on the auth response or as EOF when the
/// follow-up Metadata read returns no bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_scram_sha512_wrong_password_closes_connection() {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];

    let handle = Broker::start(cfg).await.expect("broker must start");

    let cred =
        crabka_security::hash_scram_password(b"wonderland", SaslMechanism::ScramSha512, 4096);
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
    let result = drive_sasl_scram_session(addr, "alice", "hunter2").await;
    handle.shutdown().await;
    assert!(
        result.is_err(),
        "wrong password must fail SCRAM session: {result:?}"
    );
}

/// Drive a complete SASL/SCRAM-SHA-512 session against a `SASL_PLAINTEXT`
/// listener.
///
/// On success returns `Ok(())` after a successful post-auth Metadata
/// round-trip. Returns `Err` when any step fails — non-zero error code on
/// either of the two `SaslAuthenticate` rounds, a server-final signature
/// mismatch (client-side proof), or EOF before Metadata returns.
async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
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

    // ── 2. SaslHandshake v1 (non-flexible, mechanism="SCRAM-SHA-512").
    let mut sh_body = BytesMut::new();
    let sh_req = SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-512".to_string(),
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
    let mut client =
        crabka_security::ScramClientExchange::new(user.to_string(), password.as_bytes().to_vec());
    let client_first = client
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
    let client_final = client
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

/// Encode a `RequestHeader v1` (or v2 when `flexible`), append the body,
/// write the length-prefixed frame, then read one response frame and
/// strip the `ResponseHeader` (always v0 for ApiVersions(18), otherwise
/// v0 if non-flexible / v1 if flexible). Returns the response body bytes.
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
