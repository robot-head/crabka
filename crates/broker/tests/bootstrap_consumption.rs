//! `crabka format --add-scram` -> broker bootstrap consumption.
//!
//! Each test either runs the `crabka` CLI to produce a `log_dir` containing
//! a `bootstrap.records.bin` file, or writes that file directly (for the
//! corruption case), then starts a single-broker `Broker` pointed at the
//! dir and verifies the broker's bootstrap path behaves correctly.
//!
//! The happy-path test additionally drives a full SASL/SCRAM-SHA-512
//! handshake against the broker's `SASL_PLAINTEXT` listener to prove the
//! seeded credential is queryable via the metadata image immediately
//! after `Broker::start` returns.
//!
//! ## Helper duplication
//!
//! `drive_sasl_scram_session` / `round_trip` are copied verbatim from
//! `tests/auth_handlers.rs`. Cargo's integration-test model gives each
//! `tests/*.rs` file its own crate root — sharing code between two such
//! files requires either a `tests/common/mod.rs` submodule or a copy.
//! For a self-contained test file that consumes only these two
//! helpers, a verbatim copy keeps blast radius small and avoids touching
//! the (1500+ line) auth test file.

use std::{io, net::SocketAddr, process::Command};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// Run the `crabka format` binary as a subprocess.
///
fn run_crabka_format(log_dir: &std::path::Path, add_scram: &str) {
    let out = Command::new(env!("CARGO_BIN_EXE_crabka"))
        .args([
            "format",
            "--log-dir",
            log_dir.to_str().unwrap(),
            "--add-scram",
            add_scram,
        ])
        .output()
        .expect("spawn crabka format");
    assert!(
        out.status.success(),
        "crabka format failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_records_provisions_scram_user() {
    // The broker installs the rustls crypto provider in `Broker::start`,
    // but the SCRAM client side of this test also performs PBKDF2 / SHA
    // through the same provider. Install it defensively so the test is
    // order-independent. `.ok()` swallows `AlreadySet`.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let dir = tempfile::tempdir().unwrap();
    // Format the log_dir into the *child* `boot` subdir — the CLI refuses
    // to overwrite a non-empty directory, and `tempfile::tempdir()` returns
    // a path whose parent already exists.
    let boot_dir = dir.path().join("boot");
    run_crabka_format(
        &boot_dir,
        "SCRAM-SHA-512=[name=alice,password=wonderland,iterations=4096]",
    );

    let mut cfg = BrokerConfig::for_tests(boot_dir.clone());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".into();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::ScramSha512];
    cfg.bootstrap_mode = crabka_broker::BootstrapMode::Bootstrap;

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let result = drive_sasl_scram_session(addr, "alice", "wonderland").await;
    handle.shutdown().await;
    assert!(
        result.is_ok(),
        "alice/wonderland should authenticate via SCRAM: {result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_bootstrap_refuses_start() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("bootstrap.records.bin"),
        b"this is not a length-prefixed metadata record",
    )
    .unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    // `for_tests` already defaults `bootstrap_mode` to `Bootstrap`.
    let result = Broker::start(cfg).await;
    // `BrokerHandle` doesn't implement `Debug`, so a `Result<H, E>` can't
    // be `{:?}`-formatted directly. Branch on the variant for the panic
    // message instead.
    match result {
        Err(crabka_broker::BrokerError::BootstrapFile { .. }) => {}
        Err(other) => panic!("expected BootstrapFile error, got {other:?}"),
        Ok(handle) => {
            handle.shutdown().await;
            panic!("expected BootstrapFile error, broker started successfully");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bootstrap_absent_legacy_path() {
    // No bootstrap.records.bin written. Existing fresh-bootstrap behavior
    // unchanged — single-broker cluster comes up.
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    handle.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers copied verbatim from `tests/auth_handlers.rs`.
// Cargo's integration-test model gives each `tests/*.rs` its own crate
// root; sharing helpers requires a `tests/common/mod.rs` submodule. The
// verbatim copy is intentional — see file-level docs above.
// ─────────────────────────────────────────────────────────────────────────

/// Drive a complete SASL/SCRAM-SHA-512 session against a `SASL_PLAINTEXT`
/// listener.
async fn drive_sasl_scram_session(
    addr: SocketAddr,
    user: &str,
    password: &str,
) -> Result<(), io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // 1. ApiVersions (v0, non-flexible). Pre-auth allowlist.
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    let _av_resp = ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // 2. SaslHandshake v1 (non-flexible, mechanism="SCRAM-SHA-512").
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

    // 3. SCRAM client-first → server-first.
    let client = crabka_security::ScramClientExchange::new(
        user.to_string(),
        password.as_bytes().to_vec(),
        crabka_security::SaslMechanism::ScramSha512,
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

    // 4. SCRAM client-final → server-final.
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
    client
        .verify_server_final(&scram_final_response.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    // 5. Post-auth Metadata round-trip proves the connection survived
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
/// strip the `ResponseHeader`.
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    let client_id = "crabka-bootstrap-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame size fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

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
