// rustc 1.95 clippy ICEs on this file family (same as throttle.rs /
// describe_user_scram_credentials.rs). Suppress locally; the workspace
// lint gate still applies elsewhere.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]

//! Slice 51 (KIP-48) end-to-end integration: full delegation-token lifecycle
//! against a single-broker test cluster. Plan task T11 / spec §8.2.
//!
//! One long `#[tokio::test]` walks every wire step the spec covers:
//!
//!   (a) SASL/PLAIN authenticate as `alice`.
//!   (b) `CreateDelegationToken` over that connection — owner=alice,
//!       renewers=[User:bob], `max_lifetime_ms = -1` (defer to broker
//!       ceiling). Capture `token_id` + `hmac`.
//!   (c) Open a second TCP connection; drive SASL/SCRAM-SHA-256 with
//!       username=`token_id`, password=base64(hmac). The KIP-48
//!       token-fallback path in `network::auth::handle_authenticate_scram`
//!       synthesizes a SCRAM credential for the token and accepts; the
//!       principal must surface as `User:alice` (the token owner), NOT as
//!       the token_id. We assert this by re-running `CreateDelegationToken`
//!       on the token-authed connection and observing
//!       `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) — that error is only
//!       reachable when the broker correctly sees this session as
//!       `authenticated_via_token = true`, which is set together with the
//!       owner-principal override.
//!   (d) Same connection: re-`CreateDelegationToken` → expect 64.
//!   (e) Third TCP connection; SASL/PLAIN as `bob`; `RenewDelegationToken`
//!       with the captured HMAC. Expect `error_code = 0` (the
//!       renewer-authorization gate accepts the listed renewer). The
//!       returned expiry is clamped to `max_timestamp_ms`, which the
//!       create handler set equal to the original expiry — so this
//!       round-trips the original value rather than extending past it.
//!   (f) `alice`'s connection: `DescribeDelegationToken` with
//!       `owners=[User:alice]`. Expect 1 token, matching `token_id`.
//!   (g) `alice`'s connection: `ExpireDelegationToken` with
//!       `expiry_time_period_ms = -1` (immediate-delete sentinel). Expect
//!       `error_code = 0`.
//!   (h) Fourth TCP connection; attempt SASL/SCRAM-SHA-256 with the same
//!       token creds. Expect failure — the token's tombstone is in the
//!       image and the SCRAM credential lookup misses.
//!
//! This file deliberately reuses the wire-driver shape from
//! `auth_handlers.rs` (PLAIN + SCRAM-SHA-256 + `round_trip` helper) and
//! `describe_user_scram_credentials.rs` (the `(handle, dir, addr)` cluster
//! tuple). No public test-support surface was added — the helpers are
//! inline so they don't leak into other tests.

#![cfg(not(target_os = "windows"))]

use std::io;
use std::net::SocketAddr;

use base64::Engine;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::owned::create_delegation_token_request::{
    CreatableRenewers, CreateDelegationTokenRequest,
};
use crabka_protocol::owned::create_delegation_token_response::CreateDelegationTokenResponse;
use crabka_protocol::owned::describe_delegation_token_request::{
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
};
use crabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse;
use crabka_protocol::owned::expire_delegation_token_request::ExpireDelegationTokenRequest;
use crabka_protocol::owned::expire_delegation_token_response::ExpireDelegationTokenResponse;
use crabka_protocol::owned::renew_delegation_token_request::RenewDelegationTokenRequest;
use crabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism, SecretBytes};

/// Canonical Kafka error code mirroring `crabka_broker::codes::
/// DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`. The broker's `codes` module is
/// private to the crate, so we keep a local copy — kept in sync with
/// `crates/broker/src/codes.rs` and the Apache Kafka error table.
const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ─────────────────────────────────────────────────────────────────────────────
// Wire framing (length-prefixed request/response). Same shape as
// `auth_handlers.rs::round_trip` and `describe_user_scram_credentials.rs`.
// ─────────────────────────────────────────────────────────────────────────────

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
    let client_id = "crabka-deltok-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _resp_corr_id = cur.get_i32();
    // Flexible (v2+) responses carry a 1-byte header tagged-fields prefix,
    // except ApiVersions(18) which is special-cased by the spec.
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

// ─────────────────────────────────────────────────────────────────────────────
// SASL handshake drivers. Both walk ApiVersions → SaslHandshake →
// SaslAuthenticate on a fresh TcpStream and return the still-open stream
// for follow-up requests.
// ─────────────────────────────────────────────────────────────────────────────

/// PLAIN happy-path driver. Mirrors `auth_handlers.rs::drive_sasl_plain_session`
/// but stops at the post-auth Metadata round-trip — callers want the open
/// connection so they can issue admin RPCs.
async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

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
            "SaslHandshake(PLAIN) failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let mut payload = Vec::with_capacity(2 + user.len() + password.len());
    payload.push(0); // empty authzid
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
            "SaslAuthenticate(PLAIN) failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

/// SCRAM-SHA-256 driver. Same wire shape as
/// `auth_handlers.rs::drive_sasl_scram_session` but factored to return the
/// open connection on success — needed by step (c).
async fn sasl_scram_sha256_authenticate(
    addr: SocketAddr,
    username: &str,
    password: &str,
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    let mut sh_body = BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "SCRAM-SHA-256".to_string(),
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
            "SaslHandshake(SCRAM-SHA-256) failed: error_code={}",
            sh_resp.error_code
        )));
    }

    let mut client = crabka_security::ScramClientExchange::new(
        username.to_string(),
        password.as_bytes().to_vec(),
        SaslMechanism::ScramSha256,
    );
    let client_first = client
        .client_first()
        .map_err(|e| io::Error::other(format!("scram client_first: {e:?}")))?;

    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_first),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) encode: {e}")))?;
    let r1 = round_trip(&mut stream, 36, 2, 3, true, &body).await?;
    let mut cur: &[u8] = &r1;
    let r1_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(1) decode: {e}")))?;
    if r1_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 1 failed: code={} msg={:?}",
            r1_resp.error_code, r1_resp.error_message
        )));
    }

    let client_final = client
        .step(&r1_resp.auth_bytes)
        .map_err(|e| io::Error::other(format!("scram step: {e:?}")))?;
    let mut body = BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(client_final),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) encode: {e}")))?;
    let r2 = round_trip(&mut stream, 36, 2, 4, true, &body).await?;
    let mut cur: &[u8] = &r2;
    let r2_resp = SaslAuthenticateResponse::decode(&mut cur, 2)
        .map_err(|e| io::Error::other(format!("SaslAuthenticate(2) decode: {e}")))?;
    if r2_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SCRAM round 2 failed: code={} msg={:?}",
            r2_resp.error_code, r2_resp.error_message
        )));
    }
    client
        .verify_server_final(&r2_resp.auth_bytes)
        .map_err(|e| io::Error::other(format!("server-final verify: {e:?}")))?;

    Ok(stream)
}

// ─────────────────────────────────────────────────────────────────────────────
// Delegation-token wire helpers — encode one request at the negotiated MAX
// version (the broker advertises 1..=3 for Create/Describe and 1..=2 for
// Renew/Expire). Each helper takes an already-authenticated stream and a
// monotonic `corr_id`.
// ─────────────────────────────────────────────────────────────────────────────

/// Newest CreateDelegationToken supported by Crabka (Apache Kafka v3,
/// flexible). Picking MAX_VERSION here exercises the same wire shape the
/// JVM admin client would use against a modern broker.
const CREATE_DT_VERSION: i16 = crabka_protocol::owned::create_delegation_token_request::MAX_VERSION;
const RENEW_DT_VERSION: i16 = crabka_protocol::owned::renew_delegation_token_request::MAX_VERSION;
const EXPIRE_DT_VERSION: i16 = crabka_protocol::owned::expire_delegation_token_request::MAX_VERSION;
const DESCRIBE_DT_VERSION: i16 =
    crabka_protocol::owned::describe_delegation_token_request::MAX_VERSION;

async fn send_create_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &CreateDelegationTokenRequest,
) -> Result<CreateDelegationTokenResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("CreateDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(stream, 38, CREATE_DT_VERSION, corr_id, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateDelegationTokenResponse::decode(&mut cur, CREATE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("CreateDelegationToken decode: {e}")))
}

async fn send_renew_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &RenewDelegationTokenRequest,
) -> Result<RenewDelegationTokenResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, RENEW_DT_VERSION)
        .map_err(|e| io::Error::other(format!("RenewDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(stream, 39, RENEW_DT_VERSION, corr_id, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    RenewDelegationTokenResponse::decode(&mut cur, RENEW_DT_VERSION)
        .map_err(|e| io::Error::other(format!("RenewDelegationToken decode: {e}")))
}

async fn send_expire_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &ExpireDelegationTokenRequest,
) -> Result<ExpireDelegationTokenResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, EXPIRE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("ExpireDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(stream, 40, EXPIRE_DT_VERSION, corr_id, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    ExpireDelegationTokenResponse::decode(&mut cur, EXPIRE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("ExpireDelegationToken decode: {e}")))
}

async fn send_describe_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &DescribeDelegationTokenRequest,
) -> Result<DescribeDelegationTokenResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(stream, 41, DESCRIBE_DT_VERSION, corr_id, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DescribeDelegationTokenResponse::decode(&mut cur, DESCRIBE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeDelegationToken decode: {e}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boot a single-broker SASL_PLAINTEXT cluster wired for the full
/// KIP-48 lifecycle:
///   - PLAIN credentials for `alice` + `bob`
///   - both PLAIN and SCRAM-SHA-256 enabled on the listener (PLAIN for the
///     human-user handshakes, SCRAM-SHA-256 for the token-fallback path)
///   - `delegation_token_secret_key = Some("e2e-master-key")` — gates the
///     four delegation-token RPCs and the SCRAM token-fallback lookup
async fn start_broker() -> (BrokerHandle, TempDir, SocketAddr) {
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
        .insert("alice".to_string(), "wonderland".to_string());
    cfg.plain_credentials
        .insert("bob".to_string(), "builder".to_string());
    // Inter-broker auth uses PLAIN as alice (the cluster only has one broker
    // so this is not exercised, but `BrokerConfig::validate` requires it
    // when the inter-broker listener is SASL).
    cfg.inter_broker_credentials = Some(crabka_broker::config::InterBrokerCredentials {
        mechanism: SaslMechanism::Plain,
        username: "alice".to_string(),
        password: "wonderland".to_string(),
    });
    cfg.delegation_token_secret_key = Some(SecretBytes::new(b"e2e-master-key".to_vec()));
    // Broker ceiling = 7 days (Kafka default). Note: the
    // `CreateDelegationToken` handler sets both `expiry_timestamp_ms`
    // AND `max_timestamp_ms` to `now + chosen_lifetime`, so the Renew
    // step's `min(now + renew_period_ms, max_timestamp_ms)` clamps the
    // renewed expiry to the original — Renew can never push expiry
    // beyond the original ceiling. The lifecycle test compensates by
    // asserting Renew SUCCEEDED + returned a positive timestamp, rather
    // than strict-monotonic extension.
    cfg.delegation_token_max_lifetime_ms = 7 * 24 * 60 * 60 * 1_000;

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// The lifecycle test.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn delegation_token_lifecycle_end_to_end() {
    let (handle, _dir, addr) = start_broker().await;

    let result: Result<(), String> = async {
        // ── (a) alice authenticates over SASL/PLAIN.
        let mut alice = sasl_plain_authenticate(addr, "alice", b"wonderland")
            .await
            .map_err(|e| format!("alice PLAIN auth: {e}"))?;

        // ── (b) alice mints a delegation token, with bob as a renewer.
        //         `max_lifetime_ms = -1` → broker uses its ceiling.
        let create_req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            renewers: vec![CreatableRenewers {
                principal_type: "User".to_string(),
                principal_name: "bob".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let create_resp = send_create_delegation_token(&mut alice, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(alice): {e}"))?;
        if create_resp.error_code != 0 {
            return Err(format!(
                "Create failed: code={} principal={}:{} requester={}:{}",
                create_resp.error_code,
                create_resp.principal_type,
                create_resp.principal_name,
                create_resp.token_requester_principal_type,
                create_resp.token_requester_principal_name,
            ));
        }
        assert_eq!(create_resp.principal_type, "User");
        assert_eq!(create_resp.principal_name, "alice");
        assert_eq!(create_resp.token_requester_principal_type, "User");
        assert_eq!(create_resp.token_requester_principal_name, "alice");
        assert!(!create_resp.token_id.is_empty(), "token_id must be set");
        // HMAC-SHA-256 → 32 raw bytes.
        assert_eq!(create_resp.hmac.len(), 32, "HMAC length must be 32 bytes");
        assert!(create_resp.expiry_timestamp_ms > create_resp.issue_timestamp_ms);

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();
        let original_expiry_ms = create_resp.expiry_timestamp_ms;

        // Wait briefly for the V1DelegationToken record to apply on this
        // node's image — every subsequent step reads it back via the same
        // controller, so the visibility window is tiny but non-zero.
        let img_token = wait_for_token(&handle, &token_id).await;
        assert_eq!(img_token.owner.principal_type, "User");
        assert_eq!(img_token.owner.name, "alice");
        assert_eq!(
            img_token.renewers.len(),
            1,
            "renewers must carry exactly the requested entry"
        );
        assert_eq!(img_token.renewers[0].principal_type, "User");
        assert_eq!(img_token.renewers[0].name, "bob");

        // ── (c) Open a second connection and SASL/SCRAM-SHA-256 authenticate
        //         with username=token_id, password=base64(hmac). KIP-48
        //         token-fallback in `handle_authenticate_scram` is what makes
        //         this succeed; without it the broker would respond
        //         "unknown user" at round 1.
        let token_password = base64::engine::general_purpose::STANDARD.encode(&hmac_bytes);
        let mut tokenuser = sasl_scram_sha256_authenticate(addr, &token_id, &token_password)
            .await
            .map_err(|e| format!("token SCRAM auth: {e}"))?;

        // ── (d) From the token-authed connection, Create must fail with
        //         DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64). This is the
        //         load-bearing oracle for the principal-override check —
        //         that error is only reachable when the broker sees this
        //         session as `authenticated_via_token = true`, which is set
        //         in the same branch that overrides the principal back to
        //         the token's owner (here, alice). If the override regressed
        //         and the principal stayed as the token_id, the request
        //         would fail with INVALID_REQUEST (or be authorized as a
        //         brand-new user). 64 is the unambiguous proof.
        let create_via_token = send_create_delegation_token(
            &mut tokenuser,
            200,
            &CreateDelegationTokenRequest {
                max_lifetime_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("CreateDelegationToken(token-auth): {e}"))?;
        assert_eq!(
            create_via_token.error_code, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); \
             got {} — principal override may have regressed",
            create_via_token.error_code,
        );

        // ── (e) Third connection: bob (a listed renewer) calls Renew.
        //         Renew authorization (owner OR renewer) is what's load-bearing
        //         here — the absolute expiry value is constrained by
        //         `min(now + renew_period_ms, max_timestamp_ms)`, and since
        //         the broker sets `max_timestamp_ms == expiry_timestamp_ms`
        //         at create, Renew clamps to the original expiry. The
        //         renewer-authorization gate is what's untested without this
        //         step.
        let mut bob = sasl_plain_authenticate(addr, "bob", b"builder")
            .await
            .map_err(|e| format!("bob PLAIN auth: {e}"))?;
        // Use a huge renew period so the clamp lands at `max_timestamp_ms`
        // (≈ original expiry) regardless of wall-clock drift between
        // Create and Renew.
        let renew_resp = send_renew_delegation_token(
            &mut bob,
            300,
            &RenewDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                renew_period_ms: 30 * 24 * 60 * 60 * 1_000, // 30d (> 7d ceiling)
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(bob): {e}"))?;
        assert_eq!(
            renew_resp.error_code, 0,
            "Renew by listed renewer must succeed; got {}",
            renew_resp.error_code,
        );
        assert!(
            renew_resp.expiry_timestamp_ms > 0,
            "Renew must return a positive expiry; got {}",
            renew_resp.expiry_timestamp_ms,
        );
        // The clamp pins renewed expiry to the original `max_timestamp_ms`
        // (which the Create handler sets equal to the original
        // `expiry_timestamp_ms`); this round-trips the value exactly.
        assert_eq!(
            renew_resp.expiry_timestamp_ms, original_expiry_ms,
            "Renew must clamp to original max_timestamp_ms ({}); got {}",
            original_expiry_ms, renew_resp.expiry_timestamp_ms,
        );

        // ── (f) alice describes with an explicit owner filter — should see
        //         exactly the one token she owns.
        let describe_resp = send_describe_delegation_token(
            &mut alice,
            400,
            &DescribeDelegationTokenRequest {
                owners: Some(vec![DescribeDelegationTokenOwner {
                    principal_type: "User".to_string(),
                    principal_name: "alice".to_string(),
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("DescribeDelegationToken(alice): {e}"))?;
        assert_eq!(
            describe_resp.error_code, 0,
            "Describe must succeed; got {}",
            describe_resp.error_code,
        );
        assert_eq!(
            describe_resp.tokens.len(),
            1,
            "alice must see exactly her one token; got {} entries",
            describe_resp.tokens.len(),
        );
        assert_eq!(describe_resp.tokens[0].token_id, token_id);
        assert_eq!(describe_resp.tokens[0].principal_type, "User");
        assert_eq!(describe_resp.tokens[0].principal_name, "alice");

        // ── (g) alice expires the token (negative period = immediate delete).
        let expire_resp = send_expire_delegation_token(
            &mut alice,
            500,
            &ExpireDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(alice): {e}"))?;
        assert_eq!(
            expire_resp.error_code, 0,
            "Expire must succeed; got {}",
            expire_resp.error_code,
        );

        // Drop the still-open connections we used for the wire dance —
        // they'd otherwise sit around until the test ends.
        drop(alice);
        drop(bob);
        drop(tokenuser);

        // Wait for the tombstone to apply, so the SCRAM credential lookup
        // in step (h) sees a fully-removed token.
        wait_for_token_gone(&handle, &token_id).await;

        // ── (h) Fourth connection: SCRAM auth with the same token creds
        //         must now fail (the token is gone). `sasl_scram_sha256_authenticate`
        //         surfaces the failure either as a non-zero error_code on
        //         round 1 (the credential lookup misses → "unknown user")
        //         or as an EOF / connection close.
        let fresh_attempt = sasl_scram_sha256_authenticate(addr, &token_id, &token_password).await;
        assert!(
            fresh_attempt.is_err(),
            "SCRAM with the expired token's creds must fail; got Ok"
        );

        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("delegation-token lifecycle failed: {msg}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Image-watch helpers. `submit_change` returns once the record is replicated
// to the controller's state machine, but the listener's `MetadataImage` is
// served through the same controller handle, so a tight poll converges
// within a few ms.
// ─────────────────────────────────────────────────────────────────────────────

async fn wait_for_token(handle: &BrokerHandle, token_id: &str) -> crabka_metadata::DelegationToken {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        if let Some(t) = img.delegation_token_by_id(token_id) {
            return t.clone();
        }
        if std::time::Instant::now() > deadline {
            panic!("token {token_id} not visible in image after 5s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_token_gone(handle: &BrokerHandle, token_id: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        if img.delegation_token_by_id(token_id).is_none() {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!("token {token_id} still visible in image after 5s (expected tombstone)");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}
