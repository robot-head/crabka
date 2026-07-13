// rustc 1.95 clippy ICEs on this file family (same as throttle.rs /
// describe_user_scram_credentials.rs). Suppress locally; the workspace
// lint gate still applies elsewhere.

//! KIP-48 end-to-end integration: full delegation-token lifecycle
//! against a single-broker test cluster. Spec §8.2.
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
//!       the `token_id`. We assert this by re-running `CreateDelegationToken`
//!       on the token-authed connection and observing
//!       `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64) — that error is only
//!       reachable when the broker correctly sees this session as
//!       `authenticated_via_token = true`, which is set together with the
//!       owner-principal override.
//!   (d) Same connection: re-`CreateDelegationToken` → expect 64.
//!   (e) Third TCP connection; SASL/PLAIN as `bob`; `RenewDelegationToken`
//!       with the captured HMAC. Expect `error_code = 0` (the
//!       renewer-authorization gate accepts the listed renewer). Per
//!       KIP-48, the create handler sets `expiry_timestamp_ms = now +
//!       min(default_renew_period, chosen_lifetime)` and
//!       `max_timestamp_ms = now + chosen_lifetime` as SEPARATE values,
//!       so a Renew with a large `renew_period_ms` extends the expiry
//!       strictly beyond its initial value — up to (but not past)
//!       `max_timestamp_ms`.
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

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use base64::Engine;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle, config::ListenerSpec};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
        create_delegation_token_response::CreateDelegationTokenResponse,
        describe_delegation_token_request::{
            DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
        },
        describe_delegation_token_response::DescribeDelegationTokenResponse,
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism, SecretBytes};

/// Canonical Kafka error code mirroring `crabka_broker::codes::
/// DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`. The broker's `codes` module is
/// private to the crate, so we keep a local copy — kept in sync with
/// `crates/broker/src/codes.rs` and the Apache Kafka error table.
const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
/// Canonical Kafka error code mirroring `crabka_broker::codes::
/// DELEGATION_TOKEN_AUTHORIZATION_FAILED`. Same kept-in-sync rule.
const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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

    let client = crabka_security::ScramClientExchange::new(
        username.to_string(),
        password.as_bytes().to_vec(),
        SaslMechanism::ScramSha256,
    );
    let (client_first, client) = client
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

    let (client_final, client) = client
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

/// Newest `CreateDelegationToken` supported by Crabka (Apache Kafka v3,
/// flexible). Picking `MAX_VERSION` here exercises the same wire shape the
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

/// Boot a single-broker `SASL_PLAINTEXT` cluster wired for the full
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
    cfg.inter_broker_credentials = Some(crabka_broker::config::InterBrokerCredentials::Plain {
        username: "alice".to_string(),
        password: "wonderland".to_string(),
    });
    cfg.delegation_token_secret_key = Some(SecretBytes::new(b"e2e-master-key".to_vec()));
    // KIP-48 distinguishes the absolute ceiling (`max_lifetime_ms` →
    // `max_timestamp_ms`) from the initial renew window (`default_renew_period`
    // → `expiry_timestamp_ms`). With 7d ceiling + 24h renew period (both
    // the Kafka defaults), the create handler emits expiry = issue + 24h
    // and max = issue + 7d as separate values, so Renew can extend the
    // expiry well past its initial value (and the lifecycle test asserts
    // strict-monotonic extension below).
    cfg.delegation_token_max_lifetime_ms = 7 * 24 * 60 * 60 * 1_000;
    cfg.delegation_token_default_renew_period_ms = 24 * 60 * 60 * 1_000;

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Act-as variant: boot a single-broker `SASL_PLAINTEXT` cluster with
/// caller-specified PLAIN credentials and a caller-specified set of super-users.
///
/// `plain_creds` is `&[(username, password)]`. `super_users` is `&[username]`
/// — names listed here are inserted into `BrokerConfig.super_users` and bypass
/// ACL checks (in particular, they're the only callers allowed to set
/// `owner_principal_*` on `CreateDelegationToken` per spec §1).
///
/// Same protocol surface as `start_broker`: PLAIN + SCRAM-SHA-256 enabled,
/// master delegation-token key set, 7d ceiling / 24h default renew period.
async fn start_broker_with_super_users(
    plain_creds: &[(&str, &str)],
    super_users: &[&str],
) -> (BrokerHandle, TempDir, SocketAddr) {
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
    for (user, password) in plain_creds {
        cfg.plain_credentials
            .insert((*user).to_string(), (*password).to_string());
    }
    for user in super_users {
        cfg.super_users.insert((*user).to_string());
    }
    // Inter-broker auth uses PLAIN as the first listed user. `BrokerConfig::
    // validate` requires inter-broker credentials when the inter-broker
    // listener is SASL, even though a single-broker cluster never opens an
    // inter-broker connection.
    let (ib_user, ib_pw) = plain_creds
        .first()
        .expect("must supply at least one PLAIN credential for inter-broker auth");
    cfg.inter_broker_credentials = Some(crabka_broker::config::InterBrokerCredentials::Plain {
        username: (*ib_user).to_string(),
        password: (*ib_pw).to_string(),
    });
    cfg.delegation_token_secret_key = Some(SecretBytes::new(b"act-as-master-key".to_vec()));
    cfg.delegation_token_max_lifetime_ms = 7 * 24 * 60 * 60 * 1_000;
    cfg.delegation_token_default_renew_period_ms = 24 * 60 * 60 * 1_000;

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

// ─────────────────────────────────────────────────────────────────────────────
// The lifecycle test.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
        check!(create_resp.principal_type == "User");
        check!(create_resp.principal_name == "alice");
        check!(create_resp.token_requester_principal_type == "User");
        check!(create_resp.token_requester_principal_name == "alice");
        check!(!create_resp.token_id.is_empty(), "token_id must be set");
        // HMAC-SHA-256 → 32 raw bytes.
        check!(create_resp.hmac.len() == 32, "HMAC length must be 32 bytes");
        check!(create_resp.expiry_timestamp_ms > create_resp.issue_timestamp_ms);

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();
        // Capture both timestamps: with the KIP-48 fix, Renew must extend
        // `expiry_timestamp_ms` strictly past `create_resp.expiry_timestamp_ms`
        // but never push it past `create_resp.max_timestamp_ms`.
        let initial_expiry_ms = create_resp.expiry_timestamp_ms;
        let max_timestamp_ms = create_resp.max_timestamp_ms;
        assert!(
            initial_expiry_ms < max_timestamp_ms,
            "KIP-48 separation invariant: initial expiry ({initial_expiry_ms}) must be strictly \
             less than max ({max_timestamp_ms}) when default_renew_period < max_lifetime",
        );

        // Wait briefly for the V1DelegationToken record to apply on this
        // node's image — every subsequent step reads it back via the same
        // controller, so the visibility window is tiny but non-zero.
        let img_token = wait_for_token(&handle, &token_id).await;
        check!(img_token.owner.principal_type == "User");
        check!(img_token.owner.name == "alice");
        assert!(
            img_token.renewers.len() == 1,
            "renewers must carry exactly the requested entry"
        );
        check!(img_token.renewers[0].principal_type == "User");
        check!(img_token.renewers[0].name == "bob");

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
        assert!(
            create_via_token.error_code == DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); \
             got {} — principal override may have regressed",
            create_via_token.error_code
        );

        // ── (e) Third connection: bob (a listed renewer) calls Renew.
        //         Renew authorization (owner OR renewer) is what's load-bearing
        //         here. With the KIP-48 fix, Create sets
        //         `expiry_timestamp_ms = issue + 24h` and
        //         `max_timestamp_ms = issue + 7d` as SEPARATE values, so
        //         `min(now + renew_period_ms, max_timestamp_ms)` actually
        //         advances the expiry — bounded above by `max_timestamp_ms`.
        let mut bob = sasl_plain_authenticate(addr, "bob", b"builder")
            .await
            .map_err(|e| format!("bob PLAIN auth: {e}"))?;
        // Use a huge renew period so the clamp lands at `max_timestamp_ms`
        // regardless of wall-clock drift between Create and Renew.
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
        check!(
            renew_resp.error_code == 0,
            "Renew by listed renewer must succeed; got {}",
            renew_resp.error_code
        );
        // KIP-48: with the fix, Renew strictly extends the expiry past
        // its initial value, capped at `max_timestamp_ms`.
        check!(
            renew_resp.expiry_timestamp_ms > initial_expiry_ms,
            "Renew must strictly extend expiry past initial value: \
             renewed={} initial={}",
            renew_resp.expiry_timestamp_ms,
            initial_expiry_ms,
        );
        check!(
            renew_resp.expiry_timestamp_ms <= max_timestamp_ms,
            "Renew must never push expiry past max_timestamp_ms: \
             renewed={} max={}",
            renew_resp.expiry_timestamp_ms,
            max_timestamp_ms,
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
        check!(
            describe_resp.error_code == 0,
            "Describe must succeed; got {}",
            describe_resp.error_code
        );
        assert!(
            describe_resp.tokens.len() == 1,
            "alice must see exactly her one token; got {} entries",
            describe_resp.tokens.len()
        );
        check!(describe_resp.tokens[0].token_id == token_id);
        check!(describe_resp.tokens[0].principal_type == "User");
        check!(describe_resp.tokens[0].principal_name == "alice");

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
        assert!(
            expire_resp.error_code == 0,
            "Expire must succeed; got {}",
            expire_resp.error_code
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
    // Watch the committed metadata image (same controller handle
    // `controller_image_for_test` reads) until the V1DelegationToken record
    // materializes, then re-read it to return the applied token.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_some())
        .await;
    handle
        .controller_image_for_test()
        .delegation_token_by_id(token_id)
        .expect("token present in image after wait_for_image")
        .clone()
}

async fn wait_for_token_gone(handle: &BrokerHandle, token_id: &str) {
    // Watch the committed metadata image until the token's tombstone applies.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_none())
        .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Act-as wire-path tests (spec §3.1). These exercise the
// `owner_principal_type` + `owner_principal_name` request fields on
// `CreateDelegationTokenRequest` (v3+), which let a super-user mint a token
// owned by *another* principal. Implemented in
// `handlers/create_delegation_token.rs`; these are the integration-level
// oracles for that wire path.
// ─────────────────────────────────────────────────────────────────────────────

/// Spec §3.1 test 1.
///
/// Super-user `admin` mints a delegation token owned by `alice` (act-as).
/// Verifies:
///   - request succeeds (`error_code = 0`)
///   - response `principal_*` reflects the OWNER (`User:alice`)
///   - response `token_requester_*` reflects the CALLER (`User:admin`)
///   - a second SCRAM-token-authed connection is correctly tagged
///     `authenticated_via_token = true` — proven by re-Create returning
///     `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64), which only fires on
///     token-authed sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_super_user_mints_token_owned_by_target() {
    let (handle, _dir, addr) =
        start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"])
            .await;

    let result: Result<(), String> = async {
        // (1) admin authenticates via SASL/PLAIN.
        let mut admin = sasl_plain_authenticate(addr, "admin", b"admin-pw")
            .await
            .map_err(|e| format!("admin PLAIN auth: {e}"))?;

        // (2) admin mints a token owned by alice. owner_principal_type=User,
        // owner_principal_name=alice, empty renewers, broker-chosen lifetime.
        let create_req = CreateDelegationTokenRequest {
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("alice".to_string()),
            max_lifetime_ms: -1,
            renewers: vec![],
            ..Default::default()
        };
        let create_resp = send_create_delegation_token(&mut admin, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(admin act-as alice): {e}"))?;
        if create_resp.error_code != 0 {
            return Err(format!(
                "act-as Create must succeed; got code={} principal={}:{} requester={}:{}",
                create_resp.error_code,
                create_resp.principal_type,
                create_resp.principal_name,
                create_resp.token_requester_principal_type,
                create_resp.token_requester_principal_name,
            ));
        }
        check!(create_resp.principal_type == "User");
        check!(create_resp.principal_name == "alice");
        check!(create_resp.token_requester_principal_type == "User");
        check!(create_resp.token_requester_principal_name == "admin");
        check!(!create_resp.token_id.is_empty(), "token_id must be set");
        check!(create_resp.hmac.len() == 32, "HMAC length must be 32 bytes");

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();

        // Wait for the V1DelegationToken record to replicate to this node's
        // image. Belt-and-suspenders — the SCRAM token-fallback lookup in
        // step (3) reads from the same image.
        let img_token = wait_for_token(&handle, &token_id).await;
        assert!(img_token.owner.principal_type == "User");
        assert!(img_token.owner.name == "alice");

        // (3) Open a second connection; SASL/SCRAM-SHA-256 with username =
        // token_id, password = base64(hmac). The token-fallback path
        // authenticates this session as the token's OWNER — alice.
        let token_password = base64::engine::general_purpose::STANDARD.encode(&hmac_bytes);
        let mut tokenuser = sasl_scram_sha256_authenticate(addr, &token_id, &token_password)
            .await
            .map_err(|e| format!("token SCRAM auth: {e}"))?;

        // (4) Re-Create from the token-authed connection MUST return 64
        // (DELEGATION_TOKEN_REQUEST_NOT_ALLOWED). This is the unambiguous
        // oracle that the broker tagged this session as
        // `authenticated_via_token = true` AND set the principal back to the
        // token's owner. If either flag/override regressed, the request
        // would either succeed (wrong) or fail with a different error.
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
        assert!(
            create_via_token.error_code == DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
            "token-authed Create must return DELEGATION_TOKEN_REQUEST_NOT_ALLOWED (64); \
             got {}",
            create_via_token.error_code
        );

        drop(admin);
        drop(tokenuser);
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("act-as super-user mint test failed: {msg}");
    }
}

/// Spec §3.1 test 2.
///
/// Non-super-user `alice` attempts to act-as: requests a token owned by
/// `bob`. Must be rejected with `DELEGATION_TOKEN_AUTHORIZATION_FAILED` (65).
/// This is the load-bearing authorization gate for act-as — without it,
/// any authenticated user could mint tokens impersonating any other user.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn act_as_non_super_user_rejected_with_authorization_failed() {
    let (handle, _dir, addr) = start_broker_with_super_users(&[("alice", "alice-pw")], &[]).await;

    let result: Result<(), String> = async {
        let mut alice = sasl_plain_authenticate(addr, "alice", b"alice-pw")
            .await
            .map_err(|e| format!("alice PLAIN auth: {e}"))?;

        let create_req = CreateDelegationTokenRequest {
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("bob".to_string()),
            max_lifetime_ms: -1,
            renewers: vec![],
            ..Default::default()
        };
        let resp = send_create_delegation_token(&mut alice, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(alice act-as bob): {e}"))?;
        assert!(
            resp.error_code == DELEGATION_TOKEN_AUTHORIZATION_FAILED,
            "non-super-user act-as must be rejected with \
             DELEGATION_TOKEN_AUTHORIZATION_FAILED (65); got {}",
            resp.error_code
        );

        drop(alice);
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("act-as non-super-user reject test failed: {msg}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Super-user bypass (Renew + Expire).
//
// The Renew/Expire handlers originally gated on `caller == owner || caller in
// renewers` only. With operator-driven token issuance, the operator (a
// super-user) was unable to renew/expire tokens it minted via act-as on
// behalf of `KafkaUser` principals, because it was neither owner nor
// renewer. The super-user fast path that Kafka's
// `DelegationTokenManager.isAuthorizedToOperateOnToken` includes fixes this.
//
// This integration test exercises the wire path end-to-end: admin act-as
// mints a token owned by alice, then admin Renews and Expires it — both
// must succeed (no err 63 / 65).
// ─────────────────────────────────────────────────────────────────────────────

/// Super-user-bypass regression / spec §1.3 + §1.4.
///
/// Super-user `admin` mints a token owned by `alice` via act-as,
/// then renews + expires it via the wire. Both must
/// succeed despite admin being neither owner nor renewer. Mirrors the
/// kind-kafkauser-delegation-token e2e flow that was red on main with
/// `RenewDelegationToken: UNKNOWN (63)` before this fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn super_user_can_renew_other_owners_token() {
    let (handle, _dir, addr) =
        start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"])
            .await;

    let result: Result<(), String> = async {
        // (1) admin authenticates via SASL/PLAIN.
        let mut admin = sasl_plain_authenticate(addr, "admin", b"admin-pw")
            .await
            .map_err(|e| format!("admin PLAIN auth: {e}"))?;

        // (2) admin act-as mints a token owned by alice — no renewers.
        // This is exactly what the operator does for a
        // delegation-token `KafkaUser`.
        let create_req = CreateDelegationTokenRequest {
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("alice".to_string()),
            max_lifetime_ms: -1,
            renewers: vec![],
            ..Default::default()
        };
        let create_resp = send_create_delegation_token(&mut admin, 100, &create_req)
            .await
            .map_err(|e| format!("CreateDelegationToken(admin act-as alice): {e}"))?;
        if create_resp.error_code != 0 {
            return Err(format!(
                "act-as Create must succeed; got code={}",
                create_resp.error_code,
            ));
        }
        assert!(create_resp.principal_name == "alice");

        let token_id = create_resp.token_id.clone();
        let hmac_bytes = create_resp.hmac.clone();
        let initial_expiry_ms = create_resp.expiry_timestamp_ms;
        let max_timestamp_ms = create_resp.max_timestamp_ms;
        assert!(
            initial_expiry_ms < max_timestamp_ms,
            "KIP-48 separation invariant must hold so Renew has room to extend"
        );

        // Wait for the V1DelegationToken record to apply on this node's image.
        let img_token = wait_for_token(&handle, &token_id).await;
        assert!(img_token.owner.name == "alice");
        assert!(
            img_token.renewers.is_empty(),
            "no renewers were specified, so admin is neither owner NOR renewer"
        );

        // (3) admin Renews — this is the operator's renewal
        // path. Without the super-user bypass, this returned err 63
        // (DELEGATION_TOKEN_OWNER_MISMATCH); with the super-user bypass,
        // it must succeed and strictly extend the expiry.
        let renew_resp = send_renew_delegation_token(
            &mut admin,
            200,
            &RenewDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                renew_period_ms: 30 * 24 * 60 * 60 * 1_000, // 30d (> 7d ceiling → clamps to max)
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("RenewDelegationToken(admin super-user): {e}"))?;
        check!(
            renew_resp.error_code == 0,
            "super-user Renew of another owner's token must succeed; got {} \
             (super-user bypass regressed)",
            renew_resp.error_code
        );
        check!(
            renew_resp.expiry_timestamp_ms > initial_expiry_ms,
            "Renew must strictly extend expiry: renewed={} initial={}",
            renew_resp.expiry_timestamp_ms,
            initial_expiry_ms,
        );
        check!(
            renew_resp.expiry_timestamp_ms <= max_timestamp_ms,
            "Renew must never push expiry past max_timestamp_ms",
        );

        // (4) admin Expires (tombstone path) — this is the
        // operator's finalizer path on KafkaUser delete.
        let expire_resp = send_expire_delegation_token(
            &mut admin,
            300,
            &ExpireDelegationTokenRequest {
                hmac: hmac_bytes.clone(),
                expiry_time_period_ms: -1,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("ExpireDelegationToken(admin super-user): {e}"))?;
        assert!(
            expire_resp.error_code == 0,
            "super-user Expire of another owner's token must succeed; got {} \
             (super-user bypass regressed)",
            expire_resp.error_code
        );

        // Tombstone should propagate.
        drop(admin);
        wait_for_token_gone(&handle, &token_id).await;
        Ok(())
    }
    .await;

    handle.shutdown().await;
    if let Err(msg) = result {
        panic!("super-user renew/expire bypass test failed: {msg}");
    }
}
