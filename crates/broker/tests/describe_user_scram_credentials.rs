// rustc 1.95 clippy ICEs on this file (same as throttle.rs / elect_leaders.rs).
// Suppress locally; the workspace lint gate still applies elsewhere.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]

//! Broker-side integration tests for DescribeUserScramCredentials
//! (api_key 50, KIP-554 read half).
//!
//! Tests:
//! 1. `describe_all_users_round_trip` — seed alice's SCRAM credential via
//!    `submit_metadata_record_for_test`; describe with `users=None`; assert
//!    mechanism=2 (SCRAM-SHA-512) in the response.
//! 2. `describe_unknown_user_returns_error` — describe `users=[ghost]`; assert
//!    per-user `error_code = 91` (RESOURCE_NOT_FOUND).
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14/15/15b/16.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::authorizer::SimpleAclAuthorizer;
use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerHandle};
use crabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
const WIRE_MECH_SCRAM_SHA_512: i8 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers — single length-prefixed request/response exchange.
// Copied from `client_quotas.rs`.
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
    let client_id = "crabka-scram-desc-test";
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
// SASL/PLAIN wire helpers. Copied from `client_quotas.rs`.
// ─────────────────────────────────────────────────────────────────────────────

async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // 1. ApiVersions v0 (non-flexible).
    let av_req = ApiVersionsRequest::default();
    let mut av_body = BytesMut::new();
    av_req
        .encode(&mut av_body, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions encode: {e}")))?;
    let av_resp_bytes = round_trip(&mut stream, 18, 0, 1, false, &av_body).await?;
    let mut cur: &[u8] = &av_resp_bytes;
    ApiVersionsResponse::decode(&mut cur, 0)
        .map_err(|e| io::Error::other(format!("ApiVersions decode: {e}")))?;

    // 2. SaslHandshake v1 (non-flexible, mechanism="PLAIN").
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

    // 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
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
            "SaslAuthenticate failed: error_code={} message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
async fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> (BrokerHandle, TempDir, SocketAddr) {
    start_single_broker_sasl_plaintext_with_acl_authorizer(&[super_user], users).await
}

async fn start_single_broker_sasl_plaintext_with_acl_authorizer(
    super_users: &[&str],
    users: &[(&str, &str)],
) -> (BrokerHandle, TempDir, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = crabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
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
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = super_users.iter().map(|user| (*user).to_string()).collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

async fn seed_cluster_acl(handle: &BrokerHandle, principal: &str, operation: AclOperation) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: PatternType::Literal,
            principal: format!("User:{principal}"),
            host: "*".into(),
            operation,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed cluster ACL");
    handle
        .wait_for_image(|img| {
            img.matching_acls(ResourceType::Cluster, "kafka-cluster")
                .any(|entry| entry.principal == format!("User:{principal}"))
        })
        .await;
}

async fn seed_scram_credential(
    handle: &BrokerHandle,
    user: &str,
    mechanism: SaslMechanism,
    iterations: u32,
) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1ScramCredential(
            crabka_metadata::ScramCredentialRecord {
                user: user.into(),
                mechanism,
                iterations,
                salt: vec![1, 2, 3, 4],
                server_key: vec![5; 64],
                stored_key: vec![6; 64],
            },
        ))
        .await
        .expect("seed SCRAM credential");
    handle
        .wait_for_image(|img| !img.scram_credentials_for_user(user).is_empty())
        .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for DescribeUserScramCredentials
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `DescribeUserScramCredentials` (api_key=50) over a SASL/PLAIN
/// connection.
///
/// Returns `(top_level_error, per_user_rows)` where each row is
/// `(user, error_code, credential_infos)` and each credential_info is
/// `(mechanism, iterations)`.
async fn drive_describe_user_scram_credentials_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    users_filter: Option<Vec<String>>,
) -> (i16, Vec<(String, i16, Vec<(i8, i32)>)>) {
    use crabka_protocol::owned::{
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse,
    };

    let req = DescribeUserScramCredentialsRequest {
        users: users_filter.map(|v| {
            v.into_iter()
                .map(|n| UserName {
                    name: n,
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeUserScramCredentials");

    let mut body = BytesMut::new();
    req.encode(&mut body, 0)
        .expect("encode DescribeUserScramCredentials");

    let resp_bytes = round_trip(&mut stream, 50, 0, 1, true, &body)
        .await
        .expect("DescribeUserScramCredentials round-trip");

    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeUserScramCredentialsResponse::decode(&mut cur, 0)
        .expect("decode DescribeUserScramCredentialsResponse");

    let per_user: Vec<_> = resp
        .results
        .into_iter()
        .map(|r| {
            let infos: Vec<(i8, i32)> = r
                .credential_infos
                .into_iter()
                .map(|c| (c.mechanism, c.iterations))
                .collect();
            (r.user, r.error_code, infos)
        })
        .collect();

    (resp.error_code, per_user)
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: seed alice's SCRAM credential directly via
/// `submit_metadata_record_for_test`; describe with `users=None`; assert
/// mechanism=2 (SCRAM-SHA-512) appears in the response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_all_users_round_trip() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    // Seed alice's SCRAM credential directly via metadata (bypasses the
    // AlterUserScramCredentials wire path — keeps this test focused on Describe).
    let rec = crabka_metadata::MetadataRecord::V1ScramCredential(
        crabka_metadata::ScramCredentialRecord {
            user: "alice".into(),
            mechanism: crabka_security::SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1, 2, 3, 4],
            server_key: vec![5; 64],
            stored_key: vec![6; 64],
        },
    );
    handle
        .submit_metadata_record_for_test(rec)
        .await
        .expect("seed alice ScramCredential");

    // Wait for the credential to become visible in the controller image.
    handle
        .wait_for_image(|img| !img.scram_credentials_for_user("alice").is_empty())
        .await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "admin", "admin-secret", None).await;

    assert!(top_err == 0, "top-level error should be 0");

    let alice_row = per_user
        .iter()
        .find(|(u, _, _)| u == "alice")
        .expect("alice must appear in response");
    assert!(
        alice_row.1 == 0,
        "per-user error_code should be 0 for alice"
    );
    assert!(
        alice_row.2.iter().any(|(mech, _)| *mech == 2),
        "expected mechanism=2 (SCRAM-SHA-512) in credential_infos; got {:?}",
        alice_row.2,
    );
}

/// Test 2: describe a user that does not exist (`ghost`); assert that the
/// per-user row carries `error_code = 91` (RESOURCE_NOT_FOUND).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_user_returns_error() {
    let (_handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr,
        "admin",
        "admin-secret",
        Some(vec!["ghost".into()]),
    )
    .await;

    assert!(top_err == 0, "top-level error_code should be 0");

    let row = per_user
        .iter()
        .find(|(u, _, _)| u == "ghost")
        .expect("ghost must appear in response");
    assert!(
        row.1 == 91, /* RESOURCE_NOT_FOUND */
        "expected RESOURCE_NOT_FOUND (91) for unknown user ghost; got {}",
        row.1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_duplicate_requested_user_returns_single_duplicate_resource_row() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;
    seed_scram_credential(&handle, "alice", SaslMechanism::ScramSha512, 4096).await;
    seed_scram_credential(&handle, "bob", SaslMechanism::ScramSha512, 8192).await;

    let (top_err, per_user) = drive_describe_user_scram_credentials_sasl(
        addr,
        "admin",
        "admin-secret",
        Some(vec!["alice".into(), "bob".into(), "alice".into()]),
    )
    .await;

    handle.shutdown().await;
    assert!(top_err == 0, "top-level error_code should be 0");
    assert!(
        per_user.len() == 2,
        "duplicate request users collapse: {per_user:?}"
    );

    let alice_rows: Vec<_> = per_user
        .iter()
        .filter(|(user, _, _)| user == "alice")
        .collect();
    assert!(
        alice_rows.len() == 1,
        "alice should appear once: {per_user:?}"
    );
    assert!(alice_rows[0].1 == KAFKA_DUPLICATE_RESOURCE);
    assert!(alice_rows[0].2.is_empty());

    let bob = per_user
        .iter()
        .find(|(user, _, _)| user == "bob")
        .expect("distinct users remain successful");
    assert!(bob.1 == 0);
    assert!(bob.2 == vec![(WIRE_MECH_SCRAM_SHA_512, 8192)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_allows_cluster_describe_acl() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[("alice", "alice-secret")])
            .await;
    seed_cluster_acl(&handle, "alice", AclOperation::Describe).await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "alice", "alice-secret", None).await;

    handle.shutdown().await;
    assert!(top_err == 0, "Cluster Describe ACL should authorize");
    assert!(per_user.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_rejects_without_cluster_describe_acl() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[("alice", "alice-secret")])
            .await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "alice", "alice-secret", None).await;

    handle.shutdown().await;
    assert!(
        top_err == 31,
        "missing Cluster Describe ACL should be rejected"
    );
    assert!(per_user.is_empty());
}
