// rustc 1.95 clippy::pedantic ICEs on this file (the same upstream bug
// in clippy's body-analysis / doc lint pass that already triggers on
// `tests/admin_handlers.rs`). Disable pedantic locally; the rest of the
// workspace still enforces the full pedantic gate.
#![allow(clippy::pedantic)]

//! Slice 13 broker-side ACL integration tests. No Docker.
//!
//! T22 — the first of three integration test batches — drives the
//! `CreateAcls` / `DescribeAcls` / `DeleteAcls` flow over a real
//! `SASL_PLAINTEXT` listener with the wire-typed `crabka-protocol`
//! request/response codecs. The SASL framing helpers (`drive_*`,
//! `round_trip`) are copied inline rather than shared via `mod common`
//! because Rust integration tests don't easily allow sibling-module
//! reuse across files in `tests/`.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slice 12 (the SASL listener startup is fine on Windows, but keeping
//! the gate uniform avoids one-off CI matrix surprises).

#![cfg(not(target_os = "windows"))]

use std::io;
use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerConfig};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::owned::create_acls_request::{AclCreation, CreateAclsRequest};
use crabka_protocol::owned::create_acls_response::CreateAclsResponse;
use crabka_protocol::owned::delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest};
use crabka_protocol::owned::delete_acls_response::DeleteAclsResponse;
use crabka_protocol::owned::describe_acls_request::DescribeAclsRequest;
use crabka_protocol::owned::describe_acls_response::DescribeAclsResponse;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// Wire `i8` discriminants for the Kafka ACL enums. Kept inline (rather
// than imported from `crabka-broker::handlers::acl_wire`, which is
// crate-private) so the tests exercise the same byte values JVM clients
// would send. Sourced from `crates/broker/src/handlers/acl_wire.rs`.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const PATTERN_TYPE_ANY: i8 = 1;
const PATTERN_TYPE_LITERAL: i8 = 3;
const OPERATION_ANY: i8 = 1;
const OPERATION_READ: i8 = 3;
const OPERATION_WRITE: i8 = 4;
const PERMISSION_ANY: i8 = 1;
const PERMISSION_ALLOW: i8 = 3;

// API versions chosen so the request header is the flexible v2 form
// (matches what's exercised by slice 12's `drive_sasl_plain_session`
// helper for any flexible body). All three ACL APIs went flexible at v2.
const CREATE_ACLS_VERSION: i16 = 3;
const DESCRIBE_ACLS_VERSION: i16 = 3;
const DELETE_ACLS_VERSION: i16 = 3;

/// Build a `BrokerConfig` with a single `SASL_PLAINTEXT` listener, PLAIN
/// enabled, and the given super-user. The non-super-user case still
/// declares a super-user so the compat shim (zero ACLs + no super-user →
/// ALLOW) doesn't kick in — we want the authorizer to actually evaluate
/// the cluster gate.
fn sasl_plain_broker_config(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_user: Option<&str>,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_user_name = super_user.map(str::to_string);
    cfg
}

/// Shorthand for `Allow <op> on Topic LITERAL <name> for <principal> from *`.
/// Every test in this file uses literal Topic ACLs with host `*`, so the only
/// dimensions that vary per binding are `resource_name`, `principal`, and
/// `operation` — wrap them up here to keep the test bodies short.
fn topic_allow_creation(name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: name.to_string(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

/// Permissive `DescribeAclsRequest` for `Topic` — every other axis is wildcard.
fn describe_all_topic_acls() -> DescribeAclsRequest {
    DescribeAclsRequest {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: None,
        pattern_type_filter: PATTERN_TYPE_ANY,
        principal_filter: None,
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_super_user_can_provision_and_describe() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(log_dir.path(), &[("admin", "admin-secret")], Some("admin"));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Provision: Allow Read on Topic LITERAL "foo" for User:alice from *.
    let create_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("foo", "User:alice", OPERATION_READ)],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(addr, "admin", b"admin-secret", create_req)
        .await
        .expect("CreateAcls as super-user must succeed");
    assert_eq!(
        create_resp.results.len(),
        1,
        "one result per creation: {create_resp:?}"
    );
    assert_eq!(
        create_resp.results[0].error_code, 0,
        "super-user creation must return error_code=0, got {:?}",
        create_resp.results[0]
    );

    // Describe with a permissive filter (resource_type=Topic, everything
    // else any/null) — must return exactly one resource entry carrying
    // one ACL description for User:alice / Read / Allow.
    let describe_resp =
        drive_describe_acls_as_plain(addr, "admin", b"admin-secret", describe_all_topic_acls())
            .await
            .expect("DescribeAcls as super-user must succeed");
    handle.shutdown().await;

    assert_eq!(
        describe_resp.error_code, 0,
        "DescribeAcls must succeed, got {describe_resp:?}"
    );
    assert_eq!(
        describe_resp.resources.len(),
        1,
        "expected exactly one matching resource, got {:?}",
        describe_resp.resources
    );
    let resource = &describe_resp.resources[0];
    assert_eq!(resource.resource_type, RESOURCE_TYPE_TOPIC);
    assert_eq!(resource.resource_name, "foo");
    assert_eq!(resource.pattern_type, PATTERN_TYPE_LITERAL);
    assert_eq!(
        resource.acls.len(),
        1,
        "expected exactly one ACL description, got {:?}",
        resource.acls
    );
    let acl = &resource.acls[0];
    assert_eq!(acl.principal, "User:alice");
    assert_eq!(acl.host, "*");
    assert_eq!(acl.operation, OPERATION_READ);
    assert_eq!(acl.permission_type, PERMISSION_ALLOW);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_non_super_user_rejected() {
    let log_dir = tempfile::tempdir().unwrap();
    // alice is NOT the super-user. admin is configured as super-user so
    // the compat shim stays off and the cluster-Alter gate applies.
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    let req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:bob", OPERATION_READ),
            topic_allow_creation("bar", "User:carol", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let resp = drive_create_acls_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("CreateAcls request must round-trip even when denied");
    handle.shutdown().await;

    assert_eq!(resp.results.len(), 2, "one result row per creation");
    for (i, r) in resp.results.iter().enumerate() {
        assert_eq!(
            r.error_code, 31, /* CLUSTER_AUTHORIZATION_FAILED */
            "binding {i} must be denied with CLUSTER_AUTHORIZATION_FAILED, got {r:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_acls_removes_matching() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(log_dir.path(), &[("admin", "admin-secret")], Some("admin"));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Provision two ACLs (Read on "foo", Write on "bar").
    let create_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:alice", OPERATION_READ),
            topic_allow_creation("bar", "User:alice", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(addr, "admin", b"admin-secret", create_req)
        .await
        .expect("provisioning CreateAcls must succeed");
    assert_eq!(create_resp.results.len(), 2);
    for r in &create_resp.results {
        assert_eq!(r.error_code, 0, "provisioning must succeed, got {r:?}");
    }

    // Delete only the Read-on-foo binding via a precisely-targeted filter.
    let delete_req = DeleteAclsRequest {
        filters: vec![DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some("foo".to_string()),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: Some("User:alice".to_string()),
            host_filter: Some("*".to_string()),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }],
        ..Default::default()
    };
    let delete_resp = drive_delete_acls_as_plain(addr, "admin", b"admin-secret", delete_req)
        .await
        .expect("DeleteAcls must succeed");
    assert_eq!(
        delete_resp.filter_results.len(),
        1,
        "one filter result row per filter"
    );
    assert_eq!(
        delete_resp.filter_results[0].error_code, 0,
        "filter must succeed, got {:?}",
        delete_resp.filter_results[0]
    );
    let matching = &delete_resp.filter_results[0].matching_acls;
    assert_eq!(
        matching.len(),
        1,
        "exactly one ACL must match the precise filter, got {matching:?}"
    );
    assert_eq!(matching[0].resource_name, "foo");
    assert_eq!(matching[0].operation, OPERATION_READ);
    assert_eq!(matching[0].error_code, 0);

    // Describe — only the Write-on-bar binding should remain.
    let describe_resp =
        drive_describe_acls_as_plain(addr, "admin", b"admin-secret", describe_all_topic_acls())
            .await
            .expect("DescribeAcls must succeed");
    handle.shutdown().await;

    assert_eq!(describe_resp.error_code, 0);
    // Flatten all (resource, acl) pairs so the assertion doesn't depend
    // on whether the broker groups by resource or emits one resource per
    // ACL — the contract is "the deleted binding is gone, the other one
    // is still there".
    let mut surviving: Vec<(String, i8, i8)> = Vec::new();
    for r in &describe_resp.resources {
        for a in &r.acls {
            surviving.push((r.resource_name.clone(), a.operation, a.permission_type));
        }
    }
    assert_eq!(
        surviving.len(),
        1,
        "exactly one binding must remain, got {surviving:?}"
    );
    assert_eq!(
        surviving[0],
        ("bar".to_string(), OPERATION_WRITE, PERMISSION_ALLOW),
        "the surviving binding must be Write-on-bar, got {:?}",
        surviving[0]
    );
}

// ────────────────────────────────────────────────────────────────────────
// SASL/PLAIN + ACL wire helpers.
//
// Same shape as slice 12's `drive_alter_user_scram_credentials_as_plain`:
// one ApiVersions warm-up, one SaslHandshake, one SaslAuthenticate, then
// the typed ACL request. Each helper authenticates fresh on a new TCP
// stream because that's the simplest model for "a client doing one
// admin action"; reuse is unnecessary for these tests.
// ────────────────────────────────────────────────────────────────────────

async fn drive_create_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateAclsRequest,
) -> Result<CreateAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 30, CREATE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateAclsResponse::decode(&mut cur, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls decode: {e}")))
}

async fn drive_describe_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DescribeAclsRequest,
) -> Result<DescribeAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 29, DESCRIBE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DescribeAclsResponse::decode(&mut cur, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls decode: {e}")))
}

async fn drive_delete_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DeleteAclsRequest,
) -> Result<DeleteAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 31, DELETE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DeleteAclsResponse::decode(&mut cur, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls decode: {e}")))
}

/// Open a TCP stream to `addr` and drive `ApiVersions` → `SaslHandshake(PLAIN)`
/// → `SaslAuthenticate(\0user\0password)`. Returns the authenticated stream
/// for the caller to issue follow-up requests on. Mirrors the first three
/// steps of `drive_sasl_plain_session` in `auth_handlers.rs`.
async fn sasl_plain_authenticate(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
) -> Result<TcpStream, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
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
    payload.push(0); // authzid (empty)
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
            "SaslAuthenticate failed: error_code={} error_message={:?}",
            auth_resp.error_code, auth_resp.error_message
        )));
    }

    Ok(stream)
}

/// Same shape as `auth_handlers::round_trip`. Encodes a request header
/// (v1 non-flexible / v2 flexible), prepends a 4-byte length prefix,
/// writes the frame, reads one response frame and strips the response
/// header (v0 for ApiVersions or any non-flexible response, v1 with a
/// trailing tagged-fields byte for every other flexible response).
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
    let client_id = "crabka-acl-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits in i16"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0); // empty header tagged-fields byte
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
