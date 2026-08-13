//! Broker-side integration tests for KIP-599 `controller_mutation_rate`.
//!
//! Tests:
//! 1. `controller_mutation_rate_throttles_create_topics`. Set rate=2.0 for
//!    alice. Let one strict request cross the limit, then assert the next is
//!    rejected with `THROTTLING_QUOTA_EXCEEDED`.
//! 2. `unthrottled_create_topics_unaffected`. No quota. Create a topic.
//!    Assert `throttle_time_ms` == 0.
//! 3. `controller_mutation_rate_throttles_delete_topics`. Pre-create a topic
//!    with 10 partitions. Set rate=2.0 for alice. Alice deletes. Assert
//!    `throttle_time_ms` > 0.

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use crabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

type QuotaEntity = Vec<(String, Option<String>)>;
type QuotaOperations = Vec<(String, f64, bool)>;
type QuotaEntries = Vec<(QuotaEntity, QuotaOperations)>;

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
    let client_id = "crabka-mutation-quota-test";
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
// Cluster setup helpers. Copied from `client_quotas.rs`.
// ─────────────────────────────────────────────────────────────────────────────

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> impl std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
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
    cfg.super_users = std::iter::once(super_user.to_string()).collect();

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for AlterClientQuotas. Copied from `client_quotas.rs`.
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: QuotaEntries,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    use crabka_protocol::owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    };

    const VERSION: i16 = 1; // flexible

    let req = AlterClientQuotasRequest {
        entries: entries
            .into_iter()
            .map(|(entity_parts, ops)| EntryData {
                entity: entity_parts
                    .into_iter()
                    .map(|(entity_type, entity_name)| EntityData {
                        entity_type,
                        entity_name,
                        ..Default::default()
                    })
                    .collect(),
                ops: ops
                    .into_iter()
                    .map(|(key, value, remove)| OpData {
                        key,
                        value,
                        remove,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        validate_only,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode AlterClientQuotas");
    let resp_bytes = round_trip(&mut stream, 49, VERSION, 1, true, &body)
        .await
        .expect("AlterClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterClientQuotasResponse::decode(&mut cur, VERSION)
        .expect("decode AlterClientQuotasResponse");

    resp.entries
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            (entity, e.error_code)
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for CreateTopics and DeleteTopics.
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `CreateTopics` (`api_key=19`) over a SASL/PLAIN connection.
/// Returns `(throttle_time_ms, per-topic error_code)` from the first result.
async fn drive_create_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
    partitions: i32,
) -> (i32, i16) {
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    const VERSION: i16 = 7; // MAX_VERSION; flexible (>= 5)

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 30_000,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, VERSION, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, VERSION).expect("decode CreateTopicsResponse");

    let err_code = resp.topics.first().map_or(-1, |t| t.error_code);
    (resp.throttle_time_ms, err_code)
}

/// Drive `DeleteTopics` (`api_key=20`) over a SASL/PLAIN connection.
/// Returns `(throttle_time_ms, per-topic error_code)` from the first result.
async fn drive_delete_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
) -> (i32, i16) {
    use crabka_protocol::owned::{
        delete_topics_request::DeleteTopicsRequest, delete_topics_response::DeleteTopicsResponse,
    };

    // Use version 3 (flexible=4+, topic_names field for versions 0-5).
    // Flexible starts at version 4; use version 4 to get throttle_time_ms (v1+)
    // and flexible encoding, while using topic_names (not the v6+ topics field).
    const VERSION: i16 = 4;

    let req = DeleteTopicsRequest {
        topic_names: vec![topic.to_string()],
        timeout_ms: 30_000,
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DeleteTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode DeleteTopics");
    let resp_bytes = round_trip(&mut stream, 20, VERSION, 1, true, &body)
        .await
        .expect("DeleteTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DeleteTopicsResponse::decode(&mut cur, VERSION).expect("decode DeleteTopicsResponse");

    let err_code = resp.responses.first().map_or(-1, |r| r.error_code);
    (resp.throttle_time_ms, err_code)
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: Set `controller_mutation_rate=2.0` for alice. A strict v7 request
/// may cross the limit, but the following mutation is rejected while debt
/// remains.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_create_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed an ACL granting alice Cluster Create — this also disables the
    // compat shim (allow-all when no ACLs present in image).
    let admin_acl = MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Cluster,
        resource_name: "kafka-cluster".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: AclOperation::Create,
        permission_type: PermissionType::Allow,
    });
    handle
        .submit_metadata_record_for_test(admin_acl)
        .await
        .expect("seed ACL");

    // Set controller_mutation_rate=2.0 for (user=alice).
    let alter = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("controller_mutation_rate".into(), 2.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter[0].1 == 0, "alter should succeed");

    // Wait until the controller_mutation_rate quota is committed to this
    // broker's metadata image. The CreateTopics handler reads the rate
    // straight from the image on the first consume (the bucket is created
    // lazily with that rate; the refresh task only re-rates existing buckets),
    // so image visibility — not the refresh task — is the real precondition.
    handle
        .wait_for_image(|img| {
            img.client_quotas()
                .values()
                .any(|configs| configs.contains_key("controller_mutation_rate"))
        })
        .await;

    // This operation crosses the limit but is accepted under strict quota
    // semantics because the bucket was not already exhausted.
    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "alice", "alice-secret", "throttled-topic", 10).await;
    check!(
        err_code == 0,
        "create-topics should succeed (alice has Cluster Create ACL)"
    );
    check!(throttle_ms == 0);

    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "alice", "alice-secret", "rejected-topic", 1).await;
    check!(
        err_code == crabka_broker::codes::THROTTLING_QUOTA_EXCEEDED,
        "expected strict quota rejection, got error {err_code}"
    );
    check!(
        throttle_ms > 0,
        "expected throttle_time_ms > 0, got {throttle_ms}"
    );
}

/// Test 2: No quota configured. Create a topic. Assert
/// `throttle_time_ms` == 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_create_topics_unaffected() {
    let (handle, _dir, addr) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;
    // No controller_mutation_rate quota configured.
    // admin is super_user, no ACL seeding needed.
    let _ = handle; // keep alive

    let (throttle_ms, err_code) =
        drive_create_topics_sasl(addr, "admin", "admin-secret", "unthrottled-topic", 10).await;
    assert!(err_code == 0);
    assert!(throttle_ms == 0);
}

/// Test 3: Pre-create a topic as admin with no quota. Set rate=2.0 for
/// alice. Alice deletes. Assert `throttle_time_ms` > 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_delete_topics() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed a dummy ACL to disable the compat shim (allow-all when no ACLs present).
    // Use an unrelated ACL; the real alice ACLs come below.
    let shim_disable = MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "__compat_shim_disable__".into(),
        pattern_type: PatternType::Literal,
        principal: "User:admin".into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    });
    handle
        .submit_metadata_record_for_test(shim_disable)
        .await
        .expect("seed compat shim disable ACL");
    // Wait until the shim-disable ACL is visible in the metadata image so the
    // allow-all compat shim is actually off before the scenario proceeds.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "__compat_shim_disable__")
        })
        .await;

    // Pre-create topic as admin (no quota for admin) with 10 partitions.
    let (_, ec) = drive_create_topics_sasl(addr, "admin", "admin-secret", "to-delete", 10).await;
    assert!(ec == 0);

    // Grant alice Topic Delete on "to-delete".
    let alice_delete_acl = MetadataRecord::V1AccessControlEntry(AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "to-delete".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: AclOperation::Delete,
        permission_type: PermissionType::Allow,
    });
    handle
        .submit_metadata_record_for_test(alice_delete_acl)
        .await
        .expect("seed alice Delete ACL");
    // Wait until alice's Delete ACL on "to-delete" is visible in the image so
    // the later delete is authorized.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "to-delete" && a.principal == "User:alice")
        })
        .await;

    // Now set the quota for alice and delete.
    let alter = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("controller_mutation_rate".into(), 2.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter[0].1 == 0);
    // Wait for alice's controller_mutation_rate quota to land in the image
    // before deleting; DeleteTopics reads the rate from the image on consume.
    handle
        .wait_for_image(|img| {
            img.client_quotas()
                .values()
                .any(|configs| configs.contains_key("controller_mutation_rate"))
        })
        .await;

    let (throttle_ms, err_code) =
        drive_delete_topics_sasl(addr, "alice", "alice-secret", "to-delete").await;
    assert!(err_code == 0);
    assert!(
        throttle_ms > 0,
        "expected throttle_time_ms > 0, got {throttle_ms}"
    );
}
