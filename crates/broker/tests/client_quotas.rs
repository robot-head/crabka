// rustc 1.95 clippy ICEs on this file in the same places as throttle.rs /
// elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::type_complexity)]

//! Broker-side integration tests for KIP-13/124/257 client quotas.
//!
//! Tests:
//! 1. `alter_then_describe_round_trip` — AlterClientQuotas sets
//!    `(user=alice) producer_byte_rate=1024`; DescribeClientQuotas returns it.
//! 2. `producer_byte_rate_throttles_produce` — Set low producer_byte_rate for
//!    alice; produce a large payload; assert throttle_time_ms > 0.
//! 3. `consumer_byte_rate_throttles_fetch` — Set low consumer_byte_rate for
//!    alice; produce then fetch a large payload; assert throttle_time_ms > 0.
//! 4. `user_specific_overrides_user_default` — Set (user=alice)
//!    producer_byte_rate=128 AND (user=<default>) producer_byte_rate=8192;
//!    produce as alice; the tight alice-specific limit fires, not the default.
//! 5. `non_super_user_denied` — alice (no ACLs) calls AlterClientQuotas;
//!    must receive CLUSTER_AUTHORIZATION_FAILED (31) on every entry.
//! 6. `request_percentage_throttles_produce` — Set a tiny `request_percentage`
//!    (KIP-124) for alice with NO byte-rate quota; produce a small payload;
//!    assert throttle_time_ms > 0. Proves the request-quota throttle is
//!    communicated in the response (KIP-219 throttle-then-respond) and not
//!    just silently muted.
//!
//! Test 4 uses the Option B approach: user-specific overrides user-default.
//! The (user, client-id) tuple precedence is covered by unit tests in
//! `quota/lookup.rs`. The client_id plumbing gap in Produce/Fetch handlers
//! is tracked as a known limitation (deferred to a future cleanup slice).
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14/15/15b.

use assert2::assert;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::authorizer::SimpleAclAuthorizer;
use crabka_broker::config::ListenerSpec;
use crabka_broker::{Broker, BrokerHandle};
use crabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::api_versions_response::ApiVersionsResponse;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::owned::produce_response::ProduceResponse;
use crabka_protocol::owned::sasl_authenticate_request::SaslAuthenticateRequest;
use crabka_protocol::owned::sasl_authenticate_response::SaslAuthenticateResponse;
use crabka_protocol::owned::sasl_handshake_request::SaslHandshakeRequest;
use crabka_protocol::owned::sasl_handshake_response::SaslHandshakeResponse;
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers — single length-prefixed request/response exchange.
// Copied from `throttle.rs`.
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
    let client_id = "crabka-quota-test";
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
// SASL/PLAIN wire helpers. Copied from `throttle.rs`.
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
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; the default `AllowAllAuthorizer`
    // would let every AlterClientQuotas through.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Create a topic via SASL/PLAIN as admin. Asserts success.
async fn create_topic_as_admin(
    addr: SocketAddr,
    topic: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
    use crabka_protocol::owned::create_topics_response::CreateTopicsResponse;

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, "admin", b"admin-secret")
        .await
        .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, 7).expect("encode CreateTopics");
    let resp_bytes = round_trip(&mut stream, 19, 7, 1, true, &body)
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, 7).expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({topic}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Poll until `handle` sees `(topic, partition)` present in its image.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if handle.has_partition(topic, partition).await {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "partition {topic}-{partition} never appeared within 15s"
        );
        tokio::task::yield_now().await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for AlterClientQuotas and DescribeClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `AlterClientQuotas` (api_key=49) over a SASL/PLAIN connection.
///
/// `entries` is a list of `(entity_components, ops)` where:
/// - `entity_components` is `Vec<(entity_type, entity_name)>`, e.g.
///   `vec![("user".into(), Some("alice".into()))]`
/// - `ops` is `Vec<(key, value, remove)>`, e.g.
///   `vec![("producer_byte_rate".into(), 1024.0, false)]`
///
/// Returns the per-entry `(entity, error_code)` pairs.
async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: Vec<(Vec<(String, Option<String>)>, Vec<(String, f64, bool)>)>,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    use crabka_protocol::owned::alter_client_quotas_request::{
        AlterClientQuotasRequest, EntityData, EntryData, OpData,
    };
    use crabka_protocol::owned::alter_client_quotas_response::AlterClientQuotasResponse;

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

    const VERSION: i16 = 1; // flexible

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

/// Drive `DescribeClientQuotas` (api_key=48) over a SASL/PLAIN connection.
///
/// `components` is a list of `(entity_type, match_type, match_value)`:
/// - match_type: 0=EXACT, 1=DEFAULT, 2=ANY
///
/// Returns the list of `(entity, values)` pairs from the response.
async fn drive_describe_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    components: Vec<(String, i8, Option<String>)>,
    strict: bool,
) -> Vec<(Vec<(String, Option<String>)>, Vec<(String, f64)>)> {
    use crabka_protocol::owned::describe_client_quotas_request::{
        ComponentData, DescribeClientQuotasRequest,
    };
    use crabka_protocol::owned::describe_client_quotas_response::DescribeClientQuotasResponse;

    let req = DescribeClientQuotasRequest {
        components: components
            .into_iter()
            .map(|(entity_type, match_type, match_)| ComponentData {
                entity_type,
                match_type,
                match_,
                ..Default::default()
            })
            .collect(),
        strict,
        ..Default::default()
    };

    const VERSION: i16 = 1; // flexible

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeClientQuotas");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION)
        .expect("encode DescribeClientQuotas");
    let resp_bytes = round_trip(&mut stream, 48, VERSION, 1, true, &body)
        .await
        .expect("DescribeClientQuotas round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeClientQuotasResponse::decode(&mut cur, VERSION)
        .expect("decode DescribeClientQuotasResponse");

    assert!(resp.error_code == 0, "DescribeClientQuotas top-level error");

    resp.entries
        .unwrap_or_default()
        .into_iter()
        .map(|e| {
            let entity = e
                .entity
                .into_iter()
                .map(|ed| (ed.entity_type, ed.entity_name))
                .collect();
            let values = e.values.into_iter().map(|v| (v.key, v.value)).collect();
            (entity, values)
        })
        .collect()
}

/// Drive a `Produce` request over an already-authenticated SASL stream.
/// Returns the full `ProduceResponse`.
async fn drive_produce_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    topic: &str,
    record_bytes: usize,
    count: usize,
) -> ProduceResponse {
    const VERSION: i16 = 11; // flexible, supports throttle_time_ms

    let value = vec![0u8; record_bytes];
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(bytes::Bytes::copy_from_slice(&value)),
            ..Default::default()
        })
        .collect();

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).unwrap(),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip(&mut stream, 0, VERSION, 1, true, &body)
        .await
        .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse")
}

/// Drive a `Fetch` request (consumer fetch, replica_id=-1) over SASL.
/// Returns the full `FetchResponse`.
async fn drive_fetch_sasl(addr: SocketAddr, user: &str, pass: &[u8], topic: &str) -> FetchResponse {
    const VERSION: i16 = 12; // flexible, supports throttle_time_ms

    let req = FetchRequest {
        replica_id: -1, // consumer fetch (not inter-broker)
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Fetch");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Fetch");
    let resp_bytes = round_trip(&mut stream, 1, VERSION, 1, true, &body)
        .await
        .expect("Fetch round-trip");
    let mut cur: &[u8] = &resp_bytes;
    FetchResponse::decode(&mut cur, VERSION).expect("decode FetchResponse")
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper: seed a dummy ACL to disable the compat shim (allow-all when no ACLs)
// ─────────────────────────────────────────────────────────────────────────────

async fn seed_compat_shim_disable_acl(handle: &BrokerHandle) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "__compat_shim_disable__".to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:admin".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed dummy ACL to disable compat shim");
    // Small pause to absorb raft commit-then-apply gap.
    // real-time wait (not a progress poll): raft commit-then-apply settle, no local condition to poll
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Seed an ACL that allows alice to Write topic `topic`.
async fn seed_alice_write_acl(handle: &BrokerHandle, topic: &str) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: topic.to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Write,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed alice Write ACL");
    // real-time wait (not a progress poll): raft commit-then-apply settle, no local condition to poll
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Seed an ACL that allows alice to Read topic `topic`.
async fn seed_alice_read_acl(handle: &BrokerHandle, topic: &str) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: topic.to_string(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_string(),
            host: "*".to_string(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed alice Read ACL");
    // real-time wait (not a progress poll): raft commit-then-apply settle, no local condition to poll
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: `AlterClientQuotas` sets `(user=alice) producer_byte_rate=1024`;
/// the value appears in the metadata image and in `DescribeClientQuotas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_then_describe_round_trip() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Alter: set producer_byte_rate=1024 for (user=alice).
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 1024.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp.len() == 1, "one entry in response");
    assert!(
        alter_resp[0].1 == 0,
        "alter should succeed; error_code={}",
        alter_resp[0].1
    );

    // Poll the metadata image until the quota is visible (absorb raft commit latency).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key)
            && cfgs.get("producer_byte_rate") == Some(&1024.0)
        {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "quota not visible in image within 5s"
        );
        tokio::task::yield_now().await;
    }

    // Describe: fetch back the quota.
    let desc = drive_describe_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![("user".into(), 2 /* ANY */, None)],
        false,
    )
    .await;

    let pbr = desc
        .iter()
        .find(|(entity, _)| {
            entity
                .iter()
                .any(|(t, n)| t == "user" && n.as_deref() == Some("alice"))
        })
        .and_then(|(_, values)| {
            values
                .iter()
                .find(|(k, _)| k == "producer_byte_rate")
                .map(|(_, v)| *v)
        });
    assert!(
        pbr == Some(1024.0),
        "expected producer_byte_rate=1024 from describe; got {desc:?}"
    );

    handle.shutdown().await;
}

/// Test 2: Set `(user=alice) producer_byte_rate=128`; alice produces ~8 KB;
/// assert `throttle_time_ms > 0`.
///
/// Rate = 128 bytes/sec, burst = 1 second at rate = 128 bytes free. Producing
/// 8 KB = 8192 bytes means ~7168 bytes over budget. At 128 bytes/sec that is
/// ~56 seconds of debt, but the response throttle_time_ms is capped at 1000ms.
/// We only assert throttle_time_ms > 0 here — the exact value is not load-
/// bearing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn producer_byte_rate_throttles_produce() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed ACL entries so the authorizer engages (compat shim disabled) and
    // alice can Write to the topic.
    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-produce", 1, 1).await;
    wait_partition_exists(&handle, "throttle-produce", 0).await;
    seed_alice_write_acl(&handle, "throttle-produce").await;

    // Set low producer quota for alice.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    // Wait for the quota to appear in the image before producing.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key)
            && cfgs.get("producer_byte_rate") == Some(&128.0)
        {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "quota not visible in image within 5s"
        );
        tokio::task::yield_now().await;
    }

    // Alice produces 8 KB (8 records of 1 KB each). Rate = 128 bytes/sec.
    // Retry loop: TOPIC_AUTHORIZATION_FAILED (29) can fire if the alice ACL
    // hasn't propagated yet to the image snapshot used by the handler.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r =
            drive_produce_sasl(addr, "alice", b"alice-secret", "throttle-produce", 1024, 8).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map(|p| p.error_code)
            .unwrap_or(-1);
        if ec != 29 {
            // Not TOPIC_AUTHORIZATION_FAILED — this is the response we want.
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 6: Set a tiny `(user=alice) request_percentage` and NO byte-rate quota;
/// alice produces a small payload; assert `throttle_time_ms > 0`.
///
/// This is the KIP-124 request quota (server-side CPU-time throttle), which
/// KIP-219 requires the broker to communicate via `throttle_time_ms` while
/// muting the channel — *not* silently delay. `request_percentage=0.001`
/// gives the bucket a ~10µs/sec budget, far below any real produce handler's
/// processing time, so even a single small produce trips the quota and the
/// response must carry a non-zero throttle time. No `producer_byte_rate` is
/// set, so the throttle can only come from the request quota.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_percentage_throttles_produce() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-request", 1, 1).await;
    wait_partition_exists(&handle, "throttle-request", 0).await;
    seed_alice_write_acl(&handle, "throttle-request").await;

    // Set a tiny request_percentage for alice (no byte-rate quota).
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("request_percentage".into(), 0.001, false)],
        )],
        false,
    )
    .await;
    assert!(alter_resp[0].1 == 0, "alter quota must succeed");

    // Wait for the quota to appear in the image before producing.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key)
            && cfgs.get("request_percentage") == Some(&0.001)
        {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "request_percentage quota not visible in image within 5s"
        );
        tokio::task::yield_now().await;
    }

    // Alice produces a single small record. Retry past TOPIC_AUTHORIZATION_FAILED
    // (29) while the alice Write ACL propagates to the handler's image snapshot.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r = drive_produce_sasl(addr, "alice", b"alice-secret", "throttle-request", 16, 1).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map(|p| p.error_code)
            .unwrap_or(-1);
        if ec != 29 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    assert!(
        resp.throttle_time_ms > 0,
        "expected request-quota throttle_time_ms > 0, got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 3: Set `(user=alice) consumer_byte_rate=128`; produce 8 KB as admin;
/// alice fetches; assert `throttle_time_ms > 0`.
///
/// Same rate/burst reasoning as Test 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_byte_rate_throttles_fetch() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "throttle-fetch", 1, 1).await;
    wait_partition_exists(&handle, "throttle-fetch", 0).await;
    seed_alice_read_acl(&handle, "throttle-fetch").await;

    // Set low consumer quota for alice.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("consumer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(
        alter_resp[0].1 == 0,
        "alter consumer_byte_rate must succeed"
    );

    // Wait for the quota to appear in the image.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        if let Some(cfgs) = img.client_quotas().get(&key)
            && cfgs.get("consumer_byte_rate") == Some(&128.0)
        {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "consumer_byte_rate quota not visible in image within 5s"
        );
        tokio::task::yield_now().await;
    }

    // Produce 8 KB as admin (not subject to quota yet).
    seed_alice_write_acl(&handle, "throttle-fetch").await; // give admin path a topic
    let produce_resp =
        drive_produce_sasl(addr, "admin", b"admin-secret", "throttle-fetch", 1024, 8).await;
    let part = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "admin produce must succeed, error_code={}",
        part.error_code
    );

    // Alice fetches. Rate = 128 bytes/sec, data = 8 KB → throttle fires.
    // Retry loop: auth can lag.
    let deadline = Instant::now() + Duration::from_secs(15);
    let fetch_resp = loop {
        let r = drive_fetch_sasl(addr, "alice", b"alice-secret", "throttle-fetch").await;
        // If throttle_time_ms > 0 or error_code == 0, we have a real response.
        if r.error_code == 0 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "fetch error after 15s; error_code={}",
            r.error_code
        );
        // real-time wait (not a progress poll): retry cadence between network fetch attempts, deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    assert!(
        fetch_resp.throttle_time_ms > 0,
        "expected consumer throttle_time_ms > 0, got {}",
        fetch_resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 4 (Option B — user-specific overrides user-default):
/// Set `(user=alice) producer_byte_rate=128` AND
/// `(user=<default>) producer_byte_rate=8192`. Produce as alice. The tight
/// alice-specific rate (128) fires, not the lenient default (8192).
///
/// This avoids the (user, client-id) tuple limitation described above: the
/// lookup module unit tests already cover the tuple-wins-over-user-only path
/// (`pair_specific_wins_over_user_only` in `quota/lookup.rs`). The
/// client-id plumbing gap in Produce/Fetch handlers is deferred to a future
/// cleanup slice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_specific_overrides_user_default() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "precedence-topic", 1, 1).await;
    wait_partition_exists(&handle, "precedence-topic", 0).await;
    seed_alice_write_acl(&handle, "precedence-topic").await;

    // Set lenient default quota (user=<default>) producer_byte_rate=8192.
    let alter_default = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), None)], // None = <default>
            vec![("producer_byte_rate".into(), 8192.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_default[0].1 == 0, "alter default quota must succeed");

    // Set tight user-specific quota (user=alice) producer_byte_rate=128.
    let alter_alice = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![("user".into(), Some("alice".into()))],
            vec![("producer_byte_rate".into(), 128.0, false)],
        )],
        false,
    )
    .await;
    assert!(alter_alice[0].1 == 0, "alter alice quota must succeed");

    // Wait for both quotas to appear in the image.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let img = handle.controller_image_for_test();
        let alice_key: crabka_metadata::EntityKey = vec![("user".into(), Some("alice".into()))];
        let default_key: crabka_metadata::EntityKey = vec![("user".into(), None)];
        let alice_rate = img
            .client_quotas()
            .get(&alice_key)
            .and_then(|c| c.get("producer_byte_rate"))
            .copied();
        let default_rate = img
            .client_quotas()
            .get(&default_key)
            .and_then(|c| c.get("producer_byte_rate"))
            .copied();
        if alice_rate == Some(128.0) && default_rate == Some(8192.0) {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "quotas not visible in image within 5s; alice={alice_rate:?} default={default_rate:?}"
        );
        tokio::task::yield_now().await;
    }

    // Alice produces 8 KB. The alice-specific rate (128 bytes/sec) should
    // cause throttling. The default rate (8192) would NOT cause throttling for
    // this payload size within the burst window.
    let deadline = Instant::now() + Duration::from_secs(15);
    let resp = loop {
        let r =
            drive_produce_sasl(addr, "alice", b"alice-secret", "precedence-topic", 1024, 8).await;
        let ec = r
            .responses
            .first()
            .and_then(|t| t.partition_responses.first())
            .map(|p| p.error_code)
            .unwrap_or(-1);
        if ec != 29 {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s; error_code=29"
        );
        // real-time wait (not a progress poll): retry cadence between network produce attempts (ACL propagation), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    let part = &resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce must succeed, error_code={}",
        part.error_code
    );
    // The alice-specific 128-byte-rate quota fires → throttle_time_ms > 0.
    // If only the default (8192) applied, 8 KB would fit in the burst window
    // and throttle_time_ms would be 0.
    assert!(
        resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0 because alice-specific rate=128 applies; got {}",
        resp.throttle_time_ms
    );

    handle.shutdown().await;
}

/// Test 5: alice (authenticated, no super-user, no ACLs) sends
/// `AlterClientQuotas`; every entry must carry
/// `CLUSTER_AUTHORIZATION_FAILED (31)`.
///
/// A dummy ACL is seeded first to disable the compat shim (allow-all when no
/// ACLs are present in the image).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed dummy ACL to disable compat shim.
    seed_compat_shim_disable_acl(&handle).await;

    // Retry until the shim is provably off: alice should receive 31 on every
    // AlterClientQuotas entry, not 0 (which would mean the shim allowed it).
    let deadline = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_alter_client_quotas_sasl(
            addr,
            "alice",
            "alice-secret",
            vec![(
                vec![("user".into(), Some("alice".into()))],
                vec![("producer_byte_rate".into(), 999.0, false)],
            )],
            false,
        )
        .await;
        // CLUSTER_AUTHORIZATION_FAILED = 31.
        if r.iter().all(|(_, ec)| *ec == 31) {
            break r;
        }
        assert!(
            Instant::now() <= deadline,
            "compat shim still active after 5s; got {r:?}"
        );
        // real-time wait (not a progress poll): retry cadence between network AlterClientQuotas attempts (shim disable), deadline-guarded
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    assert!(
        resp.iter().all(|(_, ec)| *ec == 31),
        "all entries must carry CLUSTER_AUTHORIZATION_FAILED (31); got {resp:?}"
    );
}
