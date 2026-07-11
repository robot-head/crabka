// rustc 1.95 clippy ICEs on this file in the same places as throttle.rs /
// client_quotas.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
//! Broker-level integration test for (user, client-id) tuple quota
//! end-to-end enforcement.
//!
//! Tests:
//! 1. `tuple_quota_throttles_only_matching_client_id` — Set
//!    `(user=alice, client-id=app-x) producer_byte_rate=1024`; produce ~4 KB
//!    as (alice, app-x) → `throttle_time_ms > 0`; produce ~4 KB as
//!    (alice, other) → `throttle_time_ms == 0` (no quota match).
//!
//! This test covers the end-to-end fix: the Produce
//! handler must forward `ctx.client_id` to the quota lookup rather than "".
//! Otherwise the tuple lookup always received `client_id`="" → no tuple match →
//! `throttle_time_ms` == 0 for both cases.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14/15/15b/16/17a.

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerHandle, config::ListenerSpec};
use crabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
    records::{Record, RecordBatch},
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
// Variant of client_quotas.rs `round_trip` that accepts an explicit client_id
// so that two produce requests in the same test can carry different client_ids.
// ─────────────────────────────────────────────────────────────────────────────

async fn round_trip_with_client_id(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    client_id: &str,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
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

// Convenience wrapper that uses a fixed test client_id (for non-produce calls).
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    round_trip_with_client_id(
        stream,
        api_key,
        api_version,
        corr_id,
        flexible,
        "crabka-tuple-quota-test",
        body,
    )
    .await
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
// Cluster setup helpers (copied from client_quotas.rs)
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
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

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

/// Await until `handle` sees `(topic, partition)` present in its image.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
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
    // intentional: absorb raft commit-then-apply gap; ACL propagation to the
    // request handler's image snapshot has no awaiter/metric to poll.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire driver for AlterClientQuotas
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `AlterClientQuotas` (`api_key=49`) over a SASL/PLAIN connection.
///
/// `entries` is a list of `(entity_components, ops)` where:
/// - `entity_components` is `Vec<(entity_type, entity_name)>`
/// - `ops` is `Vec<(key, value, remove)>`
///
/// Returns the per-entry `(entity, error_code)` pairs.
async fn drive_alter_client_quotas_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    entries: QuotaEntries,
    validate_only: bool,
) -> Vec<(Vec<(String, Option<String>)>, i16)> {
    const VERSION: i16 = 1; // flexible

    use crabka_protocol::owned::{
        alter_client_quotas_request::{AlterClientQuotasRequest, EntityData, EntryData, OpData},
        alter_client_quotas_response::AlterClientQuotasResponse,
    };

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
// Wire driver for Produce with explicit on-wire client_id
// ─────────────────────────────────────────────────────────────────────────────

/// Drive a `Produce` request over a fresh SASL/PLAIN connection.
///
/// `wire_client_id` is written into the Kafka request header — it's the value
/// the broker sees as the connection's client.id and uses for quota lookup.
/// This lets a single test send two produces with different `client_ids`.
///
/// Returns the full `ProduceResponse`.
async fn drive_produce_sasl_with_client_id(
    addr: SocketAddr,
    user: &str,
    pass: &[u8],
    wire_client_id: &str,
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

    // SASL handshake uses the default test client_id; only the Produce request
    // itself carries wire_client_id.  Each TCP connection creates a fresh
    // broker-side connection state, so the quota window resets per connection.
    let mut stream = sasl_plain_authenticate(addr, user, pass)
        .await
        .expect("SASL authenticate for Produce");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode Produce");
    let resp_bytes = round_trip_with_client_id(
        &mut stream,
        0, // Produce api_key
        VERSION,
        1,
        true, // flexible
        wire_client_id,
        &body,
    )
    .await
    .expect("Produce round-trip");
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, VERSION).expect("decode ProduceResponse")
}

async fn await_authorized_produce(addr: SocketAddr, client_id: &str) -> ProduceResponse {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let response = drive_produce_sasl_with_client_id(
            addr,
            "alice",
            b"alice-secret",
            client_id,
            "tuple-quota-topic",
            1024,
            4,
        )
        .await;
        let error_code = response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .map_or(-1, |partition| partition.error_code);
        if error_code != 29 {
            return response;
        }
        assert!(
            Instant::now() <= deadline,
            "ACL still not applied after 15s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration test
// ─────────────────────────────────────────────────────────────────────────────

/// Set `(user=alice, client-id=app-x) producer_byte_rate=1024`.
///
/// * Produce ~4 KB as (alice, app-x) → `throttle_time_ms > 0`  (tuple matches).
/// * Produce ~4 KB as (alice, other) → `throttle_time_ms == 0` (no quota match).
///
/// The second assertion verifies that the tuple quota does NOT fire on an
/// unmatched `client_id`, i.e., there is no `(user=alice)` fallback quota set.
///
/// This test covers the end-to-end fix: the Produce handler
/// must pass `ctx.client_id` to the quota lookup. Otherwise the handler always
/// passed `""` → no tuple quota ever matched → both produces would return
/// `throttle_time_ms == 0`.
///
/// NOTE: If T3 has not yet merged, the first assertion will fail
/// (`throttle_time_ms == 0`). That is the expected pre-T3 behavior. The test
/// is intentionally committed before T3 merges so it turns green automatically
/// in the next CI run that includes T3.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tuple_quota_throttles_only_matching_client_id() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed ACL entries so the authorizer engages (compat shim disabled) and
    // alice can Write to the topic.
    seed_compat_shim_disable_acl(&handle).await;
    create_topic_as_admin(addr, "tuple-quota-topic", 1, 1).await;
    wait_partition_exists(&handle, "tuple-quota-topic", 0).await;
    seed_alice_write_acl(&handle, "tuple-quota-topic").await;

    // Set tuple quota: (user=alice, client-id=app-x) producer_byte_rate=1024.
    // Rate = 1024 bytes/sec, burst = 1 second at rate = 1024 bytes free.
    // Producing 4 KB = 4096 bytes means ~3072 bytes over budget → throttle fires.
    // No (user=alice)-only quota is set, so (alice, other) has no quota at all.
    let alter_resp = drive_alter_client_quotas_sasl(
        addr,
        "admin",
        "admin-secret",
        vec![(
            vec![
                ("user".into(), Some("alice".into())),
                ("client-id".into(), Some("app-x".into())),
            ],
            vec![("producer_byte_rate".into(), 1024.0, false)],
        )],
        false,
    )
    .await;
    assert!(
        alter_resp.len() == 1,
        "one entry in AlterClientQuotas response"
    );
    assert!(
        alter_resp[0].1 == 0,
        "AlterClientQuotas must succeed; error_code={}",
        alter_resp[0].1
    );

    // Await until the quota appears in the metadata image (absorb raft latency).
    //
    // `MetadataImage` canonicalizes EntityKey by sorting entries alphabetically
    // by entity_type, so the stored key has "client-id" before "user".
    handle
        .wait_for_image(|img| {
            let key: crabka_metadata::EntityKey = vec![
                ("client-id".into(), Some("app-x".into())),
                ("user".into(), Some("alice".into())),
            ];
            img.client_quotas()
                .get(&key)
                .and_then(|cfgs| cfgs.get("producer_byte_rate"))
                == Some(&1024.0)
        })
        .await;

    // ── Case 1: (alice, app-x) — tuple matches, must throttle ────────────────
    //
    // Retry loop: TOPIC_AUTHORIZATION_FAILED (29) can fire if the alice Write
    // ACL hasn't propagated to the handler's image snapshot yet.
    let matching_resp = await_authorized_produce(addr, "app-x").await;

    let part = &matching_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce (alice, app-x) must succeed; error_code={}",
        part.error_code
    );
    assert!(
        matching_resp.throttle_time_ms > 0,
        "expected throttle_time_ms > 0 for (alice, app-x) with producer_byte_rate=1024 \
         and 4 KB payload; got {} — T3 may not have merged yet (T3 wires ctx.client_id \
         into the quota call site)",
        matching_resp.throttle_time_ms
    );

    // ── Case 2: (alice, other) — no quota match, must NOT throttle ────────────
    //
    // A fresh TCP connection means the token bucket starts from scratch, so
    // there is no residual debt from Case 1.  No (user=alice)-only quota exists,
    // so the produce must complete with throttle_time_ms == 0.
    let non_matching_resp = await_authorized_produce(addr, "other").await;

    let part = &non_matching_resp.responses[0].partition_responses[0];
    assert!(
        part.error_code == 0,
        "produce (alice, other) must succeed; error_code={}",
        part.error_code
    );
    assert!(
        non_matching_resp.throttle_time_ms == 0,
        "expected throttle_time_ms == 0 for (alice, other) — no tuple or user-only \
         quota is set for this client_id; got {}",
        non_matching_resp.throttle_time_ms
    );

    handle.shutdown().await;
}
