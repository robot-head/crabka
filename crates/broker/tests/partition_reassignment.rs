// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]

//! Broker-side integration tests for `AlterPartitionReassignments`
//! (api_key 45) and `ListPartitionReassignments` (api_key 46).
//!
//! Uses a 3-broker PLAINTEXT cluster. The authorizer's compatibility shim
//! (no super_users + no ACLs → Allow) lets the tests exercise the full wire
//! path without a SASL handshake.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14.

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerHandle, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
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

mod support;

// ─────────────────────────────────────────────────────────────────────────────
// Wire helpers — PLAINTEXT, no SASL
// ─────────────────────────────────────────────────────────────────────────────

/// Single length-prefixed request/response exchange over a PLAINTEXT
/// connection. Copied verbatim from `elect_leaders.rs`.
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
    let client_id = "crabka-reassign-test";
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

/// Create a topic via PLAINTEXT. The authorizer's compat shim (no super_users,
/// no ACLs) lets the request through. Copied from `elect_leaders.rs`.
async fn create_topic_plaintext(
    addr: SocketAddr,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) {
    use crabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    };

    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
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
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Polling helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Wait until `handle` sees `(topic, partition)` present in its image.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    // Event-driven: subscribes to the image watch channel via the awaiter.
    handle.wait_until_partition_present(topic, partition).await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup
// ─────────────────────────────────────────────────────────────────────────────

/// Start a 3-broker PLAINTEXT cluster. Returns (h1, h2, h3, d1, d2, d3, addr1)
/// where addr1 is the listen address of broker 1. Waits for all 3 brokers to
/// see each other registered before returning.
async fn start_three_broker_plaintext_cluster() -> (
    BrokerHandle,
    BrokerHandle,
    BrokerHandle,
    TempDir,
    TempDir,
    TempDir,
    SocketAddr,
) {
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let mut it = cluster.into_iter();
    let (h1, _cfg1, d1) = it.next().unwrap();
    let (h2, _cfg2, d2) = it.next().unwrap();
    let (h3, _cfg3, d3) = it.next().unwrap();
    let addr1 = h1.listen_addr();
    (h1, h2, h3, d1, d2, d3, addr1)
}

/// Poll until the raft controller leader is stable and return its listen addr.
/// Tries each handle in `handles` to find the one whose `node_id` matches the
/// reported raft leader.
async fn controller_leader_addr(handles: &[&BrokerHandle]) -> SocketAddr {
    // Event-driven: await a non-zero elected controller leader on the first
    // handle's leader watch channel instead of polling `controller_leader_id`.
    let leader_id = handles[0].wait_until_controller_leader().await;
    // We identify the leader by the node_id — it is the raft node id (u64)
    // which equals (broker_index + 1). The handles slice is ordered
    // [broker1, broker2, broker3], so handle[i] has node_id = i+1.
    let idx = usize::try_from(leader_id).unwrap().saturating_sub(1);
    assert!(
        idx < handles.len(),
        "raft leader id {leader_id} out of range for {} handles",
        handles.len()
    );
    handles[idx].listen_addr()
}

// ─────────────────────────────────────────────────────────────────────────────
// Wire drivers for AlterPartitionReassignments and ListPartitionReassignments
// ─────────────────────────────────────────────────────────────────────────────

/// Drive `AlterPartitionReassignments` over a fresh PLAINTEXT connection.
/// Returns `(topic_name, [(partition_index, error_code)])` rows.
async fn drive_alter_reassignments(
    addr: SocketAddr,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    use crabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    };

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<ReassignablePartition>> =
        std::collections::BTreeMap::new();
    for (topic, partition, target_opt) in rows {
        by_topic
            .entry(topic.to_string())
            .or_default()
            .push(ReassignablePartition {
                partition_index: partition,
                replicas: target_opt,
                ..Default::default()
            });
    }
    let topics: Vec<ReassignableTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopic {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let req = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: true,
        topics,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .expect("encode AlterPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 45, 1, 1, true, &body)
        .await
        .expect("AlterPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterPartitionReassignmentsResponse::decode(&mut cur, 1)
        .expect("decode AlterPartitionReassignmentsResponse");

    resp.responses
        .into_iter()
        .map(|r| {
            (
                r.name,
                r.partitions
                    .into_iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect()
}

/// Drive `ListPartitionReassignments` over a fresh PLAINTEXT connection.
/// Returns `(topic_name, [(partition_index, replicas, adding_replicas, removing_replicas)])` rows.
async fn drive_list_reassignments(
    addr: SocketAddr,
    filter: Option<Vec<(&str, Vec<i32>)>>,
) -> Vec<(String, Vec<(i32, Vec<i32>, Vec<i32>, Vec<i32>)>)> {
    use crabka_protocol::owned::{
        list_partition_reassignments_request::{
            ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
        },
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
    };

    let topics_arg = filter.map(|list| {
        list.into_iter()
            .map(
                |(name, partition_indexes)| ListPartitionReassignmentsTopics {
                    name: name.to_string(),
                    partition_indexes,
                    ..Default::default()
                },
            )
            .collect()
    });
    let req = ListPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        topics: topics_arg,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 0)
        .expect("encode ListPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 46, 0, 1, true, &body)
        .await
        .expect("ListPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ListPartitionReassignmentsResponse::decode(&mut cur, 0)
        .expect("decode ListPartitionReassignmentsResponse");

    resp.topics
        .into_iter()
        .map(|t| {
            (
                t.name,
                t.partitions
                    .into_iter()
                    .map(|p| {
                        (
                            p.partition_index,
                            p.replicas,
                            p.adding_replicas,
                            p.removing_replicas,
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────

/// Test 1: Send AlterPartitionReassignments, then inject ISR to include the
/// new replica. The background task observes adding ⊆ ISR and completes the
/// reassignment, clearing adding_replicas and removing_replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_then_complete_via_isr_catchup() {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let (h1, h2, h3, _d1, _d2, _d3, addr1) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr1, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    // Find which brokers are in `replicas` initially — choose target accordingly.
    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let initial_replicas = pr.replicas.clone();
    assert!(initial_replicas.len() == 2);
    // Pick the third broker (not in initial_replicas) as the new replica.
    let new_replica: i32 = (1..=3)
        .find(|n| !initial_replicas.contains(&(*n as u64)))
        .expect("free broker");
    let removing: i32 = *initial_replicas.last().unwrap() as i32;
    let staying: i32 = *initial_replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    // Send alter to controller leader (whichever broker leads raft).
    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target.clone()))]).await;
    assert!(
        resp[0].1 == vec![(0, 0)],
        "expected error_code=0; got {:?}",
        resp
    );

    // Wait for the image to reflect the in-flight reassignment.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| !p.adding_replicas.is_empty())
    })
    .await;
    let pr_after_alter = h1.partition_record_for_test("foo", 0).expect("partition");
    assert!(
        pr_after_alter
            .adding_replicas
            .contains(&(new_replica as u64)),
        "adding_replicas should contain new_replica; pr={pr_after_alter:?}"
    );
    assert!(
        pr_after_alter
            .removing_replicas
            .contains(&(removing as u64)),
        "removing_replicas should contain removing; pr={pr_after_alter:?}"
    );

    // Inject ISR including the new replica so the background task completes the reassignment.
    let injected = crabka_metadata::PartitionRecord {
        isr: vec![staying as u64, new_replica as u64, removing as u64],
        ..pr_after_alter.clone()
    };
    h1.submit_metadata_record_for_test(crabka_metadata::MetadataRecord::V1Partition(injected))
        .await
        .expect("inject");

    // The background task should observe adding ⊆ isr and complete, clearing
    // adding_replicas and removing_replicas.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| p.adding_replicas.is_empty() && p.removing_replicas.is_empty())
    })
    .await;
    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let actual: std::collections::HashSet<u64> = pr.replicas.iter().copied().collect();
    let expected: std::collections::HashSet<u64> = target.iter().map(|n| *n as u64).collect();
    assert!(
        actual == expected,
        "replicas after completion should match target; pr={pr:?}"
    );
    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

/// Test 2: After AlterPartitionReassignments starts a reassignment, the
/// ListPartitionReassignments handler should return the in-flight rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_in_flight_returns_pending_rows() {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let (h1, h2, h3, _d1, _d2, _d3, addr) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let new_replica: i32 = (1..=3)
        .find(|n| !pr.replicas.contains(&(*n as u64)))
        .expect("free");
    let staying: i32 = *pr.replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    // Wait for the image to reflect adding_replicas, then list.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| !p.adding_replicas.is_empty())
    })
    .await;

    let listed = drive_list_reassignments(raft_addr, None).await;
    let foo = listed
        .iter()
        .find(|(n, _)| n == "foo")
        .expect("foo should appear in list");
    assert!(
        foo.1.len() == 1,
        "expected 1 partition in-flight; got {:?}",
        foo.1
    );
    check!(foo.1[0].0 == 0, "expected partition_index=0");
    check!(
        foo.1[0].2 == vec![new_replica],
        "expected adding_replicas=[new_replica]; got {:?}",
        foo.1[0].2
    );

    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// SASL/PLAIN wire helpers (T10)
// ─────────────────────────────────────────────────────────────────────────────

/// Open a TCP stream to `addr` and drive `ApiVersions` → `SaslHandshake(PLAIN)`
/// → `SaslAuthenticate(\0user\0password)`. Returns the authenticated stream.
/// Copied verbatim from `elect_leaders.rs`.
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

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
/// `super_user` is set as the only super-user.
/// `users` is a slice of `(username, password)` pairs added to `plain_credentials`.
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
    // fires for non-super principals; default is `AllowAllAuthorizer`.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Create a topic via SASL/PLAIN as the given admin user.
/// Copied from `elect_leaders.rs`'s `create_topic_sasl_plain`.
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

/// Drive `AlterPartitionReassignments` over a SASL/PLAIN authenticated connection.
/// Returns `(topic_name, [(partition_index, error_code)])` rows.
async fn drive_alter_reassignments_sasl_plain(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    use crabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    };

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<ReassignablePartition>> =
        std::collections::BTreeMap::new();
    for (topic, partition, target_opt) in rows {
        by_topic
            .entry(topic.to_string())
            .or_default()
            .push(ReassignablePartition {
                partition_index: partition,
                replicas: target_opt,
                ..Default::default()
            });
    }
    let topics: Vec<ReassignableTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopic {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let req = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: true,
        topics,
        ..Default::default()
    };
    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for AlterPartitionReassignments");
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .expect("encode AlterPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 45, 1, 1, true, &body)
        .await
        .expect("AlterPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterPartitionReassignmentsResponse::decode(&mut cur, 1)
        .expect("decode AlterPartitionReassignmentsResponse");

    resp.responses
        .into_iter()
        .map(|r| {
            (
                r.name,
                r.partitions
                    .into_iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// T10: auth-deny integration test
// ─────────────────────────────────────────────────────────────────────────────

/// Test 4: alice (authenticated via SASL/PLAIN, no ACLs) sends
/// `AlterPartitionReassignments` and must receive
/// `CLUSTER_AUTHORIZATION_FAILED (31)` per-partition.
///
/// A dummy ACL is seeded first to disable the compat shim (the shim allows
/// everything when `image.acls` is empty).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed a dummy ACL to disable the compat shim.  The ACL itself is
    // irrelevant — any non-empty `image.acls` flips the shim off and forces
    // the authorizer to evaluate every request.
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
        .expect("seed dummy ACL");

    // `submit_metadata_record_for_test` blocks until the raft entry is
    // committed and the state machine applies it to the image, so the ACL
    // is already present here; await it explicitly on the image watch channel
    // rather than sleeping.
    handle
        .wait_for_image(|img| img.all_acls().next().is_some())
        .await;

    create_topic_as_admin(addr, "foo", 1, 1).await;
    wait_partition_exists(&handle, "foo", 0).await;

    // Retry up to 5s to absorb raft apply latency on slow runners.
    // intentional: bounded RPC-response poll on the alter response error code
    // (CLUSTER_AUTHORIZATION_FAILED=31) — an end-to-end authorizer verdict, not
    // a metadata-image or metric signal, so there is no awaiter to wait on.
    let deadline_auth = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_alter_reassignments_sasl_plain(
            addr,
            "alice",
            "alice-secret",
            vec![("foo", 0, Some(vec![1]))],
        )
        .await;
        // If we see 31, the shim is off and we're done.
        if r.iter()
            .all(|(_, parts)| parts.iter().all(|(_, ec)| *ec == 31))
        {
            break r;
        }
        assert!(
            Instant::now() <= deadline_auth,
            "ACL shim still active or wrong error after 5s; got {r:?}"
        );
        // real-time wait (not a progress poll): retry/backoff cadence between attempts — each attempt opens a fresh TCP connection + full SASL handshake, so the 100ms backoff bounds connection churn while the raft ACL apply propagates.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    assert!(
        resp[0].1 == vec![(0, 31)],
        "expected CLUSTER_AUTHORIZATION_FAILED (31) for alice; got {resp:?}"
    );
}

/// Test 3: Cancel an in-flight reassignment by sending target=None (null replicas).
/// The partition record should revert to the original replica set with empty
/// adding_replicas and removing_replicas.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_via_null_replicas_reverts() {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let (h1, h2, h3, _d1, _d2, _d3, addr) = start_three_broker_plaintext_cluster().await;
    create_topic_plaintext(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;

    let pr = h1.partition_record_for_test("foo", 0).expect("partition");
    let original_replicas = pr.replicas.clone();
    let new_replica: i32 = (1..=3)
        .find(|n| !original_replicas.contains(&(*n as u64)))
        .expect("free");
    let staying: i32 = *original_replicas.first().unwrap() as i32;
    let target = vec![staying, new_replica];

    let raft_addr = controller_leader_addr(&[&h1, &h2, &h3]).await;
    drive_alter_reassignments(raft_addr, vec![("foo", 0, Some(target))]).await;

    // Wait for the image to reflect adding_replicas (reassignment started).
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| !p.adding_replicas.is_empty())
    })
    .await;

    // Cancel: replicas = None.
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, None)]).await;
    assert!(
        resp[0].1 == vec![(0, 0)],
        "cancel should succeed; got {:?}",
        resp
    );

    // Wait for the image to reflect the cancellation.
    h1.wait_for_image(|img| {
        img.partition("foo", 0)
            .is_some_and(|p| p.adding_replicas.is_empty() && p.removing_replicas.is_empty())
    })
    .await;
    let pr_after_cancel = h1.partition_record_for_test("foo", 0).expect("partition");
    assert!(
        pr_after_cancel.replicas == original_replicas,
        "replicas should revert to original after cancel; pr={pr_after_cancel:?}"
    );
    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
}
