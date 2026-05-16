// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
#![allow(clippy::pedantic)]
#![allow(clippy::unnecessary_unwrap)]

//! Slice 15. Broker-side integration tests for `AlterPartitionReassignments`
//! (api_key 45) and `ListPartitionReassignments` (api_key 46).
//!
//! Uses a 3-broker PLAINTEXT cluster. The authorizer's compatibility shim
//! (no super_users + no ACLs → Allow) lets the tests exercise the full wire
//! path without a SASL handshake.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b/14.

#![cfg(not(target_os = "windows"))]

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::BrokerHandle;
use crabka_protocol::{Decode, Encode};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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
    use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
    use crabka_protocol::owned::create_topics_response::CreateTopicsResponse;

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
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(
        resp.topics[0].error_code, 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Polling helpers
// ─────────────────────────────────────────────────────────────────────────────

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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Ask the first handle for the leader node id.
        let lid = handles[0].controller_leader_id().await;
        if let Some(leader_id) = lid {
            // Find which handle has that node_id by checking its broker_id.
            // BrokerHandle::listen_addr() returns the port the leader is
            // listening on. We identify the leader by the node_id via
            // controller_leader_id() — it returns the raft node id (u64) which
            // equals (broker_index + 1). The handles slice is ordered
            // [broker1, broker2, broker3], so handle[i] has node_id = i+1.
            let idx = usize::try_from(leader_id).unwrap().saturating_sub(1);
            if idx < handles.len() {
                return handles[idx].listen_addr();
            }
        }
        assert!(
            Instant::now() <= deadline,
            "raft leader not stable within 15s; got {lid:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
    use crabka_protocol::owned::alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    };
    use crabka_protocol::owned::alter_partition_reassignments_response::AlterPartitionReassignmentsResponse;

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
    use crabka_protocol::owned::list_partition_reassignments_request::{
        ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
    };
    use crabka_protocol::owned::list_partition_reassignments_response::ListPartitionReassignmentsResponse;

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
    assert_eq!(initial_replicas.len(), 2);
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
    assert_eq!(
        resp[0].1,
        vec![(0, 0)],
        "expected error_code=0; got {:?}",
        resp
    );

    // Wait for the image to reflect the in-flight reassignment.
    let deadline = Instant::now() + Duration::from_secs(10);
    let pr_after_alter = loop {
        let pr = h1.partition_record_for_test("foo", 0).expect("partition");
        if !pr.adding_replicas.is_empty() {
            break pr;
        }
        assert!(
            Instant::now() <= deadline,
            "adding_replicas never set within 10s; pr={pr:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
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

    // Within ~10s the background task should observe adding ⊆ isr and complete.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pr = h1.partition_record_for_test("foo", 0).expect("partition");
        if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
            let actual: std::collections::HashSet<u64> = pr.replicas.iter().copied().collect();
            let expected: std::collections::HashSet<u64> =
                target.iter().map(|n| *n as u64).collect();
            assert_eq!(
                actual, expected,
                "replicas after completion should match target; pr={pr:?}"
            );
            // Clean up.
            h1.shutdown().await;
            h2.shutdown().await;
            h3.shutdown().await;
            return;
        }
        if Instant::now() > deadline {
            panic!("reassignment did not complete; pr={:?}", pr);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pr = h1.partition_record_for_test("foo", 0).expect("partition");
        if !pr.adding_replicas.is_empty() {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "adding_replicas never set within 10s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let listed = drive_list_reassignments(raft_addr, None).await;
    let foo = listed
        .iter()
        .find(|(n, _)| n == "foo")
        .expect("foo should appear in list");
    assert_eq!(
        foo.1.len(),
        1,
        "expected 1 partition in-flight; got {:?}",
        foo.1
    );
    assert_eq!(foo.1[0].0, 0, "expected partition_index=0");
    assert_eq!(
        foo.1[0].2,
        vec![new_replica],
        "expected adding_replicas=[new_replica]; got {:?}",
        foo.1[0].2
    );

    // Clean up.
    h1.shutdown().await;
    h2.shutdown().await;
    h3.shutdown().await;
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
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pr = h1.partition_record_for_test("foo", 0).expect("partition");
        if !pr.adding_replicas.is_empty() {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "adding_replicas never set within 10s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Cancel: replicas = None.
    let resp = drive_alter_reassignments(raft_addr, vec![("foo", 0, None)]).await;
    assert_eq!(
        resp[0].1,
        vec![(0, 0)],
        "cancel should succeed; got {:?}",
        resp
    );

    // Wait for the image to reflect the cancellation.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let pr_after_cancel = h1.partition_record_for_test("foo", 0).expect("partition");
        if pr_after_cancel.adding_replicas.is_empty()
            && pr_after_cancel.removing_replicas.is_empty()
        {
            assert_eq!(
                pr_after_cancel.replicas, original_replicas,
                "replicas should revert to original after cancel; pr={pr_after_cancel:?}"
            );
            // Clean up.
            h1.shutdown().await;
            h2.shutdown().await;
            h3.shutdown().await;
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "cancel did not complete within 10s; pr={pr_after_cancel:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
