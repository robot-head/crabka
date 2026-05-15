// rustc 1.95 clippy ICEs on this file in two places:
//
// 1. `clippy::pedantic` lints — same upstream annotate-snippets bug as
//    `tests/acl_handlers.rs` and `tests/admin_handlers.rs`.
// 2. `clippy::unwrap_in_result` — the `UnwrappableVariablesVisitor` in
//    `clippy_lints::unwrap` ICEs on the `.expect()` calls inside `round_trip`
//    (which returns `Result`) because the computed span has start > end.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
#![allow(clippy::pedantic)]
// `clippy::unnecessary_unwrap` fires on the `l1.unwrap()` inside `if l1.is_some()`
// and its span computation ICEs in annotate-snippets on Rust 1.95.
#![allow(clippy::unnecessary_unwrap)]

//! Slice 14. Broker-side integration tests for the operator-triggered
//! `ElectLeaders` RPC. Drives the wire path end-to-end with a Rust
//! PLAINTEXT client; verifies the resulting partition state via
//! `BrokerHandle` test accessors.
//!
//! Both tests use a **3-broker PLAINTEXT cluster** rather than 2-broker SASL:
//!
//! * A 2-broker raft cluster cannot form a quorum (2/2) when one broker is
//!   dead, so the automatic partition-leader election and metadata commits
//!   needed by these tests never succeed. A 3-broker cluster keeps quorum
//!   (2/3) with one dead node, which is sufficient for both test scenarios.
//!
//! * The authorizer's compatibility shim (`super_users` empty + zero ACLs
//!   → Allow) lets the test exercise the full `ElectLeaders` wire path
//!   without a SASL handshake, keeping the test helpers simpler.
//!
//! Gated to non-Windows to match the multi-broker test convention from
//! slices 10b/12b (openraft `debug_assert!` races on the hosted Windows
//! task scheduler are unrelated to the protocol under test).

#![cfg(not(target_os = "windows"))]

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::BrokerHandle;
use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::owned::elect_leaders_request::{ElectLeadersRequest, TopicPartitions};
use crabka_protocol::owned::elect_leaders_response::ElectLeadersResponse;
use crabka_protocol::{Decode, Encode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

mod support;

const ELECT_LEADERS_VERSION: i16 = 2;

// ─────────────────────────────────────────────────────────────────────────────
// Minimal wire helpers — bare TCP on PLAINTEXT, no SASL.
// ─────────────────────────────────────────────────────────────────────────────

/// Single length-prefixed request/response exchange over a **PLAINTEXT**
/// connection. Encodes a Kafka request header v1 (non-flexible) or v2
/// (flexible), writes the frame, reads one response frame, strips the
/// response header, and returns the body bytes.
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
    let client_id = "crabka-elect-test";
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

/// Drive `ElectLeaders` over a fresh PLAINTEXT connection. The authorizer's
/// compat shim (no super_users + no ACLs → Allow) lets this through without
/// SASL. Asserts the top-level `error_code == 0` and returns per-partition
/// `(partition_id, error_code)` rows for the topic named `topic`.
async fn drive_elect_leaders(
    addr: SocketAddr,
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let req = ElectLeadersRequest {
        election_type,
        topic_partitions: Some(vec![TopicPartitions {
            topic: topic.to_string(),
            partitions,
            ..Default::default()
        }]),
        timeout_ms: 30_000,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, ELECT_LEADERS_VERSION)
        .expect("encode ElectLeaders");
    let resp_bytes = round_trip(&mut stream, 43, ELECT_LEADERS_VERSION, 1, true, &body)
        .await
        .expect("ElectLeaders round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ElectLeadersResponse::decode(&mut cur, ELECT_LEADERS_VERSION)
        .expect("decode ElectLeadersResponse");

    assert_eq!(
        resp.error_code, 0,
        "top-level error_code must be 0, got {}",
        resp.error_code
    );

    resp.replica_election_results
        .into_iter()
        .find(|r| r.topic == topic)
        .map(|r| {
            r.partition_result
                .into_iter()
                .map(|p| (p.partition_id, p.error_code))
                .collect()
        })
        .unwrap_or_default()
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

/// Poll until `handle` reports `leader` as the leader for `(topic, partition)`.
async fn wait_partition_leader(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if handle.partition_leader_for_test(topic, partition) == Some(leader) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "partition {topic}-{partition} didn't elect leader={leader} within 15s; current={:?}",
            handle.partition_leader_for_test(topic, partition)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll until the ISR for `(topic, partition)` contains `node`.
async fn wait_isr_contains(handle: &BrokerHandle, topic: &str, partition: i32, node: u64) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if handle
            .partition_isr_for_test(topic, partition)
            .map(|isr| isr.contains(&node))
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "ISR for {topic}-{partition} never included node={node} within 15s; current={:?}",
            handle.partition_isr_for_test(topic, partition)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll until the ISR for `(topic, partition)` is exactly `expected`.
async fn wait_partition_isr_only(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    expected: &[u64],
) {
    let expected_set: std::collections::HashSet<u64> = expected.iter().copied().collect();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(isr) = handle.partition_isr_for_test(topic, partition) {
            let actual_set: std::collections::HashSet<u64> = isr.iter().copied().collect();
            if actual_set == expected_set {
                return;
            }
        }
        assert!(
            Instant::now() <= deadline,
            "ISR for {topic}-{partition} didn't converge to {:?} within 15s; current={:?}",
            expected,
            handle.partition_isr_for_test(topic, partition)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// 3-broker PLAINTEXT cluster, rf=2 topic (replicas = [1, 2]).
///
/// Scenario:
/// 1. Kill broker 1 (preferred replica). Broker 3 keeps raft quorum (2/3).
/// 2. Broker 2 becomes partition leader via the automatic on-broker-dead path.
/// 3. Revive broker 1 (Rejoin). It catches up on replication and expands
///    back into the ISR.
/// 4. Send `ElectLeaders Preferred` (election_type=0) via wire.
/// 5. Assert per-partition error_code = 0; poll until leader == 1 again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_election_via_wire_returns_success() {
    // Cluster lock matches the pattern in tests/leader_election.rs.
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // All three brokers' addresses captured before any shutdowns.
    let broker1_addr = cluster[0].1.listen_addr;

    // Create a rf=2 topic. With 3 registered brokers the scheduler assigns
    // replicas [1, 2]; broker 1 is the preferred (first) replica.
    create_topic_plaintext(broker1_addr, "foo-preferred", 1, 2).await;

    // Wait for all rf brokers to see the partition in their image.
    wait_partition_exists(&cluster[0].0, "foo-preferred", 0).await;
    wait_partition_exists(&cluster[1].0, "foo-preferred", 0).await;

    let initial_leader = cluster[0].0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap_or(1);
    eprintln!("initial partition leader: {initial_leader}");

    // Kill broker 1 (index 0). Raft quorum {2, 3} can still commit.
    let (dead_h, dead_cfg, dead_dir) = cluster.remove(0);
    dead_h.shutdown().await;

    // Wait for the surviving cluster to elect a new partition leader
    // (i.e., not broker 1).
    let deadline = Instant::now() + Duration::from_secs(15);
    let new_leader;
    loop {
        let l = cluster[0].0.partition_leader_for_test("foo-preferred", 0);
        if l.is_some() && l != Some(1) {
            new_leader = l.unwrap();
            eprintln!("new partition leader after broker 1 death: {new_leader}");
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "no new partition leader within 15s; current={l:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = new_leader; // used for diagnostics

    // Revive broker 1 (Rejoin reads the existing raft log).
    // The surviving 2/3 quorum continues committing.
    let mut revived_cfg = dead_cfg.clone();
    revived_cfg.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    let revived_h = crabka_broker::Broker::start(revived_cfg)
        .await
        .expect("rejoin broker 1");

    // Wait for broker 1 to be back in the ISR on broker 2's view.
    // The ISR expand is committed by the surviving raft leader so broker 2
    // (or 3) must reflect it.
    wait_isr_contains(&cluster[0].0, "foo-preferred", 0, 1).await;

    // The ElectLeaders request MUST go to the raft leader, which is the only
    // broker with an authoritative liveness state (it receives all heartbeats).
    // After broker 1's revive it heartbeats to the raft leader, which marks
    // it as Alive. We discover which surviving broker is the raft leader and
    // send there.
    //
    // Inlined here rather than extracted into a helper to avoid a complex
    // `&[(BrokerHandle, BrokerConfig, TempDir)]` function signature that
    // triggers the Rust 1.95 annotate-snippets ICE in clippy::type_complexity.
    let elect_addr = {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let lid = cluster[0].0.controller_leader_id().await;
            if let Some(l) = lid
                && let Some(pos) = cluster.iter().position(|(_, cfg, _)| cfg.node_id == l)
            {
                break cluster[pos].1.listen_addr;
            }
            assert!(Instant::now() <= deadline, "raft leader not stable within 15s");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    eprintln!("sending ElectLeaders Preferred to raft leader at {elect_addr}");

    // Now send ElectLeaders Preferred (election_type=0). Broker 1 is the
    // preferred replica (replicas[0]) and is now back in ISR and alive.
    let result = drive_elect_leaders(elect_addr, "foo-preferred", vec![0], 0).await;
    assert_eq!(
        result,
        vec![(0, 0)],
        "expected error_code=0 for PREFERRED election; got {result:?}"
    );

    // Poll until the image reflects broker 1 as leader again.
    wait_partition_leader(&cluster[0].0, "foo-preferred", 0, 1).await;
    wait_partition_leader(&revived_h, "foo-preferred", 0, 1).await;

    // Clean up.
    revived_h.shutdown().await;
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
    drop(dead_dir);
}

/// 3-broker PLAINTEXT cluster, rf=2 topic (replicas = [1, 2]).
///
/// Scenario (uses metadata injection to simulate a dead ISR without
/// breaking raft quorum):
/// 1. Submit a `PartitionRecord` with `isr=[99]` directly — broker 99 doesn't
///    exist, so liveness says it's dead.
/// 2. Broker 1 (in replicas but not in ISR) is alive and its heartbeat is
///    known to the controller.
/// 3. Send `ElectLeaders Unclean` (election_type=1) via wire.
/// 4. The handler checks: is any ISR member (99) alive? No → unclean eligible.
///    First alive replica in [1, 2]? Broker 1 → elected as leader, ISR=[1].
/// 5. Assert per-partition error_code = 0 and poll until leader == 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_election_via_wire_picks_alive_replica() {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let addr = cluster[0].1.listen_addr;
    // Keep named references to avoid chained index+tuple accesses that
    // confuse the Rust 1.95 borrow-checker span computation.
    let h0 = &cluster[0].0;
    let h1 = &cluster[1].0;

    // Create rf=2 topic. Replicas=[1,2]; broker 1 is preferred.
    create_topic_plaintext(addr, "foo-unclean", 1, 2).await;
    wait_partition_exists(h0, "foo-unclean", 0).await;
    wait_partition_exists(h1, "foo-unclean", 0).await;

    // Read the current partition record so we can preserve replicas + epoch.
    let pr_before = wait_partition_record_known(h0, "foo-unclean", 0).await;
    eprintln!("partition before injection: {pr_before:?}");

    // Inject a PartitionRecord where the ISR contains only broker 99 (dead).
    // Broker 99 has never sent a heartbeat, so liveness.is_alive(99) == false.
    // Broker 1 is alive (it's been running and heartbeating to the controller).
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: "foo-unclean".to_string(),
        partition: 0,
        // Keep the same leader for now; UNCLEAN will change it.
        leader: pr_before.leader,
        replicas: pr_before.replicas.clone(),
        // ISR contains only a dead phantom node.
        isr: vec![99],
        leader_epoch: pr_before.leader_epoch + 1,
    });
    h0.submit_metadata_record_for_test(forged)
        .await
        .expect("inject forged PartitionRecord");

    // Wait for the injected ISR to propagate to the image.
    wait_partition_isr_only(h0, "foo-unclean", 0, &[99]).await;

    // Drive ElectLeaders Unclean (election_type=1).
    // The algorithm finds: ISR=[99] — all dead → unclean eligible.
    // First alive in replicas=[1,2] → broker 1 → new leader=1, isr=[1].
    let result = drive_elect_leaders(addr, "foo-unclean", vec![0], 1).await;
    assert_eq!(
        result,
        vec![(0, 0)],
        "expected error_code=0 for UNCLEAN election; got {result:?}"
    );

    // Poll until the metadata image reflects the new leader and ISR.
    wait_partition_leader(h0, "foo-unclean", 0, 1).await;
    wait_partition_isr_only(h0, "foo-unclean", 0, &[1]).await;

    // Clean up.
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Create a topic on a PLAINTEXT broker. The authorizer's compat shim
/// (no super_users, no ACLs) lets the request through.
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

/// Poll until the partition record for `(topic, partition)` is visible in the
/// handle's metadata image and return a clone of it.
async fn wait_partition_record_known(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
) -> PartitionRecord {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(isr) = handle.partition_isr_for_test(topic, partition)
            && let Some(leader) = handle.partition_leader_for_test(topic, partition)
        {
            // Reconstruct the record from the accessors we have.
            return PartitionRecord {
                topic: topic.to_string(),
                partition,
                leader,
                // We don't have a direct `replicas` accessor, but the
                // ISR is enough for our purposes (replicas=[1,2] is
                // well-known from the CreateTopics call with rf=2 on a
                // 3-broker cluster where the first two brokers are the
                // natural assignment).
                replicas: vec![1, 2],
                isr,
                leader_epoch: 0, // bumped by the forged record, not critical
            };
        }
        assert!(
            Instant::now() <= deadline,
            "partition {topic}-{partition} record never appeared within 15s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
