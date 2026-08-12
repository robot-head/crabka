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
// `clippy::unnecessary_unwrap` fires on the `l1.unwrap()` inside `if l1.is_some()`
// and its span computation ICEs in annotate-snippets on Rust 1.95.
// `clippy::too_many_lines` fires on the auto-rebalance integration test body.

//! Broker-side integration tests for the operator-triggered `ElectLeaders` RPC.
//!
//! The tests drive the wire path end-to-end with a Rust PLAINTEXT client. They
//! then read the resulting partition state through `BrokerHandle` test
//! accessors.
//!
//! Both tests use a **3-broker PLAINTEXT cluster** and not a 2-broker SASL
//! cluster, for two reasons:
//!
//! * A 2-broker raft cluster cannot form a quorum (2/2) when one broker is
//!   dead. The automatic partition-leader election and the metadata commits
//!   that these tests need thus never succeed. A 3-broker cluster keeps quorum
//!   (2/3) with one dead node, which is enough for both test scenarios.
//!
//! * The compatibility shim of the authorizer maps an empty `super_users` list
//!   and zero ACLs to Allow. The test can thus exercise the full `ElectLeaders`
//!   wire path without a SASL handshake, which keeps the test helpers simpler.
//!
//! These tests are gated to non-Windows to match the multi-broker test
//! convention from slices 10b/12b. The openraft `debug_assert!` races on the
//! hosted Windows task scheduler are unrelated to the protocol under test.

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerHandle, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use crabka_metadata::{
    AclEntry, AclOperation, LeaderEpoch, MetadataRecord, PartitionRecord, PatternType,
    PermissionType, ResourceType,
};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
    },
};
use crabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

mod support;

const ELECT_LEADERS_VERSION: i16 = 2;

/// Shared cluster lock for every test in this binary.
///
/// The lock serializes the tests onto one 3-broker cluster at a time. It
/// mirrors the locks in `quorum.rs` and `leader_election.rs`. Without it, the
/// static 3-voter clusters of the tests boot at the same time on the same
/// loopback with short raft timings. They then starve each other of elections
/// and of ISR re-admission, which shows as intermittent `FENCED_LEADER_EPOCH`
/// churn.
fn cluster_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Minimal wire helpers — bare TCP on PLAINTEXT, no SASL.
// ─────────────────────────────────────────────────────────────────────────────

/// Runs one length-prefixed request and response exchange.
///
/// The exchange uses a **PLAINTEXT** connection. This function encodes a Kafka
/// request header v1, which is non-flexible, or v2, which is flexible. It then
/// writes the frame, reads one response frame, strips the response header, and
/// returns the body bytes.
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

/// Drives `ElectLeaders` over a fresh PLAINTEXT connection.
///
/// The compat shim of the authorizer maps no `super_users` and no ACLs to
/// Allow, so this request passes without SASL. This function asserts that the
/// top-level `error_code == 0`. It returns the per-partition
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

    assert!(
        resp.error_code == 0,
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

/// Waits until `handle` sees `(topic, partition)` in its image.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

/// Waits until `handle` reports `leader` as the leader for `(topic, partition)`.
async fn wait_partition_leader(handle: &BrokerHandle, topic: &str, partition: i32, leader: u64) {
    handle
        .wait_for_image(|img| img.partition(topic, partition).map(|p| p.leader.0) == Some(leader))
        .await;
}

/// Waits until the ISR for `(topic, partition)` contains `node`.
async fn wait_isr_contains(handle: &BrokerHandle, topic: &str, partition: i32, node: u64) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&crabka_broker::NodeId(node)))
        })
        .await;
}

/// Waits until the ISR for `(topic, partition)` is exactly `expected`.
async fn wait_partition_isr_only(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    expected: &[u64],
) {
    let expected_set: std::collections::HashSet<u64> = expected.iter().copied().collect();
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                let actual_set: std::collections::HashSet<u64> =
                    p.isr.iter().map(|n| n.0).collect();
                actual_set == expected_set
            })
        })
        .await;
}

/// Polls until the ISR of the partition contains `member`.
///
/// [`wait_partition_isr_only`] asserts an exact set. This function asserts
/// membership only. It thus accepts a live caught-up replica that the broker
/// admits or re-admits next to `member`.
async fn wait_partition_isr_contains(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    member: u64,
) {
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition)
                .is_some_and(|p| p.isr.contains(&crabka_broker::NodeId(member)))
        })
        .await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

/// A 3-broker PLAINTEXT cluster with an rf=2 topic where `replicas = [1, 2]`.
///
/// Scenario:
/// 1. Kill broker 1, the preferred replica. Broker 3 keeps the raft quorum
///    (2/3).
/// 2. Broker 2 becomes partition leader through the automatic on-broker-dead
///    path.
/// 3. Revive broker 1 with Rejoin. It catches up on replication and expands
///    back into the ISR.
/// 4. Send `ElectLeaders Preferred` with `election_type=0` over the wire.
/// 5. Assert per-partition `error_code` = 0. Poll until leader == 1 again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_election_via_wire_returns_success() {
    let _g = cluster_lock().lock().await;

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

    let initial_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap_or(1);
    eprintln!("initial partition leader: {initial_leader}");

    // Kill broker 1 (index 0). Raft quorum {2, 3} can still commit.
    let (dead_h, dead_cfg, dead_dir) = cluster.remove(0);
    dead_h.shutdown().await;

    // Wait for the surviving cluster to elect a new partition leader
    // (i.e., not broker 1).
    cluster[0]
        .0
        .wait_until_partition_leader_changed("foo-preferred", 0, crabka_broker::NodeId(1))
        .await;
    let new_leader = cluster[0]
        .0
        .partition_leader_for_test("foo-preferred", 0)
        .unwrap();
    eprintln!("new partition leader after broker 1 death: {new_leader}");
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
        let leader = cluster[0].0.wait_until_controller_leader().await;
        let pos = cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("raft leader must be one of the surviving brokers");
        cluster[pos].1.listen_addr
    };
    eprintln!("sending ElectLeaders Preferred to raft leader at {elect_addr}");

    // Now send ElectLeaders Preferred (election_type=0). Broker 1 is the
    // preferred replica (replicas[0]) and is now back in ISR and alive.
    let result = drive_elect_leaders(elect_addr, "foo-preferred", vec![0], 0).await;
    assert!(
        result == vec![(0, 0)],
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

/// A 3-broker PLAINTEXT cluster with an rf=2 topic where `replicas = [1, 2]`.
///
/// The scenario injects metadata to simulate a dead ISR. It does not break the
/// raft quorum.
///
/// 1. Submit a `PartitionRecord` with `isr=[99]` directly. Broker 99 does not
///    exist, so liveness reports it as dead.
/// 2. Broker 1 is in the replicas but not in the ISR. It is alive, and the
///    controller knows its heartbeat.
/// 3. Send `ElectLeaders Unclean` with `election_type=1` over the wire.
/// 4. The handler checks whether any ISR member is alive. Member 99 is not
///    alive, so the partition is eligible for an unclean election. The first
///    alive replica in [1, 2] is broker 1, so the handler elects broker 1 as
///    leader and sets ISR=[1].
/// 5. Assert per-partition `error_code` = 0 and poll until leader == 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_election_via_wire_picks_alive_replica() {
    let _g = cluster_lock().lock().await;

    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // ElectLeaders is a controller operation. Send it to the current raft
    // leader, whose liveness registry receives the broker heartbeats.
    let controller_id = cluster[0].0.wait_until_controller_leader().await;
    let addr = cluster
        .iter()
        .find(|(_, cfg, _)| cfg.node_id == controller_id)
        .expect("raft leader must be one of the brokers")
        .1
        .listen_addr;
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

    // Inject a PartitionRecord whose leader AND ISR are the dead phantom 99.
    // Broker 99 never registered/heartbeated, so liveness.is_alive(99)==false.
    //
    // Crucially, the leader must be a DEAD broker, not a live replica: a live
    // leader runs ISR management and would re-admit itself / caught-up replicas
    // before the manual election ran, healing the forged state (the partition
    // would then have a live ISR member → ELECTION_NOT_NEEDED). With a dead
    // leader (99, not in replicas) no live broker owns the partition, so the
    // forged ISR=[99] persists until the operator's unclean election. (Nothing
    // auto-elects either: failover is transition-triggered on AliveToDead, and
    // 99 never went alive→dead.)
    let forged = MetadataRecord::V1Partition(PartitionRecord {
        topic: "foo-unclean".to_string(),
        partition: 0,
        leader: crabka_broker::NodeId(99),
        replicas: pr_before.replicas.clone(),
        isr: vec![crabka_broker::NodeId(99)],
        leader_epoch: pr_before.leader_epoch.next(),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    });
    h0.submit_metadata_record_for_test(forged)
        .await
        .expect("inject forged PartitionRecord");

    // Wait for the injected ISR to propagate to the image. With a dead leader
    // it stays [99] (no live leader to repair it).
    wait_partition_isr_only(h0, "foo-unclean", 0, &[99]).await;

    // Drive ElectLeaders Unclean (election_type=1).
    // The algorithm finds: ISR=[99] — all dead → unclean eligible.
    // First alive in replicas=[1,2] → broker 1 → new leader=1, isr=[1].
    let result = drive_elect_leaders(addr, "foo-unclean", vec![0], 1).await;
    assert!(
        result == vec![(0, 0)],
        "expected error_code=0 for UNCLEAN election; got {result:?}"
    );

    // Poll until the metadata image reflects the new leader. The unclean
    // election makes broker 1 the leader with ISR=[1]; we assert leadership and
    // broker 1's ISR membership rather than an exact ISR={1}, because once
    // broker 1 leads, the other live replica (broker 2, caught up on the empty
    // log) is legitimately re-admitted to the ISR — asserting exactly [1] would
    // race that re-admission.
    wait_partition_leader(h0, "foo-unclean", 0, 1).await;
    wait_partition_isr_contains(h0, "foo-unclean", 0, 1).await;

    // Clean up.
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Creates a topic on a PLAINTEXT broker.
///
/// The compat shim of the authorizer lets the request through because there
/// are no `super_users` and no ACLs.
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

/// Polls until the metadata image of the handle shows the partition record.
///
/// The record is the one for `(topic, partition)`. This function returns a
/// clone of it.
async fn wait_partition_record_known(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
) -> PartitionRecord {
    handle.wait_until_partition_present(topic, partition).await;
    // A present partition record implies both accessors are populated.
    let isr = handle
        .partition_isr_for_test(topic, partition)
        .expect("partition present implies ISR known");
    let leader = handle
        .partition_leader_for_test(topic, partition)
        .expect("partition present implies leader known");
    // Reconstruct the record from the accessors we have.
    PartitionRecord {
        topic: topic.to_string(),
        partition,
        leader: crabka_broker::NodeId(leader),
        // We don't have a direct `replicas` accessor, but the
        // ISR is enough for our purposes (replicas=[1,2] is
        // well-known from the CreateTopics call with rf=2 on a
        // 3-broker cluster where the first two brokers are the
        // natural assignment).
        replicas: vec![crabka_broker::NodeId(1), crabka_broker::NodeId(2)],
        isr: isr.into_iter().map(crabka_broker::NodeId).collect(),
        leader_epoch: LeaderEpoch(0), // bumped by the forged record, not critical
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// T9a: non_super_user_without_acl_denied
// ─────────────────────────────────────────────────────────────────────────────

/// Single-broker SASL/PLAIN cluster.
///
/// alice authenticates with PLAIN credentials but has **no** ACLs. The test
/// seeds a dummy ACL to disable the compat shim, which allows every operation
/// while `image.acls` is empty. alice then sends `ElectLeaders Preferred` for
/// topic "foo-auth-test" partition 0. Each row must carry
/// `CLUSTER_AUTHORIZATION_FAILED (31)`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_without_acl_denied() {
    let log_dir = tempfile::tempdir().unwrap();

    // Build a single-broker SASL_PLAINTEXT config.
    // admin is the super-user so the compat shim stays off once an ACL
    // exists; alice has credentials but no ACLs.
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
    cfg.plain_credentials
        .insert("admin".to_string(), "admin-secret".to_string());
    cfg.plain_credentials
        .insert("alice".to_string(), "alice-secret".to_string());
    cfg.super_users = std::iter::once("admin".to_string()).collect();
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; default is `AllowAllAuthorizer`.
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Create the topic as admin (rf=1 fine for a single-broker cluster).
    create_topic_sasl_plain(addr, "admin", b"admin-secret", "foo-auth-test", 1, 1).await;
    wait_partition_exists(&handle, "foo-auth-test", 0).await;

    // Seed a dummy ACL so the compat shim is disabled. The ACL itself
    // is irrelevant — any non-empty `image.acls` flips the shim off and
    // forces the authorizer to evaluate every request.
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
    // is guaranteed to be in the image before we proceed. A small extra
    // wait absorbs any race on very slow CI runners.
    // intentional: defensive barrier for ACL visibility to the authorizer;
    // no ACL-image awaiter/metric exists, and the retry loop below is the
    // real convergence gate.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drive ElectLeaders Preferred as alice. Because the compat shim is
    // now off (image.acls is non-empty) and alice has no Cluster Alter
    // ACL, the handler must return CLUSTER_AUTHORIZATION_FAILED (31)
    // for every requested partition.
    //
    // If the shim were still active we'd see error_code=0 (allowed).
    // Retry up to 5s to absorb raft apply latency on slow runners.
    let deadline_auth = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_elect_leaders_sasl_plain(
            addr,
            "alice",
            b"alice-secret",
            "foo-auth-test",
            vec![0],
            0,
        )
        .await;
        // If we see 31, the shim is off and we're done.
        if r.iter().all(|(_, ec)| *ec == 31) {
            break r;
        }
        assert!(
            Instant::now() <= deadline_auth,
            "ACL shim still active or wrong error after 5s; got {r:?}"
        );
        // intentional: backoff between bounded RPC-response retries that
        // re-drive the SASL ElectLeaders wire path to observe the authorizer's
        // decision; the awaited state is on the request path, not in the
        // metadata image, and has no metric.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    // Per-row error code must be 31 (CLUSTER_AUTHORIZATION_FAILED).
    assert!(
        resp == vec![(0, 31)],
        "expected CLUSTER_AUTHORIZATION_FAILED (31) for alice; got {resp:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// T9b: auto_rebalance_restores_preferred_leader
// ─────────────────────────────────────────────────────────────────────────────

/// A 3-broker PLAINTEXT cluster with automatic leader rebalance on.
///
/// The cluster sets `auto_leader_rebalance_enable = true`,
/// `leader_imbalance_check_interval = crabka_units::secs(1)`, and `leader_imbalance_per_broker = crabka_units::percent(0)`.
///
/// Scenario:
/// 1. Create an rf=2 topic over the wire. With 3 registered brokers,
///    round-robin assigns `replicas = [1, 2]`, so broker 1 is the preferred
///    leader.
/// 2. Shut broker 1 down. Broker 2 becomes partition leader.
/// 3. Revive broker 1 with Rejoin. It catches up into the ISR.
/// 4. The background rebalance ticker runs with interval=1s and threshold=0%.
///    It fires within about 2 ticks and submits `ElectLeaders Preferred`
///    internally.
/// 5. Within 15s, broker 1 must be the partition leader again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_rebalance_restores_preferred_leader() {
    let _g = cluster_lock().lock().await;

    support::init_tracing();

    // ── Phase 1: start a 3-broker cluster with rebalance enabled. ─────────
    // We can't pass rebalance config overrides through `start_n_node`, so we
    // replicate its static multi-voter bring-up here and apply the rebalance
    // fields after building each BrokerConfig. All three brokers boot in
    // `Bootstrap` mode with the same static voter set (KIP-595 Slice 3c);
    // KIP-853 auto-join is Slice 5.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(3).await;
    let voters: Vec<(u64, std::net::SocketAddr)> = (0u64..3)
        .map(|i| (i + 1, controller_addrs[usize::try_from(i).unwrap()]))
        .collect();

    let dir0 = tempfile::TempDir::new().unwrap();
    let dir1 = tempfile::TempDir::new().unwrap();
    let dir2 = tempfile::TempDir::new().unwrap();

    let mut cfg0 = support::broker_config(
        0,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir0.path(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    cfg0.features.auto_leader_rebalance_enable = true;
    cfg0.leader_imbalance_check_interval = crabka_units::secs(1);
    cfg0.leader_imbalance_per_broker = crabka_units::percent(0);

    let mut cfg1 = support::broker_config(
        1,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir1.path(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    cfg1.features.auto_leader_rebalance_enable = true;
    cfg1.leader_imbalance_check_interval = crabka_units::secs(1);
    cfg1.leader_imbalance_per_broker = crabka_units::percent(0);

    let mut cfg2 = support::broker_config(
        2,
        &client_addrs,
        &controller_addrs,
        &voters,
        dir2.path(),
        crabka_broker::BootstrapMode::Bootstrap,
    );
    cfg2.features.auto_leader_rebalance_enable = true;
    cfg2.leader_imbalance_check_interval = crabka_units::secs(1);
    cfg2.leader_imbalance_per_broker = crabka_units::percent(0);

    // Start all three statically; they elect among themselves over the wire.
    let mut client_ls = client_listeners.into_iter();
    let mut ctrl_ls = controller_listeners.into_iter();
    let (client0, controller0) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client1, controller1) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let (client2, controller2) = (client_ls.next().unwrap(), ctrl_ls.next().unwrap());
    let cfg1_clone = cfg1.clone();
    let cfg2_clone = cfg2.clone();
    let join1 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg1_clone, Some(controller1), Some(client1)).await
    });
    let join2 = tokio::spawn(async move {
        Broker::start_with_listeners(cfg2_clone, Some(controller2), Some(client2)).await
    });
    let h0 = Broker::start_with_listeners(cfg0.clone(), Some(controller0), Some(client0))
        .await
        .expect("broker 1 start");
    let h1 = join1.await.expect("spawn join1").expect("broker 2 start");
    let h2 = join2.await.expect("spawn join2").expect("broker 3 start");

    // Wait for all 3 brokers to see each other registered.
    h0.wait_until_brokers_registered(3).await;
    h1.wait_until_brokers_registered(3).await;
    h2.wait_until_brokers_registered(3).await;

    let addr = h0.listen_addr();
    let topic = "foo-rebalance";

    // ── Phase 2: create rf=2 topic via PLAINTEXT wire. ────────────────────
    // With 3 registered brokers sorted [1, 2, 3] and rf=2, the round-robin
    // assignment for partition 0 is replicas=[1, 2]. Broker 1 is preferred.
    create_topic_plaintext(addr, topic, 1, 2).await;

    wait_partition_exists(&h0, topic, 0).await;
    wait_partition_exists(&h1, topic, 0).await;
    // Wait for broker 1 to be the initial leader (as preferred replica).
    wait_partition_leader(&h0, topic, 0, 1).await;
    eprintln!("initial partition leader is broker 1 (preferred)");

    // ── Phase 3: kill broker 1 (preferred leader). ────────────────────────
    h0.shutdown().await;
    eprintln!("broker 1 shut down; waiting for failover");

    // Wait for broker 2 or 3 to report a new leader (not broker 1).
    h1.wait_until_partition_leader_changed(topic, 0, crabka_broker::NodeId(1))
        .await;
    eprintln!(
        "new leader after broker 1 death: {:?}",
        h1.partition_leader_for_test(topic, 0)
    );

    // ── Phase 4: revive broker 1 (Rejoin). ───────────────────────────────
    let mut rejoin_cfg = cfg0.clone();
    rejoin_cfg.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    let h0_new = Broker::start(rejoin_cfg).await.expect("rejoin broker 1");
    eprintln!("broker 1 rejoined; waiting for ISR expansion");

    // Wait for broker 1 to be back in the ISR (visible from broker 2's image).
    wait_isr_contains(&h1, topic, 0, 1).await;
    eprintln!("broker 1 back in ISR; waiting for auto-rebalance tick to fire");

    // ── Phase 5: wait for auto-rebalance to restore broker 1 as leader. ──
    // The ticker fires every 1s with threshold=0%; observe the committed
    // metadata image on a surviving broker reflect broker 1 as leader again.
    wait_partition_leader(&h1, topic, 0, 1).await;
    eprintln!("auto-rebalance restored preferred leader (broker 1)");

    // Clean up.
    h0_new.shutdown().await;
    h1.shutdown().await;
    h2.shutdown().await;
    drop(dir0);
    drop(dir1);
    drop(dir2);
}

// ─────────────────────────────────────────────────────────────────────────────
// T9 helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Creates a topic with SASL/PLAIN.
///
/// The auth-deny test uses this helper, because its listener is
/// `SASL_PLAINTEXT` and not PLAINTEXT.
async fn create_topic_sasl_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
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
    let mut stream = sasl_plain_authenticate(addr, user, password)
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
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Drives `ElectLeaders` over a SASL/PLAIN authenticated connection.
///
/// This function returns the per-partition `(partition_id, error_code)` rows
/// for the given topic. It does **not** assert the top-level `error_code`, so
/// the caller can examine per-row auth failures.
async fn drive_elect_leaders_sasl_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
    partitions: Vec<i32>,
    election_type: i8,
) -> Vec<(i32, i16)> {
    let mut stream = sasl_plain_authenticate(addr, user, password)
        .await
        .expect("SASL authenticate for ElectLeaders");
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

/// Opens a TCP stream to `addr` and authenticates it.
///
/// The stream drives `ApiVersions` → `SaslHandshake(PLAIN)` →
/// `SaslAuthenticate(\0user\0password)`. This function returns the
/// authenticated stream. It mirrors the equivalent helper in
/// `tests/acl_handlers.rs`.
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
