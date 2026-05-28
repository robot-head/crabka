//! KIP-392 end-to-end fetch-from-follower (rack-aware reads) integration
//! test. Boots a 2-broker cluster in two different racks, both configured
//! with the rack-aware replica selector, and asserts the full consumer
//! redirect path:
//!
//!   1. A consumer Fetch to the LEADER carrying the follower's rack gets a
//!      `preferred_read_replica` pointing at the same-rack follower.
//!   2. A consumer Fetch sent to that FOLLOWER returns the committed
//!      records (the follower tracks the leader-reported high watermark).
//!   3. A consumer Fetch to the LEADER carrying the leader's own rack gets
//!      `preferred_read_replica == -1` (read from the leader).
//!
//! Gated `#[cfg(not(target_os = "windows"))]` to mirror `replication.rs` /
//! `quorum.rs` (openraft `debug_assert!` race on the hosted Windows runner).
//!
//! The shared `support` harness has no per-broker config customizer, so
//! this test inlines the harness's bootstrap-then-join start loop (option
//! (b) in the task plan) to set a distinct `rack` and the `RackAware`
//! selector on each broker. The blast radius stays in this file.

#![cfg(not(target_os = "windows"))]
// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` in test files; the
// sibling integration tests allow it wholesale for the same reason.
#![allow(clippy::pedantic)]
#![allow(clippy::manual_assert, clippy::cast_possible_truncation)]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crabka_broker::replica_selector::ReplicaSelectorKind;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};
use tempfile::TempDir;
use tokio::sync::Mutex;

mod support;

/// Serialize the whole test binary: a 2-broker loopback cluster plus short
/// raft timings starves the openraft election if run concurrently with
/// anything else in the same binary. Same rationale as
/// `replication.rs::cluster_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

const N_RECORDS: i32 = 5;
const RACK_A: &str = "rack-a"; // broker 1 (leader)
const RACK_B: &str = "rack-b"; // broker 2 (follower)

/// Boot a 2-broker cluster where each broker carries a distinct `rack` and
/// the `RackAware` replica selector. Mirrors
/// `support::start_n_node`'s bootstrap-then-join pattern, but injects the
/// KIP-392 config before `Broker::start`. `racks[i]` is broker `i+1`'s rack.
async fn start_rack_aware_cluster(racks: &[&str]) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    support::init_tracing();
    let n = racks.len();

    let (client_addrs, controller_addrs) = support::bind_and_drop_ports(n).await;
    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (u64::try_from(i + 1).unwrap(), controller_addrs[i]))
        .collect();

    let with_rack = |i: usize, mode: BootstrapMode, dir: &std::path::Path| -> BrokerConfig {
        let mut cfg =
            support::broker_config(i, &client_addrs, &controller_addrs, &voters, dir, mode);
        cfg.rack = Some(racks[i].to_string());
        cfg.replica_selector = ReplicaSelectorKind::RackAware;
        cfg
    };

    // Phase 1: bootstrap broker 0 alone.
    let dir0 = TempDir::new().unwrap();
    let cfg0 = with_rack(0, BootstrapMode::Bootstrap, dir0.path());
    let broker0 = Broker::start(cfg0.clone())
        .await
        .expect("bootstrap broker 0");

    // Phase 2: spawn brokers 1..n in Join mode; their Broker::start blocks
    // on watch_leader until we promote them below.
    let mut join_handles = Vec::with_capacity(n.saturating_sub(1));
    let mut join_metas: Vec<(TempDir, BrokerConfig)> = Vec::with_capacity(n.saturating_sub(1));
    for i in 1..n {
        let dir = TempDir::new().unwrap();
        let cfg = with_rack(i, BootstrapMode::Join, dir.path());
        let cfg_clone = cfg.clone();
        join_handles.push(tokio::spawn(async move { Broker::start(cfg_clone).await }));
        join_metas.push((dir, cfg));
    }

    // Phase 3: add each joiner as a learner, then promote all to voters.
    for (idx, addr) in controller_addrs.iter().enumerate().skip(1).take(n - 1) {
        broker0
            .add_learner(u64::try_from(idx + 1).unwrap(), *addr)
            .await
            .expect("add_learner");
    }
    let target_voters: BTreeSet<u64> = (1..=u64::try_from(n).unwrap()).collect();
    broker0
        .change_membership(target_voters)
        .await
        .expect("change_membership");

    let mut out = Vec::with_capacity(n);
    out.push((broker0, cfg0, dir0));
    for (h, (dir, cfg)) in join_handles.into_iter().zip(join_metas) {
        let broker = h
            .await
            .expect("broker spawn join")
            .expect("join broker start");
        out.push((broker, cfg, dir));
    }
    out
}

async fn wait_for_partition_on_all(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    topic: &str,
    partition: i32,
) {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut all = true;
        for (h, _, _) in cluster {
            if !h.has_partition(topic, partition).await {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        if Instant::now() > deadline {
            panic!("topic '{topic}' didn't propagate to all brokers within 2 min");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll until `(topic, partition)` has `leader` as leader and an ISR equal
/// to `expected_isr` (as a set), observed via `handle`.
async fn wait_leader_and_isr(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
    expected_isr: &[u64],
) {
    let want: BTreeSet<u64> = expected_isr.iter().copied().collect();
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let cur_leader = handle.partition_leader_for_test(topic, partition);
        let cur_isr = handle
            .partition_isr_for_test(topic, partition)
            .map(|v| v.into_iter().collect::<BTreeSet<u64>>());
        if cur_leader == Some(leader) && cur_isr.as_ref() == Some(&want) {
            return;
        }
        if Instant::now() > deadline {
            panic!(
                "{topic}-{partition} didn't reach leader={leader} isr={expected_isr:?} \
                 within 2 min; leader={cur_leader:?} isr={cur_isr:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn record_batch(n: i32) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: (n - 1).max(0),
        records: (0..n)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// Build a consumer Fetch (replica_id = -1) for a single (topic, partition)
/// at `offset`, carrying `rack_id`. The shared `Client` negotiates the
/// broker's max Fetch version (>= 11), so `rack_id` is serialized on the
/// wire and `preferred_read_replica` is present in the decoded response.
fn consumer_fetch(topic: &str, topic_id: WireUuid, offset: i64, rack: &str) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 800,
        min_bytes: 0,
        max_bytes: 10_485_760,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        rack_id: rack.to_string(),
        topics: vec![FetchTopic {
            topic: topic.into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: offset,
                current_leader_epoch: -1,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rack_aware_consumer_is_redirected_to_same_rack_follower() {
    let _g = cluster_lock().lock().await;

    // Step 1: 2 brokers, broker 1 = rack-a (leader), broker 2 = rack-b
    // (follower), both RackAware.
    let cluster = start_rack_aware_cluster(&[RACK_A, RACK_B]).await;
    support::wait_for_all_brokers_registered(&cluster, 2).await;

    let leader_handle = &cluster[0].0; // broker 1
    let follower_handle = &cluster[1].0; // broker 2
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let follower_addr = cluster[1].1.listen_addr.to_string();

    // Step 2: CreateTopics("t", partitions=1, rf=2) against the leader.
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "t".into(),
                num_partitions: 1,
                replication_factor: 2,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 0, "CreateTopics t");
    let topic_id = resp.topics[0].topic_id;

    wait_for_partition_on_all(&cluster, "t", 0).await;

    // With rf=2 / partition 0, round-robin placement makes node 1 the
    // leader. Wait until leader == 1 and ISR == {1,2} from the leader's
    // own image; fail fast if the cluster assigned differently so the
    // step-5 leader fetch genuinely goes to the leader.
    wait_leader_and_isr(leader_handle, "t", 0, 1, &[1, 2]).await;
    let leader_id = leader_handle.partition_leader_for_test("t", 0).unwrap();
    let isr = leader_handle.partition_isr_for_test("t", 0).unwrap();
    assert_eq!(leader_id, 1, "leader must be broker 1");
    // Follower = the in-sync replica that isn't the leader.
    let follower_id = *isr
        .iter()
        .find(|&&r| r != leader_id)
        .expect("a non-leader ISR member");
    assert_eq!(follower_id, 2, "follower (rack-b) must be broker 2");
    let follower_node = i32::try_from(follower_id).unwrap();

    // Step 3: produce N records to the leader with acks=all so they commit.
    let producer = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let prod = producer
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch(N_RECORDS).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    assert_eq!(
        prod.responses[0].partition_responses[0].error_code, 0,
        "Produce acks=all"
    );

    // Step 4: wait for the follower (broker 2) to replicate to N. The
    // follower's local log catching up to N is the prerequisite for it to
    // serve the consumer fetch in step 6.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let leo = follower_handle
            .local_log_end_offset("t", 0)
            .await
            .unwrap_or(0);
        if leo >= i64::from(N_RECORDS) {
            break;
        }
        if Instant::now() > deadline {
            panic!("follower (broker 2) didn't replicate to {N_RECORDS} within 15s; leo={leo}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Step 5: consumer Fetch to the LEADER with rack_id=rack-b → the
    // selector should redirect to the same-rack follower (node 2).
    let leader_client = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let r_leader = leader_client
        .send(consumer_fetch("t", topic_id, 0, RACK_B))
        .await
        .expect("Fetch to leader (rack-b)");
    let part = &r_leader.responses[0].partitions[0];
    assert_eq!(part.partition_index, 0);
    assert_eq!(
        part.preferred_read_replica, follower_node,
        "leader should redirect a rack-b consumer to the rack-b follower (node {follower_node})"
    );

    // Step 6: consumer Fetch to the FOLLOWER (broker 2) with rack_id=rack-b.
    // Bounded retry until the follower's HW has advanced enough to serve
    // all N records (HW propagation can lag the local log slightly).
    let follower_client = Client::builder()
        .bootstrap(follower_addr.clone())
        .build()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let got = loop {
        let r = follower_client
            .send(consumer_fetch("t", topic_id, 0, RACK_B))
            .await
            .expect("Fetch to follower (rack-b)");
        let p = &r.responses[0].partitions[0];
        assert_eq!(p.error_code, 0, "follower fetch error_code");
        let count = p
            .records
            .as_ref()
            .and_then(|rp| rp.as_v2())
            .map_or(0, |b| b.records.len());
        if count >= N_RECORDS as usize {
            break count;
        }
        if Instant::now() > deadline {
            panic!("follower didn't serve {N_RECORDS} records within 5s; last count={count}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(got, N_RECORDS as usize, "follower returned all N records");

    // Step 7: sanity — consumer Fetch to the LEADER with rack_id=rack-a
    // (same rack as the leader) yields no redirect.
    let r_same = leader_client
        .send(consumer_fetch("t", topic_id, 0, RACK_A))
        .await
        .expect("Fetch to leader (rack-a)");
    let part_same = &r_same.responses[0].partitions[0];
    assert_eq!(
        part_same.preferred_read_replica, -1,
        "same-rack consumer must read from the leader (no redirect)"
    );

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
