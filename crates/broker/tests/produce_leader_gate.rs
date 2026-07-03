//! Multi-broker coverage for the Produce leadership gate.
//!
//! Kafka semantics: only the partition LEADER accepts a Produce. A Produce
//! misrouted to a non-leader must be rejected with `NOT_LEADER_OR_FOLLOWER`
//! (6) — and crucially must NOT be appended to a local follower replica —
//! so the client refreshes metadata and re-routes to the real leader.
//!
//! Two discriminating cases:
//!
//!  * **rf=3 (follower replica present):** every broker hosts a replica, so a
//!    Produce to a non-leader lands on a *follower's* local log if the handler
//!    skips the leadership check. Pre-fix the broker silently appended to the
//!    follower (`error_code=0`, follower log grew, leader never saw the record) —
//!    silent data loss. The fix returns 6 and leaves the follower log untouched.
//!
//!  * **rf=1 (no replica on the target):** the non-leader broker holds no
//!    replica at all. Pre-fix this returned `UNKNOWN_TOPIC_OR_PARTITION` (3)
//!    because the local registry lookup missed. Kafka returns 6 (the partition
//!    exists cluster-wide; this broker just isn't its leader), which the fix
//!    now does by consulting the metadata image rather than the local registry.
//!
//! The same Produce sent to the actual leader must succeed (code 0, record
//! durably stored), proving the leader path is unaffected.
//!
//! Windows-gated like the other multi-broker tests (openraft `debug_assert!`
//! races on the hosted Windows scheduler).

#![allow(clippy::too_many_lines)]

use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_broker::BrokerHandle;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tokio::sync::Mutex;

mod support;

/// Serialize the multi-broker tests in this binary: each boots a 3-node
/// loopback cluster, and running them concurrently exhausts ephemeral ports
/// and starves openraft election timing. Same rationale as the `cluster_lock`
/// in `producer_leader_routing.rs`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn one_record_batch(v: &str) -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Send a single one-record Produce (`acks=1`) for `(topic, partition)` over
/// `client`'s bootstrap connection — i.e. directly at whichever broker the
/// client was built against, bypassing any leader routing. Returns
/// `(error_code, current_leader_id)`.
async fn produce_one(
    client: &Client,
    topic: &str,
    topic_id: WireUuid,
    partition: i32,
    value: &str,
) -> (i16, i32) {
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: partition,
                    records: Some(one_record_batch(value).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce round-trip");
    let pr = &resp.responses[0].partition_responses[0];
    (pr.error_code, pr.current_leader.leader_id)
}

/// Block until `broker` has materialized its LOCAL replica for
/// `(topic, partition)` — i.e. the supervisor reconcile has turned the
/// metadata image into a live writer-actor (`PartitionRegistry::get` returns
/// `Some`). Producing directly at a broker before this is done races its
/// image/replica catch-up: the metadata image is applied per-broker and a
/// follower lags the controller, so a Produce can surface `UNKNOWN_TOPIC_ID`
/// (100, the broker's image hasn't applied this `topic_id` yet — it resolves
/// the request by id before the leadership gate) or a transient
/// `NOT_LEADER_OR_FOLLOWER` (6, the image names this broker leader but the
/// writer-actor isn't spun up yet). A materialized local replica implies the
/// image already holds the topic + partition, so both races are closed.
/// Panics if the replica never appears within 30s.
async fn wait_for_local_replica(broker: &BrokerHandle, topic: &str, partition: i32) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while broker
        .local_log_end_offset(topic, partition)
        .await
        .is_none()
    {
        assert!(
            Instant::now() <= deadline,
            "broker never materialized a local replica for {topic}/{partition}"
        );
        // intentional: gates on the LOCAL writer-actor (PartitionRegistry)
        // being materialized by the supervisor reconcile, which lags the
        // metadata image. No image-based awaiter observes local-registry
        // materialization, so an event-driven signal isn't available here.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn produce_to_non_leader_is_rejected() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let bootstrap = cluster[0].1.listen_addr.to_string();
    let admin = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    // rf=3 topic (a replica on every broker) and a rf=1, 6-partition topic
    // (each partition on exactly one broker, so non-leaders hold no replica).
    let cr = admin
        .send(CreateTopicsRequest {
            topics: vec![
                CreatableTopic {
                    name: "gate-rf3".into(),
                    num_partitions: 1,
                    replication_factor: 3,
                    ..Default::default()
                },
                CreatableTopic {
                    name: "gate-rf1".into(),
                    num_partitions: 6,
                    replication_factor: 1,
                    ..Default::default()
                },
            ],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        cr.topics.iter().all(|t| t.error_code == 0),
        "create: {cr:?}"
    );
    // v13 Produce drops topic.name and carries only topic_id; echo the ids.
    let rf3_id = cr
        .topics
        .iter()
        .find(|t| t.name == "gate-rf3")
        .unwrap()
        .topic_id;
    let rf1_id = cr
        .topics
        .iter()
        .find(|t| t.name == "gate-rf1")
        .unwrap()
        .topic_id;

    // Wait until node 1's image knows every partition's leader. Mirrors the
    // `partition_leader_for_test(..).is_some()` predicate exactly: partition
    // present in the image AND its leader field elected (non-zero).
    cluster[0]
        .0
        .wait_for_image(|img| {
            img.partition("gate-rf3", 0)
                .is_some_and(|pr| pr.leader.get() != 0)
                && (0..6).all(|p| {
                    img.partition("gate-rf1", p)
                        .is_some_and(|pr| pr.leader.get() != 0)
                })
        })
        .await;

    // ───────────────────────────────────────────────────────────────────
    // Case A: rf=3 — Produce to a NON-leader that DOES hold a follower replica.
    // ───────────────────────────────────────────────────────────────────
    let rf3_leader = cluster[0]
        .0
        .partition_leader_for_test("gate-rf3", 0)
        .unwrap();
    let follower_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != rf3_leader)
        .expect("a non-leader broker exists at rf=3");
    let follower_node = cluster[follower_idx].0.node_id();
    let follower_addr = cluster[follower_idx].1.listen_addr.to_string();

    // Wait for the follower to materialize its LOCAL replica (supervisor
    // reconcile lags the controller image). Without a local replica the rf=3
    // case would degenerate into the rf=1 case and not prove the
    // "don't append to a follower" property.
    wait_for_local_replica(&cluster[follower_idx].0, "gate-rf3", 0).await;
    let follower_leo_before = cluster[follower_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .await
        .expect("follower hosts gate-rf3");

    let follower_client = Client::builder()
        .bootstrap(follower_addr)
        .build()
        .await
        .unwrap();
    let (code, leader_hint) =
        produce_one(&follower_client, "gate-rf3", rf3_id, 0, "rf3-to-follower").await;
    assert!(
        code == 6,
        "rf=3 Produce to follower node{follower_node} (leader=node{rf3_leader}) must be \
         NOT_LEADER_OR_FOLLOWER (6); got {code}"
    );
    assert!(
        leader_hint == i32::try_from(rf3_leader).unwrap(),
        "current_leader hint must name the real leader node{rf3_leader}; got {leader_hint}"
    );
    // The load-bearing anti-silent-append assertion: the follower's local log
    // MUST NOT have grown. Pre-fix it advanced by one (silent follower append).
    let follower_leo_after = cluster[follower_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .await
        .expect("follower hosts gate-rf3");
    assert!(
        follower_leo_after == follower_leo_before,
        "rejected Produce must NOT append to the follower's local log: \
         before={follower_leo_before} after={follower_leo_after}"
    );

    // ───────────────────────────────────────────────────────────────────
    // Case B: rf=1 — Produce to a NON-leader that holds NO replica.
    // ───────────────────────────────────────────────────────────────────
    let n1 = cluster[0].0.node_id();
    let off_node_part = (0..6)
        .find(|&p| {
            cluster[0]
                .0
                .partition_leader_for_test("gate-rf1", p)
                .is_some_and(|l| l != n1)
        })
        .expect("a gate-rf1 partition led off node 1");
    let rf1_leader = cluster[0]
        .0
        .partition_leader_for_test("gate-rf1", off_node_part)
        .unwrap();
    // Node 1 holds no replica for this rf=1 partition.
    assert!(
        cluster[0]
            .0
            .local_log_end_offset("gate-rf1", off_node_part)
            .await
            .is_none(),
        "test premise: node1 must NOT host gate-rf1/{off_node_part} (rf=1)"
    );
    let (code, leader_hint) = produce_one(
        &admin,
        "gate-rf1",
        rf1_id,
        off_node_part,
        "rf1-to-nonreplica",
    )
    .await;
    assert!(
        code == 6,
        "rf=1 Produce to non-replica node{n1} (leader=node{rf1_leader}) must be \
         NOT_LEADER_OR_FOLLOWER (6); got {code}"
    );
    assert!(
        leader_hint == i32::try_from(rf1_leader).unwrap(),
        "current_leader hint must name node{rf1_leader}; got {leader_hint}"
    );

    // ───────────────────────────────────────────────────────────────────
    // Leader path unchanged: the SAME Produce sent to the real leader of each
    // partition succeeds (code 0) and the record is durably stored.
    // ───────────────────────────────────────────────────────────────────
    let rf3_leader_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() == rf3_leader)
        .unwrap();
    let leader3_client = Client::builder()
        .bootstrap(cluster[rf3_leader_idx].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    // Gate on the leader broker's own replica/image catch-up before producing,
    // so the success Produce can't race the image apply (UNKNOWN_TOPIC_ID 100 /
    // transient NOT_LEADER 6) — and so the `leo_before` read below is non-None.
    wait_for_local_replica(&cluster[rf3_leader_idx].0, "gate-rf3", 0).await;
    let leader3_leo_before = cluster[rf3_leader_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .await
        .expect("leader hosts gate-rf3");
    let (code, _) = produce_one(&leader3_client, "gate-rf3", rf3_id, 0, "rf3-to-leader").await;
    assert!(
        code == 0,
        "rf=3 Produce to the leader must succeed; got {code}"
    );
    let leader3_leo_after = cluster[rf3_leader_idx]
        .0
        .local_log_end_offset("gate-rf3", 0)
        .await
        .expect("leader hosts gate-rf3");
    assert!(
        leader3_leo_after == leader3_leo_before + 1,
        "leader's local log must grow by one on a successful Produce: \
         before={leader3_leo_before} after={leader3_leo_after}"
    );

    let rf1_leader_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() == rf1_leader)
        .unwrap();
    let leader1_client = Client::builder()
        .bootstrap(cluster[rf1_leader_idx].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    // The observed flake: without this gate the Produce can reach the rf=1
    // leader before its image has applied the gate-rf1 topic_id, so the v13
    // topic_id resolution misses and returns UNKNOWN_TOPIC_ID (100) instead of
    // the required success. The rf=1 partition has no replica elsewhere, so
    // nothing else forces this broker's image/replica to be warm by now.
    wait_for_local_replica(&cluster[rf1_leader_idx].0, "gate-rf1", off_node_part).await;
    let (code, _) = produce_one(
        &leader1_client,
        "gate-rf1",
        rf1_id,
        off_node_part,
        "rf1-to-leader",
    )
    .await;
    assert!(
        code == 0,
        "rf=1 Produce to the leader must succeed; got {code}"
    );
    let leader1_leo = cluster[rf1_leader_idx]
        .0
        .local_log_end_offset("gate-rf1", off_node_part)
        .await
        .expect("leader hosts gate-rf1");
    assert!(
        leader1_leo >= 1,
        "rf=1 record must be durably stored on its leader; leo={leader1_leo}"
    );

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
