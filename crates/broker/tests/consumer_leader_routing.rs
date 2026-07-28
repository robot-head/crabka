//! Multi-broker integration coverage for the native `Consumer`'s per-leader
//! RPC routing.
//!
//! The native consumer used to send every data-plane RPC (`Fetch`,
//! `OffsetForLeaderEpoch`) over the bootstrap connection. On a multi-broker
//! cluster that misroutes `Fetch`: a partition whose leader is *not* the
//! bootstrap broker holds **no replica at all** (rf=1), so a bootstrap-routed
//! `Fetch` gets `UNKNOWN_TOPIC_OR_PARTITION` and delivers nothing. The
//! consumer now groups fetchable partitions by leader (via `Client::broker(id)`),
//! so records flow from every leader regardless of which broker the consumer
//! bootstrapped at.
//!
//! **Why rf=1 matters for test validity:** with rf=3, the bootstrap broker holds
//! a follower replica of every partition and Crabka serves consumer fetches from
//! any local replica up to the high-watermark (no leadership gate). A
//! bootstrap-only consumer would still succeed in that setup, making the test
//! hollow. With rf=1 each partition lives on exactly ONE broker; if the consumer
//! doesn't route to that broker it gets nothing.
//!
//! Windows-gated like the other multi-broker tests (openraft's `debug_assert!`
//! races on the hosted Windows scheduler).

use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

mod support;

use std::sync::OnceLock;

use tokio::sync::Mutex;

/// Serialize the multi-broker tests in this binary: each boots a 3-node
/// loopback cluster, and running them concurrently exhausts ephemeral ports
/// and starves openraft election timing. Same rationale as the `cluster_lock`
/// in `leader_epoch.rs` / `replication.rs`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Produce one single-record batch to a specific partition on the broker that
/// owns it, retrying the metadata-apply race (`UNKNOWN_TOPIC_OR_PARTITION` = 3,
/// `NOT_LEADER_OR_FOLLOWER` = 6).
async fn produce_one(
    client: &Client,
    topic: &str,
    topic_id: crabka_protocol::primitives::uuid::Uuid,
    partition: i32,
    value: &str,
) {
    let mut batch = RecordBatch::default();
    batch.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_string())),
        ..Default::default()
    });
    batch.last_offset_delta = 0;
    for attempt in 1..=10 {
        let resp = client
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(batch.clone().into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("produce");
        let err = resp.responses[0].partition_responses[0].error_code;
        if err == 0 {
            return;
        }
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER,
        // 100 = UNKNOWN_TOPIC_ID: all appear transiently while metadata — the
        // topic's existence, its leadership, and its topic-id mapping — fans
        // out across brokers right after CreateTopics. The producer sends by
        // topic_id, so a target leader that hasn't yet applied the topic record
        // answers UNKNOWN_TOPIC_ID until the metadata image catches up.
        if (err == 3 || err == 6 || err == 100) && attempt < 10 {
            // intentional: retry backoff between produce attempts while the
            // topic's existence / leadership / topic-id mapping fans out across
            // brokers — there is no single broker signal to await here (the
            // target leader varies per partition), so back off and re-send.
            tokio::time::sleep(Duration::from_millis(150)).await;
            continue;
        }
        panic!("produce to {topic}-{partition} failed after {attempt} attempt(s): code {err}");
    }
}

/// A native `Consumer` against a 3-broker rf=1 cluster must fetch records from
/// every partition, including partitions whose leader (and sole replica) is NOT
/// the broker it bootstrapped at.
///
/// With rf=1 there is no replica on the bootstrap broker for partitions led by
/// other nodes — a bootstrap-only consumer simply gets no records for those
/// partitions. The consumer MUST route each Fetch to the actual partition leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_fetches_from_non_bootstrap_leaders() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // Bootstrap everything (admin, consumer) at node 1.
    let bootstrap = cluster[0].1.listen_addr.to_string();
    let topic = "routing-rf1";
    // 6 partitions on a 3-broker cluster with rf=1: round-robin places 2
    // partitions per broker, so at least 4 partitions are led by non-bootstrap
    // brokers. We assert below that at least one is off-bootstrap to guard
    // against degenerate scheduling.
    let n_partitions: i32 = 6;

    let admin = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();

    // Create the topic with replication_factor=1. Each partition lives on
    // exactly ONE broker; the bootstrap broker has NO replica for partitions
    // placed on the other two nodes.
    let cr = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: n_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(cr.topics[0].error_code == 0, "create_topic: {cr:?}");
    let topic_id = cr.topics[0].topic_id;

    // Wait until node 1's controller image knows every partition AND its
    // assigned leader. The metadata image is raft-replicated and is the exact
    // image the routing path below reads (`partition_leader_for_test` on
    // `cluster[0].0`). `leader != 0` subsumes partition-presence, so a single
    // per-partition awaiter covers both the "materialized" and "leader-known"
    // phases the two old poll-loops handled separately.
    let bootstrap_node = cluster[0].0.node_id();
    for p in 0..n_partitions {
        cluster[0]
            .0
            .wait_for_image(|img| img.partition(topic, p).is_some_and(|part| part.leader != 0))
            .await;
    }

    // Discriminating guard: assert at least one partition is led by a
    // non-bootstrap broker. If the controller puts every leader on node 1 the
    // test would be vacuous (no cross-broker routing exercised). With 6
    // partitions on 3 brokers at rf=1 this is virtually impossible, but we
    // verify explicitly.
    let non_bootstrap_partitions: Vec<i32> = (0..n_partitions)
        .filter(|&p| {
            cluster[0]
                .0
                .partition_leader_for_test(topic, p)
                .is_some_and(|l| l != bootstrap_node)
        })
        .collect();
    assert!(
        !non_bootstrap_partitions.is_empty(),
        "all {n_partitions} partitions are led by the bootstrap node — \
         no cross-broker routing to exercise; test would be vacuous"
    );
    eprintln!(
        "partitions led by non-bootstrap brokers: {non_bootstrap_partitions:?} \
         (bootstrap = node {bootstrap_node})"
    );

    // Produce one record per partition, sending each Produce to that
    // partition's leader. With rf=1, only the leader holds the partition at all;
    // we therefore bootstrap a producer per broker and use the one that owns the
    // partition's leader.
    let producer_for = |node: u64| {
        let addr = cluster
            .iter()
            .find(|(h, _, _)| h.node_id() == node)
            .map(|(_, c, _)| c.listen_addr.to_string())
            .expect("leader node is in the cluster");
        async move {
            Client::builder()
                .bootstrap(addr)
                .build()
                .await
                .expect("producer client")
        }
    };
    let mut producers: std::collections::HashMap<u64, Client> = std::collections::HashMap::new();
    let mut expected: HashSet<String> = HashSet::new();
    for p in 0..n_partitions {
        let leader = cluster[0]
            .0
            .partition_leader_for_test(topic, p)
            .expect("partition has a leader");
        if let std::collections::hash_map::Entry::Vacant(e) = producers.entry(leader) {
            e.insert(producer_for(leader).await);
        }
        let v = format!("p{p}");
        produce_one(&producers[&leader], topic, topic_id, p, &v).await;
        expected.insert(v);
    }

    // Subscribe a native consumer bootstrapped at node 1. It must deliver
    // EVERY partition's record. For partitions led by nodes 2 and 3, the
    // consumer must route the Fetch to the actual leader — the bootstrap broker
    // has NO replica of those partitions (rf=1), so a bootstrap-only consumer
    // would return nothing for them.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("routing-consumer-rf1")
        .group_id("routing-grp-rf1")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while seen.len() < expected.len() && Instant::now() < deadline {
        for r in consumer.poll(crabka_units::millis(300)).await.unwrap() {
            seen.insert(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert!(
        seen == expected,
        "consumer must deliver every partition's record (incl. those on non-bootstrap leaders);\n\
         missing: {:?}\n\
         non-bootstrap partitions: {non_bootstrap_partitions:?}",
        expected.difference(&seen).collect::<Vec<_>>()
    );

    consumer.close().await.unwrap();
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
