//! Multi-broker integration coverage for the native `Consumer`'s per-leader
//! RPC routing.
//!
//! The native consumer used to send every data-plane RPC (`Fetch`,
//! `OffsetForLeaderEpoch`) over the bootstrap connection. On a multi-broker
//! cluster that misroutes `Fetch`: a partition whose leader is *not* the
//! bootstrap broker answers a bootstrap-routed `Fetch` with
//! `NOT_LEADER_OR_FOLLOWER` and serves no records. The consumer now groups
//! fetchable partitions by leader (via `Client::broker(id)`), so records flow
//! from every leader regardless of which broker the consumer bootstrapped at.
//!
//! Windows-gated like the other multi-broker tests (openraft's `debug_assert!`
//! races on the hosted Windows scheduler).

#![cfg(not(target_os = "windows"))]
#![allow(clippy::too_many_lines)]

use assert2::assert;
use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;

use crabka_broker::BrokerHandle;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

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

/// Produce one single-record batch to a specific partition, retrying the
/// metadata-apply race (`UNKNOWN_TOPIC_OR_PARTITION` = 3).
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
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER: both can
        // appear transiently while metadata propagates / leadership settles.
        if (err == 3 || err == 6) && attempt < 10 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            continue;
        }
        panic!("produce to {topic}-{partition} failed after {attempt} attempt(s): code {err}");
    }
}

/// A native `Consumer` against a 3-broker cluster must fetch records from every
/// partition, including partitions whose leader is NOT the broker it
/// bootstrapped at, and keep consuming after a leadership change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_fetches_from_non_bootstrap_leaders_and_survives_failover() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // Bootstrap everything (admin, producer, consumer) at node 1. With 6
    // partitions at rf=3, round-robin spreads the partition leaders across all
    // three nodes, so several partitions are led by nodes 2 and 3 — i.e. NOT
    // the bootstrap broker. If Fetch were still bootstrap-routed those
    // partitions would answer NOT_LEADER_OR_FOLLOWER and deliver nothing.
    let bootstrap = cluster[0].1.listen_addr.to_string();
    let topic = "routing";
    let n_partitions: i32 = 6;

    let admin = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let cr = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: n_partitions,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(cr.topics[0].error_code == 0, "create_topic: {cr:?}");
    let topic_id = cr.topics[0].topic_id;

    // Wait for the topic to materialize on every broker.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut all = true;
        for (h, _, _) in &cluster {
            for p in 0..n_partitions {
                if !h.has_partition(topic, p).await {
                    all = false;
                    break;
                }
            }
        }
        if all {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "topic didn't propagate in 2 min"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Sanity: confirm the leaders really are spread — at least one partition is
    // led by a node other than the bootstrap (node 1). If they all happened to
    // land on node 1, the test wouldn't exercise cross-broker routing.
    let bootstrap_node = cluster[0].0.node_id();
    let non_bootstrap_partition: i32 = {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let mut found: Option<i32> = None;
            let mut all_have_leader = true;
            for p in 0..n_partitions {
                match cluster[0].0.partition_leader_for_test(topic, p) {
                    Some(l) if l != bootstrap_node => found = Some(p),
                    Some(_) => {}
                    None => all_have_leader = false,
                }
            }
            if all_have_leader && let Some(p) = found {
                break p;
            }
            assert!(
                Instant::now() <= deadline,
                "leaders didn't spread off the bootstrap broker within 30s"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    eprintln!(
        "partition {non_bootstrap_partition} is led by node \
         {:?} (bootstrap = node {bootstrap_node})",
        cluster[0]
            .0
            .partition_leader_for_test(topic, non_bootstrap_partition)
    );

    // Produce one record to every partition, sending each Produce to that
    // partition's *leader*. The native producer is bootstrap-only (a separate,
    // out-of-scope gap), and the broker appends a Produce to whatever local
    // replica it holds — including a follower — so a bootstrap-routed Produce
    // for a non-bootstrap-led partition would write to the wrong replica and
    // the leader (where the consumer correctly fetches) would never see it. We
    // therefore bootstrap one producer per broker and pick the leader's.
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
        let v = format!("p{p}-a");
        produce_one(&producers[&leader], topic, topic_id, p, &v).await;
        expected.insert(v);
    }

    // Subscribe a native consumer (bootstrapped at node 1) and drain. To
    // collect every partition's record the consumer MUST route each Fetch to
    // that partition's leader — which for several partitions is not node 1.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("routing-consumer")
        .group_id("routing-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while seen.len() < expected.len() && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(300)).await.unwrap() {
            seen.insert(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert!(
        seen == expected,
        "consumer must deliver every partition's record (incl. non-bootstrap \
         leaders); missing {:?}",
        expected.difference(&seen).collect::<Vec<_>>()
    );

    // ── Leadership change: kill the leader of a non-bootstrap partition. ──────
    // Quorum of 3 tolerates one node loss, so raft + the bootstrap broker stay
    // alive. The partition's leadership moves to a surviving replica; the
    // consumer must learn the new leader (NOT_LEADER_OR_FOLLOWER re-target +
    // metadata refresh) and keep consuming.
    let dead_leader = cluster[0]
        .0
        .partition_leader_for_test(topic, non_bootstrap_partition)
        .expect("non-bootstrap partition has a leader");
    assert!(
        dead_leader != bootstrap_node,
        "we intend to kill a non-bootstrap leader"
    );
    let dead_idx = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() == dead_leader)
        .expect("dead leader is in the cluster");

    // Pull the doomed broker out of the cluster vec and shut it down.
    let mut survivors = Vec::new();
    let mut doomed: Option<BrokerHandle> = None;
    for (i, entry) in cluster.into_iter().enumerate() {
        if i == dead_idx {
            doomed = Some(entry.0);
        } else {
            survivors.push(entry);
        }
    }
    doomed.unwrap().shutdown().await;
    eprintln!("shut down node {dead_leader}; awaiting failover");

    // Wait for a surviving broker to report a new leader for the partition.
    let deadline = Instant::now() + Duration::from_secs(30);
    let new_leader = loop {
        let l = survivors[0]
            .0
            .partition_leader_for_test(topic, non_bootstrap_partition);
        if let Some(l) = l
            && l != dead_leader
        {
            eprintln!("partition {non_bootstrap_partition} new leader: node {l}");
            break l;
        }
        assert!(
            Instant::now() <= deadline,
            "no failover leader for partition {non_bootstrap_partition} within 30s (current={l:?})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    // Produce a fresh record to the failed-over partition, sending it to the
    // *new* leader (a surviving broker), and assert the consumer delivers it —
    // proving it re-targeted the new leader and resumed.
    let new_leader_addr = survivors
        .iter()
        .find(|(h, _, _)| h.node_id() == new_leader)
        .map(|(_, c, _)| c.listen_addr.to_string())
        .expect("new leader is a surviving broker");
    let post_producer = Client::builder()
        .bootstrap(new_leader_addr)
        .build()
        .await
        .unwrap();
    let post = format!("p{non_bootstrap_partition}-b");
    produce_one(
        &post_producer,
        topic,
        topic_id,
        non_bootstrap_partition,
        &post,
    )
    .await;

    let mut got_post = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !got_post && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(300)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            if v == post {
                got_post = true;
            }
        }
    }
    assert!(
        got_post,
        "consumer must keep consuming the failed-over partition after the \
         leadership change (expected {post:?})"
    );

    consumer.close().await.unwrap();
    for (h, _, _) in survivors {
        h.shutdown().await;
    }
}
