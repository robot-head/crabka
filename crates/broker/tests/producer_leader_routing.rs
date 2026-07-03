//! Multi-broker integration coverage for the native `Producer`'s per-leader
//! Produce routing.
//!
//! The native producer used to send every `ProduceRequest` over the bootstrap
//! connection. On a multi-broker cluster that misroutes writes: a partition
//! whose leader is *not* the bootstrap broker holds **no replica at all**
//! (rf=1), so a bootstrap-routed Produce gets `UNKNOWN_TOPIC_OR_PARTITION` (3)
//! and the record is never stored. The producer now caches each partition's
//! leader from `Metadata`, groups its drained batches by leader, and sends one
//! `ProduceRequest` per leader via `Client::broker(id)`, so writes land on the
//! correct broker regardless of which broker the producer bootstrapped at.
//!
//! **Why rf=1 matters for test validity:** with rf=1 each partition lives on
//! exactly ONE broker. A producer that only talks to the bootstrap broker
//! cannot store records for partitions led by the other two nodes — the
//! bootstrap broker has no replica to accept them. With rf=3 the bootstrap
//! broker would hold a replica of every partition and the misroute could be
//! masked, making the test hollow. rf=1 is what makes this discriminating.
//!
//! Windows-gated like the other multi-broker tests (openraft's `debug_assert!`
//! races on the hosted Windows scheduler).

#![allow(clippy::too_many_lines)]

use assert2::assert;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;

use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

mod support;

use std::sync::OnceLock;
use tokio::sync::Mutex;

/// Serialize the multi-broker tests in this binary: each boots a 3-node
/// loopback cluster, and running them concurrently exhausts ephemeral ports
/// and starves openraft election timing. Same rationale as the `cluster_lock`
/// in `consumer_leader_routing.rs`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// A native `Producer` against a 3-broker rf=1 cluster must store every
/// partition's record on the partition's leader, including partitions whose
/// leader (and sole replica) is NOT the broker it bootstrapped at.
///
/// With rf=1 there is no replica on the bootstrap broker for partitions led by
/// other nodes — a bootstrap-only producer would get `UNKNOWN_TOPIC_OR_PARTITION`
/// for those partitions and never store them. The producer MUST route each
/// Produce to the actual partition leader. We verify durability by consuming
/// every record back (the consumer already routes per-leader).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn producer_routes_to_non_bootstrap_leaders() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // Bootstrap everything (admin, producer, consumer) at node 1.
    let bootstrap = cluster[0].1.listen_addr.to_string();
    let topic = "producer-routing-rf1";
    // 6 partitions on a 3-broker cluster with rf=1: round-robin places 2
    // partitions per broker, so at least 4 partitions are led by non-bootstrap
    // brokers. We assert below that at least one is off-bootstrap.
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

    // Wait for all partitions to materialize on their respective single brokers.
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        let mut all_known = true;
        'outer: for p in 0..n_partitions {
            for (h, _, _) in &cluster {
                if h.has_partition(topic, p).await {
                    continue 'outer;
                }
            }
            all_known = false;
            break;
        }
        if all_known {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "topic partitions didn't materialize across the cluster in 2 min"
        );
        tokio::task::yield_now().await;
    }

    // Wait until node 1 knows every partition's leader (controller image
    // propagation may lag slightly after has_partition returns true).
    let deadline = Instant::now() + Duration::from_secs(30);
    let bootstrap_node = cluster[0].0.node_id();
    loop {
        let all_have_leader =
            (0..n_partitions).all(|p| cluster[0].0.partition_leader_for_test(topic, p).is_some());
        if all_have_leader {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "partition leaders didn't converge within 30s"
        );
        tokio::task::yield_now().await;
    }

    // Discriminating guard: at least one partition must be led by a
    // non-bootstrap broker, otherwise the test exercises no cross-broker
    // routing and is vacuous. With 6 partitions on 3 brokers at rf=1 this is
    // virtually guaranteed, but we verify explicitly.
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

    // Build ONE idempotent producer bootstrapped at node 1. It must route each
    // partition's Produce to that partition's leader. For partitions led by
    // nodes 2 and 3 the bootstrap broker has NO replica (rf=1), so a
    // bootstrap-only producer's send would fail with UNKNOWN_TOPIC_OR_PARTITION.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("routing-producer-rf1")
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        // Bound retries so a genuine misroute fails fast instead of retrying
        // for i32::MAX cycles (the build default), which would hang the test.
        .retries(8)
        .retry_backoff(Duration::from_millis(150))
        .build()
        .await
        .expect("producer build");

    // Produce one record per partition, pinning the partition explicitly so
    // every partition (incl. those on non-bootstrap leaders) is exercised.
    let mut futs = Vec::with_capacity(usize::try_from(n_partitions).unwrap_or(0));
    let mut expected: HashSet<String> = HashSet::new();
    for p in 0..n_partitions {
        let v = format!("p{p}");
        let rx = producer
            .send(ProducerRecord {
                topic: topic.into(),
                partition: Some(p),
                value: Some(Bytes::from(v.clone())),
                ..Default::default()
            })
            .await;
        futs.push((p, rx));
        expected.insert(v);
    }
    producer.flush().await.expect("flush");

    // Every record must be acked OK — including those for partitions led by
    // non-bootstrap brokers. A misroute to a non-hosting broker (rf=1) returns
    // UNKNOWN_TOPIC_OR_PARTITION, which (after the bounded re-route budget)
    // surfaces as a Server error here.
    for (p, rx) in futs {
        let meta = rx
            .await
            .expect("oneshot")
            .unwrap_or_else(|e| panic!("record for partition {p} failed: {e:?}"));
        assert!(meta.partition == p, "ack partition mismatch: {meta:?}");
    }

    // Durability check: consume every record back through a native consumer
    // (which routes per-leader). For partitions on non-bootstrap leaders the
    // records can only be present if the producer actually stored them there.
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.clone())
        .client_id("routing-consumer-rf1")
        .group_id("routing-producer-grp-rf1")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap();

    // Track which partition each consumed record came from so we can prove the
    // non-bootstrap partitions are durably stored on their leaders.
    let mut seen: HashSet<String> = HashSet::new();
    let mut seen_partitions: HashMap<i32, String> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    while seen.len() < expected.len() && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(300)).await.unwrap() {
            let v = String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned();
            seen.insert(v.clone());
            seen_partitions.insert(r.partition, v);
        }
    }
    assert!(
        seen == expected,
        "consumer must read back every produced record (incl. those on non-bootstrap leaders);\n\
         missing: {:?}\n\
         non-bootstrap partitions: {non_bootstrap_partitions:?}",
        expected.difference(&seen).collect::<Vec<_>>()
    );

    // Explicit: every non-bootstrap partition's record was stored and read back
    // from its correct leader. This is the load-bearing assertion — it can only
    // hold if the producer routed those Produces off the bootstrap connection.
    for &p in &non_bootstrap_partitions {
        assert!(
            seen_partitions.get(&p) == Some(&format!("p{p}")),
            "partition {p} (led by a non-bootstrap broker) was not durably stored on its leader; \
             seen_partitions = {seen_partitions:?}"
        );
    }

    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
