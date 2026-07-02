//! Spins up a 3-broker cluster on loopback, creates a topic with
//! replication-factor 3, produces records to the leader, and asserts
//! every follower's local log converges to the leader's
//! `log_end_offset`. Exercises the full replication path:
//! supervisor reconcile, follower Fetch loop, and
//! `Partition::replicate_batch`.

// Test-file pragmatism: casts turn 1-based `i` into broker ids.
// Hoisting these into named helpers would obscure the per-test narrative.
#![allow(
    clippy::cast_possible_truncation,
    clippy::default_trait_access,
    // The full propagation test reads top-to-bottom as one scenario
    // (bring up cluster → wait for brokers → CreateTopics → wait for
    // propagation → produce → wait for convergence). Splitting it into
    // helpers obscures the per-stage narrative without making any
    // individual piece reusable.
    clippy::too_many_lines
)]

use std::sync::OnceLock;

use assert2::assert;
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};
use tokio::sync::Mutex;

mod support;

/// Test-binary-wide serialization. Each test in this file spins up a
/// 3-broker cluster on loopback; running them concurrently exhausts
/// loopback ephemeral ports and starves the openraft election timing.
/// Same rationale as `quorum.rs::cluster_lock`.
fn cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replication_factor_three_propagates_to_all_followers() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;

    // Wait for all 3 brokers to register in each other's MetadataImage.
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(3).await;
    }

    // `start_n_node_with_retry` binds brokers in order, so cluster[0]
    // is node 1; with rf=3 / partition_index=0 the round-robin placement
    // chooses node 1 as the partition leader. We use it as the
    // CreateTopics + Produce target.
    let leader_addr = cluster[0].1.listen_addr.to_string();

    // CreateTopics("repl", num_partitions=1, replication_factor=3).
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "repl".into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);
    // ProduceRequest v13 wire format drops `topic.name` in favour of
    // `topic.topic_id` (KIP-516). The client negotiates the broker's
    // max supported version (v13), so we must echo the CreateTopics-
    // assigned topic_id on the produce path, otherwise the broker's
    // image lookup returns an empty topic name and the partition lookup
    // fails with UNKNOWN_TOPIC_OR_PARTITION.
    let topic_id = resp.topics[0].topic_id;

    // Wait for the topic to propagate to every broker's MetadataImage.
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("repl", 0).await;
    }

    // Produce 20 records to the leader.
    let producer = Client::builder()
        .bootstrap(leader_addr)
        .build()
        .await
        .unwrap();
    let batch = RecordBatch {
        base_offset: 0,
        last_offset_delta: 19,
        records: (0..20)
            .map(|i| Record {
                offset_delta: i,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let prod = producer
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "repl".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(prod.responses[0].partition_responses[0].error_code == 0);

    // Wait until every broker's local log shows log_end_offset >= 20.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("repl", 0, 20).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_range_truncates_and_recovers() {
    let _g = cluster_lock().lock().await;
    let cluster = support::start_n_node_with_retry(3).await;

    // Same broker-discovery wait as the propagation test.
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(3).await;
    }

    // CreateTopics("oor", num_partitions=1, replication_factor=3) against
    // cluster[0] (= node 1 = round-robin leader for partition 0).
    let leader_addr = cluster[0].1.listen_addr.to_string();
    let admin = Client::builder()
        .bootstrap(leader_addr.clone())
        .build()
        .await
        .unwrap();
    let resp = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "oor".into(),
                num_partitions: 1,
                replication_factor: 3,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.topics[0].error_code == 0);
    let topic_id = resp.topics[0].topic_id;

    // Wait for the topic to propagate to every broker's MetadataImage.
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("oor", 0).await;
    }

    // Produce 50 records in 50 separate single-record batches so the
    // leader's log holds them as discrete batches. A single 50-record
    // batch won't do, because `Fetch` returns the whole batch as the
    // smallest unit, so after advancing leader's `log_start` to 25 the
    // follower would still pull a batch with `base_offset=0` and reject
    // it with `OffsetMismatch`. Per-record batches let
    // `Segment::read(25, ...)` filter out the prefix cleanly.
    let producer = Client::builder()
        .bootstrap(leader_addr)
        .build()
        .await
        .unwrap();
    for i in 0..50i32 {
        let batch = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                value: Some(bytes::Bytes::from(format!("v{i}"))),
                ..Default::default()
            }],
            ..Default::default()
        };
        let prod = producer
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: "oor".into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(prod.responses[0].partition_responses[0].error_code == 0);
    }

    // Wait for every broker's local log to catch up to 50.
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("oor", 0, 50).await;
    }

    // Simulate broker 3 "falling behind past retention": truncate its
    // local log to 0 AND advance the leader's `log_start` to 25. After
    // this, broker 3's replicator will fetch at offset 0, leader will
    // return OFFSET_OUT_OF_RANGE with `log_start_offset=25`, and the
    // replicator's recovery path must `reset_to(25)` and re-fetch from
    // 25 to converge again.
    cluster[2]
        .0
        .test_truncate_local_log("oor", 0, 0)
        .await
        .expect("truncate broker 3");
    cluster[0]
        .0
        .test_advance_log_start("oor", 0, 25)
        .await
        .expect("advance leader log_start");

    // Wait for broker 3 to converge again — log_end_offset should reach
    // 50 once it has fetched records 25..50 from the leader.
    cluster[2]
        .0
        .wait_until_local_log_end_offset("oor", 0, 50)
        .await;

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
