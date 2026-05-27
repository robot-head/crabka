//! KIP-429 cooperative-sticky rebalance integration tests.
//!
//! Boots a Crabka broker + multiple Rust consumers using
//! `Assignor::CooperativeSticky` and exercises phase-1/phase-2 rebalances.

#![cfg(not(target_os = "windows"))]

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_three_member_partial_revocation() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic_with_partitions(&producer, "coop6", 6).await;

    // m1 joins alone.
    let m1 = build_cooperative_consumer(&bootstrap, "coop-grp-1", "m1", "coop6").await;
    // Let m1's initial sync land and rebalance settle.
    wait_for_assignment_count(&m1, 6, Duration::from_secs(15)).await;
    assert_eq!(m1.assignment().await.len(), 6, "m1 alone owns all 6");

    // m2 joins — triggers a rebalance. Phase-1 keeps m1's sticky retained
    // partitions and lands m2 with 0; phase-2 places the freed half onto m2.
    // Wait long enough for the full phase-1 + phase-2 cooperative round
    // (~6s worst case for two 3s initial-rebalance-delay windows) to
    // settle before adding m3, otherwise cascading rebalances can race
    // partition placements.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let m2 = build_cooperative_consumer(&bootstrap, "coop-grp-1", "m2", "coop6").await;
    wait_for_total_assignment(&[&m1, &m2], 6, Duration::from_secs(20)).await;

    // m3 joins — triggers another rebalance.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let m3 = build_cooperative_consumer(&bootstrap, "coop-grp-1", "m3", "coop6").await;

    // Wait for the final settled assignment: each member owns exactly 2.
    let deadline = Instant::now() + Duration::from_secs(30);
    let (a1, a2, a3) = loop {
        let a1 = m1.assignment().await;
        let a2 = m2.assignment().await;
        let a3 = m3.assignment().await;
        if a1.len() == 2 && a2.len() == 2 && a3.len() == 2 {
            break (a1, a2, a3);
        }
        if Instant::now() >= deadline {
            panic!("did not reach balanced 2/2/2 within deadline: m1={a1:?} m2={a2:?} m3={a3:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // Union covers all 6 partitions of `coop6`, with no overlaps.
    let mut union: HashSet<(String, i32)> = HashSet::new();
    let mut overlap_count = 0;
    for tp in a1.iter().chain(a2.iter()).chain(a3.iter()) {
        if !union.insert(tp.clone()) {
            overlap_count += 1;
        }
    }
    assert_eq!(overlap_count, 0, "no overlapping assignments allowed");
    assert_eq!(union.len(), 6, "union covers all 6 partitions");
    for (t, _) in &union {
        assert_eq!(t, "coop6", "all owned partitions are from coop6");
    }
    let mut partitions: Vec<i32> = union.into_iter().map(|(_, p)| p).collect();
    partitions.sort_unstable();
    assert_eq!(partitions, vec![0, 1, 2, 3, 4, 5]);

    m1.close().await.unwrap();
    m2.close().await.unwrap();
    m3.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_transparent_to_poll() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic_with_partitions(&producer, "cooppoll", 4).await;

    // First wave: 4 messages, one per partition.
    for p in 0..4i32 {
        produce_to_partition(&producer, "cooppoll", p, &[&format!("a{p}")]).await;
    }

    // m1 starts alone; receives all 4 messages.
    let mut m1 = build_cooperative_consumer(&bootstrap, "poll-grp", "m1", "cooppoll").await;
    wait_for_assignment_count(&m1, 4, Duration::from_secs(15)).await;

    let mut received: HashMap<String, HashSet<(i32, i64)>> = HashMap::new();
    received.insert("m1".into(), HashSet::new());
    received.insert("m2".into(), HashSet::new());
    let mut values_seen: HashSet<String> = HashSet::new();

    let deadline_first_wave = Instant::now() + Duration::from_secs(15);
    while values_seen.len() < 4 && Instant::now() < deadline_first_wave {
        let recs = m1
            .poll(Duration::from_millis(200))
            .await
            .expect("poll first wave");
        for r in recs {
            values_seen.insert(value_string(r.value.as_ref()));
            received
                .get_mut("m1")
                .unwrap()
                .insert((r.partition, r.offset));
        }
    }
    assert_eq!(values_seen.len(), 4, "m1 received all 4 first-wave msgs");

    // Second wave: produce 4 more messages.
    for p in 0..4i32 {
        produce_to_partition(&producer, "cooppoll", p, &[&format!("b{p}")]).await;
    }

    // Start m2 mid-stream — triggers a rebalance. Concurrently keep polling m1.
    let bootstrap2 = bootstrap.clone();
    let m2_handle = tokio::spawn(async move {
        build_cooperative_consumer(&bootstrap2, "poll-grp", "m2", "cooppoll").await
    });

    // Continue polling m1 for up to 15s; m1.poll() must never raise a
    // `CommitInvalid` / `RebalanceFailed` (the KIP-429 "transparent
    // rebalance" guarantee). Transient transport-layer errors are
    // tolerated — a real cooperative client would just retry.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut m1_second_wave: HashSet<String> = HashSet::new();
    while Instant::now() < deadline {
        match m1.poll(Duration::from_millis(500)).await {
            Ok(recs) => {
                for r in recs {
                    let v = value_string(r.value.as_ref());
                    if v.starts_with('b') {
                        m1_second_wave.insert(v);
                    }
                    received
                        .get_mut("m1")
                        .unwrap()
                        .insert((r.partition, r.offset));
                }
            }
            Err(
                crabka_client_consumer::ConsumerError::CommitInvalid
                | crabka_client_consumer::ConsumerError::RebalanceFailed(_),
            ) => {
                panic!("m1.poll surfaced a rebalance-specific error — KIP-429 violation");
            }
            Err(e) => {
                // Transient transport error (e.g. fetch timeout while the
                // broker is mid-rebalance). Backoff briefly and retry.
                tracing::warn!(error = %e, "m1.poll transient error");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }

    let mut m2 = m2_handle.await.expect("m2 builder task");

    // Drain m2 for any straggler messages from its newly-assigned partitions.
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    let mut m2_second_wave: HashSet<String> = HashSet::new();
    while Instant::now() < drain_deadline {
        let recs = m2.poll(Duration::from_millis(200)).await.expect("m2 poll");
        if recs.is_empty() && !m2_second_wave.is_empty() {
            // Got something earlier; no more left — done.
            break;
        }
        for r in recs {
            let v = value_string(r.value.as_ref());
            if v.starts_with('b') {
                m2_second_wave.insert(v);
            }
            received
                .get_mut("m2")
                .unwrap()
                .insert((r.partition, r.offset));
        }
        if m1_second_wave.len() + m2_second_wave.len() >= 4 {
            break;
        }
    }

    // Validate union of second-wave deliveries covers all 4 b-messages, no duplicates.
    let mut all_second_wave: HashSet<String> = HashSet::new();
    for v in m1_second_wave.iter().chain(m2_second_wave.iter()) {
        all_second_wave.insert(v.clone());
    }
    assert_eq!(
        all_second_wave.len(),
        4,
        "all 4 second-wave messages delivered (m1={m1_second_wave:?} m2={m2_second_wave:?})"
    );
    // Cross-check: each second-wave value appears in at most one consumer.
    let m1_inter_m2: HashSet<_> = m1_second_wave.intersection(&m2_second_wave).collect();
    assert!(
        m1_inter_m2.is_empty(),
        "no duplicate deliveries across consumers: {m1_inter_m2:?}"
    );

    m1.close().await.unwrap();
    m2.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cooperative_single_member_steady_state() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("p")
        .build()
        .await
        .unwrap();
    create_topic_with_partitions(&producer, "cooponly", 3).await;

    let mut consumer = build_cooperative_consumer(&bootstrap, "only-grp", "m1", "cooponly").await;
    wait_for_assignment_count(&consumer, 3, Duration::from_secs(15)).await;
    let asn = consumer.assignment().await;
    assert_eq!(asn.len(), 3, "single member owns all 3 partitions: {asn:?}");
    let mut parts: Vec<i32> = asn.iter().map(|(_, p)| *p).collect();
    parts.sort_unstable();
    assert_eq!(parts, vec![0, 1, 2]);

    for p in 0..3i32 {
        produce_to_partition(&producer, "cooponly", p, &[&format!("v{p}")]).await;
    }

    let mut seen: HashSet<String> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        let recs = consumer.poll(Duration::from_millis(250)).await.unwrap();
        for r in recs {
            seen.insert(value_string(r.value.as_ref()));
        }
    }
    assert_eq!(seen.len(), 3, "received all 3 messages: {seen:?}");

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── helpers (inlined from `integration.rs` patterns) ──────────────────────

fn value_string(v: Option<&Bytes>) -> String {
    String::from_utf8_lossy(v.map(Bytes::as_ref).unwrap_or(&[])).into_owned()
}

fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).expect("test fixture small enough for i32"),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

async fn topic_id_for(client: &Client, name: &str) -> crabka_protocol::primitives::uuid::Uuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn create_topic_with_partitions(client: &Client, name: &str, num_partitions: i32) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(cr.topics[0].error_code, 0, "create_topic failed: {cr:?}");
}

/// Produce records to a specific partition index. Mirrors `integration.rs::produce`
/// but lets the caller choose `partition`.
async fn produce_to_partition(client: &Client, topic: &str, partition: i32, values: &[&str]) {
    let topic_id = topic_id_for(client, topic).await;
    for attempt in 1..=5 {
        let resp = client
            .send(ProduceRequest {
                acks: 1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(record_batch_with_values(values)),
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
        if err == 3 && attempt < 5 {
            // UNKNOWN_TOPIC_OR_PARTITION — metadata-apply race; retry.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed after {attempt} attempt(s): {resp:?}");
    }
}

async fn build_cooperative_consumer(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    topic: &str,
) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group_id)
        .assignor(Assignor::CooperativeSticky)
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("build cooperative consumer")
}

async fn wait_for_assignment_count(consumer: &Consumer, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let n = consumer.assignment().await.len();
        if n == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "waited {:?} for assignment count {expected}, last={n}",
                timeout
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait until the union of all consumers' assignments has `expected` unique
/// `(topic, partition)` entries. Used to confirm a cooperative rebalance has
/// settled before introducing the next membership change.
async fn wait_for_total_assignment(consumers: &[&Consumer], expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut union: HashSet<(String, i32)> = HashSet::new();
        for c in consumers {
            for tp in c.assignment().await {
                union.insert(tp);
            }
        }
        if union.len() == expected {
            return;
        }
        if Instant::now() >= deadline {
            panic!(
                "waited {:?} for union-assignment {expected}, last={}",
                timeout,
                union.len()
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
