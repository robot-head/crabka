//! KIP-429 cooperative-sticky rebalance integration tests.
//!
//! Boots a Crabka broker + multiple Rust consumers using
//! `Assignor::CooperativeSticky` and exercises phase-1/phase-2 rebalances.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{Assignor, AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

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
    wait_for_assignment_count(&m1, 6).await;
    assert2::assert!(m1.assignment().await.len() == 6);

    // Intentional real-time pacing between membership changes: cooperative-sticky
    // rebalances in two phases gated by `group.initial.rebalance.delay`, and
    // introducing the next member *during* the prior round causes cascading
    // rebalances that never converge to a clean snapshot. There is no
    // client- or broker-observable "group fully stable" signal to await here
    // (only member-count, which fires at JoinGroup before SyncGroup completes),
    // so we pace the joins. This is a timing test, not a flaky guess — see spec
    // 2026-06-14-crabka-integration-tests-deflake-design.md.
    //
    // m2 joins — phase-1 keeps m1's sticky retained partitions and lands m2 with
    // 0; phase-2 places the freed half onto m2. `wait_for_total_assignment` then
    // gates the full phase-1 + phase-2 round.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let m2 = build_cooperative_consumer(&bootstrap, "coop-grp-1", "m2", "coop6").await;
    wait_for_total_assignment(&[&m1, &m2], 6).await;

    // m3 joins — paced past the m1+m2 round (see note above) before triggering
    // the final rebalance, which the 3-member settle loop below gates.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let m3 = build_cooperative_consumer(&bootstrap, "coop-grp-1", "m3", "coop6").await;

    // Wait for the final settled assignment. The real
    // cooperative-sticky correctness invariants here are:
    //
    //  - union covers all 6 partitions
    //  - no member overlaps
    //  - every member owns ≥ 1 partition
    //
    // We deliberately don't insist on a perfectly even 2/2/2 split —
    // cooperative-sticky's phase-1/phase-2 transition can briefly
    // leave one member with 1 or 3 partitions when the three
    // `assignment()` snapshots aren't taken atomically (e.g. a
    // sync-cycle snapshot lands between revoke + re-assign on a busy
    // scheduler). Folding the no-overlap + full-cover + all-≥-1
    // invariants into the loop condition lets the test ride out
    // those windows instead of failing on a transient skew.
    let union = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let a1 = m1.assignment().await;
            let a2 = m2.assignment().await;
            let a3 = m3.assignment().await;

            let mut union: HashSet<(String, i32)> = HashSet::new();
            let mut overlap = false;
            for tp in a1.iter().chain(a2.iter()).chain(a3.iter()) {
                if !union.insert(tp.clone()) {
                    overlap = true;
                }
            }

            if union.len() == 6 && !overlap && !a1.is_empty() && !a2.is_empty() && !a3.is_empty() {
                break union;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("3-member cooperative assignment settled within 30s");
    for (t, _) in &union {
        assert2::assert!(t == "coop6");
    }
    let mut partitions: Vec<i32> = union.into_iter().map(|(_, p)| p).collect();
    partitions.sort_unstable();
    assert2::assert!(partitions == vec![0, 1, 2, 3, 4, 5]);

    m1.close().await.unwrap();
    m2.close().await.unwrap();
    m3.close().await.unwrap();
    broker.shutdown().await;
}

// Linear two-phase scenario; splitting fragments the produce/consume narrative.
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
        produce_to_partition(&broker, &producer, "cooppoll", p, &[&format!("a{p}")]).await;
    }

    // m1 starts alone; receives all 4 messages.
    let mut m1 = build_cooperative_consumer(&bootstrap, "poll-grp", "m1", "cooppoll").await;
    wait_for_assignment_count(&m1, 4).await;

    let mut received: HashMap<String, HashSet<(i32, i64)>> = HashMap::new();
    received.insert("m1".into(), HashSet::new());
    received.insert("m2".into(), HashSet::new());
    let mut values_seen: HashSet<String> = HashSet::new();

    tokio::time::timeout(Duration::from_secs(30), async {
        while values_seen.len() < 4 {
            let recs = m1
                .poll(crabka_units::millis(200))
                .await
                .expect("poll first wave");
            for r in recs {
                values_seen.insert(value_string(r.value.as_ref()));
                received
                    .get_mut("m1")
                    .unwrap()
                    .insert((r.partition, r.offset));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drained 4 records within 30s");
    assert2::assert!(values_seen.len() == 4);

    // Second wave: produce 4 more messages.
    for p in 0..4i32 {
        produce_to_partition(&broker, &producer, "cooppoll", p, &[&format!("b{p}")]).await;
    }

    // m1 (sole owner of all 4 partitions) consumes the entire second wave
    // *before* any rebalance. This is the scenario the revoke-time commit
    // protects: m1 advances past `b0..b3`, so when a partition is later
    // handed to m2 the committed position must prevent re-delivery.
    let mut m1_second_wave: HashSet<String> = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while m1_second_wave.len() < 4 {
            let recs = m1.poll(crabka_units::millis(200)).await.expect("m1 poll");
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
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drained 4 records within 30s");
    assert2::assert!(m1_second_wave.len() == 4);

    // Start m2 — triggers a cooperative rebalance that moves two partitions
    // off m1. m1 keeps polling so its coordinator can complete the rejoin;
    // poll() must never surface a rebalance-specific error (KIP-429
    // transparency guarantee).
    let bootstrap2 = bootstrap.clone();
    let m2_handle = tokio::spawn(async move {
        build_cooperative_consumer(&bootstrap2, "poll-grp", "m2", "cooppoll").await
    });
    let mut m2 = m2_handle.await.expect("m2 builder task");

    // Let the group settle: m1 sheds two partitions, m2 acquires them.
    // Once m2 owns its partitions the leader has completed phase 2, which
    // is sequenced *after* m1's revoke-time commit — so m2 primes from the
    // committed position rather than from zero.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            // Keep m1 polling during the wait; ignore transient errors but
            // still fail on rebalance-specific ones.
            match m1.poll(crabka_units::millis(200)).await {
                Ok(recs) => {
                    for r in recs {
                        received
                            .get_mut("m1")
                            .unwrap()
                            .insert((r.partition, r.offset));
                    }
                }
                Err(
                    crabka_client_consumer::ConsumerError::CommitInvalid
                    | crabka_client_consumer::ConsumerError::RebalanceFailed(_),
                ) => panic!("m1.poll surfaced a rebalance-specific error — KIP-429 violation"),
                Err(_) => {}
            }
            let m1_n = m1.assignment().await.len();
            let m2_n = m2.assignment().await.len();
            if m1_n == 2 && m2_n == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("2/2 split within 30s");

    // Drain m2 until the next poll is empty. With the revoke-time commit in place m2 primes
    // its two partitions at m1's committed offset (past `b*`), so it must
    // deliver *none* of the second-wave messages. Re-delivery here means the
    // commit was lost — a regression.
    let mut m2_second_wave: HashSet<String> = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let recs = m2.poll(crabka_units::millis(200)).await.expect("m2 poll");
            if recs.is_empty() {
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
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drained m2 until empty within 30s");

    // No message loss: m1 delivered the whole second wave.
    assert2::assert!(m1_second_wave.len() == 4);
    // No re-delivery: each second-wave value appears in at most one consumer.
    let m1_inter_m2: HashSet<_> = m1_second_wave.intersection(&m2_second_wave).collect();
    assert2::assert!(m1_inter_m2.is_empty());
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
    wait_for_assignment_count(&consumer, 3).await;
    let asn = consumer.assignment().await;
    assert2::assert!(asn.len() == 3);
    let mut parts: Vec<i32> = asn.iter().map(|(_, p)| *p).collect();
    parts.sort_unstable();
    assert2::assert!(parts == vec![0, 1, 2]);

    for p in 0..3i32 {
        produce_to_partition(&broker, &producer, "cooponly", p, &[&format!("v{p}")]).await;
    }

    let mut seen: HashSet<String> = HashSet::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        while seen.len() < 3 {
            let recs = consumer.poll(crabka_units::millis(250)).await.unwrap();
            for r in recs {
                seen.insert(value_string(r.value.as_ref()));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("drained 3 records within 30s");
    assert2::assert!(seen.len() == 3);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

// ── helpers (inlined from `integration.rs` patterns) ──────────────────────

fn value_string(v: Option<&Bytes>) -> String {
    String::from_utf8_lossy(v.map_or(&[], Bytes::as_ref)).into_owned()
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
    assert2::assert!(cr.topics[0].error_code == 0);
}

/// Produce records to a specific partition index. Mirrors `integration.rs::produce`
/// but lets the caller choose `partition`.
async fn produce_to_partition(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    partition: i32,
    values: &[&str],
) {
    // Wait until the target partition is materialized before producing; the
    // bounded retry loop below remains as a backstop for residual apply lag.
    broker.wait_until_partition_present(topic, partition).await;
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
                        records: Some(record_batch_with_values(values).into()),
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
            // real-time wait (not a progress poll): bounded retry backoff between full Produce RPC round-trips
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
    // 500ms heartbeats keep cascading-rebalance round-trips well inside the
    // broker's 3s INITIAL_REBALANCE_DELAY wait: detect-via-heartbeat (≤500ms)
    // + rejoin-on-next-tick (≤500ms) = ≤1s, leaving ~2s of headroom for
    // scheduler jitter on busy CI runners. With the 1s heartbeat that lived
    // here previously, the detect+rejoin worst case was ~2s and could blow
    // past the broker's wait under macOS-CI contention, causing the leader
    // to compute the next round with stale member metadata.
    Consumer::builder()
        .bootstrap(bootstrap)
        .client_id(client_id)
        .group_id(group_id)
        .assignor(Assignor::CooperativeSticky)
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::millis(500))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([topic.to_string()])
        .build()
        .await
        .expect("build cooperative consumer")
}

async fn wait_for_assignment_count(consumer: &Consumer, expected: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if consumer.assignment().await.len() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("assignment count did not reach {expected} within 30s"));
}

/// Wait until the union of all consumers' assignments has `expected` unique
/// `(topic, partition)` entries. Used to confirm a cooperative rebalance has
/// settled before introducing the next membership change.
async fn wait_for_total_assignment(consumers: &[&Consumer], expected: usize) {
    tokio::time::timeout(Duration::from_secs(30), async {
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
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("total assignment did not reach {expected} within 30s"));
}
