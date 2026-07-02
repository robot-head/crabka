//! Integration tests for bulletproof EOS — HW + acks=all.
//!
//! Windows-gated like the other multi-broker tests: openraft +
//! `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol being tested.

use assert2::{assert, check};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::Broker;
use crabka_broker::{BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_core::Client;
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

mod support;

/// Resolve the topic UUID via Metadata. Produce/Fetch at v ≥ 13 carry
/// only `topic_id` on the wire (KIP-516); without this the broker
/// decodes the request with empty name + ZERO `topic_id` and returns
/// `UNKNOWN_TOPIC_OR_PARTITION`. Mirrors the helper in
/// `crates/client-consumer/tests/integration.rs`.
async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
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

fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (i32::try_from(values.len()).unwrap() - 1).max(0),
        max_timestamp: i64::try_from(values.len()).unwrap(),
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str, rf: i16) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: rf,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {resp:?}"
    );
    // CreateTopics ack means the controller's quorum committed the
    // metadata record, but the supervisor's reconcile loop materializes
    // the partition locally asynchronously. Wait until it appears so
    // subsequent Produce/Fetch don't race the materialization.
    broker.wait_until_partition_present(name, 0).await;
}

async fn produce_acks(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, topic).await;
    let resp = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch_with_values(values).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

/// An idempotent batch with explicit `(producer_id, base_sequence)` so a
/// "retry" can be replayed deterministically by re-sending the same batch.
fn idempotent_batch(pid: i64, base_seq: i32, values: &[&str]) -> RecordBatch {
    let mut b = record_batch_with_values(values);
    b.producer_id = pid;
    b.producer_epoch = 0;
    b.base_sequence = base_seq;
    b
}

/// Send one explicit `RecordBatch` as a single-partition Produce and return
/// `Ok(base_offset)` or `Err(error_code)`.
async fn produce_batch(
    bootstrap: &str,
    topic: &str,
    batch: RecordBatch,
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, topic).await;
    let resp = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
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
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}

/// Bug D regression: after a failover-rejoin divergence TRUNCATES an
/// idempotent batch off the leader's log (which also reverts producer-state),
/// a retry of that same batch must RE-APPEND — not deduplicate against the
/// now-truncated offset and stall its `acks=all` high-watermark gate forever.
///
/// Without the producer-state revert on truncation, the retry resolves to
/// `Decision::Duplicate{base_offset}` and waits for `HW >= base_offset + N`,
/// which the truncated log can never reach → `NOT_ENOUGH_REPLICAS_AFTER_APPEND`
/// on every attempt (the on-cluster permanent stall).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_retry_reappends_after_truncation_instead_of_stalling() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "trunc", 1).await;

    let base = produce_batch(
        &bootstrap,
        "trunc",
        idempotent_batch(42, 0, &["a", "b", "c"]),
        -1,
        5_000,
    )
    .await
    .expect("first idempotent produce succeeds");

    // Drop the just-appended batch off the leader's log (a divergence
    // truncation also reverts the dedup state, like the replicator does).
    broker
        .test_truncate_local_log("trunc", 0, base)
        .await
        .expect("truncate local log");

    // The retry of the SAME idempotent batch must re-append and complete fast,
    // not stall. A short timeout makes a regression (dedup-against-truncated
    // stall) fail as Err(NOT_ENOUGH_REPLICAS_AFTER_APPEND) rather than hang.
    let retry = produce_batch(
        &bootstrap,
        "trunc",
        idempotent_batch(42, 0, &["a", "b", "c"]),
        -1,
        3_000,
    )
    .await;
    assert!(
        retry.is_ok(),
        "idempotent retry after truncation must re-append (not dedup-stall); got {retry:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_one_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ack1", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ack1", &["a", "b", "c"], 1, 5_000)
        .await
        .expect("ack=1 success");
    let elapsed = start.elapsed();
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ackall", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ackall", &["a", "b", "c"], -1, 5_000)
        .await
        .expect("ack=-1 success");
    let elapsed = start.elapsed();
    assert!(offset == 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=-1 on rf=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_clamps_at_hw_when_followers_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "clamp", 1).await;

    let offset = produce_acks(&bootstrap, "clamp", &["x", "y", "z"], 1, 5_000)
        .await
        .expect("produce ok");
    assert!(offset == 0);

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "clamp").await;
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "clamp".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let pd = &resp.responses[0].partitions[0];
    assert!(pd.error_code == 0);
    assert!(pd.high_watermark == 3, "HW should equal LEO for rf=1");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// Regression-pin for the `min(HW, lso)` behavior, which is equivalent to
// plain `lso` for rf=1 (HW = LEO immediately). Previously flaked with the
// `INVALID_TXN_STATE` race fixed in `Producer::flush` (it now waits for
// in-flight Produce batches before `EndTxn`).
async fn read_committed_under_rf1_unchanged() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "rctxn", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("rc-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    producer.begin_transaction().await.unwrap();
    for v in ["p", "q", "r"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "rctxn".into(),
                    value: Some(Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.commit_transaction().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("rc-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["rctxn".to_string()])
        .build()
        .await
        .unwrap();

    // Await the records committed to the local log before polling the consumer
    // so the consumer poll returns promptly rather than racing the commit.
    broker.wait_until_local_log_end_offset("rctxn", 0, 3).await;

    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(seen == vec!["p", "q", "r"]);
    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
    support::init_tracing();
    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let bootstrap_1 = cluster[0].1.listen_addr.to_string();
    create_topic(&cluster[0].0, &bootstrap_1, "shrink", 3).await;
    // Wait for all 3 replicas to join the ISR before killing broker 3
    // so the scenario genuinely exercises ISR shrink rather than racing the
    // initial ISR population.
    cluster[0].0.wait_until_isr_len("shrink", 0, 3).await;

    // Kill broker 3 — its absence forces ISR to shrink within
    // replica_lag_time_max_ms (2s on CI), unblocking the acks=-1 produce.
    let dead = cluster.pop().expect("3rd broker");
    dead.0.shutdown().await;

    let start = Instant::now();
    let offset = produce_acks(&bootstrap_1, "shrink", &["x", "y", "z"], -1, 10_000)
        .await
        .expect("acks=-1 success after shrink");
    let elapsed = start.elapsed();
    check!(offset == 0);
    check!(
        elapsed >= Duration::from_millis(1_500),
        "expected to wait for ISR shrink (~2s); took {elapsed:?}"
    );
    check!(
        elapsed < Duration::from_secs(5),
        "shrink + completion should be well under 5s; took {elapsed:?}"
    );
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
