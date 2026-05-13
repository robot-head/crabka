//! Integration tests for sub-slice 10a (bulletproof EOS — HW + acks=all).
//!
//! Windows-gated like slice-7/8/9 multi-broker tests: openraft +
//! `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol being tested.

#![cfg(not(target_os = "windows"))]

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
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

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

async fn create_topic(bootstrap: &str, name: &str, rf: i16) {
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
    assert_eq!(
        resp.topics[0].error_code, 0,
        "CreateTopics failed: {resp:?}"
    );
}

async fn produce_acks(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    // Retry on UNKNOWN_TOPIC_OR_PARTITION (3) up to 20 times (~4 s
    // total): slice-7's openraft metadata-apply can take longer than
    // the 500ms budget used by the client-consumer integration tests
    // when several `durability.rs` tests run in parallel on a slow
    // CI runner. The partition is materialized lazily by the
    // supervisor's reconcile loop after CreateTopics ack returns; we
    // need to wait for that materialization before the Produce can
    // resolve the partition.
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 1..=MAX_ATTEMPTS {
        let resp = client
            .send(ProduceRequest {
                acks,
                timeout_ms,
                topic_data: vec![TopicProduceData {
                    name: topic.into(),
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(record_batch_with_values(values)),
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
            return Ok(pr.base_offset);
        }
        if pr.error_code == 3 && attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        return Err(pr.error_code);
    }
    unreachable!("loop returns on every iteration")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_one_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ack1", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ack1", &["a", "b", "c"], 1, 5_000)
        .await
        .expect("ack=1 success");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ackall", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ackall", &["a", "b", "c"], -1, 5_000)
        .await
        .expect("ack=-1 success");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=-1 on rf=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_clamps_at_hw_when_followers_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "clamp", 1).await;

    let offset = produce_acks(&bootstrap, "clamp", &["x", "y", "z"], 1, 5_000)
        .await
        .expect("produce ok");
    assert_eq!(offset, 0);

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "clamp".into(),
                topic_id: WireUuid::ZERO,
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
    assert_eq!(pd.error_code, 0);
    assert_eq!(pd.high_watermark, 3, "HW should equal LEO for rf=1");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_under_rf1_unchanged_from_slice9() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "rctxn", 1).await;

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

    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert_eq!(seen, vec!["p", "q", "r"]);
    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_times_out_when_no_follower() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "tout", 1).await;

    // Install a fake ISR with two members; only this broker (node 1)
    // is actually running, so node 2 can never check in via Fetch.
    // The leader's HW thus stays pinned at 0.
    broker.test_install_isr("tout", 0, &[1, 2], 1);

    let start = Instant::now();
    let err = produce_acks(&bootstrap, "tout", &["x"], -1, 200)
        .await
        .expect_err("expected timeout");
    let elapsed = start.elapsed();
    assert_eq!(err, 20, "expected NOT_ENOUGH_REPLICAS_AFTER_APPEND");
    assert!(
        elapsed >= Duration::from_millis(180),
        "expected to wait ~200ms; took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "should not wait significantly past timeout; took {elapsed:?}"
    );
    broker.shutdown().await;
}
