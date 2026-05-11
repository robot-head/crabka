//! End-to-end: a Rust producer (via `crabka-client-core`) writes records;
//! a Rust [`crabka_client_consumer::Consumer`] subscribes through a group
//! and reads them back; commits survive a broker restart.
//!
//! `flavor = "multi_thread", worker_threads = 2` is mandatory here for the
//! same reason as the slice-4 JVM acceptance tests: a single-threaded
//! runtime can't drive both the broker's accept loop and the test body
//! when the test makes synchronous-style blocking calls into the broker.

use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, ConsumerBuilder};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

/// Build a `RecordBatch` with one entry per value. Mirrors the helper in
/// `crates/broker/tests/integration.rs`.
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

/// Resolve the topic UUID via Metadata. Produce / Fetch at v ≥ 13 carry
/// only `topic_id` on the wire.
async fn topic_id_for(
    client: &Client,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
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

async fn produce(client: &Client, topic: &str, values: &[&str]) {
    let topic_id = topic_id_for(client, topic).await;
    let resp = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                topic_id,
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
        .expect("produce");
    assert_eq!(
        resp.responses[0].partition_responses[0].error_code, 0,
        "produce failed: {resp:?}"
    );
}

async fn create_topic(client: &Client, name: &str) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_producer_to_rust_consumer_through_group() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder(&bootstrap)
        .client_id("rust-producer")
        .build()
        .await
        .unwrap();
    create_topic(&producer, "rrtopic").await;
    produce(&producer, "rrtopic", &["a", "b", "c"]).await;

    let mut consumer = ConsumerBuilder::new(&bootstrap)
        .client_id("rust-consumer")
        .group_id("g1")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(&["rrtopic"])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline && seen.len() < 3 {
        let records = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in records {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert_eq!(seen, vec!["a", "b", "c"]);

    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offsets_survive_broker_restart() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().to_path_buf();

    // First boot: create + produce + consume + commit.
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let producer = Client::builder(&bootstrap)
            .client_id("p")
            .build()
            .await
            .unwrap();
        create_topic(&producer, "persist").await;
        produce(&producer, "persist", &["x", "y", "z"]).await;

        let mut consumer = ConsumerBuilder::new(&bootstrap)
            .client_id("c")
            .group_id("persist-grp")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .subscribe(&["persist"])
            .build()
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut seen = 0;
        while std::time::Instant::now() < deadline && seen < 3 {
            seen += consumer.poll(Duration::from_millis(500)).await.unwrap().len();
        }
        assert_eq!(seen, 3);
        consumer.commit_sync().await.unwrap();
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }

    // Second boot: same group reads from the committed offset (= end).
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut consumer = ConsumerBuilder::new(&bootstrap)
            .client_id("c2")
            .group_id("persist-grp")
            .rebalance_timeout(Duration::from_secs(2))
            .heartbeat_interval(Duration::from_secs(1))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(&["persist"])
            .build()
            .await
            .unwrap();
        // Quick poll: should NOT receive the same x/y/z again.
        let r = consumer.poll(Duration::from_millis(500)).await.unwrap();
        assert!(r.is_empty(), "expected empty poll after restart, got {r:?}");
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}
