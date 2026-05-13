//! In-process integration tests for slice-10b KIP-101 leader-epoch
//! fencing + .leader-epoch-checkpoint byte format.
//!
//! Windows-gated like other slice-7/8/9/10 multi-broker tests.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::cast_possible_truncation, clippy::default_trait_access)]

use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

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
        .expect("metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let _ = client
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
    let deadline = Instant::now() + Duration::from_secs(10);
    while !broker.has_partition(name, 0).await {
        if Instant::now() > deadline {
            panic!("materialize timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn record(value: &str) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_string())),
        ..Default::default()
    });
    b.last_offset_delta = 0;
    b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fenced_leader_epoch_truncates_zombie_writes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "fence").await;

    // Produce a record at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "fence").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "fence".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v0")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Force the partition's epoch up to 5 (simulate "split brain").
    broker.test_set_leader_epoch("fence", 0, 5);

    // Fetch with current_leader_epoch=2 → FENCED_LEADER_EPOCH (code 74).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "fence".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 2,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // FENCED_LEADER_EPOCH = 74
    assert_eq!(pd.error_code, 74, "expected FENCED_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_leader_epoch_on_metadata_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "unknown").await;
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "unknown").await;

    // Fetch with current_leader_epoch=5 — broker has epoch=0; UNKNOWN_LEADER_EPOCH (code 75).
    let resp = client
        .send(FetchRequest {
            replica_id: 99,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "unknown".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    current_leader_epoch: 5,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");
    let pd = &resp.responses[0].partitions[0];
    // UNKNOWN_LEADER_EPOCH = 75
    assert_eq!(pd.error_code, 75, "expected UNKNOWN_LEADER_EPOCH");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_checkpoint_byte_compat() {
    let (broker, bootstrap, dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "ckpt").await;

    // Produce at epoch 0.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "ckpt").await;
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v0")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Bump epoch to 1 + produce another.
    broker.test_set_leader_epoch("ckpt", 0, 1);
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "ckpt".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record("v1")),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    // Read the checkpoint file from disk.
    let path = dir.path().join("ckpt-0").join("leader-epoch-checkpoint");
    let s = std::fs::read_to_string(&path).expect("checkpoint file");
    // Format: header "0\n", count "2\n", rows "0 0\n1 1\n".
    assert!(s.starts_with("0\n"), "header should be '0\\n', got: {s:?}");
    assert!(s.contains("\n2\n"), "count should be 2, got: {s:?}");
    assert!(s.contains("0 0\n"), "epoch 0 row missing: {s:?}");
    assert!(s.contains("1 1\n"), "epoch 1 row missing: {s:?}");

    broker.shutdown().await;
}
