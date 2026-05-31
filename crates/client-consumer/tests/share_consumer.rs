//! Tests for the KIP-932 [`ShareConsumer`]: a membership smoke test (join +
//! close, Task E1) and a happy-path poll test (acquire records with the broker's
//! `delivery_count`, then implicit auto-`Accept` advances the SPSO, Task E2).
//!
//! The poll test mirrors the broker-side acquire/ack harness in
//! `crates/broker/tests/share_consume.rs` for the share-state bootstrap +
//! initialization waits (the share coordinator must be write-ready before the
//! first acquire/accept persists) and the produce helper from
//! `tests/integration.rs`. The fuller suite (explicit release/reject,
//! two-consumer sharing, close-leaves-group) is Task E3.

#![cfg(not(target_os = "windows"))]

use assert2::assert;
use std::time::Duration;

use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{ShareAckMode, ShareConsumer, ShareConsumerRecord};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

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
    assert!(cr.topics[0].error_code == 0, "create_topic failed: {cr:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn share_consumer_joins_and_closes() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("share-admin")
        .build()
        .await
        .unwrap();
    create_topic(&admin, "share-topic").await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-consumer")
        .group_id("share-group-1")
        .subscribe(["share-topic".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .session_timeout(Duration::from_secs(30))
        .heartbeat_interval(Duration::from_secs(1))
        .build()
        .await
        .expect("ShareConsumer build");

    assert!(
        !consumer.member_id().is_empty(),
        "broker must assign a member id on join"
    );
    assert!(consumer.group_id() == "share-group-1");

    consumer.close().await.expect("close");
}

// ───────────────────────── Task E2 poll helpers ─────────────────────────

/// Create `topic` (1 partition) and wait until this broker leads partition 0.
async fn create_topic_led(broker: &crabka_broker::BrokerHandle, client: &Client, topic: &str) {
    create_topic(client, topic).await;
    for _ in 0..200 {
        if broker.has_partition(topic, 0).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("partition {topic}:0 never materialized");
}

fn topic_id(broker: &crabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

/// Bootstrap `__share_group_state` via `FindCoordinator(SHARE)` and wait until
/// every state partition this broker should lead is local, so SPSO writes land.
async fn bootstrap_share_state(broker: &crabka_broker::BrokerHandle, client: &Client, key: &str) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2, // SHARE
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE)"
    );
    for _ in 0..200 {
        let mut have = 0;
        for p in 0..SHARE_STATE_PARTITIONS {
            if broker.has_partition(SHARE_STATE_TOPIC, p).await {
                have += 1;
            }
        }
        if have == SHARE_STATE_PARTITIONS {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("__share_group_state never fully materialized");
}

/// Wait until the share coordinator has durably initialized state for
/// `(group, topic, partition)` so the first accept persists.
async fn wait_for_share_init(
    broker: &crabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    for _ in 0..100 {
        if broker
            .share_state_summary_for_test(group, tid, partition)
            .await
            .is_some()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("share state for {group}:{tid}:{partition} never initialized");
}

/// Produce `n` records (values `v0..v{n-1}`) into `(topic, 0)`, retrying while
/// the partition is still materializing.
async fn produce_n(client: &Client, topic: &str, tid: uuid::Uuid, n: i64) {
    for _ in 0..40 {
        let records: Vec<Record> = (0..n)
            .map(|i| Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(bytes::Bytes::copy_from_slice(format!("v{i}").as_bytes())),
                ..Default::default()
            })
            .collect();
        let resp = client
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: i32::try_from(n - 1).unwrap(),
                                records,
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        if p.error_code == 0 {
            return;
        }
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER.
        if p.error_code == 3 || p.error_code == 6 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable");
}

/// Happy path: a `ShareConsumer` (Implicit) polls 3 produced records with
/// `delivery_count == 1`; the next poll returns empty because the implicit
/// auto-Accept advanced the SPSO past them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_acquires_and_implicit_accept_advances() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("setup")
        .build()
        .await
        .unwrap();

    create_topic_led(&broker, &admin, "t").await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &admin, &format!("g1:{tid}:0")).await;
    produce_n(&admin, "t", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-1")
        .group_id("g1")
        .subscribe(vec!["t".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(Duration::from_millis(300))
        .build()
        .await
        .unwrap();

    // The group lifecycle initializes share state asynchronously after the
    // heartbeat join; wait until it is durable so the implicit Accept persists.
    wait_for_share_init(&broker, "g1", tid, 0).await;

    // First poll: acquire all 3 offsets, each at delivery_count 1. Retry while
    // assignment / acquisition is still settling (mirrors the broker harness's
    // fetch-until-acquired loop).
    let mut first: Vec<ShareConsumerRecord> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline && first.len() < 3 {
        let recs = consumer.poll(Duration::from_millis(300)).await.unwrap();
        first.extend(recs);
    }
    assert!(
        first.len() == 3,
        "must acquire all 3 records, got {first:?}"
    );
    assert!(
        first.iter().all(|r| r.delivery_count == 1),
        "first delivery_count must be 1, got {first:?}"
    );
    let mut values: Vec<String> = first
        .iter()
        .map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned())
        .collect();
    values.sort();
    assert!(
        values == vec!["v0", "v1", "v2"],
        "record values: {values:?}"
    );
    assert!(
        first.iter().all(|r| r.topic == "t" && r.partition == 0),
        "records must carry the topic name + partition"
    );

    // Second poll: the implicit auto-Accept (piggybacked on this ShareFetch)
    // advances the SPSO past 0..2, so nothing is re-acquired. Poll a few times
    // to give the accept time to take effect.
    let mut second = 0usize;
    for _ in 0..5 {
        second += consumer
            .poll(Duration::from_millis(300))
            .await
            .unwrap()
            .len();
    }
    assert!(
        second == 0,
        "implicit accept must advance SPSO; expected no redelivery, got {second}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
