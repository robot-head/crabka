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
use crabka_client_consumer::{ShareAckMode, ShareAckType, ShareConsumer, ShareConsumerRecord};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::owned::share_group_describe_request::ShareGroupDescribeRequest;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

async fn create_topic(client: &Client, name: &str) {
    create_topic_with_partitions(client, name, 1).await;
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

/// Create `topic` with `num_partitions` and wait until this broker leads every
/// partition (single-broker test config leads them all).
async fn create_multi_partition_led(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    num_partitions: i32,
) {
    create_topic_with_partitions(client, topic, num_partitions).await;
    for _ in 0..200 {
        let mut have = 0;
        for p in 0..num_partitions {
            if broker.has_partition(topic, p).await {
                have += 1;
            }
        }
        if have == num_partitions {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{topic} partitions never all materialized");
}

/// Produce one record with value `value` into `(topic, partition)`, retrying
/// while the partition is still materializing.
async fn produce_one_to(
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    partition: i32,
    value: &str,
) {
    for _ in 0..40 {
        let resp = client
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: 0,
                                records: vec![Record {
                                    offset_delta: 0,
                                    value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                                    ..Default::default()
                                }],
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
        if p.error_code == 3 || p.error_code == 6 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition {topic}:{partition} never became produceable");
}

/// Send a `ShareGroupDescribe` for `group` and return its row (or `None` if the
/// group is absent).
async fn describe_group(
    client: &Client,
    group: &str,
) -> Option<crabka_protocol::owned::share_group_describe_response::DescribedGroup> {
    let resp = client
        .send(ShareGroupDescribeRequest {
            group_ids: vec![group.to_string()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("ShareGroupDescribe");
    resp.groups.into_iter().find(|g| g.group_id == group)
}

/// The UTF-8 value of a record (empty if absent).
fn val(r: &ShareConsumerRecord) -> String {
    String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned()
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

// ───────────────────────── Task E3 explicit ack / sharing / close ─────────

/// Poll until at least `n` records have accumulated, or the deadline passes.
async fn poll_until(
    consumer: &mut ShareConsumer,
    n: usize,
    budget: Duration,
) -> Vec<ShareConsumerRecord> {
    let mut acc: Vec<ShareConsumerRecord> = Vec::new();
    let deadline = std::time::Instant::now() + budget;
    while std::time::Instant::now() < deadline && acc.len() < n {
        let recs = consumer.poll(Duration::from_millis(300)).await.unwrap();
        acc.extend(recs);
    }
    acc
}

/// Explicit mode: produce N, poll → N records (`delivery_count` 1), `Release` each,
/// poll again → the same records redelivered with `delivery_count == 2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_release_redelivers() {
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

    create_topic_led(&broker, &admin, "rel").await;
    let tid = topic_id(&broker, "rel");
    bootstrap_share_state(&broker, &admin, &format!("relg:{tid}:0")).await;
    produce_n(&admin, "rel", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rel-1")
        .group_id("relg")
        .subscribe(vec!["rel".to_string()])
        .ack_mode(ShareAckMode::Explicit)
        .heartbeat_interval(Duration::from_millis(300))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "relg", tid, 0).await;

    let first = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert!(first.len() == 3, "must acquire all 3, got {first:?}");
    assert!(
        first.iter().all(|r| r.delivery_count == 1),
        "first delivery_count must be 1, got {first:?}"
    );

    // Release every record back to the queue.
    for r in &first {
        consumer
            .acknowledge(r, ShareAckType::Release)
            .expect("explicit release in explicit mode");
    }

    // The Release flushes (piggybacked) on the next poll; the released offsets
    // are then re-acquired with an incremented delivery_count.
    let second = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert!(
        second.len() == 3,
        "released records must be redelivered, got {second:?}"
    );
    assert!(
        second.iter().all(|r| r.delivery_count == 2),
        "redelivery must bump delivery_count to 2, got {second:?}"
    );
    let mut a: Vec<String> = first.iter().map(val).collect();
    let mut b: Vec<String> = second.iter().map(val).collect();
    a.sort();
    b.sort();
    assert!(
        a == b,
        "redelivery must return the same values: {a:?} vs {b:?}"
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// Explicit mode: `Reject` every acquired record + `commit()`; the rejected
/// offsets are archived and the SPSO advances past them, so a later poll of a
/// freshly produced record returns only the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_reject_not_redelivered() {
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

    create_topic_led(&broker, &admin, "rej").await;
    let tid = topic_id(&broker, "rej");
    bootstrap_share_state(&broker, &admin, &format!("rejg:{tid}:0")).await;
    produce_n(&admin, "rej", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rej-1")
        .group_id("rejg")
        .subscribe(vec!["rej".to_string()])
        .ack_mode(ShareAckMode::Explicit)
        .heartbeat_interval(Duration::from_millis(300))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "rejg", tid, 0).await;

    let first = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert!(first.len() == 3, "must acquire all 3, got {first:?}");

    // Reject all three, then commit (standalone ShareAcknowledge).
    for r in &first {
        consumer
            .acknowledge(r, ShareAckType::Reject)
            .expect("explicit reject in explicit mode");
    }
    consumer.commit().await.expect("commit rejects");

    // Produce one more record; only it should be delivered — the rejected
    // offsets were archived and the SPSO advanced past them.
    produce_one_to(&admin, "rej", tid, 0, "v3").await;

    let next = poll_until(&mut consumer, 1, Duration::from_secs(15)).await;
    assert!(
        next.len() == 1,
        "rejected records must not redeliver; expected only the new record, got {next:?}"
    );
    assert!(
        val(&next[0]) == "v3",
        "the only delivered record must be the new one, got {:?}",
        val(&next[0])
    );

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// One group, a 2-partition topic, records on both partitions, two
/// `ShareConsumer`s in the group: the `SimpleAssignor` distributes the partitions
/// one-each, so the two members' delivered records are disjoint and together
/// cover everything produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consumers_share_topic() {
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

    create_multi_partition_led(&broker, &admin, "shared", 2).await;
    let tid = topic_id(&broker, "shared");
    bootstrap_share_state(&broker, &admin, &format!("shareg:{tid}:0")).await;
    produce_one_to(&admin, "shared", tid, 0, "p0a").await;
    produce_one_to(&admin, "shared", tid, 0, "p0b").await;
    produce_one_to(&admin, "shared", tid, 1, "p1a").await;
    produce_one_to(&admin, "shared", tid, 1, "p1b").await;

    let mut c1 = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-c1")
        .group_id("shareg")
        .subscribe(vec!["shared".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    let mut c2 = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-c2")
        .group_id("shareg")
        .subscribe(vec!["shared".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(Duration::from_millis(200))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "shareg", tid, 0).await;
    wait_for_share_init(&broker, "shareg", tid, 1).await;

    // Drive both consumers until every produced record has been delivered. The
    // second member's assignment converges over a few heartbeats, so poll both
    // in a bounded loop rather than once. Implicit auto-Accept advances each
    // member's owned partitions, so no record is delivered twice.
    let mut got1: Vec<(i32, String)> = Vec::new();
    let mut got2: Vec<(i32, String)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while std::time::Instant::now() < deadline && got1.len() + got2.len() < 4 {
        for r in c1.poll(Duration::from_millis(250)).await.unwrap() {
            got1.push((r.partition, val(&r)));
        }
        for r in c2.poll(Duration::from_millis(250)).await.unwrap() {
            got2.push((r.partition, val(&r)));
        }
    }

    // Disjoint: no value delivered to both consumers.
    let v1: std::collections::HashSet<&String> = got1.iter().map(|(_, v)| v).collect();
    let v2: std::collections::HashSet<&String> = got2.iter().map(|(_, v)| v).collect();
    assert!(
        v1.is_disjoint(&v2),
        "members must not share records: c1={got1:?} c2={got2:?}"
    );
    // Each member owns whole partitions, so a member never sees both partitions.
    let p1: std::collections::HashSet<i32> = got1.iter().map(|(p, _)| *p).collect();
    let p2: std::collections::HashSet<i32> = got2.iter().map(|(p, _)| *p).collect();
    assert!(
        p1.is_disjoint(&p2),
        "partitions must be owned by a single member: c1={p1:?} c2={p2:?}"
    );
    // Complete: together they cover all four records.
    let mut all: Vec<String> = got1
        .iter()
        .chain(got2.iter())
        .map(|(_, v)| v.clone())
        .collect();
    all.sort();
    assert!(
        all == vec!["p0a", "p0b", "p1a", "p1b"],
        "the two members together must cover everything produced, got {all:?}"
    );

    c1.close().await.unwrap();
    c2.close().await.unwrap();
    broker.shutdown().await;
}

/// `close()` leaves the group: after closing the sole member, a
/// `ShareGroupDescribe` shows the group with zero members.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_leaves_group() {
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

    create_topic_led(&broker, &admin, "leave").await;
    let tid = topic_id(&broker, "leave");
    bootstrap_share_state(&broker, &admin, &format!("leaveg:{tid}:0")).await;
    produce_n(&admin, "leave", tid, 1).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("leave-1")
        .group_id("leaveg")
        .subscribe(vec!["leave".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(Duration::from_millis(300))
        .build()
        .await
        .unwrap();
    let member_id = consumer.member_id().to_string();
    assert!(!member_id.is_empty(), "broker must assign a member id");

    wait_for_share_init(&broker, "leaveg", tid, 0).await;
    let _ = poll_until(&mut consumer, 1, Duration::from_secs(10)).await;

    consumer.close().await.expect("close");

    // The leave heartbeat (member_epoch = -1) sent on close evicts the member;
    // give the coordinator a few hundred ms to apply it, then describe.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let g = describe_group(&admin, "leaveg").await;
        let absent = match &g {
            // Group retained but emptied, or the specific member gone.
            Some(group) => group.members.iter().all(|m| m.member_id != member_id),
            // Group row gone entirely.
            None => true,
        };
        if absent {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "member {member_id} still present after close: {g:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    broker.shutdown().await;
}
