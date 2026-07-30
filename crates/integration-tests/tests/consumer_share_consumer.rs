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

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{ShareAckMode, ShareAckType, ShareConsumer, ShareConsumerRecord};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_group_describe_request::ShareGroupDescribeRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 1;
const MAX_CONCURRENT_TEST_BROKERS: usize = 3;

async fn broker_test_permit() -> tokio::sync::OwnedSemaphorePermit {
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    Arc::clone(
        GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TEST_BROKERS))),
    )
    .acquire_owned()
    .await
    .expect("broker test concurrency gate remains open")
}

fn broker_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.share_coordinator.state_topic_num_partitions = SHARE_STATE_PARTITIONS;
    config
}

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
    assert2::assert!(cr.topics[0].error_code == 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn share_consumer_joins_and_closes() {
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
        .heartbeat_interval(crabka_units::secs(1))
        .build()
        .await
        .expect("ShareConsumer build");

    assert2::assert!(!consumer.member_id().is_empty());
    assert2::assert!(consumer.group_id() == "share-group-1");

    consumer.close().await.expect("close");
}

// ───────────────────────── Task E2 poll helpers ─────────────────────────

/// Create `topic` (1 partition) and wait until this broker leads partition 0.
async fn create_topic_led(broker: &crabka_broker::BrokerHandle, client: &Client, topic: &str) {
    create_topic(client, topic).await;
    broker.wait_until_partition_present(topic, 0).await;
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
    assert2::assert!(resp.coordinators[0].error_code == 0);
    // All SHARE_STATE_PARTITIONS must materialize a leader (subject to KIP-595
    // election jitter) so SPSO writes land. `wait_until_partition_present` is
    // internally bounded.
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Wait until the share coordinator has durably initialized state for
/// `(group, topic, partition)` so the first accept persists.
async fn wait_for_share_init(
    broker: &crabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    // Share-state init needs the `__share_group_state` partition's leader elected
    // (subject to KIP-595 election jitter) plus a ShareFetch round-trip. The
    // awaiter is internally 30s-bounded and panics on timeout.
    broker
        .wait_for_share_state_summary(group, tid, partition)
        .await;
}

/// Produce `n` records (values `v0..v{n-1}`) into `(topic, 0)`.
async fn produce_n(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    n: i64,
) {
    // Deterministically wait for leadership before producing; the bounded retry
    // below stays as a backstop against residual metadata/leadership race.
    broker.wait_until_partition_present(topic, 0).await;
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
            // real-time wait (not a progress poll): bounded retry backoff between full Produce RPC round-trips
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
    for p in 0..num_partitions {
        broker.wait_until_partition_present(topic, p).await;
    }
}

/// Produce one record with value `value` into `(topic, partition)`.
async fn produce_one_to(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    partition: i32,
    value: &str,
) {
    // Deterministically wait for leadership before producing; the bounded retry
    // below stays as a backstop against residual metadata/leadership race.
    broker.wait_until_partition_present(topic, partition).await;
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
            // real-time wait (not a progress poll): bounded retry backoff between full Produce RPC round-trips
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
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
    produce_n(&broker, &admin, "t", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-1")
        .group_id("g1")
        .subscribe(vec!["t".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(crabka_units::millis(300))
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
    assert2::assert!(first.len() == 3);
    assert2::assert!(first.iter().all(|r| r.delivery_count == 1));
    let mut values: Vec<String> = first
        .iter()
        .map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned())
        .collect();
    values.sort();
    assert2::assert!(values == vec!["v0", "v1", "v2"]);
    assert2::assert!(first.iter().all(|r| r.topic == "t" && r.partition == 0));

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
    assert2::assert!(second == 0);

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
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
    produce_n(&broker, &admin, "rel", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rel-1")
        .group_id("relg")
        .subscribe(vec!["rel".to_string()])
        .ack_mode(ShareAckMode::Explicit)
        .heartbeat_interval(crabka_units::millis(300))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "relg", tid, 0).await;

    let first = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert2::assert!(first.len() == 3);
    assert2::assert!(first.iter().all(|r| r.delivery_count == 1));

    // Release every record back to the queue.
    for r in &first {
        consumer
            .acknowledge(r, ShareAckType::Release)
            .expect("explicit release in explicit mode");
    }

    // The Release flushes (piggybacked) on the next poll; the released offsets
    // are then re-acquired with an incremented delivery_count.
    let second = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert2::assert!(second.len() == 3);
    assert2::assert!(second.iter().all(|r| r.delivery_count == 2));
    let mut a: Vec<String> = first.iter().map(val).collect();
    let mut b: Vec<String> = second.iter().map(val).collect();
    a.sort();
    b.sort();
    assert2::assert!(a == b);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// Explicit mode: `Reject` every acquired record + `commit()`; the rejected
/// offsets are archived and the SPSO advances past them, so a later poll of a
/// freshly produced record returns only the new record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_reject_not_redelivered() {
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
    produce_n(&broker, &admin, "rej", tid, 3).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rej-1")
        .group_id("rejg")
        .subscribe(vec!["rej".to_string()])
        .ack_mode(ShareAckMode::Explicit)
        .heartbeat_interval(crabka_units::millis(300))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "rejg", tid, 0).await;

    let first = poll_until(&mut consumer, 3, Duration::from_secs(15)).await;
    assert2::assert!(first.len() == 3);

    // Reject all three, then commit (standalone ShareAcknowledge).
    for r in &first {
        consumer
            .acknowledge(r, ShareAckType::Reject)
            .expect("explicit reject in explicit mode");
    }
    consumer.commit().await.expect("commit rejects");

    // Produce one more record; only it should be delivered — the rejected
    // offsets were archived and the SPSO advanced past them.
    produce_one_to(&broker, &admin, "rej", tid, 0, "v3").await;

    let next = poll_until(&mut consumer, 1, Duration::from_secs(15)).await;
    assert2::assert!(next.len() == 1);
    assert2::assert!(val(&next[0]) == "v3");

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// One group, a 2-partition topic, records on both partitions, two
/// `ShareConsumer`s in the group: the `SimpleAssignor` distributes the partitions
/// one-each, so the two members' delivered records are disjoint and together
/// cover everything produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consumers_share_topic() {
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
    produce_one_to(&broker, &admin, "shared", tid, 0, "p0a").await;
    produce_one_to(&broker, &admin, "shared", tid, 0, "p0b").await;
    produce_one_to(&broker, &admin, "shared", tid, 1, "p1a").await;
    produce_one_to(&broker, &admin, "shared", tid, 1, "p1b").await;

    let mut c1 = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-c1")
        .group_id("shareg")
        .subscribe(vec!["shared".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(crabka_units::millis(200))
        .build()
        .await
        .unwrap();
    let mut c2 = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-c2")
        .group_id("shareg")
        .subscribe(vec!["shared".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(crabka_units::millis(200))
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
    assert2::assert!(v1.is_disjoint(&v2));
    // Each member owns whole partitions, so a member never sees both partitions.
    let p1: std::collections::HashSet<i32> = got1.iter().map(|(p, _)| *p).collect();
    let p2: std::collections::HashSet<i32> = got2.iter().map(|(p, _)| *p).collect();
    assert2::assert!(p1.is_disjoint(&p2));
    // Complete: together they cover all four records.
    let mut all: Vec<String> = got1
        .iter()
        .chain(got2.iter())
        .map(|(_, v)| v.clone())
        .collect();
    all.sort();
    assert2::assert!(all == vec!["p0a", "p0b", "p1a", "p1b"]);

    c1.close().await.unwrap();
    c2.close().await.unwrap();
    broker.shutdown().await;
}

/// `close()` leaves the group: after closing the sole member, a
/// `ShareGroupDescribe` shows the group with zero members.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_leaves_group() {
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
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
    produce_n(&broker, &admin, "leave", tid, 1).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("leave-1")
        .group_id("leaveg")
        .subscribe(vec!["leave".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(crabka_units::millis(300))
        .build()
        .await
        .unwrap();
    let member_id = consumer.member_id().to_string();
    assert2::assert!(!member_id.is_empty());

    wait_for_share_init(&broker, "leaveg", tid, 0).await;
    let _ = poll_until(&mut consumer, 1, Duration::from_secs(10)).await;

    consumer.close().await.expect("close");

    // The leave heartbeat (member_epoch = -1) sent on close evicts the member;
    // poll the describe until the member is gone, bounded so it can't hang.
    tokio::time::timeout(Duration::from_secs(30), async {
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
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("member evicted from group within 30s");

    broker.shutdown().await;
}

// ───────────────────────── Slice F: client renew ─────────────────────────

/// F1 (client renew): an explicit `ShareConsumer` polls a record, `renew()`s its
/// lock before the (short) record-lock duration expires, then waits past the
/// original lock. Because the renew extended the lock the record is NOT
/// redelivered on the next poll — proving the client's renew round-trips and the
/// broker honored it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_renew_prevents_redelivery() {
    let _permit = broker_test_permit().await;
    let dir = TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    // 1s lock; the sweeper ticks at lock/2. Generous so the renew timing window
    // tolerates scheduling jitter.
    cfg.share_group.record_lock_duration = Duration::from_secs(1);
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("setup")
        .build()
        .await
        .unwrap();

    create_topic_led(&broker, &admin, "rn").await;
    let tid = topic_id(&broker, "rn");
    bootstrap_share_state(&broker, &admin, &format!("rng:{tid}:0")).await;
    produce_n(&broker, &admin, "rn", tid, 1).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rn-1")
        .group_id("rng")
        .subscribe(vec!["rn".to_string()])
        .ack_mode(ShareAckMode::Explicit)
        .heartbeat_interval(crabka_units::millis(200))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "rng", tid, 0).await;

    // Acquire the single record (explicit mode → no auto-accept). Capture the
    // instant the record is delivered: the broker-side 1s lock starts at roughly
    // this moment (the poll that returns the record is the acquiring fetch).
    let mut first: Vec<ShareConsumerRecord> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut acquired_at = std::time::Instant::now();
    while std::time::Instant::now() < deadline && first.is_empty() {
        let recs = consumer.poll(Duration::from_millis(200)).await.unwrap();
        if !recs.is_empty() {
            acquired_at = std::time::Instant::now();
            first = recs;
        }
    }
    assert2::assert!(first.len() == 1);
    assert2::assert!(first[0].delivery_count == 1);

    // Renew ~400ms after acquire, before the 1000ms lock expires → resets the
    // deadline to renew-time + 1000ms (≈ T_acq+1400ms).
    let renew_at = acquired_at + Duration::from_millis(400);
    // Intentional real-time delay: this exercises share-lock renew-before-expiry
    // (lock TTL is 1s); it tests time-based behavior and must not be replaced with
    // state polling. See spec 2026-06-14-crabka-integration-tests-deflake-design.md.
    if let Some(rem) = renew_at.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(rem).await;
    }
    consumer
        .renew(&first[0])
        .await
        .expect("renew in explicit mode must succeed");

    // Wait to ~T_acq+1150ms: PAST the original 1000ms lock (an un-renewed record
    // would already be swept + redelivered) but before the renewed ~1400ms
    // deadline. Keep the redelivery check short so it completes before 1400ms.
    let target = acquired_at + Duration::from_millis(1150);
    // Intentional real-time delay: this exercises share-lock redelivery-after-expiry
    // (waiting past the original 1s lock TTL); it tests time-based behavior and must
    // not be replaced with state polling. See spec
    // 2026-06-14-crabka-integration-tests-deflake-design.md.
    if let Some(rem) = target.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(rem).await;
    }

    // The renewed lock still holds → no redelivery. Two short polls (ending
    // ~T_acq+1300ms, still before the renewed ~1400ms deadline).
    let mut redelivered = 0usize;
    for _ in 0..2 {
        redelivered += consumer
            .poll(Duration::from_millis(60))
            .await
            .unwrap()
            .len();
    }
    assert2::assert!(redelivered == 0);

    consumer.close().await.unwrap();
    broker.shutdown().await;
}

/// F1 (client renew): `renew()` is rejected in Implicit ack mode (records are
/// auto-accepted on the next poll/close, so renewing a lock is meaningless) —
/// it returns `ConsumerError::IllegalState` without any wire round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_errors_in_implicit_mode() {
    let _permit = broker_test_permit().await;
    use crabka_client_consumer::ConsumerError;

    let dir = TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("setup")
        .build()
        .await
        .unwrap();

    create_topic_led(&broker, &admin, "imp").await;
    let tid = topic_id(&broker, "imp");
    bootstrap_share_state(&broker, &admin, &format!("impg:{tid}:0")).await;
    produce_n(&broker, &admin, "imp", tid, 1).await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("imp-1")
        .group_id("impg")
        .subscribe(vec!["imp".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .heartbeat_interval(crabka_units::millis(200))
        .build()
        .await
        .unwrap();

    wait_for_share_init(&broker, "impg", tid, 0).await;

    let first = poll_until(&mut consumer, 1, Duration::from_secs(15)).await;
    assert2::assert!(first.len() == 1);

    let err = consumer.renew(&first[0]).await;
    assert2::assert!(matches!(err, Err(ConsumerError::IllegalState(_))));

    consumer.close().await.unwrap();
    broker.shutdown().await;
}
