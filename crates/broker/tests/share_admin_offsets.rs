#![allow(clippy::pedantic)]

//! End-to-end integration tests for KIP-932 Slice D: share-group admin offset
//! RPCs — `DescribeShareGroupOffsets` (`api_key` 90), `AlterShareGroupOffsets`
//! (91), `DeleteShareGroupOffsets` (92).
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 90/91/92 and
//! all three requests impl `ProtocolRequest`, so `client.send(req)` exercises
//! the real wire path (frame parse → handler → encode, version-negotiated).
//!
//! These tests prove:
//! - Describe reflects the durable SPSO after a consume+Accept advances it, and
//!   reports lag = HWM − SPSO for a locally-led partition;
//! - Alter on an *empty* group resets the SPSO (state-epoch bump + re-init) AND
//!   invalidates the share-partition leader cache so a subsequent `ShareFetch`
//!   acquires starting at the new offset;
//! - Alter on a *non-empty* (live-member) group is rejected with NON_EMPTY_GROUP;
//! - Delete removes the durable share-state for a topic (Describe then reads the
//!   partition as missing → `start_offset` -1);
//! - Describe of an unknown topic returns UNKNOWN_TOPIC_OR_PARTITION per
//!   partition.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        alter_share_group_offsets_request::{
            AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
            AlterShareGroupOffsetsRequestTopic,
        },
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_share_group_offsets_request::{
            DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
        },
        describe_share_group_offsets_request::{
            DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
            DescribeShareGroupOffsetsRequestTopic,
        },
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{
            AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::ShareFetchResponse,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

const NONE: i16 = 0;
const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const UNSUPPORTED_VERSION: i16 = 35;
const NON_EMPTY_GROUP: i16 = 68;

const ACCEPT: i8 = 1;
const ONE_MB: i32 = 1 << 20;

// ────────────────────────────────────────────────────────────────────────
// Harness (copied from tests/share_consume.rs — tests are separate
// compilation units, each carries its own helper copies).
// ────────────────────────────────────────────────────────────────────────

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

async fn create_topic(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    partitions: i32,
) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
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

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

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
        "FindCoordinator(SHARE) error: {}",
        resp.coordinators[0].error_code
    );
    // Wait until every state partition this single broker should lead is local,
    // so the share-state writes land durably.
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

async fn wait_for_share_init(
    broker: &crabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    // Delegates to the broker-handle awaiter (30s timeout, 25ms poll interval).
    // `join()` drives the steady-state heartbeats that trigger the lifecycle hook
    // before this is called, so no repeated heartbeats are needed here.
    broker
        .wait_for_share_state_summary(group, tid, partition)
        .await;
}

async fn produce_n(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, n: i64) {
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
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
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
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-RPC retry on UNKNOWN_TOPIC_OR_PARTITION /
            // NOT_LEADER_OR_FOLLOWER while the partition's local writer materializes;
            // this helper has no BrokerHandle to await on and returns via the RPC response.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// Join `group` subscribed to `topic`, driving steady-state heartbeats so the
/// lifecycle hook initializes the subscribed partitions' share state. Returns
/// `(member_id, member_epoch)` so the caller can leave with the live epoch.
async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    let member_id = resp.member_id.expect("broker must mint a member id");
    let mut epoch = resp.member_epoch;

    for _ in 0..3 {
        let hb = client
            .send(ShareGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.clone(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec![topic.into()]),
                ..Default::default()
            })
            .await
            .expect("ShareGroupHeartbeat steady-state");
        epoch = hb.member_epoch;
        // intentional: paces steady-state heartbeats to drive the membership
        // reconciliation / lifecycle hook forward; this drives the protocol rather
        // than waiting on a single observable state (share init is awaited separately).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (member_id, epoch)
}

/// Leave the group via `member_epoch == -1`; the group is retained but reported
/// with zero members (state "Empty").
async fn leave(client: &Client, group: &str, member_id: &str) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat leave");
    assert!(resp.error_code == 0, "leave failed: {:?}", resp.error_code);
}

fn share_fetch_req(
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
    acks: Vec<FetchAckBatch>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: ONE_MB,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition,
                partition_max_bytes: ONE_MB,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}

async fn share_fetch(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
) -> crabka_protocol::owned::share_fetch_response::PartitionData {
    let req = share_fetch_req(group, member, tid, partition, epoch, max_wait_ms, vec![]);
    let resp: ShareFetchResponse = client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == NONE,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

#[allow(clippy::too_many_arguments)]
async fn share_ack(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    first: i64,
    last: i64,
    ack_type: i8,
) -> crabka_protocol::owned::share_acknowledge_response::PartitionData {
    let count = usize::try_from(last - first + 1).unwrap();
    let req = ShareAcknowledgeRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        is_renew_ack: false,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(tid),
            partitions: vec![AcknowledgePartition {
                partition_index: partition,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: first,
                    last_offset: last,
                    acknowledge_types: vec![ack_type; count],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

fn acquired_count(p: &crabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
}

async fn fetch_until_acquired(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
) -> crabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = share_fetch(client, group, member, tid, partition, epoch, 0).await;
        if row.error_code == NONE && acquired_count(&row) > 0 {
            return row;
        }
        // intentional: bounded ShareFetch-RPC poll — the fetch IS the acquiring action
        // and its response row is returned for assertions, so an awaiter can't replace it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records for {group}:{tid}:{partition}");
}

// ────────────────────────────────────────────────────────────────────────
// Admin-offset request helpers (the RPCs under test).
// ────────────────────────────────────────────────────────────────────────

/// `DescribeShareGroupOffsets` for a single `(group, topic, partitions)`.
/// Returns the (single) topic row. `partitions` empty ⇒ "all initialized".
async fn describe_offsets(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
) -> crabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    let resp = client
        .send(DescribeShareGroupOffsetsRequest {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group.into(),
                topics: Some(vec![DescribeShareGroupOffsetsRequestTopic {
                    topic_name: topic.into(),
                    partitions,
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeShareGroupOffsets");
    resp.groups[0].clone()
}

// ────────────────────────────────────────────────────────────────────────
// Tests.
// ────────────────────────────────────────────────────────────────────────

/// Describe reflects the SPSO after a consume that Accepts all records: the
/// SPSO advances to 3 and the (locally-led) partition reports lag 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_reflects_spso_after_consume() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 3).await;
    let (member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;

    // Acquire 0..2, Accept all → SPSO advances to 3.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "must acquire all 3 offsets");
    let ack = share_ack(&client, "g1", &member, tid, 0, 1, 0, 2, ACCEPT).await;
    assert!(ack.error_code == NONE, "accept error: {}", ack.error_code);

    // Let the persister land the advanced SPSO durably.
    let group = describe_until(&client, "g1", "t", vec![0], 3).await;
    let part = &group.topics[0].partitions[0];
    check!((group.error_code, group.topics[0].topic_name.as_str()) == (NONE, "t"));
    check!((part.error_code, part.start_offset, part.lag) == (NONE, 3, 0));
}

/// Poll Describe until the partition reports the expected SPSO (the persister
/// write of the advanced SPSO is asynchronous after the Accept ack).
async fn describe_until(
    client: &Client,
    group: &str,
    topic: &str,
    partitions: Vec<i32>,
    want_spso: i64,
) -> crabka_protocol::owned::describe_share_group_offsets_response::DescribeShareGroupOffsetsResponseGroup
{
    let mut last = describe_offsets(client, group, topic, partitions.clone()).await;
    for _ in 0..40 {
        if let Some(p) = last.topics.first().and_then(|t| t.partitions.first())
            && p.start_offset == want_spso
        {
            return last;
        }
        // intentional: bounded Describe-RPC poll for the async persister write of the
        // SPSO; returns the response for assertions and also serves the deleted (-1)
        // case that no share-SPSO awaiter covers.
        tokio::time::sleep(Duration::from_millis(100)).await;
        last = describe_offsets(client, group, topic, partitions.clone()).await;
    }
    last
}

/// Alter resets the SPSO of an empty group: the persister state is re-initialized
/// at the requested offset, the leader cache is invalidated, and a subsequent
/// `ShareFetch` acquires starting at the new offset.
///
/// The group has *no members* when Alter runs, so the share-state has never been
/// seeded by the membership lifecycle (a member join/leave would reap the state
/// when the group empties). Alter therefore initializes-from-absent at
/// `state_epoch = 1` with `start_offset = 5`. A subsequent first-join then
/// reconciles at `group_epoch = 1`; its lifecycle re-init (`initialize(1, 0)`) is
/// *fenced* by the equal-or-higher durable `state_epoch`, so the Alter's SPSO
/// survives and the first `ShareFetch` acquires from offset 5 — exercising the
/// real acquire path against the reset (and invalidated) leader cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_resets_empty_group() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    // Make the share coordinator write-ready WITHOUT joining (no members).
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    // Produce 6 records so offset 5 exists.
    produce_n(&client, "t", tid, 0, 6).await;

    // Alter: reset SPSO to 5 on the empty (never-joined) group. This
    // initializes-from-absent at state_epoch 1, and invalidates the (empty)
    // leader cache. Retry while the persister leadership is still settling.
    let mut altered = false;
    for _ in 0..40 {
        let resp = client
            .send(AlterShareGroupOffsetsRequest {
                group_id: "g1".into(),
                topics: vec![AlterShareGroupOffsetsRequestTopic {
                    topic_name: "t".into(),
                    partitions: vec![AlterShareGroupOffsetsRequestPartition {
                        partition_index: 0,
                        start_offset: 5,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("AlterShareGroupOffsets");
        if resp.error_code == NONE && resp.responses[0].partitions[0].error_code == NONE {
            altered = true;
            break;
        }
        // intentional: bounded retry of the Alter mutation RPC while the share
        // persister leadership settles; coordinator-local state with no
        // metadata-image signal or awaiter.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(altered, "AlterShareGroupOffsets never succeeded");

    // Describe now reports the new SPSO.
    let group = describe_until(&client, "g1", "t", vec![0], 5).await;
    assert!(
        group.topics[0].partitions[0].start_offset == 5,
        "SPSO must be 5 after Alter, got {}",
        group.topics[0].partitions[0].start_offset
    );

    // Join and ShareFetch: must acquire starting at offset 5 (the reset SPSO).
    // The first-join lifecycle re-init is fenced by the Alter's state_epoch, so
    // the acquire reads the reset SPSO 5 via the invalidated leader cache.
    let (member, _epoch) = join(&client, "g1", "t").await;
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(
        row.acquired_records[0].first_offset == 5,
        "fetch after Alter must acquire from offset 5, got {:?}",
        row.acquired_records
    );
}

/// Alter on a non-empty (live-member) group is rejected top-level with
/// NON_EMPTY_GROUP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_non_empty_group_fenced() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 3).await;

    // Live member present (steady-state heartbeat), never leaves.
    let (_member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;

    let resp = client
        .send(AlterShareGroupOffsetsRequest {
            group_id: "g1".into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: "t".into(),
                partitions: vec![AlterShareGroupOffsetsRequestPartition {
                    partition_index: 0,
                    start_offset: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterShareGroupOffsets");
    assert!(
        resp.error_code == NON_EMPTY_GROUP,
        "alter on non-empty group must be NON_EMPTY_GROUP (68), got {}",
        resp.error_code
    );
}

/// Delete removes the durable share-state for a topic of an empty group; a
/// subsequent Describe reads the partition as missing (`start_offset` -1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_topic() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 3).await;

    // Initialize the topic's share state via the join lifecycle + a consume, then
    // leave so the group is empty.
    let (member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;
    let _ = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    leave(&client, "g1", &member).await;

    let resp = client
        .send(DeleteShareGroupOffsetsRequest {
            group_id: "g1".into(),
            topics: vec![DeleteShareGroupOffsetsRequestTopic {
                topic_name: "t".into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DeleteShareGroupOffsets");
    check!(
        (
            resp.error_code,
            resp.responses[0].topic_name.as_str(),
            resp.responses[0].error_code
        ) == (NONE, "t", NONE)
    );

    // Describe now reads the removed partition as missing → start_offset -1.
    let group = describe_until(&client, "g1", "t", vec![0], -1).await;
    let part = &group.topics[0].partitions[0];
    assert!(
        (part.start_offset, part.error_code) == (-1, NONE),
        "describe of missing partition is not an error, got {}",
        part.error_code
    );
}

/// Describe of an unknown topic returns UNKNOWN_TOPIC_OR_PARTITION per partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_topic() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    // The persister must exist for the handler to reach topic resolution; a
    // FindCoordinator(SHARE) bootstrap makes the share coordinator available.
    // The key needs a syntactically valid `group:topicId:partition` shape; the
    // topic id need not refer to a real topic for the bootstrap to succeed.
    let dummy = uuid::Uuid::new_v4();
    bootstrap_share_state(&broker, &client, &format!("g1:{dummy}:0")).await;

    let group = describe_offsets(&client, "g1", "nonexistent", vec![0]).await;
    assert!(
        group.error_code == NONE,
        "group-level describe must succeed, got {}",
        group.error_code
    );
    let part = &group.topics[0].partitions[0];
    assert!(
        part.error_code == UNKNOWN_TOPIC_OR_PARTITION,
        "unknown topic must be UNKNOWN_TOPIC_OR_PARTITION (3), got {}",
        part.error_code
    );
}

/// With `share_group.enable = false` the admin offset RPCs are not implemented:
/// `DescribeShareGroupOffsets` marks each requested group `UNSUPPORTED_VERSION`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_offsets_rejected_when_share_disabled() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.share_group.enable = false;
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;

    let group = describe_offsets(&client, "g1", "t", vec![0]).await;
    assert!(
        group.error_code == UNSUPPORTED_VERSION,
        "share-disabled describe must be UNSUPPORTED_VERSION (35), got {}",
        group.error_code
    );
}

// ────────────────────────────────────────────────────────────────────────
// Slice F tests.
// ────────────────────────────────────────────────────────────────────────

/// F3 (lag restore): the cumulative `delivery_complete_count` (number of
/// terminally-acknowledged records — the basis for share-group lag) survives a
/// broker restart. Before Slice F, `load_from` hard-reset it to 0, so the
/// recovered group under-reported its completed work.
///
/// Produce N, consume + Accept all (SPSO advances to N, dcc = N), wait for the
/// persist, restart on the same dir (Rejoin), then read the share-state summary:
/// its 4th element (`delivery_complete_count`) must be the restored N, not 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_complete_count_restored_across_restart() {
    const N: i64 = 4;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
        produce_n(&client, "t", tid, 0, N).await;
        let (member, _epoch) = join(&client, "g1", "t").await;
        wait_for_share_init(&broker, "g1", tid, 0).await;

        // Acquire 0..N-1 and Accept all → SPSO advances to N, dcc = N.
        let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
        assert!(acquired_count(&row) == N, "must acquire all {N} offsets");
        let ack = share_ack(&client, "g1", &member, tid, 0, 1, 0, N - 1, ACCEPT).await;
        assert!(ack.error_code == NONE, "accept error: {}", ack.error_code);

        // Wait until the persisted summary reflects dcc == N before restarting.
        broker
            .wait_until_share_delivery_complete("g1", tid, 0, i32::try_from(N).unwrap())
            .await;
        let dcc = broker
            .share_state_summary_for_test("g1", tid, 0)
            .await
            .map(|(_, _, _, d)| d)
            .unwrap_or(-1);
        assert!(
            dcc == i32::try_from(N).unwrap(),
            "pre-restart dcc must be {N}, got {dcc}"
        );

        // The awaiter above confirms dcc is durable; shut down immediately.
        broker.shutdown().await;
    }

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

        // The recovered summary must report the RESTORED dcc == N (not 0). The
        // summary load is driven by the share coordinator reading the persisted
        // record; await until the recovered state is present, then assert.
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        let summary = broker
            .share_state_summary_for_test("g1", tid, 0)
            .await
            .expect("summary present after wait_for_share_state_summary");
        let (_se, _le, start, dcc) = summary;
        // Sanity: the SPSO also recovered past the accepted records.
        assert!(start == N, "recovered SPSO must be {N}, got {start}");
        assert!(
            dcc == i32::try_from(N).unwrap(),
            "delivery_complete_count must be restored to {N} across restart, got {dcc}"
        );
    }
}

/// F6 (delete-metadata rewrite): `DeleteShareGroupOffsets` rewrites the v14
/// state-partition-metadata record so the deleted topic is gone from the
/// group's initialized set — and STAYS gone after a restart (the seed no longer
/// re-lists it).
///
/// A describe with an explicit topic name but an EMPTY partitions list
/// enumerates the group's *initialized* partitions for that topic (read from the
/// v14 metadata cache). Before delete that returns partition [0]; after the
/// delete rewrite the topic has no initialized partitions, so the row's
/// `partitions` list is empty — before AND after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_rewrites_metadata_topic_absent_after_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
        produce_n(&client, "t", tid, 0, 3).await;

        // Initialize the topic's share state via the join lifecycle + a consume,
        // then leave so the group is empty (Delete requires an empty group).
        let (member, _epoch) = join(&client, "g1", "t").await;
        wait_for_share_init(&broker, "g1", tid, 0).await;
        let _ = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;

        // Sanity: a describe with empty partitions enumerates the initialized
        // partitions for "t" — partition [0] is present before the delete.
        let before = describe_offsets(&client, "g1", "t", vec![]).await;
        let before_parts: Vec<i32> = before
            .topics
            .iter()
            .find(|t| t.topic_name == "t")
            .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
            .unwrap_or_default();
        assert!(
            before_parts == vec![0],
            "describe must enumerate initialized partition [0] before delete, got {before_parts:?}"
        );

        leave(&client, "g1", &member).await;

        let resp = client
            .send(DeleteShareGroupOffsetsRequest {
                group_id: "g1".into(),
                topics: vec![DeleteShareGroupOffsetsRequestTopic {
                    topic_name: "t".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("DeleteShareGroupOffsets");
        assert!(
            resp.error_code == NONE && resp.responses[0].error_code == NONE,
            "delete failed: top={} per-topic={}",
            resp.error_code,
            resp.responses[0].error_code
        );

        // The describe-by-name with empty partitions no longer enumerates any
        // initialized partition for "t" (the v14 metadata record was rewritten).
        // This is a NEGATIVE condition (absence); no broker awaiter exists for
        // "metadata rewrite complete", so we poll until the absence is observed.
        // Bounded to 4s — the delete RPC already succeeded so the rewrite is
        // in-flight, not guessing arbitrary settle time.
        let mut absent = false;
        for _ in 0..40 {
            let g = describe_offsets(&client, "g1", "t", vec![]).await;
            let parts: Vec<i32> = g
                .topics
                .iter()
                .find(|t| t.topic_name == "t")
                .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
                .unwrap_or_default();
            if parts.is_empty() {
                absent = true;
                break;
            }
            // real-time wait (not a progress poll): settle between re-checks asserting the deleted topic stays absent (absence, not a positive poll)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            absent,
            "describe must not enumerate any initialized partition for the deleted topic"
        );

        // `absent` is confirmed above (positive absence observed via describe).
        // A brief flush sleep lets the v14 metadata-rewrite persist to disk
        // before shutdown, so the restart below sees the rewritten seed.
        // This is a persist-flush, not a state-guessing settle: we already know
        // the in-memory state is correct.
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

        // After restart, the v14 seed no longer lists "t" (the rewrite removed
        // it), so the describe-by-name with empty partitions must STILL
        // enumerate zero initialized partitions. Poll a window to let the
        // coordinator finish replaying state, asserting absence throughout.
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        loop {
            let g = describe_offsets(&client, "g1", "t", vec![]).await;
            let parts: Vec<i32> = g
                .topics
                .iter()
                .find(|t| t.topic_name == "t")
                .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
                .unwrap_or_default();
            assert!(
                parts.is_empty(),
                "deleted topic must remain un-initialized after restart (v14 rewrite), got {parts:?}"
            );
            if std::time::Instant::now() >= deadline {
                break;
            }
            // real-time wait (not a progress poll): settle between re-checks asserting the deleted topic stays absent throughout the window (liveness, not a positive poll)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
