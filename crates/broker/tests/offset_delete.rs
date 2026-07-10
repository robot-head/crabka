//! `OffsetDelete` (`api_key` 47, KIP-496) broker integration tests.
//!
//! Each test boots a single broker via `support::start`, drives the
//! relevant flows over the PLAINTEXT data plane, and asserts on the
//! response shape. Gated to non-Windows in line with the multi-broker
//! test convention (single-broker tests stay unconditional but this
//! file only uses `support::start` which is cross-platform — no gate
//! needed).

use assert2::check;
mod support;

use bytes::BufMut;
use crabka_protocol::{
    Encode,
    owned::{
        consumer_protocol_subscription::ConsumerProtocolSubscription,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
        offset_delete_request::{
            OffsetDeleteRequest, OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
        },
        offset_fetch_request::{
            OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

const OFFSET_ABSENT_SENTINEL: i64 = -1; // OffsetFetch returns -1 when no offset is committed.

/// Resolve a topic's UUID via Metadata. KIP-516: OffsetCommit/OffsetFetch
/// negotiate to v10/v8+, which key by `topic_id` on the wire.
async fn topic_id_for(p: &support::InProcess, name: &str) -> WireUuid {
    let resp = p
        .client
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

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    let resp = p
        .client
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
    assert2::assert!(resp.topics[0].error_code == 0);
}

async fn commit_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32, off: i64) {
    let id = topic_id_for(p, topic).await;
    let resp = p
        .client
        .send(OffsetCommitRequest {
            group_id: group.into(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            topics: vec![OffsetCommitRequestTopic {
                name: topic.into(),
                topic_id: id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: partition,
                    committed_offset: off,
                    committed_leader_epoch: -1,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert2::assert!(resp.topics[0].partitions[0].error_code == 0);
}

async fn fetch_offset(p: &support::InProcess, group: &str, topic: &str, partition: i32) -> i64 {
    let id = topic_id_for(p, topic).await;
    let resp = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: group.into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: topic.into(),
                    topic_id: id,
                    partition_indexes: vec![partition],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetFetch");
    resp.groups[0].topics[0].partitions[0].committed_offset
}

/// T1 — happy path: commit an offset for an Empty group, delete it,
/// `OffsetFetch` then returns `-1` (no committed offset).
#[tokio::test]
async fn delete_offsets_from_empty_group_round_trip() {
    let p = support::start().await;
    create_topic(&p, "t1", 2).await;

    commit_offset(&p, "g1", "t1", 0, 42).await;
    commit_offset(&p, "g1", "t1", 1, 100).await;

    // Group is Empty (no JoinGroup), delete should succeed.
    let resp = p
        .client
        .send(OffsetDeleteRequest {
            group_id: "g1".into(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "t1".into(),
                partitions: vec![OffsetDeleteRequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetDelete");
    check!(
        (
            resp.error_code,
            resp.topics.len(),
            resp.topics[0].partitions.len(),
            resp.topics[0].partitions[0].error_code,
        ) == (0, 1, 1, 0),
        "OffsetDelete response shape mismatch: {resp:?}"
    );

    // Verify partition 0 is gone, partition 1 still has its offset.
    check!(
        fetch_offset(&p, "g1", "t1", 0).await == OFFSET_ABSENT_SENTINEL,
        "partition 0 offset cleared"
    );
    check!(
        fetch_offset(&p, "g1", "t1", 1).await == 100,
        "partition 1 offset untouched"
    );

    p.broker.shutdown().await;
}

/// T2 — unknown group: `OffsetDelete` against a group that was never
/// joined or committed returns `GROUP_ID_NOT_FOUND` at the top level.
#[tokio::test]
async fn delete_offsets_unknown_group_returns_group_id_not_found() {
    let p = support::start().await;
    create_topic(&p, "t2", 1).await;

    let resp = p
        .client
        .send(OffsetDeleteRequest {
            group_id: "ghost".into(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "t2".into(),
                partitions: vec![OffsetDeleteRequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetDelete");
    assert2::assert!(resp.error_code == 69);

    p.broker.shutdown().await;
}

/// T3 — missing topic: per-partition `UNKNOWN_TOPIC_OR_PARTITION` (3)
/// for a topic that doesn't exist (even when the group does).
#[tokio::test]
async fn delete_offsets_missing_topic_returns_unknown_topic_or_partition() {
    let p = support::start().await;
    create_topic(&p, "t3", 1).await;
    commit_offset(&p, "g3", "t3", 0, 7).await;

    let resp = p
        .client
        .send(OffsetDeleteRequest {
            group_id: "g3".into(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "nonexistent".into(),
                partitions: vec![OffsetDeleteRequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetDelete");
    assert2::assert!((resp.error_code, resp.topics[0].partitions[0].error_code) == (0, 3));

    p.broker.shutdown().await;
}

/// T4 — out-of-range partition: per-partition
/// `UNKNOWN_TOPIC_OR_PARTITION` (3) for a partition index beyond the
/// topic's partition count.
#[tokio::test]
async fn delete_offsets_partition_out_of_range_returns_unknown_topic_or_partition() {
    let p = support::start().await;
    create_topic(&p, "t4", 1).await;
    commit_offset(&p, "g4", "t4", 0, 5).await;

    let resp = p
        .client
        .send(OffsetDeleteRequest {
            group_id: "g4".into(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "t4".into(),
                partitions: vec![
                    OffsetDeleteRequestPartition {
                        partition_index: 0,
                        ..Default::default()
                    },
                    OffsetDeleteRequestPartition {
                        partition_index: 99,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetDelete");
    check!(
        (
            resp.error_code,
            resp.topics[0].partitions[0].error_code,
            resp.topics[0].partitions[1].error_code
        ) == (0, 0, 3),
        "p=99 UNKNOWN_TOPIC_OR_PARTITION"
    );

    p.broker.shutdown().await;
}

/// T5 — subscription guard: a non-empty consumer-protocol group still
/// subscribed to the topic returns per-partition
/// `GROUP_SUBSCRIBED_TO_TOPIC` (86). The offset survives the request.
#[tokio::test]
async fn delete_offsets_for_subscribed_topic_returns_group_subscribed() {
    let p = support::start().await;
    create_topic(&p, "t5", 1).await;
    commit_offset(&p, "g5", "t5", 0, 9).await;

    // Build a ConsumerProtocolSubscription bytestring that subscribes to t5.
    let sub = ConsumerProtocolSubscription {
        topics: vec!["t5".into()],
        ..Default::default()
    };
    let mut sub_bytes = bytes::BytesMut::new();
    sub_bytes.put_i16(0); // protocol version negotiation prefix
    sub.encode(&mut sub_bytes, 0).expect("encode subscription");
    let sub_bytes = bytes::Bytes::from(sub_bytes.to_vec());

    // JoinGroup to create a live member with that subscription.
    let r1 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g5".into(),
            protocol_type: "consumer".into(),
            member_id: String::new(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: sub_bytes.clone(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup1");
    // First JoinGroup with empty member_id returns MEMBER_ID_REQUIRED (79).
    assert2::assert!(r1.error_code == 79);
    let mid = r1.member_id.clone();
    assert2::assert!(!mid.is_empty());

    // Re-join with the assigned member_id → become an actual member.
    let r2 = p
        .client
        .send(JoinGroupRequest {
            group_id: "g5".into(),
            protocol_type: "consumer".into(),
            member_id: mid,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 1_500,
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".into(),
                metadata: sub_bytes,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("JoinGroup2");
    assert2::assert!(r2.error_code == 0);

    // OffsetDelete on the subscribed topic → GROUP_SUBSCRIBED_TO_TOPIC.
    let resp = p
        .client
        .send(OffsetDeleteRequest {
            group_id: "g5".into(),
            topics: vec![OffsetDeleteRequestTopic {
                name: "t5".into(),
                partitions: vec![OffsetDeleteRequestPartition {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("OffsetDelete");
    check!(
        (resp.error_code, resp.topics[0].partitions[0].error_code) == (0, 86),
        "GROUP_SUBSCRIBED_TO_TOPIC (86)"
    );

    // Offset is unchanged.
    check!(
        fetch_offset(&p, "g5", "t5", 0).await == 9,
        "offset survived"
    );

    p.broker.shutdown().await;
}
