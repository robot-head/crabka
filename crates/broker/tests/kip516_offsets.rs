//! KIP-516: `OffsetCommit` v10 / `OffsetFetch` v8+ by `topic_id`.
use assert2::assert;
mod support;

use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopics,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

async fn topic_id_for(client: &crabka_client_core::Client, name: &str) -> WireUuid {
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

#[tokio::test]
async fn offset_commit_and_fetch_by_topic_id_round_trip() {
    let p = support::start().await;
    p.client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "o_topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
    let id = topic_id_for(&p.client, "o_topic").await;

    // Commit offset 42 by topic_id (v10: name empty, id set). Empty member_id
    // skips the membership check.
    p.client
        .send(OffsetCommitRequest {
            group_id: "g1".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: String::new(),
                topic_id: id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 42,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    // Fetch back via v8+ multi-group shape keyed by topic_id.
    let resp = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g1".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: String::new(),
                    topic_id: id,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset fetch");

    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g1")
        .expect("group g1");
    let t = grp
        .topics
        .iter()
        .find(|t| t.topic_id == id)
        .expect("topic by id");
    let part = t.partitions.first().expect("partition 0");
    assert!(part.committed_offset == 42);
    assert!(part.error_code == 0);
    assert!(t.topic_id == id); // id echoed
}

#[tokio::test]
async fn offset_fetch_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xabad_1dea).into_bytes());
    let resp = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g2".into(),
                topics: Some(vec![OffsetFetchRequestTopics {
                    name: String::new(),
                    topic_id: bogus,
                    partition_indexes: vec![0],
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset fetch");
    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g2")
        .expect("group g2");
    let t = grp.topics.first().expect("a topic row");
    assert!(t.partitions.first().expect("a partition").error_code == 100);
}

/// Fetch-all (null `topics`) at v10 must echo each topic's `topic_id`, since
/// the name is dropped from the wire at v10 and the client matches by id.
#[tokio::test]
async fn offset_fetch_all_echoes_topic_id() {
    let p = support::start().await;
    p.client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "fa_topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");
    let id = topic_id_for(&p.client, "fa_topic").await;

    p.client
        .send(OffsetCommitRequest {
            group_id: "g3".into(),
            topics: vec![OffsetCommitRequestTopic {
                name: String::new(),
                topic_id: id,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 7,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset commit");

    // Fetch-all: `topics: None` for the group.
    let resp = p
        .client
        .send(OffsetFetchRequest {
            groups: vec![OffsetFetchRequestGroup {
                group_id: "g3".into(),
                topics: None,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("offset fetch");
    let grp = resp
        .groups
        .iter()
        .find(|g| g.group_id == "g3")
        .expect("group g3");
    let t = grp
        .topics
        .iter()
        .find(|t| t.topic_id == id)
        .expect("topic row with echoed id");
    assert!(t.partitions.first().expect("a partition").committed_offset == 7);
}
