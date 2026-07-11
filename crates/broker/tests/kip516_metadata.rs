//! KIP-516: Metadata by `topic_id` error semantics.
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

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
async fn metadata_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xfeed_face).into_bytes());

    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: None,
                topic_id: bogus,
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");

    let t = resp
        .topics
        .iter()
        .find(|t| t.topic_id == bogus)
        .expect("topic entry echoing the requested id");
    assert2::assert!(t.error_code == 100); // UNKNOWN_TOPIC_ID
}

#[tokio::test]
async fn metadata_inconsistent_name_and_id_returns_inconsistent() {
    let p = support::start().await;
    for n in ["m_a", "m_b"] {
        p.client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: n.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("create topic");
    }
    let id_b = topic_id_for(&p.client, "m_b").await;

    // Request names "m_a" but supplies m_b's id → INCONSISTENT_TOPIC_ID.
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("m_a".into()),
                topic_id: id_b,
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");
    let t = resp
        .topics
        .iter()
        .find(|t| t.topic_id == id_b || t.name.as_deref() == Some("m_a"))
        .expect("topic entry");
    assert2::assert!(t.error_code == 103); // INCONSISTENT_TOPIC_ID
}
