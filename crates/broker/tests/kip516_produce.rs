//! KIP-516: Produce by `topic_id` error semantics.
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
};

#[tokio::test]
async fn produce_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    p.client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "p_known".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");

    let bogus = WireUuid(uuid::Uuid::from_u128(0x0bad_f00d).into_bytes());
    let resp = p
        .client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: String::new(), // v13: id-only on the wire
                topic_id: bogus,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: None,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("produce");

    let code = resp
        .responses
        .iter()
        .flat_map(|t| t.partition_responses.iter())
        .map(|pr| pr.error_code)
        .next()
        .expect("a partition response");
    assert2::assert!(code == 100); // UNKNOWN_TOPIC_ID
}
