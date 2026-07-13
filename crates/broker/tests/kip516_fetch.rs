//! KIP-516: Fetch by `topic_id` error semantics.
use assert2::assert;
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    },
    primitives::uuid::Uuid as WireUuid,
};

#[tokio::test]
async fn fetch_unknown_topic_id_returns_unknown_topic_id() {
    let p = support::start().await;
    p.client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "f_known".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("create topic");

    // A random UUID the cluster has never assigned.
    let bogus = WireUuid(uuid::Uuid::from_u128(0xdead_beef).into_bytes());
    let resp = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            topics: vec![FetchTopic {
                topic: String::new(), // v13+: name absent, id-only on the wire
                topic_id: bogus,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1_048_576,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("fetch");

    let code = resp
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .map(|pp| pp.error_code)
        .next()
        .expect("a partition row");
    assert!(code == 100); // UNKNOWN_TOPIC_ID
}
