//! KIP-516: `DeleteTopics` by `topic_id` error semantics.
mod support;

use crabka_protocol::{
    owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    primitives::uuid::Uuid as WireUuid,
};

#[tokio::test]
async fn delete_topics_by_unknown_id_returns_unknown_topic_id() {
    let p = support::start().await;
    let bogus = WireUuid(uuid::Uuid::from_u128(0xc0ff_ee00).into_bytes());

    let resp = p
        .client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: None,
                topic_id: bogus,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("delete topics");

    let r = resp.responses.first().expect("one response row");
    assert2::assert!((r.error_code, r.topic_id) == (100, bogus)); // UNKNOWN_TOPIC_ID, requested id echoed
}
