mod support;

use crabka_broker::coordinator::AUDIT_TOPIC;
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

#[tokio::test]
async fn audit_topic_exists_after_startup() {
    let p = support::start().await;

    // Send a Metadata request for `__crabka_audit` and assert the broker
    // returns it with `error_code == 0` and at least one partition.
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(AUDIT_TOPIC.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("MetadataRequest failed");

    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(AUDIT_TOPIC))
        .expect("__crabka_audit not in Metadata response");

    assert2::check!(
        topic.error_code == 0,
        "unexpected error code: {}",
        topic.error_code
    );
    assert2::check!(
        !topic.partitions.is_empty(),
        "__crabka_audit has no partitions"
    );

    p.broker.shutdown().await;
}
