mod support;

use crabka_broker::coordinator::AUDIT_TOPIC;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
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

#[tokio::test]
async fn broker_started_event_is_written_to_audit_topic() {
    let p = support::start().await;

    // Let bootstrap + the BrokerStarted emit settle.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 200,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();

    let part = &fr.responses[0].partitions[0];
    assert2::check!(part.error_code == 0);
    let batches = part
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("v2 records");
    let mut saw_started = false;
    for b in batches {
        for r in &b.records {
            if let Some(v) = &r.value {
                let j: serde_json::Value = serde_json::from_slice(v).unwrap();
                if j["class_uid"] == 6002 && j["activity_name"] == "BrokerStarted" {
                    saw_started = true;
                }
            }
        }
    }
    assert2::check!(saw_started);

    p.broker.shutdown().await;
}
