mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};

#[tokio::test]
async fn api_versions_round_trip() {
    let p = support::start().await;
    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert_eq!(resp.error_code, 0);
    // Must include ApiVersions itself.
    assert!(resp.api_keys.iter().any(|k| k.api_key == 18));
    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_then_delete_topic_round_trip() {
    let p = support::start().await;

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "alpha".into(),
            num_partitions: 2,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].num_partitions, 2);

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("alpha".into()),
            ..Default::default()
        }],
        topic_names: vec!["alpha".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let dresp = p.client.send(delete).await.expect("DeleteTopics");
    assert_eq!(dresp.responses.len(), 1);
    assert_eq!(dresp.responses[0].error_code, 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_topic_with_zero_partitions_errors() {
    let p = support::start().await;
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "zero".into(),
            num_partitions: 0,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 37); // INVALID_PARTITIONS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn duplicate_create_returns_topic_already_exists() {
    let p = support::start().await;
    let req = || CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "dup".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let r1 = p.client.send(req()).await.expect("CreateTopics 1");
    assert_eq!(r1.topics[0].error_code, 0);
    let r2 = p.client.send(req()).await.expect("CreateTopics 2");
    assert_eq!(r2.topics[0].error_code, 36); // TOPIC_ALREADY_EXISTS
    p.broker.shutdown().await;
}
