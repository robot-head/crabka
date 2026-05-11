//! Integration tests against a real Apache Kafka via testcontainers.
//!
//! All tests are gated with `#[ignore]` so `cargo test --workspace` doesn't
//! pull Docker by default. Run with:
//!
//! ```text
//! cargo test -p crabka-client-core --test integration -- --ignored --nocapture
//! ```
//!
//! Each test spins up a fresh `apache/kafka-native:3.8.0` container in KRaft
//! mode (no ZooKeeper) and tears it down when the test exits.

// Skip compilation on Windows runners where testcontainers + Docker reliability
// is poor. On Linux CI the tests run via the `client-core-integration` job.
#![cfg(not(target_os = "windows"))]

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

use crabka_client_core::Client;

/// Start a Kafka container and return the container handle + bootstrap address.
async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let kafka = Kafka::default().start().await.expect("kafka container failed to start");
    let port = kafka
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("failed to get mapped port");
    (kafka, format!("127.0.0.1:{port}"))
}

// ── tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker"]
async fn api_versions_against_real_broker() {
    use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;

    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap)
        .client_id("crabka-integration")
        .build()
        .await
        .expect("client build failed");

    let resp = client
        .send(ApiVersionsRequest::default())
        .await
        .expect("ApiVersions failed");

    assert_eq!(resp.error_code, 0, "ApiVersions returned error: {}", resp.error_code);
    assert!(!resp.api_keys.is_empty(), "broker advertised no APIs");
    // ApiVersions (key 18) is always present in any modern Kafka broker.
    assert!(
        resp.api_keys.iter().any(|k| k.api_key == 18),
        "ApiVersions key 18 not found in response"
    );

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn metadata_against_real_broker() {
    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.expect("client build failed");

    let resp = client.refresh_metadata().await.expect("refresh_metadata failed");
    assert!(!resp.brokers.is_empty(), "expected at least one broker in metadata");

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn create_then_delete_topic() {
    use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
    use crabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};

    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.expect("client build failed");

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "crabka-test-topic".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let create_resp = client.send(create).await.expect("CreateTopics failed");
    let topic_result = &create_resp.topics[0];
    assert_eq!(
        topic_result.error_code, 0,
        "CreateTopics error: {topic_result:?}"
    );

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("crabka-test-topic".into()),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let delete_resp = client.send(delete).await.expect("DeleteTopics failed");
    let del_result = &delete_resp.responses[0];
    assert_eq!(
        del_result.error_code, 0,
        "DeleteTopics error: {del_result:?}"
    );

    client.close().await;
    drop(kafka);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn list_topics() {
    use crabka_protocol::owned::metadata_request::MetadataRequest;

    let (kafka, bootstrap) = start_kafka().await;
    let client = Client::builder(&bootstrap).build().await.expect("client build failed");

    // `MetadataRequest::default()` has `topics = None`, which lists all topics.
    let resp = client.send(MetadataRequest::default()).await.expect("Metadata failed");
    // Smoke-test: we can decode the response without errors.
    let _ = resp.topics;

    client.close().await;
    drop(kafka);
}
