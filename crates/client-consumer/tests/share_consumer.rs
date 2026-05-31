//! Smoke test for the KIP-932 [`ShareConsumer`] membership skeleton: start an
//! in-process broker, create a topic, join a share group, assert we got a
//! member id, then close cleanly. `poll()`/`acknowledge()` coverage lands with
//! Task E2/E3.

use assert2::assert;
use std::time::Duration;

use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{ShareAckMode, ShareConsumer};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

async fn create_topic(client: &Client, name: &str) {
    let cr = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(cr.topics[0].error_code == 0, "create_topic failed: {cr:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn share_consumer_joins_and_closes() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .client_id("share-admin")
        .build()
        .await
        .unwrap();
    create_topic(&admin, "share-topic").await;

    let mut consumer = ShareConsumer::builder()
        .bootstrap(&bootstrap)
        .client_id("share-consumer")
        .group_id("share-group-1")
        .subscribe(["share-topic".to_string()])
        .ack_mode(ShareAckMode::Implicit)
        .session_timeout(Duration::from_secs(30))
        .heartbeat_interval(Duration::from_secs(1))
        .build()
        .await
        .expect("ShareConsumer build");

    assert!(
        !consumer.member_id().is_empty(),
        "broker must assign a member id on join"
    );
    assert!(consumer.group_id() == "share-group-1");

    consumer.close().await.expect("close");
}
