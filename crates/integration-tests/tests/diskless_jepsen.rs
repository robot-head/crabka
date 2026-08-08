//! Diskless WAL shipping-gate harness.
//!
//! This integration test is ignored on purpose. It is the place to add
//! Jepsen-style nemesis scenarios without making the normal cross-platform unit
//! lane slow. The first scenario runs the public client path across a broker
//! restart with the diskless WAL enabled.

use std::time::{Duration, Instant};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use tempfile::TempDir;

const TOPIC: &str = "diskless-jepsen";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "shipping-gate harness; run explicitly with --include-ignored"]
async fn diskless_restart_preserves_all_acked_records() {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();

    create_diskless_topic(&bootstrap).await;
    produce_values(&bootstrap, 0..8).await;

    broker.shutdown().await;

    let mut restart_config = BrokerConfig::for_tests(dir.path().to_path_buf());
    restart_config.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    let broker = Broker::start(restart_config).await.expect("broker restart");
    let bootstrap = broker.listen_addr().to_string();

    let observed = consume_count(&bootstrap, 8).await;
    assert!(observed == 8, "expected 8 records after restart");

    broker.shutdown().await;
}

async fn create_diskless_topic(bootstrap: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-jepsen-admin")
        .build()
        .await
        .expect("admin client");
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![CreatableTopicConfig {
                    name: "crabka.diskless".into(),
                    value: Some("true".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "create topic failed: {resp:?}"
    );
}

async fn produce_values(bootstrap: &str, values: impl Iterator<Item = usize>) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let mut futures = Vec::new();
    for value in values {
        futures.push(
            producer
                .send(ProducerRecord {
                    topic: TOPIC.into(),
                    value: Some(Bytes::from(format!("v{value}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.expect("flush");

    for future in futures {
        future.await.expect("oneshot").expect("produce result");
    }
    producer.close().await.expect("producer close");
}

async fn consume_count(bootstrap: &str, expected: usize) -> usize {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .client_id("diskless-jepsen-consumer")
        .group_id("diskless-jepsen-group")
        .session_timeout(crabka_units::secs(30))
        .rebalance_timeout(crabka_units::secs(2))
        .heartbeat_interval(crabka_units::secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe([TOPIC.to_string()])
        .build()
        .await
        .expect("consumer build");

    let mut seen = 0usize;
    let deadline = Instant::now() + Duration::from_secs(15);
    while seen < expected && Instant::now() < deadline {
        seen += consumer
            .poll(crabka_units::millis(500))
            .await
            .expect("poll")
            .len();
    }
    consumer.close().await.expect("consumer close");
    seen
}
