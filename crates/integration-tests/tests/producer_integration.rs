//! End-to-end: a Rust [`crabka_client_producer::Producer`] writes records
//! to an in-process [`crabka_broker`] and a Rust
//! [`crabka_client_consumer::Consumer`] reads them back.
//!
//! `flavor = "multi_thread", worker_threads = 2` is required for the same
//! reason as the acceptance tests: a single-threaded
//! runtime can't drive the broker's accept loop concurrently with the
//! producer's sender task and the test body.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use tempfile::TempDir;

/// Spin up an in-process broker and return its handle, bootstrap address,
/// and the `TempDir` (kept alive by the caller to control log-dir lifetime).
async fn boot() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

/// Create `name` with `partitions` partitions, replication factor 1, via a
/// short-lived [`Client`]. The broker handles topic-id generation.
async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    let client = Client::builder()
        .bootstrap(bootstrap)
        .client_id("test-admin")
        .build()
        .await
        .expect("admin client");
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "create_topic failed: {resp:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotence_plus_acks_zero_rejects() {
    // No broker IO needed — config validation happens before any network
    // round-trip. We still spin up a broker so the bootstrap address is a
    // real one; otherwise `Client::builder` would also fail and we
    // wouldn't be testing what we intend.
    let (broker, bootstrap, _dir) = boot().await;
    let res = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(true)
        .acks(Acks::Zero)
        .build()
        .await;
    assert!(
        matches!(res, Err(ProducerError::InvalidConfig(_))),
        "expected InvalidConfig, got {res:?}"
    );
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_idempotent_acks_zero_fire_and_forget() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "rp2", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(false)
        .acks(Acks::Zero)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let f = producer
        .send(ProducerRecord {
            topic: "rp2".into(),
            value: Some(Bytes::from_static(b"x")),
            ..Default::default()
        })
        .await;
    producer.flush().await.expect("flush");
    // acks=0: the oneshot may resolve with Ok or be dropped — either is
    // acceptable for a fire-and-forget send. Just make sure we don't hang.
    let _ = tokio::time::timeout(Duration::from_secs(2), f).await;

    producer.close().await.expect("close");
    broker.shutdown().await;
}

const PRODUCE_N: usize = 20;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_produce_then_consume() {
    let (broker, bootstrap, _dir) = boot().await;
    create_topic(&bootstrap, "rp1", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .enable_idempotence(true)
        .acks(Acks::All)
        .linger(Duration::from_millis(5))
        .build()
        .await
        .expect("producer build");

    let mut futs = Vec::with_capacity(PRODUCE_N);
    for i in 0..PRODUCE_N {
        futs.push(
            producer
                .send(ProducerRecord {
                    topic: "rp1".into(),
                    value: Some(Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.expect("flush");

    for (i, f) in futs.into_iter().enumerate() {
        let m = f
            .await
            .expect("oneshot")
            .unwrap_or_else(|e| panic!("record {i} failed: {e:?}"));
        assert!(m.partition == 0, "single-partition topic");
    }

    // Consume them back through a group.
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .client_id("rp1-consumer")
        .group_id("rp1-grp")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["rp1".to_string()])
        .build()
        .await
        .expect("consumer build");

    let mut seen = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while seen < PRODUCE_N && std::time::Instant::now() < deadline {
        seen += consumer
            .poll(Duration::from_millis(500))
            .await
            .expect("poll")
            .len();
    }
    assert!(
        seen == PRODUCE_N,
        "expected {PRODUCE_N} records, saw {seen}"
    );

    consumer.close().await.expect("consumer close");
    producer.close().await.expect("producer close");
    broker.shutdown().await;
}
