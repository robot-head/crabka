//! Integration test for `Consumer::seek`: a seek issued before the first poll
//! takes effect once the partition is assigned, so the consumer resumes from
//! the sought offset instead of `auto.offset.reset`. Proves the seek wins over
//! the post-assignment prime, drops no pre-seek records, and skips none above.

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_core::Client;
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

async fn produce_n(bootstrap: &str, topic: &str, n: u32) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap();
    for i in 0..n {
        producer
            .send(ProducerRecord {
                topic: topic.into(),
                partition: Some(0),
                key: Some(format!("k{i}").into()),
                value: Some(format!("v{i}").into()),
                headers: vec![],
                timestamp_ms: None,
            })
            .await
            .await
            .unwrap()
            .unwrap();
    }
    producer.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_before_first_poll_resumes_from_sought_offset() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    let ct = admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "s".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::assert!(ct.topics[0].error_code == 0);

    // Offsets 0..=4 on partition 0.
    produce_n(&bootstrap, "s", 5).await;

    // Fresh group with Earliest: without a seek this would read from offset 0.
    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("seek-group")
        .subscribe(vec!["s".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    // Seek to offset 2 *before* the first poll — i.e. before the partition is
    // even guaranteed assigned. The consumer must hold this pending and apply
    // it after assignment, before any fetch.
    consumer.seek("s", 0, 2).await.unwrap();

    // Collect until we have the 3 expected records (offsets 2,3,4).
    let mut got = Vec::new();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while got.len() < 3 && std::time::Instant::now() < deadline {
        let recs = consumer.poll(crabka_units::millis(500)).await.unwrap();
        got.extend(recs);
    }

    let offsets: Vec<i64> = got.iter().map(|r| r.offset).collect();
    // No pre-seek record (offset 0 or 1) is ever delivered, and nothing above
    // the seek is skipped: exactly 2, 3, 4.
    assert2::assert!(offsets == vec![2, 3, 4]);

    consumer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_rejects_negative_offset() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder()
        .bootstrap(&bootstrap)
        .build()
        .await
        .unwrap();
    admin
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "n".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    let consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("neg-group")
        .subscribe(vec!["n".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();

    let err = consumer.seek("n", 0, -1).await;
    assert2::assert!(err.is_err());

    // Offset 0 is a valid seek target (re-read from the beginning): the reject
    // boundary is strictly `offset < 0`, so 0 must be accepted.
    assert2::assert!(consumer.seek("n", 0, 0).await.is_ok());

    consumer.close().await.unwrap();
}
