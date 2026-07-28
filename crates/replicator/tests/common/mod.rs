//! Shared helpers for crabka-replicator integration tests. Included via `mod common;`
//! in each `tests/*.rs` file. Uses the crate's public `admin_util` plus the real
//! broker/producer/consumer clients.
#![allow(dead_code)]

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Acks, Producer, ProducerRecord};
use crabka_units::prelude::{StdDurationExt as _, Time, TimeExt as _, millis};
use tempfile::TempDir;

/// How long each poll waits for records while draining a topic.
const POLL_TIMEOUT: Time = millis(500);
/// Gap between successive count probes in [`await_count`].
const PROBE_INTERVAL: Time = millis(300);

/// A running in-process broker plus its bootstrap address. Holds the temp dir so
/// the data directory lives as long as the broker.
pub struct TestBroker {
    pub handle: BrokerHandle,
    pub bootstrap: String,
    _dir: TempDir,
}

/// Start a fresh single-node broker on an ephemeral port.
pub async fn start_broker() -> TestBroker {
    let dir = TempDir::new().expect("tempdir");
    let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = handle.listen_addr().to_string();
    TestBroker {
        handle,
        bootstrap,
        _dir: dir,
    }
}

/// Create a (delete-policy) topic.
pub async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    crabka_replicator::admin_util::ensure_topic(bootstrap, name, partitions, None)
        .await
        .expect("ensure_topic");
}

/// Produce a single record and flush.
pub async fn produce(bootstrap: &str, topic: &str, key: &[u8], value: &[u8]) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .enable_idempotence(false)
        .acks(Acks::All)
        .build()
        .await
        .expect("producer");
    producer
        .send(ProducerRecord {
            topic: topic.to_string(),
            partition: None,
            key: Some(bytes::Bytes::copy_from_slice(key)),
            value: Some(bytes::Bytes::copy_from_slice(value)),
            headers: vec![],
            timestamp_ms: None,
        })
        .await
        .await
        .expect("ack recv")
        .expect("produce");
    producer.flush().await.expect("flush");
    producer.close().await.expect("close");
}

/// Count records currently in `topic` (drains it via the public admin helper).
pub async fn count(bootstrap: &str, topic: &str) -> usize {
    crabka_replicator::admin_util::read_all(bootstrap, topic, None)
        .await
        .map_or(0, |v| v.len())
}

/// Poll `count` until it reaches at least `n`, panicking on timeout.
pub async fn await_count(bootstrap: &str, topic: &str, n: usize, timeout: Time) {
    let start = std::time::Instant::now();
    loop {
        if count(bootstrap, topic).await >= n {
            return;
        }
        assert2::assert!(start.elapsed().as_time() <= timeout);
        // real-time wait (not a progress poll): shared helper polling cross-cluster
        // replication via a full consumer-drain (`count`/`read_all`) round-trip; waits on
        // the replicator's copy cadence, not a cheap in-process observable.
        tokio::time::sleep(PROBE_INTERVAL.to_std()).await;
    }
}

/// Consume every record currently in `topic` under `group` and commit — leaving a
/// committed group offset equal to the number of records consumed.
pub async fn consume_and_commit(bootstrap: &str, group: &str, topic: &str) {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .subscribe(vec![topic.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer");
    let mut idle = 0;
    while idle < 3 {
        let recs = consumer.poll(POLL_TIMEOUT.to_std()).await.expect("poll");
        if recs.is_empty() {
            idle += 1;
        } else {
            idle = 0;
        }
    }
    consumer.commit_sync().await.expect("commit");
    consumer.close().await.expect("close");
}
