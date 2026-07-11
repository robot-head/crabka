//! Crate-internal test helpers.
//!
//! These functions panic on error — they are designed for use in tests only.

use std::time::Duration;

use bytes::Bytes;
use crabka_client_consumer::{AutoOffsetReset, Consumer};
use crabka_client_producer::{Producer, ProducerRecord};

/// Create a topic via [`crate::admin_util::ensure_topic`].
pub async fn create_topic(bootstrap: &str, name: &str, partitions: i32) {
    crate::admin_util::ensure_topic(bootstrap, name, partitions, None)
        .await
        .unwrap_or_else(|e| panic!("create_topic({name}): {e}"));
}

/// Produce a single record (key + value) to `topic` and flush.
pub async fn produce(bootstrap: &str, topic: &str, key: &[u8], value: &[u8]) {
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .build()
        .await
        .unwrap_or_else(|e| panic!("produce: build producer: {e}"));

    let rx = producer
        .send(ProducerRecord {
            topic: topic.to_string(),
            partition: None,
            key: Some(Bytes::copy_from_slice(key)),
            value: Some(Bytes::copy_from_slice(value)),
            headers: Vec::new(),
            timestamp_ms: None,
        })
        .await;

    rx.await
        .expect("produce: sender dropped")
        .unwrap_or_else(|e| panic!("produce({topic}): {e}"));

    producer
        .flush()
        .await
        .unwrap_or_else(|e| panic!("produce: flush: {e}"));
}

/// Return the total number of records in `topic`.
pub async fn topic_record_count(bootstrap: &str, topic: &str) -> usize {
    crate::admin_util::read_all(bootstrap, topic, None)
        .await
        .unwrap_or_else(|e| panic!("topic_record_count({topic}): {e}"))
        .len()
}

/// Poll `topic_record_count` until it reaches `n` or panic on timeout.
pub async fn await_topic_count(bootstrap: &str, topic: &str, n: usize, timeout: Duration) {
    let start = tokio::time::Instant::now();
    loop {
        let count = topic_record_count(bootstrap, topic).await;
        if count >= n {
            return;
        }
        assert2::assert!(start.elapsed() < timeout);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Build a consumer in `group`, subscribe to `topic`, poll until idle,
/// commit offsets, and close. Leaves a committed offset for the group.
#[allow(dead_code)]
pub async fn commit_group(bootstrap: &str, group: &str, topic: &str) {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .client_id("crabka-replicator-test-util")
        .subscribe(vec![topic.to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap_or_else(|e| panic!("commit_group({group}): build consumer: {e}"));

    // Poll until 3 consecutive empty batches (same sentinel as read_all).
    let mut empty_streak = 0usize;
    loop {
        let batch = consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap_or_else(|e| panic!("commit_group({group}): poll: {e}"));
        if batch.is_empty() {
            empty_streak += 1;
            if empty_streak >= 3 {
                break;
            }
        } else {
            empty_streak = 0;
        }
    }

    consumer
        .commit_sync()
        .await
        .unwrap_or_else(|e| panic!("commit_group({group}): commit_sync: {e}"));

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("commit_group({group}): close: {e}"));
}
