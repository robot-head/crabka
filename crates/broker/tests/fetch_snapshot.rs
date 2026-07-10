//! `FetchSnapshot` (`api_key` 59, KIP-630) end-to-end. Boots a single
//! in-process broker, creates a topic so the metadata image is non-empty,
//! triggers a controller snapshot, then fetches the `__cluster_metadata`
//! snapshot byte range over the wire and asserts the page is served.

use std::time::{Duration, Instant};

use assert2::check;
use crabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    fetch_snapshot_request::{FetchSnapshotRequest, PartitionSnapshot, SnapshotId, TopicSnapshot},
};

mod support;

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

fn fetch_at(position: i64) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        replica_id: -1,
        max_bytes: 1 << 20,
        topics: vec![TopicSnapshot {
            name: CLUSTER_METADATA_TOPIC.into(),
            partitions: vec![PartitionSnapshot {
                partition: 0,
                current_leader_epoch: 0,
                snapshot_id: SnapshotId::default(),
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        cluster_id: None,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_serves_metadata_snapshot() {
    let env = support::start().await;

    // Make the metadata image non-empty so the snapshot has real content.
    let resp = env
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "snap-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert2::assert!(resp.topics[0].error_code == 0);

    env.broker
        .trigger_snapshot_for_test()
        .await
        .expect("trigger snapshot");

    // The trigger only schedules the snapshot; it completes asynchronously.
    // Poll the FetchSnapshot RPC until the controller has a snapshot.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let out = env.client.send(fetch_at(0)).await.unwrap();
        assert2::assert!(out.error_code == 0);
        let part = &out.topics[0].partitions[0];
        if part.error_code == 0 {
            check!(
                (
                    part.index,
                    part.size > 0,
                    part.unaligned_records.payload_len() > 0
                ) == (0, true, true),
                "served snapshot must report index 0, nonzero size, and payload bytes"
            );
            break;
        }
        assert2::assert!(Instant::now() <= deadline);
        // intentional: snapshot production/installation completes
        // asynchronously in raft and has no metadata-image or metric signal
        // (the image was already non-empty after CreateTopics, so it does not
        // change when the snapshot is written). The only observable is the
        // FetchSnapshot RPC response state, so we poll it under a bounded
        // deadline.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    env.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_rejects_non_metadata_topic() {
    let env = support::start().await;

    let mut req = fetch_at(0);
    req.topics[0].name = "not-metadata".into();
    let out = env.client.send(req).await.unwrap();
    // INVALID_TOPIC_EXCEPTION (17) for any topic other than __cluster_metadata.
    assert2::assert!((out.error_code, out.topics[0].partitions[0].error_code) == (0, 17));

    env.broker.shutdown().await;
}
