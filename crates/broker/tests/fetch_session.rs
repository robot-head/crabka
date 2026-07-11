//! KIP-227 incremental fetch session round-trip tests against an
//! in-process broker. Drives the wire protocol directly through the
//! shared `Client` so the exact `session_id` / `session_epoch` paths
//! are exercised end-to-end.

use assert2::check;
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ForgottenTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

const FETCH_SESSION_ID_NOT_FOUND: i16 = 70;
const INVALID_FETCH_SESSION_EPOCH: i16 = 71;

fn one_record_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            ..Default::default()
        });
    }
    b
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(resp.topics[0].error_code == 0);
}

async fn topic_id_for(p: &support::InProcess, name: &str) -> WireUuid {
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn produce(p: &support::InProcess, topic: &str, partition: i32, records: i32) {
    let topic_id = topic_id_for(p, topic).await;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(one_record_batch(records).into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce");
    assert2::assert!(resp.responses[0].partition_responses[0].error_code == 0);
}

fn fetch_partition(partition: i32, offset: i64) -> FetchPartition {
    FetchPartition {
        partition,
        fetch_offset: offset,
        partition_max_bytes: 1_048_576,
        ..Default::default()
    }
}

fn fetch_topic(name: &str, topic_id: WireUuid, partitions: Vec<FetchPartition>) -> FetchTopic {
    FetchTopic {
        topic: name.into(),
        topic_id,
        partitions,
        ..Default::default()
    }
}

/// (1) New session opens, (2) immediate incremental is empty, (3) one
/// produced batch shows up on the next incremental as only-that-partition.
#[tokio::test]
async fn new_session_then_incremental_filters_unchanged_partitions() {
    let p = support::start().await;
    create_topic(&p, "t", 3).await;
    let tid = topic_id_for(&p, "t").await;

    // (1) New session — session_id=0, session_epoch=0.
    let r1 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![fetch_topic(
                "t",
                tid,
                vec![
                    fetch_partition(0, 0),
                    fetch_partition(1, 0),
                    fetch_partition(2, 0),
                ],
            )],
            ..Default::default()
        })
        .await
        .expect("Fetch new-session");
    check!(
        (
            r1.error_code,
            r1.session_id > 0,
            r1.responses.len(),
            r1.responses[0].partitions.len(),
        ) == (0, true, 1, 3),
        "new session response mismatch: {r1:?}"
    );
    let sid = r1.session_id;

    // (2) Immediate incremental: nothing changed → empty response.
    let r2 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id: sid,
            session_epoch: 1,
            topics: vec![],
            forgotten_topics_data: vec![],
            ..Default::default()
        })
        .await
        .expect("Fetch incremental empty");
    check!(
        (r2.error_code, r2.session_id, r2.responses.is_empty()) == (0, sid, true),
        "no partition changed → no topics in response, got {:?}",
        r2.responses
    );

    // (3) Produce one batch to t-0 → next incremental returns only t-0.
    produce(&p, "t", 0, 5).await;
    let r3 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 200,
            min_bytes: 1,
            session_id: sid,
            session_epoch: 2,
            topics: vec![],
            forgotten_topics_data: vec![],
            ..Default::default()
        })
        .await
        .expect("Fetch incremental after produce");
    check!(
        (
            r3.error_code,
            r3.session_id,
            r3.responses.len(),
            r3.responses[0].partitions.len(),
            r3.responses[0].partitions[0].partition_index,
        ) == (0, sid, 1, 1, 0)
    );
    let batches = r3.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records present");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert2::assert!(total == 5);

    p.broker.shutdown().await;
}

/// Forgotten partitions are dropped from the cached subscription and
/// never reappear on subsequent fetches, even after produce.
#[tokio::test]
async fn forgotten_topics_drop_partitions_from_subscription() {
    let p = support::start().await;
    create_topic(&p, "t", 3).await;
    let tid = topic_id_for(&p, "t").await;

    // Open a session covering t-0..t-2.
    let r1 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![fetch_topic(
                "t",
                tid,
                vec![
                    fetch_partition(0, 0),
                    fetch_partition(1, 0),
                    fetch_partition(2, 0),
                ],
            )],
            ..Default::default()
        })
        .await
        .expect("new session");
    let sid = r1.session_id;
    assert2::assert!(sid > 0);

    // Forget t-1.
    let r2 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id: sid,
            session_epoch: 1,
            topics: vec![],
            forgotten_topics_data: vec![ForgottenTopic {
                topic: "t".into(),
                topic_id: tid,
                partitions: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("forget t-1");
    assert2::assert!((r2.error_code, r2.session_id) == (0, sid));

    // Produce to t-1 — should NOT reappear in the next incremental.
    produce(&p, "t", 1, 4).await;
    // Also produce to t-2 — that one SHOULD appear.
    produce(&p, "t", 2, 2).await;

    let r3 = p
        .client
        .send(FetchRequest {
            max_wait_ms: 200,
            min_bytes: 1,
            session_id: sid,
            session_epoch: 2,
            topics: vec![],
            forgotten_topics_data: vec![],
            ..Default::default()
        })
        .await
        .expect("after produce");
    assert2::assert!(r3.error_code == 0);
    let mut seen_partitions: Vec<i32> = r3
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
        .collect();
    seen_partitions.sort_unstable();
    assert2::assert!(seen_partitions == vec![2]);

    p.broker.shutdown().await;
}

/// Wrong `session_id` → `FETCH_SESSION_ID_NOT_FOUND` at the top level,
/// no per-partition rows.
#[tokio::test]
async fn unknown_session_id_returns_not_found() {
    let p = support::start().await;
    let r = p
        .client
        .send(FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id: 999_999,
            session_epoch: 1,
            topics: vec![],
            ..Default::default()
        })
        .await
        .expect("Fetch unknown sid");
    check!(
        (r.error_code, r.session_id, r.responses.is_empty())
            == (FETCH_SESSION_ID_NOT_FOUND, 0, true)
    );
    p.broker.shutdown().await;
}

/// Stale epoch on a valid session → `INVALID_FETCH_SESSION_EPOCH`.
#[tokio::test]
async fn stale_session_epoch_returns_invalid_epoch() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p, "t").await;

    let r1 = p
        .client
        .send(FetchRequest {
            session_id: 0,
            session_epoch: 0,
            topics: vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
            ..Default::default()
        })
        .await
        .expect("new session");
    let sid = r1.session_id;
    assert2::assert!(sid > 0);

    // Broker expects epoch=1; send 99.
    let r2 = p
        .client
        .send(FetchRequest {
            session_id: sid,
            session_epoch: 99,
            topics: vec![],
            ..Default::default()
        })
        .await
        .expect("stale epoch");
    check!(
        (r2.error_code, r2.session_id, r2.responses.is_empty())
            == (INVALID_FETCH_SESSION_EPOCH, 0, true)
    );
    p.broker.shutdown().await;
}

/// Closing a session (epoch=-1) returns a full response and drops the
/// cache entry. A subsequent request with the same id is `NOT_FOUND`.
#[tokio::test]
async fn close_session_drops_cache_entry() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p, "t").await;

    let r1 = p
        .client
        .send(FetchRequest {
            session_id: 0,
            session_epoch: 0,
            topics: vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
            ..Default::default()
        })
        .await
        .expect("new session");
    let sid = r1.session_id;
    assert2::assert!(sid > 0);

    // Close: session_id=sid, session_epoch=-1. Broker serves the request
    // sessionless-style (session_id=0 in response) and removes the entry.
    let r2 = p
        .client
        .send(FetchRequest {
            session_id: sid,
            session_epoch: -1,
            topics: vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
            ..Default::default()
        })
        .await
        .expect("close");
    assert2::assert!((r2.error_code, r2.session_id) == (0, 0));

    // Re-using sid afterwards is NOT_FOUND.
    let r3 = p
        .client
        .send(FetchRequest {
            session_id: sid,
            session_epoch: 1,
            topics: vec![],
            ..Default::default()
        })
        .await
        .expect("after close");
    assert2::assert!(r3.error_code == FETCH_SESSION_ID_NOT_FOUND);
    p.broker.shutdown().await;
}

/// `session_id=0` with a stray epoch (not 0 and not -1) is a wire error.
#[tokio::test]
async fn sessionless_zero_id_with_stray_epoch_is_invalid() {
    let p = support::start().await;
    let r = p
        .client
        .send(FetchRequest {
            session_id: 0,
            session_epoch: 7,
            topics: vec![],
            ..Default::default()
        })
        .await
        .expect("stray");
    assert2::assert!((r.error_code, r.session_id) == (INVALID_FETCH_SESSION_EPOCH, 0));
    p.broker.shutdown().await;
}

/// Sessionless (`session_id=0`, session_epoch=-1) returns a full
/// response with `session_id=0` — the legacy path is unchanged.
#[tokio::test]
async fn sessionless_full_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let tid = topic_id_for(&p, "t").await;
    produce(&p, "t", 0, 2).await;

    let r = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            session_id: 0,
            session_epoch: -1,
            topics: vec![fetch_topic("t", tid, vec![fetch_partition(0, 0)])],
            ..Default::default()
        })
        .await
        .expect("sessionless");
    check!(
        (r.error_code, r.session_id, r.responses.len()) == (0, 0, 1),
        "sessionless response mismatch: {r:?}"
    );
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .expect("v2 records");
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert2::assert!(total == 2);
    p.broker.shutdown().await;
}
