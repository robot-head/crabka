//! End-to-end integration tests for KIP-932 Slice C: share-partition consume
//! (`ShareFetch`, `api_key` 78) + acknowledge (`ShareAcknowledge`, `api_key` 79),
//! driven against an in-process Crabka broker via `crabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 78/79; both
//! `ShareFetchRequest` / `ShareAcknowledgeRequest` impl `ProtocolRequest`, so
//! `client.send(req)` returns the typed response and exercises the real wire
//! path (version negotiation through `ApiVersions` — both RPCs are MIN=1 MAX=2,
//! so the client negotiates v2).
//!
//! These tests prove the full acquire/ack loop:
//! - acquire under a lock and read the verbatim record bytes;
//! - Accept advances the SPSO (and the advance survives a broker restart, i.e.
//!   it was persisted to the share coordinator);
//! - Release re-delivers with an incremented `delivery_count`;
//! - Reject archives and advances the SPSO past the poison record;
//! - an unacknowledged lock that expires is re-delivered by the background
//!   lock-timeout sweep;
//! - a record that exhausts `max_delivery_attempts` is archived (poison pill);
//! - the share-session epoch state machine rejects stale / unknown epochs.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use assert2::{assert, check};
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{
            AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::ShareFetchResponse,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

const NONE: i16 = 0;
const INVALID_SHARE_SESSION_EPOCH: i16 = 123;
const SHARE_SESSION_NOT_FOUND: i16 = 122;

// Ack types (KIP-932): one i8 per offset.
const ACCEPT: i8 = 1;
const RELEASE: i8 = 2;
const REJECT: i8 = 3;

const ONE_MB: i32 = 1 << 20;

// ────────────────────────────────────────────────────────────────────────
// Harness (mirrors tests/share_groups.rs + tests/share_state.rs).
// ────────────────────────────────────────────────────────────────────────

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

/// Create `topic` with `partitions` partitions and wait until this broker has
/// materialized (and leads) partition 0, so a subsequent produce won't race the
/// replicator supervisor.
async fn create_topic(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    partitions: i32,
) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
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
        "topic create failed: {resp:?}"
    );
    broker.wait_until_partition_present(topic, 0).await;
}

/// Resolve a created topic's id from this broker's metadata image.
fn topic_id(broker: &crabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

/// Bootstrap `__share_group_state` (created lazily by `FindCoordinator(SHARE)`,
/// exactly as a KIP-932 client does) and wait until this broker has materialized
/// the state partition that owns `key`. Until that partition is led locally, the
/// share-partition manager's persist would route to a not-yet-present leader and
/// the SPSO advance would only live in memory — so a restart would lose it. This
/// is the share-state analogue of waiting for the data partition.
const SHARE_STATE_TOPIC: &str = "__share_group_state";
// These single-broker tests only need one state partition. Keeping the test
// geometry small also prevents the parallel test runner from exhausting its
// process-wide file-descriptor limit while eleven brokers run concurrently.
const SHARE_STATE_PARTITIONS: i32 = 1;
const MAX_CONCURRENT_TEST_BROKERS: usize = 3;

async fn broker_test_permit() -> tokio::sync::OwnedSemaphorePermit {
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

    Arc::clone(
        GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TEST_BROKERS))),
    )
    .acquire_owned()
    .await
    .expect("broker test concurrency gate remains open")
}

fn broker_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.share_coordinator.state_topic_num_partitions = SHARE_STATE_PARTITIONS;
    config
}

async fn bootstrap_share_state(broker: &crabka_broker::BrokerHandle, client: &Client, key: &str) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2, // SHARE
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE) error: {}",
        resp.coordinators[0].error_code
    );
    // Wait until every state partition this single broker should lead is local,
    // so the share-state writes land durably.
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Wait until the group-coordinator lifecycle hook has durably initialized the
/// share state for `(group, topic, partition)` (the persister summary becomes
/// present). Until this lands the share coordinator is not yet write-ready and a
/// consume's SPSO advance would not persist.
///
/// The lifecycle hook fires on each heartbeat, so this drives steady-state
/// heartbeats inside the wait loop (mirroring `share_groups.rs`'s
/// `lifecycle_initializes_share_state` pattern) rather than sleeping.
async fn wait_for_share_init(
    broker: &crabka_broker::BrokerHandle,
    client: &Client,
    member_id: &str,
    member_epoch: i32,
    tid: uuid::Uuid,
) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // Send a steady-state heartbeat to trigger the lifecycle hook.
            let _ = client
                .send(ShareGroupHeartbeatRequest {
                    group_id: "g1".into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                })
                .await;
            if broker
                .share_state_summary_for_test("g1", tid, 0)
                .await
                .is_some()
            {
                return;
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "share state for g1:{tid}:0 never initialized within 30s"
    );
}

/// Produce `n` records into `(topic, partition)` in a single batch. Each record
/// carries a tiny distinct value so the bytes are non-empty.
///
/// Retries while the freshly-created partition is still materializing its leader
/// (`UNKNOWN_TOPIC_OR_PARTITION` / `NOT_LEADER_OR_FOLLOWER`), exactly as a real
/// producer would.
async fn produce_n(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, n: i64) {
    for _ in 0..40 {
        let records: Vec<Record> = (0..n)
            .map(|i| Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(bytes::Bytes::copy_from_slice(format!("v{i}").as_bytes())),
                ..Default::default()
            })
            .collect();
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    // Produce negotiates v13, which carries topic_id (not name)
                    // on the wire; the broker resolves the topic by id.
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: i32::try_from(n - 1).unwrap(),
                                records,
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER.
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-retry backoff while the partition
            // leader materializes; this helper has no BrokerHandle to await on
            // and mirrors a real producer's retry.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// Join `group` as a fresh member subscribed to `topic` so the share actor
/// knows the member (the `ShareFetch` membership check needs this). Returns
/// `(member_id, member_epoch)` so the caller can drive heartbeats inside the
/// `wait_for_share_init` lifecycle loop.
async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    let member_id = resp.member_id.expect("broker must mint a member id");
    let member_epoch = resp.member_epoch;
    (member_id, member_epoch)
}

/// Build a `ShareFetchRequest` for a single `(topic_id, partition)` at the given
/// share-session epoch, optionally piggybacking acknowledgement batches.
fn share_fetch_req(
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
    acks: Vec<FetchAckBatch>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: ONE_MB,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition,
                partition_max_bytes: ONE_MB,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}

/// `ShareFetch`, retrying while the share-state leadership / acquisition is still
/// settling. The first acquire pass after topic creation can briefly find the
/// `__share_group_state` partition still materializing; mirror `share_state.rs`'s
/// retry-on-not-ready loop. Returns the (single) partition row.
async fn share_fetch(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
) -> crabka_protocol::owned::share_fetch_response::PartitionData {
    let req = share_fetch_req(group, member, tid, partition, epoch, max_wait_ms, vec![]);
    let resp: ShareFetchResponse = client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == NONE,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// A `ShareAcknowledge` carrying one batch of per-offset ack types over
/// `[first, last]`. Returns the partition row.
async fn share_ack(
    client: &Client,
    member: &str,
    tid: uuid::Uuid,
    epoch: i32,
    first: i64,
    last: i64,
    ack_type: i8,
) -> crabka_protocol::owned::share_acknowledge_response::PartitionData {
    let count = usize::try_from(last - first + 1).unwrap();
    let req = ShareAcknowledgeRequest {
        group_id: Some("g1".into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        is_renew_ack: false,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(tid),
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: first,
                    last_offset: last,
                    acknowledge_types: vec![ack_type; count],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// A renew-ack `ShareAcknowledge` (`is_renew_ack = true`) over `[first, last]`
/// with *empty* ack types — the broker renew path extends each batch's lock
/// without changing record state. Returns the partition row.
async fn share_renew(
    client: &Client,
    member: &str,
    tid: uuid::Uuid,
    epoch: i32,
    first: i64,
    last: i64,
) -> crabka_protocol::owned::share_acknowledge_response::PartitionData {
    let req = ShareAcknowledgeRequest {
        group_id: Some("g1".into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        is_renew_ack: true,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(tid),
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: first,
                    last_offset: last,
                    acknowledge_types: vec![],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge renew");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge(renew) top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// Total number of offsets covered by the acquired ranges on a fetch row.
fn acquired_count(p: &crabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
}

/// Perform the very first `ShareFetch` for a freshly-created topic, retrying
/// until the acquire pass actually returns records (leadership/materialization
/// of both the data partition and `__share_group_state` may still be settling).
/// Asserts the supplied invariant on the resulting row.
async fn fetch_until_acquired(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
) -> crabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = share_fetch(client, group, member, tid, partition, epoch, 0).await;
        if row.error_code == NONE && acquired_count(&row) > 0 {
            return row;
        }
        // intentional: bounded RPC poll — the acquire happens only via this
        // ShareFetch as share-state leadership/acquisition settles; no
        // metadata-image or metric signal reflects "the next fetch will acquire".
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records for {group}:{tid}:{partition}");
}

// ────────────────────────────────────────────────────────────────────────
// Tests.
// ────────────────────────────────────────────────────────────────────────

/// Acquire 3 records, Accept them all, observe the SPSO advance, then prove the
/// advance was persisted by restarting the broker on the same data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consume_accept_restart() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(broker_config(log_dir.clone())).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
        produce_n(&client, "t", tid, 0, 3).await;
        let (member, member_epoch) = join(&client, "g1", "t").await;
        // The group lifecycle initializes share state asynchronously; wait until
        // it is durable so the SPSO advance from the Accept below also persists.
        wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

        // First fetch (epoch 0 opens the session): acquire offsets 0..2.
        let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
        check!(
            acquired_count(&row) == 3,
            "must acquire all 3 offsets, got {:?}",
            row.acquired_records
        );
        check!(
            row.acquired_records.iter().all(|r| r.delivery_count == 1),
            "first delivery_count must be 1, got {:?}",
            row.acquired_records
        );
        check!(
            row.records.is_some(),
            "acquired records must carry record bytes"
        );

        // Accept offsets 0..2 (session epoch is now 1 after the open).
        let ack = share_ack(&client, &member, tid, 1, 0, 2, ACCEPT).await;
        assert!(
            ack.error_code == NONE,
            "accept ack error: {}",
            ack.error_code
        );

        // Next fetch (epoch 2): the SPSO advanced past 2 — nothing left.
        let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
        assert!(
            acquired_count(&row2) == 0,
            "SPSO must have advanced; no records re-acquired, got {:?}",
            row2.acquired_records
        );

        // Wait until the persister has landed the advanced SPSO (>= 3, past
        // offset 2) in __share_group_state before shutting down, so the
        // restart below sees the durable SPSO.
        broker.wait_until_share_spso("g1", tid, 0, 3).await;
        broker.shutdown().await;
    }

    {
        let mut cfg = broker_config(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

        // A fresh member rejoins the recovered group; a fresh-session fetch
        // must observe the recovered SPSO (past offset 2) — zero acquired.
        // Wait until the share state is recovered on the new broker, then
        // assert in a single fetch (no timing guess needed).
        let (member, _) = join(&client, "g1", "t").await;
        broker.wait_for_share_state_summary("g1", tid, 0).await;
        let row = share_fetch(&client, "g1", &member, tid, 0, 0, 0).await;
        let acquired = acquired_count(&row);
        assert!(
            acquired == 0,
            "recovered SPSO must skip the accepted records; re-acquired {acquired}"
        );
    }
}

/// Release re-delivers the same offsets with an incremented `delivery_count`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_redelivers() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 2).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 2, "acquire both offsets");
    assert!(row.acquired_records.iter().all(|r| r.delivery_count == 1));

    // Release offsets 0..1 (epoch 1).
    let ack = share_ack(&client, &member, tid, 1, 0, 1, RELEASE).await;
    assert!(ack.error_code == NONE, "release error: {}", ack.error_code);

    // Next fetch (epoch 2): the same offsets are re-acquired at delivery_count 2.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 2,
        "released offsets must be re-acquired, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records.iter().all(|r| r.delivery_count == 2),
        "redelivery must bump delivery_count to 2, got {:?}",
        row2.acquired_records
    );
}

/// Reject archives the records: they are never re-delivered AND the SPSO
/// advances past them (a freshly produced offset is the only thing acquired).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reject_archives() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 2).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 2, "acquire both offsets");

    // Reject offsets 0..1 (epoch 1) → archived.
    let ack = share_ack(&client, &member, tid, 1, 0, 1, REJECT).await;
    assert!(ack.error_code == NONE, "reject error: {}", ack.error_code);

    // Next fetch (epoch 2): nothing re-acquired.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 0,
        "rejected offsets must not be re-acquired, got {:?}",
        row2.acquired_records
    );

    // Produce one more (offset 2). The SPSO advanced past the rejected pair, so
    // only the new offset is acquired — proving the rejected ones were skipped.
    produce_n(&client, "t", tid, 0, 1).await;
    let mut row3 = share_fetch(&client, "g1", &member, tid, 0, 3, 0).await;
    for epoch in 4..18 {
        if acquired_count(&row3) > 0 {
            break;
        }
        // intentional: bounded RPC poll — acquiring the freshly produced offset
        // 2 requires re-fetching; no image/metric signals when it becomes
        // acquirable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row3 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    assert!(
        acquired_count(&row3) == 1,
        "only the new offset must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 2 && row3.acquired_records[0].last_offset == 2,
        "acquired offset must be 2 (past the rejected 0..1), got {:?}",
        row3.acquired_records
    );
}

/// Regression: a `ShareFetch` whose acquired offset begins a *later* record
/// batch (a leading multi-record batch was already consumed/archived) must
/// still return that offset's record bytes — not an empty payload.
///
/// `ShareFetch.partition_max_bytes` is a v0-only field; at the supported
/// versions (v1+) it is absent and decodes to 0. The read path must not use
/// that 0 as the log-read byte budget: a 0 budget reads only one batch header,
/// which cannot skip the leading batch to reach the acquired offset, so the
/// acquired record is returned with no bytes (and stays locked). The read must
/// fall back to the request-level `max_bytes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_past_leading_batch_returns_bytes() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    // One 3-record batch at offsets 0..2.
    produce_n(&client, "t", tid, 0, 3).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire 0..2 and Reject them → archived, SPSO advances to 3.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "acquire all 3");
    let ack = share_ack(&client, &member, tid, 1, 0, 2, REJECT).await;
    assert!(ack.error_code == NONE, "reject error: {}", ack.error_code);

    // A separate single-record batch at offset 3 (this starts a new batch; the
    // acquired range 3..3 begins past the leading 0..2 batch).
    produce_n(&client, "t", tid, 0, 1).await;

    // Acquire offset 3 — the payload must carry the record bytes.
    let mut row3 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    for epoch in 3..18 {
        if acquired_count(&row3) > 0 {
            break;
        }
        // intentional: bounded RPC poll — acquiring the freshly produced offset
        // 3 requires re-fetching; no image/metric signals when it becomes
        // acquirable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row3 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    assert!(
        acquired_count(&row3) == 1,
        "offset 3 must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 3,
        "acquired offset must be 3, got {:?}",
        row3.acquired_records
    );
    let batches = row3
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("acquired offset 3 must carry decodable v2 record bytes");
    let values: Vec<String> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|r| r.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .collect();
    assert!(
        values == vec!["v0"],
        "offset 3's record bytes must be returned, got {values:?}"
    );
}

/// An acquired-but-unacknowledged lock that expires is reverted by the
/// background sweep, so the next fetch re-delivers at an incremented count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lock_timeout_redelivers() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(200);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Fetch but DO NOT acknowledge.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Wait until the lock expires and the background sweeper reverts the
    // record to Available (acquired-batch count drops to 0).
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Next fetch (epoch 1) re-acquires the same offset at delivery_count 2.
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1,
        "expired-lock offset must be re-acquired, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records[0].delivery_count == 2,
        "re-delivery after lock timeout must bump delivery_count to 2, got {}",
        row2.acquired_records[0].delivery_count
    );
}

/// A record that exhausts `max_delivery_attempts` without an Accept is archived
/// (poison pill): subsequent fetches acquire nothing and the SPSO advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delivery_limit_archives() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(150);
    cfg.share_group.max_delivery_attempts = 2;
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Delivery 1 (no ack).
    let row1 = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(row1.acquired_records[0].delivery_count == 1);

    // Wait until the lock expires and the sweeper reverts the record to Available
    // (acquired-batch count drops to 0), then re-fetch for delivery 2.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Delivery 2 (no ack).
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1 && row2.acquired_records[0].delivery_count == 2,
        "second delivery must be count 2, got {:?}",
        row2.acquired_records
    );

    // Wait until that lock expires too — the sweeper reverts the record back to
    // Available (delivery_count=2, which equals max_delivery_attempts). The
    // archiving (dcc increment) happens during the next acquire call when the
    // broker detects the poison pill.
    broker
        .wait_until_share_acquired_count("g1", tid, 0, 0)
        .await;

    // Subsequent fetch: the acquire path detects delivery_count >= max_attempts
    // and archives the record — SPSO advances, nothing is returned.
    let row3 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row3) == 0,
        "poison record must be archived, not re-delivered, got {:?}",
        row3.acquired_records
    );
}

/// The share-session epoch state machine rejects stale and unknown epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_epoch_validation() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Open (epoch 0) succeeds: top-level error_code 0.
    let opened: ShareFetchResponse = client
        .send(share_fetch_req("g1", &member, tid, 0, 0, 0, vec![]))
        .await
        .expect("ShareFetch open");
    assert!(
        opened.error_code == NONE,
        "open (epoch 0) must succeed, got {}",
        opened.error_code
    );

    // The stored epoch is now 1. A non-matching positive epoch (say 9) →
    // INVALID_SHARE_SESSION_EPOCH (123) at the top level.
    let stale: ShareFetchResponse = client
        .send(share_fetch_req("g1", &member, tid, 0, 9, 0, vec![]))
        .await
        .expect("ShareFetch stale");
    assert!(
        stale.error_code == INVALID_SHARE_SESSION_EPOCH,
        "stale epoch must be 123 (INVALID_SHARE_SESSION_EPOCH), got {}",
        stale.error_code
    );

    // A member with no live session sending a non-zero epoch →
    // SHARE_SESSION_NOT_FOUND (122).
    let (ghost, _) = join(&client, "g1", "t").await;
    let not_found: ShareFetchResponse = client
        .send(share_fetch_req("g1", &ghost, tid, 0, 5, 0, vec![]))
        .await
        .expect("ShareFetch unknown session");
    assert!(
        not_found.error_code == SHARE_SESSION_NOT_FOUND,
        "unknown session must be 122 (SHARE_SESSION_NOT_FOUND), got {}",
        not_found.error_code
    );
}

// ────────────────────────────────────────────────────────────────────────
// Slice F tests.
// ────────────────────────────────────────────────────────────────────────

/// F1 (renew): a renew-ack extends the acquisition lock, so a record that would
/// otherwise be re-delivered after its lock expires is NOT re-acquired.
///
/// Config: `record_lock_duration = 500ms` (sweeper ticks at 250ms). Acquire
/// offset 0 (lock 500ms), send a renew-ack ~200ms in — which resets the
/// deadline to renew-time + 500ms (≈ T0+700ms) — then check at ~T0+600ms, which
/// is PAST the original 500ms deadline (so an un-renewed lock would already
/// have been swept and re-delivered) but BEFORE the renewed 700ms deadline. The
/// renew kept the record Acquired, so the fetch acquires nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renew_extends_lock_not_redelivered() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(500);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire offset 0 (lock 500ms, delivery_count 1). Epoch is now 1.
    let acquire_at = std::time::Instant::now();
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Intentional calibrated timing: renew ~200ms in (before the 500ms lock
    // expires) to reset the deadline to renew-time + 500ms ≈ T0+700ms. Epoch
    // is now 2. This sleep proves renew timing; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let renew = share_renew(&client, &member, tid, 1, 0, 0).await;
    assert!(
        renew.error_code == NONE,
        "renew must succeed for an acquired offset, got {}",
        renew.error_code
    );

    // Intentional calibrated timing: wait until ~600ms after the ORIGINAL
    // acquire — past the original 500ms deadline (un-renewed lock would have
    // been swept) but before the renewed ~700ms deadline. The remaining sleep
    // is computed from the real acquire instant so scheduling jitter doesn't
    // overshoot the renewed window. This proves the renew suppressed redelivery;
    // it is NOT a flaky state-guess.
    let target = acquire_at + Duration::from_millis(600);
    if let Some(rem) = target.checked_duration_since(std::time::Instant::now()) {
        tokio::time::sleep(rem).await;
    }
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    assert!(
        acquired_count(&row2) == 0,
        "renew must keep the lock; offset 0 must NOT be re-acquired, got {:?}",
        row2.acquired_records
    );
}

/// F1 (control): the SAME timing WITHOUT a renew re-acquires the offset after
/// the lock expires, at `delivery_count` 2 — proving the renew above is what
/// suppressed the redelivery (not slack in the timing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_renew_redelivers_after_lock_expiry() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.record_lock_duration = Duration::from_millis(500);
    let broker = Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    produce_n(&client, "t", tid, 0, 1).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 1, "acquire the single offset");
    assert!(row.acquired_records[0].delivery_count == 1);

    // Intentional calibrated timing: no renew — wait 800ms (well past the 500ms
    // lock + a sweeper tick) so the record is reverted to Available and
    // re-delivered. This sleep mirrors the renew test's timing to prove that
    // WITHOUT a renew the lock IS swept; it is NOT a flaky state-guess.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let row2 = share_fetch(&client, "g1", &member, tid, 0, 1, 0).await;
    assert!(
        acquired_count(&row2) == 1,
        "without renew the expired lock must re-acquire, got {:?}",
        row2.acquired_records
    );
    assert!(
        row2.acquired_records[0].delivery_count == 2,
        "re-delivery after lock timeout must bump delivery_count to 2, got {}",
        row2.acquired_records[0].delivery_count
    );
}

/// F2 (`read_committed`): with `isolation_level = ReadCommitted`, a share fetch
/// never surfaces records from an OPEN transaction (offsets past the LSO).
///
/// A transactional producer begins a txn and sends 3 records but does NOT
/// commit — so the partition's HWM is 3 while the LSO stays at 0. A
/// `read_committed` share fetch clamps its read window to `min(LSO, HWM) = 0`, so
/// it acquires nothing. After the txn commits the LSO advances to 3 and the
/// same group then acquires all 3 — proving the clamp tracked the LSO and the
/// records were merely deferred, not lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_skips_open_txn_then_sees_committed() {
    use crabka_broker::coordinator::unified::share::config::ShareIsolationLevel;
    use crabka_client_producer::{Producer, ProducerRecord};

    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = broker_config(dir.path().to_path_buf());
    cfg.share_group.isolation_level = ShareIsolationLevel::ReadCommitted;
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;

    // Open a transaction and send 3 records WITHOUT committing: HWM=3, LSO=0.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("share-rc-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(bytes::Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    // Flush the records to the log (advances HWM) but keep the txn OPEN (LSO=0).
    producer.flush().await.unwrap();

    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // A read_committed share fetch must acquire NOTHING: every record is past
    // the LSO (still 0). Poll a few times to be sure it never spuriously acquires.
    for epoch in 0..6 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        assert!(
            acquired_count(&row) == 0,
            "read_committed must not surface open-txn records, got {:?}",
            row.acquired_records
        );
        // intentional: deliberately observe that nothing is acquired across a
        // window while the txn stays open (behavior under test, not a
        // state-settle guess).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Commit the transaction → the LSO advances past the records (a commit
    // control marker is appended, so HWM == LSO). The same group now acquires
    // the committed records (proving they were deferred, not dropped). The
    // acquired window also covers the control-marker offset, whose bytes the
    // read path filters out — so we assert on the surfaced record VALUES.
    txn.commit().await.unwrap();
    let mut values: Vec<String> = Vec::new();
    for epoch in 6..30 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        if acquired_count(&row) > 0
            && let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2())
        {
            values = batches
                .iter()
                .flat_map(|b| b.records.iter())
                .filter_map(|r| r.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect();
            values.sort();
            if values == vec!["a", "b", "c"] {
                break;
            }
        }
        // intentional: bounded RPC poll for the post-commit LSO advance
        // (transaction-coordinator state, not in the metadata image) to surface
        // via ShareFetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        values == vec!["a", "b", "c"],
        "after commit the read_committed fetch must surface the 3 committed \
         records, got {values:?}"
    );

    producer.close().await.unwrap();
}

/// Produce a single record carrying `value` into `(topic, partition)` as its OWN
/// batch (so each offset is a distinct on-disk batch). This matters for the
/// fragmented-window read test: the share-fetch read path reads verbatim bytes
/// at *batch* granularity, so to surface byte-exact disjoint offsets each offset
/// must be its own batch. Retries while the partition is still materializing.
async fn produce_one(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, value: &str) {
    for _ in 0..40 {
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: 0,
                                records: vec![Record {
                                    offset_delta: 0,
                                    value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-retry backoff while the partition
            // leader materializes; this helper has no BrokerHandle to await on.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// F5 (fragmented window): a single share fetch that returns DISJOINT acquired
/// ranges must carry record bytes for exactly the acquired offsets — the gap
/// offset's value must not appear.
///
/// Scenario: produce 3 records as THREE separate single-record batches (so
/// offsets 0, 1, 2 are each their own on-disk batch — the share-fetch read is
/// batch-granular, so byte-exact disjoint reads require separate batches).
/// Acquire 0..2, then Accept the MIDDLE offset (1) only and Release the outer
/// offsets 0 and 2. The SPSO stays at 0 (offset 0 isn't accepted); offset 1 is
/// acknowledged and offsets 0, 2 return to Available — leaving a gap at offset
/// 1. The re-fetch acquires the DISJOINT set {0, 2}; the read concatenates the
/// per-range bytes, so the payload decodes to exactly offsets {0, 2} (values
/// v0, v2) — never the gap offset 1's value v1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_window_records_match_acquired_offsets() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    // Three separate single-record batches: offset 0=v0, 1=v1, 2=v2.
    produce_one(&client, "t", tid, 0, "v0").await;
    produce_one(&client, "t", tid, 0, "v1").await;
    produce_one(&client, "t", tid, 0, "v2").await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire 0..2 (epoch 0 opens; stored epoch is now 1).
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "acquire all 3 offsets");

    // Accept the MIDDLE offset (1) only; Release the outer offsets 0 and 2.
    // SPSO stays at 0 (offset 0 is not accepted), offset 1 becomes Acknowledged,
    // offsets 0 and 2 return to Available — a gap at offset 1 between them.
    let a1 = share_ack(&client, &member, tid, 1, 1, 1, ACCEPT).await;
    assert!(a1.error_code == NONE, "accept 1 error: {}", a1.error_code);
    let a0 = share_ack(&client, &member, tid, 2, 0, 0, RELEASE).await;
    assert!(a0.error_code == NONE, "release 0 error: {}", a0.error_code);
    let a2 = share_ack(&client, &member, tid, 3, 2, 2, RELEASE).await;
    assert!(a2.error_code == NONE, "release 2 error: {}", a2.error_code);

    // Re-fetch: the acquired set is the DISJOINT {0, 2} (offset 1 is gone). The
    // returned records payload must decode to exactly offsets {0, 2}.
    let mut row2 = share_fetch(&client, "g1", &member, tid, 0, 4, 0).await;
    for epoch in 5..20 {
        if acquired_count(&row2) >= 2 {
            break;
        }
        // intentional: bounded RPC poll — re-acquiring the released disjoint set
        // {0, 2} happens only via this ShareFetch; no image/metric reflects it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row2 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    // The authoritative acquired offset set.
    let acquired_offsets: std::collections::BTreeSet<i64> = row2
        .acquired_records
        .iter()
        .flat_map(|r| r.first_offset..=r.last_offset)
        .collect();
    assert!(
        acquired_offsets == std::collections::BTreeSet::from([0, 2]),
        "must re-acquire the disjoint set {{0, 2}}, got {acquired_offsets:?}"
    );

    // Decode the records payload and collect the absolute offsets it carries.
    let batches = row2
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("disjoint acquired ranges must carry decodable v2 record bytes");
    let record_offsets: std::collections::BTreeSet<i64> = batches
        .iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records
                .iter()
                .map(move |r| base + i64::from(r.offset_delta))
        })
        .collect();
    assert!(
        record_offsets == acquired_offsets,
        "records payload offsets {record_offsets:?} must equal acquired offsets \
         {acquired_offsets:?} (gap offset 1 must be excluded)"
    );
    // Belt-and-suspenders: the gap offset's value (v1) must never appear.
    let values: Vec<String> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|r| r.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .collect();
    assert!(
        !values.contains(&"v1".to_string()),
        "the gap offset's value v1 must be excluded, got {values:?}"
    );
}
