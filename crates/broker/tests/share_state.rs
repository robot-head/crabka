#![allow(clippy::pedantic)]

//! End-to-end integration tests for the KIP-932 share coordinator (persister),
//! driven against an in-process Crabka broker via `crabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 83-87; each
//! `*ShareGroupState*Request` impls `ProtocolRequest`, so `client.send(req)`
//! exercises the real wire path (version negotiation through `ApiVersions`).
//!
//! Timing note: the raw persister RPC handlers do NOT create `__share_group_state`
//! — `FindCoordinator(SHARE)` does. After the topic is created the broker
//! materializes + leads its partitions asynchronously (replicator supervisor),
//! so the first `Initialize` may briefly return a coordinator-not-ready code; the
//! `*_ready` helpers retry, exactly as a real client would.

use std::{sync::Arc, time::Duration};

use assert2::check;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::{
    owned::{
        delete_share_group_state_request::{
            DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePart,
        },
        find_coordinator_request::FindCoordinatorRequest,
        initialize_share_group_state_request::{
            InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPart,
        },
        read_share_group_state_request::{
            PartitionData as ReadPart, ReadShareGroupStateRequest, ReadStateData,
        },
        read_share_group_state_summary_request::{
            PartitionData as SummaryPart, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
        },
        write_share_group_state_request::{
            PartitionData as WritePart, StateBatch, WriteShareGroupStateRequest, WriteStateData,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

const KEY_TYPE_SHARE: i8 = 2;
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const FENCED_STATE_EPOCH: i16 = 124;

fn not_ready(code: i16) -> bool {
    code == COORDINATOR_LOAD_IN_PROGRESS
        || code == COORDINATOR_NOT_AVAILABLE
        || code == NOT_COORDINATOR
}

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

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

fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

/// Create `__share_group_state` (lazily, via `FindCoordinator` SHARE) and return
/// the resolved coordinator node id for `key`.
async fn find_share(client: &Client, key: &str) -> (i16, i32) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: KEY_TYPE_SHARE,
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    let c = &resp.coordinators[0];
    (c.error_code, c.node_id)
}

/// Initialize one (group, topic, partition), retrying while the coordinator
/// is still materializing the state partition. Returns the final `error_code`.
async fn initialize_ready(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
) -> i16 {
    for _ in 0..40 {
        let resp = client
            .send(InitializeShareGroupStateRequest {
                group_id: group.into(),
                topics: vec![InitializeStateData {
                    topic_id: wire(tid),
                    partitions: vec![InitPart {
                        partition,
                        state_epoch,
                        start_offset,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("InitializeShareGroupState");
        let code = resp.results[0].partitions[0].error_code;
        if !not_ready(code) {
            return code;
        }
        // intentional: bounded retry of the Initialize RPC while the share
        // coordinator is still asynchronously loading/materializing the
        // __share_group_state partition. This helper holds only a `Client`
        // (no `BrokerHandle`), and the awaited condition is coordinator LOAD
        // state (COORDINATOR_LOAD_IN_PROGRESS) — not share-partition state, so
        // the wait_until_share_* awaiters don't apply (they observe SPSO /
        // delivery / acquired state that this very Initialize creates).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share coordinator never became ready for {group}:{tid}:{partition}");
}

#[allow(clippy::too_many_arguments)]
async fn write_state(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    batches: Vec<StateBatch>,
) -> i16 {
    let resp = client
        .send(WriteShareGroupStateRequest {
            group_id: group.into(),
            topics: vec![WriteStateData {
                topic_id: wire(tid),
                partitions: vec![WritePart {
                    partition,
                    state_epoch,
                    leader_epoch,
                    start_offset,
                    delivery_complete_count,
                    state_batches: batches,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("WriteShareGroupState");
    resp.results[0].partitions[0].error_code
}

async fn read_summary(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> crabka_protocol::owned::read_share_group_state_summary_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateSummaryRequest {
            group_id: group.into(),
            topics: vec![ReadStateSummaryData {
                topic_id: wire(tid),
                partitions: vec![SummaryPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupStateSummary");
    resp.results[0].partitions[0].clone()
}

async fn read_state(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> crabka_protocol::owned::read_share_group_state_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateRequest {
            group_id: group.into(),
            topics: vec![ReadStateData {
                topic_id: wire(tid),
                partitions: vec![ReadPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupState");
    resp.results[0].partitions[0].clone()
}

/// `FindCoordinator(SHARE)` bootstraps `__share_group_state` and routes the
/// `(group, topic, partition)` key to a real broker (this single node).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_share_returns_broker() {
    let (broker, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;

    let tid = uuid::Uuid::from_bytes([3u8; 16]);
    let (error_code, node_id) = find_share(&client, &format!("g1:{tid}:0")).await;

    assert2::assert!(error_code == 0);
    assert2::assert!(node_id == i32::try_from(broker.node_id()).unwrap());
}

/// Initialize -> Write -> Read -> `ReadSummary` -> Delete round-trips over the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persister_round_trip() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    let tid = uuid::Uuid::from_bytes([9u8; 16]);

    // Bootstrap __share_group_state, then initialize (retrying until led).
    let (fc, _) = find_share(&client, &format!("g1:{tid}:0")).await;
    assert2::assert!(fc == 0);
    let init = initialize_ready(&client, "g1", tid, 0, 0, 0).await;
    assert2::assert!(init == 0);

    // Write an in-flight batch above the new SPSO (5).
    let w = write_state(
        &client,
        "g1",
        tid,
        0,
        0, // state_epoch (== stored, not fenced)
        0, // leader_epoch
        5, // start_offset (SPSO advances to 5)
        2, // delivery_complete_count
        vec![StateBatch {
            first_offset: 5,
            last_offset: 9,
            delivery_state: 2,
            delivery_count: 1,
            ..Default::default()
        }],
    )
    .await;
    assert2::assert!(w == 0);

    // Read full state.
    let r = read_state(&client, "g1", tid, 0).await;
    check!(
        (
            r.error_code,
            r.start_offset,
            r.state_batches
                .iter()
                .any(|b| b.first_offset == 5 && b.last_offset == 9)
        ) == (0, 5, true),
        "written batch must be present: {:?}",
        r.state_batches
    );

    // Summary matches.
    let s = read_summary(&client, "g1", tid, 0).await;
    check!(
        (
            s.error_code,
            s.start_offset,
            s.state_epoch,
            s.delivery_complete_count
        ) == (0, 5, 0, 2)
    );

    // Delete, then a fresh read returns the missing/initial sentinel.
    let del = client
        .send(DeleteShareGroupStateRequest {
            group_id: "g1".into(),
            topics: vec![DeleteStateData {
                topic_id: wire(tid),
                partitions: vec![DeletePart {
                    partition: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DeleteShareGroupState");
    assert2::assert!(del.results[0].partitions[0].error_code == 0);

    let after = read_state(&client, "g1", tid, 0).await;
    assert2::assert!(
        (
            after.error_code,
            after.start_offset,
            after.state_batches.is_empty()
        ) == (0, -1, true)
    );
}

/// A write carrying a `state_epoch` below the durable one is fenced (124).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_fences_stale_state_epoch() {
    let (_b, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    let tid = uuid::Uuid::from_bytes([11u8; 16]);

    let (fc, _) = find_share(&client, &format!("g1:{tid}:0")).await;
    assert2::assert!(fc == 0);
    let init = initialize_ready(&client, "g1", tid, 0, 5, 0).await; // state_epoch 5
    assert2::assert!(init == 0);

    // Write at state_epoch 0 (< durable 5) -> fenced.
    let w = write_state(&client, "g1", tid, 0, 0, 0, 0, 0, vec![]).await;
    assert2::assert!(w == FENCED_STATE_EPOCH);
}

/// Persisted share state survives a broker restart (recover replays
/// `__share_group_state`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();
    let tid = uuid::Uuid::from_bytes([13u8; 16]);

    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;

        let (fc, _) = find_share(&client, &format!("g1:{tid}:0")).await;
        assert2::assert!(fc == 0);
        let init = initialize_ready(&client, "g1", tid, 0, 0, 0).await;
        assert2::assert!(init == 0);
        let w = write_state(&client, "g1", tid, 0, 0, 0, 7, 3, vec![]).await;
        assert2::assert!(w == 0);

        broker.wait_until_share_spso("g1", tid, 0, 7).await;
        broker.shutdown().await;
    }

    {
        let mut cfg = BrokerConfig::for_tests(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;

        // Recovered coordinator may still be materializing the led partition;
        // await until the persisted SPSO is visible, then assert the wire value.
        broker.wait_until_share_spso("g1", tid, 0, 7).await;
        let start_offset = read_summary(&client, "g1", tid, 0).await.start_offset;
        assert2::assert!(start_offset == 7);
    }
}
