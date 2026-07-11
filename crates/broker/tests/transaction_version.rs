//! KIP-890 per-level `transaction.version` integration tests.
//!
//! An in-process test broker self-bootstraps `transaction.version=2` (`TV_2`),
//! so the existing `transactions.rs` suite already covers the `TV_2` happy path.
//! These tests prove the *other* two levels end-to-end, the `TV_2`
//! verify-only `AddPartitionsToTxn` path, and that persisted txn state
//! survives a broker restart (the startup DECODE/recover-from-disk path):
//!
//! 1. **`TV_1`** — downgrade `transaction.version` to 1 (flexible, v1
//!    `TransactionLogValue` records, no epoch bump), then run a full
//!    transactional produce → commit → `read_committed` consume. Success
//!    proves the coordinator persists `__transaction_state` via the v1
//!    *encode* path at the resolved level and the transaction commits/reads
//!    end-to-end.
//! 2. **`TV_0`** — downgrade to 0 (tombstone → Classic, non-flexible v0
//!    records), then the same full cycle. Proves the v0 encode path + cycle.
//! 3. **verify-only `AddPartitionsToTxn`** at `TV_2` — confirm per-partition
//!    `NONE (0)` for an already-added partition and
//!    `TRANSACTION_ABORTABLE (120)` for one that was never added.
//! 4. **restart recovery** (v0 + v1) — persist an `Ongoing` entry, restart the
//!    broker on the same data dir, and prove `TxnCoordinator::recover` decodes
//!    the `__transaction_state` record from disk by committing the recovered
//!    txn via `EndTxn`. This is the only path that exercises the startup
//!    decode/recover code that the live-broker tests above cannot reach.
//!
//! Windows-gated like `transactions.rs`: openraft + tokio scheduling on the
//! hosted Windows runner causes intermittent `INVALID_TXN_STATE` during
//! `InitProducerId`.

use std::time::Duration;

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle};
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_core::Client;
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    end_txn_request::EndTxnRequest,
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};
use tempfile::TempDir;

// Kafka error codes asserted below.
const NONE: i16 = 0;
const TRANSACTION_ABORTABLE: i16 = 120;
const VERIFY_TID: &str = "verify-tid";

// ── shared helpers (mirrors transactions.rs) ───────────────────────────────────

async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn admin_client(bootstrap: &str) -> Client {
    Client::builder()
        .bootstrap(bootstrap)
        .client_id("crabka-txnv-test")
        .build()
        .await
        .unwrap()
}

async fn await_transaction_coordinator(client: &Client) -> (i64, i16) {
    let coordinator = client
        .send(FindCoordinatorRequest {
            key: VERIFY_TID.into(),
            key_type: 1,
            coordinator_keys: vec![VERIFY_TID.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|row| row.error_code == 0),
        "FindCoordinator: {coordinator:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(VERIFY_TID.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        if response.error_code == 0 {
            return (response.producer_id, response.producer_epoch);
        }
        assert!(
            response.error_code == 15 || response.error_code == 16,
            "InitProducerId: {response:?}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "transaction coordinator did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let cr = client
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
        .unwrap();
    assert!(
        cr.topics[0].error_code == 0 || cr.topics[0].error_code == 36,
        "create_topic {name}: error_code={}",
        cr.topics[0].error_code
    );
}

/// Downgrade the finalized `transaction.version` to `level` via a
/// `SAFE_DOWNGRADE` (`upgrade_type = 2`) `UpdateFeatures` request. Level 1
/// finalizes the Flexible level; level 0 tombstones the feature (→ absent →
/// Classic). `resolve_txn_version` reads the live image per request, so a new
/// transaction started after this returns picks up the downgraded level.
async fn downgrade_transaction_version(client: &Client, level: i16) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "transaction.version".into(),
                max_version_level: level,
                upgrade_type: 2, // SAFE_DOWNGRADE
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(resp.error_code == 0, "UpdateFeatures top-level: {resp:?}");
    if let Some(row) = resp
        .results
        .iter()
        .find(|r| r.feature == "transaction.version")
    {
        assert!(
            row.error_code == 0,
            "transaction.version downgrade to {level} rejected: {resp:?}"
        );
    }
}

fn rec(topic: &str, v: &str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from(v.to_string())),
        ..Default::default()
    }
}

/// Run a full transactional cycle at whatever `transaction.version` the
/// cluster is currently finalized at: init → begin → send 3 → commit, then a
/// fresh `read_committed` consumer must observe exactly `["a","b","c"]`.
///
/// The commit forces the coordinator to write `TransactionLogValue` records to
/// `__transaction_state` at the resolved level (v0 for `TV_0`, v1 for `TV_1/TV_2`)
/// across its state transitions, so a successful produce→commit→read cycle
/// proves the level's *encode* path runs and the transaction commits/reads
/// end-to-end. (In-memory state drives the transitions within one broker
/// lifetime; decode/recover from disk is unit-tested in `txn::log_record`.)
async fn full_cycle_commit_and_read(bootstrap: &str, topic: &str, tid: &str, group: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_string())
        .transactional_id(tid)
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(producer.send(rec(topic, v)).await);
    }
    txn.commit().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_string())
        .group_id(group)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([topic.to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && std::time::Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert!(
        seen == vec!["a", "b", "c"],
        "tid={tid} level cycle: {seen:?}"
    );

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}

// ── test 1: TV_1 full cycle (flexible v1 records) ──────────────────────────────

/// Downgrade `transaction.version` to 1, then run a full transactional
/// produce → commit → `read_committed` consume. The commit writes v1
/// (flexible, `header 00 01`) `TransactionLogValue` records across the
/// Ongoing → `PrepareCommit` → `CompleteCommit` transitions; seeing the committed
/// records proves the v1 *encode* path runs at `TV_1` and the cycle works
/// end-to-end (decode/recover byte-exactness is unit-tested in `txn::log_record`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tv1_flexible_full_cycle_commits_and_reads() {
    let (broker, bootstrap, _dir) = boot_single().await;
    let admin = admin_client(&bootstrap).await;
    create_topic(&admin, "tv1", 1).await;

    downgrade_transaction_version(&admin, 1).await;

    full_cycle_commit_and_read(&bootstrap, "tv1", "tv1-tid", "tv1-g").await;

    broker.shutdown().await;
}

// ── test 2: TV_0 full cycle (classic, non-flexible v0 records) ─────────────────

/// Downgrade `transaction.version` to 0 (tombstone → Classic), then run a full
/// transactional cycle. The commit writes v0 (non-flexible, `header 00 00`)
/// `TransactionLogValue` records; reading the committed records back proves the
/// v0 *encode* path runs at `TV_0` and the cycle works end-to-end (decode/recover
/// byte-exactness is unit-tested in `txn::log_record`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tv0_classic_full_cycle_commits_and_reads() {
    let (broker, bootstrap, _dir) = boot_single().await;
    let admin = admin_client(&bootstrap).await;
    create_topic(&admin, "tv0", 1).await;

    downgrade_transaction_version(&admin, 0).await;

    full_cycle_commit_and_read(&bootstrap, "tv0", "tv0-tid", "tv0-g").await;

    broker.shutdown().await;
}

// ── test 3: verify-only AddPartitionsToTxn at TV_2 ─────────────────────────────

/// At the default `TV_2`, verify-only `AddPartitionsToTxn` (KIP-890) returns
/// per-partition `NONE (0)` for a partition already in the ongoing txn and
/// `TRANSACTION_ABORTABLE (120)` for one that was never added.
///
/// Flow: `InitProducerId` → `AddPartitionsToTxn` (`verify_only=false`) adding
/// `(t,0)` → `AddPartitionsToTxn` (`verify_only=true`) querying both `(t,0)`
/// (added → NONE) and `(t,1)` (never added → `TRANSACTION_ABORTABLE`).
///
/// Sent over a single connection to the in-process broker, which is its own
/// transaction coordinator. `Client::send` negotiates the highest mutually
/// supported version; the broker advertises v5, which carries the same
/// batched `transactions` array + `verify_only` field as v4 and routes through
/// the identical `handle_v4` verify path, so the assertions hold regardless.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tv2_verify_only_add_partitions_reports_per_partition_codes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    // Two partitions so (t,1) is a real partition that simply isn't in the txn.
    create_topic(&client, "t", 2).await;

    // Locate (and trigger loading of) the transaction coordinator for TID.
    // On a single-broker cluster the coordinator is this same node, but the
    // `__transaction_state` partition's coordinator load can lag broker boot,
    // so `InitProducerId` may transiently return NOT_COORDINATOR (16) until it
    // settles — retry until the coordinator is ready.
    let (pid, epoch) = await_transaction_coordinator(&client).await;

    // Normal add of (t, 0): transitions the entry to Ongoing and registers the
    // partition. verify_only=false.
    let added_topic = AddPartitionsToTxnTopic {
        name: "t".into(),
        partitions: vec![0],
        ..Default::default()
    };
    let add = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: VERIFY_TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![added_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: VERIFY_TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![added_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn add");
    assert!(add.error_code == 0, "AddPartitionsToTxn add: {add:?}");
    let add_part = &add.results_by_transaction[0].topic_results[0].results_by_partition[0];
    assert!(
        add_part.partition_error_code == NONE,
        "adding (t,0) should succeed: {add:?}"
    );

    // Verify-only query for BOTH (t,0) (added → NONE) and (t,1) (not added →
    // TRANSACTION_ABORTABLE). verify_only=true must never mutate state.
    let verify_topic = AddPartitionsToTxnTopic {
        name: "t".into(),
        partitions: vec![0, 1],
        ..Default::default()
    };
    let verify = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: VERIFY_TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: true,
                topics: vec![verify_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: VERIFY_TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![verify_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn verify-only");
    assert!(verify.error_code == 0, "verify-only top-level: {verify:?}");

    let topic_result = &verify.results_by_transaction[0].topic_results[0];
    assert!(topic_result.name == "t", "verify topic name: {verify:?}");

    let code_for = |partition: i32| -> i16 {
        topic_result
            .results_by_partition
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("partition {partition} missing in verify result: {verify:?}"))
            .partition_error_code
    };

    let p0 = code_for(0);
    let p1 = code_for(1);
    assert!(
        p0 == NONE,
        "verify-only (t,0) already in txn must be NONE(0), got {p0}: {verify:?}"
    );
    assert!(
        p1 == TRANSACTION_ABORTABLE,
        "verify-only (t,1) not in txn must be TRANSACTION_ABORTABLE(120), got {p1}: {verify:?}"
    );

    broker.shutdown().await;
}

// ── restart recovery: __transaction_state DECODE / recover-from-disk ────────────

/// Re-open the broker on the SAME data dir. A populated dir replays the raft
/// log/checkpoint rather than re-bootstrapping, so the restart uses
/// `BootstrapMode::Rejoin` (same pattern as
/// `consumer_group_next_gen_persistence.rs`).
fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

/// `InitProducerId` for `tid`, retrying while the coordinator is still loading
/// (`COORDINATOR_NOT_AVAILABLE(15)` / `NOT_COORDINATOR(16)`). Returns the
/// assigned `(producer_id, producer_epoch)`.
async fn init_producer_id(client: &Client, tid: &str) -> (i64, i16) {
    // FindCoordinator locates and triggers loading of the coordinator for tid;
    // on a single-broker cluster the coordinator load can lag broker boot.
    let fc = client
        .send(FindCoordinatorRequest {
            key: tid.into(),
            key_type: 1, // TRANSACTION
            coordinator_keys: vec![tid.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        fc.error_code == 0 || fc.coordinators.iter().all(|c| c.error_code == 0),
        "FindCoordinator: {fc:?}"
    );

    let mut init = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let resp = client
            .send(InitProducerIdRequest {
                transactional_id: Some(tid.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        if resp.error_code == 0 {
            init = Some(resp);
            break;
        }
        assert!(
            resp.error_code == 15 || resp.error_code == 16,
            "InitProducerId failed: {resp:?}"
        );
        // intentional: txn-coordinator load state is not in the metadata image and
        // has no metric/awaiter; only InitProducerId's 15/16 code signals it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let init = init.expect("InitProducerId did not become ready within 10s");
    (init.producer_id, init.producer_epoch)
}

/// `AddPartitionsToTxn` to add `(topic, partition)` to the ongoing txn for
/// `tid`/`pid`/`epoch`. This transitions the coordinator entry to `Ongoing`
/// and PERSISTS a `TransactionLogValue` record to `__transaction_state` —
/// without committing it. Asserts success.
async fn add_partition_ongoing(
    client: &Client,
    tid: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
) {
    let added_topic = AddPartitionsToTxnTopic {
        name: topic.into(),
        partitions: vec![partition],
        ..Default::default()
    };
    let add = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: tid.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![added_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: tid.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![added_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn add");
    assert!(add.error_code == 0, "AddPartitionsToTxn add: {add:?}");
    let add_part = &add.results_by_transaction[0].topic_results[0].results_by_partition[0];
    assert!(
        add_part.partition_error_code == NONE,
        "adding ({topic},{partition}) should succeed: {add:?}"
    );
}

/// Wait for the transaction coordinator for `tid` to finish loading after a
/// (re)boot, then commit the in-flight transaction via `EndTxn`. The commit
/// only succeeds if the coordinator already holds an `Ongoing` entry whose
/// `(producer_id, producer_epoch)` match — which, on a freshly-rebooted broker,
/// can only have come from decoding the persisted `__transaction_state` record.
/// Returns the `EndTxn` error code.
async fn commit_via_end_txn(client: &Client, tid: &str, pid: i64, epoch: i16) -> i16 {
    // FindCoordinator both locates and triggers loading of the coordinator.
    let fc = client
        .send(FindCoordinatorRequest {
            key: tid.into(),
            key_type: 1, // TRANSACTION
            coordinator_keys: vec![tid.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        fc.error_code == 0 || fc.coordinators.iter().all(|c| c.error_code == 0),
        "FindCoordinator: {fc:?}"
    );

    // Retry while the coordinator is still loading state from disk.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = client
            .send(EndTxnRequest {
                transactional_id: tid.into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
                ..Default::default()
            })
            .await
            .expect("EndTxn");
        // 15/16: coordinator still loading — keep retrying until the deadline.
        if (resp.error_code == 15 || resp.error_code == 16) && std::time::Instant::now() < deadline
        {
            // intentional: coordinator recover/load state after restart is not in the
            // metadata image and has no metric/awaiter; only EndTxn's 15/16 code
            // signals it. Bounded RPC-response poll, not a materialization wait.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return resp.error_code;
    }
}

/// PRIMARY durability proof for the v1 (flexible, `TV_2` default) codec.
///
/// Boot a broker, start a transaction and add a partition so an `Ongoing`
/// `TransactionLogValue` (v1) is persisted to `__transaction_state` — but do
/// NOT commit. Shut the broker down, then re-boot on the SAME data dir
/// (`BootstrapMode::Rejoin`), which drives `TxnCoordinator::recover` →
/// `decode_key`/`decode_value` over the persisted record. Finally send
/// `EndTxn(commit)` for the captured `(pid, epoch)`: it returns `NONE(0)` only
/// if recovery decoded the `Ongoing` entry from disk with matching pid/epoch.
/// This is the only path that exercises the startup decode/recover code that
/// the live-broker tests above (which never restart) cannot reach.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_ongoing_txn_survives_restart_via_decode_recover() {
    const TID: &str = "recover-v1-tid";
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let (pid, epoch);
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;
        // Default self-bootstrap is transaction.version=2 → v1 records.
        create_topic(&client, "rec1", 1).await;

        (pid, epoch) = init_producer_id(&client, TID).await;
        add_partition_ongoing(&client, TID, pid, epoch, "rec1", 0).await;
        // Deliberately do NOT commit: the entry stays Ongoing on disk.

        broker.shutdown().await;
    }

    // Re-boot on the same dir: triggers TxnCoordinator::recover + decode.
    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;

        let code = commit_via_end_txn(&client, TID, pid, epoch).await;
        assert!(
            code == NONE,
            "EndTxn(commit) after restart must succeed (proves recover() decoded \
             the Ongoing v1 entry with matching pid={pid}/epoch={epoch}); got error_code={code}"
        );

        broker.shutdown().await;
    }
}

/// PRIMARY durability proof for the v0 (non-flexible, classic) codec.
///
/// Identical to the v1 test, except `transaction.version` is downgraded to 0
/// BEFORE the transaction starts, so the persisted `TransactionLogValue` is the
/// non-flexible v0 (`header 00 00`) format. The post-restart `EndTxn(commit)`
/// succeeding proves the v0 record was decoded from disk by `recover()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v0_ongoing_txn_survives_restart_via_decode_recover() {
    const TID: &str = "recover-v0-tid";
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let (pid, epoch);
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;
        create_topic(&client, "rec0", 1).await;

        // Downgrade to TV_0 so the persisted record uses the v0 codec.
        downgrade_transaction_version(&client, 0).await;

        (pid, epoch) = init_producer_id(&client, TID).await;
        add_partition_ongoing(&client, TID, pid, epoch, "rec0", 0).await;

        broker.shutdown().await;
    }

    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;

        let code = commit_via_end_txn(&client, TID, pid, epoch).await;
        assert!(
            code == NONE,
            "EndTxn(commit) after restart must succeed (proves recover() decoded \
             the Ongoing v0 entry with matching pid={pid}/epoch={epoch}); got error_code={code}"
        );

        broker.shutdown().await;
    }
}
