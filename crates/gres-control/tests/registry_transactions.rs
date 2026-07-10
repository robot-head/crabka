//! Live broker coverage for the transactional tenant-registry writer.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_producer::{Acks, Producer, ProducerError, ProducerRecord};
use crabka_gres_control::{
    ControlError, RangeLayoutEntry, RangeLayoutMove, Registry, SqlUser, TENANT_REGISTRY_TOPIC,
    TenantId, TenantName, TenantRecord, TenantState, encode_registry_record,
};
use tempfile::TempDir;
use tokio::sync::Barrier;

const REGISTRY_TRANSACTIONAL_ID: &str = "__gres_tenants.writer";
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

async fn boot_broker() -> (BrokerHandle, String, TempDir) {
    let directory = TempDir::new().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, directory)
}

async fn connected_registry(bootstrap: &str) -> Registry {
    let mut registry = Registry::connect(bootstrap)
        .await
        .expect("registry connect");
    registry.ensure_topic(1).await.expect("registry topic");
    registry
}

fn ranged_tenant() -> TenantRecord {
    TenantRecord::new(
        1,
        TenantId::try_from("tenant-a").expect("tenant id"),
        TenantName::try_from("tenant-a").expect("tenant name"),
        TenantState::Active,
        SqlUser::try_from("alice").expect("sql user"),
        "SCRAM-SHA-256$4096:salt$stored:server".to_string(),
        1,
    )
    .expect("tenant record")
    .with_range_layout(vec![RangeLayoutEntry {
        range_id: 0,
        end_key: None,
        endpoint: "range-0.initial:7432".to_string(),
        wal_generation: 1,
    }])
    .expect("range layout")
}

fn move_range(endpoint: &str, generation: u64) -> RangeLayoutMove {
    RangeLayoutMove {
        range_id: 0,
        endpoint: endpoint.to_string(),
        wal_generation: generation,
    }
}

fn is_conflict_or_fencing(error: &ControlError) -> bool {
    matches!(
        error,
        ControlError::RegistryVersionConflict { .. }
            | ControlError::Producer(
                ProducerError::FencedProducer | ProducerError::TransactionAborted
            )
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registry_writers_commit_exactly_one_layout_mutation() {
    let (broker, bootstrap, _directory) = boot_broker().await;
    let mut seed = connected_registry(&bootstrap).await;
    let initial = ranged_tenant();
    tokio::time::timeout(TEST_TIMEOUT, seed.upsert(&initial))
        .await
        .expect("seed write timed out")
        .expect("seed write");

    let mut first = connected_registry(&bootstrap).await;
    let mut second = connected_registry(&bootstrap).await;
    let start = Arc::new(Barrier::new(2));
    let first_start = Arc::clone(&start);
    let second_start = Arc::clone(&start);

    let first_write = async move {
        first_start.wait().await;
        first
            .move_range_layout_if_version(
                "tenant-a",
                initial.record_version,
                move_range("range-0.a:7432", 2),
            )
            .await
    };
    let second_write = async move {
        second_start.wait().await;
        second
            .move_range_layout_if_version(
                "tenant-a",
                initial.record_version,
                move_range("range-0.b:7432", 3),
            )
            .await
    };

    let (first_result, second_result) = Box::pin(tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(first_write, second_write)
    }))
    .await
    .expect("racing writers timed out");
    let outcomes = [&first_result, &second_result];
    assert!(outcomes.iter().filter(|result| result.is_ok()).count() == 1);
    let loser = outcomes
        .iter()
        .find_map(|result| result.as_ref().err())
        .expect("one racing writer must fail");
    assert!(
        is_conflict_or_fencing(loser),
        "unexpected loser error: {loser:?}"
    );

    let mut reader = connected_registry(&bootstrap).await;
    let final_record = tokio::time::timeout(TEST_TIMEOUT, reader.get("tenant-a"))
        .await
        .expect("final refresh timed out")
        .expect("final refresh")
        .expect("tenant remains present");
    assert!(final_record.record_version == initial.record_version + 1);
    assert!(
        matches!(
            final_record.ranges[0].endpoint.as_str(),
            "range-0.a:7432" | "range-0.b:7432"
        ),
        "unexpected final layout: {final_record:?}"
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_committed_reader_skips_aborted_snapshot_and_advances_past_it() {
    let (broker, bootstrap, _directory) = boot_broker().await;
    let mut registry = connected_registry(&bootstrap).await;
    let initial = ranged_tenant();
    tokio::time::timeout(TEST_TIMEOUT, registry.upsert(&initial))
        .await
        .expect("seed write timed out")
        .expect("seed write");
    let mut applied = registry.watch();
    let initial_offset = *applied.borrow_and_update();

    let aborted = initial
        .clone()
        .move_range_layout(0, "range-0.aborted:7432", 99)
        .expect("aborted snapshot");
    let producer = Producer::builder()
        .bootstrap(&bootstrap)
        .client_id("gres-control-aborted-snapshot")
        .enable_idempotence(true)
        .acks(Acks::All)
        .transactional_id(REGISTRY_TRANSACTIONAL_ID)
        .build()
        .await
        .expect("raw producer");
    producer
        .init_transactions()
        .await
        .expect("raw producer init transactions");
    let transaction = producer
        .begin_transaction()
        .await
        .expect("begin transaction");
    let (key, value) = encode_registry_record(&aborted).expect("encode aborted snapshot");
    let acknowledgement = producer
        .send(ProducerRecord {
            topic: TENANT_REGISTRY_TOPIC.to_string(),
            partition: Some(0),
            key: Some(Bytes::from(key)),
            value: Some(Bytes::from(value)),
            ..Default::default()
        })
        .await;
    tokio::time::timeout(TEST_TIMEOUT, acknowledgement)
        .await
        .expect("aborted snapshot acknowledgement timed out")
        .expect("aborted snapshot acknowledgement dropped")
        .expect("aborted snapshot produce");
    transaction
        .abort()
        .await
        .expect("abort snapshot transaction");

    tokio::time::timeout(
        TEST_TIMEOUT,
        registry.move_range_layout_if_version(
            "tenant-a",
            initial.record_version,
            move_range("range-0.committed:7432", 2),
        ),
    )
    .await
    .expect("committed mutation timed out")
    .expect("committed mutation");
    tokio::time::timeout(
        TEST_TIMEOUT,
        applied.wait_for(|offset| *offset > initial_offset + 1),
    )
    .await
    .expect("reader did not advance past aborted offset")
    .expect("registry reader stopped before advancing past aborted offset");

    let final_record = registry
        .get("tenant-a")
        .await
        .expect("read committed registry")
        .expect("tenant remains present");
    assert!(final_record.record_version == initial.record_version + 1);
    assert!(final_record.ranges[0].endpoint == "range-0.committed:7432");
    assert!(final_record.ranges[0].endpoint != "range-0.aborted:7432");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_retry_publishes_the_canonical_monotonic_snapshot() {
    let (broker, bootstrap, _directory) = boot_broker().await;
    let mut registry = connected_registry(&bootstrap).await;
    let initial = ranged_tenant();
    registry.upsert(&initial).await.expect("seed tenant");

    let mut replacement = initial.clone();
    replacement.record_version = initial.record_version + 1;
    replacement.wal_generation = 0;
    replacement.ranges[0].wal_generation = 0;
    registry
        .replace_if_version(&replacement, Some(initial.record_version))
        .await
        .expect("canonical replacement");
    registry
        .replace_if_version(&replacement, Some(initial.record_version))
        .await
        .expect("same replacement retry");

    let canonical = registry
        .get("tenant-a")
        .await
        .expect("read canonical replacement")
        .expect("tenant retained");
    assert!(canonical.wal_generation == initial.wal_generation);
    assert!(canonical.ranges[0].wal_generation == initial.ranges[0].wal_generation);

    broker.shutdown().await;
}
