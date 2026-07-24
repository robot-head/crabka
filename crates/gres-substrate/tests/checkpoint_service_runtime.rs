use std::sync::Arc;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, DeleteRecordsOp, DeleteRecordsOutcome};
use crabka_client_producer::{Acks, Producer};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_gres_substrate::{
    CommittedWalReader, FenceLease, GroupCommitRequest, InMemoryWalLog, ProducerWalWriter,
    SubstrateError, TransactionalWalWriter, WalFrame, WriterGeneration, apply_frame,
    checkpoint::{
        CheckpointConfig, CheckpointService, CheckpointSnapshot, CheckpointStats, CheckpointStore,
        CheckpointTrigger, CheckpointWalPruner, DEFAULT_CHECKPOINT_RETAIN, DEFAULT_PART_MAX_BYTES,
        InMemoryCheckpointStore, ObjectOpsCheckpointStore, RestoreTail,
        restore_latest_and_replay_tail,
    },
    ensure_wal_topic_for_range, recover_live_for_range_with_restore, transactional_id_for_range,
};
use crabka_object_store::{ObjectOps, ObjectStoreClient, ObjectStoreConfig, build_object_store};
use crabka_pgkv::{Kv, MemKv, SnapshotKv, WriteOp};
use tokio::sync::Mutex;

#[tokio::test]
async fn live_fence_check_registers_a_transaction_before_end_txn() {
    let (_broker, bootstrap, _dir) = boot_broker().await;
    let tenant = TenantName::parse("fence-check").expect("tenant");
    let topic = create_wal_topic(&bootstrap, &tenant, RangeId::new(0)).await;
    let writer = live_wal_writer(
        &bootstrap,
        &topic,
        transactional_id_for_range(&tenant, RangeId::new(0)),
    )
    .await;

    writer
        .assert_current(WriterGeneration(7))
        .await
        .expect("fence check must produce before EndTxn");

    writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(7),
            frames: vec![frame(0, b"after-fence-check", b"committed")],
        })
        .await
        .expect("writer remains usable after fence check");
}

#[tokio::test]
async fn spawned_checkpoint_service_prunes_wal_and_recovery_replays_retained_tail() {
    let log = InMemoryWalLog::shared();
    let serving_kv = Arc::new(MemKv::default());
    let checkpoints = InMemoryCheckpointStore::shared();
    let stats = Arc::new(CheckpointStats::default());
    let service = Arc::new(
        CheckpointService::new(
            checkpoint_config(),
            serving_kv.clone() as Arc<dyn SnapshotKv>,
            checkpoints.clone(),
            log.clone(),
            stats.clone(),
        )
        .expect("checkpoint service"),
    );
    let handle = service.spawn();

    let checkpointed_frames = vec![frame(0, b"base", b"from-wal"), frame(1, b"covered", b"yes")];
    let checkpoint_ack = log
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: checkpointed_frames.clone(),
        })
        .await
        .expect("commit checkpointed frames");
    apply_serving_frames(serving_kv.as_ref(), &checkpointed_frames).expect("apply committed state");
    stats.record_committed(2, 128);

    let checkpoint_offset = checkpoint_ack.frames.last().expect("last ack").offset;
    let run = handle
        .checkpoint_if_threshold_crossed(CheckpointSnapshot {
            covered_offset: checkpoint_offset,
            journal_seq: 2,
            producer_epoch: 0,
            wal_generation: 0,
            garbage_horizon_xid: 0,
        })
        .await
        .expect("checkpoint command")
        .expect("threshold checkpoint");

    assert!(run.trigger == CheckpointTrigger::Frames);
    assert!(run.manifest.covered_offset == checkpoint_offset);
    assert!(run.prune.delete_records[0].offset == checkpoint_offset + 1);
    assert!(log.earliest_retained_offset().await == checkpoint_offset + 1);
    assert!(
        log.committed_from_start()
            .await
            .expect("retained")
            .is_empty()
    );

    let tail_ack = log
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![frame(2, b"tail", b"retained"), barrier()],
        })
        .await
        .expect("commit retained tail");
    let barrier_offset = tail_ack.frames.last().expect("barrier ack").offset;
    let retained_tail = log
        .committed_from_start()
        .await
        .expect("read retained tail");
    let restored = MemKv::default();

    let plan = restore_latest_and_replay_tail(
        checkpoints.as_ref(),
        "tenant-a",
        &restored,
        RestoreTail {
            current_generation: 0,
            log_start: log.log_start_offset().await.expect("log start"),
            committed_frames: retained_tail,
            barrier_offset,
        },
    )
    .await
    .expect("restore checkpoint and replay retained tail");

    assert!(
        plan.restored_from
            .expect("checkpoint source")
            .covered_offset
            == checkpoint_offset
    );
    assert!(plan.replay.next_journal_seq == 3);
    assert!(restored.get(b"base").expect("base") == Some(b"from-wal".to_vec()));
    assert!(restored.get(b"covered").expect("covered") == Some(b"yes".to_vec()));
    assert!(restored.get(b"tail").expect("tail") == Some(b"retained".to_vec()));

    handle
        .shutdown()
        .await
        .expect("shutdown checkpoint service");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_broker_checkpoint_delete_records_and_recovery_replays_retained_tail() {
    let (_broker, bootstrap, _broker_dir) = boot_broker().await;
    let tenant = TenantName::parse("tenant-live-checkpoint").expect("valid tenant name");
    let range = RangeId::COORDINATOR;
    let topic = create_wal_topic(&bootstrap, &tenant, range).await;
    let checkpoint_dir = tempfile::TempDir::new().expect("checkpoint tempdir");
    let serving_kv = Arc::new(MemKv::default());
    let checkpoints = local_checkpoint_store(checkpoint_dir.path().to_path_buf());
    let stats = Arc::new(CheckpointStats::default());
    let pruner = Arc::new(AdminDeleteRecordsPruner::connect(&bootstrap).await);
    let recovery = crabka_gres_substrate::LiveRecoveryConfig::new(
        bootstrap.clone(),
        tenant.clone(),
        range,
        None,
    );
    let service = CheckpointService::new(
        CheckpointConfig::new(
            recovery.checkpoint_namespace(),
            topic.clone(),
            2,
            0,
            DEFAULT_PART_MAX_BYTES,
            DEFAULT_CHECKPOINT_RETAIN,
            std::time::Duration::from_secs(1),
        )
        .expect("checkpoint config"),
        serving_kv.clone() as Arc<dyn SnapshotKv>,
        checkpoints.clone(),
        pruner.clone(),
        stats.clone(),
    )
    .expect("checkpoint service");

    let writer = live_wal_writer(
        &bootstrap,
        &topic,
        transactional_id_for_range(&tenant, range),
    )
    .await;
    let checkpointed_frames = vec![
        frame(0, b"base", b"from-checkpoint"),
        frame(1, b"covered", b"yes"),
    ];
    let checkpoint_ack = writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: checkpointed_frames.clone(),
        })
        .await
        .expect("commit checkpointed frames");
    apply_serving_frames(serving_kv.as_ref(), &checkpointed_frames)
        .expect("apply checkpointed state");
    stats.record_committed(2, 128);

    let checkpoint_offset = checkpoint_ack
        .frames
        .last()
        .expect("last checkpoint ack")
        .offset;
    let run = service
        .checkpoint_if_threshold_crossed(CheckpointSnapshot {
            covered_offset: checkpoint_offset,
            journal_seq: 2,
            producer_epoch: 0,
            wal_generation: 0,
            garbage_horizon_xid: 0,
        })
        .await
        .expect("checkpoint command")
        .expect("threshold checkpoint");

    let retained_start = checkpoint_offset + 1;
    assert!(run.manifest.covered_offset == checkpoint_offset);
    assert!(run.prune.delete_records == vec![delete_records_op(&topic, retained_start)]);
    assert!(pruner.last_low_watermark().await == retained_start);

    assert_pause_blocks_writes(&writer, retained_start).await;

    writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![frame(2, b"tail", b"retained")],
        })
        .await
        .expect("commit retained tail after resume");

    let recovered = MemKv::default();
    let recovery = recovery.with_checkpoints(checkpoints);
    let recovered_state = recover_live_for_range_with_restore(recovery, &recovered)
        .await
        .expect("live recovery restores checkpoint and replays retained tail");

    assert!(recovered_state.next_journal_seq == 3);
    assert!(recovered_state.barrier_offset > retained_start);
    assert!(recovered.get(b"base").expect("base") == Some(b"from-checkpoint".to_vec()));
    assert!(recovered.get(b"covered").expect("covered") == Some(b"yes".to_vec()));
    assert!(recovered.get(b"tail").expect("tail") == Some(b"retained".to_vec()));
}

struct AdminDeleteRecordsPruner {
    admin: Mutex<AdminClient>,
    outcomes: Mutex<Vec<DeleteRecordsOutcome>>,
}

impl AdminDeleteRecordsPruner {
    async fn connect(bootstrap: &str) -> Self {
        let admin = AdminClient::connect(&[bootstrap.to_string()])
            .await
            .expect("admin connect");
        Self {
            admin: Mutex::new(admin),
            outcomes: Mutex::new(Vec::new()),
        }
    }

    async fn last_low_watermark(&self) -> i64 {
        self.outcomes
            .lock()
            .await
            .last()
            .expect("delete records outcome")
            .low_watermark
    }
}

#[async_trait::async_trait]
impl CheckpointWalPruner for AdminDeleteRecordsPruner {
    async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
        let outcomes = self
            .admin
            .lock()
            .await
            .delete_records(ops, 5_000)
            .await
            .map_err(|error| SubstrateError::Checkpoint(format!("delete records: {error}")))?;
        if let Some(failed) = outcomes.iter().find(|outcome| outcome.error_code != 0) {
            return Err(SubstrateError::Checkpoint(format!(
                "delete records {}-{} failed with error code {}",
                failed.topic, failed.partition, failed.error_code
            )));
        }
        self.outcomes.lock().await.extend(outcomes);
        Ok(())
    }
}

async fn boot_broker() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_wal_topic(bootstrap: &str, tenant: &TenantName, range: RangeId) -> String {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    ensure_wal_topic_for_range(&mut admin, tenant, range)
        .await
        .expect("create WAL topic")
}

async fn live_wal_writer(
    bootstrap: &str,
    topic: &str,
    transactional_id: String,
) -> ProducerWalWriter {
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap)
            .client_id("checkpoint-live-writer")
            .acks(Acks::All)
            .transactional_id(transactional_id)
            .build()
            .await
            .expect("producer build"),
    );
    producer
        .init_transactions()
        .await
        .expect("init transactions");
    ProducerWalWriter::new(producer, topic.to_string())
}

async fn assert_pause_blocks_writes(writer: &ProducerWalWriter, retained_start: i64) {
    let paused_writer = writer
        .pause_and_barrier(WriterGeneration(0))
        .await
        .expect("append pause barrier");
    assert!(paused_writer.barrier_offset >= retained_start);

    let write_error = writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![frame(2, b"tail", b"retained")],
        })
        .await
        .expect_err("paused writer rejects writes");
    assert!(matches!(write_error, SubstrateError::Unavailable(_)));

    let Err(second_pause_error) = writer.pause_and_barrier(WriterGeneration(0)).await else {
        panic!("concurrent pause must be rejected without waiting");
    };
    assert!(matches!(second_pause_error, SubstrateError::AlreadyPaused));

    paused_writer.resume();
}

fn local_checkpoint_store(root: std::path::PathBuf) -> Arc<dyn CheckpointStore> {
    let object_store =
        build_object_store(&ObjectStoreConfig::Local { root }).expect("local object store");
    let ops: Arc<dyn ObjectOps> = Arc::new(ObjectStoreClient::new(object_store));
    Arc::new(ObjectOpsCheckpointStore::new(ops))
}

fn delete_records_op(topic: &str, offset: i64) -> DeleteRecordsOp {
    DeleteRecordsOp {
        topic: topic.to_string(),
        partition: 0,
        offset,
    }
}

fn checkpoint_config() -> CheckpointConfig {
    CheckpointConfig::new(
        "tenant-a".to_string(),
        "__gres_wal.tenant-a.r0".to_string(),
        2,
        0,
        DEFAULT_PART_MAX_BYTES,
        DEFAULT_CHECKPOINT_RETAIN,
        std::time::Duration::from_secs(1),
    )
    .expect("checkpoint config")
}

fn apply_serving_frames(kv: &dyn Kv, frames: &[WalFrame]) -> Result<(), SubstrateError> {
    for frame in frames {
        apply_frame(kv, &frame.ops)?;
    }
    Ok(())
}

fn frame(journal_seq: u64, key: &[u8], value: &[u8]) -> WalFrame {
    WalFrame {
        journal_seq,
        ops: vec![WriteOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }],
    }
}

fn barrier() -> WalFrame {
    WalFrame {
        journal_seq: u64::MAX,
        ops: Vec::new(),
    }
}
