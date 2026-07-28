use std::sync::Arc;

use assert2::assert;
use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, DeleteRecordsOp};
use crabka_client_producer::{Acks, Producer};
use crabka_gres_ranges::{RangeId, TenantName};
use crabka_gres_substrate::{
    CheckpointPart, CheckpointSnapshotSource, DEFAULT_PART_MAX_SIZE, GroupCommitRequest,
    InMemoryWalLog, Manifest, ProducerWalWriter, RecoveryFencer, SubstrateError,
    TransactionalWalWriter, WalFrame, WriterGeneration, apply_frame,
    checkpoint::{
        CheckpointConfig, CheckpointFailpoint, CheckpointService, CheckpointServiceStep,
        CheckpointSnapshot, CheckpointStats, CheckpointStore, CheckpointTrigger,
        CheckpointWalPruner, InMemoryCheckpointStore,
    },
    ckpt_dir, ensure_wal_topic_for_range, ensure_wal_topic_name, manifest_key, part_key,
    recover_live_for_range_with_restore, transactional_id_for_range,
};

#[test]
fn test_feature_callback_records_named_boundary() {
    let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = observed.clone();
    let hook: CheckpointFailpoint = std::sync::Arc::new(move |step| {
        sink.lock().expect("step sink").push(step);
        false
    });

    assert!(!hook(CheckpointServiceStep::BeforeParts));
    assert!(observed.lock().expect("observed").as_slice() == [CheckpointServiceStep::BeforeParts]);
}

#[tokio::test]
async fn production_service_crash_matrix_recovers_exact_acked_state() {
    for step in [
        CheckpointServiceStep::BeforeParts,
        CheckpointServiceStep::PartsUploaded,
        CheckpointServiceStep::ManifestWritten,
        CheckpointServiceStep::Truncated,
        CheckpointServiceStep::Pruned,
    ] {
        let harness = ProductionCrashHarness::new().await;
        let observed = harness.crash_new_checkpoint_at(step).await;
        assert!(observed.contains(&step));

        let restored = harness.recover().await.expect("fresh compute recovery");
        let expected_source = if matches!(
            step,
            CheckpointServiceStep::BeforeParts | CheckpointServiceStep::PartsUploaded
        ) {
            1
        } else {
            2
        };
        assert!(restored.source.expect("checkpoint source").covered_offset == expected_source);
        assert!(restored.next_journal_seq == 3);
        assert!(restored.value(b"a") == Some(b"base".to_vec()));
        assert!(restored.value(b"b") == Some(b"second".to_vec()));
        assert!(restored.value(b"c") == Some(b"third".to_vec()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_broker_captured_service_crash_matrix_recovers_exact_state() {
    let dir = tempfile::TempDir::new().expect("broker tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    for (case, step, occurrence) in [
        ("before", CheckpointServiceStep::BeforeParts, 1),
        ("parts-one", CheckpointServiceStep::PartsUploaded, 1),
        ("parts-later", CheckpointServiceStep::PartsUploaded, 2),
        ("manifest", CheckpointServiceStep::ManifestWritten, 1),
        ("truncate", CheckpointServiceStep::Truncated, 1),
        ("prune", CheckpointServiceStep::Pruned, 1),
    ] {
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            live_broker_crash_case(&bootstrap, case, step, occurrence),
        )
        .await
        .expect("live crash deadline")
        .unwrap_or_else(|error| panic!("live crash case {case}: {error}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_fresh_generation_restores_old_checkpoint_and_fetches_offset_zero() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let dir = tempfile::TempDir::new().expect("broker tempdir");
        let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("broker start");
        let bootstrap = broker.listen_addr().to_string();
        let tenant = TenantName::parse("g3-fresh-generation").expect("tenant");
        let range = RangeId::COORDINATOR;
        let objects = InMemoryCheckpointStore::shared();
        let old = MemKv::default();
        old.put(b"old".to_vec(), b"checkpoint".to_vec())
            .expect("old state");
        crabka_gres_substrate::checkpoint::write_checkpoint(
            objects.as_ref(),
            &format!("{tenant}/r0"),
            &old,
            snapshot_at(0, 7, 0),
            DEFAULT_PART_MAX_SIZE,
        )
        .await
        .expect("old-generation checkpoint");

        let fresh_config = crabka_gres_substrate::LiveRecoveryConfig::new(
            bootstrap.clone(),
            tenant.clone(),
            range,
            None,
        )
        .with_wal_generation(1);
        let mut admin = AdminClient::connect(std::slice::from_ref(&bootstrap))
            .await
            .expect("admin");
        let topic = ensure_wal_topic_name(&mut admin, &fresh_config.wal_topic())
            .await
            .expect("fresh topic");
        drop(admin);
        let producer = Arc::new(
            Producer::builder()
                .bootstrap(bootstrap.clone())
                .client_id("g3-fresh-generation-writer")
                .acks(Acks::All)
                .transactional_id(transactional_id_for_range(&tenant, range))
                .build()
                .await
                .expect("producer"),
        );
        producer
            .init_transactions()
            .await
            .expect("init transactions");
        ProducerWalWriter::new(producer, topic)
            .commit_group(GroupCommitRequest {
                generation: WriterGeneration(1),
                frames: vec![frame(0, b"fresh", b"offset-zero")],
            })
            .await
            .expect("fresh offset-zero frame");

        let restored = MemKv::default();
        let outcome =
            recover_live_for_range_with_restore(fresh_config.with_checkpoints(objects), &restored)
                .await
                .expect("fresh-generation recovery");

        assert!(outcome.generation == WriterGeneration(1));
        assert!(outcome.next_journal_seq == 1);
        assert!(restored.get(b"old").expect("old") == Some(b"checkpoint".to_vec()));
        assert!(restored.get(b"fresh").expect("fresh") == Some(b"offset-zero".to_vec()));
        drop(outcome);
    })
    .await
    .expect("fresh-generation deadline");
}

async fn live_broker_crash_case(
    bootstrap: &str,
    case: &str,
    stop: CheckpointServiceStep,
    occurrence: usize,
) -> Result<(), SubstrateError> {
    let tenant = TenantName::parse(format!("g3-crash-{case}"))
        .map_err(|error| SubstrateError::Unavailable(error.to_string()))?;
    let range = RangeId::COORDINATOR;
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .map_err(|error| SubstrateError::Unavailable(error.to_string()))?;
    let topic = ensure_wal_topic_for_range(&mut admin, &tenant, range).await?;
    drop(admin);
    let producer = Arc::new(
        Producer::builder()
            .bootstrap(bootstrap.to_owned())
            .client_id(format!("g3-crash-{case}"))
            .acks(Acks::All)
            .transactional_id(transactional_id_for_range(&tenant, range))
            .build()
            .await
            .map_err(|error| SubstrateError::Unavailable(error.to_string()))?,
    );
    producer
        .init_transactions()
        .await
        .map_err(|error| SubstrateError::Unavailable(error.to_string()))?;
    let writer = ProducerWalWriter::new(producer, topic.clone());
    let kv = Arc::new(MemKv::default());
    let objects = InMemoryCheckpointStore::shared();
    let pruner = Arc::new(LiveAdminPruner::connect(bootstrap).await?);
    let namespace = format!("{tenant}/r0");

    let mut last_offset = -1;
    for wal_frame in [frame(0, b"a", b"base"), frame(1, b"b", b"second")] {
        let ack = writer
            .commit_group(GroupCommitRequest {
                generation: WriterGeneration(0),
                frames: vec![wal_frame.clone()],
            })
            .await?;
        last_offset = ack.frames[0].offset;
        apply_frame(kv.as_ref(), &wal_frame.ops)?;
    }
    let baseline = live_service(
        &namespace,
        &topic,
        kv.clone(),
        objects.clone(),
        pruner.clone(),
        None,
    )?;
    run_captured_service(baseline, last_offset, 2).await?;

    let third = frame(2, b"c", b"third");
    let ack = writer
        .commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![third.clone()],
        })
        .await?;
    last_offset = ack.frames[0].offset;
    apply_frame(kv.as_ref(), &third.ops)?;
    let seen = std::sync::atomic::AtomicUsize::new(0);
    let hook: CheckpointFailpoint = Arc::new(move |step| {
        if step != stop {
            return false;
        }
        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1 == occurrence
    });
    let crashing = live_service(&namespace, &topic, kv, objects.clone(), pruner, Some(hook))?;
    assert!(
        run_captured_service(crashing, last_offset, 3)
            .await
            .is_err()
    );

    let restored = MemKv::default();
    let recovery = crabka_gres_substrate::LiveRecoveryConfig::new(bootstrap, tenant, range, None)
        .with_checkpoints(objects);
    let outcome = recover_live_for_range_with_restore(recovery, &restored).await?;
    assert!(outcome.next_journal_seq == 3);
    assert!(restored.get(b"a")? == Some(b"base".to_vec()));
    assert!(restored.get(b"b")? == Some(b"second".to_vec()));
    assert!(restored.get(b"c")? == Some(b"third".to_vec()));
    drop(outcome);
    Ok(())
}

fn live_service(
    namespace: &str,
    topic: &str,
    kv: Arc<MemKv>,
    objects: Arc<InMemoryCheckpointStore>,
    pruner: Arc<LiveAdminPruner>,
    hook: Option<CheckpointFailpoint>,
) -> Result<CheckpointService<LiveAdminPruner>, SubstrateError> {
    let mut service = CheckpointService::new(
        CheckpointConfig::new(
            namespace.into(),
            topic.into(),
            1,
            0,
            crabka_units::bytes(24),
            2,
            std::time::Duration::from_secs(1),
        )?,
        kv as Arc<dyn SnapshotKv>,
        objects,
        pruner,
        Arc::new(CheckpointStats::default()),
    )?;
    if let Some(hook) = hook {
        service = service.with_test_failpoint(hook);
    }
    Ok(service)
}

async fn run_captured_service(
    service: CheckpointService<LiveAdminPruner>,
    covered_offset: i64,
    journal_seq: u64,
) -> Result<(), SubstrateError> {
    let handle = Arc::new(service).spawn();
    let result = handle
        .checkpoint_from_source(
            Arc::new(CheckpointSnapshotSource::new(
                covered_offset,
                journal_seq,
                WriterGeneration(0),
            )),
            CheckpointTrigger::Manual,
        )
        .await
        .map(|_| ());
    handle.shutdown().await?;
    result
}

struct LiveAdminPruner {
    admin: Mutex<AdminClient>,
}

#[derive(Default)]
struct RecordingPruner {
    calls: Mutex<Vec<DeleteRecordsOp>>,
}

#[async_trait::async_trait]
impl CheckpointWalPruner for RecordingPruner {
    async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
        self.calls.lock().await.extend_from_slice(ops);
        Ok(())
    }
}

impl LiveAdminPruner {
    async fn connect(bootstrap: &str) -> Result<Self, SubstrateError> {
        let admin = AdminClient::connect(&[bootstrap.to_owned()])
            .await
            .map_err(|error| SubstrateError::Unavailable(error.to_string()))?;
        Ok(Self {
            admin: Mutex::new(admin),
        })
    }
}

#[async_trait::async_trait]
impl CheckpointWalPruner for LiveAdminPruner {
    async fn delete_records(&self, ops: &[DeleteRecordsOp]) -> Result<(), SubstrateError> {
        let outcomes = self
            .admin
            .lock()
            .await
            .delete_records(ops, 5_000)
            .await
            .map_err(|error| SubstrateError::Checkpoint(error.to_string()))?;
        if let Some(failed) = outcomes.iter().find(|outcome| outcome.error_code != 0) {
            return Err(SubstrateError::Checkpoint(format!(
                "DeleteRecords {}-{} failed with {}",
                failed.topic, failed.partition, failed.error_code
            )));
        }
        Ok(())
    }
}

#[tokio::test]
async fn production_service_manifest_loss_after_truncate_refuses() {
    let harness = ProductionCrashHarness::new().await;
    harness
        .crash_new_checkpoint_at(CheckpointServiceStep::Truncated)
        .await;
    harness.delete_new_manifest().await;

    let Err(error) = harness.recover().await else {
        panic!("must refuse torn truncation");
    };
    assert!(matches!(
        error,
        SubstrateError::TornTruncation {
            log_start: 3,
            newest_manifest: 1
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(
    clippy::too_many_lines,
    reason = "the zombie race keeps each durable boundary and release ordering visible"
)]
async fn production_zombie_service_cannot_supersede_successor_manifest() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let objects = InMemoryCheckpointStore::shared();
        let log = InMemoryWalLog::shared();
        let pruner = Arc::new(RecordingPruner::default());
        let old_kv = Arc::new(MemKv::default());
        old_kv
            .put(b"owner".to_vec(), b"zombie".to_vec())
            .expect("old state");
        let (reached_tx, reached_rx) = std::sync::mpsc::sync_channel(1);
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let release_hook = Arc::clone(&release);
        let hook: CheckpointFailpoint = Arc::new(move |step| {
            if step == CheckpointServiceStep::LeaseValidated {
                reached_tx.send(()).expect("report zombie upload");
                let (lock, condition) = &*release_hook;
                let mut released = lock.lock().expect("release lock");
                while !*released {
                    released = condition.wait(released).expect("release wait");
                }
            }
            false
        });
        let old_service = CheckpointService::new(
            CheckpointConfig::new(
                "zombie-race".into(),
                "wal.g0".into(),
                1,
                0,
                crabka_units::bytes(24),
                2,
                std::time::Duration::from_secs(1),
            )
            .expect("config"),
            old_kv as Arc<dyn SnapshotKv>,
            objects.clone(),
            pruner.clone(),
            Arc::new(CheckpointStats::default()),
        )
        .expect("old service")
        .with_test_failpoint(hook);
        let old_source = Arc::new(CheckpointSnapshotSource::new(2, 3, WriterGeneration(0)));
        old_source.set_fence_lease(log.clone(), WriterGeneration(0));
        let old_handle = Arc::new(old_service).spawn();
        let old_checkpoint = tokio::spawn({
            let source = Arc::clone(&old_source);
            async move {
                let result = old_handle
                    .checkpoint_from_source(source, CheckpointTrigger::Manual)
                    .await;
                (old_handle, result)
            }
        });
        tokio::task::spawn_blocking(move || {
            reached_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("zombie reached upload boundary");
        })
        .await
        .expect("wait for zombie");

        let barrier = log.fence_with_barrier().await.expect("fence zombie");
        let successor_kv = Arc::new(MemKv::default());
        successor_kv
            .put(b"owner".to_vec(), b"successor".to_vec())
            .expect("successor state");
        let successor_service = CheckpointService::new(
            CheckpointConfig::new(
                "zombie-race".into(),
                "wal.g1".into(),
                1,
                0,
                crabka_units::bytes(24),
                2,
                std::time::Duration::from_secs(1),
            )
            .expect("config"),
            successor_kv as Arc<dyn SnapshotKv>,
            objects.clone(),
            pruner.clone(),
            Arc::new(CheckpointStats::default()),
        )
        .expect("successor service");
        let successor_source = Arc::new(CheckpointSnapshotSource::new(
            barrier.offset,
            0,
            barrier.generation,
        ));
        successor_source.set_fence_lease(log.clone(), barrier.generation);
        let successor_handle = Arc::new(successor_service).spawn();
        successor_handle
            .checkpoint_from_source(successor_source, CheckpointTrigger::Manual)
            .await
            .expect("successor checkpoint");
        successor_handle
            .shutdown()
            .await
            .expect("successor shutdown");

        let (lock, condition) = &*release;
        *lock.lock().expect("release lock") = true;
        condition.notify_all();
        let (old_handle, old_result) = old_checkpoint.await.expect("zombie task");
        old_result.expect("zombie prunes only its immutable old topic");
        old_handle.shutdown().await.expect("old shutdown");
        let pruned_topics = pruner
            .calls
            .lock()
            .await
            .iter()
            .map(|op| op.topic.clone())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            pruned_topics == std::collections::BTreeSet::from(["wal.g0".into(), "wal.g1".into()])
        );

        let restored = MemKv::default();
        let source = crabka_gres_substrate::checkpoint::restore_latest(
            objects.as_ref(),
            "zombie-race",
            &restored,
            barrier.generation.0,
            None,
        )
        .await
        .expect("restore")
        .expect("successor manifest");
        assert!(source.wal_generation == barrier.generation.0);
        assert!(restored.get(b"owner").expect("owner") == Some(b"successor".to_vec()));
    })
    .await
    .expect("zombie race deadline");
}

struct ProductionCrashHarness {
    objects: std::sync::Arc<InMemoryCheckpointStore>,
    log: std::sync::Arc<InMemoryWalLog>,
    kv: std::sync::Arc<MemKv>,
}

impl ProductionCrashHarness {
    async fn new() -> Self {
        let harness = Self {
            objects: InMemoryCheckpointStore::shared(),
            log: InMemoryWalLog::shared(),
            kv: std::sync::Arc::new(MemKv::default()),
        };
        harness.commit_and_apply(frame(0, b"a", b"base")).await;
        harness.commit_and_apply(frame(1, b"b", b"second")).await;
        harness
            .run_checkpoint(snapshot_at(0, 1, 0), None)
            .await
            .expect("baseline checkpoint");
        harness.commit_and_apply(frame(2, b"c", b"third")).await;
        harness
    }

    async fn commit_and_apply(&self, frame: WalFrame) {
        self.log
            .commit_group(GroupCommitRequest {
                generation: WriterGeneration(0),
                frames: vec![frame.clone()],
            })
            .await
            .expect("commit workload group");
        apply_frame(self.kv.as_ref(), &frame.ops).expect("apply acknowledged group");
    }

    async fn crash_new_checkpoint_at(
        &self,
        stop: CheckpointServiceStep,
    ) -> Vec<CheckpointServiceStep> {
        let observed = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = observed.clone();
        let hook: CheckpointFailpoint = std::sync::Arc::new(move |step| {
            sink.lock().expect("step sink").push(step);
            step == stop
        });
        self.run_checkpoint(snapshot_at(0, 2, 1), Some(hook))
            .await
            .expect_err("failpoint must abruptly stop checkpoint");
        observed.lock().expect("observed steps").clone()
    }

    async fn run_checkpoint(
        &self,
        snapshot: CheckpointSnapshot,
        hook: Option<CheckpointFailpoint>,
    ) -> Result<(), SubstrateError> {
        self.run_checkpoint_with_kv(snapshot, hook, self.kv.clone())
            .await
    }

    async fn run_checkpoint_with_kv(
        &self,
        snapshot: CheckpointSnapshot,
        hook: Option<CheckpointFailpoint>,
        kv: std::sync::Arc<MemKv>,
    ) -> Result<(), SubstrateError> {
        let config = CheckpointConfig::new(
            "tenant-production-crash".into(),
            "wal-production-crash".into(),
            1,
            0,
            crabka_units::bytes(24),
            2,
            std::time::Duration::from_secs(1),
        )?;
        let mut service = CheckpointService::new(
            config,
            kv as std::sync::Arc<dyn SnapshotKv>,
            self.objects.clone(),
            self.log.clone(),
            std::sync::Arc::new(CheckpointStats::default()),
        )?;
        if let Some(hook) = hook {
            service = service.with_test_failpoint(hook);
        }
        let source = Arc::new(CheckpointSnapshotSource::new(
            snapshot.covered_offset,
            snapshot.journal_seq,
            WriterGeneration(snapshot.wal_generation),
        ));
        let handle = Arc::new(service).spawn();
        let result = handle
            .checkpoint_from_source(source, CheckpointTrigger::Manual)
            .await
            .map(|_| ());
        handle.shutdown().await?;
        result
    }

    async fn recover(&self) -> Result<RestoredState, SubstrateError> {
        let barrier_ack = self
            .log
            .commit_group(GroupCommitRequest {
                generation: WriterGeneration(0),
                frames: vec![barrier_frame()],
            })
            .await?;
        let barrier_offset = barrier_ack.frames[0].offset;
        let kv = MemKv::default();
        let plan = crabka_gres_substrate::checkpoint::restore_latest_and_replay_tail(
            self.objects.as_ref(),
            "tenant-production-crash",
            &kv,
            crabka_gres_substrate::checkpoint::RestoreTail {
                current_generation: 0,
                log_start: Some(self.log.earliest_retained_offset().await),
                committed_frames: crabka_gres_substrate::CommittedWalReader::committed_from_start(
                    self.log.as_ref(),
                )
                .await?,
                barrier_offset,
            },
        )
        .await?;
        Ok(RestoredState {
            source: plan.restored_from,
            next_journal_seq: plan.replay.next_journal_seq,
            kv,
        })
    }

    async fn delete_new_manifest(&self) {
        let key = manifest_key(&ckpt_dir("tenant-production-crash", 0, 2, 0));
        self.objects
            .delete(&key)
            .await
            .expect("delete newest manifest");
    }
}
use crabka_pgkv::{Kv, MemKv, SnapshotKv, WriteOp};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CrashStep {
    BeforeParts,
    AfterParts,
    AfterManifest,
    AfterTruncate,
    AfterPrune,
}

#[tokio::test]
async fn crash_before_manifest_recovers_previous_checkpoint_and_longer_tail() {
    for step in [CrashStep::BeforeParts, CrashStep::AfterParts] {
        let harness = CrashHarness::new().await;

        let restored = harness.crash_and_recover(step).await;

        assert!(restored.source.expect("source").covered_offset == 1);
        assert!(restored.value(b"a") == Some(b"base".to_vec()));
        assert!(restored.value(b"b") == Some(b"checkpointed".to_vec()));
        assert!(restored.value(b"c") == Some(b"after-new".to_vec()));
    }
}

#[tokio::test]
async fn crash_after_manifest_uses_newest_checkpoint() {
    for step in [
        CrashStep::AfterManifest,
        CrashStep::AfterTruncate,
        CrashStep::AfterPrune,
    ] {
        let harness = CrashHarness::new().await;

        let restored = harness.crash_and_recover(step).await;

        assert!(restored.source.expect("source").covered_offset == 2);
        assert!(restored.value(b"a") == Some(b"base".to_vec()));
        assert!(restored.value(b"b") == Some(b"checkpointed".to_vec()));
        assert!(restored.value(b"c") == Some(b"after-new".to_vec()));
    }
}

#[tokio::test]
async fn manifest_lost_after_delete_records_refuses_torn_truncation() {
    let harness = CrashHarness::new().await;
    harness.write_checkpoint_at(2, b"checkpointed").await;
    harness.truncate_to(3).await;
    harness.delete_manifest_at(2).await;

    let Err(error) = harness.recover().await else {
        panic!("torn truncation unexpectedly recovered");
    };

    assert!(matches!(
        error,
        SubstrateError::TornTruncation {
            log_start: 3,
            newest_manifest: 1
        }
    ));
}

#[tokio::test]
async fn older_zombie_checkpointer_cannot_corrupt_newest_recovery() {
    let harness = CrashHarness::new().await;
    harness.write_checkpoint_at(2, b"checkpointed").await;
    harness.write_zombie_checkpoint_at(1).await;

    let restored = harness.recover().await.expect("recover");

    assert!(restored.source.expect("source").covered_offset == 2);
    assert!(restored.value(b"b") == Some(b"checkpointed".to_vec()));
    assert!(restored.value(b"c") == Some(b"after-new".to_vec()));
}

#[tokio::test]
async fn future_generation_checkpoint_is_ignored_for_current_generation_recovery() {
    let harness = CrashHarness::new().await;
    harness
        .write_checkpoint_for_generation_at(1, 2, b"future-generation")
        .await;

    let restored = harness.recover().await.expect("recover");
    let source = restored.source.expect("source");

    assert!(source.wal_generation == 0);
    assert!(source.covered_offset == 1);
    assert!(restored.value(b"b") == Some(b"checkpointed".to_vec()));
    assert!(restored.value(b"c") == Some(b"after-new".to_vec()));
}

#[tokio::test]
async fn checkpoint_restore_replays_tail_after_covered_offset_and_preserves_sequence() {
    let harness = CrashHarness::new().await;
    harness.write_checkpoint_at(2, b"checkpointed").await;

    let restored = harness.recover().await.expect("recover");

    assert!(restored.source.expect("source").covered_offset == 2);
    assert!(restored.next_journal_seq == 4);
    assert!(restored.value(b"a") == Some(b"base".to_vec()));
    assert!(restored.value(b"b") == Some(b"checkpointed".to_vec()));
    assert!(restored.value(b"c") == Some(b"after-new".to_vec()));
}

struct CrashHarness {
    objects: std::sync::Arc<InMemoryCheckpointStore>,
    log_start: tokio::sync::Mutex<i64>,
}

struct RestoredState {
    source: Option<crabka_gres_substrate::checkpoint::RestoredFrom>,
    next_journal_seq: u64,
    kv: MemKv,
}

impl RestoredState {
    fn value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kv.get(key).expect("read restored key")
    }
}

impl CrashHarness {
    async fn new() -> Self {
        let harness = Self {
            objects: InMemoryCheckpointStore::shared(),
            log_start: tokio::sync::Mutex::new(0),
        };
        harness.write_checkpoint_at(1, b"previous-tail").await;
        harness
    }

    async fn crash_and_recover(&self, step: CrashStep) -> RestoredState {
        match step {
            CrashStep::BeforeParts => {}
            CrashStep::AfterParts => self.write_parts_without_manifest_at(2).await,
            CrashStep::AfterManifest => {
                self.write_checkpoint_at(2, b"checkpointed").await;
            }
            CrashStep::AfterTruncate => {
                self.write_checkpoint_at(2, b"checkpointed").await;
                self.truncate_to(3).await;
            }
            CrashStep::AfterPrune => {
                self.write_checkpoint_at(2, b"checkpointed").await;
                self.truncate_to(3).await;
                self.prune_checkpoint_at(1).await;
            }
        }

        self.recover().await.expect("recover")
    }

    async fn recover(&self) -> Result<RestoredState, SubstrateError> {
        let kv = MemKv::default();
        let plan = crabka_gres_substrate::checkpoint::restore_latest_and_replay_tail(
            self.objects.as_ref(),
            "tenant-a",
            &kv,
            crabka_gres_substrate::checkpoint::RestoreTail {
                current_generation: 0,
                log_start: Some(*self.log_start.lock().await),
                committed_frames: tail_frames(),
                barrier_offset: 4,
            },
        )
        .await?;
        Ok(RestoredState {
            source: plan.restored_from,
            next_journal_seq: plan.replay.next_journal_seq,
            kv,
        })
    }

    async fn write_checkpoint_at(&self, covered_offset: i64, b_value: &[u8]) -> Manifest {
        self.write_checkpoint_for_generation_at(0, covered_offset, b_value)
            .await
    }

    async fn write_checkpoint_for_generation_at(
        &self,
        wal_generation: u64,
        covered_offset: i64,
        b_value: &[u8],
    ) -> Manifest {
        let kv = checkpoint_kv(b_value);
        crabka_gres_substrate::checkpoint::write_checkpoint(
            self.objects.as_ref(),
            "tenant-a",
            &kv,
            snapshot_at(wal_generation, covered_offset, 1),
            DEFAULT_PART_MAX_SIZE,
        )
        .await
        .expect("checkpoint")
    }

    async fn write_zombie_checkpoint_at(&self, covered_offset: i64) {
        let kv = checkpoint_kv(b"zombie-old");
        crabka_gres_substrate::checkpoint::write_checkpoint(
            self.objects.as_ref(),
            "tenant-a",
            &kv,
            snapshot_at(0, covered_offset, 0),
            DEFAULT_PART_MAX_SIZE,
        )
        .await
        .expect("zombie checkpoint");
    }

    async fn write_parts_without_manifest_at(&self, covered_offset: i64) {
        let dir = ckpt_dir("tenant-a", 0, covered_offset, 1);
        let bytes = CheckpointPart::new(vec![
            (b"a".to_vec(), b"base".to_vec()),
            (b"b".to_vec(), b"checkpointed".to_vec()),
        ])
        .encode();
        self.objects
            .put(&part_key(&dir, 0), bytes)
            .await
            .expect("put part");
    }

    async fn truncate_to(&self, offset: i64) {
        *self.log_start.lock().await = offset;
    }

    async fn delete_manifest_at(&self, covered_offset: i64) {
        let key = manifest_key(&ckpt_dir("tenant-a", 0, covered_offset, 1));
        self.objects.delete(&key).await.expect("delete manifest");
    }

    async fn prune_checkpoint_at(&self, covered_offset: i64) {
        let dir = ckpt_dir("tenant-a", 0, covered_offset, 1);
        for object in self.objects.list(&dir).await.expect("list checkpoint") {
            self.objects
                .delete(&object.key)
                .await
                .expect("delete object");
        }
    }
}

fn checkpoint_kv(b_value: &[u8]) -> MemKv {
    let kv = MemKv::default();
    kv.put(b"a".to_vec(), b"base".to_vec()).expect("put a");
    kv.put(b"b".to_vec(), b_value.to_vec()).expect("put b");
    kv
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

fn barrier_frame() -> WalFrame {
    WalFrame {
        journal_seq: u64::MAX,
        ops: Vec::new(),
    }
}

fn tail_frames() -> Vec<crabka_gres_substrate::ReplayItem> {
    vec![
        item(0, 0, b"a", b"pre-checkpoint"),
        item(1, 1, b"b", b"previous-tail"),
        item(2, 2, b"b", b"checkpointed"),
        item(3, 3, b"c", b"after-new"),
        barrier(4),
    ]
}

fn snapshot_at(
    wal_generation: u64,
    covered_offset: i64,
    producer_epoch: i16,
) -> CheckpointSnapshot {
    CheckpointSnapshot {
        covered_offset,
        journal_seq: u64::try_from(covered_offset + 1).expect("journal seq"),
        producer_epoch,
        wal_generation,
        garbage_horizon_xid: 0,
    }
}

fn item(
    offset: i64,
    journal_seq: u64,
    key: &[u8],
    value: &[u8],
) -> crabka_gres_substrate::ReplayItem {
    crabka_gres_substrate::ReplayItem {
        offset,
        bytes: WalFrame {
            journal_seq,
            ops: vec![WriteOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        }
        .encode(),
    }
}

fn barrier(offset: i64) -> crabka_gres_substrate::ReplayItem {
    crabka_gres_substrate::ReplayItem {
        offset,
        bytes: WalFrame {
            journal_seq: u64::MAX,
            ops: Vec::new(),
        }
        .encode(),
    }
}
