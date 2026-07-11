use assert2::assert;
use crabka_gres_substrate::{
    CheckpointPart, DEFAULT_PART_MAX_BYTES, GroupCommitRequest, InMemoryWalLog, Manifest,
    SubstrateError, TransactionalWalWriter, WalFrame, WriterGeneration, apply_frame,
    checkpoint::{
        CheckpointConfig, CheckpointFailpoint, CheckpointService, CheckpointServiceStep,
        CheckpointSnapshot, CheckpointStats, CheckpointStore, CheckpointTrigger,
        InMemoryCheckpointStore,
    },
    ckpt_dir, manifest_key, part_key,
};

#[test]
fn production_service_exposes_compile_time_test_only_boundaries() {
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

#[tokio::test]
async fn production_zombie_service_cannot_supersede_successor_manifest() {
    let harness = ProductionCrashHarness::new().await;
    harness
        .run_checkpoint(snapshot_at(0, 2, 2), None)
        .await
        .expect("successor checkpoint");
    let zombie_kv = std::sync::Arc::new(MemKv::default());
    zombie_kv
        .put(b"a".to_vec(), b"zombie-stale".to_vec())
        .expect("zombie state");
    harness
        .run_checkpoint_with_kv(snapshot_at(0, 1, 0), None, zombie_kv)
        .await
        .expect("older compute finishes checkpoint step");

    let restored = harness.recover().await.expect("recover newest checkpoint");
    assert!(restored.source.expect("source").covered_offset == 2);
    assert!(restored.value(b"a") == Some(b"base".to_vec()));
    assert!(restored.value(b"c") == Some(b"third".to_vec()));
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
            24,
            2,
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
        service
            .checkpoint(snapshot, CheckpointTrigger::Manual)
            .await
            .map(|_| ())
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
        let key = manifest_key(&ckpt_dir("tenant-production-crash", 0, 2, 1));
        self.objects
            .delete(&key)
            .await
            .expect("delete newest manifest");
    }
}
use crabka_pgkv::{Kv, MemKv, SnapshotKv, WriteOp};

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
            DEFAULT_PART_MAX_BYTES,
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
            DEFAULT_PART_MAX_BYTES,
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
