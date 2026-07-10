use assert2::assert;
use crabka_gres_substrate::{
    CheckpointPart, DEFAULT_PART_MAX_BYTES, Manifest, SubstrateError, WalFrame,
    checkpoint::{CheckpointSnapshot, CheckpointStore, InMemoryCheckpointStore},
    ckpt_dir, manifest_key, part_key,
};
use crabka_pgkv::{Kv, MemKv, WriteOp};

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
