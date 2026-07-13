//! Authoritative, non-mutating committed-fold snapshots.

use std::collections::BTreeMap;

use crabka_pgkv::{Kv, MemKv, WriteOp};

use crate::{
    SubstrateError, WalFrame,
    checkpoint::{CheckpointStore, Manifest, ManifestValidation, ckpt_prefix},
    follower::CommittedEndSampler,
    frame::BARRIER_SEQ,
    recovery::{CommittedWalReader, LiveRecoveryConfig, live_committed_reader},
};

/// Read-only witness for the range WAL generation.
#[async_trait::async_trait]
pub trait GenerationWitness: Send + Sync {
    /// Return the currently authoritative WAL generation.
    async fn current_generation(&self) -> Result<u64, SubstrateError>;
}

/// Optional raw-key projection applied consistently to checkpoint and WAL input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldProjection {
    /// Include every key.
    All,
    /// Include keys beginning with this byte prefix.
    Prefix(Vec<u8>),
    /// Include `start <= key < end`.
    Interval { start: Vec<u8>, end: Vec<u8> },
}

impl FoldProjection {
    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::All => true,
            Self::Prefix(prefix) => key.starts_with(prefix),
            Self::Interval { start, end } => start.as_slice() <= key && key < end.as_slice(),
        }
    }
}

/// Hard resource limits for an isolated fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl Default for FoldLimits {
    fn default() -> Self {
        Self {
            max_records: 1_000_000,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Durable checkpoint identity used by a fold snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldCheckpointIdentity {
    pub manifest_key: String,
    pub covered_offset: i64,
    pub journal_seq: u64,
    pub producer_epoch: i16,
    pub wal_generation: u64,
}

/// Auditable input accounting for a fold snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldProvenance {
    pub wal_generation: u64,
    pub replay_start_offset: i64,
    pub replayed_records: u64,
    pub checkpoint_pairs: u64,
}

/// Exact committed fold at one sampled WAL offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedFoldSnapshot {
    pub sample_offset: i64,
    pub checkpoint: Option<FoldCheckpointIdentity>,
    pub records: Vec<(Vec<u8>, Vec<u8>)>,
    /// Last durable source for each record, aligned with `records`.
    pub record_sources: Vec<FoldRecordSource>,
    pub provenance: FoldProvenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FoldRecordSource {
    pub offset: i64,
    pub journal_seq: u64,
}

/// Dependencies and constraints for [`committed_fold_snapshot`].
pub struct FoldSnapshotRequest<'a> {
    pub tenant: &'a str,
    pub generation: u64,
    pub checkpoints: Option<&'a dyn CheckpointStore>,
    pub wal: &'a dyn CommittedWalReader,
    pub sampler: &'a dyn CommittedEndSampler,
    pub generation_witness: &'a dyn GenerationWitness,
    pub projection: FoldProjection,
    pub limits: FoldLimits,
}

/// Build an isolated, authoritative fold without touching writer or retention state.
pub async fn committed_fold_snapshot(
    request: &FoldSnapshotRequest<'_>,
) -> Result<CommittedFoldSnapshot, SubstrateError> {
    assert_generation(request).await?;
    let sample = request.sampler.committed_end_after_call_begins().await?;
    let log_start = request.wal.log_start_offset().await?.unwrap_or(0);
    let kv = MemKv::default();
    let restored = load_checkpoint(request, sample, log_start, &kv).await?;
    let (checkpoint, checkpoint_pairs, replay_start, mut expected) =
        restored.map_or((None, 0, 0, 0), |(identity, pairs)| {
            let start = identity.covered_offset.saturating_add(1);
            let expected = identity.journal_seq;
            (Some(identity), pairs, start, expected)
        });
    let mut record_sources = BTreeMap::new();
    if let Some(identity) = &checkpoint {
        for (key, _) in kv.scan_prefix(b"")? {
            record_sources.insert(
                key,
                FoldRecordSource {
                    offset: identity.covered_offset,
                    journal_seq: identity.journal_seq,
                },
            );
        }
    }
    if checkpoint.is_none() && log_start > 0 {
        return Err(SubstrateError::PrunedHistory {
            log_start,
            sample_offset: sample,
        });
    }
    if log_start > replay_start {
        return Err(SubstrateError::PrunedHistory {
            log_start,
            sample_offset: sample,
        });
    }

    let mut replayed = 0_u64;
    if replay_start <= sample {
        let mut items = request.wal.committed_from(replay_start).await?;
        items.retain(|item| item.offset <= sample);
        if items.len() > request.limits.max_records {
            return Err(SubstrateError::FoldLimit(format!(
                "{} WAL records exceeds {}",
                items.len(),
                request.limits.max_records
            )));
        }
        let wal_bytes = items
            .iter()
            .try_fold(0_usize, |total, item| total.checked_add(item.bytes.len()))
            .ok_or_else(|| SubstrateError::FoldLimit("WAL byte count overflow".into()))?;
        if wal_bytes > request.limits.max_bytes {
            return Err(SubstrateError::FoldLimit(format!(
                "{wal_bytes} WAL bytes exceeds {}",
                request.limits.max_bytes
            )));
        }
        items.sort_by_key(|item| item.offset);
        if items.last().map(|item| item.offset) != Some(sample) {
            return Err(SubstrateError::Unavailable(format!(
                "committed fold did not reach sampled offset {sample}"
            )));
        }
        let mut previous_offset = replay_start.saturating_sub(1);
        for item in items {
            if item.offset <= previous_offset {
                return Err(SubstrateError::Unavailable(
                    "committed WAL offsets are not strictly increasing".into(),
                ));
            }
            previous_offset = item.offset;
            let frame = WalFrame::decode(&item.bytes)?;
            if frame.journal_seq == BARRIER_SEQ {
                continue;
            }
            if frame.journal_seq != expected {
                return Err(SubstrateError::SequenceGap {
                    expected,
                    found: frame.journal_seq,
                    offset: item.offset,
                });
            }
            let ops = projected_ops(&frame.ops, &request.projection);
            kv.write_batch(&ops)?;
            for op in &ops {
                match op {
                    WriteOp::Put { key, .. } | WriteOp::ConditionalPut { key, .. } => {
                        record_sources.insert(
                            key.clone(),
                            FoldRecordSource {
                                offset: item.offset,
                                journal_seq: frame.journal_seq,
                            },
                        );
                    }
                    WriteOp::Delete { key } => {
                        record_sources.remove(key);
                    }
                }
            }
            expected = expected
                .checked_add(1)
                .ok_or_else(|| SubstrateError::Frame("journal sequence exhausted".into()))?;
            replayed += 1;
        }
    }
    assert_generation(request).await?;
    let records = match &request.projection {
        FoldProjection::All => kv.scan_prefix(b"")?,
        FoldProjection::Prefix(prefix) => kv.scan_prefix(prefix)?,
        FoldProjection::Interval { start, end } => kv.scan_range(start, end)?,
    };
    enforce_limits(&records, request.limits)?;
    let sources = records
        .iter()
        .map(|(key, _)| {
            record_sources.get(key).copied().ok_or_else(|| {
                SubstrateError::Unavailable("committed fold record has no durable source".into())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CommittedFoldSnapshot {
        sample_offset: sample,
        checkpoint,
        records,
        record_sources: sources,
        provenance: FoldProvenance {
            wal_generation: request.generation,
            replay_start_offset: replay_start,
            replayed_records: replayed,
            checkpoint_pairs,
        },
    })
}

/// Build an authoritative fold from the live Kafka/checkpoint configuration.
///
/// The committed end is sampled exactly once and the private Kafka reader is
/// bounded to that sample, so callers cannot accidentally combine identities
/// from different durable instants.
pub async fn committed_fold_snapshot_live(
    config: &LiveRecoveryConfig,
    generation_witness: &dyn GenerationWitness,
    projection: FoldProjection,
    limits: FoldLimits,
) -> Result<CommittedFoldSnapshot, SubstrateError> {
    let sampler = crate::LiveCommittedEndSampler::new(config.clone());
    let sample = sampler.committed_end_after_call_begins().await?;
    committed_fold_snapshot_live_at(config, sample, generation_witness, projection, limits).await
}

/// Build a live fold at an already selected committed snapshot offset.
pub async fn committed_fold_snapshot_live_at(
    config: &LiveRecoveryConfig,
    sample: i64,
    generation_witness: &dyn GenerationWitness,
    projection: FoldProjection,
    limits: FoldLimits,
) -> Result<CommittedFoldSnapshot, SubstrateError> {
    let current = crate::LiveCommittedEndSampler::new(config.clone())
        .committed_end_after_call_begins()
        .await?;
    if sample > current {
        return Err(SubstrateError::Unavailable(format!(
            "requested fold snapshot {sample} is newer than committed end {current}"
        )));
    }
    let wal = live_committed_reader(config, sample).await?;
    let fixed = FixedEndSampler(sample);
    committed_fold_snapshot(&FoldSnapshotRequest {
        tenant: config.tenant.as_str(),
        generation: config.wal_generation,
        checkpoints: config
            .checkpoints
            .as_ref()
            .map(|value| value.store.as_ref()),
        wal: &wal,
        sampler: &fixed,
        generation_witness,
        projection,
        limits,
    })
    .await
}

struct FixedEndSampler(i64);

#[async_trait::async_trait]
impl CommittedEndSampler for FixedEndSampler {
    async fn committed_end_after_call_begins(&self) -> Result<i64, SubstrateError> {
        Ok(self.0)
    }
}

async fn assert_generation(request: &FoldSnapshotRequest<'_>) -> Result<(), SubstrateError> {
    if request.generation_witness.current_generation().await? != request.generation {
        return Err(SubstrateError::Fenced);
    }
    Ok(())
}

async fn load_checkpoint(
    request: &FoldSnapshotRequest<'_>,
    sample: i64,
    log_start: i64,
    kv: &MemKv,
) -> Result<Option<(FoldCheckpointIdentity, u64)>, SubstrateError> {
    let Some(store) = request.checkpoints else {
        return Ok(None);
    };
    let mut candidates = Vec::new();
    for object in store.list(&ckpt_prefix(request.tenant)).await? {
        if !object.key.ends_with("/MANIFEST") {
            continue;
        }
        let bytes = store.get(&object.key).await?;
        let manifest = Manifest::decode(&bytes)?;
        if manifest.tenant != request.tenant || manifest.wal_generation != request.generation {
            continue;
        }
        if manifest.covered_offset <= sample {
            candidates.push((object.key, manifest));
        }
    }
    candidates.sort_by_key(|(_, manifest)| (manifest.covered_offset, manifest.producer_epoch));
    let Some((manifest_key, manifest)) = candidates.pop() else {
        return Ok(None);
    };
    let manifest_bytes = usize::try_from(manifest.total_bytes)
        .map_err(|_| SubstrateError::FoldLimit("checkpoint byte count exceeds usize".into()))?;
    if manifest_bytes > request.limits.max_bytes {
        return Err(SubstrateError::FoldLimit(format!(
            "{manifest_bytes} checkpoint bytes exceeds {}",
            request.limits.max_bytes
        )));
    }
    let mut encoded = BTreeMap::new();
    for part in &manifest.parts {
        encoded.insert(part.name.clone(), store.get(&part.name).await?);
    }
    let parts = manifest.validate(&ManifestValidation {
        tenant: request.tenant,
        wal_generation: request.generation,
        log_start: Some(log_start),
        parts_by_name: &encoded,
    })?;
    let mut pairs = 0_u64;
    for (key, value) in parts.into_iter().flat_map(|part| part.pairs) {
        if request.projection.contains(&key) {
            kv.put(key, value)?;
            pairs += 1;
        }
    }
    Ok(Some((
        FoldCheckpointIdentity {
            manifest_key,
            covered_offset: manifest.covered_offset,
            journal_seq: manifest.journal_seq,
            producer_epoch: manifest.producer_epoch,
            wal_generation: manifest.wal_generation,
        },
        pairs,
    )))
}

fn projected_ops(ops: &[WriteOp], projection: &FoldProjection) -> Vec<WriteOp> {
    ops.iter()
        .filter(|op| match op {
            WriteOp::Put { key, .. }
            | WriteOp::ConditionalPut { key, .. }
            | WriteOp::Delete { key } => projection.contains(key),
        })
        .cloned()
        .collect()
}

fn enforce_limits(
    records: &[(Vec<u8>, Vec<u8>)],
    limits: FoldLimits,
) -> Result<(), SubstrateError> {
    if records.len() > limits.max_records {
        return Err(SubstrateError::FoldLimit(format!(
            "{} records exceeds {}",
            records.len(),
            limits.max_records
        )));
    }
    let bytes = records
        .iter()
        .try_fold(0_usize, |sum, (key, value)| {
            sum.checked_add(key.len())
                .and_then(|sum| sum.checked_add(value.len()))
        })
        .ok_or_else(|| SubstrateError::FoldLimit("record byte count overflow".into()))?;
    if bytes > limits.max_bytes {
        return Err(SubstrateError::FoldLimit(format!(
            "{bytes} bytes exceeds {}",
            limits.max_bytes
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crabka_pgkv::{Kv, MemKv, WriteOp};

    use super::*;
    use crate::{
        GroupCommitRequest, InMemoryWalLog, TransactionalWalWriter, WalFrame, WriterGeneration,
        checkpoint::{CheckpointSnapshot, InMemoryCheckpointStore, write_checkpoint},
    };

    struct Witness(AtomicU64);

    #[async_trait::async_trait]
    impl GenerationWitness for Witness {
        async fn current_generation(&self) -> Result<u64, crate::SubstrateError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    async fn append(log: &InMemoryWalLog, sequence: u64, key: &[u8], value: &[u8]) {
        log.commit_group(GroupCommitRequest {
            generation: WriterGeneration(0),
            frames: vec![WalFrame {
                journal_seq: sequence,
                ops: vec![WriteOp::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }],
            }],
        })
        .await
        .expect("append");
    }

    #[tokio::test]
    async fn checkpoint_and_tail_equal_the_committed_fold() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"a", b"old").await;
        let objects = InMemoryCheckpointStore::shared();
        let checkpoint = MemKv::default();
        checkpoint
            .put(b"a".to_vec(), b"old".to_vec())
            .expect("seed");
        write_checkpoint(
            objects.as_ref(),
            "tenant/r1",
            &checkpoint,
            CheckpointSnapshot {
                covered_offset: 0,
                journal_seq: 1,
                producer_epoch: 3,
                wal_generation: 0,
                garbage_horizon_xid: 0,
            },
            1024,
        )
        .await
        .expect("checkpoint");
        append(&log, 1, b"a", b"new").await;
        append(&log, 2, b"b", b"tail").await;

        let sampler = FixedSampler { sample: 2 };
        let result = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: Some(objects.as_ref()),
            wal: log.as_ref(),
            sampler: &sampler,
            generation_witness: &Witness(AtomicU64::new(0)),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect("snapshot");

        assert_eq!(result.sample_offset, 2);
        assert_eq!(
            result.records,
            vec![
                (b"a".to_vec(), b"new".to_vec()),
                (b"b".to_vec(), b"tail".to_vec())
            ]
        );
        assert_eq!(result.provenance.replayed_records, 2);
        assert_eq!(
            result.record_sources,
            vec![
                FoldRecordSource {
                    offset: 1,
                    journal_seq: 1
                },
                FoldRecordSource {
                    offset: 2,
                    journal_seq: 2
                },
            ]
        );
        assert!(result.checkpoint.is_some());
    }

    #[tokio::test]
    async fn no_checkpoint_replays_a_genesis_log_and_excludes_later_appends() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"a", b"one").await;
        let sampler = FixedSampler { sample: 0 };
        append(&log, 1, b"late", b"excluded").await;
        let result = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &sampler,
            generation_witness: &Witness(AtomicU64::new(0)),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect("snapshot");
        assert_eq!(result.records, vec![(b"a".to_vec(), b"one".to_vec())]);
        assert!(result.checkpoint.is_none());
    }

    #[tokio::test]
    async fn no_checkpoint_after_pruning_fails_explicitly() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"gone", b"history").await;
        crate::checkpoint::CheckpointWalPruner::delete_records(
            log.as_ref(),
            &[crabka_client_admin::DeleteRecordsOp {
                topic: "ignored".into(),
                partition: 0,
                offset: 1,
            }],
        )
        .await
        .expect("prune");
        let error = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &FixedSampler { sample: 0 },
            generation_witness: &Witness(AtomicU64::new(0)),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect_err("pruned history");
        assert!(matches!(
            error,
            crate::SubstrateError::PrunedHistory {
                log_start: 1,
                sample_offset: 0
            }
        ));
    }

    #[tokio::test]
    async fn checkpoint_exactly_at_sample_needs_no_tail_and_newer_checkpoint_is_ignored() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"a", b"sample").await;
        let objects = InMemoryCheckpointStore::shared();
        for (offset, value, sequence) in
            [(0, b"sample".as_slice(), 1), (1, b"future".as_slice(), 2)]
        {
            let checkpoint = MemKv::default();
            checkpoint.put(b"a".to_vec(), value.to_vec()).expect("seed");
            write_checkpoint(
                objects.as_ref(),
                "tenant/r1",
                &checkpoint,
                CheckpointSnapshot {
                    covered_offset: offset,
                    journal_seq: sequence,
                    producer_epoch: 0,
                    wal_generation: 0,
                    garbage_horizon_xid: 0,
                },
                1024,
            )
            .await
            .expect("checkpoint");
        }
        let result = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: Some(objects.as_ref()),
            wal: log.as_ref(),
            sampler: &FixedSampler { sample: 0 },
            generation_witness: &Witness(AtomicU64::new(0)),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect("snapshot");
        assert_eq!(result.records, vec![(b"a".to_vec(), b"sample".to_vec())]);
        assert_eq!(result.checkpoint.expect("checkpoint").covered_offset, 0);
        assert_eq!(result.provenance.replayed_records, 0);
    }

    #[tokio::test]
    async fn generation_mismatch_fails_before_reading() {
        let log = InMemoryWalLog::shared();
        let error = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &FixedSampler { sample: -1 },
            generation_witness: &Witness(AtomicU64::new(1)),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect_err("fenced");
        assert!(matches!(error, crate::SubstrateError::Fenced));
    }

    #[tokio::test]
    async fn prefix_projection_and_output_limits_are_enforced() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"yes/a", b"one").await;
        append(&log, 1, b"no/b", b"two").await;
        let witness = Witness(AtomicU64::new(0));
        let projected = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &FixedSampler { sample: 1 },
            generation_witness: &witness,
            projection: FoldProjection::Prefix(b"yes/".to_vec()),
            limits: FoldLimits::default(),
        })
        .await
        .expect("projection");
        assert_eq!(
            projected.records,
            vec![(b"yes/a".to_vec(), b"one".to_vec())]
        );
        let error = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &FixedSampler { sample: 1 },
            generation_witness: &witness,
            projection: FoldProjection::All,
            limits: FoldLimits {
                max_records: 1,
                max_bytes: 100,
            },
        })
        .await
        .expect_err("limit");
        assert!(matches!(error, crate::SubstrateError::FoldLimit(_)));
    }

    #[tokio::test]
    async fn uncommitted_frames_are_ignored_and_generation_drift_fails_closed() {
        let log = InMemoryWalLog::shared();
        append(&log, 0, b"committed", b"yes").await;
        log.append_unacked(
            WriterGeneration(0),
            &[WalFrame {
                journal_seq: 1,
                ops: vec![WriteOp::Put {
                    key: b"in-doubt".to_vec(),
                    value: b"no".to_vec(),
                }],
            }],
        )
        .await
        .expect("uncommitted");
        let generation = AtomicU64::new(0);
        let sampler = DriftingSampler {
            sample: 0,
            generation: &generation,
        };
        let error = committed_fold_snapshot(&FoldSnapshotRequest {
            tenant: "tenant/r1",
            generation: 0,
            checkpoints: None,
            wal: log.as_ref(),
            sampler: &sampler,
            generation_witness: &WitnessRef(&generation),
            projection: FoldProjection::All,
            limits: FoldLimits::default(),
        })
        .await
        .expect_err("generation drift");
        assert!(matches!(error, crate::SubstrateError::Fenced));
    }

    struct WitnessRef<'a>(&'a AtomicU64);
    #[async_trait::async_trait]
    impl GenerationWitness for WitnessRef<'_> {
        async fn current_generation(&self) -> Result<u64, crate::SubstrateError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }

    struct DriftingSampler<'a> {
        sample: i64,
        generation: &'a AtomicU64,
    }
    #[async_trait::async_trait]
    impl crate::follower::CommittedEndSampler for DriftingSampler<'_> {
        async fn committed_end_after_call_begins(&self) -> Result<i64, crate::SubstrateError> {
            self.generation.store(1, Ordering::SeqCst);
            Ok(self.sample)
        }
    }

    struct FixedSampler {
        sample: i64,
    }
    #[async_trait::async_trait]
    impl crate::follower::CommittedEndSampler for FixedSampler {
        async fn committed_end_after_call_begins(&self) -> Result<i64, crate::SubstrateError> {
            Ok(self.sample)
        }
    }
}
