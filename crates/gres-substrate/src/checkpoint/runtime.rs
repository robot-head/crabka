//! Checkpoint write, restore, tail-replay, and WAL prune planning.

use std::collections::BTreeMap;

use crabka_client_admin::DeleteRecordsOp;
use crabka_pgkv::{KvError, KvPair, KvSnapshot, RestoreKv, SnapshotKv};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    CheckpointFilter, CheckpointPart, CheckpointStore, Manifest, ManifestValidation, PartEntry,
    RewriteDecision, ckpt_dir, ckpt_prefix, manifest_key, part_key,
};
use crate::{
    error::SubstrateError,
    replay::{
        ReplayItem, ReplayOutcome, replay_committed_frames_from,
        replay_committed_frames_from_filtered, replay_committed_frames_from_table_transfer,
    },
    transfer::{TableTransferSelector, TableTransferStats},
};

/// Snapshot metadata captured between committed WAL groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointSnapshot {
    /// WAL offset covered by the snapshot.
    pub covered_offset: i64,
    /// Next journal sequence after the snapshot instant.
    pub journal_seq: u64,
    /// Producer epoch that created the checkpoint.
    pub producer_epoch: i16,
    /// WAL generation containing `covered_offset`.
    pub wal_generation: u64,
    /// Oldest XID retained for post-checkpoint garbage safety.
    pub garbage_horizon_xid: u64,
}

/// Successful restore source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoredFrom {
    /// WAL generation represented by the checkpoint.
    pub wal_generation: u64,
    /// WAL offset covered by the checkpoint.
    pub covered_offset: i64,
    /// Next expected journal sequence after restore.
    pub journal_seq: u64,
}

/// Planned WAL and object pruning after a durable checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalPrunePlan {
    /// Kafka `DeleteRecords` calls that safely advance log start.
    pub delete_records: Vec<DeleteRecordsOp>,
    /// Checkpoint objects eligible for deletion.
    pub delete_object_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointPinRecord {
    operation_id: String,
    manifest_key: String,
    wal_generation: u64,
    covered_offset: i64,
}

fn checkpoint_pin_prefix(tenant: &str) -> String {
    format!("gres/{tenant}/pins/")
}

fn checkpoint_pin_key(tenant: &str, operation_id: &str) -> String {
    let digest = Sha256::digest(operation_id.as_bytes());
    format!("{}{}", checkpoint_pin_prefix(tenant), hex::encode(digest))
}

pub(super) async fn pin_checkpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    operation_id: &str,
    manifest_key: &str,
    wal_generation: u64,
    covered_offset: i64,
) -> Result<(), SubstrateError> {
    if operation_id.is_empty() {
        return Err(SubstrateError::Checkpoint(
            "checkpoint pin operation id must not be empty".into(),
        ));
    }
    let expected_prefix = ckpt_prefix(tenant);
    if !manifest_key.starts_with(&expected_prefix) || !manifest_key.ends_with("/MANIFEST") {
        return Err(SubstrateError::Checkpoint(
            "checkpoint pin manifest key is outside its tenant namespace".into(),
        ));
    }
    let record = CheckpointPinRecord {
        operation_id: operation_id.to_owned(),
        manifest_key: manifest_key.to_owned(),
        wal_generation,
        covered_offset,
    };
    let bytes = serde_json::to_vec(&record)
        .map_err(|error| SubstrateError::Checkpoint(format!("checkpoint pin: {error}")))?;
    store
        .put(&checkpoint_pin_key(tenant, operation_id), bytes)
        .await
}

pub(super) async fn unpin_checkpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    operation_id: &str,
) -> Result<(), SubstrateError> {
    store
        .delete(&checkpoint_pin_key(tenant, operation_id))
        .await
}

async fn checkpoint_pins(
    store: &dyn CheckpointStore,
    tenant: &str,
) -> Result<Vec<CheckpointPinRecord>, SubstrateError> {
    let mut pins = Vec::new();
    for object in store.list(&checkpoint_pin_prefix(tenant)).await? {
        let bytes = store.get(&object.key).await?;
        let pin = serde_json::from_slice::<CheckpointPinRecord>(&bytes)
            .map_err(|error| SubstrateError::Checkpoint(format!("checkpoint pin: {error}")))?;
        if object.key != checkpoint_pin_key(tenant, &pin.operation_id) {
            return Err(SubstrateError::Checkpoint(
                "checkpoint pin key does not match its operation id".into(),
            ));
        }
        pins.push(pin);
    }
    Ok(pins)
}

/// Remove checkpoint pins that are not backed by the one durable active operation.
///
/// # Errors
///
/// Returns an error when pin markers cannot be read, validated, or deleted.
pub async fn reconcile_checkpoint_pins(
    store: &dyn CheckpointStore,
    tenant: &str,
    active: Option<(&str, &str, i64)>,
) -> Result<(), SubstrateError> {
    for pin in checkpoint_pins(store, tenant).await? {
        let keep = active.is_some_and(|(operation_id, manifest_key, covered_offset)| {
            pin.operation_id == operation_id
                && pin.manifest_key == manifest_key
                && pin.covered_offset == covered_offset
        });
        if !keep {
            unpin_checkpoint(store, tenant, &pin.operation_id).await?;
        }
    }
    Ok(())
}

/// Public checkpoint manifest metadata safe for operator/control-plane decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMetadata {
    /// Tenant whose checkpoint was verified.
    pub tenant: String,
    /// WAL generation represented by the checkpoint.
    pub wal_generation: u64,
    /// WAL offset covered by the checkpoint.
    pub covered_offset: i64,
    /// Object key of the durable manifest.
    pub manifest_key: String,
    /// Manifest plus part object bytes.
    pub total_bytes: u64,
}

/// Conservative read-only planner adapter over verified durable checkpoint
/// metadata. Checkpoints are range/tenant scoped rather than per-table, so the
/// verified total is an upper-bound estimate for any table in that range.
impl crabka_pgexec::plan_dist::Stats for CheckpointMetadata {
    fn estimated_bytes(&self, _table_id: u64) -> Option<u64> {
        Some(self.total_bytes)
    }
}

#[cfg(test)]
mod planner_stats_tests {
    use crabka_pgexec::plan_dist::Stats;

    use super::CheckpointMetadata;

    #[test]
    fn verified_checkpoint_metadata_is_a_read_only_stats_source() {
        let metadata = CheckpointMetadata {
            tenant: "tenant-a".into(),
            wal_generation: 3,
            covered_offset: 17,
            manifest_key: "checkpoint/MANIFEST".into(),
            total_bytes: 4096,
        };
        assert_eq!(metadata.estimated_bytes(42), Some(4096));
    }
}

/// Restore plus replay output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePlan {
    /// Checkpoint used for restore, if any.
    pub restored_from: Option<RestoredFrom>,
    /// Replay outcome after applying the tail.
    pub replay: ReplayOutcome,
}

/// Result of restoring one table-transfer checkpoint closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransferRestore {
    /// Source checkpoint used for the transfer.
    pub restored_from: RestoredFrom,
    /// Reproducible selected-input statistics.
    pub stats: TableTransferStats,
}

/// WAL tail inputs used after checkpoint restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreTail {
    /// WAL generation currently being restored.
    pub current_generation: u64,
    /// Earliest retained WAL offset for the current generation.
    pub log_start: Option<i64>,
    /// Committed WAL frames available for tail replay.
    pub committed_frames: Vec<ReplayItem>,
    /// Highest committed WAL offset included in the tail replay barrier.
    pub barrier_offset: i64,
}

/// Stream a KV snapshot into checkpoint parts and write the manifest last.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn write_checkpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn SnapshotKv,
    snapshot: CheckpointSnapshot,
    part_max_bytes: usize,
) -> Result<Manifest, SubstrateError> {
    write_checkpoint_inner(
        store,
        tenant,
        kv.snapshot()?,
        snapshot,
        part_max_bytes,
        None,
    )
    .await
}

pub(crate) async fn write_captured_checkpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    kv_snapshot: Box<dyn KvSnapshot>,
    snapshot: CheckpointSnapshot,
    part_max_bytes: usize,
) -> Result<Manifest, SubstrateError> {
    write_checkpoint_inner(store, tenant, kv_snapshot, snapshot, part_max_bytes, None).await
}

#[cfg(feature = "checkpoint-test-hooks")]
pub(crate) async fn write_captured_checkpoint_with_failpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    kv_snapshot: Box<dyn KvSnapshot>,
    snapshot: CheckpointSnapshot,
    part_max_bytes: usize,
    failpoint: &super::CheckpointFailpoint,
) -> Result<Manifest, SubstrateError> {
    write_checkpoint_inner(
        store,
        tenant,
        kv_snapshot,
        snapshot,
        part_max_bytes,
        Some(failpoint),
    )
    .await
}

#[cfg(feature = "checkpoint-test-hooks")]
pub(crate) async fn write_checkpoint_with_failpoint(
    store: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn SnapshotKv,
    snapshot: CheckpointSnapshot,
    part_max_bytes: usize,
    failpoint: &super::CheckpointFailpoint,
) -> Result<Manifest, SubstrateError> {
    write_checkpoint_inner(
        store,
        tenant,
        kv.snapshot()?,
        snapshot,
        part_max_bytes,
        Some(failpoint),
    )
    .await
}

async fn write_checkpoint_inner(
    store: &dyn CheckpointStore,
    tenant: &str,
    mut kv_snapshot: Box<dyn KvSnapshot>,
    snapshot: CheckpointSnapshot,
    part_max_bytes: usize,
    #[cfg_attr(not(feature = "checkpoint-test-hooks"), allow(unused_variables))] failpoint: Option<
        &CheckpointFailpoint,
    >,
) -> Result<Manifest, SubstrateError> {
    let pairs = collect_snapshot_pairs(kv_snapshot.as_mut())?;
    let pairs = rewrite_snapshot_pairs(pairs, snapshot.garbage_horizon_xid)?;
    if let Some(error) = checkpoint_failure(failpoint, CheckpointServiceStep::BeforeParts) {
        return Err(error);
    }
    let dir = ckpt_dir(
        tenant,
        snapshot.wal_generation,
        snapshot.covered_offset,
        snapshot.producer_epoch,
    );
    let parts = CheckpointPart::split_at_target_size(pairs, part_max_bytes)?;
    let mut entries = Vec::with_capacity(parts.len());

    for (index, part) in parts.iter().enumerate() {
        let key = part_key(&dir, index);
        let bytes = part.encode();
        store.put(&key, bytes.clone()).await?;
        entries.push(PartEntry::from_encoded_part(key, &bytes)?);
        if let Some(error) = checkpoint_failure(failpoint, CheckpointServiceStep::PartsUploaded) {
            return Err(error);
        }
    }

    let manifest = Manifest::new(
        tenant.to_owned(),
        snapshot.covered_offset,
        snapshot.journal_seq,
        snapshot.producer_epoch,
        snapshot.wal_generation,
        entries,
    );
    store.put(&manifest_key(&dir), manifest.encode()?).await?;
    if let Some(error) = checkpoint_failure(failpoint, CheckpointServiceStep::ManifestWritten) {
        return Err(error);
    }
    Ok(manifest)
}

#[cfg(feature = "checkpoint-test-hooks")]
use super::{CheckpointFailpoint, CheckpointServiceStep};

#[cfg(not(feature = "checkpoint-test-hooks"))]
type CheckpointFailpoint = ();

#[cfg(not(feature = "checkpoint-test-hooks"))]
#[derive(Clone, Copy)]
enum CheckpointServiceStep {
    BeforeParts,
    PartsUploaded,
    ManifestWritten,
}

#[cfg(feature = "checkpoint-test-hooks")]
fn checkpoint_failure(
    failpoint: Option<&CheckpointFailpoint>,
    step: CheckpointServiceStep,
) -> Option<SubstrateError> {
    if failpoint.is_some_and(|hook| hook(step)) {
        return Some(SubstrateError::Checkpoint(format!(
            "test failpoint stopped checkpoint after {step:?}"
        )));
    }
    None
}

#[cfg(not(feature = "checkpoint-test-hooks"))]
fn checkpoint_failure(
    _failpoint: Option<&CheckpointFailpoint>,
    _step: CheckpointServiceStep,
) -> Option<SubstrateError> {
    None
}

/// Restore the newest valid checkpoint for `tenant`, skipping incomplete attempts.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    current_generation: u64,
    log_start: Option<i64>,
) -> Result<Option<RestoredFrom>, SubstrateError> {
    restore_latest_with_filter(
        objects,
        tenant,
        kv,
        current_generation,
        log_start,
        None,
        None,
    )
    .await
}

pub(crate) async fn restore_latest_at_or_before(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    current_generation: u64,
    log_start: Option<i64>,
    maximum_covered_offset: i64,
) -> Result<Option<RestoredFrom>, SubstrateError> {
    restore_latest_with_filter(
        objects,
        tenant,
        kv,
        current_generation,
        log_start,
        None,
        Some(maximum_covered_offset),
    )
    .await
}

/// Restore the newest valid checkpoint subset for `tenant`, skipping incomplete attempts.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest_filtered(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    current_generation: u64,
    log_start: Option<i64>,
    filter: CheckpointFilter,
) -> Result<Option<RestoredFrom>, SubstrateError> {
    restore_latest_with_filter(
        objects,
        tenant,
        kv,
        current_generation,
        log_start,
        Some(filter),
        None,
    )
    .await
}

/// Restore the newest valid checkpoint's durable closure for one ordinary table.
///
/// This deliberately does not publish a serving range. Callers must replay the
/// ordered WAL tail with the same `selector` before using the restored KV.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest_table_transfer(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    current_generation: u64,
    log_start: Option<i64>,
    selector: &mut TableTransferSelector,
) -> Result<Option<TableTransferRestore>, SubstrateError> {
    let mut manifest_keys = objects
        .list(&ckpt_prefix(tenant))
        .await?
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.ends_with("/MANIFEST"))
        .collect::<Vec<_>>();
    manifest_keys.sort();
    manifest_keys.reverse();

    let mut invalid_checkpoint = None;
    for manifest_key in manifest_keys {
        let manifest_bytes = match objects.get(&manifest_key).await {
            Ok(bytes) => bytes,
            Err(error) => {
                invalid_checkpoint = Some(error);
                continue;
            }
        };
        let manifest = match Manifest::decode(&manifest_bytes) {
            Ok(manifest)
                if manifest.tenant == tenant && manifest.wal_generation <= current_generation =>
            {
                manifest
            }
            Ok(_) => continue,
            Err(error) => {
                invalid_checkpoint = Some(error);
                continue;
            }
        };
        match restore_manifest_table_transfer(
            objects,
            kv,
            &manifest,
            current_generation,
            log_start,
            selector,
        )
        .await
        {
            Ok(restored) => return Ok(Some(restored)),
            Err(SubstrateError::Kv(KvError::RestoreTargetNotEmpty)) => {
                return Err(SubstrateError::Kv(KvError::RestoreTargetNotEmpty));
            }
            Err(error) => invalid_checkpoint = Some(error),
        }
    }
    match invalid_checkpoint {
        Some(error) => Err(error),
        None => Ok(None),
    }
}

async fn restore_latest_with_filter(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    current_generation: u64,
    log_start: Option<i64>,
    filter: Option<CheckpointFilter>,
    maximum_covered_offset: Option<i64>,
) -> Result<Option<RestoredFrom>, SubstrateError> {
    let mut manifest_keys = objects
        .list(&ckpt_prefix(tenant))
        .await?
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.ends_with("/MANIFEST"))
        .collect::<Vec<_>>();
    manifest_keys.sort();
    manifest_keys.reverse();

    let mut checksum_error = None;
    let mut invalid_checkpoint = None;
    for manifest_object in manifest_keys {
        let manifest_bytes = match objects.get(&manifest_object).await {
            Ok(bytes) => bytes,
            Err(error) => {
                invalid_checkpoint = Some(SubstrateError::Checkpoint(format!(
                    "checkpoint manifest {manifest_object} could not be read: {error}"
                )));
                continue;
            }
        };
        let manifest = match Manifest::decode(&manifest_bytes) {
            Ok(manifest) => manifest,
            Err(error) => {
                invalid_checkpoint = Some(SubstrateError::Checkpoint(format!(
                    "checkpoint manifest {manifest_object} could not be decoded: {error}"
                )));
                continue;
            }
        };
        if manifest.tenant != tenant {
            invalid_checkpoint = Some(SubstrateError::Checkpoint(format!(
                "checkpoint manifest {manifest_object} belongs to tenant {}",
                manifest.tenant
            )));
            continue;
        }
        if manifest.wal_generation > current_generation {
            invalid_checkpoint = Some(SubstrateError::Checkpoint(format!(
                "checkpoint manifest {manifest_object} generation {} is newer than current generation {current_generation}",
                manifest.wal_generation
            )));
            continue;
        }
        if manifest.wal_generation == current_generation
            && maximum_covered_offset.is_some_and(|maximum| manifest.covered_offset > maximum)
        {
            continue;
        }
        match restore_manifest(
            objects,
            kv,
            &manifest,
            current_generation,
            log_start,
            filter.clone(),
        )
        .await
        {
            Ok(restored) => return Ok(Some(restored)),
            Err(SubstrateError::ChecksumMismatch { part }) => {
                checksum_error = Some(SubstrateError::ChecksumMismatch { part });
            }
            Err(SubstrateError::TornTruncation {
                log_start,
                newest_manifest,
            }) => {
                return Err(SubstrateError::TornTruncation {
                    log_start,
                    newest_manifest,
                });
            }
            Err(SubstrateError::Kv(KvError::RestoreTargetNotEmpty)) => {
                return Err(SubstrateError::Kv(KvError::RestoreTargetNotEmpty));
            }
            Err(error) => invalid_checkpoint = Some(error),
        }
    }

    if let Some(error) = checksum_error {
        return Err(error);
    }
    if let Some(error) = invalid_checkpoint {
        return Err(error);
    }
    Ok(None)
}

/// Return the newest manifest whose parts are present and checksummed.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn latest_checkpoint_metadata(
    objects: &dyn CheckpointStore,
    tenant: &str,
    current_generation: u64,
    log_start: Option<i64>,
) -> Result<Option<CheckpointMetadata>, SubstrateError> {
    let mut manifest_keys = objects
        .list(&ckpt_prefix(tenant))
        .await?
        .into_iter()
        .map(|object| object.key)
        .filter(|key| key.ends_with("/MANIFEST"))
        .collect::<Vec<_>>();
    manifest_keys.sort();
    manifest_keys.reverse();

    for manifest_key in manifest_keys {
        let Ok(manifest_bytes) = objects.get(&manifest_key).await else {
            continue;
        };
        let Ok(manifest) = Manifest::decode(&manifest_bytes) else {
            continue;
        };
        if manifest.tenant != tenant || manifest.wal_generation > current_generation {
            continue;
        }
        let Ok(parts_by_name) = load_manifest_parts(objects, &manifest).await else {
            continue;
        };
        if manifest
            .validate(&ManifestValidation {
                tenant,
                wal_generation: manifest.wal_generation,
                log_start: if manifest.wal_generation == current_generation {
                    log_start
                } else {
                    None
                },
                parts_by_name: &parts_by_name,
            })
            .is_err()
        {
            continue;
        }
        let part_bytes = manifest.parts.iter().try_fold(0_u64, |total, part| {
            total
                .checked_add(
                    parts_by_name
                        .get(&part.name)
                        .map_or(0, |bytes| u64::try_from(bytes.len()).unwrap_or(u64::MAX)),
                )
                .ok_or_else(|| SubstrateError::Checkpoint("checkpoint byte size overflow".into()))
        })?;
        let total_bytes = part_bytes
            .checked_add(u64::try_from(manifest_bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| SubstrateError::Checkpoint("checkpoint byte size overflow".into()))?;
        return Ok(Some(CheckpointMetadata {
            tenant: manifest.tenant,
            wal_generation: manifest.wal_generation,
            covered_offset: manifest.covered_offset,
            manifest_key,
            total_bytes,
        }));
    }

    Ok(None)
}

/// Restore the latest checkpoint and replay committed WAL frames after it.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest_and_replay_tail(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    tail: RestoreTail,
) -> Result<RestorePlan, SubstrateError> {
    let restored_from =
        restore_latest(objects, tenant, kv, tail.current_generation, tail.log_start).await?;
    let (replay_start, expected) = restored_from.map_or((0, 0), |restored| {
        if restored.wal_generation == tail.current_generation {
            (
                restored.covered_offset.saturating_add(1),
                restored.journal_seq,
            )
        } else {
            (0, 0)
        }
    });
    let replay = replay_committed_frames_from(
        kv,
        tail.committed_frames,
        tail.barrier_offset,
        replay_start,
        expected,
    )?;
    Ok(RestorePlan {
        restored_from,
        replay,
    })
}

/// Restore a checkpoint subset and replay only tail mutations in that same interval.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest_filtered_and_replay_tail(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    tail: RestoreTail,
    filter: CheckpointFilter,
) -> Result<RestorePlan, SubstrateError> {
    let restored_from = restore_latest_filtered(
        objects,
        tenant,
        kv,
        tail.current_generation,
        tail.log_start,
        filter.clone(),
    )
    .await?;
    let (replay_start, expected) = restored_from.map_or((0, 0), |restored| {
        if restored.wal_generation == tail.current_generation {
            (
                restored.covered_offset.saturating_add(1),
                restored.journal_seq,
            )
        } else {
            (0, 0)
        }
    });
    let replay = replay_committed_frames_from_filtered(
        kv,
        tail.committed_frames,
        tail.barrier_offset,
        replay_start,
        expected,
        &filter,
    )?;
    Ok(RestorePlan {
        restored_from,
        replay,
    })
}

/// Restore a table-transfer checkpoint closure and replay its matching WAL tail.
///
/// The selector state established from the checkpoint is carried into replay,
/// preserving the closure when tail tuples introduce new XID dependencies.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_latest_table_transfer_and_replay_tail(
    objects: &dyn CheckpointStore,
    tenant: &str,
    kv: &dyn RestoreKv,
    tail: RestoreTail,
    selector: &mut TableTransferSelector,
) -> Result<(TableTransferRestore, ReplayOutcome), SubstrateError> {
    let restored = restore_latest_table_transfer(
        objects,
        tenant,
        kv,
        tail.current_generation,
        tail.log_start,
        selector,
    )
    .await?
    .ok_or_else(|| {
        SubstrateError::Unavailable("no valid checkpoint source for table transfer".into())
    })?;
    let (replay_start, expected) =
        if restored.restored_from.wal_generation == tail.current_generation {
            (
                restored.restored_from.covered_offset.saturating_add(1),
                restored.restored_from.journal_seq,
            )
        } else {
            (0, 0)
        };
    let replay = replay_committed_frames_from_table_transfer(
        kv,
        tail.committed_frames,
        tail.barrier_offset,
        replay_start,
        expected,
        selector,
    )?;
    Ok((restored, replay))
}

/// Restore the table-transfer closure from one caller-selected manifest and
/// replay its bounded WAL tail.
///
/// Unlike [`restore_latest_table_transfer_and_replay_tail`], this function
/// never lists checkpoint objects. The manifest key and covered offset form a
/// transfer boundary selected by the caller; a later checkpoint must not
/// change that boundary.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_table_transfer_from_manifest_and_replay_tail(
    objects: &dyn CheckpointStore,
    manifest_key: &str,
    tenant: &str,
    expected_covered_offset: i64,
    kv: &dyn RestoreKv,
    tail: RestoreTail,
    selector: &mut TableTransferSelector,
) -> Result<(TableTransferRestore, ReplayOutcome), SubstrateError> {
    let manifest_bytes = objects.get(manifest_key).await?;
    let manifest = Manifest::decode(&manifest_bytes)?;
    if manifest.tenant != tenant {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} belongs to tenant {}",
            manifest.tenant
        )));
    }
    if manifest.covered_offset != expected_covered_offset {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} covers offset {}, expected {expected_covered_offset}",
            manifest.covered_offset
        )));
    }
    if manifest.wal_generation != tail.current_generation {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} generation {} differs from transfer generation {}",
            manifest.wal_generation, tail.current_generation
        )));
    }

    let restored = restore_manifest_table_transfer(
        objects,
        kv,
        &manifest,
        tail.current_generation,
        tail.log_start,
        selector,
    )
    .await?;
    let replay = replay_committed_frames_from_table_transfer(
        kv,
        tail.committed_frames,
        tail.barrier_offset,
        restored.restored_from.covered_offset.saturating_add(1),
        restored.restored_from.journal_seq,
        selector,
    )?;
    Ok((restored, replay))
}

/// Restore one exact interval from a caller-selected manifest and replay the
/// same interval from its bounded committed tail.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn restore_filtered_from_manifest_and_replay_tail(
    objects: &dyn CheckpointStore,
    manifest_key: &str,
    tenant: &str,
    expected_covered_offset: i64,
    kv: &dyn RestoreKv,
    tail: RestoreTail,
    filter: CheckpointFilter,
) -> Result<RestorePlan, SubstrateError> {
    let manifest_bytes = objects.get(manifest_key).await?;
    let manifest = Manifest::decode(&manifest_bytes)?;
    if manifest.tenant != tenant {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} belongs to tenant {}",
            manifest.tenant
        )));
    }
    if manifest.covered_offset != expected_covered_offset {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} covers offset {}, expected {expected_covered_offset}",
            manifest.covered_offset
        )));
    }
    if manifest.wal_generation != tail.current_generation {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint manifest {manifest_key} generation {} differs from transfer generation {}",
            manifest.wal_generation, tail.current_generation
        )));
    }
    let restored = restore_manifest(
        objects,
        kv,
        &manifest,
        tail.current_generation,
        tail.log_start,
        Some(filter.clone()),
    )
    .await?;
    let replay = replay_committed_frames_from_filtered(
        kv,
        tail.committed_frames,
        tail.barrier_offset,
        restored.covered_offset.saturating_add(1),
        restored.journal_seq,
        &filter,
    )?;
    Ok(RestorePlan {
        restored_from: Some(restored),
        replay,
    })
}

/// Build prune requests and checkpoint-object deletions after a durable checkpoint.
/// # Errors
///
/// Returns an error when the requested operation cannot be completed.
pub async fn plan_prune(
    store: &dyn CheckpointStore,
    tenant: &str,
    topic: &str,
    manifest: &Manifest,
    keep_newest: usize,
) -> Result<WalPrunePlan, SubstrateError> {
    let mut dirs = checkpoint_dirs(store, tenant).await?;
    dirs.sort();
    let keep_from = dirs.len().saturating_sub(keep_newest);
    let retained_dirs = &dirs[keep_from..];
    let pins = checkpoint_pins(store, tenant).await?;
    let pinned_dirs = pins
        .iter()
        .map(|pin| {
            pin.manifest_key
                .strip_suffix("MANIFEST")
                .ok_or_else(|| {
                    SubstrateError::Checkpoint(
                        "checkpoint pin manifest key has no MANIFEST suffix".into(),
                    )
                })
                .map(str::to_owned)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let delete_dirs = dirs[..keep_from]
        .iter()
        .filter(|dir| !pinned_dirs.contains(dir))
        .collect::<Vec<_>>();
    let mut horizon = manifest.covered_offset;
    for dir in retained_dirs {
        let retained = Manifest::decode(&store.get(&manifest_key(dir)).await?)?;
        if retained.wal_generation == manifest.wal_generation {
            horizon = horizon.min(retained.covered_offset);
        }
    }
    for pin in &pins {
        if pin.wal_generation == manifest.wal_generation {
            horizon = horizon.min(pin.covered_offset);
        }
    }
    let offset = horizon
        .checked_add(1)
        .ok_or_else(|| SubstrateError::Checkpoint("checkpoint covered offset overflow".into()))?;
    let delete_object_keys = store
        .list(&ckpt_prefix(tenant))
        .await?
        .into_iter()
        .map(|object| object.key)
        .filter(|key| delete_dirs.iter().any(|dir| key.starts_with(*dir)))
        .collect();

    Ok(WalPrunePlan {
        delete_records: vec![DeleteRecordsOp {
            topic: topic.to_owned(),
            partition: 0,
            offset,
        }],
        delete_object_keys,
    })
}

async fn restore_manifest(
    objects: &dyn CheckpointStore,
    kv: &dyn RestoreKv,
    manifest: &Manifest,
    current_generation: u64,
    log_start: Option<i64>,
    filter: Option<CheckpointFilter>,
) -> Result<RestoredFrom, SubstrateError> {
    let parts_by_name = load_manifest_parts(objects, manifest).await?;
    let parts = manifest.validate(&ManifestValidation {
        tenant: &manifest.tenant,
        wal_generation: manifest.wal_generation,
        log_start: if manifest.wal_generation == current_generation {
            log_start
        } else {
            None
        },
        parts_by_name: &parts_by_name,
    })?;
    let mut snapshot = PartKvSnapshot::new(parts, filter)?;
    let expected_pairs = snapshot.len();
    let restored_pairs = kv.restore_sorted(&mut snapshot)?;
    if restored_pairs != expected_pairs {
        return Err(SubstrateError::Checkpoint(format!(
            "checkpoint restored pair count mismatch: manifest {expected_pairs}, restored {restored_pairs}",
        )));
    }
    Ok(RestoredFrom {
        wal_generation: manifest.wal_generation,
        covered_offset: manifest.covered_offset,
        journal_seq: manifest.journal_seq,
    })
}

async fn restore_manifest_table_transfer(
    objects: &dyn CheckpointStore,
    kv: &dyn RestoreKv,
    manifest: &Manifest,
    current_generation: u64,
    log_start: Option<i64>,
    selector: &mut TableTransferSelector,
) -> Result<TableTransferRestore, SubstrateError> {
    let parts_by_name = load_manifest_parts(objects, manifest).await?;
    let parts = manifest.validate(&ManifestValidation {
        tenant: &manifest.tenant,
        wal_generation: manifest.wal_generation,
        log_start: if manifest.wal_generation == current_generation {
            log_start
        } else {
            None
        },
        parts_by_name: &parts_by_name,
    })?;
    let mut staged_selector = selector.clone();
    let pairs = parts.into_iter().flat_map(|part| part.pairs);
    let materialized = staged_selector.materialize_checkpoint(pairs)?;
    let expected_pairs = u64::try_from(materialized.pairs.len())
        .map_err(|_| SubstrateError::Checkpoint("table transfer pair count overflow".into()))?;
    let mut snapshot = PartKvSnapshot::new(vec![CheckpointPart::new(materialized.pairs)], None)?;
    let restored_pairs = kv.restore_sorted(&mut snapshot)?;
    if restored_pairs != expected_pairs {
        return Err(SubstrateError::Checkpoint(format!(
            "table transfer restored pair count mismatch: selected {expected_pairs}, restored {restored_pairs}",
        )));
    }
    *selector = staged_selector;
    Ok(TableTransferRestore {
        restored_from: RestoredFrom {
            wal_generation: manifest.wal_generation,
            covered_offset: manifest.covered_offset,
            journal_seq: manifest.journal_seq,
        },
        stats: materialized.stats,
    })
}

async fn load_manifest_parts(
    objects: &dyn CheckpointStore,
    manifest: &Manifest,
) -> Result<BTreeMap<String, Vec<u8>>, SubstrateError> {
    let mut parts_by_name = BTreeMap::new();
    for part in &manifest.parts {
        parts_by_name.insert(part.name.clone(), objects.get(&part.name).await?);
    }
    Ok(parts_by_name)
}

async fn checkpoint_dirs(
    store: &dyn CheckpointStore,
    tenant: &str,
) -> Result<Vec<String>, SubstrateError> {
    let mut dirs = store
        .list(&ckpt_prefix(tenant))
        .await?
        .into_iter()
        .filter_map(|object| {
            object
                .key
                .rsplit_once('/')
                .map(|(dir, _)| format!("{dir}/"))
        })
        .collect::<Vec<_>>();
    dirs.sort();
    dirs.dedup();
    Ok(dirs)
}

fn collect_snapshot_pairs(snapshot: &mut dyn KvSnapshot) -> Result<Vec<KvPair>, SubstrateError> {
    let mut pairs = Vec::new();
    while let Some(pair) = snapshot.next()? {
        pairs.push(pair);
    }
    Ok(pairs)
}

fn rewrite_snapshot_pairs(
    pairs: Vec<KvPair>,
    garbage_horizon_xid: u64,
) -> Result<Vec<KvPair>, SubstrateError> {
    let clog = super::collect_clog_statuses(&pairs)?;
    let mut rewritten = Vec::with_capacity(pairs.len());
    for (key, value) in pairs {
        match super::rewrite_for_checkpoint(&key, &value, garbage_horizon_xid, &clog)? {
            RewriteDecision::Keep => rewritten.push((key, value)),
            RewriteDecision::Drop => {}
            RewriteDecision::Replace(new_value) => rewritten.push((key, new_value)),
        }
    }
    Ok(rewritten)
}

struct PartKvSnapshot {
    pairs: std::vec::IntoIter<KvPair>,
}

impl PartKvSnapshot {
    fn new(
        parts: Vec<CheckpointPart>,
        filter: Option<CheckpointFilter>,
    ) -> Result<Self, SubstrateError> {
        let pairs = parts.into_iter().flat_map(|part| part.pairs);
        let pairs = match filter {
            Some(filter) => pairs
                .filter_map(|pair| match filter.filter_pair(&pair.0, &pair.1) {
                    Ok(Some(value)) => Some(Ok((pair.0, value))),
                    Ok(None) => None,
                    Err(error) => Some(Err(error)),
                })
                .collect::<Result<Vec<_>, _>>()?,
            None => pairs.collect::<Vec<_>>(),
        };

        Ok(Self {
            pairs: pairs.into_iter(),
        })
    }

    fn len(&self) -> u64 {
        u64::try_from(self.pairs.len()).unwrap_or(u64::MAX)
    }
}

impl KvSnapshot for PartKvSnapshot {
    fn next(&mut self) -> Result<Option<KvPair>, KvError> {
        Ok(self.pairs.next())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_gres_ranges::{RangeKey, RowInterval, TableId};
    use crabka_pgkv::{Kv, MemKv, WriteOp, key};
    use crabka_pgmvcc::{clog, version};
    use crabka_pgtypes::Datum;

    use super::*;
    use crate::{
        checkpoint::{DEFAULT_PART_MAX_BYTES, InMemoryCheckpointStore},
        frame::{BARRIER_SEQ, WalFrame},
    };

    #[tokio::test]
    async fn manifest_last_invisibility() {
        let objects = InMemoryCheckpointStore::shared();
        objects
            .put(&part_key(&ckpt_dir("t", 0, 3, 1), 0), b"part".to_vec())
            .await
            .expect("put");

        let restored = restore_latest(objects.as_ref(), "t", &MemKv::default(), 0, None)
            .await
            .expect("restore scan");

        assert!(restored.is_none());
    }

    #[tokio::test]
    async fn newest_valid_selection_skips_corrupt_and_incomplete() {
        let objects = InMemoryCheckpointStore::shared();
        let old = MemKv::default();
        old.put(b"a".to_vec(), b"old".to_vec()).expect("put");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &old,
            snapshot_at(5),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("old checkpoint");
        let new = MemKv::default();
        new.put(b"a".to_vec(), b"new".to_vec()).expect("put");
        let manifest = write_checkpoint(
            objects.as_ref(),
            "t",
            &new,
            snapshot_at(9),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("new checkpoint");
        let first_part = manifest.parts[0].name.clone();
        objects
            .put(&first_part, b"corrupt".to_vec())
            .await
            .expect("corrupt");
        let incomplete_dir = ckpt_dir("t", 0, 11, 1);
        objects
            .put(
                &manifest_key(&incomplete_dir),
                manifest.encode().expect("encode"),
            )
            .await
            .expect("put");
        let restored = MemKv::default();

        let source = restore_latest(objects.as_ref(), "t", &restored, 0, None)
            .await
            .expect("restore");

        assert!(source.expect("source").covered_offset == 5);
        assert!(restored.get(b"a").expect("get") == Some(b"old".to_vec()));
    }

    #[tokio::test]
    async fn table_transfer_skips_failed_newest_checkpoint_without_leaking_selector_xids() {
        let objects = InMemoryCheckpointStore::shared();
        let table_id = 7;
        let old = MemKv::default();
        let old_tuple = version::version_key_xid(table_id, 1, 5);
        old.put(
            old_tuple.clone(),
            version::encode_tuple(5, 0, &[Datum::Int4(1)]),
        )
        .expect("old tuple");
        let (old_clog_key, old_clog_value) = clog_pair(5);
        old.put(old_clog_key, old_clog_value).expect("old CLOG");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &old,
            snapshot_at(1),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("old checkpoint");

        let newest = MemKv::default();
        newest
            .put(
                version::version_key_xid(table_id, 2, 9),
                version::encode_tuple(9, 0, &[Datum::Int4(2)]),
            )
            .expect("new tuple");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &newest,
            snapshot_at(2),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("new checkpoint");

        let target = MemKv::default();
        let mut selector = TableTransferSelector::new(table_id).expect("selector");
        let (_restored, replay) = restore_latest_table_transfer_and_replay_tail(
            objects.as_ref(),
            "t",
            &target,
            RestoreTail {
                current_generation: 0,
                log_start: Some(0),
                committed_frames: vec![
                    ReplayItem {
                        offset: 2,
                        bytes: WalFrame {
                            journal_seq: 2,
                            ops: vec![
                                clog::put_op(9, clog::XidStatus::Committed),
                                clog::put_op(5, clog::XidStatus::Committed),
                            ],
                        }
                        .encode(),
                    },
                    barrier(3),
                ],
                barrier_offset: 3,
            },
            &mut selector,
        )
        .await
        .expect("older checkpoint restores");

        assert!(replay.next_journal_seq == 3);
        assert!(
            target.get(&old_tuple).expect("old tuple")
                == Some(version::encode_tuple(5, 0, &[Datum::Int4(1)]))
        );
        assert!(
            target
                .get(&key::clog_key(5))
                .expect("referenced CLOG")
                .is_some()
        );
        assert!(
            target
                .get(&key::clog_key(9))
                .expect("unrelated CLOG")
                .is_none()
        );
    }

    #[tokio::test]
    async fn restore_then_tail_replay_applies_only_after_covered_offset() {
        let objects = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"a".to_vec(), b"checkpoint".to_vec())
            .expect("put");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &base,
            snapshot_at(1),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let restored = MemKv::default();
        let frames = vec![
            item(1, 0, b"a", b"old"),
            item(2, 2, b"b", b"tail"),
            barrier(3),
        ];

        let plan = restore_latest_and_replay_tail(
            objects.as_ref(),
            "t",
            &restored,
            RestoreTail {
                current_generation: 0,
                log_start: Some(0),
                committed_frames: frames,
                barrier_offset: 3,
            },
        )
        .await
        .expect("restore replay");

        assert!(plan.restored_from.expect("source").covered_offset == 1);
        assert!(plan.replay.next_journal_seq == 3);
        assert!(restored.get(b"a").expect("get") == Some(b"checkpoint".to_vec()));
        assert!(restored.get(b"b").expect("get") == Some(b"tail".to_vec()));
    }

    #[tokio::test]
    async fn filtered_restore_matches_full_checkpoint_filtered_by_interval() {
        let objects = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        let predecessor_key = key::row_key(7, 10);
        let successor_key = key::row_key(7, 20);
        let successor_hi_key = key::row_key(7, 30);
        base.put(predecessor_key.clone(), b"predecessor".to_vec())
            .expect("put predecessor");
        base.put(successor_key.clone(), b"successor".to_vec())
            .expect("put successor");
        base.put(successor_hi_key.clone(), b"successor-hi".to_vec())
            .expect("put successor hi");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &base,
            snapshot_at(4),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let filter = CheckpointFilter::new(
            RangeKey::new(TableId::new(7), 20),
            Some(RangeKey::new(TableId::new(7), 31)),
        )
        .expect("filter")
        .with_physical_to_logical(BTreeMap::from([(TableId::new(7), TableId::new(7))]));
        let full = MemKv::default();
        let filtered = MemKv::default();

        restore_latest(objects.as_ref(), "t", &full, 0, None)
            .await
            .expect("full restore")
            .expect("full source");
        restore_latest_filtered(objects.as_ref(), "t", &filtered, 0, None, filter)
            .await
            .expect("filtered restore")
            .expect("filtered source");

        assert!(
            filtered
                .get(&predecessor_key)
                .expect("get predecessor")
                .is_none()
        );
        assert!(
            filtered.get(&successor_key).expect("get successor")
                == full.get(&successor_key).expect("get full successor")
        );
        assert!(
            filtered.get(&successor_hi_key).expect("get successor hi")
                == full.get(&successor_hi_key).expect("get full successor hi")
        );
    }

    #[tokio::test]
    async fn selected_manifest_and_tail_partition_exactly_across_two_intervals() {
        let objects = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        let left_key = key::row_key(7, 10);
        let right_key = key::row_key(7, 30);
        let clog_key = key::clog_key(9);
        base.put(left_key.clone(), b"left".to_vec()).expect("left");
        base.put(right_key.clone(), b"right".to_vec())
            .expect("right");
        base.put(clog_key.clone(), vec![1]).expect("clog");
        let manifest = write_checkpoint(
            objects.as_ref(),
            "t",
            &base,
            snapshot_at(4),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let manifest_key = objects
            .list(&ckpt_prefix("t"))
            .await
            .expect("list checkpoint")
            .into_iter()
            .map(|object| object.key)
            .find(|key| key.ends_with("/MANIFEST"))
            .expect("manifest key");
        let split = RangeKey::new(TableId::new(7), 20);
        let left = MemKv::default();
        let right = MemKv::default();
        let control = MemKv::default();
        for (target, filter) in [
            (
                &left,
                CheckpointFilter::new(RangeKey::new(TableId::new(7), 0), Some(split))
                    .expect("left filter")
                    .with_physical_to_logical(BTreeMap::from([(TableId::new(7), TableId::new(7))]))
                    .with_structural_ownership(true),
            ),
            (
                &right,
                CheckpointFilter::new(split, Some(RangeKey::new(TableId::new(8), 0)))
                    .expect("right filter")
                    .with_physical_to_logical(BTreeMap::from([(TableId::new(7), TableId::new(7))]))
                    .with_structural_ownership(false),
            ),
        ] {
            restore_filtered_from_manifest_and_replay_tail(
                objects.as_ref(),
                &manifest_key,
                "t",
                manifest.covered_offset,
                target,
                RestoreTail {
                    current_generation: manifest.wal_generation,
                    log_start: None,
                    committed_frames: vec![barrier(5)],
                    barrier_offset: 5,
                },
                filter,
            )
            .await
            .expect("filtered selected restore");
        }
        restore_filtered_from_manifest_and_replay_tail(
            objects.as_ref(),
            &manifest_key,
            "t",
            manifest.covered_offset,
            &control,
            RestoreTail {
                current_generation: manifest.wal_generation,
                log_start: None,
                committed_frames: vec![barrier(5)],
                barrier_offset: 5,
            },
            CheckpointFilter::new(
                RangeKey::new(TableId::new(7), 0),
                Some(RangeKey::new(TableId::new(8), 0)),
            )
            .expect("control filter")
            .with_physical_to_logical(BTreeMap::from([(TableId::new(7), TableId::new(7))]))
            .with_structural_ownership(true),
        )
        .await
        .expect("control selected restore");

        assert!(left.get(&left_key).expect("left get") == Some(b"left".to_vec()));
        assert!(left.get(&right_key).expect("left excludes right").is_none());
        assert!(right.get(&left_key).expect("right excludes left").is_none());
        assert!(right.get(&right_key).expect("right get") == Some(b"right".to_vec()));
        assert!(left.get(&clog_key).expect("left owns clog").is_some());
        assert!(right.get(&clog_key).expect("right excludes clog").is_none());
        let left_pairs = left.scan_range(&[], &[u8::MAX]).expect("left scan");
        let right_pairs = right.scan_range(&[], &[u8::MAX]).expect("right scan");
        let control_pairs = control.scan_range(&[], &[u8::MAX]).expect("control scan");
        let left_map = left_pairs.into_iter().collect::<BTreeMap<_, _>>();
        let right_map = right_pairs.into_iter().collect::<BTreeMap<_, _>>();
        assert!(left_map.keys().all(|key| !right_map.contains_key(key)));
        let mut union = left_map;
        union.extend(right_map);
        assert_eq!(union, control_pairs.into_iter().collect::<BTreeMap<_, _>>());
    }

    #[test]
    fn malformed_checkpoint_filter_boundaries_are_rejected() {
        let same_key = RangeKey::new(TableId::new(7), 20);

        assert!(CheckpointFilter::new(same_key, Some(same_key)).is_err());
        assert!(
            CheckpointFilter::for_table_interval(
                TableId::new(7),
                RowInterval {
                    start: Some(9),
                    end: Some(5),
                },
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn full_restore_keeps_unfiltered_checkpoint_pairs() {
        let objects = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"plain".to_vec(), b"value".to_vec())
            .expect("put plain");
        base.put(key::row_key(7, 20), b"row".to_vec())
            .expect("put row");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &base,
            snapshot_at(4),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");
        let restored = MemKv::default();

        restore_latest(objects.as_ref(), "t", &restored, 0, None)
            .await
            .expect("restore")
            .expect("source");

        assert!(restored.get(b"plain").expect("get plain") == Some(b"value".to_vec()));
        assert!(restored.get(&key::row_key(7, 20)).expect("get row") == Some(b"row".to_vec()));
    }

    #[tokio::test]
    async fn refuses_torn_truncation() {
        let objects = InMemoryCheckpointStore::shared();
        let base = MemKv::default();
        base.put(b"a".to_vec(), b"v".to_vec()).expect("put");
        write_checkpoint(
            objects.as_ref(),
            "t",
            &base,
            snapshot_at(4),
            DEFAULT_PART_MAX_BYTES,
        )
        .await
        .expect("checkpoint");

        let error = restore_latest(objects.as_ref(), "t", &MemKv::default(), 0, Some(6))
            .await
            .expect_err("torn");

        assert!(matches!(
            error,
            SubstrateError::TornTruncation {
                log_start: 6,
                newest_manifest: 4
            }
        ));
    }

    #[tokio::test]
    async fn prune_request_horizon_calculation() {
        let objects = InMemoryCheckpointStore::shared();
        for offset in [1, 2, 3] {
            let kv = MemKv::default();
            kv.put(vec![u8::try_from(offset).expect("offset")], b"v".to_vec())
                .expect("put");
            write_checkpoint(
                objects.as_ref(),
                "t",
                &kv,
                snapshot_at(offset),
                DEFAULT_PART_MAX_BYTES,
            )
            .await
            .expect("checkpoint");
        }
        let latest = Manifest::new("t".to_string(), 3, 4, 1, 0, Vec::new());

        let plan = plan_prune(objects.as_ref(), "t", "__gres_wal.t", &latest, 2)
            .await
            .expect("plan");

        assert!(
            plan.delete_records
                == vec![DeleteRecordsOp {
                    topic: "__gres_wal.t".to_string(),
                    partition: 0,
                    offset: 3,
                }]
        );
        assert!(
            plan.delete_object_keys
                .iter()
                .all(|key| key.contains("00000000000000000001"))
        );
    }

    #[tokio::test]
    async fn pinned_checkpoint_survives_retention_and_caps_wal_pruning_until_release() {
        let objects = InMemoryCheckpointStore::shared();
        for offset in [1, 2, 3] {
            let kv = MemKv::default();
            kv.put(vec![u8::try_from(offset).expect("offset")], b"v".to_vec())
                .expect("put");
            write_checkpoint(
                objects.as_ref(),
                "t",
                &kv,
                snapshot_at(offset),
                DEFAULT_PART_MAX_BYTES,
            )
            .await
            .expect("checkpoint");
        }
        let pinned = ckpt_dir("t", 0, 1, 1);
        pin_checkpoint(
            objects.as_ref(),
            "t",
            "split-a",
            &manifest_key(&pinned),
            0,
            1,
        )
        .await
        .expect("pin");
        let latest = Manifest::new("t".to_string(), 3, 4, 1, 0, Vec::new());

        let pinned_plan = plan_prune(objects.as_ref(), "t", "__gres_wal.t", &latest, 1)
            .await
            .expect("pinned plan");
        assert!(pinned_plan.delete_records[0].offset == 2);
        assert!(
            pinned_plan
                .delete_object_keys
                .iter()
                .all(|key| !key.starts_with(&pinned))
        );

        unpin_checkpoint(objects.as_ref(), "t", "split-a")
            .await
            .expect("unpin");
        let released_plan = plan_prune(objects.as_ref(), "t", "__gres_wal.t", &latest, 1)
            .await
            .expect("released plan");
        assert!(released_plan.delete_records[0].offset == 4);
        assert!(
            released_plan
                .delete_object_keys
                .iter()
                .any(|key| key.starts_with(&pinned))
        );
    }

    #[tokio::test]
    async fn startup_pin_reconciliation_keeps_only_the_exact_durable_operation() {
        let objects = InMemoryCheckpointStore::shared();
        let manifest = manifest_key(&ckpt_dir("t", 0, 7, 1));
        pin_checkpoint(objects.as_ref(), "t", "active", &manifest, 0, 7)
            .await
            .expect("active pin");
        pin_checkpoint(objects.as_ref(), "t", "orphan", &manifest, 0, 7)
            .await
            .expect("orphan pin");

        reconcile_checkpoint_pins(
            objects.as_ref(),
            "t",
            Some(("active", manifest.as_str(), 7)),
        )
        .await
        .expect("reconcile active");
        assert!(
            objects
                .get(&checkpoint_pin_key("t", "active"))
                .await
                .is_ok()
        );
        assert!(
            objects
                .get(&checkpoint_pin_key("t", "orphan"))
                .await
                .is_err()
        );

        reconcile_checkpoint_pins(objects.as_ref(), "t", None)
            .await
            .expect("reconcile terminal");
        assert!(
            objects
                .get(&checkpoint_pin_key("t", "active"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn crash_before_manifest_leaves_no_visible_checkpoint() {
        let objects = InMemoryCheckpointStore::shared();
        let part = CheckpointPart::new(vec![(b"a".to_vec(), b"v".to_vec())]).encode();
        let dir = ckpt_dir("t", 0, 7, 1);
        objects
            .put(&part_key(&dir, 0), part)
            .await
            .expect("put part");

        let restored = restore_latest(objects.as_ref(), "t", &MemKv::default(), 0, None)
            .await
            .expect("restore");

        assert!(restored.is_none());
    }

    #[tokio::test]
    async fn visible_manifest_with_missing_part_fails_when_no_checkpoint_is_valid() {
        let objects = InMemoryCheckpointStore::shared();
        let dir = ckpt_dir("t", 0, 7, 1);
        let missing_part_bytes = CheckpointPart::new(vec![(b"a".to_vec(), b"v".to_vec())]).encode();
        let manifest = Manifest::new(
            "t".to_string(),
            7,
            8,
            1,
            0,
            vec![
                PartEntry::from_encoded_part(part_key(&dir, 0), &missing_part_bytes)
                    .expect("part entry"),
            ],
        );
        objects
            .put(
                &manifest_key(&dir),
                manifest.encode().expect("encode manifest"),
            )
            .await
            .expect("put manifest");

        let error = restore_latest(objects.as_ref(), "t", &MemKv::default(), 0, None)
            .await
            .expect_err("missing checkpoint part");

        assert!(matches!(error, SubstrateError::Checkpoint(_)));
    }

    fn snapshot_at(covered_offset: i64) -> CheckpointSnapshot {
        CheckpointSnapshot {
            covered_offset,
            journal_seq: u64::try_from(covered_offset + 1).expect("seq"),
            producer_epoch: 1,
            wal_generation: 0,
            garbage_horizon_xid: 0,
        }
    }

    fn clog_pair(xid: u64) -> (Vec<u8>, Vec<u8>) {
        let WriteOp::Put { key, value } = clog::put_op(xid, clog::XidStatus::Committed) else {
            unreachable!("CLOG status writes are puts")
        };
        (key, value)
    }

    fn item(offset: i64, journal_seq: u64, key: &[u8], value: &[u8]) -> ReplayItem {
        ReplayItem {
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

    fn barrier(offset: i64) -> ReplayItem {
        ReplayItem {
            offset,
            bytes: WalFrame {
                journal_seq: BARRIER_SEQ,
                ops: Vec::new(),
            }
            .encode(),
        }
    }
}
