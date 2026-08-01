//! Checkpoint part framing, object keys, and manifest validation.

mod codec;
mod horizon;
mod manifest;
mod runtime;
mod service;
mod store;

use crabka_gres_ranges::{RangeId, TenantName, checkpoint_prefix as range_checkpoint_prefix};
use crabka_units::{ByteSize, mebibytes};

pub(crate) use self::runtime::restore_latest_at_or_before;
#[cfg(feature = "checkpoint-test-hooks")]
pub use self::service::{CheckpointFailpoint, CheckpointServiceStep};
pub use self::{
    codec::{CheckpointFilter, CheckpointPart, PartPayload},
    horizon::{
        ClogLookup, ConversionTimestampBoundary, RewriteDecision, collect_clog_statuses,
        rewrite_for_checkpoint, rewrite_snapshot_pairs_for_conversion,
    },
    manifest::{MANIFEST_FORMAT_VERSION, Manifest, ManifestValidation, PartEntry},
    runtime::{
        CheckpointMetadata, CheckpointSnapshot, RestorePlan, RestoreTail, RestoredFrom,
        TableTransferRestore, WalPrunePlan, latest_checkpoint_metadata, plan_prune,
        reconcile_checkpoint_pins, restore_filtered_from_manifest_and_replay_tail, restore_latest,
        restore_latest_and_replay_tail, restore_latest_filtered,
        restore_latest_filtered_and_replay_tail, restore_latest_table_transfer,
        restore_latest_table_transfer_and_replay_tail,
        restore_table_transfer_from_manifest_and_replay_tail, write_checkpoint,
    },
    service::{
        CheckpointConfig, CheckpointHandle, CheckpointPlannerStats, CheckpointRun,
        CheckpointService, CheckpointStats, CheckpointTrigger, CheckpointWalPruner,
        DEFAULT_CHECKPOINT_RETAIN,
    },
    store::{CheckpointObject, CheckpointStore, InMemoryCheckpointStore, ObjectOpsCheckpointStore},
};

/// Default checkpoint part target size.
pub const DEFAULT_PART_MAX_SIZE: ByteSize = mebibytes(64);

/// Object prefix that contains all checkpoints for one tenant.
#[must_use]
pub fn ckpt_prefix(tenant: &str) -> String {
    format!("gres/{tenant}/ckpt/")
}

/// Object prefix that contains all checkpoints for one tenant range.
#[must_use]
pub fn ckpt_prefix_for_range(tenant: &TenantName, range: RangeId) -> String {
    range_checkpoint_prefix(tenant, range).to_string()
}

/// Object directory for one immutable checkpoint attempt.
#[must_use]
pub fn ckpt_dir(
    tenant: &str,
    wal_generation: u64,
    covered_offset: i64,
    producer_epoch: i16,
) -> String {
    format!(
        "{}{wal_generation:010}-{covered_offset:020}-{producer_epoch:05}/",
        ckpt_prefix(tenant),
    )
}

/// Object directory for one immutable tenant-range checkpoint attempt.
#[must_use]
pub fn ckpt_dir_for_range(
    tenant: &TenantName,
    range: RangeId,
    wal_generation: u64,
    covered_offset: i64,
    producer_epoch: i16,
) -> String {
    format!(
        "{}{wal_generation:010}-{covered_offset:020}-{producer_epoch:05}/",
        ckpt_prefix_for_range(tenant, range),
    )
}

/// Object key for the manifest written after all parts.
#[must_use]
pub fn manifest_key(dir: &str) -> String {
    format!("{dir}MANIFEST")
}

/// Object key for a zero-padded part within a checkpoint directory.
#[must_use]
pub fn part_key(dir: &str, index: usize) -> String {
    format!("{dir}part-{index:05}")
}
