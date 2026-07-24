//! Substrate-backed durability for Crabka Gres tenant computes.
//!
//! Implements the engine's [`crabka_pgexec::Committer`] and
//! [`crabka_pgexec::Linearizer`] seams over a per-range WAL topic
//! (`__gres_wal.<tenant>.r<range>`): a single writer task group-commits framed batches
//! inside Kafka transactions (the broker's coordinator-checked producer epoch
//! is the zombie fence), and recovery replays the topic before serving.
//!
//! # Key Types
//! - [`WalFrame`] — the `GRW1` record framing.
//! - [`apply_frame`] — replay application with the engine's merge rules.
//! - [`replay_committed_frames`] — pure replay over committed frame bytes.

pub mod apply;
pub mod checkpoint;
pub mod error;
pub mod follower;
pub mod frame;
pub mod readonly_fold;
pub mod recovery;
pub mod replay;
pub mod split_runtime;
pub mod stats;
pub mod topic;
pub mod transfer;
pub mod writer;

pub use self::{
    apply::apply_frame,
    checkpoint::{
        CheckpointConfig, CheckpointFilter, CheckpointHandle, CheckpointMetadata, CheckpointPart,
        CheckpointPlannerStats, CheckpointRun, CheckpointService, CheckpointStats,
        CheckpointTrigger, CheckpointWalPruner, DEFAULT_CHECKPOINT_RETAIN, DEFAULT_PART_MAX_BYTES,
        MANIFEST_FORMAT_VERSION, Manifest, ManifestValidation, PartEntry, PartPayload, RestoreTail,
        RewriteDecision, ckpt_dir, ckpt_dir_for_range, ckpt_prefix, ckpt_prefix_for_range,
        latest_checkpoint_metadata, manifest_key, part_key, reconcile_checkpoint_pins,
        restore_filtered_from_manifest_and_replay_tail, restore_latest_filtered,
        restore_latest_filtered_and_replay_tail, restore_latest_table_transfer,
        restore_latest_table_transfer_and_replay_tail,
        restore_table_transfer_from_manifest_and_replay_tail, rewrite_for_checkpoint,
    },
    error::SubstrateError,
    follower::{
        BrokerRange0EndSampler, CommittedEndSampler, LiveCommittedEndSampler,
        ReadOnlyRange0Follower, rebuild_range0_tail_from_checkpoint, wal_trimmed_past_applied,
    },
    frame::WalFrame,
    readonly_fold::{
        CommittedFoldSnapshot, FoldCheckpointIdentity, FoldLimits, FoldProjection, FoldProvenance,
        FoldRecordSource, FoldSnapshotRequest, GenerationWitness, committed_fold_snapshot,
        committed_fold_snapshot_live, committed_fold_snapshot_live_at,
    },
    recovery::{
        CommittedWalReader, DEFAULT_WAL_RECOVERY_CONNECT_TIMEOUT_MS,
        DEFAULT_WAL_RECOVERY_EMPTY_FETCH_RETRIES, DEFAULT_WAL_RECOVERY_FETCH_MAX_WAIT_MS,
        DEFAULT_WAL_RECOVERY_FETCH_PARTITION_MAX_BYTES,
        DEFAULT_WAL_RECOVERY_FETCH_RESPONSE_MAX_BYTES, DEFAULT_WAL_RECOVERY_REQUEST_TIMEOUT_MS,
        InMemoryWalLog, LiveRecovered, LiveRecoveryConfig, RecoveryBarrier, RecoveryFencer,
        RecoveryReadPolicy, bootstrap_live_range0_follower, bounded_committed_tail,
        ensure_live_wal_topic, live_committed_end, live_wal_trimmed_past_applied,
        read_live_committed_tail, read_live_retained_committed,
        rebuild_live_range0_tail_from_checkpoint, recover_after_barrier, recover_live,
        recover_live_for_range, recover_live_for_range_with_restore,
    },
    replay::{
        ReplayItem, ReplayOutcome, replay_committed_frames, replay_committed_frames_from_filtered,
        replay_committed_frames_from_table_transfer,
    },
    split_runtime::{InMemorySplitStateStore, RawKvSplitRuntime},
    stats::{InMemoryRangeStatsProvider, RangeStats, RangeStatsProvider, RangeStatsSnapshot},
    topic::{
        TopicAdmin, ensure_wal_topic, ensure_wal_topic_for_range, ensure_wal_topic_name,
        transactional_id_for_range, wal_topic, wal_topic_for_generation, wal_topic_for_range,
    },
    transfer::{
        TableTransferIdentity, TableTransferMaterialization, TableTransferSelector,
        TableTransferStats,
    },
    writer::{
        CheckpointSnapshotSource, DEFAULT_MAX_FRAME_BYTES, DeferredWalWriter, FenceLease,
        GroupCommitAck, GroupCommitRequest, PausedWalAuthorization, PausedWalWriter,
        ProducerWalWriter, SubstrateCommitter, SubstrateLinearizer, SubstrateTsoHorizon,
        TransactionalWalWriter, WalAppendAck, WalWriterFaultInjector, WalWriterFaultStage,
        WriterGeneration, chunk_wal_batch,
    },
};
