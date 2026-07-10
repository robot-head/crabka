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
        CheckpointRun, CheckpointService, CheckpointStats, CheckpointTrigger, CheckpointWalPruner,
        DEFAULT_CHECKPOINT_RETAIN, DEFAULT_PART_MAX_BYTES, MANIFEST_FORMAT_VERSION, Manifest,
        ManifestValidation, PartEntry, PartPayload, RestoreTail, RewriteDecision, ckpt_dir,
        ckpt_dir_for_range, ckpt_prefix, ckpt_prefix_for_range, latest_checkpoint_metadata,
        manifest_key, part_key, restore_latest_filtered, restore_latest_filtered_and_replay_tail,
        restore_latest_table_transfer, restore_latest_table_transfer_and_replay_tail,
        restore_table_transfer_from_manifest_and_replay_tail, rewrite_for_checkpoint,
    },
    error::SubstrateError,
    follower::{BrokerRange0EndSampler, CommittedEndSampler, ReadOnlyRange0Follower},
    frame::WalFrame,
    recovery::{
        CommittedWalReader, InMemoryWalLog, LiveRecovered, LiveRecoveryConfig, RecoveryBarrier,
        RecoveryFencer, bounded_committed_tail, read_live_committed_tail, recover_after_barrier,
        recover_live, recover_live_for_range, recover_live_for_range_with_restore,
    },
    replay::{
        ReplayItem, ReplayOutcome, replay_committed_frames, replay_committed_frames_from_filtered,
        replay_committed_frames_from_table_transfer,
    },
    split_runtime::{InMemorySplitStateStore, RawKvSplitRuntime},
    stats::{InMemoryRangeStatsProvider, RangeStats, RangeStatsProvider, RangeStatsSnapshot},
    topic::{
        TopicAdmin, ensure_wal_topic, ensure_wal_topic_for_range, transactional_id_for_range,
        wal_topic, wal_topic_for_range,
    },
    transfer::{
        TableTransferIdentity, TableTransferMaterialization, TableTransferSelector,
        TableTransferStats,
    },
    writer::{
        CheckpointSnapshotSource, DEFAULT_MAX_FRAME_BYTES, FenceLease, GroupCommitAck,
        GroupCommitRequest, PausedWalWriter, ProducerWalWriter, SubstrateCommitter,
        SubstrateLinearizer, SubstrateTsoHorizon, TransactionalWalWriter, WalAppendAck,
        WriterGeneration, chunk_wal_batch,
    },
};
