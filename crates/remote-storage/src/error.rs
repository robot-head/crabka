//! Error type shared by both tiered-storage SPIs.

use crate::metadata::{
    RemoteLogSegmentId, RemoteLogSegmentState, RemotePartitionDeleteState, TopicIdPartition,
};

/// Errors raised by [`RemoteStorageManager`](crate::RemoteStorageManager)
/// and [`RemoteLogMetadataManager`](crate::RemoteLogMetadataManager)
/// implementations.
#[derive(Debug, thiserror::Error)]
pub enum RemoteStorageError {
    /// An I/O failure in the underlying store (filesystem, object store, …).
    #[error("remote storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A segment was referenced that the metadata store has never seen.
    #[error("no remote log segment metadata for {0:?}")]
    SegmentNotFound(RemoteLogSegmentId),

    /// `add_remote_log_segment_metadata` was called with a starting state
    /// other than [`RemoteLogSegmentState::CopySegmentStarted`], or for a
    /// segment id that already exists.
    #[error("invalid add for {id:?}: {reason}")]
    InvalidAdd {
        /// The offending segment id.
        id: RemoteLogSegmentId,
        /// Why the add was rejected.
        reason: String,
    },

    /// A lifecycle transition was requested that the state machine forbids.
    #[error("invalid segment state transition for {id:?}: {from:?} -> {to:?}")]
    InvalidSegmentTransition {
        /// The segment whose transition was rejected.
        id: RemoteLogSegmentId,
        /// Current state.
        from: RemoteLogSegmentState,
        /// Requested state.
        to: RemoteLogSegmentState,
    },

    /// A partition-delete lifecycle transition was requested that the
    /// state machine forbids.
    #[error("invalid partition delete transition for {tp:?}: {from:?} -> {to:?}")]
    InvalidPartitionDeleteTransition {
        /// The partition whose transition was rejected.
        tp: TopicIdPartition,
        /// Current state (`None` when the partition was never marked).
        from: Option<RemotePartitionDeleteState>,
        /// Requested state.
        to: RemotePartitionDeleteState,
    },

    /// Constructor / argument validation failed.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A backend (e.g. an object store) raised an error that doesn't map
    /// cleanly to one of the structured variants above.
    #[error("remote storage backend error: {0}")]
    Backend(String),
}
