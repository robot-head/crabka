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

    /// The metadata partition that would answer this query is assigned to
    /// this broker but its consumer has not yet caught up to the high-water
    /// mark observed when the partition was assigned. The answer is unknown,
    /// not "no segment" — callers should retry rather than treat it as a
    /// definitive miss. `Ok(None)` is reserved for "caught up, no covering
    /// segment" and for partitions this broker does not consume at all.
    #[error("remote log metadata partition {partition} not ready (assigned but not caught up)")]
    NotReady {
        /// The `__remote_log_metadata` partition that is still catching up.
        partition: i32,
    },
}

impl From<crabka_object_store::ObjectStoreError> for RemoteStorageError {
    fn from(err: crabka_object_store::ObjectStoreError) -> Self {
        use crabka_object_store::ObjectStoreError as E;

        match err {
            E::Io(e) => Self::Io(e),
            E::InvalidConfig(m) => Self::InvalidArgument(m),
            // Engine methods with segment context match NotFound before converting.
            E::NotFound(p) => Self::Backend(format!("not found: {p}")),
            E::TooLarge {
                key,
                size,
                max_bytes,
            } => Self::Backend(format!(
                "object `{key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
            )),
            E::Backend(m) => Self::Backend(m),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use object_store::path::Path;

    use super::*;

    #[test]
    fn object_store_too_large_converts_to_backend_message() {
        let err = crabka_object_store::ObjectStoreError::TooLarge {
            key: Path::from("index/snapshot.json"),
            size: 1000,
            max_bytes: 256,
        };

        let got = RemoteStorageError::from(err);

        assert!(matches!(
            got,
            RemoteStorageError::Backend(ref msg)
                if msg == "object `index/snapshot.json` is 1000 bytes, exceeds cap of 256 bytes"
        ));
    }
}
