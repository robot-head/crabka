//! The [`RemoteLogMetadataManager`] SPI: persistence and querying of
//! remote-segment metadata.

use crate::{
    error::RemoteStorageError,
    metadata::{
        RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemotePartitionDeleteMetadata,
        TopicIdPartition,
    },
};

/// SPI for the store that holds metadata about remote log segments.
///
/// The reference implementation is
/// [`InmemoryRemoteLogMetadataManager`](crate::InmemoryRemoteLogMetadataManager);
/// a production implementation backs the metadata with an internal topic
/// (Kafka's `TopicBasedRemoteLogMetadataManager`). Implementations must be
/// `Send + Sync`.
///
/// The lifecycle invariants every implementation enforces:
///
/// - A segment is introduced with
///   [`add_remote_log_segment_metadata`](RemoteLogMetadataManager::add_remote_log_segment_metadata)
///   in state
///   [`CopySegmentStarted`](crate::RemoteLogSegmentState::CopySegmentStarted).
/// - Every later change is an
///   [`update_remote_log_segment_metadata`](RemoteLogMetadataManager::update_remote_log_segment_metadata)
///   that follows a valid state transition.
/// - Only
///   [`CopySegmentFinished`](crate::RemoteLogSegmentState::CopySegmentFinished)
///   segments are visible to the offset/epoch queries.
pub trait RemoteLogMetadataManager: Send + Sync {
    /// Record a newly-started segment copy. The metadata's state must be
    /// [`CopySegmentStarted`](crate::RemoteLogSegmentState::CopySegmentStarted)
    /// and its id must not already be known.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidAdd`] if the starting state is
    /// wrong or the segment id is a duplicate.
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;

    /// Advance a known segment's lifecycle state.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment is
    /// unknown, or [`RemoteStorageError::InvalidSegmentTransition`] if the
    /// transition is not permitted.
    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError>;

    /// Find the finished segment that contains `offset` for the given
    /// `leader_epoch`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure;
    /// a simple "no such segment" is `Ok(None)`.
    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// The highest offset that the remote tier holds for `leader_epoch`
    /// (the max `end_offset` across finished segments carrying that epoch).
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Option<i64>, RemoteStorageError>;

    /// All segments known for a partition, ordered by `start_offset`
    /// (regardless of state).
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// All segments carrying `leader_epoch`, ordered by `start_offset`
    /// (regardless of state).
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] only on an underlying store failure.
    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        leader_epoch: i32,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError>;

    /// Record a partition-delete lifecycle event.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::InvalidPartitionDeleteTransition`] if
    /// the transition is not permitted from the partition's current
    /// delete-state.
    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError>;
}
