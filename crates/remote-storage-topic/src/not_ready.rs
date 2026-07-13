//! [`NotReadyRlmm`] — fail-closed [`RemoteLogMetadataManager`] placeholder.
//!
//! Used as the [`SwappableRlmm`](crate::SwappableRlmm) placeholder during the
//! topic-backed bootstrap window. Every method returns
//! [`RemoteStorageError::NotReady`] immediately:
//!
//! - The copy task's first call (`add_remote_log_segment_metadata`) fails with
//!   `NotReady` before any data is copied, so nothing is tiered with
//!   non-durable metadata — no orphaned RSM objects accumulate in the remote
//!   store while the real manager is still initialising.
//! - Remote reads return a retryable `NotReady` error rather than a definitive
//!   `Ok(None)`, so callers retry until the real manager swaps in and catches
//!   up to the high-water mark.
//!
//! This is a stricter placeholder than
//! [`crabka_remote_storage::InmemoryRemoteLogMetadataManager`], which silently
//! accepts writes that are then lost when the real manager replaces it.

use crabka_ids::LeaderEpoch;
use crabka_remote_storage::{
    RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate,
    RemotePartitionDeleteMetadata, RemoteStorageError, TopicIdPartition,
};

/// A [`RemoteLogMetadataManager`] that returns
/// [`RemoteStorageError::NotReady`] for every method.
///
/// Construct via [`NotReadyRlmm::new`] and pass it as the initial placeholder
/// to [`crate::SwappableRlmm::new`].
pub struct NotReadyRlmm;

impl NotReadyRlmm {
    /// Construct a new `NotReadyRlmm`.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for NotReadyRlmm {
    fn default() -> Self {
        Self::new()
    }
}

impl RemoteLogMetadataManager for NotReadyRlmm {
    fn add_remote_log_segment_metadata(
        &self,
        metadata: RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata
            .remote_log_segment_id()
            .topic_id_partition
            .partition;
        Err(RemoteStorageError::NotReady { partition })
    }

    fn update_remote_log_segment_metadata(
        &self,
        update: RemoteLogSegmentMetadataUpdate,
    ) -> Result<(), RemoteStorageError> {
        let partition = update.remote_log_segment_id.topic_id_partition.partition;
        Err(RemoteStorageError::NotReady { partition })
    }

    fn remote_log_segment_metadata(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: LeaderEpoch,
        _offset: i64,
    ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition,
        })
    }

    fn highest_offset_for_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: LeaderEpoch,
    ) -> Result<Option<i64>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition,
        })
    }

    fn list_remote_log_segments(
        &self,
        topic_id_partition: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition,
        })
    }

    fn list_remote_log_segments_by_epoch(
        &self,
        topic_id_partition: &TopicIdPartition,
        _leader_epoch: LeaderEpoch,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        Err(RemoteStorageError::NotReady {
            partition: topic_id_partition.partition,
        })
    }

    fn put_remote_partition_delete_metadata(
        &self,
        metadata: RemotePartitionDeleteMetadata,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata.topic_id_partition.partition;
        Err(RemoteStorageError::NotReady { partition })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_remote_storage::{
        RemoteLogSegmentId, RemoteLogSegmentState, RemotePartitionDeleteState,
    };
    use uuid::Uuid;

    use super::*;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 3)
    }

    fn seg_id(id: u128) -> RemoteLogSegmentId {
        RemoteLogSegmentId::new(tp(), Uuid::from_u128(id))
    }

    #[test]
    fn reads_return_not_ready_with_partition() {
        let m = NotReadyRlmm::new();
        let err = m
            .remote_log_segment_metadata(&tp(), LeaderEpoch(0), 0)
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));

        let err = m.list_remote_log_segments(&tp()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));

        let err = m
            .list_remote_log_segments_by_epoch(&tp(), LeaderEpoch(0))
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));

        let err = m
            .highest_offset_for_epoch(&tp(), LeaderEpoch(0))
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[test]
    fn add_returns_not_ready_with_partition() {
        let m = NotReadyRlmm::new();
        let md = RemoteLogSegmentMetadata::new(
            seg_id(10),
            0,
            99,
            100,
            1,
            100,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .unwrap();
        let err = m.add_remote_log_segment_metadata(md).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[test]
    fn update_returns_not_ready_with_partition() {
        let m = NotReadyRlmm::new();
        let upd = RemoteLogSegmentMetadataUpdate {
            remote_log_segment_id: seg_id(11),
            event_timestamp_ms: 1,
            custom_metadata: None,
            state: RemoteLogSegmentState::CopySegmentFinished,
            broker_id: 0,
        };
        let err = m.update_remote_log_segment_metadata(upd).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[test]
    fn put_partition_delete_returns_not_ready_with_partition() {
        let m = NotReadyRlmm::new();
        let del = RemotePartitionDeleteMetadata {
            topic_id_partition: tp(),
            state: RemotePartitionDeleteState::DeletePartitionMarked,
            event_timestamp_ms: 1,
            broker_id: 0,
        };
        let err = m.put_remote_partition_delete_metadata(del).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }
}
