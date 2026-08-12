//! The [`RemoteStorageManager`] SPI: copy / fetch / delete of segment data
//! and indexes to and from the remote tier.

use std::path::PathBuf;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use uuid::Uuid;

use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
};

/// The kinds of index a segment carries alongside its `.log` data.
///
/// Mirrors Kafka's `RemoteStorageManager.IndexType`. A
/// [`RemoteStorageManager`] copies all of these on
/// [`RemoteStorageManager::copy_log_segment_data`] and serves any of them
/// back on [`RemoteStorageManager::fetch_index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexType {
    /// Sparse offset → byte-position index (`.index`).
    Offset,
    /// Sparse timestamp → relative-offset index (`.timeindex`).
    Timestamp,
    /// Producer id snapshot (`.snapshot`).
    ProducerSnapshot,
    /// Leader-epoch checkpoint (`.leader_epoch_checkpoint` in Kafka's
    /// `LocalTieredStorage`).
    LeaderEpoch,
    /// Aborted-transaction index (`.txnindex`). It is optional. A segment
    /// with no aborted transactions has none.
    Transaction,
}

impl IndexType {
    /// The Kafka `LocalTieredStorage` filename suffix for this index type.
    /// Its remote leader-epoch artifact uses `.leader_epoch_checkpoint`,
    /// distinct from a partition log's local `leader-epoch-checkpoint` file.
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            IndexType::Offset => ".index",
            IndexType::Timestamp => ".timeindex",
            IndexType::ProducerSnapshot => ".snapshot",
            IndexType::LeaderEpoch => ".leader_epoch_checkpoint",
            IndexType::Transaction => ".txnindex",
        }
    }
}

/// Kafka renders UUIDs in URL-safe, unpadded Base64 for remote-tier paths.
pub(crate) fn kafka_uuid(uuid: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(uuid.as_bytes())
}

/// Directory name used by Kafka's `LocalTieredStorage` for a partition.
pub(crate) fn partition_dir_name(metadata: &RemoteLogSegmentMetadata) -> String {
    let tp = &metadata.remote_log_segment_id().topic_id_partition;
    format!("{}-{}-{}", tp.topic, tp.partition, kafka_uuid(tp.topic_id))
}

/// Filename used by Kafka's `LocalTieredStorage` for one segment artifact.
pub(crate) fn segment_file_name(metadata: &RemoteLogSegmentMetadata, suffix: &str) -> String {
    format!(
        "{:020}-{}{}",
        metadata.start_offset(),
        kafka_uuid(metadata.remote_log_segment_id().id),
        suffix
    )
}

/// The local files, and the in-memory leader-epoch bytes, that make up one
/// log segment for copy to the remote tier.
///
/// Mirrors Kafka's `LogSegmentData`. `transaction_index` is optional; a
/// segment with no aborted transactions has no `.txnindex` file.
/// `producer_snapshot_index` is optional too so third-party callers can copy
/// legacy segments that predate snapshots. krabka log exports provide one.
/// The broker passes the leader-epoch index as bytes and not as a path,
/// because it holds the relevant slice in memory at copy time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSegmentData {
    /// Path to the `.log` data file.
    pub log_segment: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index: PathBuf,
    /// Path to the `.txnindex` file, when present.
    pub transaction_index: Option<PathBuf>,
    /// Path to the producer-id `.snapshot` file, when present. Older segment
    /// sources can omit it; krabka log exports always provide one.
    pub producer_snapshot_index: Option<PathBuf>,
    /// Serialized leader-epoch index bytes for this segment's offset range.
    pub leader_epoch_index: Bytes,
}

/// SPI for the remote object store that holds offloaded segment data.
///
/// Implementations are **synchronous and blocking**. They mirror Kafka's
/// `RemoteStorageManager`, which the broker drives from a dedicated thread
/// pool, and the broker wraps these calls in `spawn_blocking`.
/// Implementations must be `Send + Sync` so the broker can share one instance
/// across tasks.
pub trait RemoteStorageManager: Send + Sync {
    /// Copies a segment's data and all of its indexes to the remote tier.
    ///
    /// Returns optional [`CustomMetadata`], for example an object-store key
    /// or a version id. The broker records it on the segment and passes it
    /// back on every later fetch or delete for that segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError`] if any underlying store operation
    /// fails.
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError>;

    /// Fetches a byte range of a segment's `.log` data.
    ///
    /// `start_position` is the inclusive starting byte offset within the
    /// segment. `end_position`, when `Some`, is the inclusive last byte
    /// offset; when `None`, the read runs to the end of the segment.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment is
    /// not present, or [`RemoteStorageError::Io`] on a store failure.
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Fetches one of a segment's indexes in full.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment or the
    /// requested index is not present, or [`RemoteStorageError::Io`] on a
    /// store failure.
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Deletes a segment's data and all of its indexes from the remote tier.
    ///
    /// Implementations must be idempotent: a delete of an absent segment
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::Io`] on a store failure.
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError>;
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn index_suffixes_match_kafka() {
        // Filesystem-backed stores key files off these exact suffixes.
        for (index_type, want) in [
            (IndexType::Offset, ".index"),
            (IndexType::Timestamp, ".timeindex"),
            (IndexType::ProducerSnapshot, ".snapshot"),
            (IndexType::LeaderEpoch, ".leader_epoch_checkpoint"),
            (IndexType::Transaction, ".txnindex"),
        ] {
            check!(index_type.suffix() == want, "{index_type:?}");
        }
    }

    #[test]
    fn local_tiered_storage_names_match_kafka() {
        use std::collections::BTreeMap;

        use crabka_ids::LeaderEpoch;

        use crate::metadata::{
            RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition,
        };

        let metadata = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 7),
                Uuid::from_u128(0xfe),
            ),
            11,
            19,
            0,
            1,
            0,
            RemoteLogSegmentDetails::new(
                1,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 11)]),
            ),
        )
        .unwrap();

        check!(partition_dir_name(&metadata) == "orders-7-AAAAAAAAAAAAAAAAAAAAAQ");
        check!(
            segment_file_name(&metadata, IndexType::ProducerSnapshot.suffix())
                == "00000000000000000011-AAAAAAAAAAAAAAAAAAAA_g.snapshot"
        );
    }
}
