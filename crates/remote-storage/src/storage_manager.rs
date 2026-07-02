//! The [`RemoteStorageManager`] SPI: copy / fetch / delete of segment data
//! and indexes to and from the remote tier.

use std::path::PathBuf;

use bytes::Bytes;

use crate::error::RemoteStorageError;
use crate::metadata::{CustomMetadata, RemoteLogSegmentMetadata};

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
    /// Leader-epoch checkpoint (`leader-epoch-checkpoint`).
    LeaderEpoch,
    /// Aborted-transaction index (`.txnindex`). Optional — a segment with
    /// no aborted transactions has none.
    Transaction,
}

impl IndexType {
    /// The conventional Kafka filename suffix for this index type, used by
    /// filesystem-backed stores. `LeaderEpoch` has no dotted suffix in
    /// Kafka; the reference store uses `.leader-epoch-checkpoint`.
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            IndexType::Offset => ".index",
            IndexType::Timestamp => ".timeindex",
            IndexType::ProducerSnapshot => ".snapshot",
            IndexType::LeaderEpoch => ".leader-epoch-checkpoint",
            IndexType::Transaction => ".txnindex",
        }
    }
}

/// The local files (and in-memory leader-epoch bytes) that make up one log
/// segment to be copied to the remote tier.
///
/// Mirrors Kafka's `LogSegmentData`. `transaction_index` is optional; a
/// segment with no aborted transactions has no `.txnindex` file.
/// `producer_snapshot_index` is optional too — Crabka does not yet write
/// producer-id snapshots, so it is typically `None` (Kafka always has one).
/// The leader-epoch index is passed as bytes (rather than a path) because
/// the broker holds the relevant slice in memory at copy time.
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
    /// Path to the producer-id `.snapshot` file, when present.
    pub producer_snapshot_index: Option<PathBuf>,
    /// Serialized leader-epoch index bytes for this segment's offset range.
    pub leader_epoch_index: Bytes,
}

/// SPI for the remote object store that holds offloaded segment data.
///
/// Implementations are **synchronous and blocking** — they mirror Kafka's
/// `RemoteStorageManager`, which the broker drives from a dedicated thread
/// pool (the broker wraps these calls in `spawn_blocking`). Implementations
/// must be `Send + Sync` so the broker can share one instance across tasks.
pub trait RemoteStorageManager: Send + Sync {
    /// Copy a segment's data and all of its indexes to the remote tier.
    ///
    /// Returns optional [`CustomMetadata`] (e.g. an object-store key or
    /// version id) that the broker records on the segment and passes back
    /// on every later fetch/delete for it.
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

    /// Fetch a byte range of a segment's `.log` data.
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

    /// Fetch one of a segment's indexes in full.
    ///
    /// # Errors
    ///
    /// Returns [`RemoteStorageError::SegmentNotFound`] if the segment (or
    /// the requested index) is not present, or [`RemoteStorageError::Io`]
    /// on a store failure.
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError>;

    /// Delete a segment's data and all of its indexes from the remote tier.
    /// Implementations must be idempotent: deleting an absent segment
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
    use super::*;
    use assert2::check;

    #[test]
    fn index_suffixes_match_kafka() {
        // Filesystem-backed stores key files off these exact suffixes.
        for (index_type, want) in [
            (IndexType::Offset, ".index"),
            (IndexType::Timestamp, ".timeindex"),
            (IndexType::ProducerSnapshot, ".snapshot"),
            (IndexType::LeaderEpoch, ".leader-epoch-checkpoint"),
            (IndexType::Transaction, ".txnindex"),
        ] {
            check!(index_type.suffix() == want, "{index_type:?}");
        }
    }
}
