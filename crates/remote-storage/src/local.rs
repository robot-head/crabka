//! [`LocalTieredStorage`] — a filesystem-backed reference
//! [`RemoteStorageManager`], mirroring Kafka's test fixture of the same
//! name. Stores each segment's data + indexes under a per-segment directory
//! rooted at a configurable path. Useful for tests and single-node setups;
//! production deployments swap in an object-store-backed implementation
//! behind the same trait.

use std::fs;
use std::path::{Path, PathBuf};

use tracing::instrument;

use crate::error::RemoteStorageError;
use crate::metadata::{CustomMetadata, RemoteLogSegmentMetadata};
use crate::storage_manager::{IndexType, LogSegmentData, RemoteStorageManager};

/// A [`RemoteStorageManager`] that keeps offloaded segments on a local
/// filesystem under `root`.
///
/// On-disk layout, per segment:
///
/// ```text
/// <root>/<topic_id>_<partition>/<segment_uuid>/
///     log
///     offset_index
///     time_index
///     producer_snapshot
///     leader_epoch
///     txn_index        (only when the segment has a transaction index)
/// ```
#[derive(Debug, Clone)]
pub struct LocalTieredStorage {
    root: PathBuf,
}

impl LocalTieredStorage {
    /// Construct a store rooted at `root`. The directory is created lazily
    /// on the first copy.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory holding all of one segment's files.
    fn segment_dir(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        let id = metadata.remote_log_segment_id();
        let tp = &id.topic_id_partition;
        self.root
            .join(format!("{}_{}", tp.topic_id, tp.partition))
            .join(id.id.to_string())
    }

    fn log_path(dir: &Path) -> PathBuf {
        dir.join("log")
    }

    fn index_path(dir: &Path, index_type: IndexType) -> PathBuf {
        let name = match index_type {
            IndexType::Offset => "offset_index",
            IndexType::Timestamp => "time_index",
            IndexType::ProducerSnapshot => "producer_snapshot",
            IndexType::LeaderEpoch => "leader_epoch",
            IndexType::Transaction => "txn_index",
        };
        dir.join(name)
    }
}

impl RemoteStorageManager for LocalTieredStorage {
    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_offset = metadata.start_offset(),
            end_offset = metadata.end_offset(),
        ),
        err
    )]
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        let dir = self.segment_dir(metadata);
        fs::create_dir_all(&dir)?;

        fs::copy(&data.log_segment, Self::log_path(&dir))?;
        fs::copy(
            &data.offset_index,
            Self::index_path(&dir, IndexType::Offset),
        )?;
        fs::copy(
            &data.time_index,
            Self::index_path(&dir, IndexType::Timestamp),
        )?;
        if let Some(snapshot) = &data.producer_snapshot_index {
            fs::copy(
                snapshot,
                Self::index_path(&dir, IndexType::ProducerSnapshot),
            )?;
        }
        fs::write(
            Self::index_path(&dir, IndexType::LeaderEpoch),
            &data.leader_epoch_index,
        )?;
        if let Some(txn) = &data.transaction_index {
            fs::copy(txn, Self::index_path(&dir, IndexType::Transaction))?;
        }
        // A local store needs no opaque key echoed back.
        Ok(None)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            start_position,
            end_position = ?end_position,
        ),
        err
    )]
    fn fetch_log_segment(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let dir = self.segment_dir(metadata);
        let path = Self::log_path(&dir);
        if !path.exists() {
            return Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ));
        }
        let bytes = fs::read(&path)?;
        let len = bytes.len();
        let start = usize::try_from(start_position).expect("u32 fits usize");
        if start > len {
            return Err(RemoteStorageError::InvalidArgument(format!(
                "start_position {start} exceeds segment length {len}"
            )));
        }
        let end_exclusive = match end_position {
            Some(end) => {
                let end = usize::try_from(end).expect("u32 fits usize");
                if end < start {
                    return Err(RemoteStorageError::InvalidArgument(format!(
                        "end_position {end} < start_position {start}"
                    )));
                }
                // `end` is inclusive; clamp to the segment length.
                end.saturating_add(1).min(len)
            }
            None => len,
        };
        Ok(bytes[start..end_exclusive].to_vec())
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
            index_type = ?index_type,
        ),
        err
    )]
    fn fetch_index(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let dir = self.segment_dir(metadata);
        let path = Self::index_path(&dir, index_type);
        if !path.exists() {
            return Err(RemoteStorageError::SegmentNotFound(
                metadata.remote_log_segment_id().clone(),
            ));
        }
        Ok(fs::read(&path)?)
    }

    #[instrument(
        skip_all,
        fields(
            topic_id = %metadata.remote_log_segment_id().topic_id_partition.topic_id,
            partition = metadata.remote_log_segment_id().topic_id_partition.partition,
            segment = %metadata.remote_log_segment_id().id,
        ),
        err
    )]
    fn delete_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
    ) -> Result<(), RemoteStorageError> {
        let dir = self.segment_dir(metadata);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(()),
            // Idempotent: deleting an absent segment succeeds.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(RemoteStorageError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use assert2::check;
    use std::collections::BTreeMap;
    use std::io::Write;

    use bytes::Bytes;
    use uuid::Uuid;

    use crate::metadata::{RemoteLogSegmentId, RemoteLogSegmentState, TopicIdPartition};

    fn metadata(id: u128) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(id),
            ),
            0,
            99,
            123,
            1,
            456,
            8,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0, 0)]),
        )
        .unwrap()
    }

    /// Write `contents` to a fresh temp file under `dir` and return its path.
    fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
        let p = dir.join(name);
        let mut f = fs::File::create(&p).unwrap();
        f.write_all(contents).unwrap();
        p
    }

    fn sample_data(src: &Path, with_txn: bool) -> LogSegmentData {
        LogSegmentData {
            log_segment: write_file(src, "00.log", b"0123456789"),
            offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
            time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
            transaction_index: with_txn.then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
            producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
            leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
        }
    }

    #[test]
    fn copy_then_fetch_full_segment() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        assert!(
            rsm.copy_log_segment_data(&md, &sample_data(src.path(), true))
                .unwrap()
                .is_none()
        );
        let full = rsm.fetch_log_segment(&md, 0, None).unwrap();
        assert!(full == b"0123456789");
    }

    #[test]
    fn fetch_partial_byte_ranges() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), false))
            .unwrap();
        for (start, end, want) in [
            // Inclusive [2, 5] -> "2345".
            (2, Some(5), b"2345".as_ref()),
            // Open-ended from 7 -> "789".
            (7, None, b"789".as_ref()),
            // End past EOF clamps.
            (8, Some(99), b"89".as_ref()),
            // Start at EOF -> empty.
            (10, None, b"".as_ref()),
        ] {
            check!(
                rsm.fetch_log_segment(&md, start, end).unwrap() == want,
                "range [{start}, {end:?}]"
            );
        }
    }

    #[test]
    fn fetch_single_byte_range_start_equals_end() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), false))
            .unwrap();
        // Inclusive [3, 3] is a valid single-byte range -> "3". (The guard is
        // `end < start`, not `<=`/`==`, so an equal start/end must succeed.)
        assert!(rsm.fetch_log_segment(&md, 3, Some(3)).unwrap() == b"3");
    }

    #[test]
    fn fetch_each_index_type() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();
        for (index_type, want) in [
            (IndexType::Offset, b"OFFSET-IDX".as_ref()),
            (IndexType::Timestamp, b"TIME-IDX".as_ref()),
            (IndexType::ProducerSnapshot, b"SNAP".as_ref()),
            (IndexType::LeaderEpoch, b"EPOCH-BYTES".as_ref()),
            (IndexType::Transaction, b"TXN-IDX".as_ref()),
        ] {
            check!(
                rsm.fetch_index(&md, index_type).unwrap() == want,
                "{index_type:?}"
            );
        }
    }

    #[test]
    fn missing_optional_txn_index_is_not_found() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), false))
            .unwrap();
        let err = rsm.fetch_index(&md, IndexType::Transaction).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn fetch_before_copy_is_not_found() {
        let remote = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(404);
        let err = rsm.fetch_log_segment(&md, 0, None).unwrap_err();
        assert!(matches!(err, RemoteStorageError::SegmentNotFound(_)));
    }

    #[test]
    fn delete_is_idempotent_and_removes_data() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();
        rsm.delete_log_segment_data(&md).unwrap();
        // Second delete is a no-op.
        rsm.delete_log_segment_data(&md).unwrap();
        assert!(matches!(
            rsm.fetch_log_segment(&md, 0, None).unwrap_err(),
            RemoteStorageError::SegmentNotFound(_)
        ));
    }

    #[test]
    fn segments_are_isolated_by_id() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let a = metadata(10);
        let b = metadata(11);
        rsm.copy_log_segment_data(&a, &sample_data(src.path(), false))
            .unwrap();
        rsm.copy_log_segment_data(&b, &sample_data(src.path(), false))
            .unwrap();
        rsm.delete_log_segment_data(&a).unwrap();
        // Deleting `a` leaves `b` intact.
        assert!(rsm.fetch_log_segment(&b, 0, None).unwrap() == b"0123456789");
    }
}
