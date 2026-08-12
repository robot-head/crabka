//! [`LocalTieredStorage`] is a filesystem-backed reference
//! [`RemoteStorageManager`]. It mirrors Kafka's test fixture of the same
//! name. It uses the same partition directories and segment filenames as
//! Kafka 4.0, so a JVM `LocalTieredStorage` can read files copied by Crabka.
//! It is useful for tests and single-node setups. Production deployments use
//! an object-store-backed implementation behind the same trait.

use std::{fs, path::PathBuf};

use tracing::instrument;

use crate::{
    error::RemoteStorageError,
    metadata::{CustomMetadata, RemoteLogSegmentMetadata},
    storage_manager::{IndexType, LogSegmentData, RemoteStorageManager},
};

/// A [`RemoteStorageManager`] that keeps offloaded segments on a local
/// filesystem under `root`.
///
/// On-disk layout, per segment:
///
/// ```text
/// <root>/<topic>-<partition>-<topic_id_base64>/
///     <base_offset>-<segment_id_base64>.log
///     <base_offset>-<segment_id_base64>.index
///     <base_offset>-<segment_id_base64>.timeindex
///     <base_offset>-<segment_id_base64>.snapshot
///     <base_offset>-<segment_id_base64>.leader_epoch_checkpoint
///     <base_offset>-<segment_id_base64>.txnindex  (when present)
/// ```
#[derive(Debug, Clone)]
pub struct LocalTieredStorage {
    root: PathBuf,
}

impl LocalTieredStorage {
    /// Constructs a store rooted at `root`. The store creates the directory
    /// on the first copy.
    ///
    /// Kafka's JVM implementation appends `kafka-tiered-storage` to its
    /// configured parent directory. To share a tier with it, pass
    /// `<remote.log.storage.local.dir>/kafka-tiered-storage` here.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The directory that holds every remote segment for one partition.
    fn partition_dir(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.root
            .join(crate::storage_manager::partition_dir_name(metadata))
    }

    fn segment_path(&self, metadata: &RemoteLogSegmentMetadata, suffix: &str) -> PathBuf {
        self.partition_dir(metadata)
            .join(crate::storage_manager::segment_file_name(metadata, suffix))
    }

    fn log_path(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.segment_path(metadata, ".log")
    }

    fn index_path(&self, metadata: &RemoteLogSegmentMetadata, index_type: IndexType) -> PathBuf {
        self.segment_path(metadata, index_type.suffix())
    }

    /// Crabka 0.3.8 and earlier stored one directory per segment. Keep reads
    /// and deletes compatible while all new copies use Kafka's flat layout.
    fn legacy_segment_dir(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        let id = metadata.remote_log_segment_id();
        self.root
            .join(format!(
                "{}_{}",
                id.topic_id_partition.topic_id, id.topic_id_partition.partition
            ))
            .join(id.id.to_string())
    }

    fn legacy_log_path(&self, metadata: &RemoteLogSegmentMetadata) -> PathBuf {
        self.legacy_segment_dir(metadata).join("log")
    }

    fn legacy_index_path(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> PathBuf {
        let name = match index_type {
            IndexType::Offset => "offset_index",
            IndexType::Timestamp => "time_index",
            IndexType::ProducerSnapshot => "producer_snapshot",
            IndexType::LeaderEpoch => "leader_epoch",
            IndexType::Transaction => "txn_index",
        };
        self.legacy_segment_dir(metadata).join(name)
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
        let dir = self.partition_dir(metadata);
        fs::create_dir_all(&dir)?;

        fs::copy(&data.log_segment, self.log_path(metadata))?;
        fs::copy(
            &data.offset_index,
            self.index_path(metadata, IndexType::Offset),
        )?;
        fs::copy(
            &data.time_index,
            self.index_path(metadata, IndexType::Timestamp),
        )?;
        if let Some(snapshot) = &data.producer_snapshot_index {
            fs::copy(
                snapshot,
                self.index_path(metadata, IndexType::ProducerSnapshot),
            )?;
        }
        fs::write(
            self.index_path(metadata, IndexType::LeaderEpoch),
            &data.leader_epoch_index,
        )?;
        if let Some(txn) = &data.transaction_index {
            fs::copy(txn, self.index_path(metadata, IndexType::Transaction))?;
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
        let path = self.log_path(metadata);
        let path = if path.exists() {
            path
        } else {
            self.legacy_log_path(metadata)
        };
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
        let path = self.index_path(metadata, index_type);
        let path = if path.exists() {
            path
        } else {
            self.legacy_index_path(metadata, index_type)
        };
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
        for path in [
            self.log_path(metadata),
            self.index_path(metadata, IndexType::Offset),
            self.index_path(metadata, IndexType::Timestamp),
            self.index_path(metadata, IndexType::ProducerSnapshot),
            self.index_path(metadata, IndexType::LeaderEpoch),
            self.index_path(metadata, IndexType::Transaction),
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(RemoteStorageError::Io(error)),
            }
        }
        match fs::remove_dir_all(self.legacy_segment_dir(metadata)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(RemoteStorageError::Io(error)),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, io::Write, path::Path};

    use assert2::{assert, check};
    use bytes::Bytes;
    use crabka_ids::LeaderEpoch;
    use uuid::Uuid;

    use super::*;
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
            crate::metadata::RemoteLogSegmentDetails::new(
                8,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0)]),
            ),
        )
        .unwrap()
    }

    /// Writes `contents` to a fresh temp file under `dir` and returns its path.
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
    fn copied_files_use_kafka_local_tiered_storage_layout() {
        let remote = tempfile::tempdir().unwrap();
        let src = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        rsm.copy_log_segment_data(&md, &sample_data(src.path(), true))
            .unwrap();

        let partition = remote.path().join("orders-0-AAAAAAAAAAAAAAAAAAAAAQ");
        for suffix in [
            ".log",
            ".index",
            ".timeindex",
            ".snapshot",
            ".leader_epoch_checkpoint",
            ".txnindex",
        ] {
            check!(
                partition
                    .join(format!(
                        "00000000000000000000-AAAAAAAAAAAAAAAAAAAACg{suffix}"
                    ))
                    .is_file(),
                "missing Kafka layout artifact {suffix}"
            );
        }
    }

    #[test]
    fn reads_and_deletes_pre_kafka_layout_segments() {
        let remote = tempfile::tempdir().unwrap();
        let rsm = LocalTieredStorage::new(remote.path());
        let md = metadata(10);
        let legacy = rsm.legacy_segment_dir(&md);
        fs::create_dir_all(&legacy).unwrap();
        fs::write(legacy.join("log"), b"legacy-log").unwrap();
        fs::write(legacy.join("producer_snapshot"), b"legacy-snapshot").unwrap();

        check!(rsm.fetch_log_segment(&md, 0, None).unwrap() == b"legacy-log");
        check!(rsm.fetch_index(&md, IndexType::ProducerSnapshot).unwrap() == b"legacy-snapshot");
        rsm.delete_log_segment_data(&md).unwrap();
        check!(!legacy.exists());
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
