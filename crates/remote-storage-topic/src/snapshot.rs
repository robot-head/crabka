//! On-disk RLMM snapshot: a versioned envelope wrapping a
//! [`RlmmCacheDump`] plus the per-metadata-partition committed
//! offsets, so a restarting broker resumes the metadata consumer from
//! `committed + 1` instead of replaying `__remote_log_metadata` from
//! offset 0.
//!
//! The per-segment / per-partition-delete encoding reuses the
//! [`MetadataEvent`] codec; the envelope
//! adds a format version, the committed-offsets vector, and
//! length-prefixed entries. Writes are atomic (temp file + rename) so
//! a crash mid-write never yields a torn snapshot; a corrupt or
//! truncated file decodes to an error (never a panic), and the caller
//! falls back to a full replay.

use std::path::Path;

use bytes::{BufMut, BytesMut};
use crabka_remote_storage::{
    PartitionDump, RemoteLogSegmentMetadata, RemotePartitionDeleteState, RlmmCacheDump,
    TopicIdPartition,
};
use tracing::instrument;

use crate::{
    error::{CodecError, SnapshotError},
    serde::{MetadataEvent, Reader, read_uvarint, write_uvarint},
};

/// Format version at the head of every snapshot file. Greenfield: bump
/// freely, no backward-compat decoder arms.
pub const SNAPSHOT_FORMAT_VERSION: u16 = 0;

/// Default snapshot file name under the snapshot directory.
pub const SNAPSHOT_FILE_NAME: &str = "snapshot";

/// A decoded snapshot: the per-metadata-partition committed offsets and
/// the cache dump to seed an [`InmemoryRemoteLogMetadataManager`].
///
/// [`InmemoryRemoteLogMetadataManager`]: crabka_remote_storage::InmemoryRemoteLogMetadataManager
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Highest offset applied into the cache per metadata partition,
    /// indexed by metadata-partition. `committed[p] == -1` means the
    /// snapshot covers no events for `p`.
    pub committed_offsets: Vec<i64>,
    /// The cache contents at the moment the snapshot was taken.
    pub dump: RlmmCacheDump,
}

impl Snapshot {
    /// Encode this snapshot into freshly-allocated bytes.
    #[must_use]
    /// # Panics
    /// Panics if an internal lock is poisoned or validated block metadata is inconsistent with its index.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = BytesMut::with_capacity(256);
        buf.put_u16(SNAPSHOT_FORMAT_VERSION);
        // Committed offsets.
        write_uvarint(self.committed_offsets.len() as u64, &mut buf);
        for (p, &off) in self.committed_offsets.iter().enumerate() {
            buf.put_i32(i32::try_from(p).expect("partition fits in i32"));
            buf.put_i64(off);
        }
        // Entries: one MetadataEvent per segment, then one per
        // partition-delete (so import sees segments before delete state).
        let mut entries: Vec<bytes::Bytes> = Vec::new();
        for p in &self.dump.partitions {
            for seg in &p.segments {
                entries.push(MetadataEvent::AddSegment(seg.clone()).encode());
            }
            if let Some(state) = p.delete_state {
                entries.push(
                    MetadataEvent::PartitionDelete(
                        crabka_remote_storage::RemotePartitionDeleteMetadata {
                            topic_id_partition: p.topic_id_partition.clone(),
                            state,
                            event_timestamp_ms: 0,
                            broker_id: 0,
                        },
                    )
                    .encode(),
                );
            }
        }
        write_uvarint(entries.len() as u64, &mut buf);
        for entry in entries {
            write_uvarint(entry.len() as u64, &mut buf);
            buf.put_slice(&entry);
        }
        buf.to_vec()
    }

    /// Decode a snapshot from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for any malformed input — bad version,
    /// short/truncated buffer, a contained event that fails to decode,
    /// or trailing bytes after the declared entries.
    /// # Panics
    /// Panics if an internal lock is poisoned or validated block metadata is inconsistent with its index.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        use std::collections::BTreeMap;

        let mut r = Reader::new(bytes);
        let version = read_u16(&mut r)?;
        if version != SNAPSHOT_FORMAT_VERSION {
            return Err(SnapshotError::UnsupportedVersion(version));
        }
        let n_offsets = usize::try_from(read_uvarint(&mut r)?)
            .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
        let mut committed_offsets = vec![-1i64; n_offsets];
        for _ in 0..n_offsets {
            let p = r.read_i32()?;
            let off = r.read_i64()?;
            let idx = usize::try_from(p).map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
            if idx >= committed_offsets.len() {
                return Err(SnapshotError::Malformed(CodecError::LengthOverflow(
                    idx as u64,
                )));
            }
            committed_offsets[idx] = off;
        }
        let n_entries = usize::try_from(read_uvarint(&mut r)?)
            .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;

        // Accumulate per-partition while preserving first-seen order.
        let mut order: Vec<TopicIdPartition> = Vec::new();
        let mut by_tp: BTreeMap<
            (uuid::Uuid, i32),
            (
                Vec<RemoteLogSegmentMetadata>,
                Option<RemotePartitionDeleteState>,
            ),
        > = BTreeMap::new();
        for _ in 0..n_entries {
            let len = usize::try_from(read_uvarint(&mut r)?)
                .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
            let raw = r.read_n(len)?;
            match MetadataEvent::decode(raw)? {
                MetadataEvent::AddSegment(md) => {
                    let tp = md.remote_log_segment_id().topic_id_partition.clone();
                    let key = (tp.topic_id, tp.partition);
                    if !by_tp.contains_key(&key) {
                        order.push(tp.clone());
                    }
                    by_tp.entry(key).or_default().0.push(md);
                }
                MetadataEvent::PartitionDelete(d) => {
                    let key = (
                        d.topic_id_partition.topic_id,
                        d.topic_id_partition.partition,
                    );
                    if !by_tp.contains_key(&key) {
                        order.push(d.topic_id_partition.clone());
                    }
                    by_tp.entry(key).or_default().1 = Some(d.state);
                }
                MetadataEvent::UpdateSegment(_) => {
                    // Snapshots only ever encode Add + PartitionDelete;
                    // segment updates are folded into the dumped cache, not
                    // stored as standalone events.
                    return Err(SnapshotError::Malformed(CodecError::Domain(
                        "snapshot must not contain an UpdateSegment event".to_string(),
                    )));
                }
            }
        }
        if r.remaining() != 0 {
            return Err(SnapshotError::TrailingBytes(r.remaining()));
        }
        let partitions = order
            .into_iter()
            .map(|tp| {
                let key = (tp.topic_id, tp.partition);
                let (segments, delete_state) = by_tp.remove(&key).expect("key present");
                PartitionDump {
                    topic_id_partition: tp,
                    segments,
                    delete_state,
                }
            })
            .collect();
        Ok(Self {
            committed_offsets,
            dump: RlmmCacheDump { partitions },
        })
    }

    /// Atomically write this snapshot to `path`: write to a sibling temp
    /// file, fsync the file, rename over `path`, then fsync the parent
    /// directory so the rename itself is durable. A crash at any point
    /// leaves either the old snapshot or none — never a torn file. The
    /// temp file is removed if any step before the rename fails, so a
    /// persistent error does not litter a stale `.tmp`.
    ///
    /// The directory fsync is best-effort: filesystems that do not support
    /// it (or platforms where opening a directory fails) are ignored
    /// rather than failing the write.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] on any filesystem failure up to and
    /// including the rename.
    #[instrument(
        skip_all,
        fields(path = %path.display(), partitions = self.dump.partitions.len()),
        err
    )]
    pub fn write_atomic(&self, path: &Path) -> Result<(), SnapshotError> {
        use std::io::Write;
        let bytes = self.encode();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let tmp = path.with_extension("tmp");
        let write_tmp = || -> Result<(), SnapshotError> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            std::fs::rename(&tmp, path)?;
            Ok(())
        };
        if let Err(e) = write_tmp() {
            // Don't leave a stale temp file behind on a failed write.
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        // Best-effort durable rename: fsync the parent directory so the
        // rename survives a crash. Ignore unsupported/not-a-dir cases.
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Load a snapshot from `path`. `Ok(None)` when the file does not
    /// exist (first boot); `Err` when the file exists but is corrupt;
    /// `Ok(Some)` on success.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] for read failures other than
    /// not-found, or a decode error for a present-but-malformed file.
    #[instrument(skip_all, fields(path = %path.display()), err)]
    pub fn load(path: &Path) -> Result<Option<Self>, SnapshotError> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SnapshotError::Io(e)),
        };
        Ok(Some(Self::decode(&bytes)?))
    }
}

fn read_u16(r: &mut Reader<'_>) -> Result<u16, CodecError> {
    let hi = u16::from(r.read_u8()?);
    let lo = u16::from(r.read_u8()?);
    Ok((hi << 8) | lo)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use crabka_ids::LeaderEpoch;
    use crabka_remote_storage::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState,
        RemotePartitionDeleteState, TopicIdPartition,
    };
    use uuid::Uuid;

    use super::*;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn started(id: u128, start: i64, end: i64) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            end + 1,
            1,
            100,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), start)]),
            ),
        )
        .unwrap()
    }

    fn sample_snapshot() -> Snapshot {
        let dump = RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: tp(),
                segments: vec![started(10, 0, 99), started(11, 100, 199)],
                delete_state: Some(RemotePartitionDeleteState::DeletePartitionMarked),
            }],
        };
        Snapshot {
            committed_offsets: vec![5, -1, 2],
            dump,
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let snap = sample_snapshot();
        let bytes = snap.encode();
        let back = Snapshot::decode(&bytes).expect("decodes");
        assert!(back == snap);
    }

    #[test]
    fn truncated_file_is_error_not_panic() {
        let bytes = sample_snapshot().encode();
        let err = Snapshot::decode(&bytes[..bytes.len() - 3]).unwrap_err();
        assert!(matches!(
            err,
            SnapshotError::Malformed(_) | SnapshotError::TrailingBytes(_)
        ));
    }

    #[test]
    fn garbage_bytes_are_error_not_panic() {
        let err = Snapshot::decode(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap_err();
        assert!(matches!(
            err,
            SnapshotError::UnsupportedVersion(_) | SnapshotError::Malformed(_)
        ));
    }

    #[test]
    fn empty_buffer_is_error_not_panic() {
        let err = Snapshot::decode(&[]).unwrap_err();
        assert!(matches!(err, SnapshotError::Malformed(_)));
    }

    #[test]
    fn write_then_load_round_trips_through_a_file() {
        let dir = std::env::temp_dir().join(format!("crabka-snap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot");
        let snap = sample_snapshot();
        snap.write_atomic(&path).expect("write");
        let loaded = Snapshot::load(&path).expect("load").expect("present");
        assert!(loaded == snap);
        // No temp file left behind.
        assert!(
            std::fs::read_dir(&dir)
                .unwrap()
                .filter_map(Result::ok)
                .all(|e| e.file_name() == "snapshot")
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn load_absent_file_is_ok_none() {
        let path = std::env::temp_dir().join("crabka-snap-does-not-exist-xyz");
        let _ = std::fs::remove_file(&path);
        assert!(Snapshot::load(&path).unwrap() == None);
    }

    #[test]
    fn load_corrupt_file_is_err() {
        let dir = std::env::temp_dir().join(format!("crabka-snap-corrupt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot");
        std::fs::write(&path, [0xFF, 0xFF, 0x00, 0x01]).unwrap();
        assert!(Snapshot::load(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
