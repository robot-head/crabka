//! KIP-405: remote read path. Wraps the broker's shared
//! [`RemoteStorageManager`] + [`RemoteLogMetadataManager`] pair and serves
//! `Fetch` / `ListOffsets` requests for offsets that no longer have a local
//! copy.
//!
//! The RSM SPI is synchronous + blocking; this module wraps every byte-range
//! and index read in `tokio::task::spawn_blocking` so the broker's reactor
//! never stalls on remote-tier I/O. The pure index-decode helpers mirror
//! `crabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the
//! Kafka-format index bytes the copy path (48b) wrote out verbatim.

use std::sync::Arc;

use crabka_protocol::records::RecordBatch;
use crabka_remote_storage::{
    IndexType, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RemoteStorageError, RemoteStorageManager, TopicIdPartition,
};
use tracing::warn;
use zerocopy::byteorder::{I64, U32};
use zerocopy::{BigEndian, FromBytes, Immutable, KnownLayout, Unaligned};

/// 8 bytes per entry: rel u32 BE + pos u32 BE. Mirrors
/// `crabka_log::index::OffsetEntryRaw` so the remote-tier copy of an
/// `OffsetIndex` file decodes through the same byte layout the local index
/// was written with.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct OffsetIndexEntry {
    relative_offset: U32<BigEndian>,
    position: U32<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<OffsetIndexEntry>() == 8);

/// 12 bytes per entry: ts i64 BE + rel u32 BE. Mirrors
/// `crabka_log::index::TimeEntryRaw`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TimeIndexEntry {
    timestamp: I64<BigEndian>,
    relative_offset: U32<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<TimeIndexEntry>() == 12);

/// Holds the broker's shared `RSM` + `RLMM` and serves remote reads.
pub(crate) struct RemoteReader {
    pub(crate) rsm: Arc<dyn RemoteStorageManager>,
    pub(crate) rlmm: Arc<dyn RemoteLogMetadataManager>,
}

impl RemoteReader {
    pub(crate) fn new(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
    ) -> Self {
        Self { rsm, rlmm }
    }

    /// Find the finished segment in the RLMM covering `(leader_epoch, offset)`,
    /// fetch its offset index, position into the `.log` data, and return the
    /// first batch whose last offset is `>= offset`. `None` when no finished
    /// segment covers the requested offset.
    ///
    /// `max_bytes` caps the byte range fetched from the remote tier; the
    /// caller's `partition_max_bytes` from the Fetch request flows in here.
    pub(crate) async fn fetch_batch(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>, RemoteStorageError> {
        let Some(metadata) = self
            .rlmm
            .remote_log_segment_metadata(tp, leader_epoch, offset)?
        else {
            return Ok(None);
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(None);
        }

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Offset)
            .await?;
        let entries = parse_offset_index(&index_bytes);
        let target_rel = u32::try_from((offset - metadata.start_offset()).max(0)).unwrap_or(0);
        let start_position = position_for_relative_offset(&entries, target_rel);

        // Cap the read so the broker doesn't pull an entire segment when the
        // Fetch asked for one batch. Always pull at least one full batch worth
        // of bytes — the segment's `size_bytes` is the safe ceiling.
        let segment_size =
            u32::try_from(metadata.segment_size_in_bytes().max(0)).unwrap_or(u32::MAX);
        let end_position = end_position_for(start_position, segment_size, max_bytes);

        let data = self
            .fetch_log_blocking(metadata.clone(), start_position, end_position)
            .await?;

        let batch = first_batch_at_or_after(&data, offset);
        Ok(batch)
    }

    /// Lowest `start_offset` across finished segments for `tp`, or `None` when
    /// no finished segment exists. Drives `ListOffsets` EARLIEST below
    /// `local_log_start_offset()`.
    pub(crate) fn earliest_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let listed = self.rlmm.list_remote_log_segments(tp)?;
        Ok(listed
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(|md| md.start_offset())
            .min())
    }

    /// Smallest absolute offset whose record timestamp is `>= target_timestamp`
    /// across finished remote segments. Walks segments oldest-first, finds the
    /// first whose `max_timestamp >= target_timestamp`, fetches that segment's
    /// time index, and returns `start_offset + relative_offset_for_timestamp`.
    /// Returns `None` when no finished remote segment qualifies.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: i64,
    ) -> Result<Option<i64>, RemoteStorageError> {
        let mut listed = self.rlmm.list_remote_log_segments(tp)?;
        listed.retain(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished);
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);

        let Some(metadata) = listed
            .into_iter()
            .find(|md| md.max_timestamp_ms() >= target_timestamp)
        else {
            return Ok(None);
        };

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Timestamp)
            .await?;
        let entries = parse_time_index(&index_bytes);
        let Some(rel) = relative_offset_for_timestamp(&entries, target_timestamp) else {
            // No entry past the target — the first record in the segment is
            // the conservative answer.
            return Ok(Some(metadata.start_offset()));
        };
        Ok(Some(metadata.start_offset() + i64::from(rel)))
    }

    async fn fetch_index_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        kind: IndexType,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        match tokio::task::spawn_blocking(move || rsm.fetch_index(&metadata, kind)).await {
            Ok(res) => res,
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_index task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_index task panicked",
                )))
            }
        }
    }

    async fn fetch_log_blocking(
        &self,
        metadata: RemoteLogSegmentMetadata,
        start_position: u32,
        end_position: Option<u32>,
    ) -> Result<Vec<u8>, RemoteStorageError> {
        let rsm = self.rsm.clone();
        match tokio::task::spawn_blocking(move || {
            rsm.fetch_log_segment(&metadata, start_position, end_position)
        })
        .await
        {
            Ok(res) => res,
            Err(e) => {
                warn!(error = %e, "remote-reader: fetch_log_segment task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "fetch_log_segment task panicked",
                )))
            }
        }
    }
}

/// Compute the inclusive `end_position` for a remote byte-range fetch.
///
/// Returns `None` (read to end of segment) when `start_position` plus
/// `max_bytes` would reach or exceed `segment_size`. Otherwise the inclusive
/// last byte to read.
pub(crate) fn end_position_for(
    start_position: u32,
    segment_size: u32,
    max_bytes: usize,
) -> Option<u32> {
    if max_bytes == 0 {
        return None;
    }
    let max_bytes_u32 = u32::try_from(max_bytes).unwrap_or(u32::MAX);
    let exclusive_end = start_position.saturating_add(max_bytes_u32);
    if exclusive_end >= segment_size {
        None
    } else {
        Some(exclusive_end.saturating_sub(1))
    }
}

/// Parse Kafka's `OffsetIndex` on-disk format (8 bytes / entry: rel u32 BE +
/// pos u32 BE).
#[must_use]
pub(crate) fn parse_offset_index(bytes: &[u8]) -> Vec<(u32, u32)> {
    let truncated_len = (bytes.len() / 8) * 8;
    let entries = <[OffsetIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .expect("len is multiple of 8 and OffsetIndexEntry is Unaligned");
    entries
        .iter()
        .map(|e| (e.relative_offset.get(), e.position.get()))
        .collect()
}

/// Floor lookup: byte position of the largest entry with `rel <= target_rel`,
/// or 0 when empty / target is before the first entry.
#[must_use]
pub(crate) fn position_for_relative_offset(entries: &[(u32, u32)], target_rel: u32) -> u32 {
    match entries.binary_search_by_key(&target_rel, |&(rel, _)| rel) {
        Ok(i) => entries[i].1,
        Err(0) => 0,
        Err(i) => entries[i - 1].1,
    }
}

/// Parse Kafka's `TimeIndex` on-disk format (12 bytes / entry: ts i64 BE + rel
/// u32 BE).
#[must_use]
pub(crate) fn parse_time_index(bytes: &[u8]) -> Vec<(i64, u32)> {
    let truncated_len = (bytes.len() / 12) * 12;
    let entries = <[TimeIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .expect("len is multiple of 12 and TimeIndexEntry is Unaligned");
    entries
        .iter()
        .map(|e| (e.timestamp.get(), e.relative_offset.get()))
        .collect()
}

/// First entry whose `ts >= target_ts`, returning the relative offset, or
/// `None` when none qualify.
#[must_use]
pub(crate) fn relative_offset_for_timestamp(entries: &[(i64, u32)], target_ts: i64) -> Option<u32> {
    entries
        .iter()
        .find(|(ts, _)| *ts >= target_ts)
        .map(|(_, rel)| *rel)
}

/// Decode batches from `data` and return the first one whose last offset is
/// `>= floor`. Used to skip past batches at the start of the returned byte
/// range that the offset-index pointed at but that don't actually cover the
/// requested offset (because Kafka offset indexes are sparse).
fn first_batch_at_or_after(data: &[u8], floor: i64) -> Option<RecordBatch> {
    let mut cur: &[u8] = data;
    while !cur.is_empty() {
        let Ok(batch) = RecordBatch::decode(&mut cur) else {
            break;
        };
        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        if last_offset >= floor {
            return Some(batch);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_offset_index_round_trips_known_entries() {
        // Mirror OffsetIndex::append: 4B rel BE, 4B pos BE.
        let mut buf = Vec::new();
        for (rel, pos) in [(0_u32, 0_u32), (10, 256), (20, 512)] {
            buf.extend_from_slice(&rel.to_be_bytes());
            buf.extend_from_slice(&pos.to_be_bytes());
        }
        let entries = parse_offset_index(&buf);
        assert_eq!(entries, vec![(0, 0), (10, 256), (20, 512)]);
    }

    #[test]
    fn position_for_relative_offset_returns_floor() {
        let entries = vec![(0_u32, 0_u32), (10, 256), (20, 512), (30, 1024)];
        assert_eq!(position_for_relative_offset(&entries, 10), 256, "exact");
        assert_eq!(position_for_relative_offset(&entries, 15), 256, "between");
        assert_eq!(
            position_for_relative_offset(&entries, 0),
            0,
            "first entry exact"
        );
        assert_eq!(
            position_for_relative_offset(&entries, 100),
            1024,
            "after last"
        );
        assert_eq!(position_for_relative_offset(&[], 50), 0, "empty");
    }

    #[test]
    fn position_for_relative_offset_below_first() {
        // Synthetic: first entry isn't at rel=0. Floor below it returns 0.
        let entries = vec![(5_u32, 100_u32), (10, 200)];
        assert_eq!(position_for_relative_offset(&entries, 3), 0);
    }

    #[test]
    fn parse_time_index_round_trips_known_entries() {
        let mut buf = Vec::new();
        for (ts, rel) in [(1_000_i64, 0_u32), (2_000, 10), (3_000, 20)] {
            buf.extend_from_slice(&ts.to_be_bytes());
            buf.extend_from_slice(&rel.to_be_bytes());
        }
        let entries = parse_time_index(&buf);
        assert_eq!(entries, vec![(1_000, 0), (2_000, 10), (3_000, 20)]);
    }

    #[test]
    fn relative_offset_for_timestamp_returns_first_ge() {
        let entries = vec![(1_000_i64, 0_u32), (2_000, 10), (3_000, 20)];
        assert_eq!(
            relative_offset_for_timestamp(&entries, 1_000),
            Some(0),
            "exact match"
        );
        assert_eq!(
            relative_offset_for_timestamp(&entries, 1_500),
            Some(10),
            "between → next"
        );
        assert_eq!(
            relative_offset_for_timestamp(&entries, 4_000),
            None,
            "after last"
        );
        assert_eq!(relative_offset_for_timestamp(&[], 1_000), None, "empty");
    }

    #[test]
    fn end_position_for_caps_with_max_bytes() {
        // start=0, segment=1024, max_bytes=256 → exclusive_end=256 →
        // inclusive=255.
        assert_eq!(end_position_for(0, 1024, 256), Some(255));
        // max_bytes >= remaining → read to end.
        assert_eq!(end_position_for(512, 1024, 999_999), None);
        // max_bytes=0 → read to end (zero is a no-cap sentinel).
        assert_eq!(end_position_for(0, 1024, 0), None);
        // start past the segment-end cap still safe via saturating add.
        assert_eq!(end_position_for(u32::MAX, 1024, 100), None);
    }

    #[test]
    fn first_batch_at_or_after_decodes_and_skips() {
        use bytes::{Bytes, BytesMut};
        use crabka_protocol::records::Record;

        // Two adjacent batches; floor=10 should skip the first (last=9) and
        // return the second.
        let mut a = RecordBatch {
            base_offset: 0,
            last_offset_delta: 9,
            ..RecordBatch::default()
        };
        for i in 0..10 {
            a.records.push(Record {
                offset_delta: i,
                value: Some(Bytes::from(vec![b'a'; 4])),
                ..Default::default()
            });
        }
        let mut b = RecordBatch {
            base_offset: 10,
            last_offset_delta: 9,
            ..RecordBatch::default()
        };
        for i in 0..10 {
            b.records.push(Record {
                offset_delta: i,
                value: Some(Bytes::from(vec![b'b'; 4])),
                ..Default::default()
            });
        }
        let mut buf = BytesMut::new();
        a.encode(&mut buf).unwrap();
        b.encode(&mut buf).unwrap();
        let bytes = buf.freeze();

        let got = first_batch_at_or_after(&bytes, 10).expect("found batch");
        assert_eq!(got.base_offset, 10);

        // Floor below everything → first batch.
        let got = first_batch_at_or_after(&bytes, 0).expect("found batch");
        assert_eq!(got.base_offset, 0);

        // Floor above everything → None.
        assert!(first_batch_at_or_after(&bytes, 1_000).is_none());

        // Empty buffer → None.
        assert!(first_batch_at_or_after(&[], 0).is_none());
    }

    // ── Integration tests against `LocalTieredStorage` +
    // ── `InmemoryRemoteLogMetadataManager`. These exercise the full RSM/RLMM
    // ── plumbing through `RemoteReader` (the actual SPI calls, not just
    // ── helpers), using the copy path's `copy_eligible` to populate the
    // ── tier from a real `Log`.

    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::Record;
    use crabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
        RemoteStorageManager,
    };
    use std::collections::BTreeMap;
    use std::fmt::Write as _;
    use uuid::Uuid;

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn batch_of(n: i32, value_size: usize) -> crabka_protocol::records::RecordBatch {
        use bytes::Bytes;
        let mut b = crabka_protocol::records::RecordBatch {
            last_offset_delta: n - 1,
            ..crabka_protocol::records::RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(vec![b'x'; value_size])),
                ..Default::default()
            });
        }
        b
    }

    /// Build a log rolled into several sealed segments under `dir`, then copy
    /// every sealed segment into a fresh `LocalTieredStorage` +
    /// `InmemoryRemoteLogMetadataManager`. Returns the constructed reader and
    /// the log (kept alive so the on-disk files outlive the call).
    fn populated_reader(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_bytes: 256,
                ..LogConfig::default()
            },
        )
        .unwrap();
        for _ in 0..12 {
            let mut b = batch_of(2, 64);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert!(exports.len() >= 2, "test needs multiple sealed segments");

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        // Manually copy each segment as `CopySegmentStarted` →
        // `CopySegmentFinished` (mirrors the copy path's copy_eligible
        // without the broker-side dependencies).
        for ex in &exports {
            let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
            let epochs: BTreeMap<i32, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(0, ex.base_offset)])
            } else {
                ex.leader_epochs.iter().copied().collect()
            };
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
                ex.base_offset,
                ex.last_offset,
                ex.max_timestamp,
                1,
                ex.max_timestamp,
                i32::try_from(ex.size_bytes).unwrap_or(i32::MAX),
                RemoteLogSegmentState::CopySegmentStarted,
                epochs.clone(),
            )
            .unwrap();
            rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
            // Render the leader-epoch checkpoint the same way the copy path
            // does so `fetch_index(LeaderEpoch)` returns real bytes.
            let mut s = String::from("0\n");
            let _ = writeln!(s, "{}", epochs.len());
            for (e, st) in &epochs {
                let _ = writeln!(s, "{e} {st}");
            }
            let data = crabka_remote_storage::LogSegmentData {
                log_segment: ex.log_path.clone(),
                offset_index: ex.offset_index_path.clone(),
                time_index: ex.time_index_path.clone(),
                transaction_index: ex.transaction_index_path.clone(),
                producer_snapshot_index: None,
                leader_epoch_index: bytes::Bytes::from(s.into_bytes()),
            };
            rsm.copy_log_segment_data(&md, &data).unwrap();
            rlmm.update_remote_log_segment_metadata(
                crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                    remote_log_segment_id: id,
                    event_timestamp_ms: ex.max_timestamp,
                    custom_metadata: None,
                    state: RemoteLogSegmentState::CopySegmentFinished,
                    broker_id: 1,
                },
            )
            .unwrap();
        }

        (RemoteReader::new(rsm, rlmm), log)
    }

    #[tokio::test]
    async fn fetch_batch_finds_segment_and_returns_first_batch() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());

        // Pick an offset inside the second sealed segment. Each batch covers
        // two records, so base_offset=2 lives in segment[1] (base=2).
        let exports = log.tierable_segments();
        let target_offset = exports[1].base_offset;

        let got = reader
            .fetch_batch(&tp(), 0, target_offset, 4096)
            .await
            .expect("ok")
            .expect("found a batch");
        // The batch returned should start at or before target_offset and end
        // at or after it.
        let last = got.base_offset + i64::from(got.last_offset_delta);
        assert!(
            got.base_offset <= target_offset && last >= target_offset,
            "batch [{},{}] doesn't cover target {target_offset}",
            got.base_offset,
            last
        );
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_when_segment_not_in_rlmm() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        // RLMM is empty → no segment for `tp` at epoch 0.
        let got = reader.fetch_batch(&tp(), 0, 0, 4096).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn fetch_batch_returns_none_for_in_progress_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id,
            0,
            99,
            100,
            1,
            100,
            1024,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0_i32, 0_i64)]),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        let reader = RemoteReader::new(rsm, rlmm);
        let got = reader.fetch_batch(&tp(), 0, 50, 4096).await.unwrap();
        assert!(
            got.is_none(),
            "started (not finished) segment must be invisible"
        );
    }

    #[tokio::test]
    async fn earliest_offset_returns_lowest_finished_start() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        let expected = exports.iter().map(|e| e.base_offset).min().unwrap();
        let got = reader.earliest_offset(&tp()).unwrap();
        assert_eq!(got, Some(expected));
    }

    #[tokio::test]
    async fn earliest_offset_returns_none_when_no_finished_segments() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        assert_eq!(reader.earliest_offset(&tp()).unwrap(), None);
    }

    #[tokio::test]
    async fn offset_for_timestamp_locates_remote_segment() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        // The segment metadata copies `max_timestamp` from the export; the
        // log's batch builder leaves base_timestamp at 0 by default, so
        // every batch's max_timestamp is 0 — so segments' max_timestamps are
        // all 0. Target a timestamp <= 0 to match the first segment.
        let target_ts = 0_i64;
        let got = reader
            .offset_for_timestamp(&tp(), target_ts)
            .await
            .unwrap()
            .expect("first segment matches ts=0");
        // The first finished segment is the lowest-base one.
        let expected = exports.iter().map(|e| e.base_offset).min().unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_none_when_past_last() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log) = populated_reader(log_dir.path(), remote_dir.path());
        // All segments have max_ts=0 by construction (see test above); any
        // strictly-positive target is past every remote segment.
        let got = reader.offset_for_timestamp(&tp(), 1).await.unwrap();
        assert_eq!(got, None);
    }
}
