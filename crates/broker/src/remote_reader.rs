//! KIP-405 remote read path.
//!
//! This module wraps the broker's shared [`RemoteStorageManager`] and
//! [`RemoteLogMetadataManager`] pair. It serves `Fetch` and `ListOffsets`
//! requests for offsets that have no local copy any more.
//!
//! The RSM and RLMM SPIs are synchronous and blocking. This module therefore
//! wraps byte-range reads, index reads, and `ListOffsets` metadata scans in
//! `tokio::task::spawn_blocking`, so those remote-tier operations do not stall
//! the broker's reactor. The pure index-decode helpers mirror
//! `crabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the Kafka-format
//! index bytes that the copy path wrote verbatim.

use std::sync::Arc;

use crabka_ids::LeaderEpoch;
use crabka_protocol::records::RecordBatch;
use crabka_remote_storage::{
    IndexType, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentState,
    RemoteStorageError, RemoteStorageManager, TopicIdPartition,
};
use tracing::warn;
use zerocopy::{
    BigEndian, FromBytes, Immutable, KnownLayout, Unaligned,
    byteorder::{I64, U32},
};

/// Absolute (partition-level) log offset.
pub(crate) type LogOffset = i64;
/// Record timestamp in milliseconds since the Unix epoch.
pub(crate) type TimestampMs = i64;
/// Offset relative to a segment's base offset. This is the offset-index key.
pub(crate) type RelativeOffset = u32;
/// Byte position within a segment's `.log` file. This is the offset-index
/// value.
pub(crate) type BytePosition = u32;

/// 8 bytes per entry: rel u32 BE, then pos u32 BE. It mirrors
/// `crabka_log::index::OffsetEntryRaw`, so the remote-tier copy of an
/// `OffsetIndex` file decodes through the same byte layout that wrote the
/// local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct OffsetIndexEntry {
    relative_offset: U32<BigEndian>,
    position: U32<BigEndian>,
}

/// Byte length of one serialized offset-index entry.
const OFFSET_INDEX_ENTRY_LEN: usize = std::mem::size_of::<OffsetIndexEntry>();

const _: () = assert!(OFFSET_INDEX_ENTRY_LEN == 8);

/// 12 bytes per entry: ts i64 BE, then rel u32 BE. It mirrors
/// `crabka_log::index::TimeEntryRaw`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct TimeIndexEntry {
    timestamp: I64<BigEndian>,
    relative_offset: U32<BigEndian>,
}

/// Byte length of one serialized time-index entry.
const TIME_INDEX_ENTRY_LEN: usize = std::mem::size_of::<TimeIndexEntry>();

const _: () = assert!(TIME_INDEX_ENTRY_LEN == 12);

/// 24 bytes per entry: `start_offset` i64 BE, `last_offset` i64 BE, then
/// `producer_id` i64 BE. It mirrors `crabka_log::txn_index::AbortedTxnRaw`, so
/// the remote-tier copy of a `.txnindex` file decodes through the same byte
/// layout that wrote the local index.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct AbortedTxnIndexEntry {
    start_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    producer_id: I64<BigEndian>,
}

/// Byte length of one serialized aborted-transaction index entry.
const TXN_INDEX_ENTRY_LEN: usize = std::mem::size_of::<AbortedTxnIndexEntry>();

const _: () = assert!(TXN_INDEX_ENTRY_LEN == 24);

/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: LogOffset,
    pub(crate) last_offset: LogOffset,
    pub(crate) producer_id: i64,
}

/// Holds the broker's shared `RSM` and `RLMM`, and serves remote reads.
pub(crate) struct RemoteReader {
    pub(crate) rsm: Arc<dyn RemoteStorageManager>,
    pub(crate) rlmm: Arc<dyn RemoteLogMetadataManager>,
}

/// The last offset durably copied to the remote tier and the leader epoch
/// that owns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TieredOffset {
    pub(crate) offset: LogOffset,
    pub(crate) leader_epoch: LeaderEpoch,
}

impl RemoteReader {
    pub(crate) fn new(
        rsm: Arc<dyn RemoteStorageManager>,
        rlmm: Arc<dyn RemoteLogMetadataManager>,
    ) -> Self {
        Self { rsm, rlmm }
    }

    /// Finds the finished segment in the RLMM that covers
    /// `(leader_epoch, offset)`, fetches its offset index, positions into the
    /// `.log` data, and returns the first batch whose last offset is
    /// `>= offset`. It returns `None` when no finished segment covers the
    /// requested offset.
    ///
    /// `max_bytes` caps the byte range that this method fetches from the
    /// remote tier. The caller's `partition_max_bytes` from the Fetch request
    /// arrives here.
    pub(crate) async fn fetch_batch(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        offset: LogOffset,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>, RemoteStorageError> {
        // Primary lookup: epoch-indexed fast path.  The caller resolves
        // `leader_epoch` from the local leader-epoch checkpoint via
        // `epoch_for_offset`, so this is the epoch that *owned* the requested
        // offset at copy time.  The RLMM indexes a segment under every epoch
        // in its `segment_leader_epochs` map, so this reliably hits after a
        // clean failover.
        let primary = self
            .rlmm
            .remote_log_segment_metadata(tp, leader_epoch, offset)?;

        // Defensive fallback: the epoch-indexed primary lookup can still miss
        // in rare edge cases (e.g. the local leader-epoch checkpoint is empty
        // on a fresh replica, or an unclean election produced a gap in the
        // checkpoint that `epoch_for_offset` cannot bridge).  When the primary
        // misses, scan `list_remote_log_segments` for finished segments that
        // cover `offset` and prefer the one whose `segment_leader_epochs` map
        // contains the passed epoch (same lineage) — this closes the
        // wrong-segment-under-log-divergence hazard.  Only if no lineage-
        // matching candidate exists does the fallback revert to
        // `max_by_key(start_offset)` as a last resort; in a clean log without
        // epoch-range overlap that tie-break is always deterministic.
        let metadata = if let Some(m) = primary {
            m
        } else {
            let candidates = self.rlmm.list_remote_log_segments(tp)?;
            let covering: Vec<_> = candidates
                .into_iter()
                .filter(|m| {
                    m.state() == RemoteLogSegmentState::CopySegmentFinished
                        && m.start_offset() <= offset
                        && offset <= m.end_offset()
                })
                .collect();
            // Prefer a segment whose epoch map contains the owning epoch
            // (same lineage as the checkpoint resolution).
            let Some(m) = covering
                .iter()
                .filter(|m| m.segment_leader_epochs().contains_key(&leader_epoch))
                .max_by_key(|m| m.start_offset())
                .or_else(|| {
                    // No lineage-matching candidate — last resort: highest
                    // start_offset among all covering finished segments.
                    covering.iter().max_by_key(|m| m.start_offset())
                })
                .cloned()
            else {
                return Ok(None);
            };
            m
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(None);
        }

        let index_bytes = self
            .fetch_index_blocking(metadata.clone(), IndexType::Offset)
            .await?;
        let entries = parse_offset_index(&index_bytes)?;
        let target_rel = u32::try_from((offset - metadata.start_offset()).max(0)).unwrap_or(0);
        let start_position = position_for_relative_offset(entries, target_rel);

        // Cap the read so the broker doesn't pull an entire segment when the
        // Fetch asked for one batch. Always pull at least one full batch worth
        // of bytes — the segment's `size` is the safe ceiling.
        let segment_size =
            u32::try_from(metadata.segment_size_in_bytes().max(0)).unwrap_or(u32::MAX);
        let end_position = end_position_for(start_position, segment_size, max_bytes);

        let data = self
            .fetch_log_blocking(metadata.clone(), start_position, end_position)
            .await?;

        let batch = first_batch_at_or_after(&data, offset);
        Ok(batch)
    }

    /// Returns the aborted transactions that overlap the inclusive offset
    /// range `[from_offset, to_offset]`, in the finished remote segment that
    /// covers `from_offset`. It returns an empty `Vec` in three cases: no
    /// finished segment covers the offset, the segment carries no transaction
    /// index (`SegmentNotFound` from `fetch_index`), or nothing overlaps.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: LeaderEpoch,
        from_offset: LogOffset,
        to_offset: LogOffset,
    ) -> Result<Vec<AbortedTxnEntry>, RemoteStorageError> {
        let Some(metadata) =
            self.rlmm
                .remote_log_segment_metadata(tp, leader_epoch, from_offset)?
        else {
            return Ok(Vec::new());
        };
        if metadata.state() != RemoteLogSegmentState::CopySegmentFinished {
            return Ok(Vec::new());
        }

        let index_bytes = match self
            .fetch_index_blocking(metadata, IndexType::Transaction)
            .await
        {
            Ok(bytes) => bytes,
            // The transaction index is optional: a segment with no aborted
            // transactions has no `.txnindex`, surfaced as SegmentNotFound.
            Err(RemoteStorageError::SegmentNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let entries = parse_txn_index(&index_bytes)?;
        Ok(entries
            .iter()
            .filter(|e| txn_overlaps(e, from_offset, to_offset))
            .map(|e| AbortedTxnEntry {
                start_offset: e.start_offset.get(),
                last_offset: e.last_offset.get(),
                producer_id: e.producer_id.get(),
            })
            .collect())
    }

    /// Returns the lowest `start_offset` across the finished segments for
    /// `tp`, or `None` when no finished segment exists. It drives
    /// `ListOffsets` EARLIEST below `local_log_start_offset()`.
    pub(crate) async fn earliest_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<LogOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        Ok(listed
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .map(|md| md.start_offset())
            .min())
    }

    /// Returns the highest offset held by a finished remote segment and the
    /// leader epoch that owns that offset. In-progress copies are invisible.
    pub(crate) async fn latest_tiered_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<TieredOffset>, RemoteStorageError> {
        let listed = self.list_remote_log_segments_blocking(tp).await?;
        let Some(metadata) = listed
            .into_iter()
            .filter(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished)
            .max_by_key(RemoteLogSegmentMetadata::end_offset)
        else {
            return Ok(None);
        };
        let offset = metadata.end_offset();
        let Some(leader_epoch) = metadata
            .segment_leader_epochs()
            .iter()
            .filter(|(_, start)| **start <= offset)
            .max_by_key(|(_, start)| **start)
            .map(|(epoch, _)| *epoch)
        else {
            return Ok(None);
        };
        Ok(Some(TieredOffset {
            offset,
            leader_epoch,
        }))
    }

    /// Returns the smallest absolute offset and its record timestamp where the
    /// timestamp is `>= target_timestamp`, across the finished remote segments.
    /// The sparse time index supplies a scan floor; the exact answer comes from
    /// decoding records from the corresponding offset-index position.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: TimestampMs,
    ) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
        let mut listed = self.list_remote_log_segments_blocking(tp).await?;
        listed.retain(|md| md.state() == RemoteLogSegmentState::CopySegmentFinished);
        listed.sort_by_key(RemoteLogSegmentMetadata::start_offset);

        for metadata in listed
            .into_iter()
            // `-1` is the persisted unknown-max sentinel for a sealed segment
            // opened without a tail scan. It must remain scan-eligible for a
            // positive timestamp lookup after broker restart.
            .filter(|md| md.max_timestamp_ms() == -1 || md.max_timestamp_ms() >= target_timestamp)
        {
            let (time_index_bytes, offset_index_bytes) = tokio::try_join!(
                self.fetch_index_blocking(metadata.clone(), IndexType::Timestamp),
                self.fetch_index_blocking(metadata.clone(), IndexType::Offset),
            )?;
            let scan_rel = relative_offset_floor_for_timestamp(
                parse_time_index(&time_index_bytes)?,
                target_timestamp,
            );
            let start_position =
                position_for_relative_offset(parse_offset_index(&offset_index_bytes)?, scan_rel);
            // ponytail: one tail read keeps the scan exact; switch to bounded,
            // batch-aligned windows only if remote segment profiling requires it.
            let data = self
                .fetch_log_blocking(metadata.clone(), start_position, None)
                .await?;
            let scan_offset = metadata
                .start_offset()
                .checked_add(i64::from(scan_rel))
                .ok_or_else(|| corrupt_log("timestamp-index offset overflow"))?;
            if let Some(found) =
                first_record_at_or_after_timestamp(&data, scan_offset, target_timestamp)?
            {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    async fn list_remote_log_segments_blocking(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
        let rlmm = self.rlmm.clone();
        let tp = tp.clone();
        match tokio::task::spawn_blocking(move || rlmm.list_remote_log_segments(&tp)).await {
            Ok(result) => result,
            Err(error) => {
                warn!(error = %error, "remote-reader: list_remote_log_segments task panicked");
                Err(RemoteStorageError::Io(std::io::Error::other(
                    "list_remote_log_segments task panicked",
                )))
            }
        }
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
        start_position: BytePosition,
        end_position: Option<BytePosition>,
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

/// Computes the inclusive `end_position` for a remote byte-range fetch.
///
/// It returns `None`, which means read to the end of the segment, when
/// `start_position` plus `max_bytes` would reach or pass `segment_size`. In
/// every other case it returns the inclusive last byte to read.
pub(crate) fn end_position_for(
    start_position: BytePosition,
    segment_size: u32,
    max_bytes: usize,
) -> Option<BytePosition> {
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

/// Helper for the `ref_from_bytes` parse error on the remote-read path.
///
/// The `zerocopy` cast can fail only on a length mismatch. The bytes come from
/// the object store, such as S3, which can return corrupt or truncated data.
/// This helper therefore returns a `RemoteStorageError` instead of a panic,
/// because a panic would be a `DoS` surface.
fn corrupt_index(kind: &str) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt remote {kind} index bytes"),
    ))
}

fn corrupt_log(detail: impl std::fmt::Display) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt remote log bytes: {detail}"),
    ))
}

/// Borrows Kafka's `OffsetIndex` on-disk format as a zero-copy
/// `&[OffsetIndexEntry]`, at 8 bytes per entry: rel u32 BE, then pos u32 BE.
/// It ignores trailing bytes that do not complete an 8-byte entry. The result
/// borrows from `bytes`.
pub(crate) fn parse_offset_index(bytes: &[u8]) -> Result<&[OffsetIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / OFFSET_INDEX_ENTRY_LEN) * OFFSET_INDEX_ENTRY_LEN;
    <[OffsetIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("offset"))
}

/// Floor lookup: the byte position of the largest entry with
/// `rel <= target_rel`. It returns 0 when the index is empty, and when the
/// target is before the first entry. It runs directly against the borrowed
/// zero-copy slice and builds no owned `Vec`.
#[must_use]
pub(crate) fn position_for_relative_offset(
    entries: &[OffsetIndexEntry],
    target_rel: RelativeOffset,
) -> BytePosition {
    match entries.binary_search_by_key(&target_rel, |e| e.relative_offset.get()) {
        Ok(i) => entries[i].position.get(),
        Err(0) => 0,
        Err(i) => entries[i - 1].position.get(),
    }
}

/// Borrows Kafka's `TimeIndex` on-disk format as a zero-copy
/// `&[TimeIndexEntry]`, at 12 bytes per entry: ts i64 BE, then rel u32 BE. It
/// ignores trailing bytes that do not complete a 12-byte entry. The result
/// borrows from `bytes`.
pub(crate) fn parse_time_index(bytes: &[u8]) -> Result<&[TimeIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TIME_INDEX_ENTRY_LEN) * TIME_INDEX_ENTRY_LEN;
    <[TimeIndexEntry]>::ref_from_bytes(&bytes[..truncated_len]).map_err(|_| corrupt_index("time"))
}

/// Borrows Kafka's transaction-index format as a zero-copy
/// `&[AbortedTxnIndexEntry]`, at 24 bytes per entry: `start_offset` i64 BE,
/// `last_offset` i64 BE, then `producer_id` i64 BE. It ignores trailing bytes
/// that do not complete a 24-byte entry. The result borrows from `bytes`.
pub(crate) fn parse_txn_index(bytes: &[u8]) -> Result<&[AbortedTxnIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / TXN_INDEX_ENTRY_LEN) * TXN_INDEX_ENTRY_LEN;
    <[AbortedTxnIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("transaction"))
}

/// Reports whether an aborted-transaction entry overlaps the inclusive offset
/// range `[from_offset, to_offset]`. It mirrors the overlap test in
/// `TxnIndex::aborted_in_range` against an inclusive range: the entry's
/// `[start, last]` intersects `[from, to]` if and only if
/// `start <= to && last >= from`.
#[must_use]
pub(crate) fn txn_overlaps(
    entry: &AbortedTxnIndexEntry,
    from_offset: LogOffset,
    to_offset: LogOffset,
) -> bool {
    entry.start_offset.get() <= to_offset && entry.last_offset.get() >= from_offset
}

/// Returns a safe relative-offset floor for an exact timestamp scan.
///
/// The last entry strictly below `target_ts` is used rather than an entry at
/// or above it: a sparse index entry is only a seek hint and may follow the
/// earliest qualifying record. Non-increasing relative offsets mark trailing
/// preallocation padding and end the usable index.
#[must_use]
pub(crate) fn relative_offset_floor_for_timestamp(
    entries: &[TimeIndexEntry],
    target_ts: TimestampMs,
) -> RelativeOffset {
    let mut floor = 0;
    let mut previous_relative_offset = None;
    for entry in entries {
        let relative_offset = entry.relative_offset.get();
        if previous_relative_offset.is_some_and(|previous| relative_offset <= previous)
            || entry.timestamp.get() >= target_ts
        {
            break;
        }
        floor = relative_offset;
        previous_relative_offset = Some(relative_offset);
    }
    floor
}

/// Decodes remote log batches and returns the earliest record at or after both
/// `floor_offset` and `target_timestamp`.
pub(crate) fn first_record_at_or_after_timestamp(
    data: &[u8],
    floor_offset: LogOffset,
    target_timestamp: TimestampMs,
) -> Result<Option<(LogOffset, TimestampMs)>, RemoteStorageError> {
    let mut cur = data;
    while !cur.is_empty() {
        let batch = RecordBatch::decode(&mut cur).map_err(corrupt_log)?;
        for record in &batch.records {
            let offset = batch
                .base_offset
                .checked_add(i64::from(record.offset_delta))
                .ok_or_else(|| corrupt_log("record offset overflow"))?;
            if offset < floor_offset {
                continue;
            }
            let timestamp = batch
                .base_timestamp
                .checked_add(record.timestamp_delta)
                .ok_or_else(|| corrupt_log("record timestamp overflow"))?;
            if timestamp >= target_timestamp {
                return Ok(Some((offset, timestamp)));
            }
        }
    }
    Ok(None)
}

/// Decodes batches from `data` and returns the first one whose last offset is
/// `>= floor`. It skips the batches at the start of the returned byte range
/// that the offset index pointed at but that do not cover the requested
/// offset. Kafka offset indexes are sparse, so such batches occur.
pub(crate) fn first_batch_at_or_after(data: &[u8], floor: LogOffset) -> Option<RecordBatch> {
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
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_offset_index_round_trips_known_entries() {
        // Mirror OffsetIndex::append: 4B rel BE, 4B pos BE.
        let mut buf = Vec::new();
        for (rel, pos) in [(0_u32, 0_u32), (10, 256), (20, 512)] {
            buf.extend_from_slice(&rel.to_be_bytes());
            buf.extend_from_slice(&pos.to_be_bytes());
        }
        let entries = parse_offset_index(&buf).expect("valid offset index");
        let decoded: Vec<(u32, u32)> = entries
            .iter()
            .map(|e| (e.relative_offset.get(), e.position.get()))
            .collect();
        assert!(decoded == vec![(0, 0), (10, 256), (20, 512)]);
    }

    fn offset_entries(pairs: &[(u32, u32)]) -> Vec<OffsetIndexEntry> {
        pairs
            .iter()
            .map(|&(rel, pos)| OffsetIndexEntry {
                relative_offset: U32::new(rel),
                position: U32::new(pos),
            })
            .collect()
    }

    fn time_entries(pairs: &[(i64, u32)]) -> Vec<TimeIndexEntry> {
        pairs
            .iter()
            .map(|&(ts, rel)| TimeIndexEntry {
                timestamp: I64::new(ts),
                relative_offset: U32::new(rel),
            })
            .collect()
    }

    #[test]
    fn position_for_relative_offset_returns_floor() {
        let entries = offset_entries(&[(0, 0), (10, 256), (20, 512), (30, 1024)]);
        let cases: [(&[OffsetIndexEntry], u32, u32); 5] = [
            (&entries, 10, 256),   // exact
            (&entries, 15, 256),   // between
            (&entries, 0, 0),      // first entry exact
            (&entries, 100, 1024), // after last
            (&[], 50, 0),          // empty
        ];
        for (entries, rel, want) in cases {
            assert!(
                position_for_relative_offset(entries, rel) == want,
                "rel {rel}"
            );
        }
    }

    #[test]
    fn position_for_relative_offset_below_first() {
        // Synthetic: first entry isn't at rel=0. Floor below it returns 0.
        let entries = offset_entries(&[(5, 100), (10, 200)]);
        assert!(position_for_relative_offset(&entries, 3) == 0);
    }

    #[test]
    fn parse_time_index_round_trips_known_entries() {
        let mut buf = Vec::new();
        for (ts, rel) in [(1_000_i64, 0_u32), (2_000, 10), (3_000, 20)] {
            buf.extend_from_slice(&ts.to_be_bytes());
            buf.extend_from_slice(&rel.to_be_bytes());
        }
        let entries = parse_time_index(&buf).expect("valid time index");
        let decoded: Vec<(i64, u32)> = entries
            .iter()
            .map(|e| (e.timestamp.get(), e.relative_offset.get()))
            .collect();
        assert!(decoded == vec![(1_000, 0), (2_000, 10), (3_000, 20)]);
    }

    #[test]
    fn timestamp_floor_stays_before_sparse_match() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (3_000, 20)]);
        let cases: [(&[TimeIndexEntry], i64, u32); 4] = [
            (&entries, 1_000, 0), // exact match scans from the segment start
            (&entries, 1_500, 0), // between entries scans from the lower hint
            (&entries, 4_000, 20),
            (&[], 1_000, 0),
        ];
        for (entries, ts, want) in cases {
            assert!(
                relative_offset_floor_for_timestamp(entries, ts) == want,
                "ts {ts} entries_len {}",
                entries.len()
            );
        }
    }

    #[test]
    fn timestamp_floor_ignores_trailing_index_padding() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (0, 0), (0, 0)]);
        assert!(relative_offset_floor_for_timestamp(&entries, 3_000) == 10);
    }

    #[test]
    fn end_position_for_caps_with_max_bytes() {
        let cases = [
            // start=0, segment=1024, max_bytes=256 → exclusive_end=256 →
            // inclusive=255.
            (0, 1024, 256, Some(255)),
            // max_bytes >= remaining → read to end.
            (512, 1024, 999_999, None),
            // max_bytes=0 → read to end (zero is a no-cap sentinel).
            (0, 1024, 0, None),
            // start past the segment-end cap still safe via saturating add.
            (u32::MAX, 1024, 100, None),
        ];
        for (start, segment, max_bytes, want) in cases {
            assert!(
                end_position_for(start, segment, max_bytes) == want,
                "start {start} segment {segment} max_bytes {max_bytes}"
            );
        }
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

        let cases = [
            // Floor=10 skips the first batch (last=9), returns the second.
            (10, Some(10)),
            // Floor below everything → first batch.
            (0, Some(0)),
            // Floor above everything → None.
            (1_000, None),
        ];
        for (floor, want_base) in cases {
            assert!(
                first_batch_at_or_after(&bytes, floor).map(|b| b.base_offset) == want_base,
                "floor {floor}"
            );
        }

        // Empty buffer → None.
        assert!(first_batch_at_or_after(&[], 0).is_none());
    }

    #[test]
    fn first_batch_at_or_after_rejects_floor_past_base_plus_delta() {
        let batch = test_batch_at(3, 4, b'z');
        let mut buf = bytes::BytesMut::new();
        batch.encode(&mut buf).unwrap();
        let bytes = buf.freeze();

        assert!(
            first_batch_at_or_after(&bytes, 7).is_none(),
            "batch 3..6 must not cover floor 7"
        );
    }

    #[test]
    fn parse_txn_index_round_trips_known_entries() {
        // Mirror TxnIndex::append: 8B start_offset BE, 8B last_offset BE,
        // 8B producer_id BE.
        let mut buf = Vec::new();
        for (start, last, pid) in [(0_i64, 4_i64, 1000_i64), (10, 14, 2000)] {
            buf.extend_from_slice(&start.to_be_bytes());
            buf.extend_from_slice(&last.to_be_bytes());
            buf.extend_from_slice(&pid.to_be_bytes());
        }
        let entries = parse_txn_index(&buf).expect("valid txn index");
        let decoded: Vec<(i64, i64, i64)> = entries
            .iter()
            .map(|e| {
                (
                    e.start_offset.get(),
                    e.last_offset.get(),
                    e.producer_id.get(),
                )
            })
            .collect();
        assert!(decoded == vec![(0, 4, 1000), (10, 14, 2000)]);
    }

    #[test]
    fn parse_txn_index_truncates_trailing_partial_bytes() {
        let mut buf = Vec::new();
        for v in [0_i64, 4, 1000] {
            buf.extend_from_slice(&v.to_be_bytes());
        }
        // 5 trailing bytes that don't complete a 24-byte entry.
        buf.extend_from_slice(&[0xAA; 5]);
        let entries = parse_txn_index(&buf).expect("valid txn index");
        assert!(entries.len() == 1, "partial trailing entry ignored");
        assert!(entries[0].producer_id.get() == 1000);
    }

    #[test]
    fn parse_txn_index_empty_is_empty() {
        assert!(parse_txn_index(&[]).expect("empty is valid").is_empty());
    }

    #[test]
    fn txn_overlaps_boundaries() {
        let e = AbortedTxnIndexEntry {
            start_offset: I64::new(10),
            last_offset: I64::new(14),
            producer_id: I64::new(1),
        };
        let cases = [
            // Range fully before the entry → excluded.
            (0, 9, false),
            // Range touching the entry's first offset → included.
            (0, 10, true),
            // Range fully inside the entry → included.
            (11, 13, true),
            // Range touching the entry's last offset → included.
            (14, 100, true),
            // Range fully after the entry → excluded.
            (15, 100, false),
            // Range fully covering the entry → included.
            (0, 100, true),
        ];
        for (start, end, want) in cases {
            assert!(
                txn_overlaps(&e, start, end) == want,
                "range [{start},{end}]"
            );
        }
    }

    // ── Integration tests against `LocalTieredStorage` +
    // ── `InmemoryRemoteLogMetadataManager`. These exercise the full RSM/RLMM
    // ── plumbing through `RemoteReader` (the actual SPI calls, not just
    // ── helpers), using the copy path's `copy_eligible` to populate the
    // ── tier from a real `Log`.

    use std::{collections::BTreeMap, fmt::Write as _};

    use crabka_log::{Log, LogConfig};
    use crabka_protocol::records::Record;
    use crabka_remote_storage::{
        InmemoryRemoteLogMetadataManager, LocalTieredStorage, RemoteLogMetadataManager,
        RemoteStorageManager,
    };
    use crabka_units::convert::ByteSizeExt as _;
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

    fn test_batch_at(
        base_offset: i64,
        record_count: i32,
        value_byte: u8,
    ) -> crabka_protocol::records::RecordBatch {
        use bytes::Bytes;
        let mut batch = crabka_protocol::records::RecordBatch {
            base_offset,
            last_offset_delta: record_count - 1,
            ..crabka_protocol::records::RecordBatch::default()
        };
        for offset_delta in 0..record_count {
            batch.records.push(Record {
                offset_delta,
                value: Some(Bytes::from(vec![value_byte; 4])),
                ..Default::default()
            });
        }
        batch
    }

    fn timestamped_batch_at(
        base_offset: i64,
        timestamps: &[i64],
        value_byte: u8,
    ) -> crabka_protocol::records::RecordBatch {
        use bytes::Bytes;

        let base_timestamp = timestamps.first().copied().unwrap_or_default();
        crabka_protocol::records::RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(timestamps.len().saturating_sub(1)).unwrap(),
            base_timestamp,
            max_timestamp: timestamps.iter().copied().max().unwrap_or_default(),
            records: timestamps
                .iter()
                .enumerate()
                .map(|(offset_delta, timestamp)| Record {
                    timestamp_delta: timestamp - base_timestamp,
                    offset_delta: i32::try_from(offset_delta).unwrap(),
                    value: Some(Bytes::from(vec![value_byte; 4])),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn offset_index_bytes(entries: &[(u32, u32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (relative_offset, position) in entries {
            buf.extend_from_slice(&relative_offset.to_be_bytes());
            buf.extend_from_slice(&position.to_be_bytes());
        }
        buf
    }

    fn time_index_bytes(entries: &[(i64, u32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (timestamp, relative_offset) in entries {
            buf.extend_from_slice(&timestamp.to_be_bytes());
            buf.extend_from_slice(&relative_offset.to_be_bytes());
        }
        buf
    }

    fn write_test_file(dir: &std::path::Path, name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn sparse_remote_segment_reader_with_max_timestamp(
        max_timestamp_ms: i64,
    ) -> (RemoteReader, tempfile::TempDir) {
        let source_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();

        let first = timestamped_batch_at(10, &[1_000, 1_100, 1_600, 1_700], b'a');
        let second = timestamped_batch_at(14, &[2_000, 2_200, 2_400], b'b');
        let mut log_bytes = bytes::BytesMut::new();
        first.encode(&mut log_bytes).unwrap();
        let second_position = u32::try_from(log_bytes.len()).unwrap();
        second.encode(&mut log_bytes).unwrap();
        let log_bytes = log_bytes.freeze();

        let log_path = write_test_file(source_dir.path(), "00000000000000000010.log", &log_bytes);
        let offset_index_path = write_test_file(
            source_dir.path(),
            "00000000000000000010.index",
            &offset_index_bytes(&[(0, 0), (4, second_position)]),
        );
        let time_index_path = write_test_file(
            source_dir.path(),
            "00000000000000000010.timeindex",
            &time_index_bytes(&[(1_700, 0), (2_400, 4)]),
        );

        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        let md = RemoteLogSegmentMetadata::new(
            id.clone(),
            10,
            16,
            max_timestamp_ms,
            1,
            2_400,
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                i32::try_from(log_bytes.len()).unwrap_or(i32::MAX),
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0_i32), 10_i64)]),
            ),
        )
        .unwrap();

        rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
        let data = crabka_remote_storage::LogSegmentData {
            log_segment: log_path,
            offset_index: offset_index_path,
            time_index: time_index_path,
            transaction_index: None,
            producer_snapshot_index: None,
            leader_epoch_index: bytes::Bytes::from_static(b"0\n1\n0 10\n"),
        };
        rsm.copy_log_segment_data(&md, &data).unwrap();
        rlmm.update_remote_log_segment_metadata(
            crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                remote_log_segment_id: id,
                event_timestamp_ms: 2_400,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            },
        )
        .unwrap();

        (RemoteReader::new(rsm, rlmm), remote_dir)
    }

    fn sparse_remote_segment_reader() -> (RemoteReader, tempfile::TempDir) {
        sparse_remote_segment_reader_with_max_timestamp(2_400)
    }

    /// Builds a log rolled into several sealed segments under `dir`, then
    /// copies every sealed segment into a fresh `LocalTieredStorage` and
    /// `InmemoryRemoteLogMetadataManager`. It returns the constructed reader
    /// and the log. The caller keeps the log alive so that the on-disk files
    /// outlive the call.
    fn populated_reader(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_size: crabka_units::bytes(256),
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
            // Unwrap the log-layer `Offset`s into the remote-storage metadata's
            // `i64` world at the seam.
            let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(LeaderEpoch(0), ex.base_offset.0)])
            } else {
                ex.leader_epochs
                    .iter()
                    .map(|&(epoch, off)| (epoch, off.0))
                    .collect()
            };
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
                ex.base_offset.0,
                ex.last_offset.0,
                ex.max_timestamp,
                1,
                ex.max_timestamp,
                crabka_remote_storage::RemoteLogSegmentDetails::new(
                    ex.size.bytes_i32(),
                    RemoteLogSegmentState::CopySegmentStarted,
                    epochs.clone(),
                ),
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

    /// Works like `populated_reader`, but before the copy it writes one
    /// aborted-txn entry into the first sealed segment's `.txnindex`. The
    /// entry is 24 BE bytes: `start_offset`, `last_offset`, and
    /// `producer_id`. The copy path then carries it to the remote tier. It
    /// returns the reader, the log, and the written
    /// `(start_offset, last_offset, producer_id)`.
    fn populated_reader_with_abort(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log, (i64, i64, i64)) {
        let mut log = Log::open(
            log_dir,
            LogConfig {
                segment_size: crabka_units::bytes(256),
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

        // Write a `.txnindex` next to the first sealed segment's `.log` so the
        // export below picks it up. The abort covers the whole first segment.
        let first = &exports[0];
        // Unwrap the log-layer `Offset`s into this helper's `i64` tuple at the seam.
        let abort = (first.base_offset.0, first.last_offset.0, 7777_i64);
        let mut txn_bytes = Vec::new();
        txn_bytes.extend_from_slice(&abort.0.to_be_bytes());
        txn_bytes.extend_from_slice(&abort.1.to_be_bytes());
        txn_bytes.extend_from_slice(&abort.2.to_be_bytes());
        let txn_path = first.log_path.with_extension("txnindex");
        std::fs::write(&txn_path, &txn_bytes).unwrap();

        // Re-derive exports so the first one now carries the txnindex path.
        let exports = log.tierable_segments();
        assert!(
            exports[0].transaction_index_path.is_some(),
            "first segment must now carry a .txnindex"
        );

        let rsm: Arc<dyn RemoteStorageManager> = Arc::new(LocalTieredStorage::new(remote_dir));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        for ex in &exports {
            let id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
            // Unwrap the log-layer `Offset`s into the remote-storage metadata's
            // `i64` world at the seam.
            let epochs: BTreeMap<LeaderEpoch, i64> = if ex.leader_epochs.is_empty() {
                BTreeMap::from([(LeaderEpoch(0), ex.base_offset.0)])
            } else {
                ex.leader_epochs
                    .iter()
                    .map(|&(epoch, off)| (epoch, off.0))
                    .collect()
            };
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
                ex.base_offset.0,
                ex.last_offset.0,
                ex.max_timestamp,
                1,
                ex.max_timestamp,
                crabka_remote_storage::RemoteLogSegmentDetails::new(
                    ex.size.bytes_i32(),
                    RemoteLogSegmentState::CopySegmentStarted,
                    epochs.clone(),
                ),
            )
            .unwrap();
            rlmm.add_remote_log_segment_metadata(md.clone()).unwrap();
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

        (RemoteReader::new(rsm, rlmm), log, abort)
    }

    #[tokio::test]
    async fn aborted_transactions_returns_copied_abort() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log, abort) = populated_reader_with_abort(log_dir.path(), remote_dir.path());
        let (start, last, pid) = abort;

        // Query the first segment's offset range → the abort overlaps.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), start, last)
            .await
            .expect("ok");
        let expected = vec![AbortedTxnEntry {
            start_offset: start,
            last_offset: last,
            producer_id: pid,
        }];
        assert!(got == expected, "the copied abort is returned");
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_segment_has_no_txnindex() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        // The default harness writes no `.txnindex` for any segment.
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let exports = log.tierable_segments();
        let seg = &exports[0];

        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), seg.base_offset.0, seg.last_offset.0)
            .await
            .expect("ok");
        assert!(
            got.is_empty(),
            "segment with no .txnindex yields an empty list, not an error"
        );
    }

    #[tokio::test]
    async fn fetch_batch_finds_segment_and_returns_first_batch() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());

        // Pick an offset inside the second sealed segment. Each batch covers
        // two records, so base_offset=2 lives in segment[1] (base=2).
        let exports = log.tierable_segments();
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[1].base_offset.0;

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), target_offset, 4096)
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
    async fn fetch_batch_uses_offset_relative_to_remote_segment_start() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 12, 4096)
            .await
            .expect("ok")
            .expect("offset 12 is in the synthetic remote segment");

        assert!(
            got.base_offset == 10,
            "relative offset 2 should read the first batch, not jump to {}",
            got.base_offset
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
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn aborted_transactions_empty_when_no_segment() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        // RLMM is empty → no covering segment → empty list, not an error.
        let got = reader
            .aborted_transactions(&tp(), LeaderEpoch(0), 0, 100)
            .await
            .expect("ok");
        assert!(got.is_empty());
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
            crabka_remote_storage::RemoteLogSegmentDetails::new(
                1024,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(LeaderEpoch(0), 0_i64)]),
            ),
        )
        .unwrap();
        rlmm.add_remote_log_segment_metadata(md).unwrap();
        let reader = RemoteReader::new(rsm, rlmm);
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 50, 4096)
            .await
            .unwrap();
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
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        let got = reader.earliest_offset(&tp()).await.unwrap();
        assert!(got == Some(expected));
    }

    #[tokio::test]
    async fn earliest_offset_returns_none_when_no_finished_segments() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> =
            Arc::new(InmemoryRemoteLogMetadataManager::new());
        let reader = RemoteReader::new(rsm, rlmm);
        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
    }

    #[tokio::test]
    async fn latest_tiered_offset_uses_highest_finished_segment_and_its_epoch() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());
        let expected = log
            .tierable_segments()
            .iter()
            .map(|segment| segment.last_offset.0)
            .max()
            .unwrap();

        let started_id = crabka_remote_storage::RemoteLogSegmentId::new(tp(), Uuid::new_v4());
        reader
            .rlmm
            .add_remote_log_segment_metadata(
                RemoteLogSegmentMetadata::new(
                    started_id,
                    expected + 1,
                    expected + 100,
                    0,
                    1,
                    0,
                    crabka_remote_storage::RemoteLogSegmentDetails::new(
                        1,
                        RemoteLogSegmentState::CopySegmentStarted,
                        BTreeMap::from([(LeaderEpoch(7), expected + 1)]),
                    ),
                )
                .unwrap(),
            )
            .unwrap();

        let got = reader
            .latest_tiered_offset(&tp())
            .await
            .unwrap()
            .expect("finished segments exist");
        assert!(
            got == TieredOffset {
                offset: expected,
                leader_epoch: LeaderEpoch(0),
            }
        );
    }

    struct SlowListRlmm {
        reactor_ticked: Arc<std::sync::atomic::AtomicBool>,
        observed_tick: Arc<std::sync::atomic::AtomicBool>,
    }

    impl RemoteLogMetadataManager for SlowListRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _metadata: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn update_remote_log_segment_metadata(
            &self,
            _update: crabka_remote_storage::RemoteLogSegmentMetadataUpdate,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }

        fn remote_log_segment_metadata(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(None)
        }

        fn highest_offset_for_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Option<i64>, RemoteStorageError> {
            Ok(None)
        }

        fn list_remote_log_segments(
            &self,
            _topic_id_partition: &TopicIdPartition,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.observed_tick.store(
                self.reactor_ticked
                    .load(std::sync::atomic::Ordering::Acquire),
                std::sync::atomic::Ordering::Release,
            );
            Ok(Vec::new())
        }

        fn list_remote_log_segments_by_epoch(
            &self,
            _topic_id_partition: &TopicIdPartition,
            _leader_epoch: LeaderEpoch,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(Vec::new())
        }

        fn put_remote_partition_delete_metadata(
            &self,
            _metadata: crabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metadata_listing_does_not_block_the_reactor() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let reactor_ticked = Arc::new(AtomicBool::new(false));
        let observed_tick = Arc::new(AtomicBool::new(false));
        let tick = reactor_ticked.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            tick.store(true, Ordering::Release);
        });
        let remote_dir = tempfile::tempdir().unwrap();
        let reader = RemoteReader::new(
            Arc::new(LocalTieredStorage::new(remote_dir.path())),
            Arc::new(SlowListRlmm {
                reactor_ticked,
                observed_tick: observed_tick.clone(),
            }),
        );

        assert!(reader.earliest_offset(&tp()).await.unwrap() == None);
        assert!(
            observed_tick.load(Ordering::Acquire),
            "the current-thread reactor must run while the blocking RLMM call is in flight"
        );
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
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let expected = exports.iter().map(|e| e.base_offset.0).min().unwrap();
        assert!(got == (expected, 0));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_before_sparse_ceiling() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 1_500)
            .await
            .unwrap()
            .expect("timestamp 1500 has a remote match");

        assert!(got == (12, 1_600));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_exact_indexed_record_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_000)
            .await
            .unwrap()
            .expect("timestamp 2000 has an exact record match");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_scans_segment_with_unknown_max_timestamp() {
        let (reader, _remote_dir) = sparse_remote_segment_reader_with_max_timestamp(-1);

        let got = reader
            .offset_for_timestamp(&tp(), 2_000)
            .await
            .unwrap()
            .expect("the unknown max sentinel must not suppress an exact remote scan");

        assert!(got == (14, 2_000));
    }

    #[tokio::test]
    async fn offset_for_timestamp_returns_none_when_past_last() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();
        let (reader, _log) = populated_reader(log_dir.path(), remote_dir.path());
        // All segments have max_ts=0 by construction (see test above); any
        // strictly-positive target is past every remote segment.
        let got = reader.offset_for_timestamp(&tp(), 1).await.unwrap();
        assert!(got == None);
    }

    // `NotReady` from the RLMM must propagate out of the reader
    // ── (not be swallowed as a miss), so the handlers can keep
    // ── OFFSET_OUT_OF_RANGE / answer conservatively.

    struct NotReadyRlmm;
    impl RemoteLogMetadataManager for NotReadyRlmm {
        fn add_remote_log_segment_metadata(
            &self,
            _m: RemoteLogSegmentMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
        fn update_remote_log_segment_metadata(
            &self,
            _u: crabka_remote_storage::RemoteLogSegmentMetadataUpdate,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
        fn remote_log_segment_metadata(
            &self,
            _tp: &TopicIdPartition,
            _epoch: LeaderEpoch,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::NotReady { partition: 3 })
        }
        fn highest_offset_for_epoch(
            &self,
            _tp: &TopicIdPartition,
            _epoch: LeaderEpoch,
        ) -> Result<Option<i64>, RemoteStorageError> {
            Ok(None)
        }
        fn list_remote_log_segments(
            &self,
            _tp: &TopicIdPartition,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::NotReady { partition: 3 })
        }
        fn list_remote_log_segments_by_epoch(
            &self,
            _tp: &TopicIdPartition,
            _epoch: LeaderEpoch,
        ) -> Result<Vec<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Ok(Vec::new())
        }
        fn put_remote_partition_delete_metadata(
            &self,
            _m: crabka_remote_storage::RemotePartitionDeleteMetadata,
        ) -> Result<(), RemoteStorageError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn fetch_batch_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader
            .fetch_batch(&tp(), LeaderEpoch(0), 0, 4096)
            .await
            .unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[tokio::test]
    async fn earliest_offset_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader.earliest_offset(&tp()).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
    }

    // ── I1: the list-based read paths (`earliest_offset` /
    // ── `offset_for_timestamp` → `list_remote_log_segments`) must observe
    // ── `NotReady` from the REAL `TopicBasedRemoteLogMetadataManager` while
    // ── an assigned metadata partition is still catching up, and an empty
    // ── result for a partition this broker does not own (Unassigned). The
    // ── `NotReadyRlmm` stub proves propagation through the reader; this test
    // ── proves the manager's list-path gate actually produces those states.

    /// Drives `reconcile_assignment` and blocks, off the reactor, until the
    /// list path stops returning `NotReady` for `tp`. At that point the
    /// partition is caught up to its assignment-time HWM.
    async fn assign_and_wait_ready(
        m: &Arc<crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager>,
        mp: i32,
        tp: &TopicIdPartition,
    ) {
        m.reconcile_assignment(&[mp]).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            // `list_remote_log_segments` is the method the list path uses.
            match m.list_remote_log_segments(tp) {
                Ok(_) => return,
                Err(RemoteStorageError::NotReady { .. }) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "list path never became ready"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(e) => panic!("unexpected list error: {e:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_path_observes_not_ready_and_unassigned_from_real_manager() {
        use crabka_remote_storage_topic::{
            InProcessMetadataEventLog, MetadataEventLog, TopicBasedRemoteLogMetadataManager,
            metadata_partition_for,
        };

        let topic_id = Uuid::from_u128(0xABCD);
        let owned = TopicIdPartition::new(topic_id, "orders", 0);
        let not_owned = TopicIdPartition::new(topic_id, "orders", 1);

        // Wide metadata topic so the two user-partitions land in distinct
        // metadata partitions.
        let n = 16;
        let mp_owned = metadata_partition_for(&owned, n);
        let mp_other = metadata_partition_for(&not_owned, n);
        assert!(mp_owned != mp_other, "test needs distinct metadata buckets");

        let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(n);

        let writer_snap_dir = tempfile::tempdir().unwrap();
        let mgr_snap_dir = tempfile::tempdir().unwrap();

        // Pre-seed a finished segment for the owned partition via a transient
        // all-consuming writer.
        {
            let writer = TopicBasedRemoteLogMetadataManager::start(
                log.clone(),
                tokio::runtime::Handle::current(),
                writer_snap_dir.path().to_path_buf(),
                std::time::Duration::from_hours(1),
            )
            .unwrap();
            writer
                .reconcile_assignment(&(0..n).collect::<Vec<_>>())
                .await;
            let id = crabka_remote_storage::RemoteLogSegmentId::new(owned.clone(), Uuid::new_v4());
            let md = RemoteLogSegmentMetadata::new(
                id.clone(),
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
            let w2 = writer.clone();
            let md2 = md.clone();
            tokio::task::spawn_blocking(move || {
                w2.add_remote_log_segment_metadata(md2).unwrap();
            })
            .await
            .unwrap();
            let w2 = writer.clone();
            tokio::task::spawn_blocking(move || {
                w2.update_remote_log_segment_metadata(
                    crabka_remote_storage::RemoteLogSegmentMetadataUpdate {
                        remote_log_segment_id: id,
                        event_timestamp_ms: 100,
                        custom_metadata: None,
                        state: RemoteLogSegmentState::CopySegmentFinished,
                        broker_id: 1,
                    },
                )
                .unwrap();
            })
            .await
            .unwrap();
            writer.shutdown();
        }

        // A fresh manager that consumes NOTHING until assigned.
        let m = TopicBasedRemoteLogMetadataManager::start(
            log.clone(),
            tokio::runtime::Handle::current(),
            mgr_snap_dir.path().to_path_buf(),
            std::time::Duration::from_hours(1),
        )
        .unwrap();

        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = m.clone();
        let reader = RemoteReader::new(rsm, rlmm);

        // Unowned partition (never assigned) → the list path treats it as a
        // genuine miss: empty, not an error.
        assert!(
            reader.earliest_offset(&not_owned).await.unwrap() == None,
            "unassigned partition is an empty list-path result, not NotReady"
        );

        // Assign the owned partition. Before catch-up the list path surfaces
        // NotReady through the reader. Poll until ready; observe at least the
        // ready (Some) terminal state.
        assign_and_wait_ready(&m, mp_owned, &owned).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == Some(0),
            "owned + caught up → real earliest from the remote tier"
        );

        // Remove the owned partition: the list path now returns empty (the
        // broker no longer owns it), NOT a stale segment.
        m.reconcile_assignment(&[]).await;
        assert!(
            reader.earliest_offset(&owned).await.unwrap() == None,
            "removed partition's list path returns empty, not stale segments"
        );

        m.shutdown();
    }

    /// The broker tiers segments under the leader epoch that was active at
    /// copy time. In normal operation `fetch_batch` receives the owning epoch,
    /// which the caller resolves from the leader-epoch checkpoint, and the
    /// epoch-indexed primary lookup hits.
    ///
    /// This test exercises the *defensive fallback*. The caller passes an
    /// epoch that is NOT in the segment's `segment_leader_epochs` map, which
    /// simulates a missing or empty checkpoint. The lineage-unmatched fallback
    /// must still resolve the segment through `list_remote_log_segments` and
    /// return the batch. It closes the wrong-segment hazard: it prefers
    /// lineage-matching candidates first, and uses `max_by_key(start_offset)`
    /// only as a last resort.
    #[tokio::test]
    async fn fallback_resolves_segment_across_leader_epoch_change() {
        let log_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();

        // `populated_reader` registers all segments under epoch 0 (the epoch
        // present in the tierable-segment export, defaulted to 0 when the log
        // was written without an explicit epoch).
        let (reader, log) = populated_reader(log_dir.path(), remote_dir.path());

        // Pick an offset inside the first sealed segment.
        let exports = log.tierable_segments();
        // Unwrap the log-layer `Offset` into this test's `i64` world at the seam.
        let target_offset = exports[0].base_offset.0;

        // Query with epoch 1 — the RLMM epoch-indexed primary path returns
        // None because the segment's `segment_leader_epochs` only contains
        // epoch 0.  The lineage-unmatched defensive fallback must find it via
        // `list_remote_log_segments` and return the batch.
        let got = reader
            .fetch_batch(&tp(), LeaderEpoch(1), target_offset, 4096)
            .await
            .expect("ok")
            .expect("defensive fallback must resolve the segment despite epoch mismatch");

        let last = got.base_offset + i64::from(got.last_offset_delta);
        assert!(
            got.base_offset <= target_offset && last >= target_offset,
            "batch [{},{}] doesn't cover target {target_offset}",
            got.base_offset,
            last,
        );
    }
}
