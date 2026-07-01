//! KIP-405: remote read path. Wraps the broker's shared
//! [`RemoteStorageManager`] + [`RemoteLogMetadataManager`] pair and serves
//! `Fetch` / `ListOffsets` requests for offsets that no longer have a local
//! copy.
//!
//! The RSM SPI is synchronous + blocking; this module wraps every byte-range
//! and index read in `tokio::task::spawn_blocking` so the broker's reactor
//! never stalls on remote-tier I/O. The pure index-decode helpers mirror
//! `crabka_log::index::{OffsetIndex,TimeIndex}::lookup` against the
//! Kafka-format index bytes written verbatim by the copy path.

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
pub(crate) struct OffsetIndexEntry {
    relative_offset: U32<BigEndian>,
    position: U32<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<OffsetIndexEntry>() == 8);

/// 12 bytes per entry: ts i64 BE + rel u32 BE. Mirrors
/// `crabka_log::index::TimeEntryRaw`.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct TimeIndexEntry {
    timestamp: I64<BigEndian>,
    relative_offset: U32<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<TimeIndexEntry>() == 12);

/// 24 bytes per entry: `start_offset` i64 BE + `last_offset` i64 BE +
/// `producer_id` i64 BE. Mirrors `crabka_log::txn_index::AbortedTxnRaw` so the
/// remote-tier copy of a `.txnindex` file decodes through the same byte layout
/// the local index was written with.
#[derive(Debug, Clone, Copy, FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct AbortedTxnIndexEntry {
    start_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    producer_id: I64<BigEndian>,
}

const _: () = assert!(std::mem::size_of::<AbortedTxnIndexEntry>() == 24);

/// One decoded aborted-transaction entry from a remote segment's `.txnindex`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AbortedTxnEntry {
    pub(crate) start_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) producer_id: i64,
}

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

    /// Aborted transactions overlapping the inclusive offset range
    /// `[from_offset, to_offset]` in the finished remote segment covering
    /// `from_offset`. Returns an empty `Vec` when no finished segment covers
    /// the offset, when the segment carries no transaction index
    /// (`SegmentNotFound` from `fetch_index`), or when nothing overlaps.
    pub(crate) async fn aborted_transactions(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: i32,
        from_offset: i64,
        to_offset: i64,
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
        let entries = parse_time_index(&index_bytes)?;
        let Some(rel) = relative_offset_for_timestamp(entries, target_timestamp) else {
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

/// Helper for the `ref_from_bytes` parse error on the remote-read path. The
/// `zerocopy` cast can only fail on a length mismatch, but the bytes come from
/// the object store (S3 etc.), which can return corrupt/truncated data — so we
/// surface a `RemoteStorageError` rather than panicking (a `DoS` surface).
fn corrupt_index(kind: &str) -> RemoteStorageError {
    RemoteStorageError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("corrupt remote {kind} index bytes"),
    ))
}

/// Borrow Kafka's `OffsetIndex` on-disk format as a zero-copy
/// `&[OffsetIndexEntry]` (8 bytes / entry: rel u32 BE + pos u32 BE). Trailing
/// bytes that don't complete an 8-byte entry are ignored. Borrows from `bytes`.
pub(crate) fn parse_offset_index(bytes: &[u8]) -> Result<&[OffsetIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / 8) * 8;
    <[OffsetIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("offset"))
}

/// Floor lookup: byte position of the largest entry with `rel <= target_rel`,
/// or 0 when empty / target is before the first entry. Runs directly against
/// the borrowed zero-copy slice — no owned `Vec` is materialized.
#[must_use]
pub(crate) fn position_for_relative_offset(entries: &[OffsetIndexEntry], target_rel: u32) -> u32 {
    match entries.binary_search_by_key(&target_rel, |e| e.relative_offset.get()) {
        Ok(i) => entries[i].position.get(),
        Err(0) => 0,
        Err(i) => entries[i - 1].position.get(),
    }
}

/// Borrow Kafka's `TimeIndex` on-disk format as a zero-copy
/// `&[TimeIndexEntry]` (12 bytes / entry: ts i64 BE + rel u32 BE). Trailing
/// bytes that don't complete a 12-byte entry are ignored. Borrows from `bytes`.
pub(crate) fn parse_time_index(bytes: &[u8]) -> Result<&[TimeIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / 12) * 12;
    <[TimeIndexEntry]>::ref_from_bytes(&bytes[..truncated_len]).map_err(|_| corrupt_index("time"))
}

/// Borrow Kafka's transaction-index format as a zero-copy
/// `&[AbortedTxnIndexEntry]` (24 bytes / entry: `start_offset` i64 BE,
/// `last_offset` i64 BE, `producer_id` i64 BE). Trailing bytes that don't
/// complete a 24-byte entry are ignored. Borrows from `bytes`.
pub(crate) fn parse_txn_index(bytes: &[u8]) -> Result<&[AbortedTxnIndexEntry], RemoteStorageError> {
    let truncated_len = (bytes.len() / 24) * 24;
    <[AbortedTxnIndexEntry]>::ref_from_bytes(&bytes[..truncated_len])
        .map_err(|_| corrupt_index("transaction"))
}

/// Whether an aborted-transaction entry overlaps the inclusive offset range
/// `[from_offset, to_offset]`. Mirrors `TxnIndex::aborted_in_range`'s overlap
/// test against an inclusive range: the entry's `[start, last]` intersects
/// `[from, to]` iff `start <= to && last >= from`.
#[must_use]
pub(crate) fn txn_overlaps(entry: &AbortedTxnIndexEntry, from_offset: i64, to_offset: i64) -> bool {
    entry.start_offset.get() <= to_offset && entry.last_offset.get() >= from_offset
}

/// First entry whose `ts >= target_ts`, returning the relative offset, or
/// `None` when none qualify.
#[must_use]
pub(crate) fn relative_offset_for_timestamp(
    entries: &[TimeIndexEntry],
    target_ts: i64,
) -> Option<u32> {
    entries
        .iter()
        .find(|e| e.timestamp.get() >= target_ts)
        .map(|e| e.relative_offset.get())
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
    use assert2::assert;

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
        let got = (
            position_for_relative_offset(&entries, 10),  // exact
            position_for_relative_offset(&entries, 15),  // between
            position_for_relative_offset(&entries, 0),   // first entry exact
            position_for_relative_offset(&entries, 100), // after last
            position_for_relative_offset(&[], 50),       // empty
        );
        assert!(got == (256, 256, 0, 1024, 0));
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
    fn relative_offset_for_timestamp_returns_first_ge() {
        let entries = time_entries(&[(1_000, 0), (2_000, 10), (3_000, 20)]);
        let got = (
            relative_offset_for_timestamp(&entries, 1_000), // exact match
            relative_offset_for_timestamp(&entries, 1_500), // between → next
            relative_offset_for_timestamp(&entries, 4_000), // after last
            relative_offset_for_timestamp(&[], 1_000),      // empty
        );
        assert!(got == (Some(0), Some(10), None, None));
    }

    #[test]
    fn end_position_for_caps_with_max_bytes() {
        let got = (
            // start=0, segment=1024, max_bytes=256 → exclusive_end=256 →
            // inclusive=255.
            end_position_for(0, 1024, 256),
            // max_bytes >= remaining → read to end.
            end_position_for(512, 1024, 999_999),
            // max_bytes=0 → read to end (zero is a no-cap sentinel).
            end_position_for(0, 1024, 0),
            // start past the segment-end cap still safe via saturating add.
            end_position_for(u32::MAX, 1024, 100),
        );
        assert!(got == (Some(255), None, None, None));
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
        assert!(got.base_offset == 10);

        // Floor below everything → first batch.
        let got = first_batch_at_or_after(&bytes, 0).expect("found batch");
        assert!(got.base_offset == 0);

        // Floor above everything → None.
        assert!(first_batch_at_or_after(&bytes, 1_000).is_none());

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
        let got = (
            // Range fully before the entry → excluded.
            txn_overlaps(&e, 0, 9),
            // Range touching the entry's first offset → included.
            txn_overlaps(&e, 0, 10),
            // Range fully inside the entry → included.
            txn_overlaps(&e, 11, 13),
            // Range touching the entry's last offset → included.
            txn_overlaps(&e, 14, 100),
            // Range fully after the entry → excluded.
            txn_overlaps(&e, 15, 100),
            // Range fully covering the entry → included.
            txn_overlaps(&e, 0, 100),
        );
        assert!(got == (false, true, true, true, false, true));
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

    fn sparse_remote_segment_reader() -> (RemoteReader, tempfile::TempDir) {
        let source_dir = tempfile::tempdir().unwrap();
        let remote_dir = tempfile::tempdir().unwrap();

        let first = test_batch_at(10, 4, b'a');
        let second = test_batch_at(14, 3, b'b');
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
            &time_index_bytes(&[(1_000, 0), (2_000, 4)]),
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
            2_000,
            1,
            2_000,
            i32::try_from(log_bytes.len()).unwrap_or(i32::MAX),
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0_i32, 10_i64)]),
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
                event_timestamp_ms: 2_000,
                custom_metadata: None,
                state: RemoteLogSegmentState::CopySegmentFinished,
                broker_id: 1,
            },
        )
        .unwrap();

        (RemoteReader::new(rsm, rlmm), remote_dir)
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

    /// Like `populated_reader`, but before copying, writes a single aborted-txn
    /// entry into the first sealed segment's `.txnindex` (24 BE bytes:
    /// `start_offset`, `last_offset`, `producer_id`) so the copy path carries
    /// it to the remote tier. Returns the reader, the log, and the
    /// `(start_offset, last_offset, producer_id)` written.
    fn populated_reader_with_abort(
        log_dir: &std::path::Path,
        remote_dir: &std::path::Path,
    ) -> (RemoteReader, Log, (i64, i64, i64)) {
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

        // Write a `.txnindex` next to the first sealed segment's `.log` so the
        // export below picks it up. The abort covers the whole first segment.
        let first = &exports[0];
        let abort = (first.base_offset, first.last_offset, 7777_i64);
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
            .aborted_transactions(&tp(), 0, start, last)
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
            .aborted_transactions(&tp(), 0, seg.base_offset, seg.last_offset)
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
    async fn fetch_batch_uses_offset_relative_to_remote_segment_start() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .fetch_batch(&tp(), 0, 12, 4096)
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
        let got = reader.fetch_batch(&tp(), 0, 0, 4096).await.unwrap();
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
            .aborted_transactions(&tp(), 0, 0, 100)
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
        assert!(reader.earliest_offset(&tp()).unwrap() == None);
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
        assert!(got == expected);
    }

    #[tokio::test]
    async fn offset_for_timestamp_adds_relative_offset_to_segment_start() {
        let (reader, _remote_dir) = sparse_remote_segment_reader();

        let got = reader
            .offset_for_timestamp(&tp(), 2_000)
            .await
            .unwrap()
            .expect("timestamp 2000 is indexed");

        assert!(got == 14);
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
            _epoch: i32,
            _offset: i64,
        ) -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError> {
            Err(RemoteStorageError::NotReady { partition: 3 })
        }
        fn highest_offset_for_epoch(
            &self,
            _tp: &TopicIdPartition,
            _epoch: i32,
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
            _epoch: i32,
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
        let err = reader.fetch_batch(&tp(), 0, 0, 4096).await.unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { partition: 3 }));
    }

    #[tokio::test]
    async fn earliest_offset_propagates_not_ready() {
        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = Arc::new(NotReadyRlmm);
        let reader = RemoteReader::new(rsm, rlmm);
        let err = reader.earliest_offset(&tp()).unwrap_err();
        assert!(matches!(err, RemoteStorageError::NotReady { .. }));
    }

    // ── I1: the list-based read paths (`earliest_offset` /
    // ── `offset_for_timestamp` → `list_remote_log_segments`) must observe
    // ── `NotReady` from the REAL `TopicBasedRemoteLogMetadataManager` while
    // ── an assigned metadata partition is still catching up, and an empty
    // ── result for a partition this broker does not own (Unassigned). The
    // ── `NotReadyRlmm` stub proves propagation through the reader; this test
    // ── proves the manager's list-path gate actually produces those states.

    /// Drive `reconcile_assignment` and block (off the reactor) until the
    /// list path stops returning `NotReady` for `tp`, i.e. the partition is
    /// caught up to its assignment-time HWM.
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
            .await
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
                2048,
                RemoteLogSegmentState::CopySegmentStarted,
                BTreeMap::from([(0, 0)]),
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
        .await
        .unwrap();

        let remote_dir = tempfile::tempdir().unwrap();
        let rsm: Arc<dyn RemoteStorageManager> =
            Arc::new(LocalTieredStorage::new(remote_dir.path()));
        let rlmm: Arc<dyn RemoteLogMetadataManager> = m.clone();
        let reader = RemoteReader::new(rsm, rlmm);

        // Unowned partition (never assigned) → the list path treats it as a
        // genuine miss: empty, not an error.
        assert!(
            reader.earliest_offset(&not_owned).unwrap() == None,
            "unassigned partition is an empty list-path result, not NotReady"
        );

        // Assign the owned partition. Before catch-up the list path surfaces
        // NotReady through the reader. Poll until ready; observe at least the
        // ready (Some) terminal state.
        assign_and_wait_ready(&m, mp_owned, &owned).await;
        assert!(
            reader.earliest_offset(&owned).unwrap() == Some(0),
            "owned + caught up → real earliest from the remote tier"
        );

        // Remove the owned partition: the list path now returns empty (the
        // broker no longer owns it), NOT a stale segment.
        m.reconcile_assignment(&[]).await;
        assert!(
            reader.earliest_offset(&owned).unwrap() == None,
            "removed partition's list path returns empty, not stale segments"
        );

        m.shutdown();
    }

    /// Segments are tiered under the leader epoch that was active at copy time.
    /// In normal operation `fetch_batch` receives the owning epoch (resolved
    /// from the leader-epoch checkpoint by the caller) and the epoch-indexed
    /// primary lookup hits.  This test exercises the *defensive fallback*: a
    /// caller passes an epoch that is NOT in the segment's
    /// `segment_leader_epochs` map (simulating a missing / empty checkpoint).
    /// The lineage-unmatched fallback must still resolve the segment via
    /// `list_remote_log_segments` and return the batch, closing the
    /// wrong-segment hazard by preferring lineage-matching candidates first
    /// and only resorting to `max_by_key(start_offset)` as a last resort.
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
        let target_offset = exports[0].base_offset;

        // Query with epoch 1 — the RLMM epoch-indexed primary path returns
        // None because the segment's `segment_leader_epochs` only contains
        // epoch 0.  The lineage-unmatched defensive fallback must find it via
        // `list_remote_log_segments` and return the batch.
        let got = reader
            .fetch_batch(&tp(), 1, target_offset, 4096)
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
