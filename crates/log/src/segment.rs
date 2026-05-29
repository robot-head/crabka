//! A single segment: `.log` + `.index` + `.timeindex` files sharing a
//! base offset.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use crabka_protocol::records::{HEADER_LEN, RecordBatch, RecordBatchHeader};
use zerocopy::FromBytes;

use crate::error::LogError;
use crate::index::{OffsetIndex, TimeIndex};
use crate::name;

/// A single log segment: the `.log` data file paired with its sparse
/// `.index` (offset → byte position) and `.timeindex` (timestamp →
/// relative offset) sidecars.
///
/// A segment is identified by its `base_offset`: the absolute offset of
/// its first record, encoded into the segment's 20-digit zero-padded
/// filename. Segments are created via [`Segment::create`] (new active
/// segment) or opened via [`Segment::open`] (read-only sealed segment)
/// or [`Segment::open_active`] (active segment with tail recovery).
#[derive(Debug)]
pub struct Segment {
    #[allow(dead_code)] // used by later phases (Log retention, recovery).
    dir: PathBuf,
    base_offset: i64,
    log_file: File,
    log_size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    /// `true` once a new segment has been started after this one. Sealed
    /// segments don't accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: i64,
}

/// Verbatim, decode-free output of [`Segment::read_raw`].
#[derive(Debug, Clone)]
pub struct RawSegmentRead {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: i64,
    /// Last absolute offset covered by `bytes` (`start_offset - 1` if empty).
    pub last_offset: i64,
    /// Verbatim `.log` bytes — one or more complete v2 batches.
    pub bytes: Bytes,
}

impl RawSegmentRead {
    fn empty() -> Self {
        Self {
            start_offset: 0,
            last_offset: -1,
            bytes: Bytes::new(),
        }
    }

    /// `true` when no batch bytes were returned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Segment {
    /// Create a fresh active segment at the given base offset. Fails if
    /// the `.log` file already exists.
    pub fn create(dir: &Path, base_offset: i64) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            log_size: 0,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    /// Open as the active segment, scanning from the last-indexed position
    /// to EOF when `validate` is true. A partial trailing batch (or one
    /// that fails to decode) is truncated; cleanly decoded batches update
    /// `last_offset` and `max_timestamp`.
    pub fn open_active(dir: &Path, base_offset: i64, validate: bool) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail()?;
        }
        Ok(seg)
    }

    fn recover_active_tail(&mut self) -> Result<(), LogError> {
        let scan_start = self
            .offset_index
            .last_entry()
            .map_or(0u64, |(_, pos)| u64::from(pos));
        if scan_start >= self.log_size {
            return Ok(());
        }

        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(scan_start))?;
        let mut buf = Vec::with_capacity(usize::try_from(self.log_size - scan_start).unwrap_or(0));
        f.read_to_end(&mut buf)?;

        let mut cur: &[u8] = &buf;
        let mut consumed: u64 = 0;
        let mut last_offset = self.last_offset;
        let mut max_ts = self.max_timestamp;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            consumed += (before - cur.len()) as u64;
            last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch.max_timestamp > max_ts {
                max_ts = batch.max_timestamp;
            }
        }

        let valid_end = scan_start + consumed;
        if valid_end < self.log_size {
            self.log_file.set_len(valid_end)?;
            self.log_size = valid_end;
        }
        self.last_offset = last_offset;
        self.max_timestamp = max_ts;
        Ok(())
    }

    /// Open an existing segment for reading. Lightweight — no full scan.
    /// Open an existing segment for reading. The log and index files
    /// must already exist on disk; the segment is initialized with
    /// `last_offset = base_offset - 1` and `max_timestamp = i64::MIN`
    /// until tail recovery (via [`Segment::open_active`]) populates them.
    pub fn open(dir: &Path, base_offset: i64) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let log_size = log_file.metadata()?.len();
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            log_size,
            offset_index,
            time_index,
            sealed: false,
            max_timestamp: i64::MIN,
            last_offset: base_offset - 1,
        })
    }

    /// Absolute offset of the first record this segment can hold.
    #[must_use]
    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    /// Path to this segment's `.txnindex` file (may not exist yet).
    #[must_use]
    pub fn txn_index_path(&self) -> std::path::PathBuf {
        crate::name::txnindex_path(&self.dir, self.base_offset)
    }

    /// Path to the per-partition `.leader-epoch-checkpoint` file in this
    /// segment's directory. The checkpoint is shared across all segments
    /// in a partition — epoch history accumulates over the log's lifetime.
    #[must_use]
    pub fn leader_epoch_checkpoint_path(&self) -> std::path::PathBuf {
        crate::name::leader_epoch_checkpoint_path(&self.dir)
    }

    /// Highest absolute offset (inclusive) of any batch appended to this
    /// segment. Returns `base_offset - 1` for an empty segment.
    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Current `.log` file size in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.log_size
    }

    /// Highest timestamp observed across all batches in this segment.
    /// Returns `i64::MIN` for an empty segment.
    #[must_use]
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// `true` once the segment has been sealed via [`Segment::seal`];
    /// sealed segments reject appends.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Read batches starting at or just before `offset`, up to roughly
    /// `max_bytes` of `.log` data. Returns an empty `Vec` when `offset`
    /// is past `last_offset`.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<Vec<RecordBatch>, LogError> {
        if offset > self.last_offset {
            return Ok(vec![]);
        }
        let target_rel = u32::try_from((offset - self.base_offset).max(0))
            .map_err(|_| LogError::BadSegmentName("target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        let initial_cap = max_bytes.min(4 * 1024 * 1024);
        let mut buf: Vec<u8> = Vec::with_capacity(initial_cap);
        self.read_log_range(start_pos, &mut buf, max_bytes)?;

        let mut out: Vec<RecordBatch> = Vec::new();
        let mut total: usize = 0;
        let mut cursor: &[u8] = &buf;
        while !cursor.is_empty() {
            let before = cursor.len();
            let Ok(batch) = RecordBatch::decode(&mut cursor) else {
                break; // partial trailing batch — stop.
            };
            let consumed = before - cursor.len();
            let batch_last = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch_last >= offset {
                out.push(batch);
                total += consumed;
                if total >= max_bytes {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// Read a contiguous run of **complete, verbatim** record-batch bytes
    /// beginning at the batch containing `fetch_offset`, including only
    /// batches whose `base_offset < limit_offset`, up to roughly `max_bytes`
    /// (always at least one batch — Kafka's anti-stall rule). No record
    /// decoding: only fixed batch headers are read to find boundaries.
    pub fn read_raw(
        &self,
        fetch_offset: i64,
        limit_offset: i64,
        max_bytes: usize,
    ) -> Result<RawSegmentRead, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(RawSegmentRead::empty());
        }
        let target_rel = u32::try_from((fetch_offset - self.base_offset).max(0))
            .map_err(|_| LogError::Corrupt("read_raw target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        let first_read = max_bytes.max(HEADER_LEN);
        let mut buf: Vec<u8> = Vec::with_capacity(first_read.min(4 * 1024 * 1024));
        self.read_log_range(start_pos, &mut buf, first_read)?;

        let mut pos = 0usize;
        let mut range_start: Option<usize> = None;
        let mut range_end = 0usize;
        let mut start_offset = fetch_offset;
        let mut last_offset = fetch_offset - 1;

        loop {
            if pos + HEADER_LEN > buf.len() {
                break;
            }
            let hdr = RecordBatchHeader::ref_from_bytes(&buf[pos..pos + HEADER_LEN])
                .map_err(|_| LogError::Corrupt("record batch header".into()))?;
            let base = hdr.base_offset.get();
            let batch_len = usize::try_from(hdr.batch_length.get().max(0)).unwrap_or(0);
            let total = 12 + batch_len;
            let batch_last = base + i64::from(hdr.last_offset_delta.get());

            if batch_last < fetch_offset {
                pos += total;
                continue;
            }
            if base >= limit_offset {
                break;
            }
            if pos + total > buf.len() {
                if range_start.is_none() {
                    let mut one: Vec<u8> = Vec::with_capacity(total);
                    self.read_log_range(start_pos + pos as u64, &mut one, total)?;
                    if one.len() < total {
                        break;
                    }
                    return Ok(RawSegmentRead {
                        start_offset: base,
                        last_offset: batch_last,
                        bytes: Bytes::from(one),
                    });
                }
                break;
            }

            if range_start.is_none() {
                range_start = Some(pos);
                start_offset = base;
            }
            range_end = pos + total;
            last_offset = batch_last;
            pos += total;

            if range_end - range_start.expect("set above") >= max_bytes {
                break;
            }
        }

        match range_start {
            Some(s) => Ok(RawSegmentRead {
                start_offset,
                last_offset,
                bytes: Bytes::from(buf).slice(s..range_end),
            }),
            None => Ok(RawSegmentRead::empty()),
        }
    }

    fn read_log_range(
        &self,
        start_pos: u64,
        buf: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<(), LogError> {
        let available = self.log_size.saturating_sub(start_pos);
        let to_read = available.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(start_pos))?;
        let mut bounded = f.take(to_read);
        bounded.read_to_end(buf)?;
        Ok(())
    }

    /// Append a record batch. Returns the byte position where the batch
    /// starts.
    ///
    /// Side effects:
    /// - Updates `log_size`, `max_timestamp`, `last_offset`.
    /// - Adds sparse index entries when bytes-since-last-entry exceeds
    ///   `index_interval_bytes` (or for the first batch).
    pub fn append(
        &mut self,
        batch: &RecordBatch,
        index_interval_bytes: u32,
    ) -> Result<u64, LogError> {
        use std::io::Write;

        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }

        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf)?;
        let bytes = buf.freeze();

        let position = self.log_size;
        self.log_file.seek(SeekFrom::End(0))?;
        self.log_file.write_all(&bytes)?;
        self.log_size += bytes.len() as u64;

        let last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
        self.last_offset = last_offset;
        if batch.max_timestamp > self.max_timestamp {
            self.max_timestamp = batch.max_timestamp;
        }

        let should_index = match self.offset_index.last_entry() {
            None => true,
            Some((_, last_pos)) => {
                position.saturating_sub(u64::from(last_pos)) >= u64::from(index_interval_bytes)
            }
        };
        if should_index {
            let rel = u32::try_from(batch.base_offset - self.base_offset)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos_u32 = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            self.offset_index.append(rel, pos_u32)?;
            self.time_index.append(self.max_timestamp, rel)?;
        }

        Ok(position)
    }

    /// Mark this segment as sealed. No more appends.
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    /// Directory holding this segment's `.log`/`.index`/`.timeindex` files.
    /// Used by the compactor to read the underlying `.log` file directly,
    /// bypassing the `Segment::read` path which depends on the in-memory
    /// `last_offset` (which is stale for sealed segments loaded via
    /// `Segment::open`).
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Force-sync everything to disk.
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.log_file.sync_data()?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    /// Truncate `.log` and indexes so no batches at `relative_offset` `>= rel`
    /// remain. Used by `Log::truncate_to`. Leaves the segment unsealed.
    pub fn truncate_to_relative(&mut self, rel: u32) -> Result<(), LogError> {
        let mut f = self.log_file.try_clone()?;
        f.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let target_abs = self.base_offset + i64::from(rel);
        let mut cur: &[u8] = &buf;
        let mut pos: u64 = 0;
        let mut last_kept_offset = self.base_offset - 1;
        let mut last_kept_ts = i64::MIN;
        while !cur.is_empty() {
            let before = cur.len();
            let Ok(batch) = RecordBatch::decode(&mut cur) else {
                break;
            };
            let batch_last_offset = batch.base_offset + i64::from(batch.last_offset_delta);
            if batch_last_offset >= target_abs {
                break;
            }
            pos += (before - cur.len()) as u64;
            last_kept_offset = batch_last_offset;
            if batch.max_timestamp > last_kept_ts {
                last_kept_ts = batch.max_timestamp;
            }
        }

        self.log_file.set_len(pos)?;
        self.log_size = pos;
        self.last_offset = last_kept_offset;
        self.max_timestamp = last_kept_ts;

        let pos_u32 =
            u32::try_from(pos).map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(pos_u32)?;
        self.time_index.truncate_by_relative_offset(rel)?;
        self.sealed = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::tempdir;

    fn sample_batch(base_offset: i64, n: i32, ts_base: i64) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset,
            base_timestamp: ts_base,
            max_timestamp: ts_base + i64::from(n),
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                timestamp_delta: i64::from(i),
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            });
        }
        b
    }

    #[test]
    fn append_then_read_back() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        let b1 = sample_batch(0, 3, 1_000_000);
        let b2 = sample_batch(3, 2, 2_000_000);
        seg.append(&b1, 4096).unwrap();
        seg.append(&b2, 4096).unwrap();
        assert_eq!(seg.last_offset(), 4);
        let read = seg.read(0, usize::MAX).unwrap();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0].records.len(), 3);
        assert_eq!(read[1].records.len(), 2);
    }

    #[test]
    fn read_at_higher_offset_skips_earlier_batches() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.append(&sample_batch(0, 3, 1_000_000), 4096).unwrap();
        seg.append(&sample_batch(3, 2, 2_000_000), 4096).unwrap();
        let read = seg.read(4, usize::MAX).unwrap();
        // Offset 4 falls inside the second batch (offsets 3..=4).
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].base_offset, 3);
    }

    #[test]
    fn append_to_sealed_segment_errors() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.seal();
        assert!(seg.is_sealed());
        let err = seg.append(&sample_batch(0, 1, 0), 4096).unwrap_err();
        assert!(matches!(err, LogError::Io(_)));
    }

    #[test]
    fn read_past_last_offset_returns_empty() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.append(&sample_batch(0, 2, 1_000), 4096).unwrap();
        let read = seg.read(100, usize::MAX).unwrap();
        assert!(read.is_empty());
    }

    #[test]
    fn flush_succeeds() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        seg.append(&sample_batch(0, 1, 42), 4096).unwrap();
        seg.flush().unwrap();
    }

    // ---- read_raw (decode-free) tests ----

    fn test_segment() -> (tempfile::TempDir, Segment) {
        let dir = tempdir().unwrap();
        let seg = Segment::create(dir.path(), 0).unwrap();
        (dir, seg)
    }

    fn test_batch_at(off: i64) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset: off,
            base_timestamp: 1_000,
            max_timestamp: 1_000,
            last_offset_delta: 0,
            ..RecordBatch::default()
        };
        b.records.push(Record {
            offset_delta: 0,
            timestamp_delta: 0,
            value: Some(Bytes::from(format!("v{off}"))),
            ..Default::default()
        });
        b
    }

    #[test]
    fn read_raw_is_byte_exact_and_multi_batch() {
        let (dir, mut seg) = test_segment();
        let mut wire = bytes::BytesMut::new();
        for off in 0..3i64 {
            let b = test_batch_at(off);
            seg.append(&b, 0).unwrap();
            b.encode(&mut wire).unwrap();
        }
        let wire = wire.freeze();
        let r = seg.read_raw(0, 3, 10 * 1024 * 1024).unwrap();
        assert_eq!(r.start_offset, 0);
        assert_eq!(r.last_offset, 2);
        assert_eq!(
            &r.bytes[..],
            &wire[..],
            "raw bytes must equal the on-disk concatenation"
        );
        let mut cur: &[u8] = &r.bytes;
        let mut n = 0;
        while !cur.is_empty() {
            crabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
            n += 1;
        }
        assert_eq!(n, 3);
        drop(dir);
    }

    #[test]
    fn read_raw_clamps_at_limit_offset() {
        let (dir, mut seg) = test_segment();
        for off in 0..3i64 {
            seg.append(&test_batch_at(off), 0).unwrap();
        }
        let r = seg.read_raw(0, 2, 10 * 1024 * 1024).unwrap();
        assert_eq!(r.last_offset, 1);
        drop(dir);
    }

    #[test]
    fn read_raw_returns_at_least_one_batch_over_budget() {
        let (dir, mut seg) = test_segment();
        seg.append(&test_batch_at(0), 0).unwrap();
        let r = seg.read_raw(0, 1, 1).unwrap();
        assert_eq!(r.start_offset, 0);
        assert_eq!(r.last_offset, 0);
        assert!(!r.bytes.is_empty());
        drop(dir);
    }
}
