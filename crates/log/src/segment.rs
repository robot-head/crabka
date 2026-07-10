//! A single segment: `.log` + `.index` + `.timeindex` files sharing a
//! base offset.

use std::{
    fs::{File, OpenOptions},
    io::{IoSlice, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use crabka_ids::{LeaderEpoch, Offset};
use crabka_protocol::records::{
    HEADER_LEN, RecordBatch, RecordBatchHeader, patch_base_offset_and_leader_epoch,
};
use tracing::instrument;
use zerocopy::FromBytes;

use crate::{
    error::LogError,
    index::{OffsetIndex, TimeIndex},
    name,
};

/// Positioned read: fill `buf` from `offset` in `file` without moving the
/// file's cursor, looping over short reads until `buf` is full or EOF.
/// Returns the number of bytes read. Lets readers share the writer's
/// `File` handle (`&self`) with no `dup(2)`/`lseek(2)` per call — the
/// hot fetch path runs this for every read.
fn read_full_at(file: &File, mut offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match read_at(file, offset, &mut buf[total..]) {
            Ok(0) => break, // EOF
            Ok(n) => {
                total += n;
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

fn seek_to_log_size(file: &File, log_size: u64) -> std::io::Result<()> {
    (&*file).seek(SeekFrom::Start(log_size))?;
    Ok(())
}

fn write_all_vectored(mut writer: impl Write, mut bufs: &mut [IoSlice<'_>]) -> std::io::Result<()> {
    while !bufs.is_empty() {
        let written = writer.write_vectored(bufs)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

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
    base_offset: Offset,
    /// The `.log` data file. Wrapped in `Arc` so the zero-copy fetch path
    /// (Increment D) can hand a `FileRegion { file: Arc<File>, .. }` to the
    /// connection's async `sendfile` loop; the `Arc` pins the inode through the
    /// send even if retention rolls/removes this segment in the meantime (the
    /// open fd keeps the inode alive on Unix). Writes go through `&*log_file`
    /// (`std::fs::File` implements `Write`/`Seek` for `&File`).
    log_file: Arc<File>,
    log_size: u64,
    offset_index: OffsetIndex,
    time_index: TimeIndex,
    /// `true` once a new segment has been started after this one. Sealed
    /// segments don't accept appends.
    sealed: bool,
    /// Highest timestamp observed across all batches written here.
    max_timestamp: i64,
    /// Last absolute offset (inclusive) of any batch in this segment.
    last_offset: Offset,
}

/// Verbatim, decode-free output of [`Segment::read_raw`].
#[derive(Debug, Clone)]
pub struct RawSegmentRead {
    /// `base_offset` of the first included batch (≤ requested offset).
    pub start_offset: Offset,
    /// Last absolute offset covered by `bytes` (`start_offset - 1` if empty).
    pub last_offset: Offset,
    /// Verbatim `.log` bytes — one or more complete v2 batches.
    pub bytes: Bytes,
}

crate::sendfile_cfg! {
    /// Descriptor form of [`Segment::read_raw`] for the zero-copy fetch path
    /// (Increments D + E): the same offset/boundary metadata, but the records
    /// run is a [`FileRegion`] (an `(Arc<File>, offset, len)` descriptor) instead
    /// of an owned `Bytes` slice — so the broker can `sendfile(2)` it straight
    /// from the page cache without a userspace copy. Compiled on the SENDFILE
    /// alias (Linux + Apple + FreeBSD/DragonFly).
    #[derive(Debug, Clone)]
    pub struct RawSegmentDesc {
        /// `base_offset` of the first included batch (≤ requested offset).
        pub start_offset: Offset,
        /// Last absolute offset covered by the region (`start_offset - 1` if empty).
        pub last_offset: Offset,
        /// The records run, as a file-backed descriptor. `None` when no complete
        /// batch was found in range.
        pub region: Option<crabka_protocol::records::FileRegion>,
    }

    impl RawSegmentDesc {
        fn empty() -> Self {
            Self {
                start_offset: Offset(0),
                last_offset: Offset(-1),
                region: None,
            }
        }

        /// Byte length of the region (0 when empty).
        #[must_use]
        pub fn len(&self) -> usize {
            self.region.as_ref().map_or(0, |r| r.len)
        }

        /// `true` when no batch bytes were described.
        #[must_use]
        pub fn is_empty(&self) -> bool {
            self.region.is_none()
        }
    }
}

impl RawSegmentRead {
    // The `last_offset` of an empty read is a never-read sentinel: every
    // consumer guards on `is_empty()` (which checks `bytes`) before touching
    // `last_offset`, so `Offset(-1)` vs `Offset(1)` is unobservable.
    #[cfg_attr(test, mutants::skip)]
    fn empty() -> Self {
        Self {
            start_offset: Offset(0),
            last_offset: Offset(-1),
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
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    pub fn create(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&log_path)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
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
    #[instrument(
        level = "info",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0, validate),
        err,
    )]
    pub fn open_active(dir: &Path, base_offset: Offset, validate: bool) -> Result<Self, LogError> {
        let mut seg = Self::open(dir, base_offset)?;
        if validate {
            seg.recover_active_tail()?;
        }
        Ok(seg)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            base_offset = self.base_offset.0,
            log_size = self.log_size,
            recovered_last_offset = tracing::field::Empty,
        ),
        err,
    )]
    fn recover_active_tail(&mut self) -> Result<(), LogError> {
        let scan_start = self
            .offset_index
            .last_entry()
            .map_or(0u64, |(_, pos)| u64::from(pos));
        if scan_start >= self.log_size {
            return Ok(());
        }

        let mut buf = Vec::new();
        let to_read = usize::try_from(self.log_size - scan_start).unwrap_or(usize::MAX);
        self.read_log_range(scan_start, &mut buf, to_read)?;

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
            last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            if batch.max_timestamp > max_ts {
                max_ts = batch.max_timestamp;
            }
        }

        let valid_end = scan_start + consumed;
        if valid_end < self.log_size {
            self.log_file.set_len(valid_end)?;
            self.log_size = valid_end;
        }
        seek_to_log_size(&self.log_file, self.log_size)?;
        self.last_offset = last_offset;
        self.max_timestamp = max_ts;
        tracing::Span::current().record("recovered_last_offset", last_offset.0);
        Ok(())
    }

    /// Open an existing segment for reading. Lightweight — no full scan.
    /// Open an existing segment for reading. The log and index files
    /// must already exist on disk; the segment is initialized with
    /// `last_offset = base_offset - 1` and `max_timestamp = i64::MIN`
    /// until tail recovery (via [`Segment::open_active`]) populates them.
    #[instrument(
        level = "debug",
        skip_all,
        fields(dir = %dir.display(), base_offset = base_offset.0),
        err,
    )]
    pub fn open(dir: &Path, base_offset: Offset) -> Result<Self, LogError> {
        let log_path = name::log_path(dir, base_offset.0);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let log_size = log_file.metadata()?.len();
        seek_to_log_size(&log_file, log_size)?;
        let offset_index = OffsetIndex::open(&name::index_path(dir, base_offset.0))?;
        let time_index = TimeIndex::open(&name::timeindex_path(dir, base_offset.0))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file: Arc::new(log_file),
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
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Path to this segment's `.txnindex` file (may not exist yet).
    #[must_use]
    pub fn txn_index_path(&self) -> std::path::PathBuf {
        crate::name::txnindex_path(&self.dir, self.base_offset.0)
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
    pub fn last_offset(&self) -> Offset {
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

    /// Absolute offset and record timestamp of the first record in this
    /// segment whose timestamp is `>= target_ts`. Uses the sparse time
    /// index for a floor position, then scans `.log` batches forward
    /// (the index is sparse, so an exact answer needs the post-index
    /// scan — matching Kafka's `LogSegment.findOffsetByTimestamp`).
    /// Returns `None` when no record in this segment qualifies.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        let floor_rel = self.time_index.lookup(target_ts);
        let scan_from = self.base_offset + i64::from(floor_rel);
        self.scan_from_floor(scan_from, |ts| ts >= target_ts)
    }

    /// Absolute offset and timestamp of the record carrying this
    /// segment's `max_timestamp`. Ties resolve to the earliest offset
    /// (Kafka). Returns `None` for an empty segment. Uses the time
    /// index's floor for the max to start the scan, then scans forward
    /// for the first record whose timestamp equals the segment max.
    #[must_use]
    pub fn offset_of_max_timestamp(&self) -> Option<(Offset, i64)> {
        if self.max_timestamp == i64::MIN {
            return None;
        }
        let floor_rel = self.time_index.lookup(self.max_timestamp);
        let scan_from = self.base_offset + i64::from(floor_rel);
        // Equality against `max_timestamp` is safe because Kafka's batch
        // `max_timestamp` is always a real record timestamp (the largest
        // among the batch's records), so some record's timestamp equals
        // the segment max exactly.
        self.scan_from_floor(scan_from, |ts| ts == self.max_timestamp)
    }

    /// Scan `.log` batches forward from `floor_offset`, returning the
    /// (absolute offset, timestamp) of the first record whose timestamp
    /// satisfies `pred`, or `None` at end of segment.
    ///
    /// Reads in bounded windows rather than slurping the whole segment
    /// tail: the common case (a match within the first window) costs one
    /// small read. When a window yields no match, the cursor advances
    /// past the last batch read and the next window is fetched. The loop
    /// terminates once the cursor passes `last_offset` (see termination
    /// argument in [`Segment::scan_from_floor_windowed`]).
    fn scan_from_floor(
        &self,
        floor_offset: Offset,
        pred: impl Fn(i64) -> bool,
    ) -> Option<(Offset, i64)> {
        // One window roughly covers a default index interval's worth of
        // log bytes, so a floor lookup typically lands a match in the
        // first read.
        const SCAN_WINDOW_BYTES: usize = 64 * 1024;
        self.scan_from_floor_windowed(floor_offset, SCAN_WINDOW_BYTES, pred)
    }

    /// Window-size-parameterized core of [`Segment::scan_from_floor`].
    /// Split out so tests can force multi-window scans with a tiny window.
    ///
    /// Termination: each iteration either (a) returns a match, (b) returns
    /// `None` because `cursor > last_offset`, or (c) decodes at least one
    /// full batch and advances `cursor` strictly past it. `read` caps
    /// reads at `max_bytes` and (unlike `read_raw`) has no anti-stall
    /// guarantee, so a single batch larger than the window decodes to an
    /// empty `Vec`; we detect that (empty result while `cursor` is still
    /// within the segment) and double the window before retrying, so the
    /// window is bounded by the largest batch rather than the whole tail.
    fn scan_from_floor_windowed(
        &self,
        floor_offset: Offset,
        window_bytes: usize,
        pred: impl Fn(i64) -> bool,
    ) -> Option<(Offset, i64)> {
        let mut cursor = floor_offset;
        let mut window = window_bytes.max(1);
        loop {
            if cursor > self.last_offset {
                return None;
            }
            let batches = self.read(cursor, window).ok()?;
            if batches.is_empty() {
                // The batch at `cursor` is larger than the window, so it
                // could not be fully decoded. Grow the window and retry
                // the same cursor; bounded by the largest batch size.
                window = window.saturating_mul(2);
                continue;
            }
            for batch in &batches {
                for rec in &batch.records {
                    let ts = batch.base_timestamp + rec.timestamp_delta;
                    if pred(ts) {
                        return Some((Offset(batch.base_offset + i64::from(rec.offset_delta)), ts));
                    }
                }
            }
            // No match in this window; resume just past the last batch
            // read. `read` includes the batch covering `cursor`, so
            // `last_read` >= cursor and the cursor strictly advances.
            let last = batches.last().expect("non-empty checked above");
            let last_read = Offset(last.base_offset + i64::from(last.last_offset_delta));
            cursor = last_read + 1;
        }
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
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0, batches = tracing::field::Empty),
        err,
    )]
    pub fn read(&self, offset: Offset, max_bytes: usize) -> Result<Vec<RecordBatch>, LogError> {
        if offset > self.last_offset {
            return Ok(vec![]);
        }
        let target_rel = u32::try_from((offset.0 - self.base_offset.0).max(0))
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
            let batch_last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            if batch_last >= offset {
                out.push(batch);
                total += consumed;
                if total >= max_bytes {
                    break;
                }
            }
        }
        tracing::Span::current().record("batches", out.len());
        Ok(out)
    }

    /// Read a contiguous run of **complete, verbatim** record-batch bytes
    /// beginning at the batch containing `fetch_offset`, including only
    /// batches whose `base_offset < limit_offset`, up to roughly `max_bytes`
    /// (always at least one batch — Kafka's anti-stall rule). No record
    /// decoding: only fixed batch headers are read to find boundaries.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0, bytes = tracing::field::Empty),
        err,
    )]
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_bytes: usize,
    ) -> Result<RawSegmentRead, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(RawSegmentRead::empty());
        }
        let target_rel = u32::try_from((fetch_offset.0 - self.base_offset.0).max(0))
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
            // Wire values from the fixed v2 header stay raw `i64`.
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
                        start_offset: Offset(base),
                        last_offset: Offset(batch_last),
                        bytes: Bytes::from(one),
                    });
                }
                break;
            }

            if range_start.is_none() {
                range_start = Some(pos);
                start_offset = Offset(base);
            }
            range_end = pos + total;
            last_offset = Offset(batch_last);
            pos += total;

            if range_end - range_start.expect("set above") >= max_bytes {
                break;
            }
        }

        match range_start {
            Some(s) => {
                let bytes = Bytes::from(buf).slice(s..range_end);
                tracing::Span::current().record("bytes", bytes.len());
                Ok(RawSegmentRead {
                    start_offset,
                    last_offset,
                    bytes,
                })
            }
            None => Ok(RawSegmentRead::empty()),
        }
    }

    crate::sendfile_cfg! {
    /// Descriptor variant of [`Segment::read_raw`] for the zero-copy
    /// (`sendfile`) fetch path: runs the **same** boundary walk — selecting the
    /// identical `[start_pos+range_start, start_pos+range_end)` byte range that
    /// `read_raw` would have sliced — but returns a [`FileRegion`] descriptor
    /// instead of `pread`ing the payload into an owned `Bytes`.
    ///
    /// The walk is header-only: it `pread`s just the fixed v2 batch headers to
    /// find batch boundaries (using the header's `batch_length`), never the
    /// record payloads. The resulting region is byte-identical to `read_raw`'s
    /// `bytes` for the same `(fetch_offset, limit_offset, max_bytes)`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(base_offset = self.base_offset.0),
        err,
    )]
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_bytes: usize,
    ) -> Result<RawSegmentDesc, LogError> {
        if fetch_offset > self.last_offset || fetch_offset >= limit_offset {
            return Ok(RawSegmentDesc::empty());
        }
        let target_rel = u32::try_from((fetch_offset.0 - self.base_offset.0).max(0))
            .map_err(|_| LogError::Corrupt("read_raw_desc target offset out of range".into()))?;
        let start_pos = u64::from(self.offset_index.lookup(target_rel));

        // Mirror `read_raw`'s windowing **exactly** so the chosen byte range is
        // byte-identical. `read_raw` first reads `first_read = max_bytes.max(
        // HEADER_LEN)` bytes (capped by the bytes available after `start_pos`)
        // into a buffer, then only includes a batch whose end lands within that
        // buffer. A batch that straddles the buffer end is included **only** as
        // the single anti-stall batch when nothing has been included yet (it is
        // then re-read in full if it's complete on disk). We reproduce that with
        // a `window` instead of an actual payload read — the scan stays
        // header-only.
        let first_read = max_bytes.max(HEADER_LEN) as u64;
        let available = self.log_size.saturating_sub(start_pos);
        let window = first_read.min(available); // == read_raw's buf.len()

        let mut pos: u64 = 0;
        let mut range_start: Option<u64> = None;
        let mut range_end: u64 = 0;
        let mut start_offset = fetch_offset;
        let mut last_offset = fetch_offset - 1;
        let mut hdr_buf = [0u8; HEADER_LEN];

        loop {
            // `read_raw` breaks when the next header can't fit in the window.
            if pos + HEADER_LEN as u64 > window {
                break;
            }
            let n = read_full_at(&self.log_file, start_pos + pos, &mut hdr_buf)?;
            if n < HEADER_LEN {
                break;
            }
            let hdr = RecordBatchHeader::ref_from_bytes(&hdr_buf)
                .map_err(|_| LogError::Corrupt("record batch header".into()))?;
            // Wire values from the fixed v2 header stay raw `i64`.
            let base = hdr.base_offset.get();
            let batch_len = usize::try_from(hdr.batch_length.get().max(0)).unwrap_or(0);
            let total = 12 + batch_len as u64;
            let batch_last = base + i64::from(hdr.last_offset_delta.get());

            if batch_last < fetch_offset {
                pos += total;
                continue;
            }
            if base >= limit_offset {
                break;
            }
            // Batch straddles the window end. `read_raw` re-reads exactly one
            // such batch when nothing is buffered yet (anti-stall: always return
            // at least one complete batch), provided it's complete on disk.
            if pos + total > window {
                if range_start.is_none() {
                    if start_pos + pos + total > self.log_size {
                        // Not a complete batch on disk — `read_raw` breaks.
                        break;
                    }
                    let len = usize::try_from(total)
                        .map_err(|_| LogError::Corrupt("read_raw_desc batch too large".into()))?;
                    return Ok(RawSegmentDesc {
                        start_offset: Offset(base),
                        last_offset: Offset(batch_last),
                        region: Some(crabka_protocol::records::FileRegion {
                            file: Arc::clone(&self.log_file),
                            offset: start_pos + pos,
                            len,
                        }),
                    });
                }
                break;
            }

            if range_start.is_none() {
                range_start = Some(pos);
                start_offset = Offset(base);
            }
            range_end = pos + total;
            last_offset = Offset(batch_last);
            pos += total;

            if range_end - range_start.expect("set above") >= max_bytes as u64 {
                break;
            }
        }

        match range_start {
            Some(s) => {
                let len = usize::try_from(range_end - s)
                    .map_err(|_| LogError::Corrupt("read_raw_desc region too large".into()))?;
                Ok(RawSegmentDesc {
                    start_offset,
                    last_offset,
                    region: Some(crabka_protocol::records::FileRegion {
                        file: Arc::clone(&self.log_file),
                        offset: start_pos + s,
                        len,
                    }),
                })
            }
            None => Ok(RawSegmentDesc::empty()),
        }
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
        let to_read = usize::try_from(to_read).unwrap_or(usize::MAX);
        let base = buf.len();
        buf.resize(base + to_read, 0);
        let n = read_full_at(&self.log_file, start_pos, &mut buf[base..])?;
        buf.truncate(base + n);
        Ok(())
    }

    /// Append a record batch. Returns the byte position where the batch
    /// starts.
    ///
    /// Side effects:
    /// - Updates `log_size`, `max_timestamp`, `last_offset`.
    /// - Adds sparse index entries when bytes-since-last-entry exceeds
    ///   `index_interval_bytes` (or for the first batch).
    #[instrument(
        level = "debug",
        skip(self, batch),
        fields(
            base_offset = self.base_offset.0,
            batch_base = batch.base_offset,
            bytes = batch.encoded_len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    pub fn append(
        &mut self,
        batch: &RecordBatch,
        index_interval_bytes: u32,
    ) -> Result<u64, LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }

        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut buf)?;
        let bytes = buf.freeze();

        let position = self.log_size;
        // The active file cursor is kept at log_size by open/recovery/truncate,
        // so the hot append path does not need an lseek before every write.
        (&*self.log_file).write_all(&bytes)?;
        self.log_size += bytes.len() as u64;

        let last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
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
            let rel = u32::try_from(batch.base_offset - self.base_offset.0)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos_u32 = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            self.offset_index.append(rel, pos_u32)?;
            self.time_index.append(self.max_timestamp, rel)?;
        }

        tracing::Span::current().record("position", position);
        Ok(position)
    }

    /// Append a batch **verbatim**, writing the producer's exact wire
    /// bytes without decode/re-encode/recompress/CRC-recompute.
    ///
    /// `bytes` is the producer's verbatim v2 batch (already CRC-validated
    /// by the caller via the borrowed header-only path). This patches only
    /// `base_offset` (bytes 0..8) and `partition_leader_epoch`
    /// (bytes 12..16) — both outside the CRC-covered region — into a
    /// writable copy, then writes those bytes. The stored CRC stays
    /// byte-identical to the producer's, because no CRC-covered byte
    /// changes.
    ///
    /// `base_offset`, `last_offset_delta`, `max_timestamp`, and
    /// `leader_epoch` come from the caller's borrowed header read; the
    /// segment side effects (`log_size`, `last_offset`, `max_timestamp`,
    /// sparse index) are updated identically to [`Segment::append`].
    ///
    /// Returns the byte position where the batch starts.
    #[instrument(
        level = "debug",
        skip(self, bytes),
        fields(
            seg_base_offset = self.base_offset.0,
            base_offset = base_offset.0,
            bytes_len = bytes.len(),
            position = tracing::field::Empty,
        ),
        err,
    )]
    // The only mutant here flips the sparse-index `rel = batch_base - seg_base`
    // to `+`, corrupting an OFFSET-INDEX hint only. Every read/truncate path
    // treats the index as a lower-bound hint and re-scans + filters, so an
    // inflated `rel` (seg_base > 0) resolves to the from-start fallback and
    // yields identical output; the last_offset/index-presence effects are
    // pinned by `append_verbatim_updates_index_and_last_offset`.
    #[cfg_attr(test, mutants::skip)]
    pub fn append_verbatim(
        &mut self,
        bytes: &[u8],
        base_offset: Offset,
        last_offset_delta: i32,
        max_timestamp: i64,
        leader_epoch: LeaderEpoch,
        index_interval_bytes: u32,
    ) -> Result<u64, LogError> {
        if self.sealed {
            return Err(LogError::Io(std::io::Error::other("segment is sealed")));
        }
        if bytes.len() < HEADER_LEN {
            return Err(LogError::Corrupt(
                "verbatim batch shorter than v2 header".into(),
            ));
        }

        // Patch base_offset + partition_leader_epoch in a copy of *just* the
        // fixed-size header — both fields live below byte 16, well under the
        // CRC-covered region (byte 21), so the producer's CRC stays valid (no
        // recompute). The batch BODY is written straight from the input slice
        // with no copy: the previous `bytes.to_vec()` was a full-payload memcpy
        // on the produce hot path (100 KiB+ per batch for large messages), the
        // dominant remaining produce-side cost. The active file cursor is kept
        // at log_size, so one writev appends the patched header plus original
        // body without an lseek or full-payload copy.
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&bytes[..HEADER_LEN]);
        // The protocol patcher writes the raw KIP-320 wire `int32`; unwrap here.
        patch_base_offset_and_leader_epoch(&mut header, base_offset.0, leader_epoch.0);

        let position = self.log_size;
        let mut bufs = [IoSlice::new(&header), IoSlice::new(&bytes[HEADER_LEN..])];
        write_all_vectored(&*self.log_file, &mut bufs)?;
        self.log_size += bytes.len() as u64;

        let last_offset = base_offset + i64::from(last_offset_delta);
        self.last_offset = last_offset;
        if max_timestamp > self.max_timestamp {
            self.max_timestamp = max_timestamp;
        }

        let should_index = match self.offset_index.last_entry() {
            None => true,
            Some((_, last_pos)) => {
                position.saturating_sub(u64::from(last_pos)) >= u64::from(index_interval_bytes)
            }
        };
        if should_index {
            let rel = u32::try_from(base_offset.0 - self.base_offset.0)
                .map_err(|_| LogError::BadSegmentName("offset overflow in segment".into()))?;
            let pos_u32 = u32::try_from(position)
                .map_err(|_| LogError::BadSegmentName("position overflow in segment".into()))?;
            self.offset_index.append(rel, pos_u32)?;
            self.time_index.append(self.max_timestamp, rel)?;
        }

        tracing::Span::current().record("position", position);
        Ok(position)
    }

    /// Mark this segment as sealed. No more appends.
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    /// Seal a segment loaded via the no-scan [`Segment::open`] path, fixing its
    /// `last_offset` to `last` (callers pass `next_segment.base_offset - 1`, the
    /// highest offset this sealed segment can hold).
    ///
    /// `Segment::open` leaves `last_offset = base_offset - 1` because it does
    /// not scan the `.log`. Without this fix a sealed segment recovered on
    /// [`Log::open`](crate::Log) reports that stale `last_offset`, and
    /// `Log::read_raw` — which skips any segment whose `last_offset() <
    /// fetch_offset` — would skip the first sealed segment after a restart and
    /// serve a later segment's base offset, manufacturing an offset gap (a
    /// follower fetching at 0 then loops on the resulting append mismatch).
    pub fn seal_at(&mut self, last: Offset) {
        self.sealed = true;
        self.last_offset = last;
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
    #[instrument(
        level = "debug",
        skip_all,
        fields(base_offset = self.base_offset.0, log_size = self.log_size),
        err,
    )]
    pub fn flush(&mut self) -> Result<(), LogError> {
        self.log_file.sync_data()?;
        self.offset_index.flush()?;
        self.time_index.flush()?;
        Ok(())
    }

    /// Truncate `.log` and indexes so no batches at `relative_offset` `>= rel`
    /// remain. Used by `Log::truncate_to`. Leaves the segment unsealed.
    #[instrument(
        level = "info",
        skip(self),
        fields(base_offset = self.base_offset.0, new_last_offset = tracing::field::Empty),
        err,
    )]
    pub fn truncate_to_relative(&mut self, rel: u32) -> Result<(), LogError> {
        // Read only as far as the cut can be: every kept batch lives below
        // the first index entry at or after `rel`. When `rel` is past the
        // last index entry, fall back to the whole file. This avoids
        // slurping the discarded tail on each truncate.
        let read_limit = self
            .offset_index
            .position_at_or_after(rel)
            .map_or(self.log_size, u64::from);
        let mut buf = Vec::new();
        let to_read = usize::try_from(read_limit).unwrap_or(usize::MAX);
        self.read_log_range(0, &mut buf, to_read)?;

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
            let batch_last_offset = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
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
        seek_to_log_size(&self.log_file, pos)?;
        self.log_size = pos;
        self.last_offset = last_kept_offset;
        self.max_timestamp = last_kept_ts;

        let pos_u32 =
            u32::try_from(pos).map_err(|_| LogError::BadSegmentName("position overflow".into()))?;
        self.offset_index.truncate_by_position(pos_u32)?;
        self.time_index.truncate_by_relative_offset(rel)?;
        self.sealed = false;
        tracing::Span::current().record("new_last_offset", self.last_offset.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use crabka_ids::Offset;
    use crabka_protocol::records::{Record, RecordBatch};
    use tempfile::tempdir;

    use super::*;

    fn sample_batch(base_offset: i64, n: i32, ts_base: i64) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset,
            base_timestamp: ts_base,
            max_timestamp: ts_base + i64::from(n - 1),
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
    fn offset_for_timestamp_finds_first_ge() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Two batches: offsets 0..=2 ts 100..=102, offsets 3..=4 ts 200..=201.
        seg.append(&sample_batch(0, 3, 100), 0).unwrap();
        seg.append(&sample_batch(3, 2, 200), 0).unwrap();
        // sample_batch sets per-record timestamp_delta = i, base_timestamp = ts_base.
        // Batch 1 records: (off0,ts100),(off1,ts101),(off2,ts102).
        // Batch 2 records: (off3,ts200),(off4,ts201).
        for (name, ts, want) in [
            ("first exact", 100, Some((Offset(0), 100))),
            ("within first batch", 101, Some((Offset(1), 101))),
            ("between batches", 150, Some((Offset(3), 200))),
            ("last exact", 201, Some((Offset(4), 201))),
            ("past end", 202, None),
        ] {
            check!(seg.offset_for_timestamp(ts) == want, "case {name}: ts={ts}");
        }
        drop(dir);
    }

    #[test]
    fn scan_from_floor_finds_match_beyond_first_window() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Many single-record batches with increasing timestamps. With a
        // tiny scan window each batch lands in its own window, so a match
        // at the tail forces the windowed loop to advance many times.
        let n = 50i64;
        for off in 0..n {
            let mut b = RecordBatch {
                base_offset: off,
                base_timestamp: 1_000 + off,
                max_timestamp: 1_000 + off,
                last_offset_delta: 0,
                ..RecordBatch::default()
            };
            b.records.push(Record {
                offset_delta: 0,
                timestamp_delta: 0,
                value: Some(Bytes::from(format!("v{off}"))),
                ..Default::default()
            });
            seg.append(&b, 0).unwrap();
        }
        // A window of 1 byte forces one batch per read (anti-stall rule).
        // Target ts is the very last record's, so the loop must advance
        // through every window before matching.
        let target = 1_000 + (n - 1);
        for (_name, threshold, expected) in [
            (
                "match at final record",
                target,
                Some((Offset(n - 1), target)),
            ),
            ("no matching record", 10_001, None),
        ] {
            assert2::assert!(
                seg.scan_from_floor_windowed(Offset(0), 1, |ts| ts >= threshold) == expected
            );
        }
        drop(dir);
    }

    #[test]
    fn scan_returns_absolute_offset_of_matching_record() {
        // A full-size window keeps the match in the first read so the
        // cursor-advance path isn't involved.
        const WINDOW: usize = 64 * 1024;
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // A leading single-record batch at offset 0, then a 3-record batch
        // based at offset 1 (abs offsets 1,2,3; timestamps 200,201,202). The
        // match is the *third* record, whose absolute offset is
        // `base_offset + offset_delta = 1 + 2 = 3` — a value that only a
        // correct `+` reproduces (`1 - 2` or `1 * 2` both differ), so this
        // pins the returned offset arithmetic.
        seg.append(&sample_batch(0, 1, 100), 0).unwrap();
        seg.append(&sample_batch(1, 3, 200), 0).unwrap();
        let got = seg.scan_from_floor_windowed(Offset(0), WINDOW, |ts| ts >= 202);
        assert2::assert!(got == Some((Offset(3), 202)));
        drop(dir);
    }

    #[test]
    fn offset_of_max_timestamp_earliest_on_tie() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // Batch records ts: 100,101,102 (max in batch = 102 at offset 2).
        seg.append(&sample_batch(0, 3, 100), 0).unwrap();
        // Second batch: offsets 3,4 ts 200,201 — segment max becomes 201 @4.
        seg.append(&sample_batch(3, 2, 200), 0).unwrap();
        assert2::assert!(seg.offset_of_max_timestamp() == Some((Offset(4), 201)));

        // Empty segment → None.
        let dir2 = tempdir().unwrap();
        let empty = Segment::create(dir2.path(), Offset(0)).unwrap();
        assert2::assert!(empty.offset_of_max_timestamp() == None);
        drop(dir);
        drop(dir2);
    }

    #[test]
    fn offset_of_max_timestamp_tie_picks_earliest() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        // All three records share timestamp 500; earliest offset is 0.
        let mut b = RecordBatch {
            base_offset: 0,
            base_timestamp: 500,
            max_timestamp: 500,
            last_offset_delta: 2,
            ..RecordBatch::default()
        };
        for i in 0..3 {
            b.records.push(Record {
                offset_delta: i,
                timestamp_delta: 0,
                value: Some(Bytes::from("v")),
                ..Default::default()
            });
        }
        seg.append(&b, 0).unwrap();
        assert2::assert!(seg.offset_of_max_timestamp() == Some((Offset(0), 500)));
        drop(dir);
    }

    #[test]
    fn append_then_read_back() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        let b1 = sample_batch(0, 3, 1_000_000);
        let b2 = sample_batch(3, 2, 2_000_000);
        seg.append(&b1, 4096).unwrap();
        seg.append(&b2, 4096).unwrap();
        let read = seg.read(Offset(0), usize::MAX).unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(read == vec![b1, b2]);
    }

    #[test]
    fn append_after_open_active_writes_at_eof() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 1, 100), 0).unwrap();
            seg.append(&sample_batch(1, 1, 200), 0).unwrap();
        }

        let mut seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        let position = seg.append(&sample_batch(2, 1, 300), 0).unwrap();

        let read = seg.read(Offset(0), usize::MAX).unwrap();
        assert2::assert!(position > 0);
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(
            read == vec![
                sample_batch(0, 1, 100),
                sample_batch(1, 1, 200),
                sample_batch(2, 1, 300),
            ]
        );
    }

    /// Tail recovery must PHYSICALLY truncate a partial/garbage trailing tail,
    /// using `consumed += before - cur.len()` (the exact bytes each valid batch
    /// decode advanced) to locate the valid end. Mutating `-`→`+` inflates
    /// `consumed`, pushing `valid_end` past `log_size` so the garbage is never
    /// truncated: the file would keep its trailing bytes.
    #[test]
    fn recover_active_tail_truncates_trailing_garbage() {
        let dir = tempdir().unwrap();
        let valid_size = {
            let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
            seg.append(&sample_batch(0, 3, 100), 0).unwrap();
            seg.append(&sample_batch(3, 2, 200), 0).unwrap();
            seg.flush().unwrap();
            seg.size_bytes()
        };

        // Append 16 bytes of garbage (an undecodable partial batch tail).
        let log_path = name::log_path(dir.path(), 0);
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        f.write_all(&[0xCD; 16]).unwrap();
        f.sync_data().unwrap();
        drop(f);

        // Reopen with validation: the tail scan must clip the garbage.
        let seg = Segment::open_active(dir.path(), Offset(0), true).unwrap();
        assert2::assert!(seg.last_offset() == Offset(4));
        assert2::assert!(seg.size_bytes() == valid_size);
    }

    // `Segment::read` maps the absolute fetch offset to a relative index key
    // via `offset - base_offset`. With a dense index and base_offset 100,
    // reading from offset 103 must start at the batch containing 103 and
    // return the batches at 103 and 105. Mutating `-`→`+` computes
    // `103 + 100 = 203`, whose index lookup lands at (or past) the last
    // batch, skipping the offset-103 batch.
    #[test]
    fn read_uses_relative_offset_for_index_lookup() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(100)).unwrap();
        // Dense index (interval 0 → every batch indexed).
        seg.append(&sample_batch(100, 3, 100), 0).unwrap(); // offsets 100..=102
        seg.append(&sample_batch(103, 2, 200), 0).unwrap(); // offsets 103..=104
        seg.append(&sample_batch(105, 1, 300), 0).unwrap(); // offset 105
        let read = seg.read(Offset(103), usize::MAX).unwrap();
        assert2::assert!(seg.last_offset() == Offset(105));
        assert2::assert!(read == vec![sample_batch(103, 2, 200), sample_batch(105, 1, 300)]);
    }

    // `Segment::read_raw` maps the fetch offset to the relative index key the
    // same way. base_offset 100, dense index, `read_raw(103)` must begin at
    // the offset-103 batch (`start_offset == 103`). Mutating `-`→`+` computes
    // `203`, whose lookup skips past the offset-103 batch → `start_offset`
    // becomes 105.
    #[test]
    fn read_raw_uses_relative_offset_for_index_lookup() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(100)).unwrap();
        seg.append(&sample_batch(100, 3, 100), 0).unwrap(); // offsets 100..=102
        seg.append(&sample_batch(103, 2, 200), 0).unwrap(); // offsets 103..=104
        seg.append(&sample_batch(105, 1, 300), 0).unwrap(); // offset 105

        let r = seg.read_raw(Offset(103), Offset(1000), usize::MAX).unwrap();
        assert2::assert!(!r.is_empty());
        assert2::assert!(r.start_offset == Offset(103));
    }

    /// `Segment::read` accumulates consumed bytes as `before - cursor.len()`
    /// (the exact bytes each batch decode advanced) to enforce the `max_bytes`
    /// budget. With `max_bytes` set to the segment's full size, all three
    /// batches fit and are returned. Mutating `-`→`+` inflates `consumed` on
    /// the first batch past `max_bytes`, breaking after one batch.
    #[test]
    fn read_consumed_bytes_gates_max_bytes_budget() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 100), 0).unwrap();
        seg.append(&sample_batch(1, 1, 200), 0).unwrap();
        seg.append(&sample_batch(2, 1, 300), 0).unwrap();
        // Exactly the whole segment: correct consumed accounting fits all three
        // batches; inflated accounting overshoots after the first.
        let max_bytes = usize::try_from(seg.size_bytes()).unwrap();

        let read = seg.read(Offset(0), max_bytes).unwrap();
        assert2::assert!(
            read == vec![
                sample_batch(0, 1, 100),
                sample_batch(1, 1, 200),
                sample_batch(2, 1, 300),
            ]
        );
    }

    #[test]
    fn append_after_truncate_writes_at_new_eof() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 100), 0).unwrap();
        let expected_position = seg.size_bytes();
        seg.append(&sample_batch(1, 1, 200), 0).unwrap();

        seg.truncate_to_relative(1).unwrap();
        let position = seg.append(&sample_batch(1, 1, 300), 0).unwrap();

        let read = seg.read(Offset(0), usize::MAX).unwrap();
        assert2::assert!(position == expected_position);
        assert2::assert!(seg.last_offset() == Offset(1));
        assert2::assert!(read == vec![sample_batch(0, 1, 100), sample_batch(1, 1, 300)]);
    }

    /// `truncate_to_relative` decides which batches to drop by each batch's last
    /// offset, `batch.base_offset + last_offset_delta`, compared against
    /// `target_abs`. Using MULTI-record batches makes the `+` load-bearing:
    /// batch A spans 0..=2, batch B spans 3..=5, and truncating to rel 3
    /// (`target_abs = 3`) must keep A (last 2 < 3) and drop B (last 5 >= 3).
    /// Mutating `+`→`-` computes A's last as -2 and B's as 1, so B is wrongly
    /// kept and the read still returns batch B.
    #[test]
    fn truncate_to_relative_uses_batch_last_offset() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 3, 100), 0).unwrap(); // offsets 0..=2
        seg.append(&sample_batch(3, 3, 200), 0).unwrap(); // offsets 3..=5
        assert2::assert!(seg.last_offset() == 5);

        // target_abs = base(0) + rel(3) = 3. Drop batches with last >= 3.
        seg.truncate_to_relative(3).unwrap();
        let read = seg.read(Offset(0), usize::MAX).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(read == vec![sample_batch(0, 3, 100)]);
    }

    #[test]
    fn read_at_higher_offset_skips_earlier_batches() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 3, 1_000_000), 4096).unwrap();
        seg.append(&sample_batch(3, 2, 2_000_000), 4096).unwrap();
        let read = seg.read(Offset(4), usize::MAX).unwrap();
        // Offset 4 falls inside the second batch (offsets 3..=4).
        assert2::assert!(read == vec![sample_batch(3, 2, 2_000_000)]);
    }

    #[test]
    fn append_to_sealed_segment_errors() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.seal();
        assert2::assert!(seg.is_sealed());
        let err = seg.append(&sample_batch(0, 1, 0), 4096).unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
    }

    #[test]
    fn read_past_last_offset_returns_empty() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 2, 1_000), 4096).unwrap();
        let read = seg.read(Offset(100), usize::MAX).unwrap();
        assert2::assert!(read.is_empty());
    }

    #[test]
    fn flush_succeeds() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), Offset(0)).unwrap();
        seg.append(&sample_batch(0, 1, 42), 4096).unwrap();
        seg.flush().unwrap();
    }

    // ---- read_raw (decode-free) tests ----

    fn test_segment() -> (tempfile::TempDir, Segment) {
        let dir = tempdir().unwrap();
        let seg = Segment::create(dir.path(), Offset(0)).unwrap();
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
        let r = seg
            .read_raw(Offset(0), Offset(3), 10 * 1024 * 1024)
            .unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(2));
        assert2::assert!(&r.bytes[..] == &wire[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_clamps_at_limit_offset() {
        let (dir, mut seg) = test_segment();
        let mut expected = bytes::BytesMut::new();
        for off in 0..3i64 {
            let batch = test_batch_at(off);
            seg.append(&batch, 0).unwrap();
            if off < 2 {
                batch.encode(&mut expected).unwrap();
            }
        }
        let r = seg
            .read_raw(Offset(0), Offset(2), 10 * 1024 * 1024)
            .unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(1));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }

    #[test]
    fn read_raw_returns_at_least_one_batch_over_budget() {
        let (dir, mut seg) = test_segment();
        let batch = test_batch_at(0);
        let mut expected = bytes::BytesMut::new();
        batch.encode(&mut expected).unwrap();
        seg.append(&batch, 0).unwrap();
        let r = seg.read_raw(Offset(0), Offset(1), 1).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.last_offset == Offset(0));
        assert2::assert!(&r.bytes[..] == &expected[..]);
        drop(dir);
    }

    // ---- read_raw_desc (zero-copy descriptor) tests (SENDFILE-alias only) ----

    crate::sendfile_cfg! {
    /// `pread` a `FileRegion` into a fresh `Vec` (the bytes the broker's
    /// sendfile would transmit / its TLS pread-fallback would copy).
    fn region_bytes(region: &crabka_protocol::records::FileRegion) -> Vec<u8> {
        use std::os::unix::fs::FileExt;
        let mut buf = vec![0u8; region.len];
        let mut filled = 0;
        let mut off = region.offset;
        while filled < buf.len() {
            let n = region.file.read_at(&mut buf[filled..], off).unwrap();
            assert2::assert!(n > 0);
            filled += n;
            off += n as u64;
        }
        buf
    }

    /// The load-bearing Increment-D/E invariant: the `read_raw_desc` region maps
    /// to exactly the bytes `read_raw` would have returned, for the same
    /// `(fetch_offset, limit_offset, max_bytes)`. Covers single-batch,
    /// multi-batch, mid-stream start offsets, the limit clamp, and the
    /// one-batch-over-budget anti-stall rule.
    #[test]
    fn read_raw_desc_region_equals_read_raw_bytes() {
        let (dir, mut seg) = test_segment();
        for off in 0..5i64 {
            seg.append(&test_batch_at(off), 0).unwrap();
        }
        let cases = [
            ("all batches", 0i64, 5i64, 10 * 1024 * 1024usize),
            ("limit clamp", 0, 3, 10 * 1024 * 1024),
            ("mid-stream start", 2, 5, 10 * 1024 * 1024),
            ("one-batch anti-stall", 0, 5, 1),
            ("last batch", 4, 5, 10 * 1024 * 1024),
        ];
        for (_name, fo, lo, mb) in cases {
            let raw = seg.read_raw(Offset(fo), Offset(lo), mb).unwrap();
            let desc = seg.read_raw_desc(Offset(fo), Offset(lo), mb).unwrap();
            assert2::assert!(desc.start_offset == raw.start_offset);
            assert2::assert!(desc.last_offset == raw.last_offset);
            match &desc.region {
                Some(region) => {
                    assert2::assert!(region.len == raw.bytes.len());
                    assert2::assert!(region_bytes(region) == raw.bytes.to_vec());
                }
                None => assert2::assert!(raw.bytes.is_empty()),
            }
        }
        drop(dir);
    }

    /// A truncated trailing batch (byte budget cuts mid-batch) must produce a
    /// region whose bytes equal `read_raw`'s clipped output — sendfile of a
    /// clipped range is wire-valid (the consumer drops the partial batch).
    #[test]
    fn read_raw_desc_matches_read_raw_when_budget_clips_run() {
        let (dir, mut seg) = test_segment();
        // Several batches; a mid-size budget will include some but not all.
        for off in 0..6i64 {
            seg.append(&test_batch_at(off), 0).unwrap();
        }
        // Budget that admits ~2-3 batches (each batch is small but > a few bytes).
        let raw = seg.read_raw(Offset(0), Offset(6), 80).unwrap();
        let desc = seg.read_raw_desc(Offset(0), Offset(6), 80).unwrap();
        let region = desc.region.expect("non-empty");
        assert2::assert!(desc.start_offset == raw.start_offset);
        assert2::assert!(desc.last_offset == raw.last_offset);
        assert2::assert!(region.len == raw.bytes.len());
        assert2::assert!(region_bytes(&region) == raw.bytes.to_vec());
        drop(dir);
    }
    } // sendfile_cfg!

    // ---- append_verbatim (byte-exact passthrough) tests ----

    #[test]
    fn append_verbatim_is_byte_exact_except_offset_and_epoch() {
        let (dir, mut seg) = test_segment();
        // Build a batch as a "producer" would, with its own base_offset and
        // leader epoch, then encode to verbatim wire bytes.
        let mut producer = test_batch_at(0);
        producer.base_offset = 999; // producer-supplied (to be overwritten)
        producer.partition_leader_epoch = -1; // producer-supplied
        producer.last_offset_delta = 0;
        producer.max_timestamp = 1_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        // Append verbatim with an assigned base_offset and a stamped epoch.
        let assigned_base = Offset(0);
        let stamped_epoch = 7i32;
        seg.append_verbatim(
            &wire,
            assigned_base,
            0,
            1_000,
            LeaderEpoch(stamped_epoch),
            0,
        )
        .unwrap();
        assert2::assert!(seg.last_offset() == 0);

        // Read back the raw .log bytes.
        let mut on_disk = Vec::new();
        seg.read_log_range(0, &mut on_disk, usize::MAX).unwrap();
        let mut expected_wire = wire.to_vec();
        expected_wire[0..8].copy_from_slice(&assigned_base.0.to_be_bytes());
        expected_wire[12..16].copy_from_slice(&stamped_epoch.to_be_bytes());
        assert2::assert!(on_disk == expected_wire);

        // And it decodes (CRC still valid).
        let mut cur: &[u8] = &on_disk;
        let decoded = crabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
        assert2::assert!(decoded.base_offset == assigned_base.0);
        assert2::assert!(decoded.partition_leader_epoch == stamped_epoch);
        drop(dir);
    }

    #[test]
    fn append_verbatim_updates_index_and_last_offset() {
        let (dir, mut seg) = test_segment();
        let mut producer = test_batch_at(0);
        producer.last_offset_delta = 2; // spans 3 offsets
        producer.max_timestamp = 5_000;
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();

        seg.append_verbatim(&wire, Offset(0), 2, 5_000, LeaderEpoch(0), 0)
            .unwrap();
        // Reading at offset 2 (inside the batch) returns the batch.
        let read = seg.read(Offset(2), usize::MAX).unwrap();
        assert2::assert!(seg.last_offset() == Offset(2));
        assert2::assert!(seg.max_timestamp() == 5_000);
        assert2::assert!(read == vec![producer]);
        drop(dir);
    }

    #[test]
    fn append_verbatim_to_sealed_segment_errors() {
        let (dir, mut seg) = test_segment();
        seg.seal();
        let mut wire = bytes::BytesMut::new();
        test_batch_at(0).encode(&mut wire).unwrap();
        let err = seg
            .append_verbatim(&wire.freeze(), Offset(0), 0, 0, LeaderEpoch(0), 0)
            .unwrap_err();
        assert2::assert!(matches!(err, LogError::Io(_)));
        drop(dir);
    }
}
