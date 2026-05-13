//! `Log` — a sorted collection of `Segment`s with append/read/truncate.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use crabka_protocol::records::RecordBatch;

use crate::config::LogConfig;
use crate::error::LogError;
use crate::leader_epoch_checkpoint::LeaderEpochCheckpoint;
use crate::name;
use crate::retention;
use crate::segment::Segment;
use crate::txn_index::{AbortedTxn, TxnIndex};

/// A Kafka-format log: a sorted collection of [`Segment`]s plus a single
/// active segment that accepts appends.
///
/// `Log` is single-writer (`&mut self` for mutation) and supports
/// concurrent readers (`&self` for `read`/`log_start_offset`/etc.).
/// Construct one with [`Log::open`].
#[derive(Debug)]
// `log_start_override` mirrors Kafka's `log_start_offset` terminology;
// renaming to drop the `log_` prefix would obscure the field's role.
#[allow(clippy::struct_field_names)]
pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    segments: Vec<Arc<Segment>>,
    active: Option<Segment>,
    /// Test-only override for `log_start_offset()`. When `Some(n)`, the
    /// effective `log_start` is `max(derived_from_segments, n)`. Used
    /// by the broker's replicator integration tests to simulate
    /// retention-driven truncation on a leader without physically
    /// deleting segments. Real retention is a future slice.
    #[cfg(any(test, feature = "test-helpers"))]
    log_start_override: Option<i64>,

    /// Last-Stable-Offset: the offset before the first record of any
    /// in-flight transaction. Defaults to `log_end_offset()` when no
    /// transactions are in flight.
    lso: i64,

    /// In-flight transactions: `producer_id` → first offset of this
    /// producer's currently-open txn. Cleared when a commit/abort
    /// marker for that `producer_id` is applied.
    pending: HashMap<i64, i64>,

    /// Active segment's `TxnIndex`. Reopened on segment roll.
    active_txn_index: TxnIndex,

    /// Per-partition leader-epoch checkpoint. Shared across segments —
    /// epoch history accumulates over the log's lifetime.
    epoch_checkpoint: LeaderEpochCheckpoint,
}

/// Result of [`Log::read`]: the absolute offset of the first batch
/// returned and the batches themselves.
///
/// `start_offset` falls back to the requested offset when no batches are
/// returned (e.g., reading at the log end), so callers can resume from
/// the value without special-casing emptiness.
#[derive(Debug)]
pub struct ReadOutput {
    /// Absolute offset of the first record in [`Self::batches`], or the
    /// requested offset when no batches were returned.
    pub start_offset: i64,
    /// Decoded batches in offset order. May be empty if the log has no
    /// data at or after the requested offset.
    pub batches: Vec<RecordBatch>,
}

impl Log {
    /// Open or create a `Log` at `dir`. Discovers existing segments by
    /// `.log` filename, marks all but the latest as sealed, and (if the
    /// directory is empty) creates a fresh active segment at offset 0.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let mut base_offsets: Vec<i64> = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let Ok(file_name) = entry.file_name().into_string() else {
                continue; // non-UTF-8 names: ignore (unlikely)
            };
            if let Ok(base) = name::parse_log_filename(&file_name) {
                base_offsets.push(base);
            }
        }
        base_offsets.sort_unstable();
        base_offsets.dedup();

        let mut segments: Vec<Arc<Segment>> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            if i + 1 < base_offsets.len() {
                let mut seg = Segment::open(&dir, *base)?;
                seg.seal();
                segments.push(Arc::new(seg));
            } else {
                active = Some(Segment::open_active(&dir, *base, config.validate_on_open)?);
            }
        }

        let active = match active {
            Some(s) => s,
            None => Segment::create(&dir, 0)?,
        };

        let active_txn_index = TxnIndex::open(active.txn_index_path())?;
        let epoch_checkpoint = LeaderEpochCheckpoint::open(active.leader_epoch_checkpoint_path())?;
        // LSO starts at log_end_offset(); computed before moving `active`.
        let lso = active.last_offset() + 1;

        Ok(Self {
            dir,
            config,
            segments,
            active: Some(active),
            #[cfg(any(test, feature = "test-helpers"))]
            log_start_override: None,
            lso,
            pending: HashMap::new(),
            active_txn_index,
            epoch_checkpoint,
        })
    }

    /// First absolute offset still in the log.
    #[must_use]
    pub fn log_start_offset(&self) -> i64 {
        let derived = if let Some(first) = self.segments.first() {
            first.base_offset()
        } else if let Some(active) = &self.active {
            active.base_offset()
        } else {
            0
        };
        #[cfg(any(test, feature = "test-helpers"))]
        {
            if let Some(o) = self.log_start_override {
                return derived.max(o);
            }
        }
        derived
    }

    /// Test-only: advance the effective `log_start_offset` to `new_start`.
    /// Used by the broker's replicator out-of-range integration test to
    /// simulate retention-driven truncation. Does NOT physically truncate
    /// on-disk segments — only shifts the in-memory pointer. Real
    /// retention is a future slice.
    ///
    /// `new_start` must be non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::OffsetMismatch`] if `new_start` is negative.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_set_log_start_offset(&mut self, new_start: i64) -> Result<(), LogError> {
        if new_start < 0 {
            return Err(LogError::OffsetMismatch {
                expected: 0,
                actual: new_start,
            });
        }
        self.log_start_override = Some(new_start);
        Ok(())
    }

    /// Reset the log to be empty starting at `new_base`. Drops every
    /// segment + on-disk file and creates a fresh active segment at
    /// `new_base`. Used by the replicator's `OFFSET_OUT_OF_RANGE`
    /// recovery path when the follower has fallen behind the leader's
    /// `log_start` — `truncate_to` can't help here because we need to
    /// move `log_start` *forward* past where there is no local data.
    pub fn reset_to(&mut self, new_base: i64) -> Result<(), LogError> {
        if new_base < 0 {
            return Err(LogError::OffsetMismatch {
                expected: 0,
                actual: new_base,
            });
        }

        // Drop every sealed segment + its on-disk files.
        while let Some(popped) = self.segments.pop() {
            let base = popped.base_offset();
            drop(popped);
            let _ = fs::remove_file(name::log_path(&self.dir, base));
            let _ = fs::remove_file(name::index_path(&self.dir, base));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
        }

        // Drop the active segment + its on-disk files.
        if let Some(active) = self.active.take() {
            let base = active.base_offset();
            drop(active);
            let _ = fs::remove_file(name::log_path(&self.dir, base));
            let _ = fs::remove_file(name::index_path(&self.dir, base));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
        }

        // Clear any test-only override so the derived value takes over.
        #[cfg(any(test, feature = "test-helpers"))]
        {
            self.log_start_override = None;
        }

        let new_active = Segment::create(&self.dir, new_base)?;
        self.active_txn_index = TxnIndex::open(new_active.txn_index_path())?;
        self.pending.clear(); // reset_to is a hard reset (after divergence)
        self.lso = new_active.last_offset() + 1; // = new_base (empty segment)
        self.active = Some(new_active);
        Ok(())
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> i64 {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        0
    }

    /// Last-Stable-Offset: the highest offset that consumers in
    /// `read_committed` isolation may see. Advances only when no
    /// transactions are in flight; held back at the first offset of any
    /// open (uncommitted/unaborted) transactional batch.
    #[must_use]
    pub fn lso(&self) -> i64 {
        self.lso
    }

    /// Close all segments. Drop runs automatically when `self` moves;
    /// this method just names the operation explicitly.
    pub fn close(self) {
        drop(self);
    }

    /// Return all aborted transactions from the active segment's
    /// `.txnindex` whose offset range overlaps `[start, end)`.
    ///
    /// For the slice-9 MVP only the active segment's index is consulted
    /// (older sealed segments' `.txnindex` files are not loaded into
    /// memory). The window `[fetch_offset, lso)` always falls within
    /// the active segment in practice because LSO can only advance past
    /// a committed/aborted marker, which lands in the same segment as
    /// the corresponding transactional batches.
    #[must_use]
    pub fn aborted_in_range(&self, start: i64, end: i64) -> Vec<crate::txn_index::AbortedTxn> {
        self.active_txn_index
            .aborted_in_range(start, end)
            .copied()
            .collect()
    }

    /// Append a `RecordBatch`. The batch's `base_offset` is overwritten
    /// by the log to be the next assigned offset; `last_offset_delta`
    /// determines how many absolute offsets this batch consumes.
    /// Returns the assigned `base_offset`.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError> {
        let leader_epoch = batch.partition_leader_epoch;
        let assigned_base = self.log_end_offset();
        batch.base_offset = assigned_base;
        self.append_preserving_offset(batch)?;
        // Record epoch transition when the epoch is valid and exceeds the
        // previously recorded epoch (or no epoch has been recorded yet).
        if leader_epoch >= 0
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, assigned_base)?;
        }
        Ok(assigned_base)
    }

    /// Access the per-partition leader-epoch checkpoint.
    #[must_use]
    pub fn epoch_checkpoint(&self) -> &LeaderEpochCheckpoint {
        &self.epoch_checkpoint
    }

    /// Append a `RecordBatch` whose `base_offset` is set by the caller.
    ///
    /// Unlike [`Log::append`], this does NOT overwrite `batch.base_offset`
    /// — it is used by the broker's replicator to preserve the
    /// leader-assigned offset on the follower's local log.
    ///
    /// `offset` must equal the log's current [`Log::log_end_offset`];
    /// otherwise this returns
    /// [`LogError::OffsetMismatch`]. On success, `batch.base_offset` is
    /// set to `offset` (it should already match) before the batch is
    /// written.
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: i64) -> Result<(), LogError> {
        let expected = self.log_end_offset();
        if offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: offset,
            });
        }
        batch.base_offset = offset;
        self.append_preserving_offset(batch)
    }

    /// Internal helper shared by [`Log::append`] and [`Log::append_at`].
    /// Performs segment-roll-if-needed, appends to the active segment, and
    /// honors `config.flush_on_append` — but does NOT reassign
    /// `batch.base_offset`. Callers are responsible for setting it first.
    /// Also updates LSO and the active `.txnindex` based on batch attributes.
    fn append_preserving_offset(&mut self, batch: &mut RecordBatch) -> Result<(), LogError> {
        let should_roll = match &self.active {
            Some(seg) => seg.size_bytes() >= self.config.segment_bytes,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append(batch, self.config.index_interval_bytes)?;

        if self.config.flush_on_append {
            active.flush()?;
        }

        // --- LSO tracking + .txnindex writes ---
        let pid = batch.producer_id;
        if batch.attributes.is_control_batch() {
            // Parse the inner control record: key = (version: i16, type: i16) BE.
            // type=0 → ABORT; type=1 → COMMIT.
            let marker_type = batch
                .records
                .first()
                .and_then(|r| r.key.as_deref())
                .and_then(parse_control_marker_type);
            if let Some(start) = self.pending.remove(&pid) {
                let last = batch.base_offset + i64::from(batch.last_offset_delta);
                if marker_type == Some(0)
                /* ABORT */
                {
                    self.active_txn_index.append(AbortedTxn {
                        start_offset: start,
                        last_offset: last,
                        producer_id: pid,
                    })?;
                }
            }
            // LSO can advance only when no pending txns remain.
            if self.pending.is_empty() {
                self.lso = self.log_end_offset();
            }
        } else if batch.attributes.is_transactional() && pid >= 0 {
            // Record the first offset of this txn on this partition.
            self.pending.entry(pid).or_insert(batch.base_offset);
            // LSO stays where it is until commit/abort.
        } else {
            // Non-transactional batch. LSO advances only when no in-flight txns.
            if self.pending.is_empty() {
                self.lso = self.log_end_offset();
            }
        }

        Ok(())
    }

    fn roll_active_segment(&mut self) -> Result<(), LogError> {
        let new_base = self.log_end_offset();
        let mut old = self
            .active
            .take()
            .expect("active segment must exist before rolling");
        old.seal();
        self.segments.push(Arc::new(old));
        let new_seg = Segment::create(&self.dir, new_base)?;
        self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
        self.active = Some(new_seg);
        Ok(())
    }

    /// Read batches starting at `offset`, returning up to roughly
    /// `max_bytes` of `.log` data. Walks sealed segments first, then the
    /// active segment, so reads can span segment boundaries.
    pub fn read(&self, offset: i64, max_bytes: usize) -> Result<ReadOutput, LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }
        if offset >= log_end {
            return Ok(ReadOutput {
                start_offset: log_end,
                batches: Vec::new(),
            });
        }

        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut current_offset = offset;
        let mut remaining = max_bytes;

        for seg in &self.segments {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs = seg.read(current_offset, remaining)?;
            if !bs.is_empty() {
                let consumed: usize = bs.iter().map(RecordBatch::encoded_len).sum();
                remaining = remaining.saturating_sub(consumed);
                let last = bs.last().expect("non-empty by branch");
                current_offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
                batches.extend(bs);
                if remaining == 0 {
                    break;
                }
            }
        }

        if (remaining > 0 || batches.is_empty())
            && let Some(active) = &self.active
            && current_offset <= active.last_offset()
        {
            let bs = active.read(current_offset, remaining.max(1))?;
            batches.extend(bs);
        }

        let start_offset = batches.first().map_or(offset, |b| b.base_offset);
        Ok(ReadOutput {
            start_offset,
            batches,
        })
    }

    /// Truncate the log so no records at offset `>= offset` remain. Used
    /// by replication / leader election.
    pub fn truncate_to(&mut self, offset: i64) -> Result<(), LogError> {
        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();
        if offset >= log_end {
            return Ok(()); // nothing to truncate
        }
        if offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: offset,
                log_start,
            });
        }

        // Drop sealed segments whose base_offset >= offset.
        while let Some(last_sealed) = self.segments.last() {
            if last_sealed.base_offset() >= offset {
                let popped = self.segments.pop().expect("non-empty by while-let");
                let base = popped.base_offset();
                drop(popped);
                let _ = fs::remove_file(name::log_path(&self.dir, base));
                let _ = fs::remove_file(name::index_path(&self.dir, base));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
            } else {
                break;
            }
        }

        // Drop the active segment if its base_offset >= offset.
        if let Some(active) = &self.active
            && active.base_offset() >= offset
        {
            let base = active.base_offset();
            self.active = None;
            let _ = fs::remove_file(name::log_path(&self.dir, base));
            let _ = fs::remove_file(name::index_path(&self.dir, base));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base));
        }

        // If no active segment, promote the last sealed one (if any) and
        // truncate it in place. Otherwise, create a fresh one at `offset`.
        if self.active.is_none() {
            if let Some(last) = self.segments.pop() {
                let mut seg =
                    Arc::try_unwrap(last).expect("no outstanding readers during truncate");
                let rel = u32::try_from(offset - seg.base_offset())
                    .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                self.active_txn_index = TxnIndex::open(seg.txn_index_path())?;
                self.active = Some(seg);
            } else {
                let new_seg = Segment::create(&self.dir, offset)?;
                self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
                self.active = Some(new_seg);
            }
        } else if let Some(active) = self.active.as_mut()
            && active.last_offset() >= offset
        {
            // The surviving active segment contains records at or past
            // `offset`; truncate them in place.
            let rel = u32::try_from(offset - active.base_offset())
                .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(rel)?;
            self.active_txn_index = TxnIndex::open(active.txn_index_path())?;
        }
        // After truncation, LSO can't exceed log_end_offset.
        self.lso = self.lso.min(self.log_end_offset());
        Ok(())
    }

    /// Periodic maintenance: apply time- and size-based retention to the
    /// sealed segments. The active segment is never deleted, and if every
    /// segment would otherwise be evicted we retain at least one.
    /// (Active-roll-on-age is a placeholder per the plan; skip it.)
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        let sealed_refs: Vec<&Segment> = self.segments.iter().map(AsRef::as_ref).collect();
        let active_size = self.active.as_ref().map_or(0, Segment::size_bytes);

        let time_evict = retention::time_based_evict(&sealed_refs, &self.config, now);
        let size_evict = retention::size_based_evict(&sealed_refs, active_size, &self.config);

        // Union preserving order: time first (oldest first), then size.
        let mut to_evict: Vec<i64> = time_evict;
        for base in size_evict {
            if !to_evict.contains(&base) {
                to_evict.push(base);
            }
        }

        // Guard: never drop the only remaining segment. `total_segments`
        // includes the active one.
        let total_segments = self.segments.len() + usize::from(self.active.is_some());
        if to_evict.len() >= total_segments {
            to_evict.truncate(total_segments.saturating_sub(1));
        }

        for base in to_evict {
            self.segments.retain(|s| s.base_offset() != base);
            let _ = retention::delete_segment_files(&self.dir, base);
        }
        Ok(())
    }
}

/// Parse the control-marker type from the key of the first record in a
/// control batch. The key encodes `(version: i16, type: i16)` in
/// big-endian. Returns `Some(0)` for ABORT and `Some(1)` for COMMIT.
/// Returns `None` if the key is shorter than 4 bytes.
fn parse_control_marker_type(key: &[u8]) -> Option<i16> {
    if key.len() < 4 {
        return None;
    }
    let _version = i16::from_be_bytes([key[0], key[1]]);
    Some(i16::from_be_bytes([key[2], key[3]]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader_epoch_checkpoint::EpochEntry;
    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record};
    use tempfile::tempdir;

    fn sample_batch(n: i32) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset: 0, // overwritten by Log::append
            max_timestamp: 0,
            last_offset_delta: n - 1,
            ..RecordBatch::default()
        };
        for i in 0..n {
            b.records.push(Record {
                offset_delta: i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}"))),
                ..Default::default()
            });
        }
        b
    }

    #[test]
    fn open_empty_dir_creates_first_segment() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_start_offset(), 0);
        assert_eq!(log.log_end_offset(), 0);
        log.close();
    }

    #[test]
    fn open_creates_log_file() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        drop(log);
        let log_path = dir.path().join("00000000000000000000.log");
        assert!(log_path.exists());
    }

    #[test]
    fn append_assigns_monotonic_offsets() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        assert_eq!(log.append(&mut b1).unwrap(), 0);
        assert_eq!(log.append(&mut b2).unwrap(), 3);
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn append_at_matching_offset_preserves_caller_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(3);
        // Pretend the caller (a replicator) already knows the leader's
        // assigned offset for this batch is 0.
        log.append_at(&mut b, 0).unwrap();
        assert_eq!(b.base_offset, 0);
        assert_eq!(log.log_end_offset(), 3);

        let mut b2 = sample_batch(2);
        log.append_at(&mut b2, 3).unwrap();
        assert_eq!(b2.base_offset, 3);
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn append_at_with_mismatched_offset_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        let err = log.append_at(&mut b, 7).unwrap_err();
        assert!(matches!(
            err,
            LogError::OffsetMismatch {
                expected: 0,
                actual: 7
            }
        ));
        // Failure must not advance the log.
        assert_eq!(log.log_end_offset(), 0);
    }

    #[test]
    fn append_then_read_back_in_order() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..3 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        let out = log.read(0, usize::MAX).unwrap();
        assert_eq!(out.batches.len(), 3);
        assert_eq!(out.start_offset, 0);
    }

    #[test]
    fn read_offset_too_low_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        assert!(matches!(
            log.read(-1, 1024),
            Err(LogError::OffsetTooLow { .. })
        ));
    }

    #[test]
    fn read_at_log_end_returns_empty() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let out = log.read(log.log_end_offset(), 1024).unwrap();
        assert!(out.batches.is_empty());
    }

    #[test]
    fn truncate_to_drops_later_records() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        assert_eq!(log.log_end_offset(), 5);
        log.truncate_to(3).unwrap();
        // First batch (offsets 0..=2) survives; last_offset == 2, end == 3.
        assert_eq!(log.log_end_offset(), 3);
    }

    #[test]
    fn truncate_to_log_end_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let before = log.log_end_offset();
        log.truncate_to(before + 100).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }

    #[test]
    fn open_recovers_partial_trailing_batch() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut b1 = sample_batch(3);
            let mut b2 = sample_batch(2);
            log.append(&mut b1).unwrap();
            log.append(&mut b2).unwrap();
        }
        // Append 10 bytes of garbage to the .log file.
        let log_path = dir.path().join("00000000000000000000.log");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .unwrap();
        std::io::Write::write_all(&mut f, &[0xAB; 10]).unwrap();
        f.sync_data().unwrap();
        drop(f);
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(log.log_end_offset(), 5);
    }

    #[test]
    fn tick_with_no_retention_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(2);
        let mut b2 = sample_batch(3);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        let before = log.log_end_offset();
        log.tick(SystemTime::now()).unwrap();
        assert_eq!(log.log_end_offset(), before);
    }

    #[test]
    fn tick_never_deletes_only_segment() {
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention_ms: Some(Duration::from_secs(1)),
            retention_bytes: Some(0),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut b1 = sample_batch(2);
        log.append(&mut b1).unwrap();
        // Advance "now" 30 days into the future.
        let now = SystemTime::now() + Duration::from_hours(30 * 24);
        log.tick(now).unwrap();
        assert_eq!(log.log_end_offset(), 2);
    }

    #[test]
    fn segment_rolls_when_bytes_exceeded() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_bytes: 200, // tiny so we roll fast
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..5 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        // Multiple .log files should exist now.
        let log_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        assert!(
            log_files.len() >= 2,
            "expected segment roll; got {} .log files",
            log_files.len()
        );
    }

    // ---- helpers for transactional tests ----

    /// A transactional (non-control) batch for the given pid/epoch containing `values`.
    fn transactional_batch(pid: i64, epoch: i16, values: &[&str]) -> RecordBatch {
        let last_offset_delta = i32::try_from(values.len()).unwrap() - 1;
        let mut records = Vec::new();
        for (i, v) in values.iter().enumerate() {
            records.push(Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(Bytes::from(v.to_string())),
                ..Default::default()
            });
        }
        RecordBatch {
            base_offset: 0, // overwritten by Log::append
            last_offset_delta,
            producer_id: pid,
            producer_epoch: epoch,
            attributes: Attributes::default().with_transactional(true),
            records,
            ..RecordBatch::default()
        }
    }

    /// Build a 4-byte control-marker key: (version=0: i16, `marker_type`: i16) BE.
    fn control_key(marker_type: i16) -> Bytes {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&0i16.to_be_bytes()); // version = 0
        buf[2..4].copy_from_slice(&marker_type.to_be_bytes());
        Bytes::from(buf.to_vec())
    }

    /// A commit control batch (`marker_type=1`) for the given pid/epoch.
    /// Offsets are rewritten by `Log::append`.
    fn commit_marker(pid: i64, epoch: i16) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: pid,
            producer_epoch: epoch,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(control_key(1 /* COMMIT */)),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// An abort control batch (`marker_type=0`) for the given pid/epoch.
    /// Offsets are rewritten by `Log::append`.
    fn abort_marker(pid: i64, epoch: i16) -> RecordBatch {
        RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: pid,
            producer_epoch: epoch,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(control_key(0 /* ABORT */)),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    // ---- transactional LSO / txnindex tests ----

    #[test]
    fn transactional_batch_holds_lso() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // First, a non-txn batch — LSO advances past it.
        let mut b0 = sample_batch(1);
        log.append(&mut b0).unwrap();
        assert_eq!(log.lso(), log.log_end_offset());

        // Now an in-flight txn batch — LSO stays.
        let mut b1 = transactional_batch(1000, 0, &["a", "b"]); // pid=1000 epoch=0
        let old_lso = log.lso();
        log.append(&mut b1).unwrap();
        assert_eq!(
            log.lso(),
            old_lso,
            "LSO must not advance while txn in flight"
        );

        // Commit marker — LSO catches up.
        let mut commit = commit_marker(1000, 0);
        log.append(&mut commit).unwrap();
        assert_eq!(log.lso(), log.log_end_offset());
    }

    #[test]
    fn abort_marker_writes_txnindex_entry() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap();

        let mut a = abort_marker(1000, 0);
        log.append(&mut a).unwrap();

        let idx = TxnIndex::open(dir.path().join("00000000000000000000.txnindex")).unwrap();
        let entries = idx.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].producer_id, 1000);
        // Txn batch was the first append: start_offset = 0.
        assert_eq!(entries[0].start_offset, 0);
        // last_offset = abort marker's base_offset + last_offset_delta = 3 + 0 = 3.
        // (The 3-record txn batch occupies offsets 0-2; the marker lands at offset 3.)
        assert_eq!(entries[0].last_offset, 3);
    }

    #[test]
    fn lso_held_by_remaining_producer_after_partial_commit() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();

        // Open two producers' transactions in parallel.
        let mut t1 = transactional_batch(1000, 0, &["a", "b"]);
        log.append(&mut t1).unwrap();
        let mut t2 = transactional_batch(2000, 0, &["c"]);
        log.append(&mut t2).unwrap();
        let lso_after_open = log.lso();

        // Commit producer 1000. LSO must still be held back by 2000.
        let mut c1 = commit_marker(1000, 0);
        log.append(&mut c1).unwrap();
        assert_eq!(log.lso(), lso_after_open, "LSO held by producer 2000");

        // Commit producer 2000. LSO advances to log_end_offset.
        let mut c2 = commit_marker(2000, 0);
        log.append(&mut c2).unwrap();
        assert_eq!(log.lso(), log.log_end_offset());
    }

    fn sample_batch_with_epoch(n: i32, epoch: i32) -> RecordBatch {
        let mut b = sample_batch(n);
        b.partition_leader_epoch = epoch;
        b
    }

    #[test]
    fn append_records_epoch_transition() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch_with_epoch(3, 0);
        log.append(&mut b).unwrap();
        let mut b2 = sample_batch_with_epoch(2, 1); // 2 records at epoch 1
        log.append(&mut b2).unwrap();
        assert_eq!(
            log.epoch_checkpoint().entries(),
            &[
                EpochEntry {
                    epoch: 0,
                    start_offset: 0
                },
                EpochEntry {
                    epoch: 1,
                    start_offset: 3
                }
            ]
        );
    }
}
