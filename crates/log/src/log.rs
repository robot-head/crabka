//! `Log`: a sorted collection of `Segment`s with append, read, and truncate.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use bytes::{Bytes, BytesMut};
use crabka_ids::{LeaderEpoch, Offset, ProducerId};
use crabka_protocol::records::{HEADER_LEN, RecordBatch};
use crabka_units::prelude::{ByteSize, ByteSizeExt, TimeExt as _, bytes};
use tracing::instrument;

use crate::{
    config::LogConfig,
    error::LogError,
    leader_epoch_checkpoint::LeaderEpochCheckpoint,
    name,
    producer_snapshot::{self, ProducerSnapshotEntry},
    retention,
    segment::{RawSegmentRead, Segment},
    stamp_index::{StampEntry, StampIndex},
    stamp_source::StampSource,
    txn_index::{AbortedTxn, TxnIndex},
};

/// A `usize` byte length as a quantity.
///
/// The read paths track their budget as a [`ByteSize`], but the lengths they
/// subtract come from `Bytes::len` and `FileRegion::len`, which are `usize`.
fn size_from_len(len: usize) -> ByteSize {
    ByteSize::from_bytes(u64::try_from(len).unwrap_or(u64::MAX))
}

/// The fixed v2 record-batch header, as a quantity.
///
/// The log asks every per-segment read for at least this much, so the header
/// walk that finds batch boundaries always has a header to read. This is
/// Kafka's anti-stall rule. The value comes from [`HEADER_LEN`] rather than a
/// restated constant, so the floor cannot drift from the protocol.
fn batch_header() -> ByteSize {
    size_from_len(HEADER_LEN)
}

/// A Kafka-format log: a sorted collection of [`Segment`]s plus a single
/// active segment that accepts appends.
///
/// `Log` has one writer and many concurrent readers. Mutation takes
/// `&mut self`; `read`, `log_start_offset`, and the other read methods take
/// `&self`. Construct one with [`Log::open`].
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    config: std::sync::Arc<std::sync::RwLock<LogConfig>>,
    segments: Vec<Segment>,
    active: Option<Segment>,
    dir_sync_needed: bool,
    /// Override for `log_start_offset()`. When `Some(n)`, the effective
    /// `log_start` is `max(derived_from_segments, n)`. `trim_to_offset` uses
    /// this in the active-segment case to advance the log start pointer
    /// without deletion of on-disk segments. Integration tests also use it to
    /// simulate retention-driven truncation. KIP-405's
    /// `local_log_start_offset` co-advances with this pointer, so
    /// [`Log::local_log_start_offset`] delegates here. There is a single
    /// source of truth.
    start_offset_override: Option<Offset>,

    /// Last-Stable-Offset: the offset before the first record of any
    /// in-flight transaction. Defaults to `log_end_offset()` when no
    /// transactions are in flight.
    lso: Offset,

    /// In-flight transactions: `producer_id` → first offset of this
    /// producer's currently-open txn. The log clears the entry when it
    /// applies a commit or abort marker for that `producer_id`.
    pending: HashMap<ProducerId, Offset>,

    /// Exact data-batch ranges for each in-flight transaction. A commit
    /// marker stamps only these ranges, so interleaved transactions and
    /// ordinary records never inherit another transaction's commit stamp.
    pending_stamp_ranges: HashMap<ProducerId, Vec<(Offset, Offset)>>,

    /// Last transaction-coordinator epoch observed in a durable commit or
    /// abort marker for each producer. The marker value carries this field,
    /// so log recovery can rebuild it without a separate sidecar.
    coordinator_epochs: HashMap<ProducerId, i32>,

    /// Producer sequence, epoch, and transaction metadata persisted in
    /// Kafka-compatible `.snapshot` files at segment boundaries.
    producer_state: HashMap<ProducerId, ProducerSnapshotEntry>,

    /// Active segment's `TxnIndex`. The log reopens it on segment roll.
    active_txn_index: TxnIndex,

    /// Injected source of the additional internal stamp coordinate. `None`
    /// is the default and means this partition stamps nothing. Behavior and
    /// all wire-exact bytes stay exactly as they are without a stamp.
    /// [`Log::set_stamp_source`] sets this field. The broker shares one tenant
    /// source across all hosted partitions when internal stamping is enabled.
    stamp_source: Option<std::sync::Arc<dyn StampSource>>,

    /// Open `.stampindex` sidecars keyed by segment base offset. Transactional
    /// data can commit after its segment rolls, so the log keeps sealed
    /// indexes available instead of retaining only the active one.
    stamp_indexes: BTreeMap<Offset, StampIndex>,

    /// Per-partition leader-epoch checkpoint. All segments share it, and
    /// epoch history accumulates over the log's lifetime.
    epoch_checkpoint: LeaderEpochCheckpoint,

    /// External next-offset authority used by diskless recovery. The broker
    /// sets this after it reads the committed `KRaft` frontier. Caller-supplied
    /// append-at bases must then equal
    /// `max(log_end_offset, reconciled_frontier)`.
    reconciled_frontier: Offset,
}

/// Result of [`Log::read`]: the absolute offset of the first batch
/// returned and the batches themselves.
///
/// `start_offset` falls back to the requested offset when the read returns
/// no batches, for example a read at the log end. Callers can then resume
/// from the value and need no special case for an empty result.
#[derive(Debug)]
pub struct ReadOutput {
    /// Absolute offset of the first record in [`Self::batches`], or the
    /// requested offset when no batches were returned.
    pub start_offset: Offset,
    /// Decoded batches in offset order. May be empty if the log has no
    /// data at or after the requested offset.
    pub batches: Vec<RecordBatch>,
}

/// Verbatim, decode-free output of [`Log::read_raw`].
#[derive(Debug, Clone)]
pub struct RawRead {
    /// Absolute offset of the first batch in [`Self::bytes`], or the
    /// requested offset when no bytes were returned.
    pub start_offset: Offset,
    /// Verbatim `.log` bytes: zero or more complete v2 batches. The bytes
    /// can span segment boundaries.
    pub bytes: Bytes,
    /// Length of [`Self::bytes`] in bytes.
    pub total: usize,
    /// Last offset included in [`Self::bytes`], or `None` when empty.
    pub last_offset: Option<Offset>,
}

impl RawRead {
    fn empty(off: Offset) -> Self {
        Self {
            start_offset: off,
            bytes: Bytes::new(),
            total: 0,
            last_offset: None,
        }
    }
}

#[cfg(test)]
mod sync_observer {
    use std::cell::RefCell;
    #[cfg(unix)]
    use std::path::PathBuf;

    use crabka_ids::Offset;

    #[cfg(unix)]
    thread_local! {
        static DIR_SYNCS: RefCell<Vec<PathBuf>> = const { RefCell::new(Vec::new()) };
    }

    thread_local! {
        static SEGMENT_FLUSHES: RefCell<Vec<Offset>> = const { RefCell::new(Vec::new()) };
    }

    #[cfg(unix)]
    pub(super) fn take_dir_syncs() -> Vec<PathBuf> {
        DIR_SYNCS.take()
    }

    pub(super) fn take_segment_flushes() -> Vec<Offset> {
        SEGMENT_FLUSHES.take()
    }

    #[cfg(unix)]
    pub(super) fn record_dir_sync(dir: PathBuf) {
        DIR_SYNCS.with_borrow_mut(|synced| synced.push(dir));
    }

    pub(super) fn record_segment_flush(base: Offset) {
        SEGMENT_FLUSHES.with_borrow_mut(|flushed| flushed.push(base));
    }
}

crate::sendfile_cfg! {
    /// Descriptor form of [`Log::read_raw`] for the zero-copy `sendfile` fetch
    /// path, Increments D + E.
    ///
    /// One [`crabka_protocol::records::FileRegion`] describes the records run
    /// for each contributing segment. A multi-segment fetch therefore goes
    /// out as several `sendfile` regions with **no** coalescing copy.
    /// `read_raw` instead concatenates cross-segment chunks into a fresh
    /// `BytesMut`. This type is compiled on the SENDFILE alias: Linux, Apple,
    /// and FreeBSD/DragonFly.
    #[derive(Debug, Clone)]
    pub struct RawReadDesc {
        /// Absolute offset of the first batch in the regions, or the requested
        /// offset when no bytes were returned.
        pub start_offset: Offset,
        /// One file-backed region per contributing segment, in wire order.
        pub regions: Vec<crabka_protocol::records::FileRegion>,
        /// Total byte length across all regions.
        pub total: usize,
    }

    impl RawReadDesc {
        fn empty(off: Offset) -> Self {
            Self {
                start_offset: off,
                regions: Vec::new(),
                total: 0,
            }
        }
    }
}

/// A producer batch that the log appends **verbatim**, with no decode and
/// no re-encode.
///
/// The produce zero-copy passthrough path uses this type. It carries the
/// producer's exact wire bytes plus the header fields the log needs for
/// offset assignment, LSO and transaction tracking, the leader-epoch
/// checkpoint, and the time index. The caller has already read all of those
/// fields from the batch header with a borrowed header-only decode.
///
/// The append patches only `base_offset` and `partition_leader_epoch` into a
/// writable copy of [`Self::bytes`]. Both fields sit outside the CRC region.
/// The log writes the body and the CRC byte-for-byte as the producer sent
/// them.
///
/// This type deliberately **cannot** hold a control batch, that is, a
/// transaction marker. The LSO bookkeeping for a control batch needs the
/// inner marker record, which the header-only path does not read. Such
/// batches take the owned [`Log::append`] path instead.
#[derive(Debug, Clone)]
pub struct VerbatimBatch {
    /// The producer's verbatim v2 batch bytes (CRC-validated by the caller).
    pub bytes: Bytes,
    /// `last_offset_delta` from the header: how many offsets the batch spans.
    pub last_offset_delta: i32,
    /// `max_timestamp` from the header (for `max_timestamp` + time index).
    pub max_timestamp: i64,
    /// Leader epoch to stamp into the batch (`partition_leader_epoch`).
    pub leader_epoch: LeaderEpoch,
    /// `producer_id` from the header (for LSO/transaction tracking).
    pub producer_id: ProducerId,
    /// `producer_epoch` from the header.
    pub producer_epoch: i16,
    /// `base_sequence` from the header.
    pub base_sequence: i32,
    /// `true` when the batch's attributes mark it transactional.
    pub is_transactional: bool,
}

/// A sealed segment described for tiered-storage offload (KIP-405).
///
/// It carries the on-disk file paths, the offset, timestamp, and size
/// metadata, and the leader-epoch ranges that a `RemoteLogManager` needs to
/// build remote-segment metadata. [`Log::tierable_segments`] produces these
/// values.
// No `Eq`: `size` is a `ByteSize`, which stores `f64`. The derive was unused —
// `SegmentExport` is never hashed nor used as a map key.
#[derive(Debug, Clone, PartialEq)]
pub struct SegmentExport {
    /// First absolute offset in the segment.
    pub base_offset: Offset,
    /// Last absolute offset (inclusive) in the segment.
    pub last_offset: Offset,
    /// Highest record timestamp in the segment, or `-1` when unknown
    /// (a sealed segment loaded from disk without a tail scan).
    pub max_timestamp: i64,
    /// `.log` file size.
    pub size: ByteSize,
    /// Path to the `.log` data file.
    pub log_path: PathBuf,
    /// Path to the `.index` (offset index) file.
    pub offset_index_path: PathBuf,
    /// Path to the `.timeindex` file.
    pub time_index_path: PathBuf,
    /// Path to the `.txnindex` file, present only when it exists on disk.
    pub transaction_index_path: Option<PathBuf>,
    /// Producer-state snapshot at `last_offset + 1`.
    pub producer_snapshot_path: PathBuf,
    /// Leader epochs whose coverage overlaps `[base_offset, last_offset]`,
    /// as `(epoch, start_offset)` clamped to `base_offset`, ordered by
    /// offset. May be empty when no epochs were recorded for this log.
    pub leader_epochs: Vec<(LeaderEpoch, Offset)>,
}

impl Log {
    /// Open or create a `Log` at `dir`.
    ///
    /// This method finds existing segments by `.log` filename and marks all
    /// but the latest as sealed. If the directory is empty, it creates a
    /// fresh active segment at offset 0.
    #[instrument(
        level = "info",
        skip_all,
        fields(
            dir = %dir.as_ref().display(),
            segments = tracing::field::Empty,
            log_end = tracing::field::Empty,
        ),
        err,
    )]
    // The only mutant here is the `segments.len() + 1` in the `span.record`
    // call, a tracing-span diagnostic field with no behavioral effect. The
    // sibling `seal_at(next_base - 1)` recovery arithmetic is separately pinned
    // by `reopen_seals_recovered_segments_at_next_base_minus_one`.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Heal any orphaned compaction `.swap` files before
        // we scan the directory for segments.
        crate::recovery::swap_orphan_recover(&dir)?;

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

        let mut segments: Vec<Segment> = Vec::with_capacity(base_offsets.len());
        let mut active: Option<Segment> = None;
        for (i, base) in base_offsets.iter().enumerate() {
            if i + 1 < base_offsets.len() {
                let mut seg = Segment::open(&dir, Offset(*base))?;
                // `Segment::open` is a no-scan load that leaves
                // `last_offset = base - 1`. A sealed segment's true last offset
                // is one below the next segment's base; set it so `read_raw`
                // (which skips a segment whose `last_offset() < fetch_offset`)
                // doesn't skip this recovered segment and serve a later base
                // offset — which after a restart manufactures an offset gap that
                // strands a follower fetching from a low offset.
                seg.seal_at(Offset(base_offsets[i + 1] - 1));
                segments.push(seg);
            } else {
                active = Some(Segment::open_active(
                    &dir,
                    Offset(*base),
                    config.validate_on_open,
                )?);
            }
        }

        let (active, dir_sync_needed) = match active {
            // We cannot know whether the process that created this segment
            // fsynced the parent directory before crashing. Conservatively
            // require one directory fsync on the next explicit `sync()` so a
            // diskless WAL ack never relies only on file data durability.
            Some(s) => (s, true),
            None => (Segment::create(&dir, Offset(0))?, true),
        };

        let active_txn_index = TxnIndex::open(active.txn_index_path())?;
        let mut epoch_checkpoint =
            LeaderEpochCheckpoint::open(active.leader_epoch_checkpoint_path())?;
        // LSO starts at log_end_offset(); computed before moving `active`.
        let lso = active.last_offset() + 1;
        epoch_checkpoint.truncate_from_end(lso)?;

        let config = std::sync::Arc::new(std::sync::RwLock::new(config));

        let span = tracing::Span::current();
        span.record("segments", segments.len() + 1);
        span.record("log_end", lso.0);

        let mut log = Self {
            dir,
            config,
            segments,
            active: Some(active),
            dir_sync_needed,
            start_offset_override: None,
            lso,
            pending: HashMap::new(),
            pending_stamp_ranges: HashMap::new(),
            coordinator_epochs: HashMap::new(),
            producer_state: HashMap::new(),
            active_txn_index,
            stamp_source: None,
            stamp_indexes: BTreeMap::new(),
            epoch_checkpoint,
            reconciled_frontier: Offset(0),
        };
        log.rebuild_producer_and_transaction_state()?;
        Ok(log)
    }

    /// Restore the latest valid producer snapshot, then replay the uncovered
    /// log tail. Missing boundary snapshots are created during the replay so
    /// every sealed segment can be copied to remote storage with its matching
    /// producer state.
    fn rebuild_producer_and_transaction_state(&mut self) -> Result<(), LogError> {
        self.pending.clear();
        self.pending_stamp_ranges.clear();
        self.coordinator_epochs.clear();
        self.producer_state.clear();
        let end = self.log_end_offset();
        let mut next = self.log_start_offset();
        if let Some((snapshot_offset, entries)) =
            producer_snapshot::latest_at_or_before(&self.dir, end)?
        {
            self.producer_state = entries;
            for (&producer_id, entry) in &self.producer_state {
                if let Some(first_offset) = entry.current_txn_first_offset {
                    self.pending.insert(producer_id, first_offset);
                }
                if entry.coordinator_epoch >= 0 {
                    self.coordinator_epochs
                        .insert(producer_id, entry.coordinator_epoch);
                }
            }
            next = snapshot_offset.max(next);
        }

        let boundaries: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(self.active.iter().map(Segment::base_offset))
            .filter(|boundary| *boundary > next)
            .collect();
        let mut boundary_index = 0;
        while next < end {
            let read = self.read(next, crabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery made no progress at offset {next}"
                )));
            }
            let mut advanced_to = next;
            for batch in &read.batches {
                self.apply_recovered_batch_state(batch)?;
                advanced_to = Offset(batch.base_offset + i64::from(batch.last_offset_delta) + 1);
                while boundary_index < boundaries.len() && boundaries[boundary_index] <= advanced_to
                {
                    producer_snapshot::write(
                        &self.dir,
                        boundaries[boundary_index],
                        &self.producer_state,
                    )?;
                    boundary_index += 1;
                }
            }
            if advanced_to <= next {
                return Err(LogError::Corrupt(format!(
                    "producer-state recovery did not advance past offset {next}"
                )));
            }
            next = advanced_to;
        }
        self.rebuild_pending_stamp_ranges()?;
        self.lso = self
            .pending
            .values()
            .copied()
            .min()
            .unwrap_or_else(|| self.log_end_offset());
        Ok(())
    }

    fn apply_recovered_batch_state(&mut self, batch: &RecordBatch) -> Result<(), LogError> {
        self.update_owned_producer_entry(batch)?;
        let producer_id = ProducerId(batch.producer_id);
        if batch.attributes.is_control_batch() {
            self.pending.remove(&producer_id);
            if let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
            {
                self.coordinator_epochs.insert(producer_id, epoch);
            }
        } else if batch.attributes.is_transactional() && !producer_id.is_none() {
            self.pending
                .entry(producer_id)
                .or_insert(Offset(batch.base_offset));
        }
        Ok(())
    }

    fn rebuild_pending_stamp_ranges(&mut self) -> Result<(), LogError> {
        self.pending_stamp_ranges.clear();
        let mut next = self.log_start_offset();
        let end = self.log_end_offset();
        while next < end {
            let read = self.read(next, crabka_units::mebibytes(1))?;
            if read.batches.is_empty() {
                return Err(LogError::Corrupt(format!(
                    "transaction-stamp recovery made no progress at offset {next}"
                )));
            }
            for batch in &read.batches {
                let producer_id = ProducerId(batch.producer_id);
                let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
                if batch.attributes.is_control_batch() {
                    self.pending_stamp_ranges.remove(&producer_id);
                } else if batch.attributes.is_transactional() && !producer_id.is_none() {
                    self.pending_stamp_ranges
                        .entry(producer_id)
                        .or_default()
                        .push((Offset(batch.base_offset), last));
                }
                next = last + 1;
            }
        }
        Ok(())
    }

    fn update_owned_producer_entry(&mut self, batch: &RecordBatch) -> Result<(), LogError> {
        let producer_id = ProducerId(batch.producer_id);
        if producer_id.is_none() {
            return Ok(());
        }
        if batch.attributes.is_control_batch() {
            let entry = self
                .producer_state
                .entry(producer_id)
                .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, batch.producer_epoch));
            if entry.producer_epoch != batch.producer_epoch {
                // Kafka clears the retained data-batch metadata when an end
                // marker advances the producer epoch (transaction version 2).
                entry.last_sequence = -1;
                entry.last_offset = Offset(-1);
                entry.offset_delta = 0;
            }
            entry.producer_epoch = batch.producer_epoch;
            entry.timestamp = batch.max_timestamp;
            entry.current_txn_first_offset = None;
            if let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
            {
                entry.coordinator_epoch = epoch;
            }
            return Ok(());
        }
        self.update_data_producer_entry(
            (producer_id, batch.producer_epoch),
            (batch.base_sequence, batch.last_offset_delta),
            (
                Offset(batch.base_offset),
                batch.max_timestamp,
                batch.attributes.is_transactional(),
            ),
        )
    }

    fn update_data_producer_entry(
        &mut self,
        producer: (ProducerId, i16),
        sequence: (i32, i32),
        append: (Offset, i64, bool),
    ) -> Result<(), LogError> {
        let (producer_id, producer_epoch) = producer;
        let (base_sequence, last_offset_delta) = sequence;
        let (base_offset, timestamp, is_transactional) = append;
        let Some((last_sequence, last_offset)) =
            Self::data_producer_tail(producer_id, base_sequence, last_offset_delta, base_offset)?
        else {
            return Ok(());
        };
        let entry = self
            .producer_state
            .entry(producer_id)
            .or_insert_with(|| ProducerSnapshotEntry::empty(producer_id, producer_epoch));
        entry.producer_epoch = producer_epoch;
        entry.last_sequence = last_sequence;
        entry.last_offset = last_offset;
        entry.offset_delta = last_offset_delta;
        entry.timestamp = timestamp;
        if is_transactional && entry.current_txn_first_offset.is_none() {
            entry.current_txn_first_offset = Some(base_offset);
        }
        Ok(())
    }

    fn data_producer_tail(
        producer_id: ProducerId,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: Offset,
    ) -> Result<Option<(i32, Offset)>, LogError> {
        if producer_id.is_none() || base_sequence < 0 {
            return Ok(None);
        }
        if last_offset_delta < 0 {
            return Err(LogError::InvalidArgument(format!(
                "negative producer offset delta for producer {producer_id}"
            )));
        }
        let last_sequence = base_sequence
            .checked_add(last_offset_delta)
            .ok_or_else(|| {
                LogError::InvalidArgument(format!(
                    "producer sequence overflow for producer {producer_id}"
                ))
            })?;
        let last_offset = base_offset
            .0
            .checked_add(i64::from(last_offset_delta))
            .map(Offset)
            .ok_or_else(|| {
                LogError::InvalidArgument(format!(
                    "producer offset overflow for producer {producer_id}"
                ))
            })?;
        Ok(Some((last_sequence, last_offset)))
    }

    /// Directory this log was opened against. The broker's intra-broker
    /// log-dir reassignment (KIP-113) reads this to find the current owning
    /// `log.dir` of a partition. The broker does not have to repeat the
    /// directory-layout convention.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// First absolute offset still in the log.
    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        let derived = if let Some(first) = self.segments.first() {
            first.base_offset()
        } else if let Some(active) = &self.active {
            active.base_offset()
        } else {
            Offset(0)
        };
        if let Some(o) = self.start_offset_override {
            return derived.max(o);
        }
        derived
    }

    /// Advance `log_start_offset` to `new_start`.
    ///
    /// `new_start` must be in `[current log_start, log_end]`.
    /// `trim_to_offset` uses this method for the active-segment case, and the
    /// broker's `DeleteRecords` handler uses it too. This method does NOT
    /// truncate on-disk segments. It only moves the in-memory start pointer.
    ///
    /// `new_start` must be non-negative.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `new_start` is negative.
    pub fn set_log_start_offset(&mut self, new_start: Offset) -> Result<(), LogError> {
        if new_start < 0 {
            return Err(LogError::InvalidArgument(
                "set_log_start_offset: new_start must be >= 0".into(),
            ));
        }
        self.start_offset_override = Some(new_start);
        Ok(())
    }

    /// Deprecated alias kept for existing test/feature-helpers callers.
    #[deprecated(note = "use set_log_start_offset")]
    #[cfg(any(test, feature = "test-helpers"))]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn test_set_log_start_offset(&mut self, new_start: Offset) -> Result<(), LogError> {
        self.set_log_start_offset(new_start)
    }

    /// Reset the log to be empty at `new_base`.
    ///
    /// This method drops every segment and every on-disk file, then creates
    /// a fresh active segment at `new_base`. The replicator's
    /// `OFFSET_OUT_OF_RANGE` recovery path uses it when the follower has
    /// fallen behind the leader's `log_start`. `truncate_to` cannot help
    /// there, because `log_start` must move *forward* past the point where
    /// no local data exists.
    #[instrument(level = "info", skip_all, fields(new_base = new_base.0), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn reset_to(&mut self, new_base: Offset) -> Result<(), LogError> {
        if new_base < 0 {
            return Err(LogError::OffsetMismatch {
                expected: Offset(0),
                actual: new_base,
            });
        }

        producer_snapshot::remove_all(&self.dir)?;

        // Drop every sealed segment + its on-disk files.
        while let Some(popped) = self.segments.pop() {
            let base = popped.base_offset();
            drop(popped);
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
        }

        // Drop the active segment + its on-disk files.
        if let Some(active) = self.active.take() {
            let base = active.base_offset();
            drop(active);
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
        }

        // Clear the start override so the derived value takes over.
        self.start_offset_override = None;

        let new_active = Segment::create(&self.dir, new_base)?;
        self.active_txn_index = TxnIndex::open(new_active.txn_index_path())?;
        let stamp_index_path = new_active.stamp_index_path();
        self.pending.clear(); // reset_to is a hard reset (after divergence)
        self.pending_stamp_ranges.clear();
        self.coordinator_epochs.clear();
        self.producer_state.clear();
        self.stamp_indexes.clear();
        self.lso = new_active.last_offset() + 1; // = new_base (empty segment)
        self.active = Some(new_active);
        self.dir_sync_needed = true;
        // Preserve any injected stamp source; reopen its (fresh) sidecar.
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        // The log now holds no records, so the leader-epoch cache must hold no
        // entries (Kafka's truncateFullyAndStartAt → leaderEpochCache.clearAndFlush).
        // Leaving stale entries makes a follower advertise a `last_fetched_epoch`
        // it has no record for, so the leader's KIP-320 reconciliation serves a
        // batch at a mismatched base offset and the follower loops forever on
        // append_at — a phantom ISR member that pins the high-watermark.
        self.epoch_checkpoint.clear()?;
        Ok(())
    }

    /// Next offset that `append` will assign.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        if let Some(active) = &self.active {
            return active.last_offset() + 1;
        }
        Offset(0)
    }

    /// Total `.log` size across sealed and active segments.
    ///
    /// The value comes from the segments' tracked logical size, not from a
    /// filesystem stat. It therefore shows buffered appends immediately and
    /// in the same way on every platform. On some operating systems a
    /// directory stat can lag an open, unflushed write handle.
    #[must_use]
    pub fn size(&self) -> ByteSize {
        let active = self.active.as_ref().map_or(ByteSize::ZERO, Segment::size);
        self.segments
            .iter()
            .fold(active, |total, seg| total + seg.size())
    }

    /// Last-Stable-Offset: the highest offset that consumers in
    /// `read_committed` isolation may see. Advances only when no
    /// transactions are in flight; held back at the first offset of any
    /// open (uncommitted/unaborted) transactional batch.
    #[must_use]
    pub fn lso(&self) -> Offset {
        self.lso
    }

    /// First offset of `producer_id`'s currently open transaction on this
    /// partition, or `None` when no transaction from that producer is pending.
    #[must_use]
    pub fn pending_transaction_start(&self, producer_id: ProducerId) -> Option<Offset> {
        self.pending.get(&producer_id).copied()
    }

    /// Transaction fields reported by `DescribeProducers` for `producer_id`.
    ///
    /// The coordinator epoch is the last epoch embedded in a durable end
    /// marker, or `-1` before the first marker. The start offset is present
    /// only while a transaction is open on this partition.
    #[must_use]
    pub fn producer_transaction_state(&self, producer_id: ProducerId) -> (i32, Option<Offset>) {
        (
            self.coordinator_epochs
                .get(&producer_id)
                .copied()
                .unwrap_or(-1),
            self.pending_transaction_start(producer_id),
        )
    }

    /// Producer state restored from the newest valid Kafka-compatible
    /// snapshot and the uncovered local log tail.
    #[must_use]
    pub fn producer_state_snapshot(&self) -> Vec<ProducerSnapshotEntry> {
        self.producer_state.values().copied().collect()
    }

    /// Close all segments. Drop runs automatically when `self` moves;
    /// this method just names the operation explicitly.
    pub fn close(self) {
        drop(self);
    }

    /// Atomically swap the active `LogConfig`.
    ///
    /// The next retention or roll check reads the new value. In-flight
    /// `append` calls hold the lock for very short windows and will not see
    /// a half-applied config.
    ///
    /// Callers can use this method through `&self`. The `Arc<RwLock<…>>`
    /// wrapper permits mutation of the inner value without an exclusive
    /// borrow on the `Log`.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn set_config(&self, new: LogConfig) {
        *self.config.write().unwrap() = new;
    }

    /// Snapshot the current config. This allocates a clone, which is cheap
    /// because `LogConfig` is small and `Clone`.
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn config_snapshot(&self) -> LogConfig {
        self.config.read().unwrap().clone()
    }

    /// Return all aborted transactions from the active segment's
    /// `.txnindex` whose offset range overlaps `[start, end)`.
    ///
    /// This method reads only the active segment's index. It does not load
    /// the `.txnindex` files of older sealed segments into memory. In
    /// practice the window `[fetch_offset, lso)` always falls within the
    /// active segment, because the LSO can only advance past a commit or
    /// abort marker, and that marker lands in the same segment as the
    /// related transactional batches.
    #[must_use]
    pub fn aborted_in_range(
        &self,
        start: Offset,
        end: Offset,
    ) -> Vec<crate::txn_index::AbortedTxn> {
        self.active_txn_index
            .aborted_in_range(start, end)
            .copied()
            .collect()
    }

    /// Inject the [`StampSource`] that folds the additional internal stamp
    /// coordinate into this partition.
    ///
    /// This method opens or recovers every local segment's `.stampindex`.
    /// From this point each durable non-transactional batch is stamped at
    /// append, while transactional batches are stamped together when their
    /// COMMIT marker lands. Aborted and still-open transactions stay
    /// unstamped.
    ///
    /// This is the only switch that enables the feature. With no source
    /// injected the log stamps nothing and its bytes are identical to those
    /// of an unstamped log. It changes no wire-facing state: not the `.log`
    /// bytes, not offset assignment, not the LSO, and not the
    /// high-watermark.
    ///
    /// # Errors
    /// Returns an error when opening the `.stampindex` sidecar fails.
    /// # Panics
    /// Panics only if there is no active segment, which cannot happen after
    /// [`Log::open`].
    pub fn set_stamp_source(
        &mut self,
        source: std::sync::Arc<dyn StampSource>,
    ) -> Result<(), LogError> {
        let mut indexes = self
            .segments
            .iter()
            .chain(self.active.iter())
            .map(|segment| {
                Ok((
                    segment.base_offset(),
                    StampIndex::open(segment.stamp_index_path())?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LogError>>()?;
        let pending_ranges: Vec<(Offset, Offset)> = self
            .pending_stamp_ranges
            .values()
            .flatten()
            .copied()
            .collect();
        for index in indexes.values_mut() {
            index.remove_ranges(&pending_ranges)?;
        }
        if let Some(horizon) = indexes
            .values()
            .flat_map(StampIndex::entries)
            .map(|entry| entry.stamp)
            .max()
        {
            source.observe(horizon);
        }
        self.stamp_indexes = indexes;
        self.stamp_source = Some(source);
        Ok(())
    }

    /// Clone the currently installed internal stamp source.
    ///
    /// Log-directory moves use this to preserve stamping when they close and
    /// reopen a partition around the final filesystem swap.
    #[must_use]
    pub fn stamp_source(&self) -> Option<std::sync::Arc<dyn StampSource>> {
        self.stamp_source.as_ref().map(std::sync::Arc::clone)
    }

    /// The internal stamp that covers `offset`, or `None` when there is no
    /// such stamp.
    ///
    /// The result is `None` when the partition is unstamped, that is, when no
    /// source is injected. It is also `None` when no stamped range covers
    /// `offset`. This method searches all local segment indexes because a
    /// transaction can commit after the segment holding its data has rolled.
    /// This is an internal, server-side query. No produce or fetch handler
    /// calls it, so the stamp can never reach a client-facing response.
    #[must_use]
    pub fn stamp_for_offset(&self, offset: Offset) -> Option<u64> {
        self.stamp_indexes
            .values()
            .find_map(|index| index.stamp_for_offset(offset))
    }

    /// Record a stamped `[base, last]` range for a durably-appended data
    /// batch.
    ///
    /// This method does nothing when no [`StampSource`] is injected.
    fn record_stamp(&mut self, base: Offset, last: Offset) -> Result<(), LogError> {
        let Some(source) = self.stamp_source.as_ref() else {
            return Ok(());
        };
        self.record_stamp_value(base, last, source.next_stamp())
    }

    /// Record a supplied commit stamp for one transactional data range.
    fn record_stamp_value(
        &mut self,
        base: Offset,
        last: Offset,
        stamp: u64,
    ) -> Result<(), LogError> {
        if self.stamp_source.is_none() {
            return Ok(());
        }
        // Retention can remove an old transactional batch before its marker
        // arrives. There is no local data left to stamp in that case, and the
        // marker must still be allowed to close the transaction.
        if last < self.local_log_start_offset() {
            return Ok(());
        }
        let (_, index) = self
            .stamp_indexes
            .range_mut(..=base)
            .next_back()
            .ok_or_else(|| {
                LogError::Corrupt(format!(
                    "no stamp index segment covers transactional range {base}..={last}"
                ))
            })?;
        index.upsert(StampEntry {
            base_offset: base,
            last_offset: last,
            stamp,
        })
    }

    /// Reopen the active `.stampindex` for a new active segment.
    ///
    /// This mirrors the `active_txn_index` reopen on roll, truncate, and
    /// reset. When no source is injected, this method clears the index, which
    /// is already absent, and does no I/O.
    ///
    /// # Errors
    /// Returns an error when opening the `.stampindex` sidecar fails.
    fn reopen_active_stamp_index(
        &mut self,
        base_offset: Offset,
        path: PathBuf,
    ) -> Result<(), LogError> {
        if self.stamp_source.is_some() {
            self.stamp_indexes
                .insert(base_offset, StampIndex::open(path)?);
        }
        Ok(())
    }

    /// Append a `RecordBatch` and return the assigned `base_offset`.
    ///
    /// The log overwrites the batch's `base_offset` with the next assigned
    /// offset. `last_offset_delta` sets how many absolute offsets this batch
    /// consumes.
    #[instrument(
        level = "debug",
        skip_all,
        fields(assigned_base = tracing::field::Empty, leader_epoch = batch.partition_leader_epoch),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, batch: &mut RecordBatch) -> Result<Offset, LogError> {
        // `partition_leader_epoch` is the raw KIP-320 wire `int32`; wrap it into
        // the domain newtype at this boundary.
        let leader_epoch = LeaderEpoch(batch.partition_leader_epoch);
        let assigned_base = self.log_end_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        batch.base_offset = assigned_base.0;
        self.append_preserving_offset(batch, None)?;
        // Record epoch transition when the epoch is valid and exceeds the
        // previously recorded epoch (or no epoch has been recorded yet).
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, assigned_base)?;
        }
        Ok(assigned_base)
    }

    /// Append a commit-marker batch with a coordinator-supplied transaction
    /// stamp and return its assigned base offset.
    ///
    /// The stamp is internal metadata. It is not encoded into the marker or
    /// any client-facing bytes. The marker must be a COMMIT control batch and
    /// a [`StampSource`] must already be installed.
    ///
    /// # Errors
    /// Returns an error for a non-commit batch, a missing stamp source, or any
    /// log/index I/O failure.
    pub fn append_with_commit_stamp(
        &mut self,
        batch: &mut RecordBatch,
        stamp: u64,
    ) -> Result<Offset, LogError> {
        self.validate_commit_stamp_batch(batch)?;
        let assigned_base = self.log_end_offset();
        batch.base_offset = assigned_base.0;
        self.observe_stamp(stamp);
        self.append_preserving_offset(batch, Some(stamp))?;
        Ok(assigned_base)
    }

    /// Append a producer batch **verbatim** and return the assigned
    /// `base_offset`.
    ///
    /// The log does not decode or re-encode the batch, and it takes
    /// `base_offset` from the log's current end. This is the produce
    /// zero-copy passthrough path. The caller has already CRC-validated the
    /// bytes and read the header fields into [`VerbatimBatch`]. The log
    /// patches `base_offset` and `partition_leader_epoch`, which both sit
    /// outside the CRC region, then writes the bytes as they are. Offset
    /// assignment, segment roll, flush, LSO and transaction tracking, and the
    /// leader-epoch checkpoint behave exactly as in [`Log::append`]. The
    /// verbatim path and the owned path differ only in how the log gets the
    /// batch bytes, not in any log-level invariant.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            assigned_base = tracing::field::Empty,
            leader_epoch = batch.leader_epoch.0,
            bytes = batch.bytes.len(),
        ),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_verbatim(&mut self, batch: &VerbatimBatch) -> Result<Offset, LogError> {
        let leader_epoch = batch.leader_epoch;
        let assigned_base = self.log_end_offset();
        tracing::Span::current().record("assigned_base", assigned_base.0);
        self.append_verbatim_preserving_offset(batch, assigned_base)?;
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, assigned_base)?;
        }
        Ok(assigned_base)
    }

    /// Append a producer batch **verbatim** at a caller-supplied base offset.
    ///
    /// `base_offset` must equal the log's current [`Log::log_end_offset`]. If
    /// it does not, this method returns [`LogError::OffsetMismatch`] and
    /// appends nothing. On success the log stamps the stored batch with
    /// `base_offset` and the batch's leader epoch. It does not decode or
    /// re-encode the CRC-covered bytes.
    ///
    /// # Errors
    /// Returns [`LogError::OffsetMismatch`] when `base_offset` is not the log
    /// end offset. It also propagates segment and checkpoint I/O errors and
    /// validation errors.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            supplied_base = base_offset.0,
            leader_epoch = batch.leader_epoch.0,
            bytes = batch.bytes.len(),
        ),
        err,
    )]
    pub fn append_verbatim_at(
        &mut self,
        batch: &VerbatimBatch,
        base_offset: Offset,
    ) -> Result<Offset, LogError> {
        let expected = self.append_at_expected_offset();
        if base_offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: base_offset,
            });
        }

        let leader_epoch = batch.leader_epoch;
        self.append_verbatim_preserving_offset(batch, base_offset)?;
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, base_offset)?;
        }
        Ok(base_offset)
    }

    /// Flush and `fsync` the active segment to stable storage, independent of
    /// [`LogConfig::flush_on_append`].
    ///
    /// This method also fsyncs the log directory after it creates a new
    /// segment file. The segment therefore stays reachable after a crash on
    /// filesystems that require a parent-directory fsync.
    ///
    /// # Errors
    /// Returns a [`LogError`] if the underlying segment or directory flush fails.
    pub fn sync(&mut self) -> Result<(), LogError> {
        for segment in &mut self.segments {
            Self::segment_flush(segment)?;
        }
        self.active_segment_flush()?;
        if self.dir_sync_needed {
            // Rust's standard directory-open path is supported on Unix, where
            // syncing the parent makes newly-created segment names durable. On
            // Windows the platform provides no equivalent through `std`; the
            // segment, offset-index, and time-index handles above have still
            // been flushed with `sync_data`.
            #[cfg(unix)]
            Self::sync_log_dir(&self.dir)?;
            self.dir_sync_needed = false;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn sync_log_dir(dir: &Path) -> Result<(), LogError> {
        let log_dir = fs::File::open(dir)?;
        log_dir.sync_all()?;
        #[cfg(test)]
        sync_observer::record_dir_sync(dir.to_path_buf());
        Ok(())
    }

    fn active_segment_flush(&mut self) -> Result<(), LogError> {
        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        Self::segment_flush(active)
    }

    fn segment_flush(segment: &mut Segment) -> Result<(), LogError> {
        #[cfg(test)]
        sync_observer::record_segment_flush(segment.base_offset());
        segment.flush()
    }

    /// Verbatim counterpart of [`Log::append_preserving_offset`].
    ///
    /// This function rolls the segment if necessary, appends the verbatim
    /// bytes to the active segment, honors `flush_on_append`, and updates the
    /// LSO from the batch's transactional and producer metadata. It mirrors
    /// the non-control branches of the owned path. Control batches never
    /// reach here, because they take the owned path.
    fn append_verbatim_preserving_offset(
        &mut self,
        batch: &VerbatimBatch,
        base_offset: Offset,
    ) -> Result<(), LogError> {
        Self::data_producer_tail(
            batch.producer_id,
            batch.base_sequence,
            batch.last_offset_delta,
            base_offset,
        )?;
        let (segment_size, index_interval, flush_on_append) = {
            let cfg = self.config.read().unwrap();
            (cfg.segment_size, cfg.index_interval, cfg.flush_on_append)
        };

        let should_roll = match &self.active {
            Some(seg) => seg.size() >= segment_size,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append_verbatim(
            &batch.bytes,
            base_offset,
            batch.last_offset_delta,
            batch.max_timestamp,
            batch.leader_epoch,
            index_interval,
        )?;

        if flush_on_append {
            self.active_segment_flush()?;
        }

        // --- .stampindex write (internal sidecar) ---
        // Ordinary verbatim data is stamped now. Transactional verbatim data
        // is retained below and stamped only if its commit marker lands.
        let last_offset = Offset(base_offset.0 + i64::from(batch.last_offset_delta));
        if !batch.is_transactional {
            self.record_stamp(base_offset, last_offset)?;
        }

        // --- LSO tracking (no control batches on this path) ---
        let pid = batch.producer_id;
        if batch.is_transactional && !pid.is_none() {
            // Record the first offset of this txn on this partition; LSO
            // stays put until a commit/abort marker (which arrives via the
            // owned control-batch path).
            self.pending.entry(pid).or_insert(base_offset);
            self.pending_stamp_ranges
                .entry(pid)
                .or_default()
                .push((base_offset, last_offset));
        } else if self.pending.is_empty() {
            // Non-transactional batch with no in-flight txns: LSO advances.
            self.lso = self.log_end_offset();
        }

        self.update_data_producer_entry(
            (batch.producer_id, batch.producer_epoch),
            (batch.base_sequence, batch.last_offset_delta),
            (base_offset, batch.max_timestamp, batch.is_transactional),
        )?;

        Ok(())
    }

    /// Access the per-partition leader-epoch checkpoint.
    #[must_use]
    pub fn epoch_checkpoint(&self) -> &LeaderEpochCheckpoint {
        &self.epoch_checkpoint
    }

    /// Reconcile append-at offset assignment to an external next-offset frontier.
    ///
    /// Diskless partitions use the `KRaft` metadata log as the offset authority.
    /// After a crash, `KRaft` may have committed a next-offset that is ahead of the
    /// recovered local WAL tail. In that case the gap is intentional: the caller
    /// sets this frontier and the next append-at must use it instead of the local
    /// LEO. Classic logs never call this method and keep the default frontier 0.
    pub fn reconcile_next_offset(&mut self, frontier: Offset) {
        self.reconciled_frontier = self.reconciled_frontier.max(frontier);
    }

    fn append_at_expected_offset(&self) -> Offset {
        self.log_end_offset().max(self.reconciled_frontier)
    }

    /// Append a `RecordBatch` whose `base_offset` the caller sets.
    ///
    /// Unlike [`Log::append`], this method does NOT overwrite
    /// `batch.base_offset`. The broker's replicator uses it to keep the
    /// leader-assigned offset on the follower's local log.
    ///
    /// `offset` must equal the log's current [`Log::log_end_offset`]. If it
    /// does not, this method returns [`LogError::OffsetMismatch`]. On success
    /// the log sets `batch.base_offset` to `offset`, which should already
    /// match, before it writes the batch.
    #[instrument(
        level = "debug",
        skip(self, batch),
        fields(leader_epoch = batch.partition_leader_epoch),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append_at(&mut self, batch: &mut RecordBatch, offset: Offset) -> Result<(), LogError> {
        let expected = self.append_at_expected_offset();
        if offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: offset,
            });
        }
        // `partition_leader_epoch` is the raw KIP-320 wire `int32`; wrap it here.
        let leader_epoch = LeaderEpoch(batch.partition_leader_epoch);
        batch.base_offset = offset.0;
        self.append_preserving_offset(batch, None)?;
        // Mirror the leader-side epoch bookkeeping in [`Log::append`]: record the
        // batch's leader epoch when it advances past the latest recorded epoch,
        // so a follower's leader-epoch checkpoint tracks replicated epochs.
        if leader_epoch.is_known()
            && self
                .epoch_checkpoint
                .latest_epoch()
                .is_none_or(|e| leader_epoch > e)
        {
            self.epoch_checkpoint.append(leader_epoch, offset)?;
        }
        Ok(())
    }

    /// Append a replicated commit marker at `offset` with its internal commit
    /// stamp. This is the follower counterpart of
    /// [`Log::append_with_commit_stamp`].
    ///
    /// # Errors
    /// Returns an error for an offset mismatch, a non-commit batch, a missing
    /// stamp source, or any log/index I/O failure.
    pub fn append_at_with_commit_stamp(
        &mut self,
        batch: &mut RecordBatch,
        offset: Offset,
        stamp: u64,
    ) -> Result<(), LogError> {
        let expected = self.append_at_expected_offset();
        if offset != expected {
            return Err(LogError::OffsetMismatch {
                expected,
                actual: offset,
            });
        }
        self.validate_commit_stamp_batch(batch)?;
        batch.base_offset = offset.0;
        self.observe_stamp(stamp);
        self.append_preserving_offset(batch, Some(stamp))
    }

    /// Internal helper shared by [`Log::append`] and [`Log::append_at`].
    ///
    /// This function rolls the segment if necessary, appends to the active
    /// segment, and honors `config.flush_on_append`. It does NOT reassign
    /// `batch.base_offset`. Callers must set that field first. It also
    /// updates the LSO and the active `.txnindex` from the batch attributes.
    fn append_preserving_offset(
        &mut self,
        batch: &mut RecordBatch,
        transaction_stamp: Option<u64>,
    ) -> Result<(), LogError> {
        if !batch.attributes.is_control_batch() {
            Self::data_producer_tail(
                ProducerId(batch.producer_id),
                batch.base_sequence,
                batch.last_offset_delta,
                Offset(batch.base_offset),
            )?;
        }
        let (segment_size, index_interval, flush_on_append) = {
            let cfg = self.config.read().unwrap();
            (cfg.segment_size, cfg.index_interval, cfg.flush_on_append)
        };

        let should_roll = match &self.active {
            Some(seg) => seg.size() >= segment_size,
            None => false,
        };
        if should_roll {
            self.roll_active_segment()?;
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment must exist after Log::open");
        active.append(batch, index_interval)?;

        if flush_on_append {
            self.active_segment_flush()?;
        }

        // --- .stampindex write (internal sidecar) ---
        // Ordinary data is stamped after the batch is durable. Transactional
        // data stays unstamped until its COMMIT marker lands; an ABORT leaves
        // it unstamped. Control records themselves never receive a stamp.
        if !batch.attributes.is_control_batch() && !batch.attributes.is_transactional() {
            let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            self.record_stamp(Offset(batch.base_offset), last)?;
        }

        // --- LSO tracking + .txnindex writes ---
        let pid = ProducerId(batch.producer_id);
        if batch.attributes.is_control_batch() {
            // Parse the inner control record: key = (version: i16, type: i16) BE.
            // type=0 → ABORT; type=1 → COMMIT.
            let marker_type = batch
                .records
                .first()
                .and_then(|r| r.key.as_deref())
                .and_then(parse_control_marker_type);
            if let Some(epoch) = batch
                .records
                .first()
                .and_then(|record| record.value.as_deref())
                .and_then(parse_control_marker_coordinator_epoch)
            {
                self.coordinator_epochs.insert(pid, epoch);
            }
            if let Some(start) = self.pending.get(&pid).copied() {
                let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
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
            let stamp_ranges = self
                .pending_stamp_ranges
                .get(&pid)
                .cloned()
                .unwrap_or_default();
            if marker_type == Some(1) && !stamp_ranges.is_empty() {
                let stamp = transaction_stamp
                    .or_else(|| self.stamp_source.as_ref().map(|source| source.next_stamp()));
                if let Some(stamp) = stamp {
                    for (base, last) in stamp_ranges {
                        self.record_stamp_value(base, last, stamp)?;
                    }
                }
            }
            // Keep the in-memory transaction state until all durable sidecar
            // writes succeed. A caller can then retry a marker whose log
            // append succeeded but whose index update failed.
            self.pending.remove(&pid);
            self.pending_stamp_ranges.remove(&pid);
            // LSO can advance only when no pending txns remain.
            if self.pending.is_empty() {
                self.lso = self.log_end_offset();
            }
        } else if batch.attributes.is_transactional() && !pid.is_none() {
            // Record the first offset of this txn on this partition.
            let base = Offset(batch.base_offset);
            let last = Offset(batch.base_offset + i64::from(batch.last_offset_delta));
            self.pending.entry(pid).or_insert(base);
            self.pending_stamp_ranges
                .entry(pid)
                .or_default()
                .push((base, last));
            // LSO stays where it is until commit/abort.
        } else {
            // Non-transactional batch. LSO advances only when no in-flight txns.
            if self.pending.is_empty() {
                self.lso = self.log_end_offset();
            }
        }

        self.update_owned_producer_entry(batch)?;

        Ok(())
    }

    fn validate_commit_stamp_batch(&self, batch: &RecordBatch) -> Result<(), LogError> {
        let is_commit = batch.attributes.is_control_batch()
            && batch
                .records
                .first()
                .and_then(|record| record.key.as_deref())
                .and_then(parse_control_marker_type)
                == Some(1);
        if !is_commit {
            return Err(LogError::InvalidArgument(
                "an explicit transaction stamp requires a COMMIT control batch".into(),
            ));
        }
        if self.stamp_source.is_none() {
            return Err(LogError::InvalidArgument(
                "an explicit transaction stamp requires an installed stamp source".into(),
            ));
        }
        Ok(())
    }

    fn observe_stamp(&self, stamp: u64) {
        if let Some(source) = &self.stamp_source {
            source.observe(stamp);
        }
    }

    #[instrument(
        level = "info",
        skip_all,
        fields(new_base = tracing::field::Empty),
        err,
    )]
    fn roll_active_segment(&mut self) -> Result<(), LogError> {
        let new_base = self.log_end_offset();
        tracing::Span::current().record("new_base", new_base.0);
        // The snapshot must never become durable ahead of the records it
        // describes. Flush the segment first, then fsync and publish the
        // boundary snapshot.
        self.active_segment_flush()?;
        producer_snapshot::write(&self.dir, new_base, &self.producer_state)?;
        let mut old = self
            .active
            .take()
            .expect("active segment must exist before rolling");
        old.seal();
        self.segments.push(old);
        let new_seg = Segment::create(&self.dir, new_base)?;
        self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
        let stamp_index_path = new_seg.stamp_index_path();
        self.active = Some(new_seg);
        self.dir_sync_needed = true;
        self.reopen_active_stamp_index(new_base, stamp_index_path)?;
        Ok(())
    }

    /// Read batches from `offset` and return up to about `max_size` of
    /// `.log` data.
    ///
    /// The read walks sealed segments first, then the active segment, so a
    /// read can span segment boundaries.
    #[instrument(
        level = "debug",
        skip(self),
        fields(batches = tracing::field::Empty),
        err,
    )]
    // The `current_offset = base + last_offset_delta + 1` cursor advance only
    // ever moves the cursor too LOW under these mutations; each segment's
    // `read` self-filters via `batch_last >= offset` and clamps sub-base
    // offsets, so a too-low cursor yields the same batches and `start_offset`
    // (taken from `batches.first()`). No distinguishing input exists.
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read(&self, offset: Offset, max_size: ByteSize) -> Result<ReadOutput, LogError> {
        let read_buffer_cap = self.config.read().unwrap().read_buffer_cap;
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
        let mut remaining = max_size;

        for seg in &self.segments {
            if seg.last_offset() < current_offset {
                continue;
            }
            let bs = seg.read_with_buffer_cap(current_offset, remaining, read_buffer_cap)?;
            if !bs.is_empty() {
                let consumed: usize = bs.iter().map(RecordBatch::encoded_len).sum();
                remaining = (remaining - size_from_len(consumed)).max(ByteSize::ZERO);
                let last = bs.last().expect("non-empty by branch");
                current_offset = Offset(last.base_offset + i64::from(last.last_offset_delta) + 1);
                batches.extend(bs);
                if remaining == ByteSize::ZERO {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || batches.is_empty())
            && let Some(active) = &self.active
            && current_offset <= active.last_offset()
        {
            let bs = active.read_with_buffer_cap(
                current_offset,
                remaining.max(bytes(1)),
                read_buffer_cap,
            )?;
            batches.extend(bs);
        }

        let start_offset = batches.first().map_or(offset, |b| Offset(b.base_offset));
        tracing::Span::current().record("batches", batches.len());
        Ok(ReadOutput {
            start_offset,
            batches,
        })
    }

    /// Like [`Log::read`], but returns verbatim wire bytes with no decode.
    ///
    /// The read walks sealed segments and then the active segment. It
    /// includes only batches with `base_offset < limit_offset`, up to about
    /// `max_size`, and always at least one batch.
    #[instrument(
        level = "debug",
        skip(self),
        fields(total = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn read_raw(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawRead, LogError> {
        let read_buffer_cap = self.config.read().unwrap().read_buffer_cap;
        let log_start = self.log_start_offset();
        if fetch_offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: fetch_offset,
                log_start,
            });
        }
        if fetch_offset >= limit_offset {
            return Ok(RawRead::empty(fetch_offset));
        }

        let mut chunks: Vec<Bytes> = Vec::new();
        let mut start_offset = fetch_offset;
        let mut current = fetch_offset;
        let mut remaining = max_size;
        let mut got_first = false;
        let mut last_offset = None;

        for seg in &self.segments {
            if seg.last_offset() < current {
                continue;
            }
            let r: RawSegmentRead = seg.read_raw_with_buffer_cap(
                current,
                limit_offset,
                remaining.max(batch_header()),
                read_buffer_cap,
            )?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                    got_first = true;
                }
                remaining = (remaining - size_from_len(r.bytes.len())).max(ByteSize::ZERO);
                current = r.last_offset + 1;
                last_offset = Some(r.last_offset);
                chunks.push(r.bytes);
                if remaining == ByteSize::ZERO || current >= limit_offset {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || !got_first)
            && current < limit_offset
            && let Some(active) = &self.active
            && current <= active.last_offset()
        {
            let r = active.read_raw_with_buffer_cap(
                current,
                limit_offset,
                remaining.max(batch_header()),
                read_buffer_cap,
            )?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                }
                chunks.push(r.bytes);
                last_offset = Some(r.last_offset);
            }
        }

        let bytes = match chunks.len() {
            0 => Bytes::new(),
            1 => chunks.pop().expect("len==1"),
            _ => {
                let total: usize = chunks.iter().map(Bytes::len).sum();
                let mut b = BytesMut::with_capacity(total);
                for c in &chunks {
                    b.extend_from_slice(c);
                }
                b.freeze()
            }
        };
        let total = bytes.len();
        tracing::Span::current().record("total", total);
        Ok(RawRead {
            start_offset,
            bytes,
            total,
            last_offset,
        })
    }

    crate::sendfile_cfg! {
    /// Descriptor variant of [`Log::read_raw`] for the zero-copy `sendfile`
    /// fetch path.
    ///
    /// This method walks sealed segments and then the active segment exactly
    /// as `read_raw` does. It collects one [`crabka_protocol::records::FileRegion`] for each contributing segment
    /// through [`Segment::read_raw_desc`], instead of owned `Bytes`.
    /// Multi-segment fetches are **not** coalesced. Each region goes out in
    /// its own `sendfile` call, so the cross-segment copy disappears.
    ///
    /// The selected byte ranges are byte-identical to what `read_raw` would
    /// have returned for the same `(fetch_offset, limit_offset, max_size)`.
    #[instrument(
        level = "debug",
        skip(self),
        fields(regions = tracing::field::Empty, total = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn read_raw_desc(
        &self,
        fetch_offset: Offset,
        limit_offset: Offset,
        max_size: ByteSize,
    ) -> Result<RawReadDesc, LogError> {
        let log_start = self.log_start_offset();
        if fetch_offset < log_start {
            return Err(LogError::OffsetTooLow {
                requested: fetch_offset,
                log_start,
            });
        }
        if fetch_offset >= limit_offset {
            return Ok(RawReadDesc::empty(fetch_offset));
        }

        let mut regions: Vec<crabka_protocol::records::FileRegion> = Vec::new();
        let mut start_offset = fetch_offset;
        let mut current = fetch_offset;
        let mut remaining = max_size;
        let mut got_first = false;

        for seg in &self.segments {
            if seg.last_offset() < current {
                continue;
            }
            let r = seg.read_raw_desc(current, limit_offset, remaining.max(batch_header()))?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                    got_first = true;
                }
                remaining = (remaining - size_from_len(r.len())).max(ByteSize::ZERO);
                current = r.last_offset + 1;
                if let Some(region) = r.region {
                    regions.push(region);
                }
                if remaining == ByteSize::ZERO || current >= limit_offset {
                    break;
                }
            }
        }

        if (remaining > ByteSize::ZERO || !got_first)
            && current < limit_offset
            && let Some(active) = &self.active
            && current <= active.last_offset()
        {
            let r = active.read_raw_desc(current, limit_offset, remaining.max(batch_header()))?;
            if !r.is_empty() {
                if !got_first {
                    start_offset = r.start_offset;
                }
                if let Some(region) = r.region {
                    regions.push(region);
                }
            }
        }

        let total: usize = regions.iter().map(|r| r.len).sum();
        let span = tracing::Span::current();
        span.record("regions", regions.len());
        span.record("total", total);
        Ok(RawReadDesc {
            start_offset,
            regions,
            total,
        })
    }
    }

    /// Truncate the log so that no record at offset `>= offset` remains.
    /// Replication and leader election use this method.
    #[instrument(level = "info", skip(self), err)]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn truncate_to(&mut self, offset: Offset) -> Result<(), LogError> {
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

        producer_snapshot::remove_after(&self.dir, offset)?;

        // Drop sealed segments whose base_offset >= offset.
        while let Some(last_sealed) = self.segments.last() {
            if last_sealed.base_offset() >= offset {
                let popped = self.segments.pop().expect("non-empty by while-let");
                let base = popped.base_offset();
                drop(popped);
                let _ = fs::remove_file(name::log_path(&self.dir, base.0));
                let _ = fs::remove_file(name::index_path(&self.dir, base.0));
                let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
                let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
                let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
                self.stamp_indexes.remove(&base);
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
            let _ = fs::remove_file(name::log_path(&self.dir, base.0));
            let _ = fs::remove_file(name::index_path(&self.dir, base.0));
            let _ = fs::remove_file(name::timeindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::txnindex_path(&self.dir, base.0));
            let _ = fs::remove_file(name::stampindex_path(&self.dir, base.0));
            self.stamp_indexes.remove(&base);
        }

        // If no active segment, promote the last sealed one (if any) and
        // truncate it in place. Otherwise, create a fresh one at `offset`.
        if self.active.is_none() {
            if let Some(mut seg) = self.segments.pop() {
                let rel = u32::try_from(offset.0 - seg.base_offset().0)
                    .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
                seg.truncate_to_relative(rel)?;
                self.active_txn_index = TxnIndex::open(seg.txn_index_path())?;
                let base = seg.base_offset();
                let stamp_index_path = seg.stamp_index_path();
                self.active = Some(seg);
                self.stamp_indexes
                    .retain(|segment_base, _| *segment_base <= base);
                self.reopen_active_stamp_index(base, stamp_index_path)?;
                if let Some(index) = self.stamp_indexes.get_mut(&base) {
                    index.truncate_from(offset)?;
                }
            } else {
                let new_seg = Segment::create(&self.dir, offset)?;
                self.active_txn_index = TxnIndex::open(new_seg.txn_index_path())?;
                let stamp_index_path = new_seg.stamp_index_path();
                self.active = Some(new_seg);
                self.dir_sync_needed = true;
                self.stamp_indexes.clear();
                self.reopen_active_stamp_index(offset, stamp_index_path)?;
            }
        } else if let Some(active) = self.active.as_mut()
            && active.last_offset() >= offset
        {
            // The surviving active segment contains records at or past
            // `offset`; truncate them in place.
            let rel = u32::try_from(offset.0 - active.base_offset().0)
                .map_err(|_| LogError::BadSegmentName("offset overflow".into()))?;
            active.truncate_to_relative(rel)?;
            self.active_txn_index = TxnIndex::open(active.txn_index_path())?;
            let base = active.base_offset();
            let stamp_index_path = active.stamp_index_path();
            self.reopen_active_stamp_index(base, stamp_index_path)?;
            if let Some(index) = self.stamp_indexes.get_mut(&base) {
                index.truncate_from(offset)?;
            }
        }
        self.rebuild_producer_and_transaction_state()?;
        // After truncation, LSO can't exceed log_end_offset.
        self.lso = self.lso.min(self.log_end_offset());
        // Drop leader-epoch checkpoint entries for the truncated-away tail so
        // latest_epoch()/end_offset_for_epoch() don't report epochs that no
        // longer have records (mirrors Kafka's truncateFromEnd).
        self.epoch_checkpoint
            .truncate_from_end(self.log_end_offset())?;
        Ok(())
    }

    /// Trim from the start of the log and return the resulting
    /// `log_start_offset`.
    ///
    /// This method drops every sealed segment whose last offset is
    /// `< target`. It advances `log_start_offset` when `target` falls inside
    /// the active segment. It never deletes the active segment.
    ///
    /// `target` is clamped to `[0, log_end_offset()]`. A caller that asks for
    /// a trim past the LEO gets a trim to the LEO.
    ///
    /// # Errors
    ///
    /// Returns `LogError::InvalidArgument` if `target < 0`.
    #[instrument(
        level = "info",
        skip(self),
        fields(new_log_start = tracing::field::Empty),
        err,
    )]
    pub fn trim_to_offset(&mut self, target: Offset) -> Result<Offset, LogError> {
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "trim_to_offset: target must be >= 0".into(),
            ));
        }
        let leo = self.log_end_offset();
        let target = target.min(leo);
        let log_start = self.log_start_offset();
        if target <= log_start {
            tracing::Span::current().record("new_log_start", log_start.0);
            return Ok(log_start);
        }

        // Drop sealed segments whose last record is < target. A sealed
        // segment covers [base_offset, next_segment_base_offset). The
        // "last offset" of a sealed segment equals `next_base - 1`
        // where `next_base` is the next segment's `base_offset`
        // (or, for the most-recent sealed segment, the active segment's
        // `base_offset`).
        let active_base = self.active.as_ref().map_or(leo, Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        let mut to_drop: Vec<Offset> = Vec::new();
        for (seg, next_base) in self.segments.iter().zip(next_bases.iter()) {
            if *next_base <= target {
                to_drop.push(seg.base_offset());
            } else {
                break;
            }
        }

        let drop_set: HashSet<Offset> = to_drop.iter().copied().collect();
        self.segments
            .retain(|s| !drop_set.contains(&s.base_offset()));
        self.stamp_indexes
            .retain(|base, _| !drop_set.contains(base));
        for base in &to_drop {
            let _ = retention::delete_segment_files(&self.dir, *base);
        }

        // If target falls inside the active segment (or between the first
        // remaining sealed segment's base and `target`), advance the
        // start override.
        let new_log_start = self
            .segments
            .first()
            .map_or(active_base, Segment::base_offset);
        if target > new_log_start {
            self.set_log_start_offset(target)?;
        }
        let result = self.log_start_offset();
        tracing::Span::current().record("new_log_start", result.0);
        Ok(result)
    }

    /// Periodic maintenance: roll an old active segment, then apply time- and
    /// size-based retention to sealed segments. The active segment is never
    /// deleted, and if every segment would otherwise be evicted we retain at
    /// least one.
    #[instrument(
        level = "debug",
        skip_all,
        fields(evicted = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn tick(&mut self, now: SystemTime) -> Result<(), LogError> {
        let segment_roll_interval = self.config.read().unwrap().segment_roll_interval;
        let roll_cutoff =
            retention::now_ms(now).saturating_sub(segment_roll_interval.millis_i64_trunc());
        let should_roll = self.active.as_ref().is_some_and(|segment| {
            segment
                .offset_for_timestamp(i64::MIN)
                .is_some_and(|(_, first_timestamp)| first_timestamp < roll_cutoff)
        });
        if should_roll {
            self.roll_active_segment()?;
        }

        // Tiered topics' segment lifecycle is owned by the RemoteLogManager.
        if self.config.read().unwrap().remote_storage_enable {
            return Ok(());
        }
        let sealed_refs: Vec<&Segment> = self.segments.iter().collect();
        let active_size = self.active.as_ref().map_or(ByteSize::ZERO, Segment::size);

        let cfg_guard = self.config.read().unwrap();
        let time_evict = retention::time_based_evict(&sealed_refs, &cfg_guard, now);
        let size_evict = retention::size_based_evict(&sealed_refs, active_size, &cfg_guard);
        drop(cfg_guard);

        // Union preserving order: time first (oldest first), then size.
        let mut to_evict: Vec<Offset> = time_evict;
        let mut seen: HashSet<Offset> = to_evict.iter().copied().collect();
        for base in size_evict {
            if seen.insert(base) {
                to_evict.push(base);
            }
        }

        // Guard: never drop the only remaining segment. `total_segments`
        // includes the active one.
        let total_segments = self.segments.len() + usize::from(self.active.is_some());
        if to_evict.len() >= total_segments {
            to_evict.truncate(total_segments.saturating_sub(1));
        }

        let evict: HashSet<Offset> = to_evict.iter().copied().collect();
        tracing::Span::current().record("evicted", evict.len());
        self.segments.retain(|s| !evict.contains(&s.base_offset()));
        self.stamp_indexes.retain(|base, _| !evict.contains(base));
        for base in to_evict {
            let _ = retention::delete_segment_files(&self.dir, base);
        }
        Ok(())
    }

    /// First absolute offset still present on this broker's local disk
    /// (KIP-405).
    ///
    /// This method delegates to [`Log::log_start_offset`]. The two pointers
    /// co-advance.
    #[must_use]
    pub fn local_log_start_offset(&self) -> Offset {
        self.log_start_offset()
    }

    /// Earliest local `(offset, record_timestamp)` whose record timestamp is
    /// `>= target_ts`.
    ///
    /// The search reads sealed segments oldest-first and then the active
    /// segment. The first segment whose `max_timestamp >= target_ts` holds
    /// the answer. The per-segment helper does the index lookup and the
    /// forward scan. The result is `None` when no local record qualifies,
    /// including the case of an empty log.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        for seg in &self.segments {
            if seg.max_timestamp() >= target_ts
                && let Some(hit) = seg.offset_for_timestamp_with_window(target_ts, scan_window)
            {
                return Some(hit);
            }
        }
        if let Some(active) = &self.active
            && active.max_timestamp() >= target_ts
        {
            return active.offset_for_timestamp_with_window(target_ts, scan_window);
        }
        None
    }

    /// Offset and timestamp of the record that carries the partition's
    /// largest timestamp.
    ///
    /// The scan reads sealed segments and then the active segment. Ties
    /// resolve to the earliest offset: the first segment wins, and the first
    /// record within it wins. The result is `None` when the log holds no
    /// records.
    ///
    /// # Panics
    ///
    /// Panics when another thread poisoned the log configuration lock.
    #[must_use]
    pub fn max_timestamp_offset_and_ts(&self) -> Option<(Offset, i64)> {
        let scan_window = self.config.read().unwrap().timestamp_scan_window;
        let mut best: Option<(i64, Offset)> = None; // (timestamp, offset)
        let candidates = self.segments.iter().chain(self.active.as_ref());
        for seg in candidates {
            if let Some((offset, ts)) = seg.offset_of_max_timestamp_with_window(scan_window)
                && best.is_none_or(|(best_ts, _)| ts > best_ts)
            {
                best = Some((ts, offset));
            }
        }
        best.map(|(ts, offset)| (offset, ts))
    }

    /// Offset of the record carrying the partition's largest timestamp,
    /// or `log_start_offset()` when the log holds no records (KIP-734
    /// `MAX_TIMESTAMP`).
    #[must_use]
    pub fn offset_of_max_timestamp(&self) -> Offset {
        self.max_timestamp_offset_and_ts()
            .map_or_else(|| self.log_start_offset(), |(offset, _)| offset)
    }

    /// Delete every sealed segment whose `last_offset < target` from disk,
    /// then advance `log_start_offset` to `target` (KIP-405).
    ///
    /// This method never touches the active segment. It returns the number of
    /// segments removed. It does nothing and returns `Ok(0)` when
    /// `target <= local_log_start_offset()`.
    ///
    /// The caller must confirm that these segments are safely in the remote
    /// tier (`CopySegmentFinished`) before it calls this method. `Log`
    /// enforces no tiered-storage invariants. See
    /// `crates/broker/src/remote_log_manager.rs` for the production caller.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::InvalidArgument`] if `target` is negative.
    #[instrument(
        level = "info",
        skip(self),
        fields(removed = tracing::field::Empty),
        err,
    )]
    pub fn delete_local_segments_through(&mut self, target: Offset) -> Result<usize, LogError> {
        if target < 0 {
            return Err(LogError::InvalidArgument(
                "delete_local_segments_through: target must be >= 0".into(),
            ));
        }
        if target <= self.local_log_start_offset() {
            return Ok(0);
        }

        // Mirror `tierable_segments`: each sealed segment's last offset is
        // `next.base_offset - 1`, where `next` is the next sealed segment
        // or — for the most-recent sealed segment — the active segment.
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        let to_drop: Vec<Offset> = self
            .segments
            .iter()
            .zip(next_bases.iter())
            .filter_map(|(seg, next_base)| {
                let last = *next_base - 1;
                (last < target).then(|| seg.base_offset())
            })
            .collect();

        let removed = to_drop.len();
        tracing::Span::current().record("removed", removed);
        let drop_set: HashSet<Offset> = to_drop.iter().copied().collect();
        self.segments
            .retain(|s| !drop_set.contains(&s.base_offset()));
        self.stamp_indexes
            .retain(|base, _| !drop_set.contains(base));
        for base in &to_drop {
            let _ = retention::delete_segment_files(&self.dir, *base);
        }

        // Advance the (single) log-start pointer. `local_log_start_offset`
        // delegates here, so the local floor moves in lockstep.
        self.start_offset_override = Some(target);

        Ok(removed)
    }

    /// Describe every sealed segment for tiered-storage offload (KIP-405).
    ///
    /// The result never includes the active segment. Only sealed segments are
    /// immutable and safe to copy.
    ///
    /// `last_offset` comes from the next segment's `base_offset`. For the
    /// most-recent sealed segment it comes from the active segment's base.
    /// The value is therefore correct even for segments loaded from disk
    /// without a tail scan. `max_timestamp` falls back to `-1`, which means
    /// unknown, when the in-memory value is not set.
    #[must_use]
    pub fn tierable_segments(&self) -> Vec<SegmentExport> {
        // Sort the epoch entries once here rather than per-segment inside
        // `epochs_for_range`.
        let mut epoch_entries = self.epoch_checkpoint.entries().to_vec();
        epoch_entries.sort_by_key(|e| e.start_offset);
        let active_base = self
            .active
            .as_ref()
            .map_or_else(|| self.log_end_offset(), Segment::base_offset);
        let next_bases: Vec<Offset> = self
            .segments
            .iter()
            .map(Segment::base_offset)
            .skip(1)
            .chain(std::iter::once(active_base))
            .collect();

        self.segments
            .iter()
            .zip(next_bases)
            .map(|(seg, next_base)| {
                let base = seg.base_offset();
                let last = next_base - 1;
                let max_ts = seg.max_timestamp();
                let txn = name::txnindex_path(&self.dir, base.0);
                SegmentExport {
                    base_offset: base,
                    last_offset: last,
                    max_timestamp: if max_ts == i64::MIN { -1 } else { max_ts },
                    size: seg.size(),
                    log_path: name::log_path(&self.dir, base.0),
                    offset_index_path: name::index_path(&self.dir, base.0),
                    time_index_path: name::timeindex_path(&self.dir, base.0),
                    transaction_index_path: txn.exists().then_some(txn),
                    producer_snapshot_path: producer_snapshot::path(&self.dir, next_base),
                    leader_epochs: epochs_for_range(&epoch_entries, base, last),
                }
            })
            .collect()
    }

    /// Run one compaction pass over the sealed segment list.
    ///
    /// This method does nothing when fewer than 2 sealed segments exist,
    /// because there is nothing to dedup yet. It never touches the active
    /// segment. The output is a single new sealed segment at the lowest input
    /// base offset, and it replaces all consumed sealed segments.
    ///
    /// `ctx` carries the wall clock, which drives the KIP-534 delete-horizon
    /// computation, and the set of currently-active producers. The cleaner
    /// keeps the last batch of each active producer with `RETAIN_EMPTY`, even
    /// when compaction removes all of its records.
    #[instrument(
        level = "info",
        skip_all,
        fields(sealed_segments = self.segments.len()),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn compact(&mut self, ctx: &CompactionContext) -> Result<(), LogError> {
        if self.segments.is_empty() {
            return Ok(());
        }

        let (index_interval, delete_retention) = {
            let cfg_guard = self.config.read().unwrap();
            if cfg_guard.cleanup_policy != crate::CleanupPolicy::Compact {
                return Ok(());
            }
            (cfg_guard.index_interval, cfg_guard.delete_retention)
        };

        let now_ms = retention::now_ms(ctx.now);
        let consumed_bases: Vec<Offset> = self.segments.iter().map(Segment::base_offset).collect();

        // Borrow sealed segments to run map + rewrite (which open
        // additional file handles internally for reading). Then drop the
        // borrows and clear self.segments so the original segments'
        // file handles close before atomic_swap deletes/renames
        // (Windows requires no open handle on a file before remove/rename).
        let rewrite = {
            let sealed_refs: Vec<&Segment> = self.segments.iter().collect();
            let offset_map = crate::compact::build_offset_map(&sealed_refs)?;
            let txn_meta =
                crate::compact::CleanedTransactionMetadata::build(&sealed_refs, &offset_map)?;
            crate::compact::rewrite_segments(
                &self.dir,
                &sealed_refs,
                &offset_map,
                &txn_meta,
                crate::compact::RewriteRetention {
                    now_ms,
                    delete_retention,
                },
                &ctx.active_producers,
                index_interval,
            )?
        };

        self.segments.clear();
        crate::compact::atomic_swap(&self.dir, &consumed_bases, &rewrite)?;

        // open_active(validate=true) tail-scans the new .log to populate
        // last_offset + max_timestamp; then seal() flips the flag.
        let mut new_seg = Segment::open_active(&self.dir, rewrite.new_base_offset, true)?;
        new_seg.seal();
        self.segments.push(new_seg);
        Ok(())
    }
}

/// Inputs to one [`Log::compact`] pass that depend on broker-side state.
///
/// These inputs are the wall clock that computes the KIP-534 delete horizons,
/// and the set of producers that count as active.
///
/// `active_producers` maps `producer_id` to the `base_offset` of that
/// producer's last batch. When compaction removes every record of that batch,
/// the cleaner writes a bare batch header (`RETAIN_EMPTY`) again, so the
/// producer's sequence and epoch state and the log-end offset survive.
#[derive(Debug, Clone)]
pub struct CompactionContext {
    /// Wall clock for this pass. It drives delete-horizon stamps and expiry.
    pub now: std::time::SystemTime,
    /// `producer_id` → last batch `base_offset` for currently-active
    /// producers.
    pub active_producers: std::collections::HashMap<ProducerId, Offset>,
}

/// Leader epochs whose coverage `[start_e, start_{e+1})` overlaps the
/// segment range `[base, last]`, returned as `(epoch, start_offset)` with
/// the start clamped up to `base` and ordered by offset. An epoch with no
/// recorded entries yields an empty result.
///
/// `sorted` must be ordered by `start_offset` ascending (the caller sorts
/// once and reuses the slice across segments).
fn epochs_for_range(
    sorted: &[crate::leader_epoch_checkpoint::EpochEntry],
    base: Offset,
    last: Offset,
) -> Vec<(LeaderEpoch, Offset)> {
    let mut out = Vec::new();
    for (i, e) in sorted.iter().enumerate() {
        // Coverage of this epoch is [start_offset, next.start_offset).
        let end = sorted
            .get(i + 1)
            .map_or(Offset(i64::MAX), |n| n.start_offset);
        if e.start_offset <= last && end > base {
            out.push((e.epoch, e.start_offset.max(base)));
        }
    }
    out
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

/// Parse the transaction coordinator epoch from an end-marker value.
/// Kafka encodes `(version: i16, coordinator_epoch: i32)` in big-endian.
/// Older krabka markers had no value, so malformed or absent values are
/// ignored for backward compatibility.
fn parse_control_marker_coordinator_epoch(value: &[u8]) -> Option<i32> {
    if value.len() < 6 {
        return None;
    }
    let _version = i16::from_be_bytes([value[0], value[1]]);
    Some(i32::from_be_bytes([value[2], value[3], value[4], value[5]]))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;
    use crabka_ids::Offset;
    use crabka_protocol::records::{Attributes, Record};
    use crabka_units::prelude::{gibibytes, kibibytes, mebibytes, millis, minutes, secs};
    use tempfile::tempdir;

    use super::*;
    use crate::leader_epoch_checkpoint::EpochEntry;

    /// A read budget larger than anything these tests write, so the byte
    /// budget never clips the result.
    const NO_LIMIT: ByteSize = gibibytes(4);

    #[test]
    fn batch_header_floor_is_the_protocol_header_length() {
        // `read_raw`'s anti-stall floor: each segment must be asked for at
        // least one whole v2 header, or the boundary walk has nothing to read.
        assert2::assert!(batch_header().bytes_usize() == HEADER_LEN);
    }

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

    fn test_log() -> (tempfile::TempDir, Log) {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        (dir, log)
    }

    fn test_batch_at(_off: i64) -> RecordBatch {
        // `Log::append` overwrites `base_offset`; one record per batch.
        let mut b = RecordBatch {
            base_offset: 0,
            base_timestamp: 1_000,
            max_timestamp: 1_000,
            last_offset_delta: 0,
            ..RecordBatch::default()
        };
        b.records.push(Record {
            offset_delta: 0,
            value: Some(Bytes::from("v")),
            ..Default::default()
        });
        b
    }

    #[test]
    fn log_read_raw_spans_and_is_byte_exact() {
        let (dir, mut log) = test_log();
        let mut wire = bytes::BytesMut::new();
        for off in 0..4i64 {
            let mut b = test_batch_at(off);
            log.append(&mut b).unwrap();
            b.encode(&mut wire).unwrap();
        }
        let wire = wire.freeze();
        let log_end = log.log_end_offset();
        let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.total == wire.len());
        assert2::assert!(&r.bytes[..] == &wire[..]);
        drop(dir);
    }

    #[test]
    fn log_read_raw_spans_multiple_segments() {
        // A tiny `segment_size` forces a roll partway through, so the
        // read must walk at least one sealed segment AND the active
        // segment — exercising the multi-chunk `BytesMut` concat path
        // that `log_read_raw_spans_and_is_byte_exact` (default ~1 GiB
        // segments) never reaches.
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(100), // tiny: roll after roughly each batch
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();

        let n: i64 = 6;
        let mut wire = bytes::BytesMut::new();
        let mut expected_bases = Vec::new();
        for off in 0..n {
            let mut b = test_batch_at(off);
            let base = log.append(&mut b).unwrap();
            expected_bases.push(base);
            b.encode(&mut wire).unwrap();
        }
        let wire = wire.freeze();

        // The roll must actually have happened: at least one sealed
        // segment plus the active segment.
        assert2::assert!(!log.segments.is_empty());
        assert2::assert!(log.active.is_some());

        let log_end = log.log_end_offset();
        let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
        assert2::assert!(r.start_offset == Offset(0));
        assert2::assert!(r.total == wire.len());
        assert2::assert!(&r.bytes[..] == &wire[..]);

        // Decode back to N batches with the expected base offsets.
        let mut cur: &[u8] = &r.bytes;
        let mut bases = Vec::new();
        while !cur.is_empty() {
            let b = crabka_protocol::records::RecordBatch::decode(&mut cur).unwrap();
            bases.push(Offset(b.base_offset));
        }
        assert2::assert!(bases == expected_bases);
        drop(dir);
    }

    crate::sendfile_cfg! {
    /// Increment D/E: `Log::read_raw_desc` across a segment seam must yield
    /// regions whose **concatenation** is byte-identical to the coalesced
    /// bytes of `read_raw`. The result must be several `FileRegion`s, one for
    /// each contributing segment. That proves the cross-segment copy is gone.
    #[test]
    fn log_read_raw_desc_multi_segment_regions_equal_read_raw() {
        use std::os::unix::fs::FileExt;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(100), // tiny: roll roughly each batch
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();

        let n: i64 = 6;
        for off in 0..n {
            let mut b = test_batch_at(off);
            log.append(&mut b).unwrap();
        }
        assert2::assert!(!log.segments.is_empty());

        let log_end = log.log_end_offset();
        let raw = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
        let desc = log.read_raw_desc(Offset(0), log_end, mebibytes(10)).unwrap();

        assert2::assert!(desc.start_offset == raw.start_offset);
        assert2::assert!(desc.total == raw.total);
        // Multi-segment ⇒ more than one region (no coalescing copy).
        check!(
            desc.regions.len() >= 2,
            "expected >=2 regions across the seam, got {}",
            desc.regions.len()
        );

        // Concatenate the pread'd regions and compare to read_raw's bytes.
        let mut assembled = Vec::with_capacity(desc.total);
        for region in &desc.regions {
            let mut buf = vec![0u8; region.len];
            let mut filled = 0;
            let mut off = region.offset;
            while filled < buf.len() {
                let r = region.file.read_at(&mut buf[filled..], off).unwrap();
                assert2::assert!(r > 0);
                filled += r;
                off += r as u64;
            }
            assembled.extend_from_slice(&buf);
        }
        assert2::assert!(assembled == raw.bytes[..]);
        drop(dir);
    }
    } // sendfile_cfg!

    /// Encode a "producer" batch with a producer-chosen `base_offset` and
    /// leader epoch. Return both the wire bytes and a `VerbatimBatch`.
    fn verbatim_from(producer: &RecordBatch, leader_epoch: LeaderEpoch) -> (Bytes, VerbatimBatch) {
        let mut wire = bytes::BytesMut::new();
        producer.encode(&mut wire).unwrap();
        let wire = wire.freeze();
        let vb = VerbatimBatch {
            bytes: wire.clone(),
            last_offset_delta: producer.last_offset_delta,
            max_timestamp: producer.max_timestamp,
            leader_epoch,
            producer_id: ProducerId(producer.producer_id),
            producer_epoch: producer.producer_epoch,
            base_sequence: producer.base_sequence,
            is_transactional: producer.attributes.is_transactional(),
        };
        (wire, vb)
    }

    #[test]
    fn append_verbatim_assigns_offsets_and_is_byte_exact() {
        let (dir, mut log) = test_log();

        // Append three single-record batches verbatim. Each producer batch
        // carries a bogus base_offset (999) that the log must overwrite.
        let mut expected_wire = bytes::BytesMut::new();
        for _ in 0..3 {
            let mut producer = test_batch_at(0);
            producer.base_offset = 999;
            producer.partition_leader_epoch = -1;
            let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));
            log.append_verbatim(&vb).unwrap();
            // Re-encode the expectation with the assigned offset + epoch.
            let mut stamped = producer.clone();
            stamped.base_offset = (log.log_end_offset() - 1).0;
            stamped.partition_leader_epoch = 4;
            stamped.encode(&mut expected_wire).unwrap();
        }
        assert2::assert!(log.log_end_offset() == 3);

        let log_end = log.log_end_offset();
        let r = log.read_raw(Offset(0), log_end, mebibytes(10)).unwrap();
        assert2::assert!(&r.bytes[..] == &expected_wire[..]);

        // Decodes cleanly (CRC valid) with the assigned offsets.
        let mut cur: &[u8] = &r.bytes;
        let mut bases = Vec::new();
        while !cur.is_empty() {
            bases.push(Offset(RecordBatch::decode(&mut cur).unwrap().base_offset));
        }
        assert2::assert!(bases == vec![Offset(0), Offset(1), Offset(2)]);
        drop(dir);
    }

    #[test]
    fn append_verbatim_at_stamps_base_byte_exact() {
        let (dir, mut log) = test_log();

        let mut prefix = test_batch_at(0);
        prefix.partition_leader_epoch = 2;
        log.append(&mut prefix).unwrap();

        let mut producer = test_batch_at(0);
        producer.base_offset = 999;
        producer.partition_leader_epoch = -1;
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

        let appended = log.append_verbatim_at(&vb, Offset(1)).unwrap();

        assert_eq!(appended, Offset(1));
        assert_eq!(log.log_end_offset(), Offset(2));
        assert_eq!(
            log.epoch_checkpoint().entries(),
            &[
                EpochEntry {
                    epoch: LeaderEpoch(2),
                    start_offset: Offset(0),
                },
                EpochEntry {
                    epoch: LeaderEpoch(4),
                    start_offset: Offset(1),
                },
            ]
        );

        let mut expected_wire = bytes::BytesMut::new();
        prefix.encode(&mut expected_wire).unwrap();
        let mut stamped = producer.clone();
        stamped.base_offset = 1;
        stamped.partition_leader_epoch = 4;
        stamped.encode(&mut expected_wire).unwrap();

        let r = log
            .read_raw(Offset(0), log.log_end_offset(), mebibytes(10))
            .unwrap();
        assert_eq!(
            r.bytes[..],
            expected_wire[..],
            "verbatim append_at must be byte-exact after supplied base+epoch stamping"
        );
        drop(dir);
    }

    #[test]
    fn append_verbatim_at_rejects_non_leo_base() {
        let (dir, mut log) = test_log();

        let producer = test_batch_at(0);
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

        let err = log.append_verbatim_at(&vb, Offset(1)).unwrap_err();

        assert!(
            matches!(
                err,
                LogError::OffsetMismatch {
                    expected: Offset(0),
                    actual: Offset(1)
                }
            ),
            "non-LEO append_verbatim_at must report OffsetMismatch"
        );
        assert_eq!(log.log_end_offset(), Offset(0));
        assert!(
            log.read_raw(Offset(0), Offset(0), kibibytes(1))
                .unwrap()
                .bytes
                .is_empty()
        );
        drop(dir);
    }

    #[test]
    fn append_at_uses_reconciled_frontier_floor() {
        let (dir, mut log) = test_log();
        let mut prefix = test_batch_at(0);
        log.append(&mut prefix).unwrap();

        log.reconcile_next_offset(Offset(3));
        let mut gap_batch = test_batch_at(0);
        let mut rejected = gap_batch.clone();
        let err = log.append_at(&mut rejected, Offset(1)).unwrap_err();
        assert!(matches!(
            err,
            LogError::OffsetMismatch {
                expected: Offset(3),
                actual: Offset(1)
            }
        ));

        log.append_at(&mut gap_batch, Offset(3)).unwrap();
        assert_eq!(log.log_end_offset(), Offset(4));
        drop(dir);
    }

    #[test]
    fn append_verbatim_at_uses_reconciled_frontier_floor() {
        let (dir, mut log) = test_log();

        log.reconcile_next_offset(Offset(5));
        let producer = test_batch_at(0);
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));

        let appended = log.append_verbatim_at(&vb, Offset(5)).unwrap();

        assert_eq!(appended, Offset(5));
        assert_eq!(log.log_end_offset(), Offset(6));
        drop(dir);
    }

    #[test]
    fn append_verbatim_matches_owned_append_bytes() {
        // The verbatim path and the owned path must write byte-identical
        // .log bytes for the same logical batch — proving passthrough does
        // not perturb the stored representation.
        let dir_owned = tempdir().unwrap();
        let mut log_owned = Log::open(dir_owned.path(), LogConfig::default()).unwrap();
        let dir_verb = tempdir().unwrap();
        let mut log_verb = Log::open(dir_verb.path(), LogConfig::default()).unwrap();

        let mut producer = test_batch_at(0);
        producer.base_offset = 12345; // overwritten by both paths
        producer.partition_leader_epoch = -1;

        // Owned path: stamp epoch like the produce handler does, then append.
        let mut owned = producer.clone();
        owned.partition_leader_epoch = 9;
        log_owned.append(&mut owned).unwrap();

        // Verbatim path: same epoch via the meta.
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(9));
        log_verb.append_verbatim(&vb).unwrap();

        let end_owned = log_owned.log_end_offset();
        let end_verb = log_verb.log_end_offset();
        assert2::assert!(end_owned == end_verb);
        let r_owned = log_owned
            .read_raw(Offset(0), end_owned, mebibytes(10))
            .unwrap();
        let r_verb = log_verb
            .read_raw(Offset(0), end_verb, mebibytes(10))
            .unwrap();
        assert2::assert!(&r_owned.bytes[..] == &r_verb.bytes[..]);
        drop(dir_owned);
        drop(dir_verb);
    }

    #[test]
    fn append_verbatim_transactional_holds_lso() {
        let (dir, mut log) = test_log();
        // A transactional batch must hold the LSO at the batch's base offset
        // (it isn't stable until a commit/abort marker arrives).
        let mut producer = test_batch_at(0);
        producer.last_offset_delta = 1; // spans offsets 0..=1
        producer.producer_id = 77;
        producer.producer_epoch = 0;
        producer.attributes = producer.attributes.with_transactional(true);
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(0));
        log.append_verbatim(&vb).unwrap();
        // LSO stays at 0 (the open txn's first offset), not log_end (2).
        assert2::assert!(log.log_end_offset() == Offset(2));
        assert2::assert!(log.lso() == Offset(0));
        drop(dir);
    }

    #[test]
    fn open_empty_dir_creates_first_segment() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.log_start_offset() == Offset(0));
        assert2::assert!(log.log_end_offset() == Offset(0));
        log.close();
    }

    #[test]
    fn dir_returns_open_path() {
        // The broker's KIP-113 move machinery reads this back to
        // determine a partition's current owning `log.dir` without
        // re-implementing the directory-layout convention.
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.dir() == dir.path());
    }

    #[test]
    fn open_creates_log_file() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        drop(log);
        let log_path = dir.path().join("00000000000000000000.log");
        assert2::assert!(log_path.exists());
    }

    #[test]
    fn sync_persists_appended_records() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut sample_batch(3)).unwrap();
            log.sync().unwrap(); // fsync without relying on flush_on_append
        }
        // Reopen from disk: the synced records are present.
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.log_end_offset() == Offset(3));
    }

    #[cfg(unix)]
    #[test]
    fn sync_fsyncs_parent_dir_after_segment_lifecycle_events() {
        enum Case {
            InitialCreation,
            ReopenBeforePriorSync,
            Rollover,
        }

        for case in [
            Case::InitialCreation,
            Case::ReopenBeforePriorSync,
            Case::Rollover,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut log = match case {
                Case::InitialCreation => Log::open(dir.path(), LogConfig::default()).unwrap(),
                Case::ReopenBeforePriorSync => {
                    drop(Log::open(dir.path(), LogConfig::default()).unwrap());
                    Log::open(dir.path(), LogConfig::default()).unwrap()
                }
                Case::Rollover => {
                    let mut log = Log::open(
                        dir.path(),
                        LogConfig {
                            segment_size: bytes(1),
                            ..LogConfig::default()
                        },
                    )
                    .unwrap();
                    log.append(&mut sample_batch(1)).unwrap();
                    log.sync().unwrap();
                    log.append(&mut sample_batch(1)).unwrap();
                    log
                }
            };
            sync_observer::take_dir_syncs();

            log.sync().unwrap();

            assert2::assert!(sync_observer::take_dir_syncs() == vec![dir.path().to_path_buf()]);
        }
    }

    #[test]
    fn sync_flushes_sealed_and_active_segments_after_rollover() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.append(&mut sample_batch(1)).unwrap();
        log.sync().unwrap();
        sync_observer::take_segment_flushes();

        log.append(&mut sample_batch(1)).unwrap();
        log.sync().unwrap();

        // Rolling flushes offset 0 before publishing its producer snapshot;
        // explicit sync then flushes both the sealed and active segments.
        assert2::assert!(
            sync_observer::take_segment_flushes() == vec![Offset(0), Offset(0), Offset(1)]
        );
    }

    #[test]
    fn append_assigns_monotonic_offsets() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        let first_offset = log.append(&mut b1).unwrap();
        let second_offset = log.append(&mut b2).unwrap();
        assert2::assert!(first_offset == Offset(0));
        assert2::assert!(second_offset == Offset(3));
        assert2::assert!(log.log_end_offset() == Offset(5));
    }

    #[test]
    fn append_at_matching_offset_preserves_caller_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(3);
        // Pretend the caller (a replicator) already knows the leader's
        // assigned offset for this batch is 0.
        log.append_at(&mut b, Offset(0)).unwrap();
        assert2::assert!(b.base_offset == 0);
        assert2::assert!(log.log_end_offset() == Offset(3));

        let mut b2 = sample_batch(2);
        log.append_at(&mut b2, Offset(3)).unwrap();
        assert2::assert!(b2.base_offset == 3);
        assert2::assert!(log.log_end_offset() == Offset(5));
    }

    #[test]
    fn append_at_with_mismatched_offset_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        let err = log.append_at(&mut b, Offset(7)).unwrap_err();
        assert2::assert!(matches!(
            err,
            LogError::OffsetMismatch {
                expected: Offset(0),
                actual: Offset(7)
            }
        ));
        // Failure must not advance the log.
        assert2::assert!(log.log_end_offset() == 0);
    }

    #[test]
    fn append_then_read_back_in_order() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut expected = Vec::new();
        for _ in 0..3 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
            expected.push(b);
        }
        let out = log.read(Offset(0), NO_LIMIT).unwrap();
        assert2::assert!(out.batches == expected);
        assert2::assert!(out.start_offset == Offset(0));
    }

    #[test]
    fn read_offset_too_low_errors() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        assert2::assert!(matches!(
            log.read(Offset(-1), kibibytes(1)),
            Err(LogError::OffsetTooLow { .. })
        ));
    }

    #[test]
    fn read_at_log_end_returns_empty() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let log_end = log.log_end_offset();
        let out = log.read(log_end, kibibytes(1)).unwrap();
        assert2::assert!(out.batches == Vec::new());
        assert2::assert!(out.start_offset == log_end);
    }

    #[test]
    fn truncate_to_drops_later_records() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b1 = sample_batch(3);
        let mut b2 = sample_batch(2);
        log.append(&mut b1).unwrap();
        log.append(&mut b2).unwrap();
        assert2::assert!(log.log_end_offset() == 5);
        log.truncate_to(Offset(3)).unwrap();
        // First batch (offsets 0..=2) survives; last_offset == 2, end == 3.
        assert2::assert!(log.log_end_offset() == 3);
    }

    #[test]
    fn truncate_to_log_end_is_noop() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(2);
        log.append(&mut b).unwrap();
        let before = log.log_end_offset();
        log.truncate_to(before + 100).unwrap();
        assert2::assert!(log.log_end_offset() == before);
    }

    // `truncate_to` promoting a **sealed** segment with base_offset > 0 must
    // compute the relative cut as `offset - base` (line: `offset.0 -
    // seg.base_offset().0`). We build sealed segment base 1 holding three
    // single-record batches (offsets 1,2,3), drop the active segment, and
    // truncate to offset 3. Correct `rel = 3 - 1 = 2` drops only the offset-3
    // batch → log_end 3. Both the `+` mutant (`rel = 4`) and the `/` mutant
    // (`rel = 3`) leave every batch in place → log_end 4.
    #[test]
    fn truncate_to_promoted_sealed_uses_relative_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let big = LogConfig {
            segment_size: gibibytes(1),
            ..LogConfig::default()
        };
        let tiny = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        // Batch A → active base 0.
        log.append(&mut test_batch_at(0)).unwrap();
        // Roll: seal base 0, fresh active base 1, batch B.
        log.set_config(tiny.clone());
        log.append(&mut test_batch_at(1)).unwrap();
        // No roll: batches C, D accumulate in active base 1 (offsets 2, 3).
        log.set_config(big);
        log.append(&mut test_batch_at(2)).unwrap();
        log.append(&mut test_batch_at(3)).unwrap();
        // Roll: seal base 1 (offsets 1,2,3), fresh active base 4, batch E.
        log.set_config(tiny);
        log.append(&mut test_batch_at(4)).unwrap();
        assert2::assert!(log.log_end_offset() == 5);

        // Truncate to 3: active base 4 (>=3) is dropped, then sealed base 1 is
        // promoted and truncated. rel = 3 - 1 = 2 keeps offsets 1,2, drops 3.
        log.truncate_to(Offset(3)).unwrap();
        assert2::assert!(log.log_end_offset() == 3);
    }

    // `truncate_to` truncating a **surviving active** segment with
    // base_offset > 0 must compute the cut as `offset - base` (line:
    // `offset.0 - active.base_offset().0`). Active segment base 1 holds three
    // single-record batches (offsets 1,2,3); truncate to offset 3. Correct
    // `rel = 3 - 1 = 2` drops only the offset-3 batch → log_end 3. The `+`
    // mutant (`rel = 4`) drops nothing → log_end 4.
    #[test]
    fn truncate_to_active_segment_uses_relative_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let big = LogConfig {
            segment_size: gibibytes(1),
            ..LogConfig::default()
        };
        let tiny = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        // Batch A → active base 0.
        log.append(&mut test_batch_at(0)).unwrap();
        // Roll: seal base 0, fresh active base 1, batch B.
        log.set_config(tiny);
        log.append(&mut test_batch_at(1)).unwrap();
        // No roll: batches C, D accumulate in active base 1 (offsets 2, 3).
        log.set_config(big);
        log.append(&mut test_batch_at(2)).unwrap();
        log.append(&mut test_batch_at(3)).unwrap();
        assert2::assert!(log.log_end_offset() == 4);

        // Active base 1 survives (1 < 3); rel = 3 - 1 = 2 drops the offset-3
        // batch, keeps offsets 1,2 → log_end 3.
        log.truncate_to(Offset(3)).unwrap();
        assert2::assert!(log.log_end_offset() == 3);
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
        assert2::assert!(log.log_end_offset() == 5);
    }

    #[test]
    fn open_truncates_epoch_checkpoint_to_recovered_leo() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("00000000000000000000.log");
        let first_batch_len = {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut first = sample_batch_with_epoch(1, 1);
            log.append(&mut first).unwrap();
            let first_batch_len = log.read_raw(Offset(0), Offset(1), NO_LIMIT).unwrap().total;
            let mut torn = sample_batch_with_epoch(1, 7);
            log.append(&mut torn).unwrap();
            assert_eq!(log.epoch_checkpoint().latest_epoch(), Some(LeaderEpoch(7)));
            first_batch_len
        };

        std::fs::OpenOptions::new()
            .write(true)
            .open(&log_path)
            .unwrap()
            .set_len(u64::try_from(first_batch_len + 5).unwrap())
            .unwrap();

        let cfg = LogConfig {
            validate_on_open: true,
            ..LogConfig::default()
        };
        let reopened = Log::open(dir.path(), cfg).unwrap();

        assert_eq!(reopened.log_end_offset(), Offset(1));
        assert!(
            reopened
                .epoch_checkpoint()
                .entries()
                .iter()
                .all(|entry| entry.start_offset < reopened.log_end_offset())
        );
        assert_eq!(
            reopened.epoch_checkpoint().latest_epoch(),
            Some(LeaderEpoch(1))
        );
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
        assert2::assert!(log.log_end_offset() == before);
    }

    #[test]
    fn tick_never_deletes_only_segment() {
        use std::time::Duration;
        let dir = tempdir().unwrap();
        let config = LogConfig {
            retention: Some(secs(1)),
            retention_size: Some(ByteSize::ZERO),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut b1 = sample_batch(2);
        log.append(&mut b1).unwrap();
        // Advance "now" 30 days into the future.
        let now = SystemTime::now() + Duration::from_hours(30 * 24);
        log.tick(now).unwrap();
        assert2::assert!(log.log_end_offset() == 2);
    }

    #[test]
    fn tick_rolls_active_segment_when_first_record_is_old() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(10),
            retention: None,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(2);
        batch.base_timestamp = 1_000;
        batch.max_timestamp = 1_000;
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(12))
            .unwrap();

        assert2::assert!(log.segments.len() == 1);
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(2));
    }

    #[test]
    fn tick_keeps_active_segment_at_roll_boundary() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(10),
            retention: None,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(1);
        batch.base_timestamp = 1_000;
        batch.max_timestamp = 1_000;
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(11))
            .unwrap();

        assert2::assert!(log.segments.is_empty());
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(0));
    }

    #[test]
    fn tick_rolls_tiered_segment_without_local_eviction() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_roll_interval: secs(1),
            retention: Some(secs(1)),
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        let mut batch = sample_batch(1);
        log.append(&mut batch).unwrap();

        log.tick(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(10))
            .unwrap();

        assert2::assert!(log.segments.len() == 1);
        assert2::assert!(log.active.as_ref().unwrap().base_offset() == Offset(1));
    }

    #[test]
    fn segment_rolls_when_bytes_exceeded() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(200), // tiny so we roll fast
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
        assert2::assert!(log_files.len() >= 2);
    }

    #[test]
    fn producer_snapshot_survives_local_segment_deletion_and_restart() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        let mut producer = sample_batch(2);
        producer.producer_id = 42;
        producer.producer_epoch = 3;
        producer.base_sequence = 7;
        producer.max_timestamp = 1_234;
        log.append(&mut producer).unwrap();

        // The second append rolls the producer batch into a sealed segment and
        // writes the snapshot at the new segment's base offset.
        log.append(&mut sample_batch(1)).unwrap();
        let export = log.tierable_segments().into_iter().next().unwrap();
        check!(export.producer_snapshot_path.exists());
        let local_start = export.last_offset + 1;
        check!(log.delete_local_segments_through(local_start).unwrap() == 1);
        drop(log);

        let reopened = Log::open(dir.path(), config).unwrap();
        let entry = reopened
            .producer_state_snapshot()
            .into_iter()
            .find(|entry| entry.producer_id == 42)
            .unwrap();
        check!(entry.producer_epoch == 3);
        check!(entry.last_sequence == 8);
        check!(entry.last_offset == 1);
        check!(entry.offset_delta == 1);
        check!(entry.timestamp == 1_234);
    }

    #[test]
    fn snapshot_recovery_preserves_open_and_completed_transaction_fields() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config.clone()).unwrap();
        let mut data = transactional_batch(77, 4, &["a", "b"]);
        data.base_sequence = 10;
        log.append(&mut data).unwrap();
        log.append(&mut sample_batch(1)).unwrap();

        let open = log
            .producer_state_snapshot()
            .into_iter()
            .find(|entry| entry.producer_id == 77)
            .unwrap();
        check!(open.current_txn_first_offset == Some(Offset(0)));

        log.append(&mut commit_marker(77, 4)).unwrap();
        drop(log);
        let reopened = Log::open(dir.path(), config).unwrap();
        let completed = reopened
            .producer_state_snapshot()
            .into_iter()
            .find(|entry| entry.producer_id == 77)
            .unwrap();
        check!(completed.last_sequence == 11);
        check!(completed.current_txn_first_offset == None);
        check!(completed.coordinator_epoch == 17);
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

    /// Build an end-marker value: (version=0: i16, coordinator epoch: i32) BE.
    fn control_value(coordinator_epoch: i32) -> Bytes {
        let mut buf = [0u8; 6];
        buf[0..2].copy_from_slice(&0i16.to_be_bytes());
        buf[2..6].copy_from_slice(&coordinator_epoch.to_be_bytes());
        Bytes::from(buf.to_vec())
    }

    /// A commit control batch (`marker_type=1`) for the given pid and epoch.
    /// `Log::append` rewrites the offsets.
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
                value: Some(control_value(17)),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    /// An abort control batch (`marker_type=0`) for the given pid and epoch.
    /// `Log::append` rewrites the offsets.
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
                value: Some(control_value(17)),
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
        assert2::assert!(log.lso() == log.log_end_offset());

        // Now an in-flight txn batch — LSO stays.
        let mut b1 = transactional_batch(1000, 0, &["a", "b"]); // pid=1000 epoch=0
        let old_lso = log.lso();
        log.append(&mut b1).unwrap();
        assert2::assert!(log.lso() == old_lso);

        // Commit marker — LSO catches up.
        let mut commit = commit_marker(1000, 0);
        log.append(&mut commit).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());
    }

    #[test]
    fn reopen_rebuilds_pending_transactions_and_lso() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut transactional_batch(1000, 4, &["a", "b"]))
                .unwrap();
            assert2::assert!(log.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
            assert2::assert!(log.lso() == Offset(0));
        }

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(reopened.pending_transaction_start(ProducerId(1000)) == Some(Offset(0)));
        assert2::assert!(reopened.lso() == Offset(0));
        reopened
            .set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(30, 1),
            ))
            .unwrap();

        reopened.append(&mut commit_marker(1000, 4)).unwrap();
        check!(reopened.stamp_for_offset(Offset(0)) == Some(30));
        check!(reopened.stamp_for_offset(Offset(1)) == Some(30));
        check!(reopened.stamp_for_offset(Offset(2)) == None);
        assert2::assert!(reopened.producer_transaction_state(ProducerId(1000)) == (17, None));
        assert2::assert!(
            reopened
                .pending_transaction_start(ProducerId(1000))
                .is_none()
        );
        assert2::assert!(reopened.lso() == reopened.log_end_offset());

        reopened
            .append(&mut transactional_batch(1000, 4, &["next"]))
            .unwrap();
        assert2::assert!(
            reopened.producer_transaction_state(ProducerId(1000)) == (17, Some(Offset(3)))
        );
        drop(reopened);

        let recovered_again = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(
            recovered_again.producer_transaction_state(ProducerId(1000)) == (17, Some(Offset(3)))
        );
    }

    #[test]
    fn reopen_does_not_treat_non_transactional_producer_data_as_pending() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut batch = sample_batch(1);
            batch.producer_id = 42;
            log.append(&mut batch).unwrap();
        }

        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        check!(reopened.pending_transaction_start(ProducerId(42)) == None);
        check!(reopened.lso() == reopened.log_end_offset());
    }

    // ---- .stampindex append-path wiring tests ----

    /// Append three data batches with distinct offset ranges to a
    /// stamp-enabled log. The `.stampindex` records one entry for each batch,
    /// in order, with the successive stamps from the source.
    /// `stamp_for_offset` resolves every covered offset, including interior
    /// offsets and the inclusive end offset. Offsets past the end resolve to
    /// `None`.
    #[test]
    fn stampindex_records_appended_batches_and_resolves_offsets() {
        let (dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(1_000, 10),
        ))
        .unwrap();
        check!(log.stamp_source().is_some());

        log.append(&mut sample_batch(2)).unwrap(); // offsets 0..=1, stamp 1000
        log.append(&mut sample_batch(1)).unwrap(); // offset  2,     stamp 1010
        log.append(&mut sample_batch(3)).unwrap(); // offsets 3..=5, stamp 1020

        // Query surface resolves every covered offset to its batch's stamp.
        check!(log.stamp_for_offset(Offset(0)) == Some(1_000));
        check!(log.stamp_for_offset(Offset(1)) == Some(1_000));
        check!(log.stamp_for_offset(Offset(2)) == Some(1_010));
        check!(log.stamp_for_offset(Offset(3)) == Some(1_020));
        check!(log.stamp_for_offset(Offset(5)) == Some(1_020));
        check!(log.stamp_for_offset(Offset(6)) == None);

        // The durable on-disk sidecar holds exactly one entry per data batch.
        let idx = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
        assert2::assert!(
            idx.entries()
                == [
                    StampEntry {
                        base_offset: Offset(0),
                        last_offset: Offset(1),
                        stamp: 1_000,
                    },
                    StampEntry {
                        base_offset: Offset(2),
                        last_offset: Offset(2),
                        stamp: 1_010,
                    },
                    StampEntry {
                        base_offset: Offset(3),
                        last_offset: Offset(5),
                        stamp: 1_020,
                    },
                ]
        );
    }

    /// A log with no injected stamp source stamps nothing. `stamp_for_offset`
    /// is always `None` and the log never creates a `.stampindex` file. This
    /// is the unchanged-behavior guarantee for pure-Kafka partitions.
    #[test]
    fn no_stamp_source_stamps_nothing() {
        let (dir, mut log) = test_log();
        log.append(&mut sample_batch(2)).unwrap();
        log.append(&mut sample_batch(3)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(4)) == None);
        assert2::assert!(!dir.path().join("00000000000000000000.stampindex").exists());
    }

    /// Transactional data stays unstamped until commit. The commit marker is
    /// not stamped because a stamp is a coordinate for data records.
    #[test]
    fn control_markers_are_not_stamped() {
        let (dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(1, 1),
        ))
        .unwrap();

        // Transactional data at offsets 0..=1, then its commit marker at 2.
        log.append(&mut transactional_batch(1000, 0, &["a", "b"]))
            .unwrap();
        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        log.append(&mut commit_marker(1000, 0)).unwrap();
        // A following non-txn data batch at offset 3.
        log.append(&mut sample_batch(1)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == Some(1)); // txn data
        check!(log.stamp_for_offset(Offset(1)) == Some(1));
        check!(log.stamp_for_offset(Offset(2)) == None); // commit marker: unstamped
        check!(log.stamp_for_offset(Offset(3)) == Some(2)); // next data batch

        let idx = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
        assert2::assert!(
            idx.entries()
                == [
                    StampEntry {
                        base_offset: Offset(0),
                        last_offset: Offset(1),
                        stamp: 1,
                    },
                    StampEntry {
                        base_offset: Offset(3),
                        last_offset: Offset(3),
                        stamp: 2,
                    },
                ]
        );
    }

    #[test]
    fn commit_stamps_only_matching_interleaved_transaction_ranges() {
        let (_dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 10),
        ))
        .unwrap();

        log.append(&mut transactional_batch(1000, 0, &["a"]))
            .unwrap(); // offset 0
        log.append(&mut transactional_batch(2000, 0, &["b", "c"]))
            .unwrap(); // offsets 1..=2
        log.append(&mut transactional_batch(1000, 0, &["d"]))
            .unwrap(); // offset 3

        log.append(&mut commit_marker(2000, 0)).unwrap(); // offset 4
        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == Some(10));
        check!(log.stamp_for_offset(Offset(2)) == Some(10));
        check!(log.stamp_for_offset(Offset(3)) == None);

        log.append(&mut commit_marker(1000, 0)).unwrap(); // offset 5
        check!(log.stamp_for_offset(Offset(0)) == Some(20));
        check!(log.stamp_for_offset(Offset(3)) == Some(20));
        check!(log.stamp_for_offset(Offset(4)) == None);
        check!(log.stamp_for_offset(Offset(5)) == None);
    }

    #[test]
    fn abort_leaves_transactional_data_unstamped() {
        let (_dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(7, 1),
        ))
        .unwrap();

        log.append(&mut transactional_batch(1000, 0, &["a", "b"]))
            .unwrap();
        log.append(&mut abort_marker(1000, 0)).unwrap();
        log.append(&mut sample_batch(1)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == None);
        check!(log.stamp_for_offset(Offset(3)) == Some(7));
    }

    #[test]
    fn commit_succeeds_after_transaction_data_is_retained_away() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(7, 1),
        ))
        .unwrap();
        log.append(&mut transactional_batch(1000, 0, &["old"]))
            .unwrap();
        log.append(&mut sample_batch(1)).unwrap();
        log.trim_to_offset(Offset(1)).unwrap();

        log.append(&mut commit_marker(1000, 0)).unwrap();

        check!(log.lso() == log.log_end_offset());
        check!(log.stamp_for_offset(Offset(0)) == None);
    }

    #[test]
    fn supplied_commit_stamp_is_recorded_and_observed() {
        let (_dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(1, 1),
        ))
        .unwrap();

        log.append(&mut transactional_batch(1000, 0, &["a", "b"]))
            .unwrap();
        log.append_with_commit_stamp(&mut commit_marker(1000, 0), 100)
            .unwrap();
        log.append(&mut sample_batch(1)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == Some(100));
        check!(log.stamp_for_offset(Offset(1)) == Some(100));
        check!(log.stamp_for_offset(Offset(2)) == None);
        check!(log.stamp_for_offset(Offset(3)) == Some(101));
    }

    #[test]
    fn supplied_commit_stamp_rejects_invalid_marker_paths() {
        let (_dir, mut log) = test_log();
        let error = log
            .append_with_commit_stamp(&mut commit_marker(1000, 0), 100)
            .unwrap_err();
        assert2::assert!(let LogError::InvalidArgument(_) = error);
        check!(log.log_end_offset() == Offset(0));

        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(1, 1),
        ))
        .unwrap();
        let error = log
            .append_with_commit_stamp(&mut abort_marker(1000, 0), 100)
            .unwrap_err();
        assert2::assert!(let LogError::InvalidArgument(_) = error);
        let error = log
            .append_with_commit_stamp(&mut sample_batch(1), 100)
            .unwrap_err();
        assert2::assert!(let LogError::InvalidArgument(_) = error);
        check!(log.log_end_offset() == Offset(0));
    }

    #[test]
    fn replicated_commit_uses_supplied_stamp() {
        let (_dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(1, 1),
        ))
        .unwrap();
        log.append(&mut transactional_batch(1000, 0, &["a"]))
            .unwrap();

        log.append_at_with_commit_stamp(&mut commit_marker(1000, 0), Offset(1), 40)
            .unwrap();
        log.append(&mut sample_batch(1)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == Some(40));
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == Some(41));
    }

    #[test]
    fn installing_source_observes_durable_stamp_horizon() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(100, 1),
            ))
            .unwrap();
            log.append(&mut sample_batch(1)).unwrap();
        }

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        reopened
            .set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(1, 1),
            ))
            .unwrap();
        reopened.append(&mut sample_batch(1)).unwrap();

        check!(reopened.stamp_for_offset(Offset(0)) == Some(100));
        check!(reopened.stamp_for_offset(Offset(1)) == Some(101));
    }

    #[test]
    fn startup_hides_legacy_append_stamp_for_open_transaction() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            log.append(&mut transactional_batch(1000, 0, &["a"]))
                .unwrap();
        }
        let path = dir.path().join("00000000000000000000.stampindex");
        let mut legacy = StampIndex::open(path).unwrap();
        legacy
            .append(StampEntry {
                base_offset: Offset(0),
                last_offset: Offset(0),
                stamp: 5,
            })
            .unwrap();

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        reopened
            .set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(10, 1),
            ))
            .unwrap();
        check!(reopened.stamp_for_offset(Offset(0)) == None);

        reopened.append(&mut commit_marker(1000, 0)).unwrap();
        check!(reopened.stamp_for_offset(Offset(0)) == Some(10));
    }

    #[test]
    fn transaction_commit_stamps_data_in_sealed_segments() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(50, 1),
        ))
        .unwrap();

        log.append(&mut transactional_batch(1000, 0, &["a"]))
            .unwrap(); // segment 0
        log.append(&mut transactional_batch(1000, 0, &["b"]))
            .unwrap(); // segment 1
        log.append(&mut commit_marker(1000, 0)).unwrap(); // segment 2

        check!(log.stamp_for_offset(Offset(0)) == Some(50));
        check!(log.stamp_for_offset(Offset(1)) == Some(50));
        check!(log.stamp_for_offset(Offset(2)) == None);
        assert2::assert!(
            StampIndex::open(dir.path().join("00000000000000000000.stampindex"))
                .unwrap()
                .entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(0),
                    stamp: 50,
                }]
        );
        assert2::assert!(
            StampIndex::open(dir.path().join("00000000000000000001.stampindex"))
                .unwrap()
                .entries()
                == [StampEntry {
                    base_offset: Offset(1),
                    last_offset: Offset(1),
                    stamp: 50,
                }]
        );
    }

    #[test]
    fn truncate_removes_stamps_for_discarded_tail() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        log.append(&mut sample_batch(1)).unwrap();
        log.append(&mut sample_batch(1)).unwrap();
        check!(log.stamp_for_offset(Offset(1)) == Some(11));

        log.truncate_to(Offset(1)).unwrap();
        check!(log.stamp_for_offset(Offset(0)) == Some(10));
        check!(log.stamp_for_offset(Offset(1)) == None);
        assert2::assert!(
            StampIndex::open(dir.path().join("00000000000000000000.stampindex"))
                .unwrap()
                .entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(0),
                    stamp: 10,
                }]
        );
    }

    #[test]
    fn truncate_preserves_stamp_indexes_before_promoted_segment() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        for _ in 0..3 {
            log.append(&mut sample_batch(1)).unwrap();
        }

        log.truncate_to(Offset(2)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == Some(10));
        check!(log.stamp_for_offset(Offset(1)) == Some(11));
        check!(log.stamp_for_offset(Offset(2)) == None);
    }

    #[test]
    fn trim_removes_only_evicted_segment_stamp_indexes() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        for _ in 0..3 {
            log.append(&mut sample_batch(1)).unwrap();
        }

        log.trim_to_offset(Offset(1)).unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == Some(11));
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
    }

    #[test]
    fn tick_removes_only_retained_away_segment_stamp_indexes() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        for _ in 0..3 {
            log.append(&mut sample_batch(1)).unwrap();
        }

        log.tick(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
            .unwrap();

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == None);
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
    }

    #[test]
    fn tiered_local_delete_removes_only_deleted_segment_stamp_indexes() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(10, 1),
        ))
        .unwrap();
        for _ in 0..3 {
            log.append(&mut sample_batch(1)).unwrap();
        }

        check!(log.delete_local_segments_through(Offset(1)).unwrap() == 1);

        check!(log.stamp_for_offset(Offset(0)) == None);
        check!(log.stamp_for_offset(Offset(1)) == Some(11));
        check!(log.stamp_for_offset(Offset(2)) == Some(12));
    }

    /// The verbatim replication append path stamps the *full* offset span of
    /// the batch. A multi-record batch appended through `append_verbatim`
    /// records `last_offset == base_offset + last_offset_delta`. Interior
    /// offsets and the inclusive end offset therefore resolve, and one offset
    /// past the end does not. This test guards the `base + delta` arithmetic
    /// on that path, which the owned-append tests above do not exercise.
    #[test]
    fn append_verbatim_stamps_full_offset_range() {
        let (dir, mut log) = test_log();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(500, 1),
        ))
        .unwrap();

        // A four-record producer batch, appended verbatim, spans offsets 0..=3.
        let mut producer = sample_batch(4);
        producer.base_offset = 999; // bogus; the log overwrites it with 0
        producer.partition_leader_epoch = -1;
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(4));
        log.append_verbatim(&vb).unwrap();

        // last_offset is base(0) + delta(3) = 3.
        check!(log.stamp_for_offset(Offset(0)) == Some(500));
        check!(log.stamp_for_offset(Offset(3)) == Some(500));
        check!(log.stamp_for_offset(Offset(4)) == None);

        let idx = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
        assert2::assert!(
            idx.entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(3),
                    stamp: 500,
                }]
        );
    }

    /// A roll to a new segment reopens the active `.stampindex` at the
    /// sidecar of the new segment. The entry for the post-roll batch lands in
    /// the new segment's file and does not leak back into the sealed
    /// segment's file. This test guards the reopen. A reopen that did nothing
    /// would keep the stamps in the sealed segment's index.
    #[test]
    fn roll_reopens_stampindex_for_new_segment() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(1), // roll on every append after the first
                ..LogConfig::default()
            },
        )
        .unwrap();
        log.set_stamp_source(std::sync::Arc::new(
            crate::stamp_source::MonotonicStampSource::new(100, 5),
        ))
        .unwrap();

        log.append(&mut sample_batch(1)).unwrap(); // offset 0, segment 0, stamp 100
        log.append(&mut sample_batch(1)).unwrap(); // rolls: segment @ base 1, stamp 105

        // The new segment's own sidecar holds exactly its post-roll entry.
        let seg1 = StampIndex::open(dir.path().join("00000000000000000001.stampindex")).unwrap();
        assert2::assert!(
            seg1.entries()
                == [StampEntry {
                    base_offset: Offset(1),
                    last_offset: Offset(1),
                    stamp: 105,
                }]
        );
        // The sealed segment kept only its pre-roll entry — nothing leaked back.
        let seg0 = StampIndex::open(dir.path().join("00000000000000000000.stampindex")).unwrap();
        assert2::assert!(
            seg0.entries()
                == [StampEntry {
                    base_offset: Offset(0),
                    last_offset: Offset(0),
                    stamp: 100,
                }]
        );
    }

    /// Wire-exactness invariance. This test appends an identical mixed
    /// sequence to a stamp-enabled log and to an unstamped log: non-txn,
    /// transactional data, commit marker, non-txn. Both logs give
    /// byte-for-byte identical `.log` output, and identical assigned offsets
    /// and LSO at every step. The stamp is only an added sidecar and cannot
    /// change any client-facing coordinate. The high-watermark comes from
    /// these values, so it too stays the same.
    #[test]
    fn stamping_does_not_change_offsets_lso_or_log_bytes() {
        // Build the same append script for both logs.
        fn script() -> Vec<RecordBatch> {
            vec![
                sample_batch(2),                      // non-txn, offsets 0..=1
                transactional_batch(1000, 0, &["a"]), // txn data, offset 2
                commit_marker(1000, 0),               // commit marker, offset 3
                sample_batch(3),                      // non-txn, offsets 4..=6
            ]
        }

        let dir_plain = tempdir().unwrap();
        let mut plain = Log::open(dir_plain.path(), LogConfig::default()).unwrap();

        let dir_stamped = tempdir().unwrap();
        let mut stamped = Log::open(dir_stamped.path(), LogConfig::default()).unwrap();
        stamped
            .set_stamp_source(std::sync::Arc::new(
                crate::stamp_source::MonotonicStampSource::new(7, 3),
            ))
            .unwrap();

        let mut plain_bases = Vec::new();
        let mut stamped_bases = Vec::new();
        let mut plain_lsos = Vec::new();
        let mut stamped_lsos = Vec::new();
        for (mut pb, mut sb) in script().into_iter().zip(script()) {
            plain_bases.push(plain.append(&mut pb).unwrap());
            stamped_bases.push(stamped.append(&mut sb).unwrap());
            plain_lsos.push(plain.lso());
            stamped_lsos.push(stamped.lso());
        }

        // Identical offset assignment and LSO progression at every step.
        assert2::assert!(plain_bases == stamped_bases);
        assert2::assert!(plain_lsos == stamped_lsos);
        assert2::assert!(plain.log_end_offset() == stamped.log_end_offset());
        // The unstamped log never sees a stamp; the stamped one does.
        check!(plain.stamp_for_offset(Offset(0)) == None);
        check!(stamped.stamp_for_offset(Offset(0)) == Some(7));

        // Byte-for-byte identical client-facing `.log` output.
        let end = plain.log_end_offset();
        let plain_raw = plain.read_raw(Offset(0), end, mebibytes(10)).unwrap();
        let stamped_raw = stamped.read_raw(Offset(0), end, mebibytes(10)).unwrap();
        assert2::assert!(plain_raw.bytes == stamped_raw.bytes);
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
        // Txn batch was the first append: start_offset = 0.
        // last_offset = abort marker's base_offset + last_offset_delta = 3 + 0 = 3.
        // (The 3-record txn batch occupies offsets 0-2; the marker lands at offset 3.)
        assert2::assert!(
            entries
                == [AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(3),
                    producer_id: ProducerId(1000),
                }]
        );
    }

    // The aborted-txn `last_offset` is `marker.base_offset +
    // marker.last_offset_delta`. Using a marker that spans TWO offsets
    // (`last_offset_delta = 1`) pins the `+`: the txn batch occupies offsets
    // 0..=2, the abort marker lands at base_offset 3 with delta 1, so the
    // recorded `last_offset` is `3 + 1 = 4`. Mutating `+`→`-` would record 2.
    #[test]
    fn abort_marker_last_offset_uses_base_plus_delta() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut t = transactional_batch(1000, 0, &["a", "b", "c"]);
        log.append(&mut t).unwrap(); // offsets 0..=2

        // Abort marker spanning two offsets (delta 1): base 3, last 4.
        let mut a = abort_marker(1000, 0);
        a.last_offset_delta = 1;
        log.append(&mut a).unwrap();

        let idx = TxnIndex::open(dir.path().join("00000000000000000000.txnindex")).unwrap();
        assert2::assert!(
            idx.entries()
                == [AbortedTxn {
                    start_offset: Offset(0),
                    last_offset: Offset(4), // 3 + 1, not 3 - 1
                    producer_id: ProducerId(1000),
                }]
        );
    }

    // LSO tracking (owned path) keys on `is_transactional() && !pid.is_none()`.
    // A NON-transactional batch that carries a valid producer_id (idempotent
    // producer, pid >= 0) must NOT be treated as an open txn: LSO advances to
    // log_end. Mutating `&&`→`||` would hold LSO at the batch base (0).
    #[test]
    fn non_txn_batch_with_valid_pid_advances_lso() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // Idempotent (not transactional) producer: pid >= 0, no transactional
        // attribute bit set.
        let mut b = sample_batch(2);
        b.producer_id = 55;
        b.producer_epoch = 0;
        assert2::assert!(!b.attributes.is_transactional());
        log.append(&mut b).unwrap();
        // Not an open txn → LSO advances to log_end (2), not held at 0.
        assert2::assert!(log.lso() == Offset(2));
        assert2::assert!(log.log_end_offset() == Offset(2));
    }

    // Verbatim counterpart of `non_txn_batch_with_valid_pid_advances_lso`,
    // pinning the `&&` in the verbatim LSO-tracking branch. A non-transactional
    // verbatim batch with a valid producer_id must advance LSO; `&&`→`||`
    // would hold it at the batch base (0).
    #[test]
    fn non_txn_verbatim_batch_with_valid_pid_advances_lso() {
        let (dir, mut log) = test_log();
        let mut producer = test_batch_at(0);
        producer.last_offset_delta = 1; // spans offsets 0..=1
        producer.producer_id = 55; // valid pid, but NOT transactional
        producer.producer_epoch = 0;
        assert2::assert!(!producer.attributes.is_transactional());
        let (_wire, vb) = verbatim_from(&producer, LeaderEpoch(0));
        log.append_verbatim(&vb).unwrap();
        assert2::assert!(log.log_end_offset() == Offset(2));
        assert2::assert!(log.lso() == Offset(2));
        drop(dir);
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
        assert2::assert!(log.lso() == lso_after_open);

        // Commit producer 2000. LSO advances to log_end_offset.
        let mut c2 = commit_marker(2000, 0);
        log.append(&mut c2).unwrap();
        assert2::assert!(log.lso() == log.log_end_offset());
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
        assert2::assert!(
            log.epoch_checkpoint().entries()
                == &[
                    EpochEntry {
                        epoch: LeaderEpoch(0),
                        start_offset: Offset(0)
                    },
                    EpochEntry {
                        epoch: LeaderEpoch(1),
                        start_offset: Offset(3)
                    }
                ]
        );
    }

    #[test]
    fn truncate_to_drops_stale_epoch_checkpoint_entries() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // Epoch 1 at offsets 0..3, then epoch 7 starting at offset 3.
        let mut b1 = sample_batch_with_epoch(3, 1);
        log.append(&mut b1).unwrap();
        let epoch7_start = log.log_end_offset();
        let mut b2 = sample_batch_with_epoch(2, 7);
        log.append(&mut b2).unwrap();
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(7)));

        // Truncate away the entire epoch-7 tail.
        log.truncate_to(epoch7_start).unwrap();

        let leo = log.log_end_offset();
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
        assert2::assert!(
            log.epoch_checkpoint()
                .end_offset_for_epoch(LeaderEpoch(7), leo)
                == Offset(-1)
        );
        assert2::assert!(
            log.epoch_checkpoint()
                .end_offset_for_epoch(LeaderEpoch(1), leo)
                == leo
        );
    }

    #[test]
    fn reset_to_clears_leader_epoch_checkpoint() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        // A follower that replicated real data builds an epoch history.
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        log.append(&mut sample_batch_with_epoch(2, 2)).unwrap(); // epoch 2 @ 3
        log.append(&mut sample_batch_with_epoch(1, 5)).unwrap(); // epoch 5 @ 5
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(5)));

        // Hard reset to an empty log — the replicator's OFFSET_OUT_OF_RANGE
        // recovery path (Kafka's `truncateFullyAndStartAt`). The log now has
        // NO records, so it must advertise NO leader epoch. Otherwise the
        // follower keeps sending a stale `last_fetched_epoch` and the leader's
        // KIP-320 reconciliation serves a batch at a mismatched base offset,
        // looping forever on `append_at` (phantom ISR member → pinned HW →
        // acks=all stall).
        log.reset_to(Offset(0)).unwrap();

        assert2::assert!(log.epoch_checkpoint().latest_epoch() == None);
        assert2::assert!(log.epoch_checkpoint().entries() == &[][..]);
        // The cleared state must survive a reopen (a restarted broker re-reads
        // the on-disk checkpoint file).
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(reopened.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn read_raw_after_reopen_does_not_skip_first_sealed_segment() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            segment_size: bytes(1), // roll on every append → one segment per batch
            ..LogConfig::default()
        };
        {
            let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
            log.append(&mut sample_batch(1)).unwrap(); // offset 0 → sealed seg base 0
            log.append(&mut sample_batch(1)).unwrap(); // offset 1 → sealed seg base 1
            log.append(&mut sample_batch(1)).unwrap(); // offset 2 → active seg base 2
            assert2::assert!(log.log_end_offset() == 3);
            assert2::assert!(log.segments.len() >= 2);
        }
        // Reopen simulates a broker restart: sealed segments are loaded via the
        // no-scan Segment::open, which leaves last_offset = base - 1. Without
        // fixing last_offset from the next segment's base, read_raw skips the
        // first sealed segment (its stale last_offset < fetch_offset) and serves
        // a later segment's base — the on-cluster phantom-follower gap that
        // pinned the high-watermark and stalled acks=all.
        let reopened = Log::open(dir.path(), cfg).unwrap();
        let r = reopened
            .read_raw(Offset(0), reopened.log_end_offset(), mebibytes(1))
            .unwrap();
        assert2::assert!(r.start_offset == 0);
    }

    /// On reopen, each recovered sealed segment's `last_offset` is set to
    /// `next_base - 1` (line: `seg.seal_at(Offset(base_offsets[i + 1] - 1))`).
    /// Multi-record segments give non-consecutive bases so the `- 1` is
    /// observable: for consecutive exports `last_offset + 1 == next_base`.
    /// Mutating `- 1`→`+ 1` sets `last_offset = next_base + 1` (so
    /// `last_offset + 1 == next_base + 2`); mutating `- 1`→`/ 1` sets
    /// `last_offset = next_base` (so `last_offset + 1 == next_base + 1`).
    #[test]
    fn reopen_seals_recovered_segments_at_next_base_minus_one() {
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let cfg = LogConfig {
            segment_size: bytes(1), // roll on every append
            ..LogConfig::default()
        };
        {
            let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
            // Multi-record batches → segment bases are 0, 2, 4, ... (each
            // sealed segment spans two offsets), so next_base - base == 2.
            for _ in 0..4 {
                log.append(&mut sample_batch(2)).unwrap();
            }
            assert2::assert!(log.segments.len() >= 2);
        }
        // Reopen: sealed segments recovered via no-scan open + seal_at(next-1).
        let reopened = Log::open(dir.path(), cfg).unwrap();
        let exports = reopened.tierable_segments();
        assert2::assert!(exports.len() >= 2);
        for pair in exports.windows(2) {
            // last_offset must be exactly one below the next segment's base.
            assert2::assert!(pair[0].last_offset + 1 == pair[1].base_offset);
        }
    }

    #[test]
    fn reset_to_nonzero_base_clears_all_epochs_not_just_tail() {
        // Guards against the subtly-wrong fix `truncate_from_end(new_base)`,
        // which retains an entry whose `start_offset < new_base` even though
        // the reset log holds no records below `new_base`.
        use tempfile::TempDir;
        let dir = TempDir::new().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        log.append(&mut sample_batch_with_epoch(3, 1)).unwrap(); // epoch 1 @ 0
        assert2::assert!(log.epoch_checkpoint().latest_epoch() == Some(LeaderEpoch(1)));
        log.reset_to(Offset(1000)).unwrap(); // empty log starting at 1000
        assert2::assert!(log.epoch_checkpoint().entries().is_empty());
    }

    #[test]
    fn set_config_swaps_active_config() {
        let dir = tempdir().expect("tempdir");
        let log = Log::open(
            dir.path(),
            LogConfig {
                retention: Some(minutes(1)),
                ..LogConfig::default()
            },
        )
        .expect("open");
        log.set_config(LogConfig {
            retention: Some(minutes(2)),
            ..LogConfig::default()
        });
        assert2::assert!(log.config_snapshot().retention == Some(minutes(2)));
    }

    #[test]
    fn trim_to_offset_drops_old_segments() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_size: bytes(200), // small so we roll fast
                ..LogConfig::default()
            },
        )
        .expect("open");
        // Append 30 records to force multiple sealed segments.
        for _ in 0..30 {
            let mut b = sample_batch(1);
            log.append(&mut b).expect("append");
        }
        let leo = log.log_end_offset();
        let new_start = log.trim_to_offset(Offset(15)).expect("trim");
        // Trim clamps to next segment boundary <= target; new_start may
        // be less than 15 if 15 falls inside a sealed segment that we
        // can't drop without losing in-range records. LEO is unaffected.
        check!(new_start <= 15);
        check!(log.log_end_offset() == leo);
        // If target landed inside the active segment, log_start advanced
        // exactly to target. Otherwise it advanced to a sealed boundary.
        check!(log.log_start_offset() >= 0);
    }

    #[test]
    fn trim_to_offset_clamps_to_leo() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
        for _ in 0..3 {
            let mut b = sample_batch(1);
            log.append(&mut b).expect("append");
        }
        let leo = log.log_end_offset();
        let new_start = log.trim_to_offset(Offset(999)).expect("trim");
        // Asking to trim past LEO means trim to LEO.
        assert2::assert!(new_start == leo);
    }

    #[test]
    fn trim_to_offset_rejects_negative() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
        assert2::assert!(log.trim_to_offset(Offset(-5)).is_err());
    }

    #[test]
    fn trim_to_offset_idempotent_at_or_below_log_start() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open");
        for _ in 0..3 {
            let mut b = sample_batch(1);
            log.append(&mut b).expect("append");
        }
        // Trim to 0 on a fresh log → no change.
        let r = log.trim_to_offset(Offset(0)).expect("trim");
        assert2::assert!(r == log.log_start_offset());
    }

    fn keyed_batch(base: i64, items: &[(i32, &[u8], &[u8])]) -> RecordBatch {
        let records: Vec<Record> = items
            .iter()
            .map(|(d, k, v)| Record {
                offset_delta: *d,
                key: Some(Bytes::copy_from_slice(k)),
                value: Some(Bytes::copy_from_slice(v)),
                ..Default::default()
            })
            .collect();
        let last_delta = items.iter().map(|(d, _, _)| *d).max().unwrap_or(0);
        RecordBatch {
            base_offset: base,
            last_offset_delta: last_delta,
            max_timestamp: 0,
            records,
            ..RecordBatch::default()
        }
    }

    /// A `CompactionContext` with a fixed, deterministic epoch and no active
    /// producers. The in-crate compaction tests use it where tombstone and
    /// marker age are not under test.
    fn compaction_ctx() -> CompactionContext {
        CompactionContext {
            now: SystemTime::UNIX_EPOCH,
            active_producers: HashMap::new(),
        }
    }

    #[test]
    fn compact_no_op_when_only_one_segment() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        let mut b = keyed_batch(0, &[(0, b"k1", b"v1")]);
        log.append(&mut b).unwrap();
        // Only the active segment exists; sealed list is empty.
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == 1);
    }

    #[test]
    fn compact_dedupes_sealed_segments_keeps_active_intact() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256), // force rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Write 3 sealed segments, each with one record under "k1".
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
            // Roll the active segment by forcing a tick or a large pad batch.
            // Easiest: call set_segment_size or rely on the small segment_size.
        }
        // Add one more append to ensure the last write is in a fresh active
        // segment (not part of what compaction touches).
        let mut b = keyed_batch(0, &[(0, b"active-key", b"active-value")]);
        log.append(&mut b).unwrap();

        let active_leo_before = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        assert2::assert!(log.log_end_offset() == active_leo_before);

        // After compaction: read everything, assert only the newest k1 plus
        // the active "active-key" survive.
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let all_records: Vec<_> = out.batches.iter().flat_map(|b| b.records.iter()).collect();
        let keys: Vec<&[u8]> = all_records
            .iter()
            .map(|r| r.key.as_ref().unwrap().as_ref())
            .collect();
        assert2::assert!(keys.contains(&b"k1".as_ref()));
        assert2::assert!(keys.contains(&b"active-key".as_ref()));
    }

    /// Compaction must run and must not do nothing. Three sealed segments
    /// each carry a record under the SAME key "k1". After `compact`, exactly
    /// ONE k1 record must remain, the newest one, "v2". The sealed segment
    /// list must collapse to a single rewritten segment. A compaction that
    /// returned `Ok(())` at once would leave all three k1 records and three
    /// sealed segments.
    #[test]
    fn compact_actually_dedupes_reducing_record_count() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(1), // one batch per segment: every append exceeds this and rolls
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Three sealed segments, each one record under "k1" (v0, v1, v2).
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        // A final append lands in a fresh active segment (untouched by compact).
        let mut tail = keyed_batch(0, &[(0, b"tail", b"t")]);
        log.append(&mut tail).unwrap();

        // Sanity: before compaction there are >= 2 sealed segments holding the
        // three k1 versions.
        assert2::assert!(log.segments.len() >= 2);

        log.compact(&compaction_ctx()).unwrap();

        // Sealed segments collapse to exactly one rewritten segment.
        assert2::assert!(log.segments.len() == 1);

        // Exactly one surviving k1 record, and it is the newest value "v2".
        let out = log.read(Offset(0), mebibytes(1)).unwrap();
        let k1_values: Vec<&[u8]> = out
            .batches
            .iter()
            .flat_map(|b| b.records.iter())
            .filter(|r| r.key.as_deref() == Some(b"k1".as_ref()))
            .map(|r| r.value.as_deref().unwrap())
            .collect();
        assert2::assert!(k1_values == vec![b"v2".as_ref()]);
    }

    #[test]
    fn tierable_segments_excludes_active_and_reports_paths() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(200), // small so we roll fast
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..10 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        let sealed_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("log"))
            .count()
            - 1; // minus the active segment's .log

        let exports = log.tierable_segments();
        assert2::assert!(exports.len() == sealed_count);

        let active_base = log.log_end_offset(); // not literally, but exports must be below it
        let mut prev_last = Offset(-1);
        for ex in &exports {
            check!(ex.log_path.exists(), "log file present: {:?}", ex.log_path);
            check!(ex.offset_index_path.exists());
            check!(ex.time_index_path.exists());
            check!(ex.last_offset >= ex.base_offset);
            check!(ex.base_offset > prev_last, "segments are offset-ordered");
            prev_last = ex.last_offset;
            assert2::assert!(ex.last_offset < active_base);
        }
    }

    #[test]
    fn tierable_segments_empty_for_single_active_segment() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut b = sample_batch(3);
        log.append(&mut b).unwrap();
        // No roll happened: the only segment is active and never tierable.
        assert2::assert!(log.tierable_segments().is_empty());
    }

    #[test]
    fn tierable_segments_last_offset_matches_next_base() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(200),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for _ in 0..8 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        // Each sealed segment's last_offset is exactly one below the next
        // segment's base — contiguous coverage with no gaps.
        for pair in exports.windows(2) {
            assert2::assert!(pair[0].last_offset + 1 == pair[1].base_offset);
        }
    }

    #[test]
    fn tierable_segments_carry_leader_epochs() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(200),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // epoch 0 for the first few, then epoch 1.
        for _ in 0..4 {
            let mut b = sample_batch_with_epoch(2, 0);
            log.append(&mut b).unwrap();
        }
        for _ in 0..4 {
            let mut b = sample_batch_with_epoch(2, 1);
            log.append(&mut b).unwrap();
        }
        let exports = log.tierable_segments();
        assert2::assert!(!exports.is_empty());
        // Every export carries at least one epoch, and each recorded start
        // offset is clamped to >= the segment base.
        for ex in &exports {
            assert2::assert!(!ex.leader_epochs.is_empty());
            for (_epoch, start) in &ex.leader_epochs {
                assert2::assert!(*start >= ex.base_offset);
                assert2::assert!(*start <= ex.last_offset);
            }
        }
    }

    #[test]
    fn epochs_for_range_clamps_and_filters() {
        use crate::leader_epoch_checkpoint::EpochEntry;
        let entries = vec![
            EpochEntry {
                epoch: LeaderEpoch(0),
                start_offset: Offset(0),
            },
            EpochEntry {
                epoch: LeaderEpoch(1),
                start_offset: Offset(50),
            },
            EpochEntry {
                epoch: LeaderEpoch(2),
                start_offset: Offset(100),
            },
        ];
        for (name, start, end, want) in [
            // Segment [60, 90] sits entirely in epoch 1.
            (
                "within one epoch",
                Offset(60),
                Offset(90),
                vec![(LeaderEpoch(1), Offset(60))],
            ),
            // Segment [40, 60] straddles epoch 0 (->clamped to 40) and epoch 1.
            (
                "straddles epochs",
                Offset(40),
                Offset(60),
                vec![(LeaderEpoch(0), Offset(40)), (LeaderEpoch(1), Offset(50))],
            ),
            // Segment [0, 200] covers all three.
            (
                "covers all epochs",
                Offset(0),
                Offset(200),
                vec![
                    (LeaderEpoch(0), Offset(0)),
                    (LeaderEpoch(1), Offset(50)),
                    (LeaderEpoch(2), Offset(100)),
                ],
            ),
        ] {
            check!(
                epochs_for_range(&entries, start, end) == want,
                "case {name}: range [{}, {}]",
                start.0,
                end.0
            );
        }
        // No entries -> empty.
        assert2::assert!(epochs_for_range(&[], Offset(0), Offset(100)).is_empty());
    }

    #[test]
    fn compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let cfg = LogConfig {
            cleanup_policy: crate::CleanupPolicy::Compact,
            segment_size: bytes(256),
            ..Default::default()
        };
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        let mut b = keyed_batch(0, &[(0, b"active", b"x")]);
        log.append(&mut b).unwrap();
        log.compact(&compaction_ctx()).unwrap();
        let leo1 = log.log_end_offset();
        log.compact(&compaction_ctx()).unwrap();
        let leo2 = log.log_end_offset();
        assert2::assert!(leo1 == leo2);
    }

    // ---- Local-retention helpers (KIP-405) ----

    /// Build a log rolled into several sealed segments under `dir`. This
    /// mirrors the `remote_log_manager` test helper and stays local to this
    /// module.
    fn rolled_log(dir: &std::path::Path, extra: &LogConfig) -> Log {
        let mut log = Log::open(
            dir,
            LogConfig {
                segment_size: bytes(200),
                ..extra.clone()
            },
        )
        .unwrap();
        for _ in 0..16 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        log
    }

    #[test]
    fn local_log_start_offset_matches_log_start_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for _ in 0..3 {
            let mut b = sample_batch(2);
            log.append(&mut b).unwrap();
        }
        assert2::assert!(log.local_log_start_offset() == log.log_start_offset());
    }

    #[test]
    fn delete_local_segments_through_drops_sealed_below_target() {
        let dir = tempdir().unwrap();
        let mut log = rolled_log(dir.path(), &LogConfig::default());
        let exports = log.tierable_segments();
        assert2::assert!(exports.len() >= 3);

        // Pick a target strictly between two sealed-segment boundaries:
        // one past the second sealed segment's last_offset. Every sealed
        // segment whose last_offset < target should be deleted.
        let target = exports[1].last_offset + 1;
        let expected_deleted: Vec<Offset> = exports
            .iter()
            .filter(|e| e.last_offset < target)
            .map(|e| e.base_offset)
            .collect();
        let active_base_before = log.log_end_offset();

        let removed = log.delete_local_segments_through(target).unwrap();
        assert2::assert!(removed == expected_deleted.len());

        // (a) sealed segments below target are gone from the in-memory list.
        let remaining_bases: Vec<Offset> = log
            .tierable_segments()
            .iter()
            .map(|e| e.base_offset)
            .collect();
        for base in &expected_deleted {
            assert2::assert!(!remaining_bases.contains(base));
        }

        // (b) on-disk files for deleted segments are gone.
        for base in &expected_deleted {
            check!(!name::log_path(dir.path(), base.0).exists());
            check!(!name::index_path(dir.path(), base.0).exists());
            check!(!name::timeindex_path(dir.path(), base.0).exists());
        }

        // (c) the active segment is untouched.
        assert2::assert!(log.log_end_offset() == active_base_before);
    }

    #[test]
    fn delete_local_segments_through_keeps_active_segment() {
        let dir = tempdir().unwrap();
        let mut log = rolled_log(dir.path(), &LogConfig::default());
        let leo_before = log.log_end_offset();
        let active_log = dir.path().join(format!(
            "{:020}.log",
            log.tierable_segments().last().unwrap().last_offset + 1
        ));
        // The active segment's .log file should exist before and after.
        assert2::assert!(active_log.exists());

        // First: target far beyond every sealed segment but well past
        // active.base_offset. The active segment must not be removed.
        let huge_target = leo_before + 1_000_000;
        let _ = log.delete_local_segments_through(huge_target).unwrap();
        check!(active_log.exists(), "active segment must survive");
        check!(
            log.log_end_offset() == leo_before,
            "active segment untouched (LEO unchanged)"
        );
        // Sealed-segment pointer should have advanced past everything.
        check!(log.tierable_segments().is_empty());
    }

    #[test]
    fn delete_local_segments_through_advances_local_start_pointer() {
        let dir = tempdir().unwrap();
        let mut log = rolled_log(dir.path(), &LogConfig::default());
        let exports = log.tierable_segments();
        let target = exports[1].last_offset + 1;
        log.delete_local_segments_through(target).unwrap();
        assert2::assert!(log.local_log_start_offset() == target);
        assert2::assert!(log.log_start_offset() == target);
    }

    #[test]
    fn delete_local_segments_through_is_noop_at_or_below_current_start() {
        let dir = tempdir().unwrap();
        let mut log = rolled_log(dir.path(), &LogConfig::default());
        let start_before = log.log_start_offset();
        let sealed_before = log.tierable_segments().len();

        let removed = log.delete_local_segments_through(start_before).unwrap();
        assert2::assert!(removed == 0);
        let removed_below = log
            .delete_local_segments_through((start_before - 1).max(Offset(0)))
            .unwrap();
        assert2::assert!(removed_below == 0);
        assert2::assert!(log.log_start_offset() == start_before);
        assert2::assert!(log.tierable_segments().len() == sealed_before);
    }

    #[test]
    fn delete_local_segments_through_rejects_negative_target() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let err = log.delete_local_segments_through(Offset(-1)).unwrap_err();
        assert2::assert!(matches!(err, LogError::InvalidArgument(_)));
    }

    #[test]
    fn tick_rolls_but_skips_retention_when_remote_storage_enable_is_true() {
        use std::time::Duration;
        let far_future = SystemTime::now() + Duration::from_hours(365 * 24);

        // Tiered topic: tick rolls the old active segment but must not delete
        // any segment. The remote-log manager owns local eviction.
        let dir_tiered = tempdir().unwrap();
        let mut tiered = rolled_log(
            dir_tiered.path(),
            &LogConfig {
                remote_storage_enable: true,
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        let sealed_before = tiered.tierable_segments().len();
        assert2::assert!(sealed_before > 0);
        tiered.tick(far_future).unwrap();
        assert2::assert!(tiered.tierable_segments().len() == sealed_before + 1);

        // Non-tiered baseline: tick should still evict aggressively.
        let dir_plain = tempdir().unwrap();
        let mut plain = rolled_log(
            dir_plain.path(),
            &LogConfig {
                remote_storage_enable: false,
                retention: Some(millis(1)),
                ..LogConfig::default()
            },
        );
        assert2::assert!(!plain.tierable_segments().is_empty());
        plain.tick(far_future).unwrap();
        // Non-tiered path keeps at least one segment (the active one); every
        // sealed segment is evicted.
        assert2::assert!(plain.tierable_segments().len() == 0);
    }

    fn ts_batch(ts: i64) -> RecordBatch {
        let mut b = RecordBatch {
            base_offset: 0, // overwritten by Log::append
            base_timestamp: ts,
            max_timestamp: ts,
            last_offset_delta: 0,
            ..RecordBatch::default()
        };
        b.records.push(Record {
            offset_delta: 0,
            timestamp_delta: 0,
            value: Some(Bytes::from("v")),
            ..Default::default()
        });
        b
    }

    #[test]
    fn log_offset_for_timestamp_across_segments() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1), // roll after every batch → each record its own segment
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // offsets 0..=4 with timestamps 100,200,300,400,500.
        for (_name, i, ts) in [
            ("first", 0, 100),
            ("second", 1, 200),
            ("third", 2, 300),
            ("fourth", 3, 400),
            ("fifth", 4, 500),
        ] {
            let mut b = ts_batch(ts);
            assert2::assert!(log.append(&mut b).unwrap() == Offset(i));
        }
        for (name, ts, want) in [
            // before-first → offset 0.
            ("before first", 50, Some((Offset(0), 100))),
            // exact match on a sealed segment.
            ("exact sealed", 300, Some((Offset(2), 300))),
            // between records → next record up.
            ("between records", 350, Some((Offset(3), 400))),
            // landing on the active segment's record.
            ("active record", 500, Some((Offset(4), 500))),
            // after-last → None.
            ("after last", 600, None),
        ] {
            check!(log.offset_for_timestamp(ts) == want, "case {name}: ts={ts}");
        }
        log.close();
        drop(dir);
    }

    #[test]
    fn configured_io_policy_reaches_reads_and_timestamp_scans() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                read_buffer_cap: bytes(1),
                timestamp_scan_window: bytes(1),
                ..LogConfig::default()
            },
        )
        .unwrap();
        let mut batch = ts_batch(100);
        log.append(&mut batch).unwrap();

        assert2::assert!(
            !log.read_raw(Offset(0), Offset(1), kibibytes(1))
                .unwrap()
                .bytes
                .is_empty()
        );
        assert2::assert!(log.offset_for_timestamp(100) == Some((Offset(0), 100)));
    }

    #[test]
    fn log_offset_for_timestamp_empty_log_is_none() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.offset_for_timestamp(0) == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_offset_of_max_timestamp_in_active() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1), // each record its own segment
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        // timestamps 100,300,200 at offsets 0,1,2. Max is 300 @ offset 1.
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        assert2::assert!(log.offset_of_max_timestamp() == 1);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_offset_of_max_timestamp_empty_is_log_start() {
        let dir = tempdir().unwrap();
        let log = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!(log.offset_of_max_timestamp() == log.log_start_offset());
        assert2::assert!(log.max_timestamp_offset_and_ts() == None);
        log.close();
        drop(dir);
    }

    #[test]
    fn log_max_timestamp_offset_and_ts_returns_pair() {
        let dir = tempdir().unwrap();
        let config = LogConfig {
            segment_size: bytes(1),
            ..LogConfig::default()
        };
        let mut log = Log::open(dir.path(), config).unwrap();
        for ts in [100, 300, 200] {
            let mut b = ts_batch(ts);
            log.append(&mut b).unwrap();
        }
        // Max timestamp 300 lives at offset 1.
        assert2::assert!(log.max_timestamp_offset_and_ts() == Some((Offset(1), 300)));
        log.close();
        drop(dir);
    }
}
