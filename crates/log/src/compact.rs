//! Log compaction primitives. Pure-ish helpers that operate on
//! [`Segment`] handles and the on-disk file layout, used by
//! [`crate::Log::compact`].
//!
//! The algorithm is single-pass over the **sealed** segment list,
//! oldest-to-newest, building a key→latest-offset map and then
//! rewriting the surviving records into a single new segment at the
//! lowest input base offset. The active segment is never touched.
//!
//! Records with `key.is_none()` are dropped (matches Kafka's
//! `LogCleaner`). Tombstones (records with `key.is_some()` and
//! `value.is_none()`) are treated like any other value and are kept
//! as the most-recent entry for their key. `delete.retention.ms`
//! ages them out.

use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::{Bytes, BytesMut};
use crabka_protocol::records::RecordBatch;
use tracing::instrument;

use crate::error::LogError;
use crate::name;
use crate::segment::Segment;
use crate::txn_index::{AbortedTxn, TxnIndex};

// ---------------------------------------------------------------------------
// KIP-534 pure decision cores
//
// These are `pub(crate)` (reachable from a sibling test module via `super::`)
// because a later task builds a stateright model + proptest on them. The
// retain/horizon logic lives here in pure form so it can be exhaustively
// model-checked without touching the filesystem.
// ---------------------------------------------------------------------------

/// Per-record facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecordMeta {
    pub has_key: bool,
    pub has_value: bool,
}

/// Per-batch facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchMeta {
    pub is_control: bool,
    pub producer_id: i64,
    /// The batch's existing delete horizon (`base_timestamp` when bit 6 is
    /// set), `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Whether a producer's transactional DATA still survives compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TxnDataState {
    /// `producer_id < 0`: not a transactional producer.
    NotTransactional,
    /// At least one of this producer's data records survives compaction.
    DataSurvives,
    /// All of this producer's data records have been compacted away.
    DataFullyGone,
}

/// What to do with a record during the rewrite pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetainDecision {
    /// Keep the record as-is.
    Keep,
    /// Keep the record but stamp its batch with this delete horizon
    /// (`base_timestamp = horizon`, bit 6 set).
    SetHorizon(i64),
    /// Drop the record.
    Delete,
}

/// `build_offset_map` filter; the control-batch bug fix lives here. Control-batch
/// records carry a control-type key (commit/abort marker) that must NEVER enter
/// the dedup map. Null-key data is also never indexed.
pub(crate) fn should_index_key(key: Option<&[u8]>, is_control_batch: bool) -> bool {
    !is_control_batch && key.is_some()
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`. The
/// tombstone/marker is retained until wall-clock reaches this value.
pub(crate) fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    now_ms.saturating_add(delete_retention_ms)
}

/// Reinterpret per-record timestamp deltas (`i64`) when stamping a delete
/// horizon into `base_timestamp`, preserving each record's absolute timestamp.
///
/// Exercised by `core_tests` and the upcoming stateright/proptest model; the
/// production rewrite path delegates the same arithmetic to
/// `RecordBatch::with_delete_horizon`, hence `dead_code` outside tests.
#[allow(dead_code)]
pub(crate) fn rewrite_batch_horizon(
    base_timestamp: i64,
    deltas: &[i64],
    horizon: i64,
) -> (i64, Vec<i64>) {
    let new = deltas
        .iter()
        .map(|d| base_timestamp.saturating_add(*d).saturating_sub(horizon))
        .collect();
    (horizon, new)
}

/// Whether a transactional producer's data is fully compacted away: the
/// `producer_id` is not in the `survivors` set of producers with a
/// surviving data record.
///
/// The production rewrite path uses [`CleanedTransactionMetadata::txn_state`]
/// (which folds this in); this standalone form exists for `core_tests` and
/// the upcoming stateright/proptest model.
#[allow(dead_code)]
pub(crate) fn txn_data_fully_gone(producer_id: i64, survivors: &HashSet<i64>) -> bool {
    !survivors.contains(&producer_id)
}

/// The single per-record KIP-534 retain decision.
///
/// Control batches (txn commit/abort markers) are retained as long as their
/// transaction's data survives; once the data is fully compacted away the
/// marker ages out via the delete horizon. Data records dedup newest-wins;
/// tombstones (null value) age out via the delete horizon once they are the
/// newest entry for their key.
pub(crate) fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    if batch.is_control {
        return match txn {
            TxnDataState::DataSurvives | TxnDataState::NotTransactional => RetainDecision::Keep,
            TxnDataState::DataFullyGone => match batch.existing_horizon {
                Some(h) if now_ms >= h => RetainDecision::Delete,
                Some(_) => RetainDecision::Keep,
                None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
            },
        };
    }
    if !rec.has_key {
        return RetainDecision::Delete;
    }
    if !is_newest_for_key {
        return RetainDecision::Delete;
    }
    if rec.has_value {
        return RetainDecision::Keep;
    }
    // Newest-for-key tombstone: age out via the delete horizon.
    match batch.existing_horizon {
        Some(h) if now_ms >= h => RetainDecision::Delete,
        Some(_) => RetainDecision::Keep,
        None => RetainDecision::SetHorizon(compute_horizon(now_ms, delete_retention_ms)),
    }
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod core_tests {
    use super::*;
    use assert2::assert;
    use assert2::check;

    fn data(has_key: bool, has_value: bool) -> RecordMeta {
        RecordMeta { has_key, has_value }
    }

    fn batch(is_control: bool, producer_id: i64, existing_horizon: Option<i64>) -> BatchMeta {
        BatchMeta {
            is_control,
            producer_id,
            existing_horizon,
        }
    }

    #[test]
    fn control_batch_key_is_never_indexed() {
        // A control batch's key (commit/abort marker) must NOT enter the
        // dedup map, regardless of whether the key is present.
        for (key, is_control, want) in [
            (Some(b"\x00\x00\x00\x01".as_ref()), true, false),
            // Null-key data is also never indexed.
            (None, false, false),
            // Ordinary keyed data IS indexed.
            (Some(b"k".as_ref()), false, true),
        ] {
            check!(
                should_index_key(key, is_control) == want,
                "key={key:?} is_control={is_control}"
            );
        }
    }

    #[test]
    fn tombstone_sets_horizon_then_deletes_after_expiry() {
        let rec = data(true, false); // keyed, null value (tombstone)
        for (existing_horizon, is_newest, now_ms, want) in [
            // Newest tombstone, no existing horizon: stamp now+ret = 100+50 = 150.
            (None, true, 100, RetainDecision::SetHorizon(150)),
            // Now=149 < horizon 150: keep.
            (Some(150), true, 149, RetainDecision::Keep),
            // Now=150 >= horizon 150: delete.
            (Some(150), true, 150, RetainDecision::Delete),
            // Superseded tombstone (not newest-for-key): delete outright.
            (None, false, 100, RetainDecision::Delete),
        ] {
            check!(
                retain_decision(
                    rec,
                    batch(false, -1, existing_horizon),
                    is_newest,
                    TxnDataState::NotTransactional,
                    now_ms,
                    50
                ) == want,
                "horizon={existing_horizon:?} newest={is_newest} now={now_ms}"
            );
        }
    }

    #[test]
    fn marker_retained_while_data_survives_then_ages() {
        let marker = data(true, false); // control records carry a key, no value
        for (existing_horizon, txn_state, now_ms, want) in [
            // Data still survives: keep the marker.
            (None, TxnDataState::DataSurvives, 100, RetainDecision::Keep),
            // Data fully gone, no horizon yet: stamp now+ret = 100+50 = 150.
            (
                None,
                TxnDataState::DataFullyGone,
                100,
                RetainDecision::SetHorizon(150),
            ),
            // Data fully gone, horizon 150, now 150: delete.
            (
                Some(150),
                TxnDataState::DataFullyGone,
                150,
                RetainDecision::Delete,
            ),
        ] {
            check!(
                retain_decision(
                    marker,
                    batch(true, 1000, existing_horizon),
                    false,
                    txn_state,
                    now_ms,
                    50
                ) == want,
                "horizon={existing_horizon:?} now={now_ms}"
            );
        }
    }

    #[test]
    fn live_data_kept_nullkey_dropped() {
        for (has_key, is_newest, want) in [
            // Newest-for-key data with a value: keep.
            (true, true, RetainDecision::Keep),
            // Null-key data: dropped regardless of newest-ness.
            (false, true, RetainDecision::Delete),
            // Keyed data with a value but not newest-for-key: dropped.
            (true, false, RetainDecision::Delete),
        ] {
            check!(
                retain_decision(
                    data(has_key, true),
                    batch(false, -1, None),
                    is_newest,
                    TxnDataState::NotTransactional,
                    100,
                    50
                ) == want,
                "has_key={has_key} newest={is_newest}"
            );
        }
    }

    #[test]
    fn rewrite_batch_horizon_preserves_absolute_timestamps() {
        let (base, deltas) = rewrite_batch_horizon(1000, &[0, 5, 20], 9999);
        assert!(base == 9999);
        // Reconstructed absolute timestamps (base + delta) must equal the
        // originals: 1000, 1005, 1020.
        let reconstructed: Vec<i64> = deltas.iter().map(|d| base + d).collect();
        assert!(reconstructed == vec![1000, 1005, 1020]);
    }

    #[test]
    fn txn_data_fully_gone_checks_survivor_set() {
        let mut survivors = HashSet::new();
        survivors.insert(1000i64);
        assert!(txn_data_fully_gone(2000, &survivors) == true);
        assert!(txn_data_fully_gone(1000, &survivors) == false);
    }
}

/// Read every `RecordBatch` from a sealed segment by streaming the
/// whole `.log` file directly. We bypass `Segment::read` because that
/// path early-returns when the segment's in-memory `last_offset` is
/// stale (sealed segments loaded from disk via `Segment::open` have
/// `last_offset = base_offset - 1` until a tail-scan populates it,
/// and `Segment::read(base_offset, ..)` would short-circuit to empty).
fn read_all_batches(seg: &Segment) -> Result<Vec<RecordBatch>, LogError> {
    let path = name::log_path(seg.dir(), seg.base_offset());
    let bytes = std::fs::read(&path)?;
    let mut cursor: &[u8] = &bytes;
    let mut out: Vec<RecordBatch> = Vec::new();
    while !cursor.is_empty() {
        let Ok(batch) = RecordBatch::decode(&mut cursor) else {
            break;
        };
        out.push(batch);
    }
    Ok(out)
}

/// Build a map of `key → latest absolute offset` across the given
/// sealed segments in input order. Records with `key.is_none()` are
/// excluded (they will be dropped by [`rewrite_segments`]).
///
/// The map's value is the absolute offset of the **newest** record
/// observed for each key (later writes overwrite earlier ones).
#[instrument(
    level = "debug",
    skip_all,
    fields(segments = segments.len(), keys = tracing::field::Empty),
    err,
)]
pub fn build_offset_map(segments: &[&Segment]) -> Result<HashMap<Bytes, i64>, LogError> {
    // Keyed by `Bytes` (cheap refcounted clone of the record key) rather
    // than `Vec<u8>` to avoid a heap copy of every key. Zero-length keys
    // are legal in Kafka and dedup as a distinct "empty key" like any other.
    let mut map: HashMap<Bytes, i64> = HashMap::new();
    for seg in segments {
        for batch in read_all_batches(seg)? {
            // Control batches (txn commit/abort markers) carry a control-type
            // key that must NEVER enter the dedup map. Skip them entirely —
            // indexing their key silently dropped all-but-newest markers and
            // broke read_committed (the control-batch data-loss bug).
            if batch.attributes.is_control_batch() {
                continue;
            }
            for record in &batch.records {
                if !should_index_key(record.key.as_deref(), false) {
                    continue;
                }
                let key_bytes = record.key.as_ref().expect("should_index_key checked Some");
                let absolute = batch.base_offset + i64::from(record.offset_delta);
                map.insert(key_bytes.clone(), absolute);
            }
        }
    }
    tracing::Span::current().record("keys", map.len());
    Ok(map)
}

/// Per-producer transactional-data survival, computed in a first pass over
/// the sealed segments. KIP-534 keeps a transaction's commit/abort marker as
/// long as any of that transaction's data records survive compaction; once
/// the data is fully gone the marker ages out via the delete horizon.
///
/// Seeded with aborted-txn entries from each sealed segment's `.txnindex` so
/// the rewritten survivor `.txnindex` can be reconstructed for transactions
/// whose data still partially survives.
pub struct CleanedTransactionMetadata {
    /// Producers (`producer_id`) with at least one surviving data record.
    survivors: HashSet<i64>,
    /// Aborted-txn entries gathered from the consumed segments' `.txnindex`
    /// files, in input order.
    aborted: Vec<AbortedTxn>,
}

impl CleanedTransactionMetadata {
    /// Build the metadata: for each producer, whether any of its
    /// transactional DATA records will survive (a data record that is
    /// newest-for-key per `offset_map`). Aborted-txn entries are seeded from
    /// every sealed segment's `.txnindex`.
    #[instrument(
        level = "debug",
        skip_all,
        fields(segments = segments.len(), survivors = tracing::field::Empty),
        err,
    )]
    pub fn build(
        segments: &[&Segment],
        offset_map: &HashMap<Bytes, i64>,
    ) -> Result<Self, LogError> {
        let mut survivors: HashSet<i64> = HashSet::new();
        let mut aborted: Vec<AbortedTxn> = Vec::new();
        for seg in segments {
            // Seed aborted-txn entries from this segment's transaction index.
            let idx = TxnIndex::open(seg.txn_index_path())?;
            aborted.extend(idx.entries().iter().copied());

            for batch in read_all_batches(seg)? {
                // Only data batches contribute survivors. Control batches
                // carry no data records.
                if batch.attributes.is_control_batch() {
                    continue;
                }
                // Only transactional producers (producer_id >= 0) matter for
                // marker retention.
                if batch.producer_id < 0 {
                    continue;
                }
                for record in &batch.records {
                    // A surviving data record is one that is newest-for-key.
                    let Some(key_bytes) = record.key.as_ref() else {
                        continue;
                    };
                    let absolute = batch.base_offset + i64::from(record.offset_delta);
                    if offset_map.get(key_bytes.as_ref()).copied() == Some(absolute) {
                        survivors.insert(batch.producer_id);
                        break;
                    }
                }
            }
        }
        tracing::Span::current().record("survivors", survivors.len());
        Ok(Self { survivors, aborted })
    }

    /// The transactional-data state for a given producer.
    #[must_use]
    pub fn txn_state(&self, producer_id: i64) -> TxnDataState {
        if producer_id < 0 {
            return TxnDataState::NotTransactional;
        }
        if self.survivors.contains(&producer_id) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }

    /// Aborted-txn entries to carry forward into the rewritten survivor
    /// `.txnindex`: those whose aborted data still partially survives (the
    /// producer is in the survivor set). Entries for producers whose data is
    /// fully gone are dropped along with the (now-removable) marker.
    fn retained_aborted(&self) -> impl Iterator<Item = &AbortedTxn> {
        self.aborted
            .iter()
            .filter(move |e| self.survivors.contains(&e.producer_id))
    }
}

#[cfg(test)]
#[allow(
    clippy::similar_names,
    clippy::redundant_closure,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
mod build_map_tests {
    use super::*;
    use assert2::assert;
    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record};
    use tempfile::tempdir;

    pub(super) fn make_record(
        offset_delta: i32,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Record {
        Record {
            offset_delta,
            key: key.map(Bytes::copy_from_slice),
            value: value.map(Bytes::copy_from_slice),
            ..Default::default()
        }
    }

    pub(super) fn write_sealed_segment(
        dir: &Path,
        base_offset: i64,
        records: Vec<Record>,
    ) -> Segment {
        let mut seg = Segment::create(dir, base_offset).unwrap();
        let n = i32::try_from(records.len()).expect("record count fits i32");
        let max_ts = records.iter().map(|r| r.timestamp_delta).max().unwrap_or(0);
        let batch = RecordBatch {
            base_offset,
            last_offset_delta: n - 1,
            max_timestamp: max_ts,
            records,
            attributes: Attributes::default(),
            ..RecordBatch::default()
        };
        seg.append(&batch, 4096).unwrap();
        seg.seal();
        seg
    }

    /// Write a sealed segment containing the given batches verbatim
    /// (`base_offset`/attributes/`producer_id` preserved). Lets tests build
    /// control batches and mixed data/control layouts.
    pub(super) fn write_sealed_batches(dir: &Path, batches: &[RecordBatch]) -> Segment {
        let base = batches.first().map_or(0, |b| b.base_offset);
        let mut seg = Segment::create(dir, base).unwrap();
        for batch in batches {
            seg.append(batch, 4096).unwrap();
        }
        seg.seal();
        seg
    }

    /// A control batch carrying a single commit/abort marker record. The
    /// marker key is `(version: i16, marker_type: i16)` big-endian.
    pub(super) fn control_batch(
        base_offset: i64,
        producer_id: i64,
        marker_type: i16,
    ) -> RecordBatch {
        let mut key = [0u8; 4];
        key[2..4].copy_from_slice(&marker_type.to_be_bytes());
        RecordBatch {
            base_offset,
            last_offset_delta: 0,
            producer_id,
            attributes: Attributes::default()
                .with_transactional(true)
                .with_control(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(&key)),
                ..Default::default()
            }],
            ..RecordBatch::default()
        }
    }

    #[test]
    fn control_batch_key_is_not_indexed() {
        let dir = tempdir().unwrap();
        // A control batch (commit marker) at offset 0, then keyed data at
        // offset 1. Only the data key should appear in the map; the control
        // marker's key must be absent.
        let mut data = RecordBatch {
            base_offset: 1,
            last_offset_delta: 0,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            attributes: Attributes::default(),
            ..RecordBatch::default()
        };
        data.records[0].offset_delta = 0;
        let seg = write_sealed_batches(dir.path(), &[control_batch(0, 1000, 1 /* COMMIT */), data]);
        let segs: Vec<&Segment> = vec![&seg];
        let map = build_offset_map(&segs).unwrap();
        // The data key is present.
        assert!(map.get(b"k1".as_ref()) == Some(&1));
        // The control-marker key (\x00\x00\x00\x01) is NOT present.
        let marker_key: &[u8] = &[0, 0, 0, 1];
        assert!(map.get(marker_key) == None);
        assert!(map.len() == 1);
    }

    #[test]
    fn build_offset_map_keeps_newest_offset_per_key() {
        let dir = tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v2")),
                make_record(2, Some(b"k1"), Some(b"v3")), // k1 overwritten
            ],
        );
        let segs: Vec<&Segment> = vec![&seg0];
        let map = build_offset_map(&segs).unwrap();
        assert!(map.get(b"k1".as_ref()) == Some(&2));
        assert!(map.get(b"k2".as_ref()) == Some(&1));
    }

    #[test]
    fn build_offset_map_drops_null_key_records() {
        let dir = tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, None, Some(b"no-key-1")),
                make_record(1, Some(b"k1"), Some(b"v1")),
                make_record(2, None, Some(b"no-key-2")),
            ],
        );
        let segs: Vec<&Segment> = vec![&seg0];
        let map = build_offset_map(&segs).unwrap();
        assert!(map.len() == 1);
        assert!(map.get(b"k1".as_ref()) == Some(&1));
    }

    #[test]
    fn build_offset_map_across_segments_uses_newest() {
        let dir = tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        );
        let seg1 = write_sealed_segment(
            dir.path(),
            10,
            vec![make_record(0, Some(b"k1"), Some(b"v2"))],
        );
        let segs: Vec<&Segment> = vec![&seg0, &seg1];
        let map = build_offset_map(&segs).unwrap();
        assert!(map.get(b"k1".as_ref()) == Some(&10));
    }
}

/// Result of [`rewrite_segments`]: paths to the three `.swap` files
/// that should be promoted by [`atomic_swap`].
pub struct RewriteOutput {
    pub log_swap: PathBuf,
    pub index_swap: PathBuf,
    pub timeindex_swap: PathBuf,
    /// `base_offset` of the new segment (== lowest input segment).
    pub new_base_offset: i64,
    /// Highest absolute offset of any surviving record.
    #[allow(dead_code)]
    pub new_last_offset: i64,
    /// Path to the rewritten survivor `.txnindex`, written only when any
    /// aborted-txn entries were carried forward. `None` when no aborted
    /// transactions survive.
    pub txnindex_swap: Option<PathBuf>,
}

/// Stream `segments` (oldest → newest) into new `.swap` files, applying the
/// KIP-534 per-record [`retain_decision`].
///
/// For each record the decision is:
///   - `Keep` → write it through.
///   - `SetHorizon(h)` → write it through, and stamp the output batch with
///     delete horizon `h` (bit 6 set, `base_timestamp = h`).
///   - `Delete` → drop it.
///
/// Records keep their **absolute** offsets — the output `RecordBatch`es may
/// contain gaps in their `offset_delta` values where superseded records used
/// to live. This matches Kafka's on-disk format for compacted topics.
///
/// `RETAIN_EMPTY`: a batch that ends up with no kept records is normally
/// skipped, but it is re-emitted as a bare header (no records) when it is the
/// last batch of an active producer (`active_producers`) or the last batch of
/// the consolidated output — preserving producer sequence/epoch and the
/// log-end offset (Kafka's `retainEmpty`).
///
/// The `.swap` files are written to the segments' shared directory. Caller is
/// responsible for fsyncing + promoting via [`atomic_swap`].
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
#[instrument(
    level = "info",
    skip_all,
    fields(
        dir = %dir.display(),
        segments = segments.len(),
        new_base = tracing::field::Empty,
        new_last_offset = tracing::field::Empty,
    ),
    err,
)]
pub fn rewrite_segments(
    dir: &Path,
    segments: &[&Segment],
    offset_map: &HashMap<Bytes, i64>,
    txn_meta: &CleanedTransactionMetadata,
    now_ms: i64,
    delete_retention_ms: i64,
    active_producers: &HashMap<i64, i64>,
    _index_interval_bytes: u32,
) -> Result<RewriteOutput, LogError> {
    let first = segments
        .first()
        .ok_or_else(|| LogError::Io(std::io::Error::other("rewrite_segments: empty input")))?;
    let new_base = first.base_offset();
    tracing::Span::current().record("new_base", new_base);

    let log_swap = swap_path(dir, new_base, "log");
    let index_swap = swap_path(dir, new_base, "index");
    let timeindex_swap = swap_path(dir, new_base, "timeindex");

    // Truncate (or create) all three swap files. We rewrite the .log
    // file proper here; for the index sidecars we write empty files
    // and let Segment::open populate them via tail-scan in the recovery
    // promotion path. (Sparse indexes are derivable from the .log; an
    // empty index is correct and small.)
    let mut log_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&log_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&index_swap)?;
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&timeindex_swap)?;

    // Flatten all batches across all segments so we can identify the last
    // batch (for RETAIN_EMPTY) and the last batch per active producer.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    for seg in segments {
        all_batches.extend(read_all_batches(seg)?);
    }
    let last_batch_index = all_batches.len().saturating_sub(1);
    // The index of each active producer's last batch in `all_batches`.
    let mut producer_last_batch: HashMap<i64, usize> = HashMap::new();
    for (i, batch) in all_batches.iter().enumerate() {
        if active_producers.contains_key(&batch.producer_id) {
            producer_last_batch.insert(batch.producer_id, i);
        }
    }

    let mut last_kept_offset = new_base - 1;

    for (batch_idx, batch) in all_batches.iter().enumerate() {
        let is_control = batch.attributes.is_control_batch();
        let txn = txn_meta.txn_state(batch.producer_id);
        let batch_meta = BatchMeta {
            is_control,
            producer_id: batch.producer_id,
            existing_horizon: batch.delete_horizon_ms(),
        };

        let mut kept: Vec<crabka_protocol::records::Record> =
            Vec::with_capacity(batch.records.len());
        // Stamp the output batch with a delete horizon if any record's
        // decision asks for it (stamp once per batch).
        let mut stamp_horizon: Option<i64> = None;
        for record in &batch.records {
            let absolute = batch.base_offset + i64::from(record.offset_delta);
            let is_newest_for_key = record
                .key
                .as_ref()
                .is_some_and(|k| offset_map.get(k.as_ref()).copied() == Some(absolute));
            let rec_meta = RecordMeta {
                has_key: record.key.is_some(),
                has_value: record.value.is_some(),
            };
            match retain_decision(
                rec_meta,
                batch_meta,
                is_newest_for_key,
                txn,
                now_ms,
                delete_retention_ms,
            ) {
                RetainDecision::Keep => kept.push(record.clone()),
                RetainDecision::SetHorizon(h) => {
                    kept.push(record.clone());
                    stamp_horizon = Some(h);
                }
                RetainDecision::Delete => {}
            }
        }

        if kept.is_empty() {
            // RETAIN_EMPTY: re-emit a bare header for an emptied batch when
            // it is the last batch of an active producer or the last batch
            // of the consolidated output, so producer sequence/epoch and the
            // log-end offset survive.
            let is_producer_last =
                producer_last_batch.get(&batch.producer_id).copied() == Some(batch_idx);
            let is_output_last = batch_idx == last_batch_index;
            if !(is_producer_last || is_output_last) {
                continue;
            }
            let out_batch = RecordBatch {
                base_offset: batch.base_offset,
                last_offset_delta: batch.last_offset_delta,
                max_timestamp: batch.max_timestamp,
                base_timestamp: batch.base_timestamp,
                attributes: batch.attributes,
                producer_id: batch.producer_id,
                producer_epoch: batch.producer_epoch,
                base_sequence: batch.base_sequence,
                partition_leader_epoch: batch.partition_leader_epoch,
                records: vec![],
            };
            let mut buf = BytesMut::with_capacity(out_batch.encoded_len());
            out_batch.encode(&mut buf)?;
            log_file.write_all(&buf)?;
            let batch_last = out_batch.base_offset + i64::from(out_batch.last_offset_delta);
            if batch_last > last_kept_offset {
                last_kept_offset = batch_last;
            }
            continue;
        }

        // Compute new last_offset_delta covering the kept range (relative to
        // the batch's original base_offset). Kafka preserves base_offset and
        // only updates last_offset_delta when records are removed mid-batch.
        let last_delta = kept
            .iter()
            .map(|r| r.offset_delta)
            .max()
            .expect("kept non-empty");
        let mut out_batch = RecordBatch {
            base_offset: batch.base_offset,
            last_offset_delta: last_delta,
            max_timestamp: batch.max_timestamp,
            attributes: batch.attributes,
            records: kept,
            ..batch.clone()
        };
        // Stamp the delete horizon once, after the kept batch is built. This
        // rewrites each kept record's timestamp_delta so absolute timestamps
        // are preserved (see `RecordBatch::with_delete_horizon`).
        if let Some(h) = stamp_horizon {
            out_batch = out_batch.with_delete_horizon(h);
        }

        let mut buf = BytesMut::with_capacity(out_batch.encoded_len());
        out_batch.encode(&mut buf)?;
        log_file.write_all(&buf)?;

        let batch_last = out_batch.base_offset + i64::from(out_batch.last_offset_delta);
        if batch_last > last_kept_offset {
            last_kept_offset = batch_last;
        }
    }
    log_file.sync_all()?;

    // Rebuild the survivor `.txnindex`: carry forward aborted-txn entries
    // whose aborted data still partially survives. Producers whose data is
    // fully compacted away have their entries (and markers) dropped.
    let retained: Vec<AbortedTxn> = txn_meta.retained_aborted().copied().collect();
    let txnindex_swap = if retained.is_empty() {
        None
    } else {
        let path = swap_path(dir, new_base, "txnindex");
        // Truncate any stale swap, then append the retained entries.
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let mut idx = TxnIndex::open(path.clone())?;
        for entry in retained {
            idx.append(entry)?;
        }
        Some(path)
    };

    tracing::Span::current().record("new_last_offset", last_kept_offset);
    Ok(RewriteOutput {
        log_swap,
        index_swap,
        timeindex_swap,
        new_base_offset: new_base,
        new_last_offset: last_kept_offset,
        txnindex_swap,
    })
}

fn swap_path(dir: &Path, base_offset: i64, ext: &str) -> PathBuf {
    dir.join(format!(
        "{}.{}.swap",
        name::format_base_offset(base_offset),
        ext
    ))
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod rewrite_tests {
    use super::build_map_tests::{
        control_batch, make_record, write_sealed_batches, write_sealed_segment,
    };
    use super::*;
    use assert2::assert;
    use assert2::check;
    use crabka_protocol::records::Record;
    use std::fs;

    /// A far-future `now` so nothing in the simple tests ages out, plus an
    /// empty active-producer set and no surviving transactions.
    const NEVER_AGE_NOW_MS: i64 = 0;
    const RET_MS: i64 = 1_000;

    fn rewrite_simple(dir: &Path, segs: &[&Segment]) -> RewriteOutput {
        let map = build_offset_map(segs).unwrap();
        let txn = CleanedTransactionMetadata::build(segs, &map).unwrap();
        let active: HashMap<i64, i64> = HashMap::new();
        rewrite_segments(
            dir,
            segs,
            &map,
            &txn,
            NEVER_AGE_NOW_MS,
            RET_MS,
            &active,
            4096,
        )
        .unwrap()
    }

    fn decode_all(bytes: &[u8]) -> Vec<RecordBatch> {
        let mut cursor = bytes;
        let mut out = Vec::new();
        while !cursor.is_empty() {
            let Ok(b) = RecordBatch::decode(&mut cursor) else {
                break;
            };
            out.push(b);
        }
        out
    }

    #[test]
    fn rewrite_drops_superseded_records() {
        let dir = tempfile::tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v2")),
                make_record(2, Some(b"k1"), Some(b"v3")),
            ],
        );
        let segs = vec![&seg0];
        let out = rewrite_simple(dir.path(), &segs);
        assert!(out.new_base_offset == 0);

        // Decode the swap .log to verify contents.
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert!(batch.records.len() == 2);
        let keys: Vec<_> = batch
            .records
            .iter()
            .map(|r| r.key.as_ref().unwrap().to_vec())
            .collect();
        assert!(keys == vec![b"k2".to_vec(), b"k1".to_vec()]);
    }

    #[test]
    fn rewrite_keeps_tombstone_as_latest() {
        let dir = tempfile::tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k1"), None), // tombstone
            ],
        );
        let segs = vec![&seg0];
        let out = rewrite_simple(dir.path(), &segs);
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert!(batch.records.len() == 1);
        check!(batch.records[0].value.is_none());
        check!(batch.records[0].key.as_ref().unwrap().as_ref() == b"k1");
    }

    #[test]
    fn rewrite_preserves_absolute_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            100,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")), // abs 100
                make_record(1, Some(b"k2"), Some(b"v2")), // abs 101
                make_record(2, Some(b"k1"), Some(b"v3")), // abs 102 — kept
            ],
        );
        let segs = vec![&seg0];
        let out = rewrite_simple(dir.path(), &segs);
        assert!(out.new_base_offset == 100);
        assert!(out.new_last_offset == 102);

        let bytes = std::fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert!(batch.base_offset == 100);
        // k2 kept at offset_delta 1, k1 kept at offset_delta 2; base 100,
        // last_offset_delta 2 → batch covers abs offsets 100..=102 with k2,k1.
        assert!(batch.last_offset_delta == 2);
        let abs_offsets: Vec<i64> = batch
            .records
            .iter()
            .map(|r| batch.base_offset + i64::from(r.offset_delta))
            .collect();
        assert!(abs_offsets == vec![101, 102]);
    }

    /// (a) End-to-end control-batch bug fix: two commit markers at different
    /// offsets BOTH survive when their transactions' data survives.
    #[test]
    fn rewrite_both_commit_markers_survive_when_data_survives() {
        let dir = tempfile::tempdir().unwrap();
        // pid 1000: data batch at offset 0 (key k1), commit marker at offset 1.
        // pid 2000: data batch at offset 2 (key k2), commit marker at offset 3.
        let data1 = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: 1000,
            attributes: crabka_protocol::records::Attributes::default().with_transactional(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(b"k1")),
                value: Some(Bytes::copy_from_slice(b"v1")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };
        let marker1 = control_batch(1, 1000, 1 /* COMMIT */);
        let data2 = RecordBatch {
            base_offset: 2,
            last_offset_delta: 0,
            producer_id: 2000,
            attributes: crabka_protocol::records::Attributes::default().with_transactional(true),
            records: vec![Record {
                offset_delta: 0,
                key: Some(Bytes::copy_from_slice(b"k2")),
                value: Some(Bytes::copy_from_slice(b"v2")),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };
        let marker2 = control_batch(3, 2000, 1 /* COMMIT */);
        let seg = write_sealed_batches(dir.path(), &[data1, marker1, data2, marker2]);
        let segs = vec![&seg];
        let out = rewrite_simple(dir.path(), &segs);

        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        let control_count = batches
            .iter()
            .filter(|b| b.attributes.is_control_batch())
            .count();
        assert!(
            control_count == 2,
            "both commit markers must survive (control-batch bug fix); got {control_count}"
        );
    }

    /// (b) A newest-for-key tombstone with no existing horizon gets bit 6 set
    /// and `base_timestamp == now + delete_retention_ms`.
    #[test]
    fn rewrite_tombstone_gets_horizon_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let seg0 = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), None)], // tombstone, newest for k1
        );
        let segs = vec![&seg0];
        let map = build_offset_map(&segs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segs, &map).unwrap();
        let now = 5_000i64;
        let ret = 50i64;
        let out = rewrite_segments(
            dir.path(),
            &segs,
            &map,
            &txn,
            now,
            ret,
            &HashMap::new(),
            4096,
        )
        .unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        check!(batch.attributes.has_delete_horizon());
        check!(batch.delete_horizon_ms() == Some(now + ret));
        check!(batch.base_timestamp == now + ret);
    }

    /// (c) A commit marker whose transaction's data is fully gone and whose
    /// existing horizon has elapsed is dropped.
    #[test]
    fn rewrite_marker_dropped_when_data_gone_and_horizon_elapsed() {
        let dir = tempfile::tempdir().unwrap();
        // A standalone commit marker for pid 1000 with NO surviving data, and
        // an already-stamped delete horizon at base_timestamp = 100.
        let mut marker = control_batch(0, 1000, 1 /* COMMIT */);
        marker.base_timestamp = 100;
        marker.attributes = marker.attributes.with_delete_horizon(true);
        // A second data batch (pid -1) so the marker is not the last batch
        // (otherwise RETAIN_EMPTY would keep a bare header).
        let data = RecordBatch {
            base_offset: 1,
            last_offset_delta: 0,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[marker, data]);
        let segs = vec![&seg];
        let map = build_offset_map(&segs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segs, &map).unwrap();
        // now=200 >= horizon 100 → marker deleted.
        let out = rewrite_segments(
            dir.path(),
            &segs,
            &map,
            &txn,
            200,
            50,
            &HashMap::new(),
            4096,
        )
        .unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        let control_count = batches
            .iter()
            .filter(|b| b.attributes.is_control_batch())
            .count();
        assert!(control_count == 0, "expired marker with no data must drop");
        // The data record survives.
        assert!(batches.iter().any(|b| !b.records.is_empty()));
    }

    /// (d) `RETAIN_EMPTY`: an active producer's fully-emptied batch is
    /// re-emitted as a bare header (no records), preserving
    /// `producer_id`/`epoch`/`sequence`.
    #[test]
    fn rewrite_retain_empty_for_active_producer() {
        let dir = tempfile::tempdir().unwrap();
        // pid 1000 data batch under k1 at offset 0, then a NEWER data batch
        // (pid -1) under k1 at offset 1 that supersedes it — so pid 1000's
        // only record is dropped, emptying its batch. pid 1000 is active.
        let data1 = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: 1000,
            producer_epoch: 7,
            base_sequence: 3,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            ..RecordBatch::default()
        };
        let data2 = RecordBatch {
            base_offset: 1,
            last_offset_delta: 0,
            producer_id: -1,
            records: vec![make_record(0, Some(b"k1"), Some(b"v2"))], // newest for k1
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[data1, data2]);
        let segs = vec![&seg];
        let map = build_offset_map(&segs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segs, &map).unwrap();
        let mut active = HashMap::new();
        active.insert(1000i64, 0i64); // pid 1000 active, last batch base 0
        let out =
            rewrite_segments(dir.path(), &segs, &map, &txn, 0, RET_MS, &active, 4096).unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        // The emptied pid-1000 batch is re-emitted as a bare header.
        let bare = batches
            .iter()
            .find(|b| b.producer_id == 1000)
            .expect("pid 1000 bare header retained");
        check!(bare.records.is_empty());
        check!(bare.producer_epoch == 7);
        check!(bare.base_sequence == 3);
        check!(bare.base_offset == 0);
    }
}

/// Promote the three `.swap` files produced by [`rewrite_segments`]
/// to final segment files, deleting all consumed sealed segments in
/// between.
///
/// Algorithm (crash-safe):
///   1. `fsync` each `.swap` file.
///   2. For every `consumed_base` in `consumed_base_offsets`,
///      remove `<base>.log`, `<base>.index`, `<base>.timeindex`.
///   3. Rename each `.swap` → final name.
///   4. `fsync` the directory.
///
/// On crash recovery, [`crate::recovery::swap_orphan_recover`] heals
/// any intermediate state.
#[instrument(
    level = "info",
    skip_all,
    fields(
        dir = %dir.display(),
        consumed = consumed_base_offsets.len(),
        new_base = rewrite.new_base_offset,
    ),
    err,
)]
pub fn atomic_swap(
    dir: &Path,
    consumed_base_offsets: &[i64],
    rewrite: &RewriteOutput,
) -> Result<(), LogError> {
    // Step 1: fsync swap files. Open with write access so
    // `FlushFileBuffers` (Windows) / `fsync` (Linux) succeeds.
    OpenOptions::new()
        .write(true)
        .open(&rewrite.log_swap)?
        .sync_all()?;
    OpenOptions::new()
        .write(true)
        .open(&rewrite.index_swap)?
        .sync_all()?;
    OpenOptions::new()
        .write(true)
        .open(&rewrite.timeindex_swap)?
        .sync_all()?;
    if let Some(txn_swap) = &rewrite.txnindex_swap {
        OpenOptions::new().write(true).open(txn_swap)?.sync_all()?;
    }

    // Step 2: delete originals (including consumed `.txnindex` files — the
    // rewritten survivor `.txnindex` carries forward only surviving aborted
    // transactions).
    for base in consumed_base_offsets {
        let _ = std::fs::remove_file(name::log_path(dir, *base));
        let _ = std::fs::remove_file(name::index_path(dir, *base));
        let _ = std::fs::remove_file(name::timeindex_path(dir, *base));
        let _ = std::fs::remove_file(name::txnindex_path(dir, *base));
    }

    // Step 3: rename swap → final.
    std::fs::rename(
        &rewrite.log_swap,
        name::log_path(dir, rewrite.new_base_offset),
    )?;
    std::fs::rename(
        &rewrite.index_swap,
        name::index_path(dir, rewrite.new_base_offset),
    )?;
    std::fs::rename(
        &rewrite.timeindex_swap,
        name::timeindex_path(dir, rewrite.new_base_offset),
    )?;
    if let Some(txn_swap) = &rewrite.txnindex_swap {
        std::fs::rename(txn_swap, name::txnindex_path(dir, rewrite.new_base_offset))?;
    }

    // Step 4: fsync the directory. On Windows this is a no-op
    // (`std::fs::File::open` on a dir fails with EACCES); guard the call.
    #[cfg(unix)]
    {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::similar_names)]
mod swap_tests {
    use super::build_map_tests::{make_record, write_sealed_segment};
    use super::*;
    use assert2::check;

    #[test]
    fn atomic_swap_replaces_two_segments_with_one() {
        let dir = tempfile::tempdir().unwrap();
        // Build the offset map and rewrite output while segments are open,
        // then drop the segments before atomic_swap so their file handles
        // are closed. On Windows an open file handle prevents rename/delete.
        let rewrite = {
            let seg0 = write_sealed_segment(
                dir.path(),
                0,
                vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            );
            let seg1 = write_sealed_segment(
                dir.path(),
                10,
                vec![make_record(0, Some(b"k1"), Some(b"v2"))],
            );
            let segs = vec![&seg0, &seg1];
            let map = build_offset_map(&segs).unwrap();
            let txn = CleanedTransactionMetadata::build(&segs, &map).unwrap();
            rewrite_segments(
                dir.path(),
                &segs,
                &map,
                &txn,
                0,
                1_000,
                &HashMap::new(),
                4096,
            )
            .unwrap()
            // seg0, seg1 dropped here — file handles closed
        };
        atomic_swap(dir.path(), &[0, 10], &rewrite).unwrap();

        // After swap: only one .log (base 0). The base 10 segment is gone.
        check!(name::log_path(dir.path(), 0).exists());
        check!(!name::log_path(dir.path(), 10).exists());
        // No leftover .swap files.
        check!(!dir.path().join("00000000000000000000.log.swap").exists());
    }
}

// Exhaustive stateright enumeration of the KIP-534 retention contract over the
// pure decision cores above (reachable via `super::`).
#[cfg(test)]
#[path = "compact_model.rs"]
mod compact_model;

/// Proptest fuzz of the same KIP-534 retention cores at large N: a randomized
/// op sequence is folded into an abstract log, every `Compact` is checked for
/// convergence/idempotence, monotone shrink, no-data-loss, marker safety,
/// tombstone aging, and single-horizon stamping. A separate prop checks the
/// real `RecordBatch` delete-horizon wire round-trip.
#[cfg(test)]
mod retention_fuzz {
    use super::*;
    use proptest::prelude::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum EntryKind {
        Data { value: Option<u8> },
        Marker { producer_id: u8, commit: bool },
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Entry {
        key: Option<u8>,
        kind: EntryKind,
        horizon: Option<i64>,
    }

    #[derive(Clone, Debug)]
    enum Op {
        AppendData(u8, u8),
        AppendTombstone(u8),
        AppendCommit(u8),
        AppendAbort(u8),
        Tick(i64),
        Compact,
    }

    /// Key→newest-index dedup map over keyed data entries (control never
    /// indexed), mirroring the production `build_offset_map` filter.
    fn offset_map(log: &[Entry]) -> std::collections::HashMap<u8, usize> {
        let mut map = std::collections::HashMap::new();
        for (idx, e) in log.iter().enumerate() {
            if !matches!(e.kind, EntryKind::Data { .. }) {
                continue;
            }
            let Some(k) = e.key else { continue };
            if should_index_key(Some(&[k]), false) {
                map.insert(k, idx);
            }
        }
        map
    }

    /// Producers (by key == pid association) whose newest-for-key live data
    /// survives.
    fn data_survives(
        log: &[Entry],
        map: &std::collections::HashMap<u8, usize>,
    ) -> std::collections::HashSet<u8> {
        let mut s = std::collections::HashSet::new();
        for (idx, e) in log.iter().enumerate() {
            let EntryKind::Data { value } = &e.kind else {
                continue;
            };
            let Some(k) = e.key else { continue };
            if value.is_none() {
                continue;
            }
            if map.get(&k).copied() == Some(idx) {
                s.insert(k);
            }
        }
        s
    }

    fn txn_state(pid: u8, survivors: &std::collections::HashSet<u8>) -> TxnDataState {
        if survivors.contains(&pid) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }

    /// One compaction pass: apply the real `retain_decision` to each entry,
    /// returning the next log. Mirrors the abstract applier in the model.
    fn compact(log: &[Entry], clock: i64, ret_ms: i64) -> Vec<Entry> {
        let map = offset_map(log);
        let survivors = data_survives(log, &map);
        let mut next = Vec::with_capacity(log.len());
        for (idx, e) in log.iter().enumerate() {
            let (rec, batch, is_newest, txn) = match &e.kind {
                EntryKind::Data { value } => (
                    RecordMeta {
                        has_key: e.key.is_some(),
                        has_value: value.is_some(),
                    },
                    BatchMeta {
                        is_control: false,
                        producer_id: -1,
                        existing_horizon: e.horizon,
                    },
                    e.key.is_some_and(|k| map.get(&k).copied() == Some(idx)),
                    TxnDataState::NotTransactional,
                ),
                EntryKind::Marker { producer_id, .. } => (
                    RecordMeta {
                        has_key: true,
                        has_value: false,
                    },
                    BatchMeta {
                        is_control: true,
                        producer_id: i64::from(*producer_id),
                        existing_horizon: e.horizon,
                    },
                    false,
                    txn_state(*producer_id, &survivors),
                ),
            };
            match retain_decision(rec, batch, is_newest, txn, clock, ret_ms) {
                RetainDecision::Keep => next.push(e.clone()),
                RetainDecision::SetHorizon(h) => {
                    // Single-horizon stamping: the core must only ever stamp an
                    // entry that has no horizon yet. A `SetHorizon` over an
                    // already-stamped entry would re-stamp it — a violation
                    // checked here at the exact point of assignment (where this
                    // specific entry's prior horizon is known unambiguously).
                    if let Some(existing) = e.horizon {
                        assert!(existing == h, "horizon re-stamped {existing} -> {h}");
                    }
                    let mut ne = e.clone();
                    ne.horizon = Some(h);
                    next.push(ne);
                }
                RetainDecision::Delete => {}
            }
        }
        next
    }

    fn apply(log: &mut Vec<Entry>, clock: &mut i64, op: &Op, ret_ms: i64) {
        match *op {
            Op::AppendData(key, value) => log.push(Entry {
                key: Some(key),
                kind: EntryKind::Data { value: Some(value) },
                horizon: None,
            }),
            Op::AppendTombstone(key) => log.push(Entry {
                key: Some(key),
                kind: EntryKind::Data { value: None },
                horizon: None,
            }),
            Op::AppendCommit(pid) => log.push(Entry {
                key: None,
                kind: EntryKind::Marker {
                    producer_id: pid,
                    commit: true,
                },
                horizon: None,
            }),
            Op::AppendAbort(pid) => log.push(Entry {
                key: None,
                kind: EntryKind::Marker {
                    producer_id: pid,
                    commit: false,
                },
                horizon: None,
            }),
            Op::Tick(dt) => *clock += dt,
            Op::Compact => {
                let before = log.clone();
                let after = compact(&before, *clock, ret_ms);

                // --- Convergence / idempotence at a fixed clock. ---
                let twice = compact(&after, *clock, ret_ms);
                prop_assert_eq_inner(&after, &twice);

                // --- Monotone shrink. ---
                assert!(
                    after.len() <= before.len(),
                    "compaction grew the log: {} -> {}",
                    before.len(),
                    after.len()
                );

                // --- No-data-loss: every newest-for-key live data survives. ---
                let map = offset_map(&before);
                for (idx, e) in before.iter().enumerate() {
                    if let EntryKind::Data { value: Some(_) } = &e.kind
                        && let Some(k) = e.key
                        && map.get(&k).copied() == Some(idx)
                    {
                        assert!(
                            after.iter().any(|x| x.key == Some(k)
                                && matches!(x.kind, EntryKind::Data { value: Some(_) })),
                            "no-data-loss: key {k} newest-live dropped"
                        );
                    }
                }

                // --- Marker safety: survives iff its txn data survives; never
                // deleted before clock >= horizon. ---
                let survivors = data_survives(&before, &map);
                for e in &before {
                    if let EntryKind::Marker { producer_id, .. } = &e.kind {
                        let alive = after.iter().any(|x| {
                            matches!(
                                &x.kind,
                                EntryKind::Marker { producer_id: p, .. } if p == producer_id
                            )
                        });
                        if survivors.contains(producer_id) {
                            assert!(
                                alive,
                                "marker for pid {producer_id} dropped while data survives"
                            );
                        }
                        // If the marker had a horizon and clock < horizon, it
                        // must still be alive (not aged out prematurely).
                        if let (Some(h), false) = (e.horizon, survivors.contains(producer_id))
                            && *clock < h
                        {
                            assert!(
                                alive,
                                "marker for pid {producer_id} aged out at \
                                 clock {} < horizon {h}",
                                *clock
                            );
                        }
                    }
                }

                // --- Tombstone aging: a surviving tombstone is present iff it
                // has no horizon or clock < horizon. ---
                for x in &after {
                    if matches!(x.kind, EntryKind::Data { value: None })
                        && let Some(h) = x.horizon
                    {
                        assert!(
                            *clock < h,
                            "tombstone survived with elapsed horizon {h} at clock {}",
                            *clock
                        );
                    }
                }

                // --- Single horizon stamping is enforced inside `compact` at
                // the exact point a horizon is assigned (a `SetHorizon` over an
                // already-stamped entry panics there). Nothing to re-check here.

                *log = after;
            }
        }
    }

    /// `prop_assert_eq` is macro-bound to a `proptest!` body; inside a plain fn
    /// we use a panicking equality so a mismatch surfaces as a case failure.
    fn prop_assert_eq_inner(a: &[Entry], b: &[Entry]) {
        assert!(
            a == b,
            "compaction not idempotent at fixed clock:\n  once={a:?}\n  twice={b:?}"
        );
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        prop_oneof![
            (0u8..=2, 0u8..=2).prop_map(|(k, v)| Op::AppendData(k, v)),
            (0u8..=2).prop_map(Op::AppendTombstone),
            (0u8..=2).prop_map(Op::AppendCommit),
            (0u8..=2).prop_map(Op::AppendAbort),
            (1i64..=5).prop_map(Op::Tick),
            Just(Op::Compact),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn retention_invariants_hold(
            ops in proptest::collection::vec(op_strategy(), 0..200),
            ret_ms in 1i64..100,
        ) {
            let mut log: Vec<Entry> = Vec::new();
            let mut clock: i64 = 0;
            for op in &ops {
                apply(&mut log, &mut clock, op, ret_ms);
            }
        }

        /// Wire round-trip: a real `RecordBatch` with two keyed records gets a
        /// random delete horizon stamped, then encode→decode must preserve
        /// `delete_horizon_ms()` and every record's absolute timestamp.
        #[test]
        fn delete_horizon_wire_round_trip(
            horizon in -1_000i64..1_000_000,
            base_ts in 0i64..1_000,
            d0 in 0i64..500,
            d1 in 0i64..500,
        ) {
            use bytes::{Bytes, BytesMut};
            use crabka_protocol::records::{Record, RecordBatch};

            let rec = |delta: i64, k: &[u8]| Record {
                offset_delta: 0,
                timestamp_delta: delta,
                key: Some(Bytes::copy_from_slice(k)),
                value: Some(Bytes::copy_from_slice(b"v")),
                ..Default::default()
            };
            let batch = RecordBatch {
                base_offset: 0,
                last_offset_delta: 1,
                base_timestamp: base_ts,
                max_timestamp: base_ts + d0.max(d1),
                records: vec![rec(d0, b"k0"), rec(d1, b"k1")],
                ..RecordBatch::default()
            };
            // Original absolute per-record timestamps.
            let orig_abs: Vec<i64> = batch
                .records
                .iter()
                .map(|r| batch.base_timestamp + r.timestamp_delta)
                .collect();

            let stamped = batch.with_delete_horizon(horizon);
            let mut buf = BytesMut::with_capacity(stamped.encoded_len());
            stamped.encode(&mut buf).unwrap();
            let mut cursor: &[u8] = &buf[..];
            let decoded = RecordBatch::decode(&mut cursor).unwrap();

            prop_assert_eq!(decoded.delete_horizon_ms(), Some(horizon));
            // Reconstructed absolute timestamps (base + delta, delta is i64)
            // equal the originals.
            let new_abs: Vec<i64> = decoded
                .records
                .iter()
                .map(|r| decoded.base_timestamp + r.timestamp_delta)
                .collect();
            prop_assert_eq!(new_abs, orig_abs);
        }
    }
}
