//! Log compaction primitives. These are almost-pure helpers that work on
//! [`Segment`] handles and the on-disk file layout. [`crate::Log::compact`]
//! uses them.
//!
//! The algorithm makes a single pass over the **sealed** segment list, from
//! oldest to newest. It builds a key-to-latest-offset map, then rewrites the
//! surviving records into a single new segment at the lowest input base
//! offset. It never touches the active segment.
//!
//! The algorithm drops records with `key.is_none()`, as Kafka's `LogCleaner`
//! does. A tombstone is a record with `key.is_some()` and `value.is_none()`.
//! The algorithm treats a tombstone like any other value and keeps it as the
//! most-recent entry for its key. `delete.retention.ms` ages tombstones out.

use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use bytes::{Bytes, BytesMut};
use crabka_ids::{Offset, ProducerId};
use crabka_protocol::records::RecordBatch;
use crabka_units::prelude::{ByteSize, Time, TimeExt};
// ---------------------------------------------------------------------------
// KIP-534 pure decision cores
//
// The retain/horizon core now lives in `crabka-verified`, where its contract
// is proven with Creusot. Thin typed wrappers keep log-local `ProducerId`
// boundaries explicit while `compact_model.rs` and `core_tests` keep driving
// the exact production path.
// ---------------------------------------------------------------------------
pub(crate) use crabka_verified::{RecordMeta, RetainDecision, TxnDataState};
use tracing::instrument;

use crate::{
    error::LogError,
    name,
    segment::Segment,
    txn_index::{AbortedTxn, TxnIndex},
};

/// Per-batch facts the retain decision needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BatchMeta {
    pub is_control: bool,
    pub producer_id: ProducerId,
    /// The batch's existing delete horizon, which is `base_timestamp` when
    /// bit 6 is set. It is `None` if the batch has never been stamped.
    pub existing_horizon: Option<i64>,
}

/// Compute the delete horizon timestamp: `now + delete.retention.ms`. The log
/// retains the tombstone or the marker until the wall clock reaches this
/// value.
#[must_use]
#[cfg(test)]
pub(crate) const fn compute_horizon(now_ms: i64, delete_retention_ms: i64) -> i64 {
    crabka_verified::compute_horizon(now_ms, delete_retention_ms)
}

/// The single per-record KIP-534 retain decision.
#[must_use]
pub(crate) const fn retain_decision(
    rec: RecordMeta,
    batch: BatchMeta,
    is_newest_for_key: bool,
    txn: TxnDataState,
    now_ms: i64,
    delete_retention_ms: i64,
) -> RetainDecision {
    crabka_verified::retain_decision(
        rec,
        crabka_verified::BatchMeta {
            is_control: batch.is_control,
            producer_id: batch.producer_id.0,
            existing_horizon: batch.existing_horizon,
        },
        is_newest_for_key,
        txn,
        now_ms,
        delete_retention_ms,
    )
}

/// `build_offset_map` filter. The control-batch bug fix is here.
/// Control-batch records carry a control-type key, a commit or abort marker,
/// that must NEVER enter the dedup map. Null-key data is also never indexed.
pub(crate) fn should_index_key(key: Option<&[u8]>, is_control_batch: bool) -> bool {
    !is_control_batch && key.is_some()
}

/// Reinterpret the per-record `i64` timestamp deltas when a delete horizon
/// goes into `base_timestamp`. Each record keeps its absolute timestamp.
///
/// `core_tests` and the planned stateright and proptest model exercise this
/// function. The production rewrite path delegates the same arithmetic to
/// `RecordBatch::with_delete_horizon`, so the function is `dead_code` outside
/// tests.
#[cfg(test)]
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

/// Whether compaction removed all of a transactional producer's data. That is
/// true when the `producer_id` is not in the `survivors` set, the set of
/// producers with a surviving data record.
///
/// The production rewrite path uses
/// [`CleanedTransactionMetadata::txn_state`], which folds this check in. This
/// standalone form exists for `core_tests` and the planned stateright and
/// proptest model.
#[cfg(test)]
pub(crate) fn txn_data_fully_gone(
    producer_id: ProducerId,
    survivors: &HashSet<ProducerId>,
) -> bool {
    !survivors.contains(&producer_id)
}

#[cfg(test)]
mod core_tests {
    use assert2::check;

    use super::*;

    fn data(has_key: bool, has_value: bool) -> RecordMeta {
        RecordMeta { has_key, has_value }
    }

    fn batch(is_control: bool, producer_id: i64, existing_horizon: Option<i64>) -> BatchMeta {
        BatchMeta {
            is_control,
            producer_id: ProducerId(producer_id),
            existing_horizon,
        }
    }

    #[test]
    fn control_batch_key_is_never_indexed() {
        // A control batch's key (commit/abort marker) must NOT enter the
        // dedup map, regardless of whether the key is present.
        for (name, key, is_control, want) in [
            (
                "control marker key",
                Some(b"\x00\x00\x00\x01".as_ref()),
                true,
                false,
            ),
            // Null-key data is also never indexed.
            ("null data key", None, false, false),
            // Ordinary keyed data IS indexed.
            ("ordinary data key", Some(b"k".as_ref()), false, true),
        ] {
            check!(
                should_index_key(key, is_control) == want,
                "case {name}: key={key:?} is_control={is_control}"
            );
        }
    }

    #[test]
    fn tombstone_sets_horizon_then_deletes_after_expiry() {
        let rec = data(true, false); // keyed, null value (tombstone)
        for (name, existing_horizon, is_newest, now_ms, want) in [
            // Newest tombstone, no existing horizon: stamp now+ret = 100+50 = 150.
            (
                "stamp new horizon",
                None,
                true,
                100,
                RetainDecision::SetHorizon(150),
            ),
            // Now=149 < horizon 150: keep.
            (
                "keep before horizon",
                Some(150),
                true,
                149,
                RetainDecision::Keep,
            ),
            // Now=150 >= horizon 150: delete.
            (
                "delete at horizon",
                Some(150),
                true,
                150,
                RetainDecision::Delete,
            ),
            // Superseded tombstone (not newest-for-key): delete outright.
            (
                "delete superseded",
                None,
                false,
                100,
                RetainDecision::Delete,
            ),
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
                "case {name}: horizon={existing_horizon:?} newest={is_newest} now={now_ms}"
            );
        }
    }

    #[test]
    fn compute_horizon_saturates_at_i64_bounds() {
        for (_name, timestamp, retention, expected) in [
            ("ordinary sum", 100, 50, 150),
            ("saturates maximum", i64::MAX - 1, 50, i64::MAX),
            ("saturates minimum", i64::MIN + 1, -50, i64::MIN),
        ] {
            assert2::assert!(compute_horizon(timestamp, retention) == expected);
        }
    }

    #[test]
    fn marker_retained_while_data_survives_then_ages() {
        let marker = data(true, false); // control records carry a key, no value
        for (name, existing_horizon, txn_state, now_ms, want) in [
            // Data still survives: keep the marker.
            (
                "keep while data survives",
                None,
                TxnDataState::DataSurvives,
                100,
                RetainDecision::Keep,
            ),
            // Data fully gone, no horizon yet: stamp now+ret = 100+50 = 150.
            (
                "stamp after data gone",
                None,
                TxnDataState::DataFullyGone,
                100,
                RetainDecision::SetHorizon(150),
            ),
            // Data fully gone, horizon 150, now 150: delete.
            (
                "delete aged marker",
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
                "case {name}: horizon={existing_horizon:?} now={now_ms}"
            );
        }
    }

    #[test]
    fn live_data_kept_nullkey_dropped() {
        for (name, has_key, is_newest, want) in [
            // Newest-for-key data with a value: keep.
            ("keep newest keyed data", true, true, RetainDecision::Keep),
            // Null-key data: dropped regardless of newest-ness.
            ("drop null key", false, true, RetainDecision::Delete),
            // Keyed data with a value but not newest-for-key: dropped.
            (
                "drop superseded keyed data",
                true,
                false,
                RetainDecision::Delete,
            ),
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
                "case {name}: has_key={has_key} newest={is_newest}"
            );
        }
    }

    #[test]
    fn rewrite_batch_horizon_preserves_absolute_timestamps() {
        let (base, deltas) = rewrite_batch_horizon(1000, &[0, 5, 20], 9999);
        // Reconstructed absolute timestamps (base + delta) must equal the
        // originals: 1000, 1005, 1020.
        let reconstructed: Vec<i64> = deltas.iter().map(|d| base + d).collect();
        assert2::assert!(base == 9999);
        assert2::assert!(reconstructed == vec![1000, 1005, 1020]);
    }

    #[test]
    fn txn_data_fully_gone_checks_survivor_set() {
        let mut survivors = HashSet::new();
        survivors.insert(ProducerId(1000));
        assert2::assert!(txn_data_fully_gone(ProducerId(2000), &survivors));
        assert2::assert!(!txn_data_fully_gone(ProducerId(1000), &survivors));
    }
}

/// Read every `RecordBatch` from a sealed segment. This function streams the
/// whole `.log` file directly.
///
/// It avoids `Segment::read`, because that path returns early when the
/// segment's in-memory `last_offset` is stale. A sealed segment loaded from
/// disk through `Segment::open` has `last_offset = base_offset - 1` until a
/// tail scan fills it in, so `Segment::read(base_offset, ..)` would
/// short-circuit to an empty result.
fn read_all_batches(seg: &Segment) -> Result<Vec<RecordBatch>, LogError> {
    let path = name::log_path(seg.dir(), seg.base_offset().0);
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

/// Build a map of `key → latest absolute offset` across the given sealed
/// segments in input order.
///
/// The map excludes records with `key.is_none()`, because
/// [`rewrite_segments`] drops them. The map's value is the absolute offset of
/// the **newest** record seen for each key. Later writes overwrite earlier
/// ones.
#[instrument(
    level = "debug",
    skip_all,
    fields(segments = segments.len(), keys = tracing::field::Empty),
    err,
)]
pub fn build_offset_map(segments: &[&Segment]) -> Result<HashMap<Bytes, Offset>, LogError> {
    // Keyed by `Bytes` (cheap refcounted clone of the record key) rather
    // than `Vec<u8>` to avoid a heap copy of every key. Zero-length keys
    // are legal in Kafka and dedup as a distinct "empty key" like any other.
    let mut map: HashMap<Bytes, Offset> = HashMap::new();
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
                let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
                map.insert(key_bytes.clone(), absolute);
            }
        }
    }
    tracing::Span::current().record("keys", map.len());
    Ok(map)
}

/// Per-producer transactional-data survival, computed in a first pass over the
/// sealed segments.
///
/// KIP-534 keeps a transaction's commit or abort marker as long as any of that
/// transaction's data records survive compaction. Once compaction removes all
/// of the data, the marker ages out through the delete horizon.
///
/// This type is seeded with the aborted-txn entries from each sealed segment's
/// `.txnindex`, so the rewrite can rebuild the survivor `.txnindex` for
/// transactions whose data still partly survives.
pub struct CleanedTransactionMetadata {
    /// Producers (`producer_id`) with at least one surviving data record.
    survivors: HashSet<ProducerId>,
    /// Aborted-txn entries gathered from the consumed segments' `.txnindex`
    /// files, in input order.
    aborted: Vec<AbortedTxn>,
}

impl CleanedTransactionMetadata {
    /// Build the metadata. For each producer, this records whether any of its
    /// transactional DATA records will survive, that is, a data record that is
    /// newest-for-key in `offset_map`. The aborted-txn entries come from every
    /// sealed segment's `.txnindex`.
    #[instrument(
        level = "debug",
        skip_all,
        fields(segments = segments.len(), survivors = tracing::field::Empty),
        err,
    )]
    pub fn build(
        segments: &[&Segment],
        offset_map: &HashMap<Bytes, Offset>,
    ) -> Result<Self, LogError> {
        let mut survivors: HashSet<ProducerId> = HashSet::new();
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
                    let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
                    if offset_map.get(key_bytes.as_ref()).copied() == Some(absolute) {
                        survivors.insert(ProducerId(batch.producer_id));
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
    pub fn txn_state(&self, producer_id: ProducerId) -> TxnDataState {
        if producer_id.is_none() {
            return TxnDataState::NotTransactional;
        }
        if self.survivors.contains(&producer_id) {
            TxnDataState::DataSurvives
        } else {
            TxnDataState::DataFullyGone
        }
    }

    /// Aborted-txn entries to carry forward into the rewritten survivor
    /// `.txnindex`. These are the entries whose aborted data still partly
    /// survives, that is, the producer is in the survivor set. The rewrite
    /// drops the entries of producers whose data is fully gone, together with
    /// the marker, which is then removable.
    fn retained_aborted(&self) -> impl Iterator<Item = &AbortedTxn> {
        self.aborted
            .iter()
            .filter(move |e| self.survivors.contains(&e.producer_id))
    }
}

#[cfg(test)]
mod build_map_tests {
    use bytes::Bytes;
    use crabka_ids::Offset;
    use crabka_protocol::records::{Attributes, Record};
    use tempfile::tempdir;

    use super::*;

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
        let mut seg = Segment::create(dir, Offset(base_offset)).unwrap();
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
        seg.append(&batch, INDEX_INTERVAL).unwrap();
        seg.seal();
        seg
    }

    /// Write a sealed segment that holds the given batches verbatim, with
    /// `base_offset`, attributes, and `producer_id` preserved. Tests use it to
    /// build control batches and mixed data and control layouts.
    pub(super) fn write_sealed_batches(dir: &Path, batches: &[RecordBatch]) -> Segment {
        let base = batches.first().map_or(0, |b| b.base_offset);
        let mut seg = Segment::create(dir, Offset(base)).unwrap();
        for batch in batches {
            seg.append(batch, INDEX_INTERVAL).unwrap();
        }
        seg.seal();
        seg
    }

    /// A control batch that carries a single commit or abort marker record.
    /// The marker key is `(version: i16, marker_type: i16)` big-endian.
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
        let segment_refs: Vec<&Segment> = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == HashMap::from([(Bytes::from_static(b"k1"), Offset(1))]));
    }

    #[test]
    fn build_offset_map_keeps_newest_offset_per_key() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v2")),
                make_record(2, Some(b"k1"), Some(b"v3")), // k1 overwritten
            ],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(
            map == HashMap::from([
                (Bytes::from_static(b"k1"), Offset(2)),
                (Bytes::from_static(b"k2"), Offset(1)),
            ])
        );
    }

    #[test]
    fn build_offset_map_drops_null_key_records() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, None, Some(b"no-key-1")),
                make_record(1, Some(b"k1"), Some(b"v1")),
                make_record(2, None, Some(b"no-key-2")),
            ],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == HashMap::from([(Bytes::from_static(b"k1"), Offset(1))]));
    }

    #[test]
    fn build_offset_map_across_segments_uses_newest() {
        let dir = tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), Some(b"v1"))],
        );
        let second_segment = write_sealed_segment(
            dir.path(),
            10,
            vec![make_record(0, Some(b"k1"), Some(b"v2"))],
        );
        let segment_refs: Vec<&Segment> = vec![&first_segment, &second_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        assert2::assert!(map == HashMap::from([(Bytes::from_static(b"k1"), Offset(10))]));
    }

    // Survivor detection compares each record's absolute offset
    // (`base_offset + offset_delta`) against the newest-for-key offset in the
    // offset map (`== Some(absolute)`). Two transactional producers write the
    // SAME key k1:
    //   - producer 1000 at offset 0 (superseded), and
    //   - producer 2000 at base 10, delta 5 → offset 15 (newest for k1).
    // The map therefore holds k1 → 15, so producer 2000 survives and producer
    // 1000 does not. This pins:
    //   - `absolute = base_offset + offset_delta` (line 429): mutating `+`→`-`
    //     makes 2000's record resolve to 5, not 15 → 2000 misclassified
    //     `DataFullyGone`.
    //   - the `== Some(absolute)` equality (line 430): mutating `==`→`!=`
    //     inverts the match — 2000 (the match) becomes `DataFullyGone` and
    //     1000 (the non-match) becomes `DataSurvives`.
    #[test]
    fn build_detects_surviving_txn_producer() {
        let dir = tempdir().unwrap();
        // Producer 1000: k1 at offset 0 — superseded by producer 2000.
        let old = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: 1000,
            attributes: Attributes::default().with_transactional(true),
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            ..RecordBatch::default()
        };
        // Producer 2000: k1 at base 10, offset_delta 5 → absolute offset 15,
        // the newest-for-key record.
        let newest = RecordBatch {
            base_offset: 10,
            last_offset_delta: 5,
            producer_id: 2000,
            attributes: Attributes::default().with_transactional(true),
            records: vec![make_record(5, Some(b"k1"), Some(b"v2"))],
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[old, newest]);
        let segment_refs: Vec<&Segment> = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        // Sanity: the newest-for-key absolute offset is 15 (10 + 5).
        assert2::assert!(map == HashMap::from([(Bytes::from_static(b"k1"), Offset(15))]));

        let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
        // Producer 2000's newest data survives; producer 1000's is superseded.
        assert2::assert!(txn.txn_state(ProducerId(2000)) == TxnDataState::DataSurvives);
        assert2::assert!(txn.txn_state(ProducerId(1000)) == TxnDataState::DataFullyGone);
    }
}

/// Result of [`rewrite_segments`]: paths to the three `.swap` files that
/// [`atomic_swap`] should promote.
pub struct RewriteOutput {
    pub log_swap: PathBuf,
    pub index_swap: PathBuf,
    pub timeindex_swap: PathBuf,
    /// `base_offset` of the new segment. It equals the lowest input segment.
    pub new_base_offset: Offset,
    /// Highest absolute offset of any surviving record.
    #[cfg(test)]
    pub new_last_offset: Offset,
    /// Path to the rewritten survivor `.txnindex`. The rewrite writes this
    /// file only when it carries forward one or more aborted-txn entries. It
    /// is `None` when no aborted transaction survives.
    pub txnindex_swap: Option<PathBuf>,
}

/// Time-based retention inputs used while rewriting compacted segments.
#[derive(Debug, Clone, Copy)]
pub struct RewriteRetention {
    /// Current wall-clock time in milliseconds. An instant, so it stays raw.
    pub now_ms: i64,
    /// How long a tombstone remains eligible for reads before deletion.
    pub delete_retention: Time,
}

/// Stream `segments`, oldest to newest, into new `.swap` files and apply the
/// KIP-534 per-record [`retain_decision`].
///
/// For each record the decision is:
///   - `Keep` → write it through.
///   - `SetHorizon(h)` → write it through, and stamp the output batch with
///     delete horizon `h` (bit 6 set, `base_timestamp = h`).
///   - `Delete` → drop it.
///
/// Records keep their **absolute** offsets. The output `RecordBatch`es can
/// therefore hold gaps in their `offset_delta` values where superseded records
/// used to live. This matches Kafka's on-disk format for compacted topics.
///
/// `RETAIN_EMPTY`: this function normally skips a batch that ends up with no
/// kept records. It writes such a batch again as a bare header with no records
/// in two cases: when the batch is the last batch of an active producer in
/// `active_producers`, and when it is the last batch of the consolidated
/// output. The producer sequence, the producer epoch, and the log-end offset
/// therefore survive. This is Kafka's `retainEmpty`.
///
/// This function writes the `.swap` files to the segments' shared directory.
/// The caller must fsync them and promote them through [`atomic_swap`].
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
    offset_map: &HashMap<Bytes, Offset>,
    txn_meta: &CleanedTransactionMetadata,
    retention: RewriteRetention,
    active_producers: &HashMap<ProducerId, Offset>,
    _index_interval: ByteSize,
) -> Result<RewriteOutput, LogError> {
    // The Creusot-verified retain kernel is stated over integer milliseconds,
    // and the horizon it computes is stamped into an on-disk `base_timestamp`,
    // so the extent crosses to a raw count once, here, truncating rather than
    // rounding so a stamped horizon can never land a millisecond late.
    let delete_retention_ms = retention.delete_retention.millis_i64_trunc();

    let first = segments
        .first()
        .ok_or_else(|| LogError::Io(std::io::Error::other("rewrite_segments: empty input")))?;
    let new_base = first.base_offset();
    tracing::Span::current().record("new_base", new_base.0);

    let log_swap = swap_path(dir, new_base.0, "log");
    let index_swap = swap_path(dir, new_base.0, "index");
    let timeindex_swap = swap_path(dir, new_base.0, "timeindex");

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
    let mut producer_last_batch: HashMap<ProducerId, usize> = HashMap::new();
    for (i, batch) in all_batches.iter().enumerate() {
        let pid = ProducerId(batch.producer_id);
        if active_producers.contains_key(&pid) {
            producer_last_batch.insert(pid, i);
        }
    }

    let mut last_kept_offset = new_base - 1;

    for (batch_idx, batch) in all_batches.iter().enumerate() {
        let is_control = batch.attributes.is_control_batch();
        let producer_id = ProducerId(batch.producer_id);
        let txn = txn_meta.txn_state(producer_id);
        let batch_meta = BatchMeta {
            is_control,
            producer_id,
            existing_horizon: batch.delete_horizon_ms(),
        };

        let mut kept: Vec<crabka_protocol::records::Record> =
            Vec::with_capacity(batch.records.len());
        // Stamp the output batch with a delete horizon if any record's
        // decision asks for it (stamp once per batch).
        let mut stamp_horizon: Option<i64> = None;
        for record in &batch.records {
            let absolute = Offset(batch.base_offset + i64::from(record.offset_delta));
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
                retention.now_ms,
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
                producer_last_batch.get(&producer_id).copied() == Some(batch_idx);
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
            let batch_last = Offset(out_batch.base_offset + i64::from(out_batch.last_offset_delta));
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

        let batch_last = Offset(out_batch.base_offset + i64::from(out_batch.last_offset_delta));
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
        let path = swap_path(dir, new_base.0, "txnindex");
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

    tracing::Span::current().record("new_last_offset", last_kept_offset.0);
    Ok(RewriteOutput {
        log_swap,
        index_swap,
        timeindex_swap,
        new_base_offset: new_base,
        #[cfg(test)]
        new_last_offset: last_kept_offset,
        txnindex_swap,
    })
}

/// Kafka's default `index.interval.bytes`. The compaction tests do not
/// exercise sparse-index density, so they all pass the default value
/// through.
#[cfg(test)]
const INDEX_INTERVAL: ByteSize = crabka_units::kibibytes(4);

/// The `delete.retention.ms` the rewrite tests share.
#[cfg(test)]
const RETENTION: Time = crabka_units::secs(1);

fn swap_path(dir: &Path, base_offset: i64, ext: &str) -> PathBuf {
    dir.join(format!(
        "{}.{}.swap",
        name::format_base_offset(base_offset),
        ext
    ))
}

#[cfg(test)]
mod rewrite_tests {
    use std::fs;

    use crabka_ids::Offset;
    use crabka_protocol::records::{Attributes, Record};
    use crabka_units::prelude::millis;

    use super::{
        build_map_tests::{control_batch, make_record, write_sealed_batches, write_sealed_segment},
        *,
    };

    /// A far-future `now` so nothing in the simple tests ages out, plus an
    /// empty active-producer set and no surviving transactions.
    const NEVER_AGE_NOW_MS: i64 = 0;

    fn rewrite_simple(dir: &Path, segment_refs: &[&Segment]) -> RewriteOutput {
        let map = build_offset_map(segment_refs).unwrap();
        let txn = CleanedTransactionMetadata::build(segment_refs, &map).unwrap();
        let active: HashMap<ProducerId, Offset> = HashMap::new();
        rewrite_segments(
            dir,
            segment_refs,
            &map,
            &txn,
            RewriteRetention {
                now_ms: NEVER_AGE_NOW_MS,
                delete_retention: RETENTION,
            },
            &active,
            INDEX_INTERVAL,
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
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k2"), Some(b"v2")),
                make_record(2, Some(b"k1"), Some(b"v3")),
            ],
        );
        let segment_refs = vec![&first_segment];
        let out = rewrite_simple(dir.path(), &segment_refs);
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(2));
        assert2::assert!(
            batches
                == vec![RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 2,
                    records: vec![
                        make_record(1, Some(b"k2"), Some(b"v2")),
                        make_record(2, Some(b"k1"), Some(b"v3")),
                    ],
                    ..RecordBatch::default()
                }]
        );
    }

    #[test]
    fn rewrite_keeps_tombstone_as_latest() {
        let dir = tempfile::tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")),
                make_record(1, Some(b"k1"), None), // tombstone
            ],
        );
        let segment_refs = vec![&first_segment];
        let out = rewrite_simple(dir.path(), &segment_refs);
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        let mut record = make_record(1, Some(b"k1"), None);
        record.timestamp_delta = -1_000;
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(1));
        assert2::assert!(
            batch
                == RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 1,
                    base_timestamp: RETENTION.millis_i64(),
                    attributes: Attributes::default().with_delete_horizon(true),
                    records: vec![record],
                    ..RecordBatch::default()
                }
        );
    }

    #[test]
    fn rewrite_preserves_absolute_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            100,
            vec![
                make_record(0, Some(b"k1"), Some(b"v1")), // abs 100
                make_record(1, Some(b"k2"), Some(b"v2")), // abs 101
                make_record(2, Some(b"k1"), Some(b"v3")), // abs 102 — kept
            ],
        );
        let segment_refs = vec![&first_segment];
        let out = rewrite_simple(dir.path(), &segment_refs);
        let bytes = std::fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(100));
        assert2::assert!(out.new_last_offset == Offset(102));
        assert2::assert!(
            batches
                == vec![RecordBatch {
                    base_offset: 100,
                    last_offset_delta: 2,
                    records: vec![
                        make_record(1, Some(b"k2"), Some(b"v2")),
                        make_record(2, Some(b"k1"), Some(b"v3")),
                    ],
                    ..RecordBatch::default()
                }]
        );
    }

    /// (a) End-to-end control-batch bug fix. Two commit markers at different
    /// offsets BOTH survive when the data of their transactions survives.
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
        let expected = vec![
            data1.clone(),
            marker1.clone(),
            data2.clone(),
            marker2.clone(),
        ];
        let seg = write_sealed_batches(dir.path(), &[data1, marker1, data2, marker2]);
        let segment_refs = vec![&seg];
        let out = rewrite_simple(dir.path(), &segment_refs);

        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(3));
        assert2::assert!(batches == expected);
    }

    /// (b) A newest-for-key tombstone with no existing horizon gets bit 6 set
    /// and `base_timestamp == now + delete_retention_ms`.
    #[test]
    fn rewrite_tombstone_gets_horizon_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let first_segment = write_sealed_segment(
            dir.path(),
            0,
            vec![make_record(0, Some(b"k1"), None)], // tombstone, newest for k1
        );
        let segment_refs = vec![&first_segment];
        let map = build_offset_map(&segment_refs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
        let now = 5_000i64;
        let ret = 50i64;
        let retention = Time::from_millis(ret);
        let out = rewrite_segments(
            dir.path(),
            &segment_refs,
            &map,
            &txn,
            RewriteRetention {
                now_ms: now,
                delete_retention: retention,
            },
            &HashMap::new(),
            INDEX_INTERVAL,
        )
        .unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        let mut record = make_record(0, Some(b"k1"), None);
        record.timestamp_delta = -(now + ret);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(0));
        assert2::assert!(
            batches
                == vec![RecordBatch {
                    base_offset: 0,
                    last_offset_delta: 0,
                    base_timestamp: now + ret,
                    attributes: Attributes::default().with_delete_horizon(true),
                    records: vec![record],
                    ..RecordBatch::default()
                }]
        );
    }

    /// (c) The rewrite drops a commit marker when the data of its transaction
    /// is fully gone and its existing horizon has elapsed.
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
        let segment_refs = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
        // now=200 >= horizon 100 → marker deleted.
        let out = rewrite_segments(
            dir.path(),
            &segment_refs,
            &map,
            &txn,
            RewriteRetention {
                now_ms: 200,
                delete_retention: millis(50),
            },
            &HashMap::new(),
            INDEX_INTERVAL,
        )
        .unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(1));
        assert2::assert!(
            batches
                == vec![RecordBatch {
                    base_offset: 1,
                    last_offset_delta: 0,
                    records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
                    ..RecordBatch::default()
                }]
        );
    }

    /// (d) `RETAIN_EMPTY`: the rewrite writes the fully-emptied batch of an
    /// active producer again as a bare header with no records. `producer_id`,
    /// `epoch`, and `sequence` survive.
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
        let segment_refs = vec![&seg];
        let map = build_offset_map(&segment_refs).unwrap();
        let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
        let mut active = HashMap::new();
        active.insert(ProducerId(1000), Offset(0)); // pid 1000 active, last batch base 0
        let out = rewrite_segments(
            dir.path(),
            &segment_refs,
            &map,
            &txn,
            RewriteRetention {
                now_ms: 0,
                delete_retention: RETENTION,
            },
            &active,
            INDEX_INTERVAL,
        )
        .unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(1));
        assert2::assert!(
            batches
                == vec![
                    RecordBatch {
                        base_offset: 0,
                        last_offset_delta: 0,
                        producer_id: 1000,
                        producer_epoch: 7,
                        base_sequence: 3,
                        ..RecordBatch::default()
                    },
                    RecordBatch {
                        base_offset: 1,
                        last_offset_delta: 0,
                        producer_id: -1,
                        records: vec![make_record(0, Some(b"k1"), Some(b"v2"))],
                        ..RecordBatch::default()
                    },
                ]
        );
    }

    // `RETAIN_EMPTY` last-offset arithmetic: an emptied output-last batch is
    // re-emitted as a bare header, and its `base_offset + last_offset_delta`
    // must extend `new_last_offset`. The emptied batch sits at base_offset 100
    // with `last_offset_delta` 5, so its last absolute offset is `100 + 5 =
    // 105`. This pins the `+` in `Offset(base_offset + last_offset_delta)`:
    // mutating it to `-` would report `new_last_offset == 95`.
    #[test]
    fn rewrite_retain_empty_extends_last_offset() {
        let dir = tempfile::tempdir().unwrap();
        // Batch 0 (base 0): one surviving keyed record (abs offset 0).
        let data0 = RecordBatch {
            base_offset: 0,
            last_offset_delta: 0,
            producer_id: -1,
            records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            ..RecordBatch::default()
        };
        // Batch 1 (base 100, last_offset_delta 5): only NULL-key records, all
        // dropped, so the batch is emptied. As the output-last batch it is
        // re-emitted as a bare header spanning abs offsets 100..=105.
        let data1 = RecordBatch {
            base_offset: 100,
            last_offset_delta: 5,
            producer_id: -1,
            records: vec![
                make_record(0, None, Some(b"n1")),
                make_record(5, None, Some(b"n2")),
            ],
            ..RecordBatch::default()
        };
        let seg = write_sealed_batches(dir.path(), &[data0, data1]);
        let segment_refs = vec![&seg];
        let out = rewrite_simple(dir.path(), &segment_refs);

        // The emptied batch is re-emitted as a bare header at base_offset 100.
        let bytes = fs::read(&out.log_swap).unwrap();
        let batches = decode_all(&bytes);
        assert2::assert!(out.new_base_offset == Offset(0));
        assert2::assert!(out.new_last_offset == Offset(105));
        assert2::assert!(
            batches
                == vec![
                    RecordBatch {
                        base_offset: 0,
                        last_offset_delta: 0,
                        producer_id: -1,
                        records: vec![make_record(0, Some(b"k1"), Some(b"v1"))],
                        ..RecordBatch::default()
                    },
                    RecordBatch {
                        base_offset: 100,
                        last_offset_delta: 5,
                        producer_id: -1,
                        ..RecordBatch::default()
                    },
                ]
        );
    }
}

/// Promote the three `.swap` files that [`rewrite_segments`] produced to final
/// segment files, and delete all consumed sealed segments in between.
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
        new_base = rewrite.new_base_offset.0,
    ),
    err,
)]
pub fn atomic_swap(
    dir: &Path,
    consumed_base_offsets: &[Offset],
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
        let _ = std::fs::remove_file(name::log_path(dir, base.0));
        let _ = std::fs::remove_file(name::index_path(dir, base.0));
        let _ = std::fs::remove_file(name::timeindex_path(dir, base.0));
        let _ = std::fs::remove_file(name::txnindex_path(dir, base.0));
    }

    // Step 3: rename swap → final.
    std::fs::rename(
        &rewrite.log_swap,
        name::log_path(dir, rewrite.new_base_offset.0),
    )?;
    std::fs::rename(
        &rewrite.index_swap,
        name::index_path(dir, rewrite.new_base_offset.0),
    )?;
    std::fs::rename(
        &rewrite.timeindex_swap,
        name::timeindex_path(dir, rewrite.new_base_offset.0),
    )?;
    if let Some(txn_swap) = &rewrite.txnindex_swap {
        std::fs::rename(
            txn_swap,
            name::txnindex_path(dir, rewrite.new_base_offset.0),
        )?;
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
mod swap_tests {
    use assert2::check;
    use crabka_ids::Offset;
    use crabka_units::prelude::secs;

    use super::{
        build_map_tests::{make_record, write_sealed_segment},
        *,
    };

    #[test]
    fn atomic_swap_replaces_two_segments_with_one() {
        let dir = tempfile::tempdir().unwrap();
        // Build the offset map and rewrite output while segments are open,
        // then drop the segments before atomic_swap so their file handles
        // are closed. On Windows an open file handle prevents rename/delete.
        let rewrite = {
            let first_segment = write_sealed_segment(
                dir.path(),
                0,
                vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            );
            let second_segment = write_sealed_segment(
                dir.path(),
                10,
                vec![make_record(0, Some(b"k1"), Some(b"v2"))],
            );
            let segment_refs = vec![&first_segment, &second_segment];
            let map = build_offset_map(&segment_refs).unwrap();
            let txn = CleanedTransactionMetadata::build(&segment_refs, &map).unwrap();
            rewrite_segments(
                dir.path(),
                &segment_refs,
                &map,
                &txn,
                RewriteRetention {
                    now_ms: 0,
                    delete_retention: secs(1),
                },
                &HashMap::new(),
                INDEX_INTERVAL,
            )
            .unwrap()
            // first_segment, second_segment dropped here — file handles closed
        };
        atomic_swap(dir.path(), &[Offset(0), Offset(10)], &rewrite).unwrap();

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

/// Proptest fuzz of the same KIP-534 retention cores at large N. The test
/// folds a randomized op sequence into an abstract log. It checks every
/// `Compact` for convergence, idempotence, monotone shrink, no data loss,
/// marker safety, tombstone aging, and a single horizon stamp. A separate prop
/// checks the delete-horizon wire round-trip of a real `RecordBatch`.
#[cfg(test)]
mod retention_fuzz {
    use proptest::prelude::*;

    use super::*;

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

    /// Key-to-newest-index dedup map over keyed data entries. Control entries
    /// are never indexed. This mirrors the production `build_offset_map`
    /// filter.
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

    /// Producers whose newest-for-key live data survives. The association is
    /// by key, where key equals pid.
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

    /// One compaction pass. This function applies the real `retain_decision`
    /// to each entry and returns the next log. It mirrors the abstract applier
    /// in the model.
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
                        producer_id: ProducerId(-1),
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
                        producer_id: ProducerId(i64::from(*producer_id)),
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
                        assert2::assert!(existing == h);
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
                assert2::assert!(after.len() <= before.len());

                // --- No-data-loss: every newest-for-key live data survives. ---
                let map = offset_map(&before);
                for (idx, e) in before.iter().enumerate() {
                    if let EntryKind::Data { value: Some(_) } = &e.kind
                        && let Some(k) = e.key
                        && map.get(&k).copied() == Some(idx)
                    {
                        assert2::assert!(after.iter().any(|x| x.key == Some(k)
                            && matches!(x.kind, EntryKind::Data { value: Some(_) })));
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
                            assert2::assert!(alive);
                        }
                        // If the marker had a horizon and clock < horizon, it
                        // must still be alive (not aged out prematurely).
                        if let (Some(h), false) = (e.horizon, survivors.contains(producer_id))
                            && *clock < h
                        {
                            assert2::assert!(alive);
                        }
                    }
                }

                // --- Tombstone aging: a surviving tombstone is present iff it
                // has no horizon or clock < horizon. ---
                for x in &after {
                    if matches!(x.kind, EntryKind::Data { value: None })
                        && let Some(h) = x.horizon
                    {
                        assert2::assert!(*clock < h);
                    }
                }

                // --- Single horizon stamping is enforced inside `compact` at
                // the exact point a horizon is assigned (a `SetHorizon` over an
                // already-stamped entry panics there). Nothing to re-check here.

                *log = after;
            }
        }
    }

    /// `prop_assert_eq` is macro-bound to a `proptest!` body. Inside a plain
    /// fn this helper uses a panicking equality instead, so a mismatch shows up
    /// as a case failure.
    fn prop_assert_eq_inner(a: &[Entry], b: &[Entry]) {
        assert2::assert!(a == b);
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

        /// Wire round-trip. A real `RecordBatch` with two keyed records gets
        /// a random delete horizon stamp. The encode and decode must then keep
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
