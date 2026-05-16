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
//! as the most-recent entry for their key. Slice 18b adds
//! `delete.retention.ms` to age them out.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use crabka_protocol::records::RecordBatch;

use crate::error::LogError;
use crate::name;
use crate::segment::Segment;

/// Read every `RecordBatch` from a sealed segment by streaming the
/// whole `.log` file. The segment's offset/time indexes are not used —
/// compaction reads all batches regardless of sparse-index granularity.
fn read_all_batches(seg: &Segment) -> Result<Vec<RecordBatch>, LogError> {
    // `Segment::read` already streams from the lowest indexed position
    // and bounds by `max_bytes`. For compaction we want every batch in
    // the segment, so use a max_bytes large enough to cover the file
    // (segment.bytes is at most a few GiB; usize on 64-bit hosts is
    // ample). On 32-bit hosts the cast saturates to usize::MAX.
    let max_bytes = usize::try_from(seg.size_bytes()).unwrap_or(usize::MAX);
    seg.read(seg.base_offset(), max_bytes)
}

/// Build a map of `key → latest absolute offset` across the given
/// sealed segments in input order. Records with `key.is_none()` are
/// excluded (they will be dropped by [`rewrite_segments`]).
///
/// The map's value is the absolute offset of the **newest** record
/// observed for each key (later writes overwrite earlier ones).
pub fn build_offset_map(segments: &[&Segment]) -> Result<HashMap<Vec<u8>, i64>, LogError> {
    let mut map: HashMap<Vec<u8>, i64> = HashMap::new();
    for seg in segments {
        for batch in read_all_batches(seg)? {
            for record in &batch.records {
                let Some(key_bytes) = record.key.as_ref() else {
                    continue;
                };
                if key_bytes.is_empty() {
                    // Zero-length keys are legal in Kafka and dedup-able as a
                    // distinct "empty key". Kafka treats them like any other key.
                }
                let absolute = batch.base_offset + i64::from(record.offset_delta);
                map.insert(key_bytes.to_vec(), absolute);
            }
        }
    }
    Ok(map)
}

#[cfg(test)]
mod build_map_tests {
    use super::*;
    use bytes::Bytes;
    use crabka_protocol::records::{Attributes, Record};
    use tempfile::tempdir;

    pub(super) fn make_record(offset_delta: i32, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
        Record {
            offset_delta,
            key: key.map(|k| Bytes::copy_from_slice(k)),
            value: value.map(|v| Bytes::copy_from_slice(v)),
            ..Default::default()
        }
    }

    pub(super) fn write_sealed_segment(dir: &Path, base_offset: i64, records: Vec<Record>) -> Segment {
        let mut seg = Segment::create(dir, base_offset).unwrap();
        let n = records.len() as i32;
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
        assert_eq!(map.get(b"k1".as_ref()), Some(&2));
        assert_eq!(map.get(b"k2".as_ref()), Some(&1));
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
        assert_eq!(map.len(), 1);
        assert_eq!(map.get(b"k1".as_ref()), Some(&1));
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
        assert_eq!(map.get(b"k1".as_ref()), Some(&10));
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
    pub new_last_offset: i64,
}

/// Stream `segments` (oldest → newest) into new `.swap` files, dropping
/// records whose key is missing or whose offset is not the newest known
/// offset for that key (per `offset_map`).
///
/// Records keep their **absolute** offsets — the output `RecordBatch`es
/// may contain gaps in their `offset_delta` values where superseded
/// records used to live. This matches Kafka's on-disk format for
/// compacted topics.
///
/// The `.swap` files are written to the segments' shared directory.
/// Caller is responsible for fsyncing + promoting via
/// [`atomic_swap`].
pub fn rewrite_segments(
    dir: &Path,
    segments: &[&Segment],
    offset_map: &HashMap<Vec<u8>, i64>,
    _index_interval_bytes: u32,
) -> Result<RewriteOutput, LogError> {
    let first = segments.first().ok_or_else(|| LogError::Io(
        std::io::Error::other("rewrite_segments: empty input"),
    ))?;
    let new_base = first.base_offset();

    let log_swap = swap_path(dir, new_base, "log");
    let index_swap = swap_path(dir, new_base, "index");
    let timeindex_swap = swap_path(dir, new_base, "timeindex");

    // Truncate (or create) all three swap files. We rewrite the .log
    // file proper here; for the index sidecars we write empty files
    // and let Segment::open populate them via tail-scan in T3's
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

    let mut last_kept_offset = new_base - 1;

    for seg in segments {
        for batch in read_all_batches(seg)? {
            let mut kept: Vec<crabka_protocol::records::Record> = Vec::with_capacity(batch.records.len());
            for record in batch.records.iter() {
                let Some(key_bytes) = record.key.as_ref() else {
                    continue;
                };
                let absolute = batch.base_offset + i64::from(record.offset_delta);
                if offset_map.get(key_bytes.as_ref()).copied() == Some(absolute) {
                    kept.push(record.clone());
                }
            }
            if kept.is_empty() {
                continue;
            }

            // Compute new last_offset_delta covering the kept range
            // (relative to the batch's original base_offset). Kafka
            // preserves base_offset and only updates last_offset_delta
            // when records are removed mid-batch.
            let last_delta = kept
                .iter()
                .map(|r| r.offset_delta)
                .max()
                .expect("kept non-empty");
            let out_batch = RecordBatch {
                base_offset: batch.base_offset,
                last_offset_delta: last_delta,
                max_timestamp: batch.max_timestamp,
                attributes: batch.attributes,
                records: kept,
                ..batch.clone()
            };

            let mut buf = BytesMut::with_capacity(out_batch.encoded_len());
            out_batch.encode(&mut buf)?;
            log_file.write_all(&buf)?;

            let batch_last = out_batch.base_offset + i64::from(out_batch.last_offset_delta);
            if batch_last > last_kept_offset {
                last_kept_offset = batch_last;
            }
        }
    }
    log_file.sync_all()?;

    Ok(RewriteOutput {
        log_swap,
        index_swap,
        timeindex_swap,
        new_base_offset: new_base,
        new_last_offset: last_kept_offset,
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
mod rewrite_tests {
    use super::*;
    use super::build_map_tests::{make_record, write_sealed_segment};
    use std::fs;

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
        let map = build_offset_map(&segs).unwrap();
        let out = rewrite_segments(dir.path(), &segs, &map, 4096).unwrap();
        assert_eq!(out.new_base_offset, 0);

        // Decode the swap .log to verify contents.
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert_eq!(batch.records.len(), 2);
        let keys: Vec<_> = batch.records.iter()
            .map(|r| r.key.as_ref().unwrap().to_vec())
            .collect();
        assert_eq!(keys, vec![b"k2".to_vec(), b"k1".to_vec()]);
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
        let map = build_offset_map(&segs).unwrap();
        let out = rewrite_segments(dir.path(), &segs, &map, 4096).unwrap();
        let bytes = fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert!(batch.records[0].value.is_none());
        assert_eq!(batch.records[0].key.as_ref().unwrap().as_ref(), b"k1");
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
        let map = build_offset_map(&segs).unwrap();
        let out = rewrite_segments(dir.path(), &segs, &map, 4096).unwrap();
        assert_eq!(out.new_base_offset, 100);
        assert_eq!(out.new_last_offset, 102);

        let bytes = std::fs::read(&out.log_swap).unwrap();
        let mut cursor = &bytes[..];
        let batch = RecordBatch::decode(&mut cursor).unwrap();
        assert_eq!(batch.base_offset, 100);
        // k2 kept at offset_delta 1, k1 kept at offset_delta 2; base 100,
        // last_offset_delta 2 → batch covers abs offsets 100..=102 with k2,k1.
        assert_eq!(batch.last_offset_delta, 2);
        let abs_offsets: Vec<i64> = batch.records.iter()
            .map(|r| batch.base_offset + i64::from(r.offset_delta))
            .collect();
        assert_eq!(abs_offsets, vec![101, 102]);
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
pub fn atomic_swap(
    dir: &Path,
    consumed_base_offsets: &[i64],
    rewrite: &RewriteOutput,
) -> Result<(), LogError> {
    // Step 1: fsync swap files. Open with write access so
    // `FlushFileBuffers` (Windows) / `fsync` (Linux) succeeds.
    OpenOptions::new().write(true).open(&rewrite.log_swap)?.sync_all()?;
    OpenOptions::new().write(true).open(&rewrite.index_swap)?.sync_all()?;
    OpenOptions::new().write(true).open(&rewrite.timeindex_swap)?.sync_all()?;

    // Step 2: delete originals.
    for base in consumed_base_offsets {
        let _ = std::fs::remove_file(name::log_path(dir, *base));
        let _ = std::fs::remove_file(name::index_path(dir, *base));
        let _ = std::fs::remove_file(name::timeindex_path(dir, *base));
    }

    // Step 3: rename swap → final.
    std::fs::rename(&rewrite.log_swap, name::log_path(dir, rewrite.new_base_offset))?;
    std::fs::rename(&rewrite.index_swap, name::index_path(dir, rewrite.new_base_offset))?;
    std::fs::rename(
        &rewrite.timeindex_swap,
        name::timeindex_path(dir, rewrite.new_base_offset),
    )?;

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
    use super::*;
    use super::build_map_tests::{make_record, write_sealed_segment};

    #[test]
    fn atomic_swap_replaces_two_segments_with_one() {
        let dir = tempfile::tempdir().unwrap();
        // Build the offset map and rewrite output while segments are open,
        // then drop the segments before atomic_swap so their file handles
        // are closed. On Windows an open file handle prevents rename/delete.
        let rewrite = {
            let seg0 = write_sealed_segment(
                dir.path(), 0,
                vec![make_record(0, Some(b"k1"), Some(b"v1"))],
            );
            let seg1 = write_sealed_segment(
                dir.path(), 10,
                vec![make_record(0, Some(b"k1"), Some(b"v2"))],
            );
            let segs = vec![&seg0, &seg1];
            let map = build_offset_map(&segs).unwrap();
            rewrite_segments(dir.path(), &segs, &map, 4096).unwrap()
            // seg0, seg1 dropped here — file handles closed
        };
        atomic_swap(dir.path(), &[0, 10], &rewrite).unwrap();

        // After swap: only one .log (base 0). The base 10 segment is gone.
        assert!(name::log_path(dir.path(), 0).exists());
        assert!(!name::log_path(dir.path(), 10).exists());
        // No leftover .swap files.
        assert!(!dir.path().join("00000000000000000000.log.swap").exists());
    }
}
