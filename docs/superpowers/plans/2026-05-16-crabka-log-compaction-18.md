# Slice 18: Log compaction (cleanup.policy=compact) — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Implement Kafka-compatible log compaction for topics with `cleanup.policy=compact`. End-to-end: per-topic config, pure-function compaction engine, `.swap` orphan recovery, per-broker cleaner ticker, broker integration test, and one JVM acceptance test.

**Architecture:** A new `crates/log/src/compact.rs` defines three pure-ish primitives (`build_offset_map`, `rewrite_segments`, `atomic_swap`). `Log::compact` orchestrates them over the sealed-segment list, leaving the active segment untouched. `Log::open` heals `.swap` orphans on recovery. A new `crates/broker/src/cleaner.rs` ticker spawned from `Broker::start` walks the partition registry every 30 s; for each leader-owned partition with `cleanup.policy=compact` it sends a new `WriterMessage::Compact` through the existing partition writer actor, preserving the single-writer invariant.

**Tech Stack:** Rust 1.95.0. Builds on existing `crates/log` primitives (`Segment`, `RecordBatch` codec, `name::*_path` helpers). No new dependencies.

**Reference spec:** [`docs/superpowers/specs/2026-05-16-crabka-log-compaction-18-design.md`](../specs/2026-05-16-crabka-log-compaction-18-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/log-compaction-18` already created with spec committed at `6333f9f`.

---

## File structure

```
crates/log/src/
├── compact.rs                                      # NEW — build_offset_map + rewrite_segments + atomic_swap + tests
├── config.rs                                       # MODIFIED — CleanupPolicy enum + LogConfig.cleanup_policy field
├── lib.rs                                          # MODIFIED — pub use CleanupPolicy
├── log.rs                                          # MODIFIED — Log::compact() method + .swap recovery in open() + tests
└── recovery.rs                                     # MODIFIED — swap_orphan_recover() helper
crates/broker/src/
├── cleaner.rs                                      # NEW — Cleaner ticker
├── broker.rs                                       # MODIFIED — spawn Cleaner alongside leader_rebalance
├── config_keys.rs                                  # MODIFIED — accept cleanup.policy=compact + apply
├── partition.rs                                    # MODIFIED — WriterMessage::Compact + Partition::compact_log()
└── partition_writer.rs                             # MODIFIED — handle WriterMessage::Compact
crates/broker/tests/
├── compaction.rs                                   # NEW — broker integration test
└── jvm_acceptance.rs                               # MODIFIED — 1 new JVM test
```

**8 tasks across 4 batches.**

- **Batch 1 (parallel):** T1 LogConfig/CleanupPolicy, T2 compact.rs primitives
- **Batch 2 (parallel):** T3 Log::compact + `.swap` recovery (in log.rs + recovery.rs), T4 config_keys.rs validation/apply
- **Batch 3 (alone):** T5 WriterMessage::Compact + Cleaner task + broker spawn
- **Batch 4 (parallel):** T6 broker integration test, T7 JVM acceptance test

---

## Batch 1 — Config types & compaction primitives (parallel: T1, T2)

### Task 1: `CleanupPolicy` enum + `LogConfig` field

**Files:**
- Modify: `crates/log/src/config.rs`
- Modify: `crates/log/src/lib.rs`

- [ ] **Step 1: Add `CleanupPolicy` enum and field**

Edit `crates/log/src/config.rs`. After `use std::time::Duration;` (line 3), add:

```rust
/// Per-topic policy for what to do with old log segments.
///
/// `Delete` (default): age- or size-based segment deletion via
/// [`crate::retention`]. `Compact`: newest-wins dedup-by-key,
/// implemented in [`crate::compact`] and invoked through
/// [`crate::Log::compact`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CleanupPolicy {
    #[default]
    Delete,
    Compact,
}
```

In the `LogConfig` struct, add a new field after `validate_on_open: bool,`:

```rust
    /// Cleanup policy. Defaults to `Delete`. See [`CleanupPolicy`].
    pub cleanup_policy: CleanupPolicy,
```

In the `Default` impl for `LogConfig`, add to the struct literal:

```rust
            cleanup_policy: CleanupPolicy::Delete,
```

(Place it as the last field, matching the struct order.)

- [ ] **Step 2: Export from `lib.rs`**

Edit `crates/log/src/lib.rs`. The existing line is:

```rust
pub use config::LogConfig;
```

Change it to:

```rust
pub use config::{CleanupPolicy, LogConfig};
```

- [ ] **Step 3: Add a unit test for the default**

Append to the `#[cfg(test)] mod tests` block at the bottom of `crates/log/src/config.rs`:

```rust
    #[test]
    fn default_cleanup_policy_is_delete() {
        let c = LogConfig::default();
        assert_eq!(c.cleanup_policy, CleanupPolicy::Delete);
    }
```

- [ ] **Step 4: Compile + run tests**

```
cargo test -p crabka-log --lib config::tests
```

Expected: clean compile; the existing `defaults_match_kafka_4x` test still passes; new `default_cleanup_policy_is_delete` passes.

- [ ] **Step 5: Commit**

```
git add crates/log/src/config.rs crates/log/src/lib.rs
git commit -m "feat(slice-18): CleanupPolicy enum + LogConfig.cleanup_policy field" -m "Defaults to Delete (matches Kafka). T2 adds the compaction engine; T4 wires the broker config key; T5 wires the cleaner." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Compaction primitives in `compact.rs`

**Files:**
- Create: `crates/log/src/compact.rs`

This task implements the three pure-ish primitives that `Log::compact` will orchestrate. Task does NOT modify `Log::open`/`Log::compact` — those are wired in T3.

- [ ] **Step 1: Read the segment reader for context**

Read `crates/log/src/segment.rs` lines 192–238 (`Segment::read` and `read_log_range`). The compaction reader uses the same `RecordBatch::decode` loop but reads the entire segment (not a max-bytes bounded slice). Read `crates/log/src/name.rs` for the `log_path`/`index_path`/`timeindex_path`/`format_base_offset` helpers — you'll write `<base>.log.swap`, `<base>.index.swap`, `<base>.timeindex.swap` alongside the originals.

- [ ] **Step 2: Create `crates/log/src/compact.rs` with module header + read helper**

Write the file with this initial scaffold (subsequent steps fill in the bodies):

```rust
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
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use bytes::BytesMut;
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};

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
```

- [ ] **Step 3: Add `build_offset_map` + tests**

Append:

```rust
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

    fn make_record(offset_delta: i32, key: Option<&[u8]>, value: Option<&[u8]>) -> Record {
        Record {
            offset_delta,
            key: key.map(|k| Bytes::copy_from_slice(k)),
            value: value.map(|v| Bytes::copy_from_slice(v)),
            ..Default::default()
        }
    }

    fn write_sealed_segment(dir: &Path, base_offset: i64, records: Vec<Record>) -> Segment {
        let mut seg = Segment::create(dir, base_offset).unwrap();
        let n = records.len() as i32;
        let max_ts = records.iter().map(|r| r.timestamp_delta as i64).max().unwrap_or(0);
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
```

**Note:** the test helper writes records via a single batch then seals — `Segment::seal()` may need adapting. If `Segment` exposes only a private `sealed` field, add a small `Segment::seal_for_test()` test helper in `segment.rs` gated by `#[cfg(any(test, feature = "test-helpers"))]`. Verify with `git grep "fn seal" crates/log/src/segment.rs` whether one already exists.

- [ ] **Step 4: Add `rewrite_segments` + tests**

Append to `crates/log/src/compact.rs`:

```rust
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
            let mut kept: Vec<Record> = Vec::with_capacity(batch.records.len());
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
```

- [ ] **Step 5: Add `atomic_swap` + tests**

Append to `crates/log/src/compact.rs`:

```rust
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
    // Step 1: fsync swap files. `OpenOptions::write(true).open(...)` is
    // cheaper than reopening with `create(true).append(false)`.
    File::open(&rewrite.log_swap)?.sync_all()?;
    File::open(&rewrite.index_swap)?.sync_all()?;
    File::open(&rewrite.timeindex_swap)?.sync_all()?;

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
    // (`File::open` on a dir fails with EACCES); guard the call.
    #[cfg(unix)]
    {
        if let Ok(d) = File::open(dir) {
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
        let rewrite = rewrite_segments(dir.path(), &segs, &map, 4096).unwrap();
        atomic_swap(dir.path(), &[0, 10], &rewrite).unwrap();

        // After swap: only one .log (base 0). The base 10 segment is gone.
        assert!(name::log_path(dir.path(), 0).exists());
        assert!(!name::log_path(dir.path(), 10).exists());
        // No leftover .swap files.
        assert!(!dir.path().join("00000000000000000000.log.swap").exists());
    }
}
```

- [ ] **Step 6: Register the module in `crates/log/src/lib.rs`**

Add `mod compact;` near the other module declarations (alphabetical between `recovery` and `retention` is fine, but the existing file has them in a custom order — find the existing `mod retention;` line and add `mod compact;` just before it):

```rust
mod compact;
```

Make `compact` `pub(crate)` if its primitives need to be visible to `crate::log` (they do for T3). Use `mod compact;` (private to the crate) — `Log::compact()` in `log.rs` uses `crate::compact::*` for the primitives but they don't need to be `pub`.

- [ ] **Step 7: Compile + run tests**

```
cargo test -p crabka-log --lib compact::
```

Expected: all primitive tests pass. If `Segment::seal()` doesn't exist publicly, add `pub fn seal(&mut self) { self.sealed = true; }` to `segment.rs` (the bare assignment is fine — sealing is a flag flip, no I/O).

- [ ] **Step 8: Commit**

```
git add crates/log/src/compact.rs crates/log/src/lib.rs crates/log/src/segment.rs
git commit -m "feat(slice-18): compaction primitives (build_offset_map + rewrite_segments + atomic_swap)" -m "Pure-function helpers in crates/log/src/compact.rs covering the three steps of a compaction pass. 6 unit tests. T3 wires them into Log::compact." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — `Log::compact` + recovery + broker config (parallel: T3, T4)

### Task 3: `Log::compact` method + `.swap` orphan recovery

**Files:**
- Modify: `crates/log/src/log.rs` — add `compact()` method + integration tests
- Modify: `crates/log/src/recovery.rs` — `.swap` orphan healing helper
- Modify: `crates/log/src/log.rs` — call recovery helper in `Log::open`

- [ ] **Step 1: Add `.swap` orphan recovery in `recovery.rs`**

Edit `crates/log/src/recovery.rs`. Replace the existing stub contents with:

```rust
//! Open-time recovery for log directories.
//!
//! - `Segment::open_active` handles partial trailing batches in the
//!   active segment.
//! - [`swap_orphan_recover`] handles `.swap` files left behind by an
//!   interrupted [`crate::compact::atomic_swap`].

use std::collections::HashSet;
use std::path::Path;

use crate::error::LogError;
use crate::name;

/// Heal any `<base>.log.swap` triples found in `dir`:
///
/// - If the matching plain `<base>.log` exists, the swap was in
///   step 1 or 2 (originals still authoritative) → delete the swap
///   triple.
/// - Else the swap was in step 3 mid-rename (originals deleted,
///   `.swap` files complete) → finish the rename to final names.
///
/// Idempotent. Safe to call on every `Log::open`.
pub fn swap_orphan_recover(dir: &Path) -> Result<(), LogError> {
    let entries = std::fs::read_dir(dir)?;
    let mut log_swaps: Vec<i64> = Vec::new();
    let mut existing_log_bases: HashSet<i64> = HashSet::new();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".log.swap")
            && stem.len() == name::FILENAME_DIGITS
            && let Ok(base) = stem.parse::<i64>()
        {
            log_swaps.push(base);
        }
        if let Ok(base) = name::parse_log_filename(name) {
            existing_log_bases.insert(base);
        }
    }

    for base in log_swaps {
        let log_swap = swap_triple(dir, base, "log");
        let index_swap = swap_triple(dir, base, "index");
        let timeindex_swap = swap_triple(dir, base, "timeindex");

        if existing_log_bases.contains(&base) {
            // Orphan partial — discard.
            let _ = std::fs::remove_file(&log_swap);
            let _ = std::fs::remove_file(&index_swap);
            let _ = std::fs::remove_file(&timeindex_swap);
        } else {
            // Complete swap interrupted mid-rename — promote.
            std::fs::rename(&log_swap, name::log_path(dir, base))?;
            // The index / timeindex .swap files may not exist if the
            // crash happened *between* the three renames. Tolerate
            // missing sidecars — `Segment::open` accepts empty index files
            // and rebuilds on tail-scan.
            if index_swap.exists() {
                std::fs::rename(&index_swap, name::index_path(dir, base))?;
            } else {
                std::fs::File::create(name::index_path(dir, base))?;
            }
            if timeindex_swap.exists() {
                std::fs::rename(&timeindex_swap, name::timeindex_path(dir, base))?;
            } else {
                std::fs::File::create(name::timeindex_path(dir, base))?;
            }
        }
    }
    Ok(())
}

fn swap_triple(dir: &Path, base: i64, ext: &str) -> std::path::PathBuf {
    dir.join(format!(
        "{}.{}.swap",
        name::format_base_offset(base),
        ext
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn touch(path: &std::path::Path) {
        std::fs::File::create(path).unwrap();
    }

    #[test]
    fn discards_swap_when_original_log_still_present() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        touch(&name::log_path(p, 0));
        touch(&name::index_path(p, 0));
        touch(&name::timeindex_path(p, 0));
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        assert!(name::log_path(p, 0).exists());
        assert!(!p.join("00000000000000000000.log.swap").exists());
        assert!(!p.join("00000000000000000000.index.swap").exists());
        assert!(!p.join("00000000000000000000.timeindex.swap").exists());
    }

    #[test]
    fn promotes_swap_when_original_log_missing() {
        let dir = tempdir().unwrap();
        let p = dir.path();
        // No originals — only .swap triples (= post-step-2, pre-step-3).
        touch(&p.join("00000000000000000000.log.swap"));
        touch(&p.join("00000000000000000000.index.swap"));
        touch(&p.join("00000000000000000000.timeindex.swap"));
        swap_orphan_recover(p).unwrap();
        assert!(name::log_path(p, 0).exists());
        assert!(name::index_path(p, 0).exists());
        assert!(name::timeindex_path(p, 0).exists());
        assert!(!p.join("00000000000000000000.log.swap").exists());
    }
}
```

Remove the existing `#![allow(dead_code)]` at the top — the file now has live code.

- [ ] **Step 2: Wire recovery into `Log::open`**

Edit `crates/log/src/log.rs`. Find the `Log::open` function (starts around line 79). At the very beginning of the function body, after the path-to-PathBuf conversion, add:

```rust
        // Slice 18: heal any orphaned compaction `.swap` files before
        // we scan the directory for segments.
        crate::recovery::swap_orphan_recover(&dir)?;
```

- [ ] **Step 3: Add `Log::compact` method**

Edit `crates/log/src/log.rs`. Just after the `pub fn tick` method (around line 642), add:

```rust
    /// Run one compaction pass over the sealed segment list. No-op if
    /// fewer than 2 sealed segments exist (nothing to dedup yet).
    ///
    /// The active segment is never touched. Output is a single new
    /// sealed segment at the lowest input base offset, replacing all
    /// consumed sealed segments.
    pub fn compact(&mut self) -> Result<(), LogError> {
        if self.segments.len() < 2 {
            return Ok(());
        }

        let cfg_guard = self.config.read().unwrap();
        if cfg_guard.cleanup_policy != crate::CleanupPolicy::Compact {
            return Ok(());
        }
        let index_interval = cfg_guard.index_interval_bytes;
        drop(cfg_guard);

        let sealed_refs: Vec<&Segment> = self.segments.iter().map(AsRef::as_ref).collect();
        let consumed_bases: Vec<i64> = sealed_refs.iter().map(|s| s.base_offset()).collect();

        let offset_map = crate::compact::build_offset_map(&sealed_refs)?;
        let rewrite = crate::compact::rewrite_segments(
            &self.dir,
            &sealed_refs,
            &offset_map,
            index_interval,
        )?;
        crate::compact::atomic_swap(&self.dir, &consumed_bases, &rewrite)?;

        // Replace the in-memory segment list with the new single
        // segment. `open_active(validate=true)` tail-scans the .log so
        // last_offset + max_timestamp are populated; then seal() flips
        // the flag so future appends correctly target the broker's
        // active segment (not this one).
        let mut new_seg = Segment::open_active(&self.dir, rewrite.new_base_offset, true)?;
        new_seg.seal();
        self.segments.clear();
        self.segments.push(Arc::new(new_seg));
        Ok(())
    }
```

- [ ] **Step 4: Add `Log::compact` integration tests**

Append to `#[cfg(test)] mod tests` in `crates/log/src/log.rs` (the existing test module at the bottom). Tests need `cleanup.policy = Compact` and force-rolled segments to exercise the algorithm. Find a helper similar to `sample_batch`; reuse or adapt.

```rust
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

    #[test]
    fn compact_no_op_when_only_one_segment() {
        let dir = tempdir().unwrap();
        let mut cfg = LogConfig::default();
        cfg.cleanup_policy = crate::CleanupPolicy::Compact;
        let mut log = Log::open(dir.path(), cfg).unwrap();
        let mut b = keyed_batch(0, &[(0, b"k1", b"v1")]);
        log.append(&mut b).unwrap();
        // Only the active segment exists; sealed list is empty.
        log.compact().unwrap();
        assert_eq!(log.log_end_offset(), 1);
    }

    #[test]
    fn compact_dedupes_sealed_segments_keeps_active_intact() {
        let dir = tempdir().unwrap();
        let mut cfg = LogConfig::default();
        cfg.cleanup_policy = crate::CleanupPolicy::Compact;
        cfg.segment_bytes = 256; // force rolls
        let mut log = Log::open(dir.path(), cfg).unwrap();

        // Write 3 sealed segments, each with one record under "k1".
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
            // Roll the active segment by forcing a tick or a large pad batch.
            // Easiest: call set_segment_bytes or rely on the small segment_bytes.
        }
        // Add one more append to ensure the last write is in a fresh active
        // segment (not part of what compaction touches).
        let mut b = keyed_batch(0, &[(0, b"active-key", b"active-value")]);
        log.append(&mut b).unwrap();

        let active_leo_before = log.log_end_offset();
        log.compact().unwrap();
        assert_eq!(log.log_end_offset(), active_leo_before,
            "compaction must not change LEO");

        // After compaction: read everything, assert only the newest k1 plus
        // the active "active-key" survive.
        let out = log.read(0, 1024 * 1024).unwrap();
        let all_records: Vec<_> = out.batches.iter()
            .flat_map(|b| b.records.iter())
            .collect();
        let keys: Vec<&[u8]> = all_records.iter()
            .map(|r| r.key.as_ref().unwrap().as_ref())
            .collect();
        assert!(keys.contains(&b"k1".as_ref()), "k1 must survive as newest");
        assert!(keys.contains(&b"active-key".as_ref()), "active segment record must survive");
    }

    #[test]
    fn compact_is_idempotent() {
        let dir = tempdir().unwrap();
        let mut cfg = LogConfig::default();
        cfg.cleanup_policy = crate::CleanupPolicy::Compact;
        cfg.segment_bytes = 256;
        let mut log = Log::open(dir.path(), cfg).unwrap();
        for i in 0..3 {
            let v = format!("v{i}");
            let mut b = keyed_batch(0, &[(0, b"k1", v.as_bytes())]);
            log.append(&mut b).unwrap();
        }
        let mut b = keyed_batch(0, &[(0, b"active", b"x")]);
        log.append(&mut b).unwrap();
        log.compact().unwrap();
        let leo1 = log.log_end_offset();
        log.compact().unwrap();
        let leo2 = log.log_end_offset();
        assert_eq!(leo1, leo2);
    }
```

**Note on test mechanics:** `Log::append` rolls segments automatically when `log_size >= segment_bytes`. With `segment_bytes = 256` and ~50-byte single-record batches, you get a new segment per few appends. If `Log` does not auto-roll based on size, find the roll mechanism via `git grep -n 'should_roll\|roll\|segment_bytes' crates/log/src/log.rs` and adapt.

- [ ] **Step 5: Compile + run tests**

```
cargo test -p crabka-log --lib log::tests
cargo test -p crabka-log --lib recovery::tests
```

Expected: all pass.

- [ ] **Step 6: Commit**

```
git add crates/log/src/log.rs crates/log/src/recovery.rs
git commit -m "feat(slice-18): Log::compact + .swap orphan recovery" -m "Compacts the sealed-segment list into a single new segment when cleanup_policy=Compact; never touches the active segment. Log::open heals any .swap orphans left by an interrupted compaction. 5 new tests." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: Broker `cleanup.policy=compact` validation + apply

**Files:**
- Modify: `crates/broker/src/config_keys.rs`

- [ ] **Step 1: Allow `compact` in `validate_topic_config`**

Edit `crates/broker/src/config_keys.rs`. Find the `CLEANUP_POLICY` arm (line 38) which currently looks like:

```rust
        CLEANUP_POLICY => {
            if value == "delete" {
                Ok(())
            } else {
                Err(format!(
                    "cleanup.policy={value} not supported; only `delete` is currently honored"
                ))
            }
        }
```

Replace with:

```rust
        CLEANUP_POLICY => match value {
            "delete" | "compact" => Ok(()),
            _ => Err(format!(
                "cleanup.policy={value} not supported; expected `delete` or `compact`"
            )),
        },
```

- [ ] **Step 2: Apply `cleanup.policy` in `apply_to_log_config`**

In the `apply_to_log_config` function (line 108), add a new match arm before the catch-all `_ => {}`:

```rust
            CLEANUP_POLICY => {
                out.cleanup_policy = if v == "compact" {
                    crabka_log::CleanupPolicy::Compact
                } else {
                    crabka_log::CleanupPolicy::Delete
                };
            }
```

- [ ] **Step 3: Update the module docstring**

The file's docstring lines 1-13 currently say "`cleanup.policy` (only `delete`)". Update to:

```rust
//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! Eight keys are recognized. Four propagate live to `Log.config`
//! (`retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`).
//! Two are accepted as no-op defaults for compatibility but reject
//! non-default values: `compression.type` (only `producer`),
//! `min.insync.replicas` (integers >= 1 accepted but not yet enforced —
//! see the design spec for the rationale). Two are KIP-73 throttle keys
//! (`leader.replication.throttled.replicas`,
//! `follower.replication.throttled.replicas`) validated via
//! `ThrottledReplicas::parse`.
//!
//! Unknown keys are rejected with `INVALID_CONFIG`.
```

- [ ] **Step 4: Add unit tests**

Find the existing `validate_retention_ms_*` tests at the bottom of the file. Append:

```rust
    #[test]
    fn validate_cleanup_policy_accepts_delete_and_compact() {
        assert!(validate_topic_config(CLEANUP_POLICY, "delete").is_ok());
        assert!(validate_topic_config(CLEANUP_POLICY, "compact").is_ok());
    }

    #[test]
    fn validate_cleanup_policy_rejects_unknown() {
        assert!(validate_topic_config(CLEANUP_POLICY, "compact,delete").is_err());
        assert!(validate_topic_config(CLEANUP_POLICY, "junk").is_err());
    }

    #[test]
    fn apply_cleanup_policy_compact_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "compact".to_string());
        let out = apply_to_log_config(&overrides, &crabka_log::LogConfig::default());
        assert_eq!(out.cleanup_policy, crabka_log::CleanupPolicy::Compact);
    }

    #[test]
    fn apply_cleanup_policy_delete_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "delete".to_string());
        let mut base = crabka_log::LogConfig::default();
        base.cleanup_policy = crabka_log::CleanupPolicy::Compact;
        let out = apply_to_log_config(&overrides, &base);
        assert_eq!(out.cleanup_policy, crabka_log::CleanupPolicy::Delete);
    }
```

- [ ] **Step 5: Compile + run tests**

```
cargo test -p crabka-broker --lib config_keys::tests
```

Expected: 4 new tests pass; existing tests still pass.

- [ ] **Step 6: Commit**

```
git add crates/broker/src/config_keys.rs
git commit -m "feat(slice-18): accept cleanup.policy=compact + propagate to LogConfig" -m "Replaces the slice-pre-18 placeholder that rejected any non-\`delete\` value. Now wires \`compact\` through to LogConfig.cleanup_policy via the existing apply_to_log_config pipeline." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Cleaner ticker + broker wiring (alone)

### Task 5: `WriterMessage::Compact` + `Cleaner` task + broker spawn

**Files:**
- Modify: `crates/broker/src/partition.rs` — add `WriterMessage::Compact` + `Partition::compact_log()` method
- Modify: `crates/broker/src/partition_writer.rs` — handle the new message
- Create: `crates/broker/src/cleaner.rs` — Cleaner ticker
- Modify: `crates/broker/src/lib.rs` — `mod cleaner;`
- Modify: `crates/broker/src/broker.rs` — spawn Cleaner

- [ ] **Step 1: Add `WriterMessage::Compact` variant**

Edit `crates/broker/src/partition.rs`. Find the `enum WriterMessage` (line 44). After the `SetLogConfig` variant, add:

```rust
    /// Run one compaction pass. The writer actor serializes this with
    /// appends to preserve the single-writer invariant on `Log`.
    Compact {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
```

- [ ] **Step 2: Add `Partition::compact_log()` method**

Find the `impl Partition` block in the same file. Near the existing send-WriterMessage methods (e.g., `set_log_config`, look around line 183), append:

```rust
    /// Send a `WriterMessage::Compact` to the partition's writer
    /// actor and await the ack. Used by the broker-wide [`Cleaner`]
    /// ticker.
    pub async fn compact_log(&self) -> Result<(), BrokerError> {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        self.writer_tx
            .send(WriterMessage::Compact { ack: ack_tx })
            .await
            .map_err(|_| BrokerError::Internal("writer task closed".into()))?;
        ack_rx
            .await
            .map_err(|_| BrokerError::Internal("compact ack dropped".into()))?
    }
```

**Note:** confirm the exact error variant — check what `set_log_config` uses for similar channel-closed cases (`git grep -n "writer task closed\|writer_tx.send" crates/broker/src/partition.rs`). Mirror that.

- [ ] **Step 3: Handle the message in the writer actor**

Edit `crates/broker/src/partition_writer.rs`. In the `match msg` block (line 26), after the `WriterMessage::SetLogConfig` arm, add:

```rust
            WriterMessage::Compact { ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.compact().map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
                // No `append_notify` — compaction doesn't produce new
                // records, only consolidates existing ones at the same
                // absolute offsets.
            }
```

- [ ] **Step 4: Create `crates/broker/src/cleaner.rs`**

Modeled on `leader_rebalance.rs`. Write:

```rust
//! Per-broker log-compaction ticker. Every `interval`, walks the
//! partitions registry and dispatches [`Partition::compact_log`] for
//! every partition where:
//!
//!   - the topic's `cleanup.policy` is `compact`, and
//!   - this broker is currently the leader.
//!
//! The actual compaction runs on the partition's writer actor, so
//! appends and compaction are serialized.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crabka_metadata::NodeId;

use crate::partition::Partition;

/// Tunables for [`run`].
#[derive(Debug, Clone)]
pub(crate) struct CleanerConfig {
    pub interval: Duration,
}

impl Default for CleanerConfig {
    fn default() -> Self {
        Self { interval: Duration::from_secs(30) }
    }
}

/// Spawned task entry point.
pub(crate) async fn run(
    partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    node_id: NodeId,
    cfg: CleanerConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            () = shutdown.cancelled() => {
                debug!("cleaner task shutting down");
                return;
            }
        }
        tick_all(&partitions, node_id).await;
    }
}

pub(crate) async fn tick_all(
    partitions: &DashMap<(String, i32), Arc<Partition>>,
    node_id: NodeId,
) {
    // Snapshot first to avoid holding the DashMap iter across await.
    let snapshot: Vec<Arc<Partition>> = partitions.iter()
        .map(|kv| kv.value().clone())
        .collect();
    for partition in snapshot {
        let leader = partition.current_leader.load(Ordering::Relaxed);
        if leader != u64::from(node_id) {
            continue;
        }
        let policy = {
            let log = partition.log.lock().expect("log mutex poisoned");
            log.config().cleanup_policy
        };
        if policy != crabka_log::CleanupPolicy::Compact {
            continue;
        }
        if let Err(e) = partition.compact_log().await {
            warn!(
                topic = %partition.topic,
                partition_id = partition.partition_id,
                error = %e,
                "compaction failed for partition",
            );
        }
    }
}
```

**Note 1:** `Log` may not expose `config()`. If `git grep -n 'pub fn config\|pub fn cleanup_policy' crates/log/src/log.rs` returns nothing, add a small accessor in `Log`:

```rust
/// Snapshot the current config (cheap clone of `LogConfig`).
#[must_use]
pub fn config(&self) -> LogConfig {
    self.config.read().unwrap().clone()
}
```

(Place near `set_config` if it exists, else near the end of `impl Log`.)

**Note 2:** `current_leader` is `Arc<AtomicU64>` per the existing struct. The `u64::from(node_id)` conversion assumes `NodeId` is `u32` or `u64`; check via `git grep -n 'pub struct NodeId\|type NodeId\b'`. Adjust the cast if needed.

- [ ] **Step 5: Register module in `crates/broker/src/lib.rs`**

Add `pub(crate) mod cleaner;` alongside the other module declarations in `crates/broker/src/lib.rs`. Find the alphabetical position (between `broker` and `codes` or similar).

- [ ] **Step 6: Spawn the cleaner from `Broker::start`**

Edit `crates/broker/src/broker.rs`. Find the `tokio::spawn(crate::leader_rebalance::run(...))` block (line 1013) — the cleaner spawn fits the same pattern. After the `leader_rebalance` spawn (and its containing `if`/braces), add:

```rust
        // KIP-N/A: per-broker log compaction ticker. Always-on; the
        // cleaner internally filters to (leader && cleanup.policy=compact)
        // partitions so brokers with no compact topics pay nothing.
        {
            let partitions = partitions.clone();
            let shutdown = supervisor_shutdown.child_token();
            let cfg = crate::cleaner::CleanerConfig::default();
            tokio::spawn(crate::cleaner::run(
                partitions,
                config.node_id,
                cfg,
                shutdown,
            ));
        }
```

- [ ] **Step 7: Compile + run tests**

```
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
```

Expected: clean compile + clippy; existing 274+ broker lib tests pass.

- [ ] **Step 8: Commit**

```
git add crates/broker/src/partition.rs crates/broker/src/partition_writer.rs crates/broker/src/cleaner.rs crates/broker/src/lib.rs crates/broker/src/broker.rs crates/log/src/log.rs
git commit -m "feat(slice-18): per-broker cleaner ticker + WriterMessage::Compact" -m "Cleaner spawns from Broker::start, ticks every 30s, dispatches Partition::compact_log() for leader-owned partitions with cleanup.policy=compact. Compaction runs on the partition writer actor to preserve the single-writer invariant." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Tests (parallel: T6, T7)

### Task 6: Broker integration test — compaction dedupes via native client

**Files:**
- Create: `crates/broker/tests/compaction.rs`

- [ ] **Step 1: Locate the test scaffold**

Read `crates/broker/tests/throttle.rs` for the canonical non-SASL single-broker integration-test idiom (it's the simplest production-flow test in the repo). Identify:

- How the broker is spawned (look for `spawn_broker`, `single_broker_no_sasl`, or `common/mod.rs`)
- How a topic is created with a `cleanup.policy` config override (look for `kafka-topics --config` or `CreateTopicsRequest` helpers)
- How produces with explicit keys flow (native client's `Producer::send` with a key)
- How fetches with consume-from-zero are driven (native client's `Consumer` or raw `FetchRequest`)
- How to override the cleaner interval for a test (find `CleanerConfig` — if not exposed for tests, add a test-only setter or expose `pub(crate)` access from `broker.rs`)

- [ ] **Step 2: Write the failing test**

Create `crates/broker/tests/compaction.rs`:

```rust
//! Slice 18 — log compaction end-to-end via native client.

#![cfg(not(target_os = "windows"))]

use std::collections::BTreeMap;
use std::time::Duration;

#[path = "common/mod.rs"]
mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_dedupes_via_native_client() {
    // Override the cleaner interval to 1s for this test. The default 30s
    // is too slow to assert on. The override mechanism depends on how
    // Broker::start exposes CleanerConfig — adapt accordingly.
    let cluster = common::spawn_single_broker_with_cleaner_interval(
        Duration::from_secs(1),
    )
    .await;

    // Create topic `compacted` with cleanup.policy=compact + tiny
    // segment.bytes to force frequent rolls.
    let mut config_overrides = BTreeMap::new();
    config_overrides.insert("cleanup.policy".to_string(), "compact".to_string());
    config_overrides.insert("segment.bytes".to_string(), "256".to_string());
    common::create_topic_with_configs(
        &cluster,
        "compacted",
        /* partitions */ 1,
        /* rf */ 1,
        config_overrides,
    )
    .await
    .expect("create topic");

    // Produce 30 records: 10 each under k1, k2, k3, with values
    // v0..v9. Final values per key: k1=v9, k2=v9, k3=v9.
    for round in 0..10 {
        for key in ["k1", "k2", "k3"] {
            let value = format!("v{round}-{key}");
            common::produce(
                &cluster,
                "compacted",
                /* partition */ 0,
                key.as_bytes(),
                value.as_bytes(),
            )
            .await
            .expect("produce");
        }
    }

    // Sleep > 2 cleaner ticks (1s + buffer) to ensure compaction ran.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Fetch from offset 0 to end. With segment.bytes=256 the producer above
    // has rolled many segments; compaction should have collapsed all but
    // the newest record per key into a single sealed segment, plus any
    // records still in the active segment.
    let records = common::fetch_all_records(&cluster, "compacted", 0).await
        .expect("fetch");

    // The active segment may still hold the most recent few writes
    // uncompacted, so we don't assert exact count == 3. Instead:
    //   - assert no record value other than v9-* survives, OR
    //   - assert each key appears at most once-per-segment
    //
    // Strongest assertion: after compaction the *total* record count
    // for each key collapses to 1 once everything is in sealed
    // segments. To force this, do another round of writes to roll the
    // active segment, then wait + assert exactly 3 records remain.

    // Force-roll the active segment by writing one more record per key.
    for key in ["k1", "k2", "k3"] {
        let value = format!("v10-{key}");
        common::produce(&cluster, "compacted", 0, key.as_bytes(), value.as_bytes())
            .await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    let records = common::fetch_all_records(&cluster, "compacted", 0).await
        .expect("fetch");

    // After two compaction passes covering all segments, exactly 3
    // distinct keys remain (the active segment may hold up to 3 more
    // uncompacted; the sealed segments collapse to 1 record per key).
    let mut keys: Vec<_> = records.iter()
        .map(|r| String::from_utf8(r.key.clone()).unwrap())
        .collect();
    keys.sort();
    keys.dedup();
    assert_eq!(keys, vec!["k1".to_string(), "k2".to_string(), "k3".to_string()],
        "exactly 3 distinct keys must survive compaction");

    // For each key, the *latest* value is v10-* (the post-force-roll
    // writes). Older v0..v9 values must be gone from any sealed segment.
    for key in ["k1", "k2", "k3"] {
        let values_for_key: Vec<_> = records.iter()
            .filter(|r| r.key == key.as_bytes())
            .map(|r| String::from_utf8(r.value.clone()).unwrap())
            .collect();
        let expected = format!("v10-{key}");
        assert!(values_for_key.contains(&expected),
            "key {key} should have latest value {expected}; got {values_for_key:?}");
        // No old value should survive in any sealed segment. The active
        // segment may still hold an old v10-* write itself, but never
        // v0..v9 (those are 30 records back in the rolled segments).
        for old_round in 0..10 {
            let old_value = format!("v{old_round}-{key}");
            assert!(!values_for_key.contains(&old_value),
                "key {key} should NOT have stale value {old_value}; got {values_for_key:?}");
        }
    }
}
```

**Helpers needed:** `spawn_single_broker_with_cleaner_interval`, `create_topic_with_configs`, `produce`, `fetch_all_records`. The first one is novel and needs to plumb a test-only path into `Broker::start` to override the cleaner interval; the other three are likely either already present in `common/mod.rs` or trivially derivable from existing slice-16/17 tests.

If `Broker::start` doesn't accept a `CleanerConfig`, the smallest viable change is to add an optional `cleaner_interval_override: Option<Duration>` field to `BrokerConfig` (gated `#[cfg(any(test, feature = "test-helpers"))]` if you want to keep it test-only) and use it when present in the spawn block (Step 6 of T5). Adjust T5 retroactively if needed.

- [ ] **Step 3: Run the test**

```
cargo test -p crabka-broker --test compaction -- --nocapture
```

Expected: PASS.

If it fails because no compaction happened (records still under all 11 rounds), check:
- Was the topic created with `cleanup.policy=compact`? `git grep --break compacted` in broker logs.
- Did the cleaner tick? Add a `tracing::info!` at the start of `cleaner::tick_all` to confirm.
- Is the partition's `current_leader` actually this broker's `node_id`?

- [ ] **Step 4: Commit**

```
git add crates/broker/tests/compaction.rs crates/broker/tests/common/mod.rs
git commit -m "test(slice-18): broker integration test for log compaction" -m "Produces 30 records across 3 keys + 3 more after a force-roll; asserts exactly 3 keys survive and only the latest value per key remains after two compaction ticks. Cleaner interval overridden to 1s for the test." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: JVM acceptance test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Find the JVM test scaffold**

Read the most recent JVM acceptance test in `crates/broker/tests/jvm_acceptance.rs` (likely the slice-17a `jvm_kafka_configs_describe_users_scram_credentials_end_to_end`). Identify:

- The 3-broker SASL/PLAINTEXT fixture (or whichever cluster shape is canonical for this file)
- The `docker_run_kafka_tool_with_image_and_mount` helper (or whatever the current spelling is)
- How `kafka-topics --create --config <k>=<v>` is shaped on the existing tests
- How `kafka-console-producer` is invoked with `--property parse.key=true`
- How `kafka-console-consumer --from-beginning --timeout-ms` is invoked
- How stdout is captured + asserted

Copy the most recent test's structure verbatim, then adapt for compaction.

- [ ] **Step 2: Write the test**

Append to `crates/broker/tests/jvm_acceptance.rs`. The function name format and docstring style must match the existing tests in the file.

```rust
/// Slice 18 — `kafka-console-consumer` sees a compacted topic with only
/// the latest value per key.
///
/// 1. 3-broker cluster (SASL/PLAINTEXT, admin pre-provisioned).
/// 2. `kafka-topics --create --topic compacted-jvm --config cleanup.policy=compact --config segment.bytes=256 --partitions 1 --replication-factor 1`.
/// 3. `kafka-console-producer --property parse.key=true --property key.separator=:` piping:
///    `k1:v1\nk1:v2\nk2:v3\nk1:v4\nk3:v5\n`.
/// 4. Sleep ~5s for cleaner ticks + segment rolls.
/// 5. `kafka-console-consumer --from-beginning --timeout-ms 5000`.
/// 6. Assert stdout contains `v4`, `v3`, `v5` (latest per-key values) and not `v1` or `v2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jvm_kafka_console_consumer_sees_compacted_topic_end_to_end() {
    // [Mirror the structure of the existing slice-17a JVM test:
    //  3-broker SASL/PLAINTEXT fixture, kafka-topics + kafka-console-producer +
    //  kafka-console-consumer via docker_run_kafka_tool_with_image_and_mount,
    //  std::process::Command piping for the producer's stdin if needed.]
    //
    // The body is intentionally not stubbed in this plan because the
    // exact spelling of the docker helper, the SASL config-file mount,
    // and the stdout-capture pattern must mirror whatever the slice-17a
    // test does verbatim. Copy that test's setup; replace the test's
    // body with the steps above.
}
```

**Important:** the plan deliberately does not pre-fill the test body because the JVM helpers have non-trivial mounts/env-var setups that change between slices. The implementer should:

1. Read the most recent passing JVM test in `jvm_acceptance.rs` end-to-end.
2. Copy its setup verbatim into the new test.
3. Replace its docker-tool invocations with `kafka-topics --create`, `kafka-console-producer`, `kafka-console-consumer` calls matching the script above.
4. Use `std::process::Command` directly for `kafka-console-producer` with `--property parse.key=true --property key.separator=:` piping the input string into the child's stdin (slice-16 family has precedent — `git grep -n 'kafka-console-producer\|parse.key=true' crates/broker/tests/jvm_acceptance.rs`). If no precedent exists, use the docker helper anyway and pass the input via a `printf … | docker exec -i …` style invocation.

- [ ] **Step 3: Run locally if Docker is available**

```
cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_console_consumer_sees_compacted_topic -- --nocapture
```

Expected on a Docker-equipped Linux/macOS runner: PASS. On Windows (the dev machine): SKIPPED via the `#![cfg(not(target_os = "windows"))]` convention.

If the JVM tools error with "Cannot increase the number of partitions for a non-compact topic" or similar — that's a topic-already-existed problem; ensure each test run uses a fresh cluster fixture.

- [ ] **Step 4: Commit**

```
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(slice-18): JVM acceptance — console-consumer sees compacted topic" -m "kafka-topics --config cleanup.policy=compact + kafka-console-producer piping duplicate keys + kafka-console-consumer asserting only latest per-key values survive." -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Final review (after all 7 tasks)

- [ ] **Step 1: Full clippy + test sweep**

```
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: all green.

- [ ] **Step 2: Spot-check the README feature matrix**

Update `README.md` storage section. Find the row:

```
| Log compaction (`cleanup.policy=compact`) | ❌ |
```

Change to:

```
| Log compaction (`cleanup.policy=compact`) | ✅ |
```

Commit as a separate small commit:

```
git add README.md
git commit -m "docs: mark log compaction as implemented (slice 18)" -m "Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 3: Push branch and open PR**

Confirm with the user before pushing or opening a PR.
