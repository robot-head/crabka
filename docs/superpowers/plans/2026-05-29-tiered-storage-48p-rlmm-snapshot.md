# Tiered Storage 48p — RLMM snapshot / fast-bootstrap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop replaying the entire `__remote_log_metadata` topic from offset 0 on every broker restart by persisting the in-memory RLMM cache plus per-partition consumed offsets to a local on-disk snapshot, and resuming the metadata consumer from `committed + 1` on start.

**Architecture:** `InmemoryRemoteLogMetadataManager` gains `export`/`import` (a `RlmmCacheDump` of every partition's segment + partition-delete metadata; `import` seeds the per-tp `RemoteLogMetadataCache` directly, bypassing transition validation so terminal states survive a round trip). A new `snapshot.rs` wraps that dump in a versioned envelope (format version + per-metadata-partition committed offsets + the encoded cache) with atomic temp-file-plus-rename write and a non-panicking loader. `manager.rs` loads the snapshot on `start` (falling back to a full replay on absence/corruption), builds the 48o assignment from the recovered offsets, and runs a snapshotter task that writes on a configurable interval and once on `CancellationToken` shutdown — capturing the pump's `applied` offsets together with `inner.export()` under a consistent lock.

**Tech Stack:** Rust, tokio, crabka workspace crates (remote-storage-topic, remote-storage)

---

## Locked upstream types (48o) — reuse verbatim, do NOT redefine

This slice **depends on slice 48o**, which introduces these exact public types in `crates/remote-storage-topic/src/log.rs` (re-exported from `lib.rs`). Use them as-is; if a step below appears to redefine one, that is a mistake — import it instead.

```rust
// crates/remote-storage-topic/src/log.rs (re-exported from lib.rs)
pub struct PartitionStart { pub partition: i32, pub start_offset: i64 }
pub trait AssignmentHandle: Send + Sync {
    fn add(&self, start: PartitionStart);
    fn remove(&self, partition: i32);
    fn assigned(&self) -> Vec<i32>;
}
// MetadataEventLog::subscribe(&self, assignment: Vec<PartitionStart>) -> (MetadataEventStream, Arc<dyn AssignmentHandle>)
// manager.rs holds `assignment: Arc<dyn AssignmentHandle>`; pump tracks per-partition `applied: Arc<Mutex<Vec<i64>>>`
```

After 48o lands, `TopicBasedRemoteLogMetadataManager::start` already:
- subscribes with an explicit `Vec<PartitionStart>` assignment instead of the bare `subscribe()`;
- holds `assignment: Arc<dyn AssignmentHandle>` on the struct;
- keeps the `applied: Arc<std::sync::Mutex<Vec<i64>>>` vector the pump advances (one slot per metadata partition, `-1` = nothing applied);
- keeps the `shutdown: CancellationToken` and `wait_for_targets(&[i64])` catch-up gate.

48p only adds: snapshot load before building the assignment, and a snapshotter task that flushes on interval and on shutdown.

> **Sequencing:** 48p is sequenced **after 48o and before 48q**. Both 48p and 48q touch `manager.rs`, so they are NOT parallel with each other. The two tasks **inside this plan** that touch `manager.rs` (Task 3 and Task 4) must run **sequentially** (Task 3 then Task 4). Task 1 (`remote-storage/src/inmemory.rs` + new `dump.rs`) and Task 2 (`remote-storage-topic/src/snapshot.rs`) touch disjoint files and MAY run in parallel as the first batch. Task 5 (config + broker) touches `broker/src/config.rs` + `broker/src/broker.rs` and is independent of Tasks 1–4's files, but depends on the new config-field surface, so run it after Task 4.

---

## File structure

```
crates/remote-storage/src/
  dump.rs            (new)   — RlmmCacheDump + SegmentDump / PartitionDeleteDump value types
  inmemory.rs        (edit)  — InmemoryRemoteLogMetadataManager::export / import
  cache.rs           (edit)  — pub(crate) seeding + dumping helpers on RemoteLogMetadataCache
  lib.rs             (edit)  — re-export RlmmCacheDump (+ dump value types)
crates/remote-storage-topic/src/
  snapshot.rs        (new)   — Snapshot envelope: encode/decode, atomic write, load
  serde.rs           (edit)  — expose segment/partition-delete entry codec for snapshot.rs
  manager.rs         (edit)  — snapshotter task, shutdown flush, snapshot load + resume in start
  lib.rs             (edit)  — `pub mod snapshot;` + re-export Snapshot / SnapshotError
  error.rs           (edit)  — SnapshotError variant(s) if not folded into CodecError
crates/broker/src/
  config.rs          (edit)  — snapshot interval + directory fields on KafkaRlmmConfig
  broker.rs          (edit)  — thread broker data dir into bootstrap_topic_rlmm
```

---

### Task 1: `RlmmCacheDump` + `InmemoryRemoteLogMetadataManager::export` / `import`

**Files:**
- `crates/remote-storage/src/dump.rs` (new)
- `crates/remote-storage/src/inmemory.rs` (edit)
- `crates/remote-storage/src/cache.rs` (edit)
- `crates/remote-storage/src/lib.rs` (edit)

The cache fields (`id_to_metadata`, `epoch_to_offset_to_id`, `delete_state`) are private to `cache.rs`. `import` must rebuild the epoch index and accept terminal states, so we add `pub(crate)` dump/seed helpers on `RemoteLogMetadataCache` rather than going through `add`/`update` (which reject terminal states and re-running transitions). The dump is a flat, owned value type: every partition's full segment list (all states) + optional partition-delete state.

- [ ] **Step 1.1 — Write the dump value type (no logic yet).** Create `crates/remote-storage/src/dump.rs`:

```rust
//! [`RlmmCacheDump`] — a flat, owned snapshot of an
//! [`InmemoryRemoteLogMetadataManager`](crate::InmemoryRemoteLogMetadataManager)'s
//! cache, used by the topic-backed manager's on-disk snapshot
//! (slice 48p). Unlike the live mutation path, importing a dump
//! bypasses lifecycle-transition validation: the dumped states are
//! already the product of valid transitions, so re-applying them
//! through `add`/`update` would wrongly reject terminal states.

use crate::metadata::{RemoteLogSegmentMetadata, RemotePartitionDeleteState, TopicIdPartition};

/// Every partition's cache contents, flattened for snapshotting.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RlmmCacheDump {
    /// One entry per partition that has any cached state.
    pub partitions: Vec<PartitionDump>,
}

/// One partition's dumped cache: all of its segments (every lifecycle
/// state, terminal included) plus its partition-delete state, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionDump {
    /// The partition this dump belongs to.
    pub topic_id_partition: TopicIdPartition,
    /// Every segment currently tracked for this partition, in no
    /// particular order (import re-derives ordering / epoch index).
    pub segments: Vec<RemoteLogSegmentMetadata>,
    /// Partition-delete lifecycle state, if the partition was ever
    /// marked for deletion.
    pub delete_state: Option<RemotePartitionDeleteState>,
}
```

- [ ] **Step 1.2 — Wire the module + re-export.** In `crates/remote-storage/src/lib.rs` add `pub mod dump;` next to the other `pub mod` lines, and add `pub use dump::{PartitionDump, RlmmCacheDump};` to the re-export block.

- [ ] **Step 1.3 — Write a FAILING test for cache dump/seed helpers.** In `crates/remote-storage/src/cache.rs` `#[cfg(test)] mod tests`, add a test that a finished-then-delete-started cache survives a dump→seed round trip including the epoch index:

```rust
#[test]
fn dump_then_seed_rebuilds_epoch_index() {
    let mut c = RemoteLogMetadataCache::default();
    c.add(seg(10, &[(0, 0)], 0, 99)).unwrap();
    c.add(seg(11, &[(0, 100)], 100, 199)).unwrap();
    c.update(&finish(10)).unwrap();
    c.update(&finish(11)).unwrap();
    c.update(&transition(11, RemoteLogSegmentState::DeleteSegmentStarted))
        .unwrap();
    c.set_delete_state(RemotePartitionDeleteState::DeletePartitionMarked);

    let segments = c.dump_segments();
    let delete_state = c.delete_state();

    let mut seeded = RemoteLogMetadataCache::default();
    seeded.seed(segments, delete_state);

    // Finished seg 10 is queryable; delete-started seg 11 is hidden
    // but still listed; delete_state survives.
    assert_eq!(
        seeded.segment_for(0, 50).unwrap().remote_log_segment_id().id,
        Uuid::from_u128(10)
    );
    assert!(seeded.segment_for(0, 150).is_none());
    assert_eq!(seeded.list().len(), 2);
    assert_eq!(
        seeded.delete_state(),
        Some(RemotePartitionDeleteState::DeletePartitionMarked)
    );
}
```

- [ ] **Step 1.4 — Run it; expect FAIL (no `dump_segments` / `seed`).** `cargo test -p crabka-remote-storage cache::tests::dump_then_seed_rebuilds_epoch_index` — expect a compile error: no method `dump_segments` / `seed` on `RemoteLogMetadataCache`.

- [ ] **Step 1.5 — Implement the cache helpers.** In `crates/remote-storage/src/cache.rs`, add to `impl RemoteLogMetadataCache`:

```rust
/// Every tracked segment (all states), unordered. The owning
/// manager pairs this with [`Self::delete_state`] to dump the
/// partition for snapshotting.
pub(crate) fn dump_segments(&self) -> Vec<RemoteLogSegmentMetadata> {
    self.id_to_metadata.values().cloned().collect()
}

/// Seed this (assumed-empty) cache from a dump, bypassing
/// lifecycle-transition validation. Rebuilds the per-epoch offset
/// index for every finished segment exactly as the live path does,
/// so reads after seeding behave identically. `delete_started` /
/// `delete_finished` segments are kept in `id_to_metadata` but not
/// indexed (delete_finished never reaches a dump, but we tolerate
/// it for robustness). Partition-delete state is set verbatim.
pub(crate) fn seed(
    &mut self,
    segments: Vec<RemoteLogSegmentMetadata>,
    delete_state: Option<RemotePartitionDeleteState>,
) {
    for md in segments {
        let id = md.remote_log_segment_id().id;
        if md.state() == RemoteLogSegmentState::CopySegmentFinished {
            self.index_epochs(&md);
        }
        // DeleteSegmentFinished segments are dropped entirely in the
        // live path; if one somehow appears in a dump, skip it.
        if md.state() != RemoteLogSegmentState::DeleteSegmentFinished {
            self.id_to_metadata.insert(id, md);
        }
    }
    self.delete_state = delete_state;
}
```

(`delete_state()` and `set_delete_state()` already exist and are `pub(crate)`; reuse them in the test.)

- [ ] **Step 1.6 — Run it; expect PASS.** `cargo test -p crabka-remote-storage cache::tests::dump_then_seed_rebuilds_epoch_index` — expect 1 passed.

- [ ] **Step 1.7 — Write a FAILING test for manager-level `export`/`import`.** In `crates/remote-storage/src/inmemory.rs` `#[cfg(test)] mod tests`, add (uses the file's existing `tp`, `started`, `finish` helpers):

```rust
#[test]
fn export_then_import_reproduces_cache() {
    let m = InmemoryRemoteLogMetadataManager::new();
    m.add_remote_log_segment_metadata(started(10, 0, 99)).unwrap();
    m.add_remote_log_segment_metadata(started(11, 100, 199)).unwrap();
    m.update_remote_log_segment_metadata(finish(10)).unwrap();
    m.put_remote_partition_delete_metadata(RemotePartitionDeleteMetadata {
        topic_id_partition: tp(),
        state: RemotePartitionDeleteState::DeletePartitionMarked,
        event_timestamp_ms: 500,
        broker_id: 1,
    })
    .unwrap();

    let dump = m.export();
    let restored = InmemoryRemoteLogMetadataManager::new();
    restored.import(dump);

    // list_remote_log_segments matches across the partition.
    let before = m.list_remote_log_segments(&tp()).unwrap();
    let after = restored.list_remote_log_segments(&tp()).unwrap();
    assert_eq!(before, after);
    // Finished segment still queryable post-import.
    assert_eq!(
        restored.highest_offset_for_epoch(&tp(), 0).unwrap(),
        Some(99)
    );
    // Re-exporting yields the same dump (idempotent round trip).
    assert_eq!(m.export(), restored.export());
}
```

- [ ] **Step 1.8 — Run it; expect FAIL (no `export`/`import`).** `cargo test -p crabka-remote-storage inmemory::tests::export_then_import_reproduces_cache` — expect a compile error: no method `export` / `import`.

- [ ] **Step 1.9 — Implement `export`/`import`.** In `crates/remote-storage/src/inmemory.rs`, add `use crate::dump::{PartitionDump, RlmmCacheDump};` to the imports, and add this `impl` block (a plain inherent impl, next to `new`):

```rust
impl InmemoryRemoteLogMetadataManager {
    /// Dump every partition's segment + partition-delete metadata for
    /// snapshotting (slice 48p). The result is order-independent;
    /// [`Self::import`] re-derives ordering and the epoch index.
    #[must_use]
    pub fn export(&self) -> RlmmCacheDump {
        let guard = self.partitions.lock().expect("metadata mutex poisoned");
        let mut partitions: Vec<PartitionDump> = guard
            .iter()
            .map(|(tp, cache)| PartitionDump {
                topic_id_partition: tp.clone(),
                segments: cache.dump_segments(),
                delete_state: cache.delete_state(),
            })
            .collect();
        // Stable order so `export()` is deterministic and comparable.
        partitions.sort_by(|a, b| {
            (a.topic_id_partition.topic_id, a.topic_id_partition.partition)
                .cmp(&(b.topic_id_partition.topic_id, b.topic_id_partition.partition))
        });
        // Within a partition, sort segments by (start_offset, id) so the
        // dump is canonical regardless of HashMap iteration order.
        for p in &mut partitions {
            p.segments.sort_by(|a, b| {
                a.start_offset().cmp(&b.start_offset()).then_with(|| {
                    a.remote_log_segment_id().id.cmp(&b.remote_log_segment_id().id)
                })
            });
        }
        RlmmCacheDump { partitions }
    }

    /// Seed the cache from a dump, bypassing transition validation
    /// (slice 48p). Intended for a freshly-constructed manager during
    /// snapshot restore; existing partitions are overwritten.
    pub fn import(&self, dump: RlmmCacheDump) {
        let mut guard = self.partitions.lock().expect("metadata mutex poisoned");
        for p in dump.partitions {
            let cache = guard.entry(p.topic_id_partition).or_default();
            cache.seed(p.segments, p.delete_state);
        }
    }
}
```

- [ ] **Step 1.10 — Run it; expect PASS.** `cargo test -p crabka-remote-storage inmemory::tests::export_then_import_reproduces_cache` — expect 1 passed.

- [ ] **Step 1.11 — fmt + clippy + commit.** Run `cargo fmt --all`, then `cargo clippy -p crabka-remote-storage --all-targets -- -D warnings`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage): RlmmCacheDump export/import for snapshotting (48p)"`

---

### Task 2: snapshot envelope — `snapshot.rs` (encode / decode / atomic write / load)

**Files:**
- `crates/remote-storage-topic/src/snapshot.rs` (new)
- `crates/remote-storage-topic/src/serde.rs` (edit — expose entry codec)
- `crates/remote-storage-topic/src/error.rs` (edit — `SnapshotError`)
- `crates/remote-storage-topic/src/lib.rs` (edit — module + re-exports)

The snapshot envelope reuses `serde.rs`'s existing per-event encoders by encoding each dumped segment as a `MetadataEvent::AddSegment` (which carries the full `RemoteLogSegmentMetadata` including state) and each partition-delete as `MetadataEvent::PartitionDelete`. The envelope wraps: format version (u16), the committed-offsets vector, and the length-prefixed encoded entries.

Layout (big-endian, reusing the `serde.rs` uvarint helper exposed in Step 2.1):

```text
snapshot := u16 SNAPSHOT_FORMAT_VERSION
          | uvarint n_offsets | n_offsets × (i32 partition, i64 offset)
          | uvarint n_entries | n_entries × (uvarint len, len bytes = MetadataEvent::encode())
```

- [ ] **Step 2.1 — Expose the entry codec + varint from `serde.rs`.** In `crates/remote-storage-topic/src/serde.rs`, change `fn write_uvarint`, `fn read_uvarint`, and the `Reader` struct + its `new`/`read_u8`/`read_i32`/`read_i64`/`read_n` methods from private to `pub(crate)`. (Leave the `#[allow(clippy::cast_possible_truncation)]` on `write_uvarint`.) These let `snapshot.rs` frame entries with the same varint and read them back without a second varint implementation. Do not change the `MetadataEvent::encode` / `decode` signatures — they are already `pub`.

- [ ] **Step 2.2 — Add `SnapshotError`.** In `crates/remote-storage-topic/src/error.rs`, add:

```rust
/// Errors from the on-disk RLMM snapshot (slice 48p).
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The snapshot file could not be read or written.
    #[error("snapshot io error: {0}")]
    Io(#[from] std::io::Error),

    /// A snapshot format version the loader does not understand.
    #[error("unsupported snapshot format version {0}")]
    UnsupportedVersion(u16),

    /// The snapshot bytes were malformed (truncated, bad framing, or a
    /// contained event failed to decode).
    #[error("malformed snapshot: {0}")]
    Malformed(#[from] CodecError),

    /// Trailing bytes remained after the declared entries were read.
    #[error("snapshot has {0} trailing bytes")]
    TrailingBytes(usize),
}
```

- [ ] **Step 2.3 — Write the module skeleton + a FAILING round-trip test.** Create `crates/remote-storage-topic/src/snapshot.rs` with the type and a test, but leave `encode`/`decode` `todo!()`:

```rust
//! On-disk RLMM snapshot (slice 48p): a versioned envelope wrapping a
//! [`RlmmCacheDump`] plus the per-metadata-partition committed
//! offsets, so a restarting broker resumes the metadata consumer from
//! `committed + 1` instead of replaying `__remote_log_metadata` from
//! offset 0.
//!
//! The per-segment / per-partition-delete encoding reuses the
//! [`MetadataEvent`](crate::serde::MetadataEvent) codec; the envelope
//! adds a format version, the committed-offsets vector, and
//! length-prefixed entries. Writes are atomic (temp file + rename) so
//! a crash mid-write never yields a torn snapshot; a corrupt or
//! truncated file decodes to an error (never a panic), and the caller
//! falls back to a full replay.

use std::path::Path;

use bytes::{BufMut, BytesMut};

use crabka_remote_storage::{PartitionDump, RlmmCacheDump};

use crate::error::SnapshotError;
use crate::serde::{MetadataEvent, Reader, read_uvarint, write_uvarint};

/// Format version at the head of every snapshot file. Greenfield: bump
/// freely, no backward-compat decoder arms.
pub const SNAPSHOT_FORMAT_VERSION: u16 = 0;

/// Default snapshot file name under the snapshot directory.
pub const SNAPSHOT_FILE_NAME: &str = "snapshot";

/// A decoded snapshot: the per-metadata-partition committed offsets and
/// the cache dump to seed an [`InmemoryRemoteLogMetadataManager`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Highest offset applied into the cache per metadata partition,
    /// indexed by metadata-partition. `committed[p] == -1` means the
    /// snapshot covers no events for `p`.
    pub committed_offsets: Vec<i64>,
    /// The cache contents at the moment the snapshot was taken.
    pub dump: RlmmCacheDump,
}

impl Snapshot {
    /// Encode this snapshot into freshly-allocated bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }

    /// Decode a snapshot from `bytes`.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for any malformed input — bad version,
    /// short/truncated buffer, a contained event that fails to decode,
    /// or trailing bytes after the declared entries.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    use crabka_remote_storage::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState,
        RemotePartitionDeleteState, TopicIdPartition,
    };

    fn tp() -> TopicIdPartition {
        TopicIdPartition::new(Uuid::from_u128(1), "orders", 0)
    }

    fn started(id: u128, start: i64, end: i64) -> RemoteLogSegmentMetadata {
        RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(tp(), Uuid::from_u128(id)),
            start,
            end,
            end + 1,
            1,
            100,
            2048,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(0, start)]),
        )
        .unwrap()
    }

    fn sample_snapshot() -> Snapshot {
        let dump = RlmmCacheDump {
            partitions: vec![PartitionDump {
                topic_id_partition: tp(),
                segments: vec![started(10, 0, 99), started(11, 100, 199)],
                delete_state: Some(RemotePartitionDeleteState::DeletePartitionMarked),
            }],
        };
        Snapshot {
            committed_offsets: vec![5, -1, 2],
            dump,
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let snap = sample_snapshot();
        let bytes = snap.encode();
        let back = Snapshot::decode(&bytes).expect("decodes");
        assert_eq!(back, snap);
    }

    #[test]
    fn truncated_file_is_error_not_panic() {
        let bytes = sample_snapshot().encode();
        let err = Snapshot::decode(&bytes[..bytes.len() - 3]).unwrap_err();
        assert!(matches!(
            err,
            SnapshotError::Malformed(_) | SnapshotError::TrailingBytes(_)
        ));
    }

    #[test]
    fn garbage_bytes_are_error_not_panic() {
        let err = Snapshot::decode(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap_err();
        assert!(matches!(
            err,
            SnapshotError::UnsupportedVersion(_) | SnapshotError::Malformed(_)
        ));
    }

    #[test]
    fn empty_buffer_is_error_not_panic() {
        let err = Snapshot::decode(&[]).unwrap_err();
        assert!(matches!(err, SnapshotError::Malformed(_)));
    }
}
```

- [ ] **Step 2.4 — Wire module + re-exports.** In `crates/remote-storage-topic/src/lib.rs`: add `pub mod snapshot;` after `pub mod serde;`, add `SnapshotError` to the `pub use error::{...}` line, and add `pub use snapshot::{Snapshot, SNAPSHOT_FILE_NAME, SNAPSHOT_FORMAT_VERSION};`.

- [ ] **Step 2.5 — Run it; expect FAIL (`todo!()` panics).** `cargo test -p crabka-remote-storage-topic snapshot::tests::encode_decode_round_trip` — expect the test to fail via `not yet implemented` panic.

- [ ] **Step 2.6 — Implement `encode`.** Replace the `encode` body in `snapshot.rs`:

```rust
#[must_use]
pub fn encode(&self) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(256);
    buf.put_u16(SNAPSHOT_FORMAT_VERSION);
    // Committed offsets.
    write_uvarint(self.committed_offsets.len() as u64, &mut buf);
    for (p, &off) in self.committed_offsets.iter().enumerate() {
        buf.put_i32(i32::try_from(p).expect("partition fits in i32"));
        buf.put_i64(off);
    }
    // Entries: one MetadataEvent per segment, then one per
    // partition-delete (so import sees segments before delete state).
    let mut entries: Vec<bytes::Bytes> = Vec::new();
    for p in &self.dump.partitions {
        for seg in &p.segments {
            entries.push(MetadataEvent::AddSegment(seg.clone()).encode());
        }
        if let Some(state) = p.delete_state {
            entries.push(
                MetadataEvent::PartitionDelete(
                    crabka_remote_storage::RemotePartitionDeleteMetadata {
                        topic_id_partition: p.topic_id_partition.clone(),
                        state,
                        event_timestamp_ms: 0,
                        broker_id: 0,
                    },
                )
                .encode(),
            );
        }
    }
    write_uvarint(entries.len() as u64, &mut buf);
    for entry in entries {
        write_uvarint(entry.len() as u64, &mut buf);
        buf.put_slice(&entry);
    }
    buf.to_vec()
}
```

> Note: `event_timestamp_ms` / `broker_id` on the synthesized partition-delete are not round-tripped — the cache only retains `delete_state`, so those fields are irrelevant to `import`. Re-decoding rebuilds the same `PartitionDump.delete_state`. (The round-trip test asserts `Snapshot` equality on the *dump*, whose `PartitionDump` carries only `delete_state`, not the dropped fields.)

- [ ] **Step 2.7 — Implement `decode`.** Replace the `decode` body:

```rust
pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
    let mut r = Reader::new(bytes);
    let version = read_u16(&mut r)?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }
    let n_offsets = usize::try_from(read_uvarint(&mut r)?)
        .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
    let mut committed_offsets = vec![-1i64; n_offsets];
    for _ in 0..n_offsets {
        let p = r.read_i32()?;
        let off = r.read_i64()?;
        let idx = usize::try_from(p).map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
        if idx >= committed_offsets.len() {
            return Err(SnapshotError::Malformed(CodecError::LengthOverflow(idx as u64)));
        }
        committed_offsets[idx] = off;
    }
    let n_entries = usize::try_from(read_uvarint(&mut r)?)
        .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;

    use std::collections::BTreeMap;
    // Accumulate per-partition while preserving first-seen order.
    let mut order: Vec<TopicIdPartition> = Vec::new();
    let mut by_tp: BTreeMap<(uuid::Uuid, i32), (Vec<RemoteLogSegmentMetadata>, Option<RemotePartitionDeleteState>)> =
        BTreeMap::new();
    for _ in 0..n_entries {
        let len = usize::try_from(read_uvarint(&mut r)?)
            .map_err(|_| CodecError::LengthOverflow(u64::MAX))?;
        let raw = r.read_n(len)?;
        match MetadataEvent::decode(raw)? {
            MetadataEvent::AddSegment(md) => {
                let tp = md.remote_log_segment_id().topic_id_partition.clone();
                let key = (tp.topic_id, tp.partition);
                if !by_tp.contains_key(&key) {
                    order.push(tp.clone());
                }
                by_tp.entry(key).or_default().0.push(md);
            }
            MetadataEvent::PartitionDelete(d) => {
                let key = (d.topic_id_partition.topic_id, d.topic_id_partition.partition);
                if !by_tp.contains_key(&key) {
                    order.push(d.topic_id_partition.clone());
                }
                by_tp.entry(key).or_default().1 = Some(d.state);
            }
            MetadataEvent::UpdateSegment(_) => {
                // Snapshots only ever encode Add + PartitionDelete.
                return Err(SnapshotError::Malformed(CodecError::UnknownTag(1)));
            }
        }
    }
    if r.remaining() != 0 {
        return Err(SnapshotError::TrailingBytes(r.remaining()));
    }
    let partitions = order
        .into_iter()
        .map(|tp| {
            let key = (tp.topic_id, tp.partition);
            let (segments, delete_state) = by_tp.remove(&key).expect("key present");
            PartitionDump {
                topic_id_partition: tp,
                segments,
                delete_state,
            }
        })
        .collect();
    Ok(Self {
        committed_offsets,
        dump: RlmmCacheDump { partitions },
    })
}
```

  Add these imports at the top of `snapshot.rs`: `use crabka_remote_storage::{RemoteLogSegmentMetadata, RemotePartitionDeleteState, TopicIdPartition};` and `use crate::error::CodecError;`. Also add two tiny private helpers at the bottom of the module:

```rust
fn read_u16(r: &mut Reader<'_>) -> Result<u16, CodecError> {
    let hi = u16::from(r.read_u8()?);
    let lo = u16::from(r.read_u8()?);
    Ok((hi << 8) | lo)
}
```

  And expose `remaining` on `Reader` in `serde.rs` (`pub(crate) fn remaining(&self) -> usize { self.buf.len() - self.pos }`).

> **Note on the `export()` canonical ordering:** Task 1's `export()` sorts partitions and segments, so the `decode` first-seen order will match `export`'s order for a freshly-exported dump. The round-trip test passes because the sample dump is already in that canonical order. (Do not rely on this for arbitrary hand-built dumps; the snapshot always originates from `export()`.)

- [ ] **Step 2.8 — Run it; expect PASS.** `cargo test -p crabka-remote-storage-topic snapshot::tests` — expect 4 passed (round_trip, truncated, garbage, empty).

- [ ] **Step 2.9 — Write a FAILING test for atomic write + load.** Append to `snapshot.rs` tests:

```rust
#[test]
fn write_then_load_round_trips_through_a_file() {
    let dir = std::env::temp_dir().join(format!("crabka-snap-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot");
    let snap = sample_snapshot();
    snap.write_atomic(&path).expect("write");
    let loaded = Snapshot::load(&path).expect("load").expect("present");
    assert_eq!(loaded, snap);
    // No temp file left behind.
    assert!(std::fs::read_dir(&dir).unwrap().filter_map(Result::ok)
        .all(|e| e.file_name() == "snapshot"));
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn load_absent_file_is_ok_none() {
    let path = std::env::temp_dir().join("crabka-snap-does-not-exist-xyz");
    let _ = std::fs::remove_file(&path);
    assert_eq!(Snapshot::load(&path).unwrap(), None);
}

#[test]
fn load_corrupt_file_is_err() {
    let dir = std::env::temp_dir().join(format!("crabka-snap-corrupt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("snapshot");
    std::fs::write(&path, [0xFF, 0xFF, 0x00, 0x01]).unwrap();
    assert!(Snapshot::load(&path).unwrap().is_err());
    std::fs::remove_dir_all(&dir).ok();
}
```

  (Note `load` returns `Result<Option<Result<Snapshot, SnapshotError>>>`? No — keep it simple: `load` returns `Result<Option<Snapshot>, SnapshotError>` where `Ok(None)` = file absent, `Err` = present-but-corrupt, `Ok(Some)` = good. Adjust `load_corrupt_file_is_err` to `assert!(Snapshot::load(&path).is_err());` and `load_absent_file_is_ok_none` to `assert_eq!(Snapshot::load(&path).unwrap(), None);`. Use that signature.)

- [ ] **Step 2.10 — Run it; expect FAIL (no `write_atomic` / `load`).** `cargo test -p crabka-remote-storage-topic snapshot::tests::write_then_load_round_trips_through_a_file` — expect a compile error.

- [ ] **Step 2.11 — Implement `write_atomic` + `load`.** Add to `impl Snapshot` in `snapshot.rs`:

```rust
/// Atomically write this snapshot to `path`: write to a sibling
/// temp file, fsync, then rename over `path`. A crash mid-write
/// leaves either the old snapshot or none — never a torn file.
///
/// # Errors
///
/// Returns [`SnapshotError::Io`] on any filesystem failure.
pub fn write_atomic(&self, path: &Path) -> Result<(), SnapshotError> {
    use std::io::Write;
    let bytes = self.encode();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Load a snapshot from `path`. `Ok(None)` when the file does not
/// exist (first boot); `Err` when the file exists but is corrupt;
/// `Ok(Some)` on success.
///
/// # Errors
///
/// Returns [`SnapshotError::Io`] for read failures other than
/// not-found, or a decode error for a present-but-malformed file.
pub fn load(path: &Path) -> Result<Option<Self>, SnapshotError> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SnapshotError::Io(e)),
    };
    Ok(Some(Self::decode(&bytes)?))
}
```

- [ ] **Step 2.12 — Run it; expect PASS.** `cargo test -p crabka-remote-storage-topic snapshot::tests` — expect 7 passed.

- [ ] **Step 2.13 — fmt + clippy + commit.** `cargo fmt --all`, then `cargo clippy -p crabka-remote-storage-topic --all-targets -- -D warnings`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage-topic): RLMM snapshot envelope + atomic write/load (48p)"`

---

### Task 3: snapshotter task + shutdown flush in `manager.rs`

**Files:**
- `crates/remote-storage-topic/src/manager.rs` (edit)

> **Runs AFTER Task 2 (needs `Snapshot`) and BEFORE Task 4 (both edit `manager.rs`).**

Add a snapshot directory + interval to the manager, a method that captures `(applied snapshot, inner.export())` under a consistent lock and writes a `Snapshot`, a background task that flushes on the interval when the cache advanced, and a flush on the `CancellationToken` shutdown path. This task wires the *writer*; Task 4 wires the *reader* into `start`.

- [ ] **Step 3.1 — Write a FAILING test for shutdown-flush.** In `manager.rs` `#[cfg(test)] mod tests`, add a test that after a graceful shutdown a snapshot file exists covering the applied events. The 48o `start` signature now takes a snapshot dir — assume the helper builds a temp dir. Add:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_flushes_a_snapshot_covering_applied_events() {
    use crabka_remote_storage_topic::Snapshot; // self crate path in tests: `crate::snapshot::Snapshot`
    let dir = std::env::temp_dir().join(format!("crabka-mgr-snap-{}-{:?}",
        std::process::id(), std::time::Instant::now()));
    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
    let m = TopicBasedRemoteLogMetadataManager::start(
        log.clone(),
        Handle::current(),
        dir.clone(),
        std::time::Duration::from_secs(3600), // long interval: only shutdown flushes
    )
    .await
    .unwrap();
    let m2 = m.clone();
    on_blocking(move || {
        m2.add_remote_log_segment_metadata(started(10, 0, 99)).unwrap();
    })
    .await;
    let m2 = m.clone();
    on_blocking(move || m2.update_remote_log_segment_metadata(finish(10)).unwrap()).await;

    m.shutdown_and_flush().await;

    let path = dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
    let snap = crate::snapshot::Snapshot::load(&path).unwrap().expect("snapshot written");
    // The orders partition's committed offset covers both events.
    let p = crate::partitioning::metadata_partition_for(&tp(), 4);
    let idx = usize::try_from(p).unwrap();
    assert!(snap.committed_offsets[idx] >= 1, "committed >= last applied offset");
    // The dump contains the finished segment.
    assert_eq!(snap.dump.partitions.len(), 1);
    assert_eq!(snap.dump.partitions[0].segments.len(), 1);
    std::fs::remove_dir_all(&dir).ok();
}
```

  > The test references `start(log, handle, dir, interval)` and `shutdown_and_flush()`. The 4-arg `start` is the 48p signature (Task 4 finalizes the load logic; Task 3 only needs the two new params plumbed). To keep Task 3 self-contained, **introduce the 4-arg `start` here** with the snapshot *load* still a no-op (offsets all `-1`, empty assignment-equivalent to today) and wire the *writer*; Task 4 fills in the load+resume body. Update the existing `start_manager` test helper to pass a fresh temp dir + a 1-hour interval.

- [ ] **Step 3.2 — Run it; expect FAIL.** `cargo test -p crabka-remote-storage-topic manager::tests::shutdown_flushes_a_snapshot_covering_applied_events` — expect compile errors (`start` arity, no `shutdown_and_flush`).

- [ ] **Step 3.3 — Add fields + new `start` params + the writer.** In `manager.rs`:
  - Add to the struct: `snapshot_dir: std::path::PathBuf,` and `snapshotter: std::sync::Mutex<Option<JoinHandle<()>>>,`.
  - Change `start` to `pub async fn start(log, runtime, snapshot_dir: std::path::PathBuf, snapshot_interval: std::time::Duration) -> Result<Arc<Self>, RemoteStorageError>`. (Task 4 changes the body's load logic; for now, after building `manager`, spawn the snapshotter and keep the existing assignment/wait flow.)
  - Add a private method that captures a consistent `(offsets, dump)` and writes:

```rust
/// Capture the pump's committed offsets together with a cache
/// export under a consistent lock, and write a snapshot. The
/// `applied` lock is held only long enough to clone the offsets and
/// run `export()` (which takes the inner partitions lock); no Kafka
/// round-trips happen inside, so the hold is bounded.
fn write_snapshot(&self) -> Result<i64, crate::snapshot::SnapshotError> {
    let (committed_offsets, dump) = {
        let applied = self.applied.lock().expect("applied mutex poisoned");
        let dump = self.inner.export();
        (applied.clone(), dump)
    };
    let max = committed_offsets.iter().copied().max().unwrap_or(-1);
    let snap = crate::snapshot::Snapshot {
        committed_offsets,
        dump,
    };
    let path = self.snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
    snap.write_atomic(&path)?;
    Ok(max) // highest committed offset written (for "advanced since last" check)
}
```

  - Add `shutdown_and_flush`:

```rust
/// Cancel the pump + snapshotter, then write a final snapshot
/// capturing everything applied so far. Idempotent enough for tests:
/// safe to call once on graceful shutdown.
pub async fn shutdown_and_flush(&self) {
    self.shutdown.cancel();
    // Let the pump observe cancellation and stop touching `applied`.
    if let Some(h) = self.snapshotter.lock().expect("snapshotter mutex poisoned").take() {
        let _ = h.await;
    }
    if let Err(e) = self.write_snapshot() {
        warn!(error = ?e, "topic-based RLMM: final snapshot flush failed");
    }
}
```

  - Spawn the snapshotter in `start` (after `manager` is built, before/after `wait_for_targets` — after is fine):

```rust
let snapshotter = {
    let weak = Arc::downgrade(&manager);
    let shutdown = manager.shutdown.clone();
    runtime.spawn(async move {
        let mut last_written: i64 = -1;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(snapshot_interval) => {}
            }
            let Some(m) = weak.upgrade() else { return };
            // Only write when the cache advanced since the last snapshot.
            let highest = {
                let applied = m.applied.lock().expect("applied mutex poisoned");
                applied.iter().copied().max().unwrap_or(-1)
            };
            if highest > last_written {
                match m.write_snapshot() {
                    Ok(written) => last_written = written,
                    Err(e) => warn!(error = ?e, "topic-based RLMM: periodic snapshot failed"),
                }
            }
        }
    })
};
*manager.snapshotter.lock().expect("snapshotter mutex poisoned") = Some(snapshotter);
```

  (Initialize the struct's `snapshotter` field to `std::sync::Mutex::new(None)` when building `manager`, then assign as above. `manager` must be the `Arc<Self>` already built before this block.)
  - In `Drop`, also abort the snapshotter handle alongside the pump.

- [ ] **Step 3.4 — Update the `start_manager` test helper.** Change it to:

```rust
async fn start_manager(
    log: Arc<dyn MetadataEventLog>,
) -> Arc<TopicBasedRemoteLogMetadataManager> {
    let dir = std::env::temp_dir().join(format!(
        "crabka-rlmm-test-{}-{}",
        std::process::id(),
        SNAP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    TopicBasedRemoteLogMetadataManager::start(
        log,
        Handle::current(),
        dir,
        std::time::Duration::from_secs(3600),
    )
    .await
    .unwrap()
}
```

  Add `static SNAP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);` to the test module so each test gets a distinct snapshot dir (avoids cross-test interference).

- [ ] **Step 3.5 — Run it; expect PASS + no regressions in the file.** `cargo test -p crabka-remote-storage-topic manager::tests` — expect all manager tests pass (existing ones still green with the new `start` arity via the helper, plus the new shutdown-flush test).

- [ ] **Step 3.6 — fmt + clippy + commit.** `cargo fmt --all`, then `cargo clippy -p crabka-remote-storage-topic --all-targets -- -D warnings`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage-topic): RLMM snapshotter task + shutdown flush (48p)"`

---

### Task 4: snapshot load + resume in `manager.rs::start`

**Files:**
- `crates/remote-storage-topic/src/manager.rs` (edit)

> **Runs AFTER Task 3 (same file; Task 3 added the 4-arg `start` + writer).**

Now fill in the load-and-resume body of `start`: load the snapshot, seed `inner` via `import`, take the committed offsets, build the 48o assignment as `PartitionStart { partition, start_offset: committed + 1 }`, and pre-seed the `applied` vector to the committed offsets so `wait_for_targets` only waits for the delta. On absence/corruption, fall back to empty cache + offsets all `-1` (full replay) — never fatal.

- [ ] **Step 4.1 — Write a FAILING resume test.** In `manager.rs` tests, add:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn restart_resumes_from_snapshot_without_replaying_from_zero() {
    let dir = std::env::temp_dir().join(format!(
        "crabka-rlmm-resume-{}-{}",
        std::process::id(),
        SNAP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let log: Arc<dyn MetadataEventLog> = InProcessMetadataEventLog::new(4);
    let interval = std::time::Duration::from_secs(3600);

    // First lifetime: seed three finished segments, then shutdown-flush.
    let pre_cache;
    {
        let m = TopicBasedRemoteLogMetadataManager::start(
            log.clone(), Handle::current(), dir.clone(), interval,
        )
        .await
        .unwrap();
        for (id, start, end) in [(10u128, 0, 99), (11, 100, 199), (12, 200, 299)] {
            let m2 = m.clone();
            on_blocking(move || {
                m2.add_remote_log_segment_metadata(started(id, start, end)).unwrap();
            })
            .await;
            let m2 = m.clone();
            on_blocking(move || m2.update_remote_log_segment_metadata(finish(id)).unwrap()).await;
        }
        pre_cache = m.list_remote_log_segments(&tp()).unwrap();
        m.shutdown_and_flush().await;
    }

    // Snapshot now records committed offset N for the orders partition.
    let p = crate::partitioning::metadata_partition_for(&tp(), 4);
    let idx = usize::try_from(p).unwrap();
    let snap = crate::snapshot::Snapshot::load(&dir.join(crate::snapshot::SNAPSHOT_FILE_NAME))
        .unwrap()
        .expect("snapshot present");
    let committed = snap.committed_offsets[idx];
    assert!(committed >= 5, "6 events (3 add + 3 finish) → committed >= 5");

    // Second lifetime against the SAME log + dir: must resume, not replay.
    let assignment = TopicBasedRemoteLogMetadataManager::resume_assignment(&dir, 4);
    // The orders partition's assignment starts at committed + 1.
    let orders_start = assignment
        .iter()
        .find(|s| s.partition == p)
        .map(|s| s.start_offset)
        .unwrap();
    assert_eq!(orders_start, committed + 1, "resume from N+1, not 0");

    let fresh = TopicBasedRemoteLogMetadataManager::start(
        log.clone(), Handle::current(), dir.clone(), interval,
    )
    .await
    .unwrap();
    let post_cache = fresh.list_remote_log_segments(&tp()).unwrap();
    assert_eq!(post_cache, pre_cache, "post-load cache equals pre-restart cache");
    assert_eq!(fresh.highest_offset_for_epoch(&tp(), 0).unwrap(), Some(299));
    fresh.shutdown();
    std::fs::remove_dir_all(&dir).ok();
}
```

  > This test asserts the *assignment* is `committed + 1` via a small pure helper `resume_assignment(dir, partition_count) -> Vec<PartitionStart>` (Step 4.2), and that the resumed cache equals the pre-restart cache. Because `InProcessMetadataEventLog::subscribe` (even the 48o assignment-aware version) replays full history when asked to start at 0, the strongest portable assertion is on the *assignment offsets* the loader produced; the cache-equality assertion confirms `import` seeded correctly. Both fail before the load logic exists.

- [ ] **Step 4.2 — Add the pure `resume_assignment` helper.** This isolates the snapshot→assignment mapping so it is unit-testable without spawning a pump. In `manager.rs`:

```rust
/// Build the metadata-consumer assignment from a snapshot on disk:
/// each metadata partition resumes at `committed + 1`. Absent or
/// corrupt snapshot → every partition starts at 0 (full replay).
#[must_use]
pub fn resume_assignment(
    snapshot_dir: &std::path::Path,
    partition_count: i32,
) -> Vec<crate::log::PartitionStart> {
    let n = usize::try_from(partition_count).expect("partition_count fits in usize");
    let committed = Self::load_committed(snapshot_dir, n);
    (0..n)
        .map(|i| crate::log::PartitionStart {
            partition: i32::try_from(i).expect("partition fits in i32"),
            start_offset: committed[i] + 1,
        })
        .collect()
}

/// Load the per-partition committed offsets from a snapshot, padded /
/// truncated to `n` partitions. Absent or corrupt → all `-1`.
fn load_committed(snapshot_dir: &std::path::Path, n: usize) -> Vec<i64> {
    let path = snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME);
    match crate::snapshot::Snapshot::load(&path) {
        Ok(Some(snap)) => {
            let mut out = vec![-1i64; n];
            for (i, &off) in snap.committed_offsets.iter().take(n).enumerate() {
                out[i] = off;
            }
            out
        }
        Ok(None) => vec![-1i64; n],
        Err(e) => {
            warn!(error = ?e, "topic-based RLMM: snapshot corrupt; full replay");
            vec![-1i64; n]
        }
    }
}
```

- [ ] **Step 4.3 — Fill in the `start` load+resume body.** Replace the relevant lines of `start` (the `applied` init, `inner` build, and `subscribe`) with:

```rust
let n = usize::try_from(log.partition_count()).expect("partition_count fits in usize");
let (applied_tx, _) = watch::channel(0u64);
let inner = Arc::new(InmemoryRemoteLogMetadataManager::new());
let shutdown = CancellationToken::new();

// Load the snapshot (if any) and seed the cache; on absence/corruption,
// committed[] is all -1 (full replay) and the cache stays empty.
let committed = Self::load_committed(&snapshot_dir, n);
match crate::snapshot::Snapshot::load(&snapshot_dir.join(crate::snapshot::SNAPSHOT_FILE_NAME)) {
    Ok(Some(snap)) => inner.import(snap.dump),
    Ok(None) => {}
    Err(e) => warn!(error = ?e, "topic-based RLMM: snapshot corrupt; starting from empty cache"),
}

// Pre-seed `applied` to the committed offsets so wait_for_targets only
// blocks on the delta from committed+1 to HWM.
let applied = Arc::new(std::sync::Mutex::new(committed.clone()));

// 48o assignment: resume each partition at committed + 1.
let assignment: Vec<crate::log::PartitionStart> = (0..n)
    .map(|i| crate::log::PartitionStart {
        partition: i32::try_from(i).expect("partition fits in i32"),
        start_offset: committed[i] + 1,
    })
    .collect();
let (stream, _assignment_handle) = log.subscribe(assignment);
```

  Keep the existing `runtime.spawn(pump_loop(...))`, the `Arc::new(Self { ... })` build (now also storing `snapshot_dir`, `snapshotter: Mutex::new(None)`, and the assignment handle if 48o requires the manager to hold it — store `_assignment_handle` as `assignment: Arc<dyn AssignmentHandle>` on the struct per the locked 48o shape), the snapshotter spawn from Task 3, and `manager.wait_for_targets(&target_hwms).await`.

  > **`wait_for_targets` correctness:** it already treats `applied[i] >= targets[i] - 1` as caught up. With `applied` pre-seeded to `committed`, an empty delta (`committed[i] == targets[i] - 1`) is satisfied immediately — no replay. Leave `wait_for_targets` unchanged.

  > **48o `subscribe` shape:** the locked signature is `subscribe(&self, assignment: Vec<PartitionStart>) -> (MetadataEventStream, Arc<dyn AssignmentHandle>)`. Destructure both; store the handle on the struct as the 48o code already does. Do not reintroduce the old no-arg `subscribe`.

- [ ] **Step 4.4 — Run it; expect PASS.** `cargo test -p crabka-remote-storage-topic manager::tests::restart_resumes_from_snapshot_without_replaying_from_zero` — expect 1 passed. Then `cargo test -p crabka-remote-storage-topic manager::tests` — all green (the old `restart_rehydrates_from_log` test still passes: with no snapshot dir reused across its two lifetimes, or sharing one, it either resumes or replays; ensure that test uses a *fresh* dir per lifetime if it must exercise replay — if it shares the helper's per-call dir, each `start_manager` call already gets a distinct dir via `SNAP_COUNTER`, so its second manager replays from 0 as before and still sees all 3 segments).

  > **Watch-out:** `restart_rehydrates_from_log` calls `start_manager` twice; with `SNAP_COUNTER` giving distinct dirs, the second manager finds no snapshot and replays the full log — its existing assertions (3 segments, end 299) still hold. No change needed to that test.

- [ ] **Step 4.5 — fmt + clippy + commit.** `cargo fmt --all`, then `cargo clippy -p crabka-remote-storage-topic --all-targets -- -D warnings`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(remote-storage-topic): load snapshot + resume metadata consumer on start (48p)"`

---

### Task 5: config + broker wiring

**Files:**
- `crates/broker/src/config.rs` (edit)
- `crates/broker/src/broker.rs` (edit)

> **Runs AFTER Task 4 (depends on the 4-arg `start` signature).** Greenfield — just add the fields, no defaults-for-compat.

- [ ] **Step 5.1 — Write a FAILING config test.** In `crates/broker/src/config.rs` tests, add (near the other `KafkaRlmmConfig` usage):

```rust
#[test]
fn kafka_rlmm_config_carries_snapshot_settings() {
    let c = KafkaRlmmConfig {
        bootstrap: "127.0.0.1:9092".into(),
        num_partitions: 50,
        replication: 1,
        snapshot_interval: std::time::Duration::from_secs(60),
        snapshot_dir: std::path::PathBuf::from("/data/remote-log-metadata"),
    };
    assert_eq!(c.snapshot_interval, std::time::Duration::from_secs(60));
    assert_eq!(c.snapshot_dir, std::path::PathBuf::from("/data/remote-log-metadata"));
}
```

- [ ] **Step 5.2 — Run it; expect FAIL.** `cargo test -p crabka-broker config::tests::kafka_rlmm_config_carries_snapshot_settings` — expect compile error: missing fields.

- [ ] **Step 5.3 — Add the fields to `KafkaRlmmConfig`.** In `crates/broker/src/config.rs`, extend the struct:

```rust
pub struct KafkaRlmmConfig {
    /// `host:port` the manager dials to reach its own broker (loopback
    /// in a single-broker setup, the inter-broker listener in a
    /// multi-broker setup).
    pub bootstrap: String,
    /// Partition count to create `__remote_log_metadata` with on first
    /// startup. Ignored when the topic already exists.
    pub num_partitions: i32,
    /// Replication factor to create `__remote_log_metadata` with on
    /// first startup. Ignored when the topic already exists.
    pub replication: i32,
    /// 48p: how often the topic-backed manager flushes its RLMM cache
    /// snapshot to disk. Maps to Kafka's
    /// `remote.log.metadata.snapshot.interval`. Default 60s.
    pub snapshot_interval: std::time::Duration,
    /// 48p: directory the RLMM cache snapshot is written to (one
    /// `snapshot` file). Derived from the broker `log.dir`.
    pub snapshot_dir: std::path::PathBuf,
}
```

  If any existing constructor / parser builds a `KafkaRlmmConfig` literal (search the crate: `rg "KafkaRlmmConfig \{"`), update each site to set `snapshot_interval: std::time::Duration::from_secs(60)` and a `snapshot_dir` derived from `log_dir` (see Step 5.5). Add a named const `pub const DEFAULT_RLMM_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);` near the other defaults and use it.

- [ ] **Step 5.4 — Run it; expect PASS.** `cargo test -p crabka-broker config::tests::kafka_rlmm_config_carries_snapshot_settings` — expect 1 passed.

- [ ] **Step 5.5 — Thread the snapshot dir into the bootstrap.** In `crates/broker/src/broker.rs`:
  - The `KafkaSwapKickoff` struct carries `cfg: KafkaRlmmConfig` + `broker_id`. The broker data dir is `BrokerConfig::log_dir` (a `PathBuf`). When constructing the `KafkaRlmmConfig` (or when building `KafkaSwapKickoff`), set `snapshot_dir = log_dir.join("remote-log-metadata")`. Find where `remote_log_metadata_kafka` / `KafkaSwapKickoff` is assembled (`rg "KafkaSwapKickoff|remote_log_metadata_kafka" crates/broker/src`) and compute `snapshot_dir` from the config's `log_dir` there.
  - In `bootstrap_topic_rlmm`, change the `TopicBasedRemoteLogMetadataManager::start(log, runtime)` call to the 4-arg form:

```rust
let manager = match crabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager::start(
    log,
    runtime,
    cfg.cfg.snapshot_dir.clone(),
    cfg.cfg.snapshot_interval,
)
.await
{
    Ok(m) => m,
    Err(e) => {
        tracing::warn!(
            error = %e,
            "topic-backed RLMM manager start failed; staying on in-memory placeholder"
        );
        return;
    }
};
```

  - If the broker performs a graceful shutdown of the topic-backed manager anywhere (`rg "shutdown\(\)" ` around the RLMM manager), prefer calling the new `shutdown_and_flush().await` on the shutdown path so the final snapshot is written; if the broker only relies on `Drop`, the snapshotter's interval + `Drop` abort is acceptable for greenfield, but a clean flush on intentional shutdown is preferred where a hook exists.

- [ ] **Step 5.6 — Build the broker; expect PASS.** `cargo build -p crabka-broker` — expect a clean build. If there are other `KafkaRlmmConfig { ... }` literals (e.g. TOML parsing, defaults, or test fixtures), fix each to set the two new fields. Run `cargo test -p crabka-broker` to confirm no regressions.

- [ ] **Step 5.7 — fmt + clippy + commit.** `cargo fmt --all`, then `cargo clippy -p crabka-broker --all-targets -- -D warnings`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "feat(broker): wire RLMM snapshot dir + interval into bootstrap (48p)"`

---

### Task 6: final workspace verification

**Files:** none (verification only)

- [ ] **Step 6.1 — fmt check.** `cargo fmt --all --check` — expect no diff.
- [ ] **Step 6.2 — clippy workspace.** `cargo clippy --workspace --all-targets -- -D warnings` — expect no warnings.
- [ ] **Step 6.3 — targeted tests.** `cargo test -p crabka-remote-storage-topic -p crabka-remote-storage` — expect all pass.
- [ ] **Step 6.4 — full workspace tests.** `cargo test --workspace` — expect no regressions.
- [ ] **Step 6.5 — final commit (if Step 6.1–6.4 required any fixups).** `cargo fmt --all`, then:
  `git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -am "chore(48p): fmt/clippy/test cleanup"`

---

## New public types introduced by 48p (for 48q consistency)

- `crabka_remote_storage::RlmmCacheDump` (+ `crabka_remote_storage::PartitionDump`) — flat owned cache dump; re-exported from `remote-storage/src/lib.rs`.
- `crabka_remote_storage::InmemoryRemoteLogMetadataManager::export(&self) -> RlmmCacheDump`
- `crabka_remote_storage::InmemoryRemoteLogMetadataManager::import(&self, dump: RlmmCacheDump)`
- `crabka_remote_storage_topic::Snapshot { committed_offsets: Vec<i64>, dump: RlmmCacheDump }` with `encode(&self) -> Vec<u8>`, `decode(&[u8]) -> Result<Self, SnapshotError>`, `write_atomic(&self, &Path) -> Result<(), SnapshotError>`, `load(&Path) -> Result<Option<Self>, SnapshotError>`.
- `crabka_remote_storage_topic::SNAPSHOT_FORMAT_VERSION: u16`, `crabka_remote_storage_topic::SNAPSHOT_FILE_NAME: &str`.
- `crabka_remote_storage_topic::error::SnapshotError`.
- `TopicBasedRemoteLogMetadataManager::start(log, runtime, snapshot_dir: PathBuf, snapshot_interval: Duration)` — 48o's start signature gains two trailing params in 48p.
- `TopicBasedRemoteLogMetadataManager::shutdown_and_flush(&self)` (async) and `TopicBasedRemoteLogMetadataManager::resume_assignment(&Path, i32) -> Vec<PartitionStart>`.
- `crabka_broker::config::KafkaRlmmConfig` gains `snapshot_interval: Duration` + `snapshot_dir: PathBuf`; `DEFAULT_RLMM_SNAPSHOT_INTERVAL`.
- `serde.rs` internals (`Reader`, `read_uvarint`, `write_uvarint`, `Reader::remaining`) become `pub(crate)` — crate-internal, not part of the public API.
