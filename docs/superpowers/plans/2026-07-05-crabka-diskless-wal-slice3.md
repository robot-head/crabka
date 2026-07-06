# Diskless WAL — Slice 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A per-broker background flusher that batches acked WAL tails from many diskless partitions into one immutable object-storage object (Crabka-private framing), records a `WalFlushRecord` offset→object index on a new `__diskless_wal_index` internal topic, and derives a `flushed` frontier — with the trim seam built but gated off. Async/background, *after* the ack; the produce path is untouched.

**Architecture:** The flusher (modeled on `remote_log_manager::run`/`tick_all`) reads each led diskless partition's tail via `Log::read_raw(flushed_frontier, high_watermark, budget)` (byte-exact v2 batches, `< hw` so always acked), concatenates the runs into one object with a footer manifest, PUTs it on the raw `Arc<dyn ObjectStore>` from `build_object_store`, then publishes one `WalFlushRecord` to `__diskless_wal_index` via the record-agnostic `KafkaMetadataEventLog`. A projection consumes the topic into a `WalIndexCache` (per-`(tp)` `BTreeMap` floor lookup) whose frontier *is* the `flushed` cursor. No fetch-from-object and no trimming yet.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `object_store` 0.13 (via `build_object_store`), `serde`/`serde_wincode`, `tokio`, `bytes`, `uuid`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice3-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice3-design.md).

**PREREQUISITES (unlanded):** Slices 1 (`WalStore`/`LocalFsyncWal`/fsync-gated HW/`diskless` flag) and 2 (KRaft offsets). Also depends on `crabka-object-store` (`build_object_store`, landed/executing) and `crabka-remote-storage-topic` (`KafkaMetadataEventLog`). Land Slices 1–2 first; this plan reuses their `high_watermark()`-from-WAL-durable and the `diskless` per-topic flag.

---

## Invariants

1. **Ack path untouched.** No edits to `produce.rs`, the `WriterMessage::Produce` arm, or `WalStore`. The flusher is a separate task reading only `< high_watermark` (acked, fsync-durable), so it can never observe or block un-acked data.
2. **Verbatim byte-exactness.** Runs come from `read_raw` (unmodified v2 batches) and are concatenated without transformation, so Slice-4 fetch-from-object can serve them byte-for-byte.
3. **`flushed` never exceeds what is durably committed.** The `flushed` frontier is derived from committed `WalFlushRecord`s only; a PUT whose index publish fails does NOT advance it (the object is a harmless orphan).
4. **No trimming this slice.** The trim seam is wired but gated off by default (no object-read fallback until Slice 4). No `TrimToOffset` is issued under default config.
5. **Reuse the record-agnostic transport.** `KafkaMetadataEventLog::publish(partition, Bytes)` (`crates/remote-storage-topic/src/kafka_log.rs:276`) carries opaque bytes — reuse it; do NOT fork the RLMM or touch `RemoteLogMetadata` types.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the combined-object framing codec; `WalFlushRecord`/`WalIndexEntry` + `WalIndexCache` projection; the `__diskless_wal_index` event-log wiring; the per-broker flush worker; the (gated-off) trim seam; the recoverability/monotonicity/ack-untouched tests.
- **Deferred:** fetch-from-object + enabling trim (Slice 4); crash-mid-flush atomicity + orphan GC (Slice 5); extracting the S3 PUT primitives into `crabka-object-store`; diskless+tiered coexistence on one partition.

---

## File Structure

- **`crates/broker/src/diskless/wal_object.rs`** (new) — the combined-object framing codec (builder + parser). One responsibility: object bytes ↔ (runs, manifest).
- **`crates/broker/src/diskless/wal_index.rs`** (new) — `WalFlushRecord`/`WalIndexEntry` + `WalIndexCache` (projection + floor lookup + frontier). One responsibility: the offset→object index.
- **`crates/broker/src/diskless/index_log.rs`** (new) — `__diskless_wal_index` event-log wiring + the projection pump. One responsibility: the durable index transport.
- **`crates/broker/src/diskless/flusher.rs`** (new) — the per-broker flush worker. One responsibility: tick → read → PUT → publish.
- **`crates/broker/src/diskless/mod.rs`** (new) + **`crates/broker/src/lib.rs`** — module wiring.
- **`crates/broker/Cargo.toml`** — add `crabka-object-store`, `crabka-remote-storage-topic`, `serde_wincode`, `uuid` deps if absent.

---

## Task 1: The combined-object framing codec

A self-contained builder + parser for the object body: `[MAGIC · version] · concatenated runs · [manifest] · [footer_len · MAGIC]`.

**Files:**
- Create: `crates/broker/src/diskless/wal_object.rs`
- Create: `crates/broker/src/diskless/mod.rs`; Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Wire the module**

`crates/broker/src/diskless/mod.rs`: `pub(crate) mod wal_object;`. In `crates/broker/src/lib.rs` add `mod diskless;`.

- [ ] **Step 2: Write the failing round-trip test**

Create `crates/broker/src/diskless/wal_object.rs` with its test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn build_then_parse_round_trips_all_runs_byte_exact() {
        let t = Uuid::from_u128(1);
        let mut b = WalObjectBuilder::new();
        b.append_run(t, 0, 0, 2, b"partition-0-verbatim-bytes");
        b.append_run(t, 1, 10, 11, b"p1-bytes");
        let obj = b.finish();

        let entries = parse_wal_object(&obj).unwrap();
        assert!(entries.len() == 2);
        assert!(entries[0].partition == 0 && entries[0].first_offset == 0 && entries[0].last_offset == 2);
        assert!(&run_bytes(&obj, &entries[0])[..] == b"partition-0-verbatim-bytes");
        assert!(&run_bytes(&obj, &entries[1])[..] == b"p1-bytes");
    }

    #[test]
    fn parse_rejects_bad_trailer_magic() {
        let mut obj = WalObjectBuilder::new().finish_with_run(Uuid::nil(), 0, 0, 0, b"x");
        let last = obj.len() - 1;
        // corrupt the trailer magic
        let mut v = obj.to_vec(); v[last] ^= 0xff; let obj = bytes::Bytes::from(v);
        assert!(parse_wal_object(&obj).is_err());
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `cargo test -p crabka-broker wal_object`
Expected: FAIL — codec undefined.

- [ ] **Step 4: Implement the codec**

Insert at the TOP of `crates/broker/src/diskless/wal_object.rs`:

```rust
//! Combined diskless-WAL object framing: one object concatenates many
//! partitions' verbatim v2-batch runs, delimited by a footer manifest.
//! `[MAGIC · version:u16] · runs · [manifest] · [footer_len:u32 · MAGIC]`.
//! Little-endian, Crabka-private (only the embedded runs need byte-exactness).

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

const MAGIC: [u8; 4] = *b"CKWL";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 6; // MAGIC(4) + version(2)
const ENTRY_LEN: usize = 16 + 4 + 8 + 8 + 8 + 4; // topic_id + partition + first + last + byte_start + byte_len

/// One partition's run within a combined object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64, // absolute offset of the run within the object
    pub byte_len: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum WalObjectError {
    #[error("wal object too short")]
    TooShort,
    #[error("bad wal object magic")]
    BadMagic,
    #[error("unsupported wal object version {0}")]
    BadVersion(u16),
    #[error("corrupt wal object manifest")]
    BadManifest,
}

/// Accumulates runs, then serializes header + runs + footer manifest + trailer.
#[derive(Default)]
pub struct WalObjectBuilder {
    body: BytesMut,
    entries: Vec<WalObjectEntry>,
}

impl WalObjectBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self { body: BytesMut::new(), entries: Vec::new() }
    }

    /// Bytes accumulated so far (for the size trigger).
    #[must_use]
    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Append one partition's verbatim run; records its absolute byte range.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) {
        let byte_start = (HEADER_LEN + self.body.len()) as u64;
        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len: run.len() as u32,
        });
    }

    /// Serialize the final object bytes.
    #[must_use]
    pub fn finish(self) -> Bytes {
        let mut out = BytesMut::with_capacity(HEADER_LEN + self.body.len() + self.entries.len() * ENTRY_LEN + 8);
        out.extend_from_slice(&MAGIC);
        out.put_u16_le(VERSION);
        out.extend_from_slice(&self.body);
        let footer_start = out.len();
        for e in &self.entries {
            out.extend_from_slice(e.topic_id.as_bytes());
            out.put_i32_le(e.partition);
            out.put_i64_le(e.first_offset);
            out.put_i64_le(e.last_offset);
            out.put_u64_le(e.byte_start);
            out.put_u32_le(e.byte_len);
        }
        let footer_len = (out.len() - footer_start) as u32;
        out.put_u32_le(footer_len);
        out.extend_from_slice(&MAGIC);
        out.freeze()
    }

    /// Test convenience: append one run and finish.
    #[cfg(test)]
    #[must_use]
    pub fn finish_with_run(mut self, t: Uuid, p: i32, f: i64, l: i64, run: &[u8]) -> Bytes {
        self.append_run(t, p, f, l, run);
        self.finish()
    }
}

/// Parse the footer manifest of a combined object.
pub fn parse_wal_object(obj: &Bytes) -> Result<Vec<WalObjectEntry>, WalObjectError> {
    if obj.len() < HEADER_LEN + 8 {
        return Err(WalObjectError::TooShort);
    }
    if obj[0..4] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let version = u16::from_le_bytes([obj[4], obj[5]]);
    if version != VERSION {
        return Err(WalObjectError::BadVersion(version));
    }
    let n = obj.len();
    if obj[n - 4..n] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let footer_len = u32::from_le_bytes([obj[n - 8], obj[n - 7], obj[n - 6], obj[n - 5]]) as usize;
    if footer_len % ENTRY_LEN != 0 || n < 8 + footer_len {
        return Err(WalObjectError::BadManifest);
    }
    let footer_start = n - 8 - footer_len;
    let mut entries = Vec::with_capacity(footer_len / ENTRY_LEN);
    let mut p = footer_start;
    while p < footer_start + footer_len {
        let topic_id = Uuid::from_slice(&obj[p..p + 16]).map_err(|_| WalObjectError::BadManifest)?;
        let partition = i32::from_le_bytes(obj[p + 16..p + 20].try_into().unwrap());
        let first_offset = i64::from_le_bytes(obj[p + 20..p + 28].try_into().unwrap());
        let last_offset = i64::from_le_bytes(obj[p + 28..p + 36].try_into().unwrap());
        let byte_start = u64::from_le_bytes(obj[p + 36..p + 44].try_into().unwrap());
        let byte_len = u32::from_le_bytes(obj[p + 44..p + 48].try_into().unwrap());
        entries.push(WalObjectEntry { topic_id, partition, first_offset, last_offset, byte_start, byte_len });
        p += ENTRY_LEN;
    }
    Ok(entries)
}

/// Slice out a run's bytes (zero-copy over the object `Bytes`).
#[must_use]
pub fn run_bytes(obj: &Bytes, e: &WalObjectEntry) -> Bytes {
    obj.slice(e.byte_start as usize..(e.byte_start as usize + e.byte_len as usize))
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `cargo test -p crabka-broker wal_object`
Expected: PASS — round-trip byte-exact; corrupt trailer rejected.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/diskless/ crates/broker/src/lib.rs
git commit -m "feat(broker): diskless WAL combined-object framing codec"
```

---

## Task 2: `WalFlushRecord` + `WalIndexCache` projection

The durable index record + the in-memory projection with the `segment_for`-style floor lookup and the derived `flushed` frontier.

**Files:**
- Create: `crates/broker/src/diskless/wal_index.rs`; Modify: `crates/broker/src/diskless/mod.rs`

- [ ] **Step 1: Write the failing tests**

Create `crates/broker/src/diskless/wal_index.rs` with its test module first:

```rust
#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    fn entry(p: i32, f: i64, l: i64) -> WalIndexEntry {
        WalIndexEntry { topic_id: Uuid::from_u128(1), partition: p, first_offset: f, last_offset: l, byte_start: 0, byte_len: 1 }
    }

    #[test]
    fn floor_lookup_returns_covering_object() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord { object_key: "o1".into(), format_version: 1, entries: vec![entry(0, 0, 4)] });
        c.apply(&WalFlushRecord { object_key: "o2".into(), format_version: 1, entries: vec![entry(0, 5, 9)] });
        let t = Uuid::from_u128(1);
        assert!(c.lookup(t, 0, 3).unwrap().0 == "o1");
        assert!(c.lookup(t, 0, 7).unwrap().0 == "o2");
        assert!(c.lookup(t, 0, 20).is_none()); // beyond last flushed
        assert!(c.flushed_frontier(t, 0) == Some(10)); // last_offset 9 + 1
    }

    #[test]
    fn apply_is_idempotent() {
        let mut c = WalIndexCache::default();
        let rec = WalFlushRecord { object_key: "o1".into(), format_version: 1, entries: vec![entry(0, 0, 4)] };
        c.apply(&rec);
        c.apply(&rec); // benign replay
        let t = Uuid::from_u128(1);
        assert!(c.flushed_frontier(t, 0) == Some(5));
    }

    #[test]
    fn wincode_round_trips() {
        let rec = WalFlushRecord { object_key: "o".into(), format_version: 1, entries: vec![entry(3, 1, 2)] };
        let bytes = rec.to_bytes().unwrap();
        assert!(WalFlushRecord::from_bytes(&bytes).unwrap() == rec);
    }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p crabka-broker wal_index`
Expected: FAIL — types undefined.

- [ ] **Step 3: Implement the record + cache**

Insert at the TOP of `crates/broker/src/diskless/wal_index.rs`:

```rust
//! The diskless offset→object index: the durable `WalFlushRecord` (one per flush
//! object, carried on `__diskless_wal_index`) and the in-memory `WalIndexCache`
//! projection with a per-(topic,partition) `BTreeMap` floor lookup — the analog
//! of `RemoteLogMetadataCache::segment_for` (crates/remote-storage/src/cache.rs:118).

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// One partition's coverage within a flushed object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
}

/// One flush's index event (all partitions in one object).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    /// # Errors
    /// Serialization failure.
    pub fn to_bytes(&self) -> Result<bytes::Bytes, String> {
        <serde_wincode::SerdeCompat<Self>>::serialize(self)
            .map(bytes::Bytes::from)
            .map_err(|e| e.to_string())
    }

    /// # Errors
    /// Deserialization failure.
    pub fn from_bytes(b: &[u8]) -> Result<Self, String> {
        <serde_wincode::SerdeCompat<Self>>::deserialize(b).map_err(|e| e.to_string())
    }
}

/// In-memory projection of `__diskless_wal_index`.
#[derive(Default)]
pub struct WalIndexCache {
    // (topic_id, partition) -> first_offset -> (object_key, entry)
    by_tp: HashMap<(Uuid, i32), BTreeMap<i64, (String, WalIndexEntry)>>,
}

impl WalIndexCache {
    /// Apply a flush record (idempotent: re-inserting the same first_offset overwrites identically).
    pub fn apply(&mut self, rec: &WalFlushRecord) {
        for e in &rec.entries {
            self.by_tp
                .entry((e.topic_id, e.partition))
                .or_default()
                .insert(e.first_offset, (rec.object_key.clone(), e.clone()));
        }
    }

    /// Which object + byte-range covers `(topic_id, partition, offset)`. Floor lookup.
    #[must_use]
    pub fn lookup(&self, topic_id: Uuid, partition: i32, offset: i64) -> Option<(String, u64, u32)> {
        let m = self.by_tp.get(&(topic_id, partition))?;
        let (_first, (key, e)) = m.range(..=offset).next_back()?;
        (offset <= e.last_offset).then(|| (key.clone(), e.byte_start, e.byte_len))
    }

    /// The highest flushed offset + 1 for a partition (the `flushed` frontier);
    /// `None` if nothing flushed. Used as the flusher's lower read bound.
    #[must_use]
    pub fn flushed_frontier(&self, topic_id: Uuid, partition: i32) -> Option<i64> {
        let m = self.by_tp.get(&(topic_id, partition))?;
        m.values().next_back().map(|(_, e)| e.last_offset + 1)
    }

    /// Drop all entries whose object was trimmed away (Slice-4 trim + compaction).
    pub fn tombstone_object(&mut self, object_key: &str) {
        for m in self.by_tp.values_mut() {
            m.retain(|_, (k, _)| k != object_key);
        }
    }
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p crabka-broker wal_index`
Expected: PASS. (Add `serde_wincode`, `uuid`, `serde` to `crates/broker/Cargo.toml` if the build reports them missing — all are workspace deps.)

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/diskless/wal_index.rs crates/broker/src/diskless/mod.rs crates/broker/Cargo.toml
git commit -m "feat(broker): WalFlushRecord + WalIndexCache floor-lookup projection"
```

---

## Task 3: `__diskless_wal_index` event-log wiring + projection pump

Reuse the record-agnostic `KafkaMetadataEventLog` to publish `WalFlushRecord` bytes and consume them into a shared `WalIndexCache`, behind a fail-closed boot facade.

**Files:**
- Create: `crates/broker/src/diskless/index_log.rs`; Modify: `crates/broker/src/diskless/mod.rs`, `crates/broker/Cargo.toml`

- [ ] **Step 1: Study the transport API**

Read `crates/remote-storage-topic/src/kafka_log.rs`: `KafkaMetadataEventLog::start(cfg)` (`:132`), `publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError>` (`:276`), `high_water_marks` (`:332`), and the subscription/assignment API (`MetadataEventLog` trait at `:265`, `subscribe`/`PartitionStart`/`AssignmentHandle` around `:301`). Read `crates/remote-storage-topic/src/manager.rs` `pump_loop` (`:674-722`) as the projection-consumer template, and `snapshot`/committed-offset resume (`:317-334`). These are the exact seams to mirror; the transport carries opaque `Bytes` so no RLMM record type is involved.

- [ ] **Step 2: Write the failing integration test**

```rust
#[cfg(test)]
mod tests {
    // Against an in-process broker + a __diskless_wal_index topic (reuse the
    // remote-storage-topic test harness that spins up KafkaMetadataEventLog):
    // 1. start a DisklessIndexLog (publisher + projection),
    // 2. publish two WalFlushRecords,
    // 3. await read-your-writes (publish returns the offset; project up to it),
    // 4. assert the shared WalIndexCache floor-lookup resolves both objects and
    //    reports the right flushed_frontier.
    #[tokio::test]
    async fn published_records_project_into_the_cache() { /* ... */ }
}
```

- [ ] **Step 3: Implement `DisklessIndexLog`**

Create `crates/broker/src/diskless/index_log.rs` with a struct that:
- holds an `Arc<KafkaMetadataEventLog>` (from `KafkaMetadataEventLog::start` against `__diskless_wal_index`, provisioned like `ensure_topic` with `cleanup.policy=compact`),
- exposes `async fn publish_flush(&self, partition: i32, rec: &WalFlushRecord) -> Result<i64, ...>` = `event_log.publish(partition, rec.to_bytes()?.into())`,
- runs a projection pump (mirror `manager.rs` `pump_loop`) that consumes each partition's events, `WalFlushRecord::from_bytes`, and `cache.lock().apply(&rec)` into a shared `Arc<Mutex<WalIndexCache>>`,
- boots fail-closed (a `NotReady`/`Swappable` facade analog: reads return "not ready" until the projection has caught up to the start-time high-water marks — mirror `NotReadyRlmm`/`SwappableRlmm`).

```rust
//! `__diskless_wal_index` durable index: publish `WalFlushRecord`s and project
//! them into a shared `WalIndexCache`. Reuses the record-agnostic
//! `KafkaMetadataEventLog` transport (opaque `Bytes`); the RLMM record types are
//! NOT involved.

pub const DISKLESS_WAL_INDEX_TOPIC: &str = "__diskless_wal_index";
// struct DisklessIndexLog { event_log, cache: Arc<Mutex<WalIndexCache>> } + publish_flush + pump + ready-gate
```

(Concrete field types: `event_log: Arc<crabka_remote_storage_topic::KafkaMetadataEventLog>`; add `crabka-remote-storage-topic` to `crates/broker/Cargo.toml` if absent — the broker already depends on it for the RSM path, confirm with `grep remote-storage-topic crates/broker/Cargo.toml`.)

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker index_log`
Expected: PASS — publish → project → floor-lookup resolves.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/diskless/index_log.rs crates/broker/src/diskless/mod.rs crates/broker/Cargo.toml
git commit -m "feat(broker): __diskless_wal_index event-log + projection pump"
```

---

## Task 4: The per-broker flush worker

Tie it together: tick → led diskless partitions → `read_raw` tail → build object → PUT → publish `WalFlushRecord`.

**Files:**
- Create: `crates/broker/src/diskless/flusher.rs`; Modify: `crates/broker/src/diskless/mod.rs`

- [ ] **Step 1: Constants + a single-partition flush unit test**

Define `FLUSH_INTERVAL = Duration::from_millis(250)` and `FLUSH_MAX_BYTES: usize = 8 * 1024 * 1024`. Write a test (against an `InMemory` object store from `build_object_store(&ObjectStoreConfig::InMemory)` + an in-process `DisklessIndexLog` + a real `Log` seeded with verbatim batches and an HW) that runs one flush tick and asserts: an object was PUT, a `WalFlushRecord` was published, and `flushed_frontier` advanced to the HW.

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-broker flusher`
Expected: FAIL — flusher undefined.

- [ ] **Step 3: Implement the worker**

Mirror `remote_log_manager::run`/`tick_all` (`crates/broker/src/remote_log_manager.rs:55-121`): a `tokio::time::interval(FLUSH_INTERVAL)` + `CancellationToken` loop; each tick snapshots `partitions.arcs()`, filters `partition.current_leader.load(Ordering::Relaxed) == node_id` AND the topic's `diskless` flag (Slice-1), and for each led diskless partition:

```rust
// under the log lock: read the flushable tail, then drop the lock
let (first, run) = {
    let log = partition.log.lock().expect("poisoned");
    let hw = partition.high_watermark();                    // Slice-1 WAL-durable HW
    let from = index.flushed_frontier(topic_id, pidx).unwrap_or(log.log_start_offset().0);
    let raw = log.read_raw(Offset(from), hw, budget)?;      // verbatim, < hw
    (raw.start_offset, raw.bytes)
};
if run.is_empty() { continue; }
builder.append_run(topic_id, pidx, first.0, last_offset_of(&run), &run);
// when builder.body_len() >= FLUSH_MAX_BYTES OR the interval fired with pending:
let key = format!("diskless-wal/{broker_id}/{flush_uuid}");
store.put(&object_store::path::Path::from(key.clone()), object_store::PutPayload::from(builder.finish().to_vec())).await?;
index.publish_flush(part_of(&key), &WalFlushRecord { object_key: key, format_version: 1, entries }).await?;
// flushed_frontier now advances via the projection (read-your-writes on publish)
```

Notes:
- `last_offset_of(&run)` = the last batch's `base_offset + last_offset_delta` in the verbatim bytes; compute it while reading (the `RawRead` gives `start_offset`; the last offset is `read_raw`'s `current-1` — expose it, or derive from the final batch header). Simplest: have the flusher request `read_raw` and also capture the partition's `hw - 1` as the run's `last_offset` only when the run reaches `hw` (it does, since the upper bound is `hw`); otherwise parse the last batch header. Prefer capturing from `read_raw` — add a `last_offset` to `RawRead` if not present (it tracks `current` internally at `log.rs:853`).
- `flush_uuid`: generate per flush (the codebase forbids `Math.random`; use `uuid::Uuid::new_v4()` — `uuid` is a normal dep, not the workflow sandbox).
- Build on the raw `Arc<dyn ObjectStore>` from `build_object_store(&cfg)`; `store.put`/`put_multipart` are `object_store` 0.13 trait methods.
- **Do not** advance any cursor on PUT/publish failure (Invariant 3) — log and retry next tick; the un-indexed object is a harmless orphan.

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker flusher`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/diskless/flusher.rs crates/broker/src/diskless/mod.rs
git commit -m "feat(broker): per-broker diskless flush worker (read->object->index)"
```

---

## Task 5: The trim seam (built, gated off)

Wire the flush frontier to trimming, but default it off.

**Files:**
- Modify: `crates/broker/src/diskless/flusher.rs`

- [ ] **Step 1: Write the failing test**

```rust
    #[tokio::test]
    async fn default_config_issues_no_trim() {
        // run several flush ticks with the default FlushConfig (trim disabled);
        // assert Partition::trim_to_offset / WriterMessage::TrimToOffset is NEVER
        // sent (spy on the writer channel or assert log_start_offset is unchanged).
    }
```

- [ ] **Step 2: Run to verify it fails / passes vacuously**

Run: `cargo test -p crabka-broker default_config_issues_no_trim`
Expected: FAIL only if a trim path exists prematurely; otherwise implement the (disabled) gate to make the intent explicit.

- [ ] **Step 3: Implement the gated trim**

Add a `FlushConfig { interval, max_bytes, trim_safety_lag: Option<i64> }` with `trim_safety_lag: None` (disabled) as default. After a successful flush+index for a partition, compute — only when `trim_safety_lag` is `Some(lag)` — `trim_target = min(flushed_frontier, hw - lag)` and, if `trim_target > log_start_offset`, send `WriterMessage::TrimToOffset` via `Partition::trim_to_offset` (`partition.rs:329`). With the default `None`, the branch is never taken. Document that Slice 4 sets `trim_safety_lag` once object-read exists, and that trim must be issued only AFTER the index publish (ordering; crash atomicity is Slice 5).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-broker default_config_issues_no_trim`
Expected: PASS — no trim under default config.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/diskless/flusher.rs
git commit -m "feat(broker): gated (default-off) diskless local-WAL trim seam"
```

---

## Task 6: Cross-cutting proof tests

**Files:**
- Modify: `crates/broker/src/diskless/flusher.rs` (or a `diskless/tests.rs`)

- [ ] **Step 1: Recoverability round-trip (behavior, not source)**

Seed several partitions with known verbatim batches, flush, then `store.get(object_key)` the object back, `parse_wal_object` it, and for each partition assert `run_bytes(&obj, entry)` is byte-exact equal to what `read_raw` produced — AND that the footer manifest and the published `WalFlushRecord` agree on every `(topic_id, partition, offset-range, byte-range)`.

- [ ] **Step 2: Watermark never trims un-flushed + union coverage**

Assert `flushed_frontier` only advances after a committed PUT + published record; inject a PUT error and assert `flushed_frontier` does NOT advance and no trim is issued; assert every offset in `[log_start_offset, hw)` is recoverable from the local log OR a committed object+index (no gap).

- [ ] **Step 3: Index monotonicity + trigger + combine**

Per-partition entries non-overlapping and contiguous-forward across flushes; the floor lookup returns the unique covering entry; flush fires on `≥ 8 MiB` and on `≥ 250 ms` with pending, empty tick is a no-op; one object with N partitions round-trips all N runs with exact boundaries.

- [ ] **Step 4: Ack path untouched**

Assert produce/ack latency and semantics are unchanged with a flush in flight (drive a produce while the flusher runs; the ack still resolves off the Slice-1 HW gate), and that the flusher only ever reads `< hw`.

- [ ] **Step 5: Run + commit**

Run: `cargo test -p crabka-broker diskless`
Expected: PASS across all diskless tests.

```bash
git add crates/broker/src/diskless/
git commit -m "test(broker): diskless flush recoverability, coverage, and ack-untouched proofs"
```

---

## Task 7: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `--check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-broker` (or `cargo test`) — PASS.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** per-broker flusher (Task 4); combined object + framing (Task 1); offset→object index on `__diskless_wal_index` (Tasks 2-3); `flushed` frontier derived from the index (Task 2 `flushed_frontier`); trim seam built + gated off (Task 5); recoverability / watermark / monotonicity / trigger / combine / ack-untouched proofs (Task 6). Deferred set (fetch S4, crash S5, PUT-extraction) untouched — Scope boundary. ✅

**2. Placeholder scan:** Tasks 1-2 are complete code. Tasks 3-4 (the integration against `KafkaMetadataEventLog` + the flush worker) give the concrete structure + the exact seams to mirror (`kafka_log.rs:276/332`, `manager.rs:674-722`, `remote_log_manager.rs:55-121`) — named code to copy, not blanks. Task 4's `last_offset_of`/`RawRead.last_offset` note flags a real, located gap (extend `RawRead` from `log.rs:853`) rather than hand-waving. No `TBD`/`TODO`.

**3. Type consistency:** `WalObjectEntry` (Task 1) and `WalIndexEntry` (Task 2) share the same six fields; the flusher (Task 4) builds `WalObjectBuilder` entries and maps them 1:1 into a `WalFlushRecord`. `WalIndexCache::{apply, lookup, flushed_frontier, tombstone_object}` (Task 2) are used by the pump (Task 3), the flusher (Task 4), and the trim gate (Task 5) with matching signatures. `DISKLESS_WAL_INDEX_TOPIC` + `publish_flush` (Task 3) match the flusher's call (Task 4).

**4. Invariant check:** ack path untouched — no edits to produce/WalStore, flusher reads only `< hw` (Task 4/6); verbatim byte-exact via `read_raw` + no transform (Task 1/6); `flushed` advances only post-commit, PUT-fail → no advance (Task 4 note + Task 6); trim gated off by default (Task 5); transport reused, no RLMM fork (Task 3). Each task ends green.

**5. Prerequisites flagged:** Slices 1-2 unlanded + `crabka-object-store`/`crabka-remote-storage-topic` deps — stated in the header.
