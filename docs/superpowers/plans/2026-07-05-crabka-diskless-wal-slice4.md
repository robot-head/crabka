# Diskless WAL — Slice 4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the diskless read loop and enable trimming: a `try_diskless_read` cold path that serves trimmed offsets from object storage, a `ListOffsets EARLIEST` fix so trim stays wire-safe, rejection of transactional produce for diskless topics, and flipping the Slice-3 trim gate on — in that order.

**Architecture:** A Fetch for an offset below the local floor already returns `OFFSET_OUT_OF_RANGE` and hits the dispatch at `fetch.rs:514`. Slice 4 adds `try_diskless_read` alongside `try_remote_read` (mutually exclusive: diskless vs KIP-405-tiered), which does `WalIndexCache.lookup → object get_range → first_batch_at_or_after → records`. `ListOffsets EARLIEST` for a diskless partition becomes `min(local_start, WalIndexCache.earliest_covered)` (mirroring the tiered branch, so the consumer-visible earliest stays at the earliest object-covered offset while trim advances the local floor). Then `trim_safety_lag` flips from `None` to `Some(lag)`.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `object_store` 0.13, `tokio`, `bytes`, `uuid`, `assert2`, `cargo +nightly fmt`, `clippy::pedantic` (`unsafe_code = "forbid"`).

**Spec:** [`docs/superpowers/specs/2026-07-05-crabka-diskless-wal-slice4-design.md`](../specs/2026-07-05-crabka-diskless-wal-slice4-design.md).

**PREREQUISITES (unlanded):** Slices 1–3. This plan consumes: the Slice-1 `diskless` per-topic flag — **surfaced on `LogConfig` (mirroring `remote_storage_enable`)** so fetch/produce/list-offsets read it locally; Slice-3's `WalIndexCache` (+ its shared projection from `DisklessIndexLog`) and the flush object store (`build_object_store`); and Slice-3's `FlushConfig.trim_safety_lag` gate. Land Slices 1–3 first.

---

## Invariants

1. **Land in order:** (1) cold read → (2) `ListOffsets` fix → (3) enable trim. Never enable trim before (1)+(2).
2. **Cold reads byte-exact.** Reuse the landed `first_batch_at_or_after` scan; the returned batch's bytes must round-trip byte-identically to the pre-trim local batch (the wire-compat gate test).
3. **Cold path returns owned bytes** (`p.out.records = Some(batch.into())`), never a sendfile `FileRegion`. Mirror `try_remote_read` (`fetch.rs:1368`).
4. **`ListOffsets EARLIEST` stays ≤ any fetchable offset.** The diskless min-branch mirrors the tiered branch (`list_offsets.rs:142-162`).
5. **Diskless is non-transactional this slice.** Transactional produce to a diskless topic is rejected; `LSO = HW`.
6. **Hot path + ack path + KIP-405 untouched.** Hot fetches still use sendfile; `try_remote_read` still serves tiered topics; the two cold predicates are mutually exclusive.
7. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** `try_diskless_read` + its dispatch; `WalIndexCache::earliest_covered` + the ListOffsets branch; transactional-produce rejection; enabling + tightening trim; the coverage/byte-exact/wire tests.
- **Deferred:** object retention/GC; crash atomicity of trim-vs-index (Slice 5); leaderless serving + in-memory hot-tail cache (Slice 6); transactional diskless (an aborted-txn manifest in the object).

---

## File Structure

- **`crates/broker/src/remote_reader.rs`** — make `first_batch_at_or_after` `pub(crate)`.
- **`crates/broker/src/diskless/read.rs`** (new) — `DisklessReadHandle` (the shared `WalIndexCache` + object store) + `try_diskless_read`.
- **`crates/broker/src/broker.rs`** — a `diskless_read: Option<Arc<DisklessReadHandle>>` field, wired at construction.
- **`crates/broker/src/handlers/fetch.rs`** — dispatch `try_diskless_read` at the `OFFSET_OUT_OF_RANGE` seam (`:514`, `:1471`).
- **`crates/broker/src/diskless/wal_index.rs`** (Slice 3) — add `WalIndexCache::earliest_covered`.
- **`crates/broker/src/handlers/list_offsets.rs`** — the diskless `EARLIEST` min-branch.
- **`crates/broker/src/handlers/produce.rs`** — reject transactional produce for diskless topics.
- **`crates/broker/src/diskless/flusher.rs`** (Slice 3) — set `trim_safety_lag`; tighten the gate to index-projected offsets.

---

## Task 1: The `try_diskless_read` cold path + dispatch

**Files:**
- Modify: `crates/broker/src/remote_reader.rs`, `crates/broker/src/broker.rs`, `crates/broker/src/handlers/fetch.rs`
- Create: `crates/broker/src/diskless/read.rs`

- [ ] **Step 1: Expose the batch scanner**

In `crates/broker/src/remote_reader.rs`, change `fn first_batch_at_or_after` (`:432`) to `pub(crate) fn first_batch_at_or_after`.

- [ ] **Step 2: Add the Broker handle field**

In `crates/broker/src/broker.rs`, add to `struct Broker` (next to `remote_reader`, `:202`):

```rust
    pub(crate) diskless_read: Option<Arc<crate::diskless::read::DisklessReadHandle>>,
```

Wire it at broker construction next to where `remote_reader` is built (`broker.rs:3330`): `Some` when the broker runs diskless topics, holding the shared `WalIndexCache` projection (from Slice-3 `DisklessIndexLog`) and the flush object store (`build_object_store(&cfg)?`); `None` otherwise. (Thread the two through the same constructor path `remote_reader` uses.)

- [ ] **Step 3: Write the failing cold-read test**

Create `crates/broker/src/diskless/read.rs` with its test module first — a unit test of `try_diskless_read` against an `InMemory` object store seeded with a framed WAL object and a `WalIndexCache` populated with its entry:

```rust
#[cfg(test)]
mod tests {
    // Build an InMemory store, PUT one WAL object holding a known partition run
    // (verbatim v2 batches for offsets [5..=9]); populate a WalIndexCache with a
    // WalFlushRecord for it; construct a DisklessReadHandle; call try_diskless_read
    // with fetch_offset=7 and assert:
    //   - it returns Some(bytes_est),
    //   - p.out.error_code == NONE,
    //   - p.out.records decodes to a batch whose last_offset >= 7,
    //   - the returned bytes round-trip byte-identically to the object's run slice
    //     from the covering batch boundary (the wire-compat gate).
    #[tokio::test]
    async fn cold_read_returns_byte_exact_covering_batch() { /* ... */ }

    #[tokio::test]
    async fn cold_read_miss_leaves_out_of_range() {
        // WalIndexCache lookup miss -> returns None, p.out.error_code stays OFFSET_OUT_OF_RANGE.
    }
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cargo test -p crabka-broker cold_read`
Expected: FAIL — `DisklessReadHandle`/`try_diskless_read` undefined.

- [ ] **Step 5: Implement `DisklessReadHandle` + `try_diskless_read`**

Insert at the TOP of `crates/broker/src/diskless/read.rs` (mirror `try_remote_read`'s shape, `fetch.rs:1275-1370`, swapping the RLMM lookup for `WalIndexCache` + `get_range`):

```rust
//! Diskless cold read: serve a Fetch for a trimmed offset from the shared WAL
//! object via the WalIndexCache floor lookup + a ranged GET. Mirrors
//! `try_remote_read` but with a run-granular index and object-store range read.

use std::sync::Arc;

use object_store::{GetOptions, GetRange, ObjectStore};
use tokio::sync::Mutex;

use super::wal_index::WalIndexCache;

/// The shared state the diskless cold read needs: the index projection + the
/// flush object store.
pub(crate) struct DisklessReadHandle {
    pub(crate) index: Arc<Mutex<WalIndexCache>>,
    pub(crate) store: Arc<dyn ObjectStore>,
}

/// Serve `p.fetch_offset` from object storage when the local read returned
/// `OFFSET_OUT_OF_RANGE` and the partition is diskless. Returns the estimated
/// byte size on a hit; `None` (leaving `OFFSET_OUT_OF_RANGE`) on a miss.
pub(crate) async fn try_diskless_read(
    broker: &crate::broker::Broker,
    p: &mut crate::handlers::fetch::PendingRead,
    part: &crate::partition::Partition,
) -> Option<usize> {
    let handle = broker.diskless_read.clone()?;
    // Diskless topic-mode gate (mutually exclusive with remote.storage.enable).
    let diskless = {
        let log = part.log.lock().expect("log mutex poisoned");
        log.config_snapshot().diskless // Slice-1 flag surfaced on LogConfig
    };
    if !diskless || p.topic_id == crate::handlers::fetch::WireUuid::ZERO {
        return None;
    }
    let topic_id = uuid::Uuid::from_bytes(p.topic_id.0);

    let (object_key, byte_start, byte_len) = {
        let idx = handle.index.lock().await;
        idx.lookup(topic_id, p.partition_index, p.fetch_offset)?
    };
    let path = object_store::path::Path::from(object_key);
    let range = GetRange::Bounded(byte_start..byte_start + u64::from(byte_len));
    let run = handle
        .store
        .get_opts(&path, GetOptions { range: Some(range), ..Default::default() })
        .await
        .ok()?
        .bytes()
        .await
        .ok()?;

    let batch = crate::remote_reader::first_batch_at_or_after(&run, p.fetch_offset)?;
    let bytes_est = <crabka_protocol::records::RecordBatch as crabka_protocol::Encode>::encoded_len(&batch, 0);
    p.out.error_code = crate::codes::NONE;
    // Diskless is non-transactional this slice: an empty abort list is correct
    // in read_committed (there are no aborts). LSO/HW/log_start stay local.
    if p.read_committed && !p.is_follower_fetch {
        p.out.aborted_transactions = Some(Vec::new());
    }
    p.out.records = Some(batch.into());
    Some(bytes_est)
}
```

(Confirm the exact `PendingRead`/`WireUuid`/`codes` paths against `fetch.rs` — mirror `try_remote_read`'s imports. Add `pub(crate) mod read;` to `crates/broker/src/diskless/mod.rs`.)

- [ ] **Step 6: Dispatch it at the `OFFSET_OUT_OF_RANGE` seam**

In `crates/broker/src/handlers/fetch.rs`, replace the dispatch at `:514-518` with a mutually-exclusive pair (and mirror the same change at the long-poll re-read `:1471-1473`):

```rust
        if p.out.error_code == codes::OFFSET_OUT_OF_RANGE {
            if let Some(b) = try_remote_read(broker, p, &part).await {
                total_bytes += b;
            } else if let Some(b) = crate::diskless::read::try_diskless_read(broker, p, &part).await {
                total_bytes += b;
            }
        }
```

- [ ] **Step 7: Run to verify it passes**

Run: `cargo test -p crabka-broker cold_read`
Expected: PASS — byte-exact cold read; miss leaves `OFFSET_OUT_OF_RANGE`.

- [ ] **Step 8: Commit**

```bash
git add crates/broker/src/remote_reader.rs crates/broker/src/broker.rs crates/broker/src/diskless/ crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): diskless cold fetch-from-object (try_diskless_read)"
```

---

## Task 2: `ListOffsets EARLIEST` diskless min-branch

**Files:**
- Modify: `crates/broker/src/diskless/wal_index.rs`, `crates/broker/src/handlers/list_offsets.rs`

- [ ] **Step 1: Add `WalIndexCache::earliest_covered` (failing test)**

In `crates/broker/src/diskless/wal_index.rs` tests:

```rust
    #[test]
    fn earliest_covered_is_smallest_first_offset() {
        let mut c = WalIndexCache::default();
        c.apply(&WalFlushRecord { object_key: "o2".into(), format_version: 1, entries: vec![entry(0, 5, 9)] });
        c.apply(&WalFlushRecord { object_key: "o1".into(), format_version: 1, entries: vec![entry(0, 0, 4)] });
        assert2::assert!(c.earliest_covered(uuid::Uuid::from_u128(1), 0) == Some(0));
        assert2::assert!(c.earliest_covered(uuid::Uuid::from_u128(1), 9) == None);
    }
```

- [ ] **Step 2: Run to verify it fails; implement**

Run: `cargo test -p crabka-broker earliest_covered` → FAIL. Then add to `impl WalIndexCache`:

```rust
    /// The smallest first_offset covered for a partition (the earliest
    /// object-covered offset), or None if nothing is covered.
    #[must_use]
    pub fn earliest_covered(&self, topic_id: uuid::Uuid, partition: i32) -> Option<i64> {
        self.by_tp
            .get(&(topic_id, partition))
            .and_then(|m| m.keys().next().copied())
    }
```

Run: `cargo test -p crabka-broker earliest_covered` → PASS.

- [ ] **Step 3: Wire the ListOffsets branch (failing test)**

In `crates/broker/src/handlers/list_offsets.rs`, mirror the tiered `EARLIEST` branch (`:142-162`): for a diskless partition, `earliest = earliest.min(diskless_earliest_covered)`. Add:

```rust
                    EARLIEST_TIMESTAMP => {
                        let mut earliest = local_start;
                        // ... existing tiered `reader.earliest_offset` min ...
                        if let (Some(handle), Some(tid)) = (broker.diskless_read.as_ref(), topic_id_of(&topic.name)) {
                            let cov = { handle.index.blocking_lock().earliest_covered(tid, idx) };
                            if let Some(c) = cov { earliest = earliest.min(c); }
                        }
                        (earliest, UNKNOWN_TIMESTAMP)
                    }
```

Add a test: after a trim advances `local_log_start` past 0, `EARLIEST (-2)` still returns 0 (object-covered), `EARLIEST_LOCAL (-4)` returns the advanced local floor, `LATEST (-1)` unchanged.

(`topic_id_of` = resolve the topic's id from `controller.current_image()` as the tiered branch does at `:131-138`; `blocking_lock` is valid here only if the handler is sync at this point — if it is async, use `.lock().await`. Match the surrounding handler's sync/async shape.)

- [ ] **Step 4: Run to verify + commit**

Run: `cargo test -p crabka-broker list_offsets` → PASS.

```bash
git add crates/broker/src/diskless/wal_index.rs crates/broker/src/handlers/list_offsets.rs
git commit -m "feat(broker): ListOffsets EARLIEST anchors at diskless object-covered floor"
```

---

## Task 3: Reject transactional produce for diskless topics

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Write the failing test**

A produce with a transactional batch (`attributes.is_transactional()`) to a diskless topic returns `INVALID_TXN_STATE` (48) for that partition and does not append.

```rust
    #[tokio::test]
    async fn transactional_produce_to_diskless_topic_is_rejected() { /* ... */ }
```

- [ ] **Step 2: Run to verify it fails; implement**

Run → FAIL. In `crates/broker/src/handlers/produce.rs`, near the `is_transactional` computation (`:537`) / txn handling (`:541`), before the txn path: if the target topic is diskless (`log.config_snapshot().diskless`) and `is_transactional`, set the partition's `error_code = codes::INVALID_TXN_STATE` and skip the append (mirror the existing per-partition error-and-skip pattern). Document that diskless is non-transactional this slice and `LSO = HW` for diskless partitions (no pending txns).

- [ ] **Step 3: Run to verify + commit**

Run: `cargo test -p crabka-broker transactional_produce_to_diskless` → PASS.

```bash
git add crates/broker/src/handlers/produce.rs
git commit -m "feat(broker): reject transactional produce to diskless topics (Slice 4)"
```

---

## Task 4: Enable trim (flip the gate, tighten to index-projected offsets)

**Files:**
- Modify: `crates/broker/src/diskless/flusher.rs`

- [ ] **Step 1: Write the failing trimmed-then-fetched test**

An end-to-end test: produce offsets 0..20 to a diskless partition; run flushes; set `trim_safety_lag = Some(lag)`; let a trim advance the local floor past some offset O; then fetch at O and assert it is served byte-exact from object storage (the cold path), and a fetch at `local_floor` (hot) still uses the local/sendfile path.

- [ ] **Step 2: Run to verify it fails; implement**

Run → FAIL (trim disabled or coverage hole). Implement in `crates/broker/src/diskless/flusher.rs`:
- Change the default `FlushConfig.trim_safety_lag` to `Some(DEFAULT_TRIM_SAFETY_LAG)` (a real value, e.g. keeping a live local tail), OR make it broker-config-driven defaulting to enabled — but **tighten the gate**: `trim_target = min(index_projected_frontier, hw − lag)`, where `index_projected_frontier` is `WalIndexCache.flushed_frontier(tp)` (only offsets whose index entry is already projected — index durability, not merely object PUT). Never trim past that. Keep the existing `WriterMessage::TrimToOffset` send (`partition.rs:329`).

- [ ] **Step 3: Run to verify + commit**

Run: `cargo test -p crabka-broker trimmed_then_fetched` → PASS.

```bash
git add crates/broker/src/diskless/flusher.rs
git commit -m "feat(broker): enable diskless local-WAL trim (gated on index-projected offsets)"
```

---

## Task 5: Cross-cutting proof tests

**Files:**
- Modify: `crates/broker/src/diskless/` (a `tests`/read test module)

- [ ] **Step 1: Union coverage — no gap, no overlap**

Over `[earliest_object, hw)`, assert every offset is served exactly once: `[earliest_object, local_floor)` cold, `[local_floor, hw)` local. Pin the boundary: `O == local_floor` routes local; `O == local_floor − 1` routes cold. No offset returns `OFFSET_OUT_OF_RANGE` while covered; no offset is ambiguously served by both.

- [ ] **Step 2: Mid-batch positioning**

A cold fetch with `base < O ≤ batch_last` returns from the covering batch boundary (client skips records `< O`); assert byte-exact.

- [ ] **Step 3: Ack + hot path + KIP-405 untouched**

A hot fetch (offset ≥ local floor) still takes the sendfile/`FileRegions` path (not the cold owned-bytes path); produce/ack semantics unchanged; a KIP-405 tiered topic still routes via `try_remote_read` (the diskless predicate returns `None` for it).

- [ ] **Step 4: Routing miss**

A genuinely-uncovered offset (below `earliest_covered`, or a cache miss) returns `OFFSET_OUT_OF_RANGE` (retryable, not a hard error).

- [ ] **Step 5: Run + commit**

Run: `cargo test -p crabka-broker diskless` → PASS across all diskless tests.

```bash
git add crates/broker/src/diskless/
git commit -m "test(broker): diskless cold-read coverage, boundary, and untouched-path proofs"
```

---

## Task 6: Final gate

- [ ] **Step 1:** `cargo +nightly fmt` then `--check` — no diff.
- [ ] **Step 2:** `cargo clippy --workspace --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-broker` (or `cargo test`) — PASS.
- [ ] **Step 4:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** cold read at the `OFFSET_OUT_OF_RANGE` seam (Task 1); `WalIndexCache.earliest_covered` + ListOffsets `EARLIEST` min (Task 2); non-transactional diskless (Task 3); enable + tighten trim (Task 4); union-coverage / boundary / byte-exact / untouched-path / routing proofs (Tasks 1,4,5). Deferred set (object retention, crash atomicity S5, hot-tail/leaderless S6, transactional diskless) untouched — Scope boundary. ✅

**2. Placeholder scan:** Task 1's cold-read body and Task 2's accessor are complete code. Tasks 2-5's handler edits give the concrete branch + the exact template to mirror (`list_offsets.rs:142-162`, `produce.rs:537`, `fetch.rs:1275-1370`) with the surrounding sync/async caveat named. No `TBD`/`TODO`.

**3. Type consistency:** `try_diskless_read(&Broker, &mut PendingRead, &Partition) -> Option<usize>` mirrors `try_remote_read` exactly and is dispatched identically (Task 1). `WalIndexCache::{lookup, earliest_covered, flushed_frontier}` (Slice 3 + Task 2) are used consistently by the cold read (Task 1), ListOffsets (Task 2), and the trim gate (Task 4). `DisklessReadHandle{index, store}` is the single handle threaded from the Broker field to the cold read and ListOffsets.

**4. Invariant check:** land-order enforced by task order (cold read T1 → ListOffsets T2 → enable trim T4); byte-exact via reused scan + round-trip test (T1/T5); owned-bytes not sendfile (T1); EARLIEST ≤ fetchable (T2); non-transactional (T3); hot/ack/KIP-405 untouched (T5); trim gated on index-projected offsets (T4). Each task green.

**5. Prerequisites flagged:** Slices 1-3 unlanded + the Slice-1 `diskless` flag surfaced on `LogConfig` — stated in the header.
