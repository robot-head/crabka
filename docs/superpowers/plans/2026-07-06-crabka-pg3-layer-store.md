# PG-3: The versioned-page layer store — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A new async crate `crabka-page-store`: immutable delta/image layers (one container format, footer + sparse index, byte-range reads) on the object bucket, an open-layer ingest of PG-2's `Sharded` stream with idempotent flush, a `list()`-rebuildable layer map, `get_reconstruct_data(key, lsn)`, and structural L0→L1 compaction — gated by an FPI byte-match test over the PG-2 fixture corpus.

**Architecture:** Single-writer ingest per timeline fills a `BTreeMap` open layer (`Value::Image` for FPIs, `Value::Wal{will_init}` otherwise; `Meta` retained verbatim), flushing L0 delta layers via `ObjectOps::put_from_path`; readers query an `Arc<RwLock<LayerMap>>` and read layers by `get_range`. No redo — reads return a reconstruction *plan*.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `tokio`, `bytes`, `thiserror`, `crabka-postgres-wal` (PG-2 types), `crabka-object-store` (`ObjectOps`, `InMemory` for tests), `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg3-layer-store-design.md`](../specs/2026-07-06-crabka-pg3-layer-store-design.md).

**PREREQUISITES (unlanded):** **PG-2** (`crabka-postgres-wal` — its `Lsn`/`PageKey`/`RelTag`/`Sharded`/decoder types and its committed fixture corpus). Nothing else; the object-store crate is landed.

---

## Invariants

1. **Layers are immutable** — written once, never overwritten; point reads via footer→index→`get_range`, never whole-object downloads.
2. **Gap-safe history:** `get_reconstruct_data(K, L)` returns the newest base ≤ L (image or `will_init`) and *exactly* the deltas in `(base, L]`, oldest-first — never a delta below the base, never a missing delta.
3. **Idempotent ingest:** re-feeding WAL ≤ `disk_consistent_lsn` changes nothing.
4. **Rebuildable:** the layer map is a pure function of the bucket listing (names encode kind/key-range/LSN-range).
5. **FPI byte fidelity:** a stored FPI base is byte-identical to the WAL's hole-reconstructed image.
6. **Compaction preserves reads:** `get_reconstruct_data` results identical before/after L0→L1.
7. **New-crate hygiene:** `publish = false` + private release-plz entry; every task ends green before its commit.

## Scope boundary

- **In scope:** container format (both kinds) + reader/writer; open layer + flush + `disk_consistent_lsn`; layer map + rebuild; `get_reconstruct_data`; meta-lane retention; L0→L1 structural compaction; the fixture-corpus gate.
- **Deferred:** redo/materialization, image-layer *creation*, GC (PG-4); rmgr interpretation/SLRU (PG-4+); `nblocks` service (PG-5); branching (PG-6); live ingest (PG-1); multi-node sharding.

---

## File Structure

- **`crates/page-store/`** (new crate `crabka-page-store`):
  - `Cargo.toml` (`publish = false`; deps: `crabka-postgres-wal`, `crabka-object-store`, `tokio`, `bytes`, `thiserror`)
  - `src/lib.rs`, `src/value.rs` (`Value`), `src/name.rs` (`LayerName` encode/parse)
  - `src/container.rs` — the shared layer file format (writer + reader)
  - `src/open_layer.rs` — the ingest buffer + flush
  - `src/layer_map.rs` — the map + rebuild + `get_reconstruct_data`
  - `src/compact.rs` — L0→L1
  - `tests/fixture_gate.rs` — the end-to-end FPI gate (reads PG-2's corpus via `../postgres-wal/tests/fixtures` from `CARGO_MANIFEST_DIR`)
- **`release-plz.toml`** — private entry (alphabetical slot).

Tasks 1–2 are foundation; Tasks 3 and 4 both build on 2 and touch disjoint files (`open_layer.rs` vs `layer_map.rs`) → **parallel batch**; Task 5 (gate) needs 3+4; Task 6 (compaction) needs 4; Task 7 last.

---

## Task 1: Scaffold + core types (`Value`, `LayerName`)

**Files:**
- Create: `crates/page-store/{Cargo.toml, src/lib.rs, src/value.rs, src/name.rs}`
- Modify: `release-plz.toml`

- [ ] **Step 1: Write the failing tests**

```rust
// src/name.rs tests
    #[test]
    fn layer_name_round_trips() {
        let n = LayerName {
            kind: LayerKind::Delta,
            key_start: key(1663, 5, 16384, 0, 0),   // (spc, db, rel, fork, blk)
            key_end: key(1663, 5, 16384, 0, 128),
            lsn_start: Lsn(0x0100_0000),
            lsn_end: Lsn(0x0140_0000),
        };
        assert!(LayerName::parse(&n.encode()).unwrap() == n);
    }

    #[test]
    fn name_encoding_sorts_like_keys() {
        // Property: for any two keys, hex-name ordering == PageKey ordering.
        let a = key(1663, 5, 16384, 0, 7);
        let b = key(1663, 5, 16385, 0, 0);
        assert!((a < b) == (encode_key(&a) < encode_key(&b)));
    }

// src/value.rs tests
    #[test]
    fn image_value_is_exactly_a_page() {
        let_assert!(Err(_) = Value::image(Bytes::from(vec![0u8; 100]))); // must be 8192
        assert!(Value::image(Bytes::from(vec![0u8; 8192])).is_ok());
    }
```

- [ ] **Step 2: Run to verify failure, then implement**

`Value::{Image(Bytes), Wal{will_init: bool, rec: Bytes}}` with the 8192-byte image guard; `LayerKind::{Delta, Image}`; `LayerName` with fixed-width lowercase-hex encoding of `(spc, db, rel, fork, blk)` fields and LSNs — `pg/<tenant>/<timeline>/<key_start>-<key_end>__<lsn_start>-<lsn_end>.<delta|image>`. `Cargo.toml` with `publish = false` (comment: internal; see the publish allowlist). Add the `crabka-page-store` private entry to `release-plz.toml` (alphabetical slot, `publish = false` / `release = false`).

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store --lib` → PASS; `./tools/check-publish-allowlist.sh` → exit 0.

```bash
git add crates/page-store release-plz.toml
git commit -m "feat(page-store): scaffold + layer naming and value types"
```

---

## Task 2: The layer container format (writer + reader)

**Files:**
- Create: `crates/page-store/src/container.rs`

- [ ] **Step 1: Write the failing tests** (over `ObjectStoreConfig::InMemory` via `ObjectOps`)

```rust
    #[tokio::test]
    async fn container_round_trips_and_point_reads() {
        let ops = in_memory_ops();
        let entries = vec![
            (key(1663,5,16384,0,0), Lsn(10), Value::image(page(b'a')).unwrap()),
            (key(1663,5,16384,0,0), Lsn(20), Value::Wal { will_init: false, rec: Bytes::from_static(b"r1") }),
            (key(1663,5,16384,0,7), Lsn(15), Value::Wal { will_init: true,  rec: Bytes::from_static(b"r2") }),
        ];
        let name = write_layer(&ops, &tl(), LayerKind::Delta, &entries).await.unwrap();
        let rdr = LayerReader::open(&ops, &name).await.unwrap();           // footer + sparse index only
        let got = rdr.entries_for_key(&key(1663,5,16384,0,0)).await.unwrap(); // ranged read
        assert!(got.len() == 2 && got[0].1 == Lsn(10));
        let_assert!(Value::Image(img) = &got[0].2);
        assert!(img.as_ref() == page(b'a').as_ref());
    }

    #[tokio::test]
    async fn corrupt_footer_is_a_loud_error() {
        // Flip a byte in the stored object's footer; LayerReader::open -> Err(LayerError::Corrupt{key,..}).
    }
```

- [ ] **Step 2: Implement**

Writer: stream entries (sorted by `(key, lsn)`; enforce sortedness) to a temp file — `[header: magic, version, kind, tenant/timeline, key-range, lsn-range] · [entries: key, lsn, value_tag, len, bytes] · [sparse index: every Nth entry's (key, lsn) → byte offset] · [footer: index_off, index_len, entry_count, crc32c(header..index), footer_magic]` — then `ObjectOps::put_from_path`. Reader: `get_range` the fixed-size footer tail, validate magic+CRC coverage, `get_range` the sparse index, then `entries_for_key` binary-searches the index and `get_range`s only the covering entry block. Reuse the workspace CRC-32C choice from PG-2.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store --lib container` → PASS.

```bash
git add crates/page-store/src/container.rs crates/page-store/src/lib.rs
git commit -m "feat(page-store): immutable layer container with footer index + ranged point reads"
```

---

## Task 3 (∥ Task 4): Open layer, flush, `disk_consistent_lsn`, idempotence

**Files:**
- Create: `crates/page-store/src/open_layer.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[tokio::test]
    async fn flush_produces_l0_and_advances_disk_consistent_lsn() {
        let (mut ing, ops) = ingest_on_inmemory();
        ing.put(key0(), Lsn(10), wal(b"r1"));
        ing.put(key0(), Lsn(20), wal(b"r2"));
        let flushed = ing.flush(Lsn(20)).await.unwrap();
        assert!(flushed.lsn_end == Lsn(20) && ing.disk_consistent_lsn() == Lsn(20));
        assert!(ops_list_layers(&ops).await.len() == 1); // one L0, full key range
    }

    #[tokio::test]
    async fn reingest_below_disk_consistent_lsn_is_a_noop() {
        // put+flush [10..20]; put the SAME (key,lsn,value) pairs again; flush(20);
        // assert no new object and identical listing.
    }

    #[test]
    fn meta_records_are_retained_verbatim() { /* Sharded::Meta -> the meta lane buffer, bytes unchanged */ }
```

- [ ] **Step 2: Implement**

`OpenLayer` (`BTreeMap<(PageKey, Lsn), Value>` + a meta-lane `Vec<(Lsn, u8 /*rmid*/, Bytes)>`); `Ingest::put` maps a `Sharded::Page` to `Value::Image` when the block ref carries an FPI else `Value::Wal{will_init}` (from `BKPBLOCK_WILL_INIT`); inserts at or below `disk_consistent_lsn` are dropped (idempotence). `flush(upto)` writes an L0 delta layer covering the full key range for the buffered LSN span via Task 2's writer (meta lane flushes beside it under a `.meta` suffix), then advances `disk_consistent_lsn`.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store --lib open_layer` → PASS.

```bash
git add crates/page-store/src/open_layer.rs crates/page-store/src/lib.rs
git commit -m "feat(page-store): open-layer ingest with idempotent flush to L0"
```

---

## Task 4 (∥ Task 3): Layer map + `get_reconstruct_data` + rebuild

**Files:**
- Create: `crates/page-store/src/layer_map.rs`

- [ ] **Step 1: Write the failing tests** (synthetic layers via Task 2's writer)

```rust
    #[tokio::test]
    async fn reconstruct_stops_at_image_base() {
        // Layers: delta[Lsn 10: Image(A)], delta[Lsn 20: Wal r1], delta[Lsn 30: Wal r2]
        let rd = map.get_reconstruct_data(&key0(), Lsn(25)).await.unwrap();
        let_assert!(Some((Lsn(10), img)) = &rd.base);
        assert!(rd.deltas.iter().map(|d| d.0).collect::<Vec<_>>() == vec![Lsn(20)]); // oldest-first, ≤ 25 only
    }

    #[tokio::test]
    async fn will_init_terminates_with_no_base() {
        // Wal{will_init:true} at Lsn 20 under a query at 30: base None; deltas = [20, 30]
        // (the will_init record IS the first delta; redo applies it against a zeroed page).
    }

    #[tokio::test]
    async fn below_oldest_history_is_trimmed_error() { /* query at Lsn 5 -> LayerError::HistoryTrimmed */ }

    #[tokio::test]
    async fn rebuild_from_listing_matches() {
        // Build map A by registration; map B = LayerMap::rebuild(&ops, &tl()).await;
        // identical get_reconstruct_data on a probe set.
    }
```

- [ ] **Step 2: Implement**

`LayerMap` (sorted-by-`lsn_end` vector of `LayerDesc { name, kind, key_range, lsn_range }`): `query(key, lsn)` iterates intersecting layers newest-first; `get_reconstruct_data` collects values for the key with `lsn ≤ L`, terminating at `Value::Image` (→ `base`) or `Value::Wal{will_init: true}` (→ first delta, `base: None`), then reverses deltas to oldest-first. `rebuild(ops, timeline)` = `ObjectOps::list(prefix)` + `LayerName::parse`. `ReconstructData { base: Option<(Lsn, Bytes)>, deltas: Vec<(Lsn, Bytes)> }`.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store --lib layer_map` → PASS.

```bash
git add crates/page-store/src/layer_map.rs crates/page-store/src/lib.rs
git commit -m "feat(page-store): layer map, get_reconstruct_data, rebuild-from-listing"
```

---

## Task 5: The fixture gate — end-to-end FPI byte match

**Files:**
- Create: `crates/page-store/tests/fixture_gate.rs`

Depends on Tasks 3 + 4 (+ PG-2's landed corpus).

- [ ] **Step 1: Write the gate test**

Drive the full path on `InMemory`: `WalStreamDecoder` over PG-2's committed segments (path: `{CARGO_MANIFEST_DIR}/../postgres-wal/tests/fixtures`) → `shard_record` → `Ingest::put` → periodic `flush`. Then: **for every FPI-bearing `(key, lsn)` observed during decode** (collect them while ingesting), `get_reconstruct_data(key, lsn)` returns `base == Some((lsn, image))` with the image **byte-identical** to the WAL's hole-reconstructed FPI and **zero deltas**. Also assert: a delta-only page (no FPI in corpus) returns `base: None` + a non-empty oldest-first delta chain (structural check only — content validation is PG-4); re-running the ingest over the same corpus (idempotence at scale) leaves the listing unchanged.

- [ ] **Step 2: Run to verify it passes**

Run: `cargo test -p crabka-page-store --test fixture_gate` → PASS. A base/byte mismatch here is a real ingest/container bug — fix the store, never the assertion.

- [ ] **Step 3: Commit**

```bash
git add crates/page-store/tests/fixture_gate.rs
git commit -m "test(page-store): end-to-end fixture gate with FPI byte-match oracle"
```

---

## Task 6: Structural compaction (L0 → L1)

**Files:**
- Create: `crates/page-store/src/compact.rs`

- [ ] **Step 1: Write the failing test**

Build ≥ `L0_COMPACT_THRESHOLD` L0 layers (via Task 3 flushes over the fixture corpus); snapshot `get_reconstruct_data` over a probe key/LSN grid; run `compact_l0(&ops, &mut map)`; assert (a) results **identical** on the full grid, (b) the L0s are replaced by key-partitioned L1s, (c) per-query layer fan-in decreased (count layers touched).

- [ ] **Step 2: Implement**

K-way merge of the L0 entry streams (already `(key, lsn)`-sorted) split into key-partitioned L1 delta layers (target size bound), written with Task 2's writer; swap descriptors in the map atomically (write new → register → deregister old; the old objects are deleted only after the swap — readers hold immutable objects).

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store` → PASS (compaction + all prior).

```bash
git add crates/page-store/src/compact.rs crates/page-store/src/lib.rs
git commit -m "feat(page-store): structural L0->L1 compaction preserving reconstruction"
```

---

## Task 7: Final gate

- [ ] **Step 1:** `cargo +nightly fmt --check` — no diff.
- [ ] **Step 2:** `cargo clippy -p crabka-page-store --all-targets -- -D warnings` — no warnings.
- [ ] **Step 3:** `cargo nextest run -p crabka-page-store` — PASS (container, ingest, map, gate, compaction).
- [ ] **Step 4:** `./tools/check-publish-allowlist.sh` — exit 0.
- [ ] **Step 5:** Commit any formatting.

---

## Self-Review

**1. Spec coverage:** container format for both kinds (Task 2); `Value::{Image, Wal{will_init}}` + naming (Task 1); open layer + idempotent flush + `disk_consistent_lsn` + meta retention (Task 3); layer map + `get_reconstruct_data` (image/`will_init` termination, trimmed-history error) + rebuild-from-listing (Task 4); the FPI byte-match gate + delta-only structural check + at-scale idempotence (Task 5); structural L0→L1 with read-equivalence (Task 6); allowlist hygiene (Tasks 1, 7). Deferred set (redo, image creation, GC, SLRU, nblocks, branching, live ingest) untouched — Scope boundary. ✅

**2. Placeholder scan:** container layout, termination semantics, and the compaction swap discipline are spelled out; test bodies given for every pure/decisive behavior; I/O test helpers (`in_memory_ops`, `ingest_on_inmemory`) are small fixtures the implementer writes alongside. No `TBD`.

**3. Type consistency:** `Value` (Task 1) is written/read by the container (Task 2), produced by ingest (Task 3), interpreted by `get_reconstruct_data` (Task 4), and asserted in the gate (Task 5); `LayerName` (Task 1) is the rebuild contract (Task 4); `ReconstructData{base, deltas}` shape is identical in Tasks 4–6 tests; `Lsn`/`PageKey`/`Sharded` come from `crabka-postgres-wal` throughout.

**4. Invariant check:** immutability + ranged reads (Task 2); gap-safe plan semantics (Task 4 tests); idempotence (Tasks 3, 5); rebuildability (Task 4); FPI byte fidelity (Task 5); compaction equivalence (Task 6); allowlist green (Tasks 1, 7). Each task green before commit.

**5. Prerequisites flagged:** PG-2 (`crabka-postgres-wal` + its corpus) is the one unlanded prerequisite — stated in the header. Batching: 1 → 2 → (3 ∥ 4) → 5 → 6 → 7.
