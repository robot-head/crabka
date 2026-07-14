# PG-4b: SLRU materialization, relation lifecycle, and the index-rmgr long tail — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Interpret what PG-3 retained: clog/multixact SLRU pages materialized through the layer store (tagged `Key` enum — the PG-3 amendment), a `RelMeta` projection making `GetRelSize` exact (+ `BlockBeyondEof`/`NotFound` semantics), and the five index-rmgr redo arm families — extending the standby gate to SLRU segment bytes and unblocking PG-5's boot gate.

**Architecture:** A light meta interpreter in `crabka-postgres-redo` (`shard_meta`: which keys does this record touch?) feeds the existing ingest; full interpretation lives in new redo arms (clog status folding, multixact writes, RelMeta lifecycle folding) beside PG-4's. Basebackup renders SLRU pages into segment files. Index families land per-arm with per-family fixtures + standby differentials.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), the PG-2/3/4 crates, `proptest` (fan-out properties), `assert2`/`nextest`, a local/containerized PG 17 for fixture regeneration, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg4b-slru-longtail-design.md`](../specs/2026-07-06-crabka-pg4b-slru-longtail-design.md).

**PREREQUISITES (unlanded):** PG-2, PG-3, PG-4 executed (this plan modifies their crates). If PG-3 execution has not started, implement the `Key` enum there from the start (Task 1 becomes a no-op fold-in).

---

## Invariants

1. **One store:** SLRU and RelMeta state live in the layer store under the tagged `Key` — no parallel storage machinery.
2. **Ingest-light / redo-full:** ingest interpretation computes only touched keys; semantic folding happens in redo arms.
3. **Versioned lifecycle:** reads below a truncate/drop LSN still serve the old state; at/above it, `BlockBeyondEof`/`NotFound`.
4. **Per-family shipping:** no index arm family ships without its fixture workload + standby byte-differential; until then it fails loudly.
5. **SLRU byte-exactness:** `pg_xact`/`pg_multixact` segments at the capture LSN equal the standby's, byte-for-byte, no masking.
6. **Every task ends green** before its commit.

## Scope boundary

- **In scope:** the `Key` enum; `shard_meta` (XACT/CLOG/MULTIXACT/SMGR/DBASE); clog + multixact + RelMeta redo arms; exact `GetRelSize`; basebackup SLRU segments; BRIN/hash/GiST/SP-GiST/GIN arms; the extended gate.
- **Deferred:** commit_ts/subtrans/serial/notify; DBASE FILE_COPY (refused); GENERIC/LOGICALMSG; reclamation beyond existing GC; PG-6.

---

## File Structure

- **`crates/page-store/src/{name.rs, …}`** — `Key::{Rel, Slru, RelMeta}` + ordered encoding (Task 1).
- **`crates/postgres-redo/src/{meta.rs, rm_slru.rs, rm_relmeta.rs, rm_brin.rs, rm_hash.rs, rm_gist.rs, rm_spgist.rs, rm_gin.rs}`** (Tasks 2–3, 6–7).
- **`crates/pageserver/src/{live_ingest.rs, service.rs, basebackup.rs}`** — wire `shard_meta`, exact `GetRelSize`, SLRU rendering (Tasks 4–5).
- **`tools/gen-pg-wal-fixtures.sh`** — index/multixact/lifecycle workloads (Task 6 step 1, shared by 6–8).

**Batching:** Task 1 first (types). Task 2 (`meta.rs`) ∥ Task 6-step-1 (fixtures). Task 3 (SLRU/RelMeta arms) after 2. Tasks 4–5 (pageserver wiring) after 3. Tasks 6–7 (index families) after the fixtures, independent of 4–5. Task 8 last.

---

## Task 1: The `Key` enum (the PG-3 amendment)

**Files:**
- Modify: `crates/page-store/src/name.rs` (+ every `PageKey` use site)

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn key_order_is_total_and_encoding_sorts_identically() {
        let ks = vec![
            Key::Rel { rel: rel_tag(1663, 5, 16384, 0), blk: 0 },
            Key::Rel { rel: rel_tag(1663, 5, 16384, 0), blk: 7 },
            Key::RelMeta { rel: rel_tag(1663, 5, 16384, 0) },
            Key::Slru { kind: SlruKind::Clog, segno: 0, blk: 3 },
            Key::Slru { kind: SlruKind::MultiXactOff, segno: 1, blk: 0 },
        ];
        let mut sorted = ks.clone(); sorted.sort();
        let mut by_enc = ks.clone(); by_enc.sort_by_key(Key::encode);
        assert!(sorted == by_enc);                       // encoding order == key order
        for k in &ks { assert!(Key::parse(&k.encode()).unwrap() == *k); } // round-trip
    }
```

- [ ] **Step 2: Implement** — a tag byte (`0=Rel, 1=Slru, 2=RelMeta`) + fixed-width big-endian fields; rename `PageKey` → `Key::Rel` across `page-store` (mechanical; greenfield, no shims); PG-2's `shard_record` output maps to `Key::Rel`.
- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store` → PASS (all PG-3/4 tests under the new type).

```bash
git add crates/page-store crates/pageserver crates/postgres-redo
git commit -m "feat(page-store): tagged Key enum (Rel/Slru/RelMeta) — the PG-4b key-space amendment"
```

---

## Task 2: `shard_meta` — the light interpreter

**Files:**
- Create: `crates/postgres-redo/src/meta.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn commit_with_subxids_fans_out_to_every_touched_clog_page() {
        // xid 100 with subxids [101, 32768*4+5]: two distinct clog pages
        // (CLOG holds 32768 xids/page: 8192 bytes * 4 xids/byte).
        let rec = xact_commit_record(100, &[101, 131_077], &[]);
        let keys: Vec<_> = shard_meta(&rec).unwrap().into_iter().map(|s| s.key).collect();
        let pages: std::collections::BTreeSet<_> = keys.iter().filter_map(slru_page_of).collect();
        assert!(pages.len() == 2);
    }

    proptest! {
        #[test]
        fn fanout_covers_exactly_the_touched_pages(xid in 3u32..u32::MAX/2, subs in proptest::collection::vec(3u32..u32::MAX/2, 0..40)) {
            let rec = xact_commit_record(xid, &subs, &[]);
            let want: std::collections::BTreeSet<u32> =
                std::iter::once(xid).chain(subs.iter().copied()).map(|x| x / 32_768).collect();
            let got: std::collections::BTreeSet<u32> = shard_meta(&rec).unwrap()
                .iter().filter_map(|s| clog_pageno(&s.key)).collect();
            prop_assert!(got == want);
        }
    }

    #[test]
    fn commit_carried_drops_route_to_relmeta() {
        let rec = xact_commit_record(7, &[], &[rel_tag(1663, 5, 16400, 0)]);
        assert!(shard_meta(&rec).unwrap().iter().any(|s|
            matches!(s.key, Key::RelMeta { rel } if rel.rel_number == 16400)));
    }

    #[test]
    fn dbase_file_copy_is_refused() {
        let_assert!(Err(RedoError::UnsupportedRecord { .. }) = shard_meta(&dbase_filecopy_record()));
    }
```

- [ ] **Step 2: Implement** — parse XACT commit/abort `main_data` per PG 17's `xact.h` (`xinfo` flag-gated optional sections: consume subxids + relfilelocator drops, **length-skip** invalidations/origin/GID); CLOG `ZEROPAGE`/`TRUNCATE`; MULTIXACT `ZERO_OFF_PAGE`/`ZERO_MEM_PAGE`/`CREATE_ID` (offsets + members page arithmetic); SMGR `CREATE`/`TRUNCATE` → `RelMeta`; DBASE `WAL_LOG`-strategy records pass through (their page writes arrive as ordinary block refs), `FILE_COPY` → `UnsupportedRecord`. Output: `Vec<MetaSharded { key: Key, rec: Arc<DecodedRecord> }>`; unrecognized meta rmgrs → empty (retained-uninterpreted, unchanged).
- [ ] **Step 3: Verify + commit**

```bash
git add crates/postgres-redo/src/meta.rs crates/postgres-redo/src/lib.rs
git commit -m "feat(postgres-redo): shard_meta — clog/multixact/lifecycle key fan-out"
```

---

## Task 3: SLRU + RelMeta redo arms

**Files:**
- Create: `crates/postgres-redo/src/rm_slru.rs`, `src/rm_relmeta.rs`

- [ ] **Step 1: Write the failing tests** — clog folding: commit sets `01`, abort `10`, subcommitted handling, at the exact 2-bit offset for the xid (unit-computed positions); a zeropage arm yields an all-zero 8 KB page; multixact offsets/members writes at computed offsets; RelMeta folding: create(0) → truncate(n) → drop sequence produces `{nblocks, dropped_at}` values at the right LSNs.
- [ ] **Step 2: Implement** — SLRU arms fold `shard_meta`-routed records into 8 KB pages (`will_init` semantics for zeropage); `RelMeta` "pages" are a fixed little-endian struct `{nblocks: u32, flags: u32, dropped_at: u64}` treated as an ordinary `Value::Image` so `get_reconstruct_data` versioning applies unchanged.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/postgres-redo/src
git commit -m "feat(postgres-redo): clog/multixact and RelMeta redo arms"
```

---

## Task 4: Pageserver wiring — ingest + exact `GetRelSize`

**Files:**
- Modify: `crates/pageserver/src/{live_ingest.rs, service.rs}`

- [ ] **Step 1: Write the failing tests** — ingest a fixture with commits + a `TRUNCATE` + a `DROP TABLE`: `GetRelSize(lsn)` is exact (`exact == true`) and steps down at the truncate LSN; `GetPage` beyond `nblocks` → `BlockBeyondEof`; at ≥ drop LSN → `NotFound`; **below** those LSNs the old tail still serves (versioned reads). Seeded relations report exact sizes at LSN₀ (seed writes `RelMeta` from file sizes — extend Task-1-touched `seed.rs`).
- [ ] **Step 2: Implement** — ingest routes `Sharded::Meta` through `shard_meta` into ordinary `Ingest::put`s; `GetRelSize` = `get_reconstruct_data(Key::RelMeta…)` ⊕ the RelMeta arm, falling back to max-blkno only for pre-projection history (none, once seeding writes it); `GetPage` consults the projection for the bounds/drop checks.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src
git commit -m "feat(pageserver): exact GetRelSize + lifecycle-aware page serving"
```

---

## Task 5: Basebackup SLRU segments

**Files:**
- Modify: `crates/pageserver/src/basebackup.rs`

- [ ] **Step 1: Write the failing test** — un-`#[ignore]` PG-5a's SLRU assertions: the tarball's `pg_xact/0000` (and multixact files) at the capture LSN are **byte-identical** to the standby's (extend the fixture capture to copy the standby's `pg_xact`/`pg_multixact` alongside the relation files).
- [ ] **Step 2: Implement** — enumerate `Key::Slru` entries ≤ LSN per kind, materialize each page via redo, assemble 32-page (256 KiB) segment files with zeroed untouched pages, correct hex segment naming.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src/basebackup.rs tools/gen-pg-wal-fixtures.sh crates/postgres-wal/tests/fixtures
git commit -m "feat(pageserver): SLRU segment rendering in basebackup (standby byte-validated)"
```

---

## Task 6: Index arm families I — BRIN + hash

**Files:**
- Modify: `tools/gen-pg-wal-fixtures.sh`; Create: `crates/postgres-redo/src/{rm_brin.rs, rm_hash.rs}`

- [ ] **Step 1: Extend the fixture generator** (one regeneration for Tasks 6–8): per index family, `CREATE INDEX USING <am>` + inserts/updates/deletes/`VACUUM`; plus multixact traffic (two sessions, `SELECT … FOR SHARE`), `TRUNCATE`, `DROP TABLE`, `CREATE DATABASE wal_log_db` — capture the new relations + SLRU files in the standby snapshot. Regenerate + commit the corpus.
- [ ] **Step 2: BRIN arms** (insert/samepage-update/revmap-extend/desummarize) + **hash arms** (insert/split-allocate/squeeze/move-page-contents…) per PG 17's `brin_xlog.h`/`hash_xlog.h`, each with per-arm unit tests + the family's standby differential over its index relation pages.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/postgres-redo/src tools/gen-pg-wal-fixtures.sh crates/postgres-wal/tests/fixtures
git commit -m "feat(postgres-redo): BRIN and hash redo arms (fixture-gated)"
```

---

## Task 7: Index arm families II — GiST, SP-GiST, GIN

**Files:**
- Create: `crates/postgres-redo/src/{rm_gist.rs, rm_spgist.rs, rm_gin.rs}`

- [ ] **Step 1–3:** Same per-family discipline, in order (GiST: page-update/split/delete; SP-GiST: add-leaf/move-leafs/add-node/split-tuple/vacuum; GIN last: insert/split/vacuum-page/update-metapage/insert-listpage/delete-listpage — posting trees + pending lists, the densest fixtures). Each family: arms + unit tests + its standby differential green before its own commit (three commits).

```bash
git commit -m "feat(postgres-redo): GiST redo arms (fixture-gated)"   # then spgist, then gin
```

---

## Task 8: The extended gate + final gate

- [ ] **Step 1:** The full standby gate now covers: all PG-4 relations, all five index families' relations, **and** `pg_xact`/`pg_multixact` segment bytes — one test, everything byte-exact at the capture LSN.
- [ ] **Step 2:** Note in PG-5's plan that Task 7 (the boot gate) is unblocked; un-`#[ignore]` anything remaining.
- [ ] **Step 3:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-postgres-redo -p crabka-page-store -p crabka-pageserver --all-targets -- -D warnings`; `cargo nextest run -p crabka-postgres-redo -p crabka-page-store -p crabka-pageserver`; `./tools/check-publish-allowlist.sh` — all green. Commit.

---

## Self-Review

**1. Spec coverage:** the `Key` amendment (Task 1); ingest-light interpretation with subxid fan-out + commit-carried drops + DBASE policy (Task 2); redo-full SLRU/RelMeta arms (Task 3); exact `GetRelSize` + `BlockBeyondEof`/`NotFound` + versioned lifecycle (Task 4); basebackup SLRU segments, standby-byte-validated (Task 5); the five families in order, fixture-gated (Tasks 6–7); the extended gate + PG-5 unblock (Task 8). Deferred set untouched — Scope boundary. ✅

**2. Placeholder scan:** clog arithmetic (32768 xids/page, 2 bits/xid), the fan-out property test, the RelMeta value struct, and segment geometry (32 pages) are concrete; arm families follow PG-4's established specified-by-layout-plus-differential pattern with their per-family gates. No `TBD`.

**3. Type consistency:** `Key::{Rel, Slru, RelMeta}` (Task 1) is what `shard_meta` emits (Task 2), the arms fold (Task 3), the pageserver reads (Tasks 4–5), and the gate compares (Task 8); `MetaSharded{key, rec: Arc<DecodedRecord>}` mirrors PG-2's `Sharded` shape; `RedoError::{UnsupportedRecord, BlockBeyondEof}` extend PG-4's enum.

**4. Invariant check:** one store (everything through `Ingest::put`/`get_reconstruct_data`); ingest-light/redo-full (Tasks 2 vs 3); versioned lifecycle (Task 4's below-LSN assertions); per-family shipping (each family's own commit gated on its differential); SLRU byte-exactness unmasked (Tasks 5, 8). Each task green before commit.

**5. Prerequisites flagged:** PG-2/3/4 executed (header), with the fold-in note if PG-3 hasn't started; one fixture regeneration (local PG 17) shared across Tasks 6–8. Batching: 1 → (2 ∥ fixtures) → 3 → (4 ∥ 5 after 3) → 6 → 7 → 8.
