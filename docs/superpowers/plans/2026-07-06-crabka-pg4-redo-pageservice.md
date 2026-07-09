# PG-4: Native redo, `get_page@LSN`, and the page service — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pure native-Rust redo engine (`crabka-postgres-redo`, v1 rmgrs: XLOG-FPI/HEAP/HEAP2/BTREE/SEQ, loud `UnsupportedRmgr`), `get_page@LSN` + materializing compaction + horizon GC in `crabka-page-store` behind a `Redo` trait, and a Connect-RPC page service (`crabka-pageserver`) — gated by byte-exact comparison against a WAL-replayed stock-Postgres standby.

**Architecture:** `postgres-redo` dispatches per `(rmid, info)` over PG-2's decoded envelope, panic-free; `page-store` composes `get_reconstruct_data ⊕ Redo` into pages and drives image creation/GC through the trait; `pageserver` wires them behind unary `GetPage`/`GetRelSize` on the gateway's connectrpc-axum idiom. The oracle is redo-vs-redo: our pages vs a standby's files at the same LSN.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `bytes`/`thiserror` (redo: no tokio), `tokio` + `connectrpc-axum` + `prost` (service), `proptest` + `cargo-fuzz`-style harness, `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg4-redo-pageservice-design.md`](../specs/2026-07-06-crabka-pg4-redo-pageservice-design.md).

**PREREQUISITES (unlanded):** **PG-2** (`crabka-postgres-wal` + corpus) and **PG-3** (`crabka-page-store`). A local Postgres 17 once, to extend the fixture corpus with the standby capture.

---

## Invariants

1. **Byte-exact redo:** a materialized page equals the standby's page byte-for-byte, per covered rmgr, before that rmgr counts as shipped.
2. **Loud refusal:** every unimplemented `(rmid, info)` arm is `RedoError::UnsupportedRmgr { rmid, info, lsn }` — never a silent skip.
3. **Panic-free:** decode→apply never panics on any input (fuzzed); all page arithmetic bounds-checked → `RedoError::BadRecord`.
4. **`pd_lsn` discipline:** after applying a record, the page's `pd_lsn` = that record's end LSN.
5. **Reads unchanged by materialization:** image creation and GC never change any `get_page(key, lsn ≥ gc_horizon)` result.
6. **Clean seams:** `page-store` never depends on `postgres-redo` (only on `trait Redo`).
7. **New-crate hygiene:** both crates `publish = false` + release-plz entries; every task green before its commit.

## Scope boundary

- **In scope:** the v1 rmgr arms; `get_page`; the standby capture + gate; materializing compaction + horizon GC; the Connect service (`GetPage`, `GetRelSize`-as-hint); fuzz/property coverage.
- **Deferred:** GIN/GiST/SP-GiST/BRIN/hash + SLRU/CLOG + SMGR-record interpretation (PG-4b); basebackup/streaming RPCs/page cache (PG-5); branching (PG-6); live ingest (PG-1).

## Strict-audit status

- **Implemented in-repo exact arms:** XLOG/any supported-rmgr full-page images, empty HEAP init-to-zero-page, HEAP2 visible (`PD_ALL_VISIBLE`), and SEQ records only when the decoded block payload is exactly one full page.
- **Decoder-model blocked arms:** HEAP tuple deltas, HEAP2 non-visible deltas, BTREE tuple/split/delete deltas, unsupported SEQ opcodes, long-tail index deltas, and unsupported XACT opcodes remain explicit `UnsupportedRedoFamily` errors unless the decoder exposes family-specific tuple/item/page-opaque/status fields and a real PG17 standby oracle corpus proves byte identity. `XLOG_SEQ_LOG` records whose decoded payload is not exactly one page are malformed and fail as `BadRecord`, not as blockers. Supported metadata arms (SLRU zero/truncate, CLOG status folding, and exact relmap payload materialization) are tracked as implemented, not as broad blocker families, in `crates/postgres-redo/tests/fixtures/decoder_model_blockers.toml`.

---

## File Structure

- **`crates/postgres-redo/`** (new `crabka-postgres-redo`): `src/lib.rs`, `src/page.rs` (page layout + checked helpers), `src/dispatch.rs`, `src/rm_xlog.rs`, `src/rm_heap.rs`, `src/rm_btree.rs`, `src/rm_seq.rs`, `src/consts_v17.rs`, `fuzz/` harness.
- **`crates/page-store/`** (extend): `src/redo_seam.rs` (`trait Redo`, `get_page`), `src/materialize.rs` (image creation + GC).
- **`crates/pageserver/`** (new `crabka-pageserver`): `proto/crabka/pageserver/v1/pageserver.proto`, `build.rs` (gateway idiom), `src/lib.rs`, `src/service.rs`, `tests/serve.rs`.
- **`tools/gen-pg-wal-fixtures.sh`** (extend): standby capture + manifest.
- **`release-plz.toml`** — two private entries.

**Batching:** Task 1 (redo scaffold) ∥ Task 5a (`page-store` trait seam — disjoint crate). Tasks 2–3 sequential within `postgres-redo`. Task 4 (fixtures+gate) after 2–3. Task 5b (materialize) after 5a+Task 1 types. Task 6 (service) after 4–5. Task 7 last.

---

## Task 1: `crabka-postgres-redo` scaffold — dispatch, page module, the FPI arm

**Files:**
- Create: `crates/postgres-redo/{Cargo.toml, src/lib.rs, src/page.rs, src/dispatch.rs, src/rm_xlog.rs, src/consts_v17.rs}`
- Modify: `release-plz.toml`

- [ ] **Step 1: Write the failing tests**

```rust
    #[test]
    fn fpi_record_applies_as_the_image() {
        // From the PG-2 corpus: any FPI-bearing block ref (image already hole-reconstructed).
        let (lsn, rec, blk_idx) = fixture_fpi_record();
        let page = apply(None, &[(lsn, &rec, blk_idx)]).unwrap();
        assert!(page.as_ref() == rec.blocks[blk_idx].image.as_ref().unwrap().as_ref());
        assert!(page_lsn(&page) == rec.end_lsn); // pd_lsn discipline
    }

    #[test]
    fn unsupported_rmgr_refuses_loudly() {
        let rec = record_with_rmid(consts_v17::RM_GIN_ID);
        let_assert!(Err(RedoError::UnsupportedRmgr { rmid, .. }) = apply(None, &[(Lsn(1), &rec, 0)]));
        assert!(rmid == consts_v17::RM_GIN_ID);
    }

    #[test]
    fn non_init_chain_without_base_is_base_missing() {
        let rec = heap_insert_record(); // no BKPBLOCK_WILL_INIT
        let_assert!(Err(RedoError::BaseMissing { .. }) = apply(None, &[(Lsn(1), &rec, 0)]));
    }

    // page.rs property tests
    proptest! {
        #[test]
        fn add_item_never_exceeds_blcksz(off in 1u16..300, len in 1usize..2048) {
            let mut p = PageBuf::init(0);
            let _ = p.add_item(off, &vec![0u8; len]); // Ok or BadRecord — never panic, never OOB
            prop_assert!(p.lower() <= p.upper() && p.upper() as usize <= 8192);
        }
    }
```

- [ ] **Step 2: Implement**

`page.rs`: `PageBuf([u8; 8192])` with the PG page layout — header `pd_lsn:u64, pd_checksum:u16, pd_flags:u16, pd_lower:u16, pd_upper:u16, pd_special:u16, pd_pagesize_version:u16, pd_prune_xid:u32` (24 bytes, LE); line pointers (`u32`: `lp_off:15, lp_flags:2, lp_len:15`) growing up from `pd_lower`, items growing down from `pd_upper`; checked `init(special_size)`, `add_item(offnum, data)`, `item(offnum)`, `set_lsn` — every mutation bounds-checked → `RedoError::BadRecord`. `dispatch.rs`: `apply(base, records) -> Result<PageImage, RedoError>` folding records through `match (rmid, info & !XLR_INFO_MASK)`; `will_init` records start from `PageBuf::zeroed`. `rm_xlog.rs`: FPI/`FPI_FOR_HINT` = replace with the block ref's image. `consts_v17.rs`: rmgr ids + opcode masks (`XLR_INFO_MASK = 0x0F`, `XLOG_HEAP_OPMASK = 0x70`, `XLOG_HEAP_INIT_PAGE = 0x80`), values taken from PG 17 headers and **self-corrected against fixture decode on first run** (the PG-2 magic pattern). `Cargo.toml` `publish = false`; release-plz private entry.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-postgres-redo` → PASS; `./tools/check-publish-allowlist.sh` → exit 0.

```bash
git add crates/postgres-redo release-plz.toml
git commit -m "feat(postgres-redo): scaffold, checked page module, FPI arm, loud refusal"
```

---

## Task 2: HEAP + HEAP2 arms

**Files:**
- Create: `crates/postgres-redo/src/rm_heap.rs`

- [ ] **Step 1: Write the failing tests** — one per arm, each against a fixture-extracted record with a known before/after page (extract via a test helper that replays the corpus up to the record's LSN using FPI bases + already-shipped arms; where the corpus lacks isolation, craft the *before* page with `PageBuf` and take the *after* from the standby capture in Task 4 — mark those `#[ignore]` until Task 4 lands, then un-ignore):

`insert` (place tuple at `offnum`, header from `xl_heap_header {t_infomask2, t_infomask, t_hoff}`), `delete` (set `xmax`, infomask bits from `infobits_set`, clear HOT/moved bits), `update` + `hot_update` (old page: `xmax`+ctid; new page: insert), `lock`, `inplace`; HEAP2: `multi_insert` (N tuples, `XLH_INSERT_LAST_IN_MULTI` accounting), `prune` (redirect/dead/unused line-pointer arrays), `vacuum`, `visible` (PD_ALL_VISIBLE on the heap page; the vm-fork block ref sets the vm bits).

- [ ] **Step 2: Implement**

Arms keyed by `info & XLOG_HEAP_OPMASK` (+ `INIT_PAGE` handling via the zeroed base); record structs parsed from `main_data`/block data per PG 17's `heapam_xlog.h` layouts (spelled in code comments with field offsets; constants in `consts_v17.rs`). Every arm ends `page.set_lsn(end_lsn)`.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-postgres-redo rm_heap` → PASS (non-ignored set).

```bash
git add crates/postgres-redo/src
git commit -m "feat(postgres-redo): HEAP/HEAP2 redo arms"
```

---

## Task 3: BTREE + SEQ arms

**Files:**
- Create: `crates/postgres-redo/src/rm_btree.rs`, `src/rm_seq.rs`

- [ ] **Step 1: Tests** (same fixture-extraction pattern): btree `insert_leaf`, `insert_upper`, `split_l`/`split_r` (the dense ones: left-page truncation to `firstrightoff`, high-key install, right-page build from the record payload — the record carries the full new right page content), `dedup`, `vacuum`/`delete`; `seq_log` (the whole 1-tuple page is in the record — near-FPI).

- [ ] **Step 2: Implement** per PG 17 `nbtxlog.h`/`sequence.h` layouts; split arms manipulate both block refs of one record (dispatch feeds each block ref to its own page apply — the `blk_idx` parameter from Task 1).

- [ ] **Step 3: Verify + commit**

```bash
git add crates/postgres-redo/src
git commit -m "feat(postgres-redo): BTREE and SEQ redo arms"
```

---

## Task 4: Standby capture + the differential gate

**Files:**
- Modify: `tools/gen-pg-wal-fixtures.sh`
- Create: `crates/postgres-redo/tests/standby_gate.rs`; fixture additions under `crates/postgres-wal/tests/fixtures/standby/`

- [ ] **Step 1: Extend the generator** — after the traffic and before teardown: record `CAPTURE_LSN=$(psql … 'SELECT pg_current_wal_flush_lsn()')`; `pg_basebackup -D "$D/standby"`; write `standby.signal` + `recovery_target_lsn='$CAPTURE_LSN'`, `recovery_target_action='shutdown'`; start the standby and wait for exit; copy the **covered relations'** files (query `pg_relation_filepath` on the primary for the test table, its pkey index, its sequence — main + vm forks only; **fsm is not WAL-logged and is excluded**) into `fixtures/standby/`, plus `manifest.toml` (`capture_lsn`, per-relation `(reltag, fork, nblocks, path)`). Re-run once locally; commit the few-MB capture.

- [ ] **Step 2: Write the gate test** — ingest the corpus (PG-2 decoder → PG-3 ingest on `InMemory`); for every covered `(rel, fork)` and every `blk < nblocks`: `get_page(key, capture_lsn)` (Task 5's composition; until then, `get_reconstruct_data ⊕ apply` inline) **byte-equal** to the standby file's block. Un-ignore Task 2/3's capture-dependent tests. Any mismatch: fix the engine, never the oracle.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-postgres-redo --test standby_gate` → PASS.

```bash
git add tools/gen-pg-wal-fixtures.sh crates/postgres-wal/tests/fixtures crates/postgres-redo/tests
git commit -m "test(postgres-redo): byte-exact standby differential gate"
```

---

## Task 5: `page-store` — `Redo` seam, `get_page`, materializing compaction, GC

**Files:**
- Create: `crates/page-store/src/redo_seam.rs`, `src/materialize.rs`

- [ ] **Step 1 (5a, parallel-safe with Task 1): the seam + `get_page`**

```rust
pub trait Redo: Send + Sync {
    fn apply(&self, base: Option<Bytes>, records: &[(Lsn, Bytes, usize)]) -> Result<Bytes, RedoSeamError>;
}
pub async fn get_page(map: &LayerMap, ops: &dyn ObjectOps, key: &PageKey, lsn: Lsn, redo: &dyn Redo) -> Result<Bytes, …>
```

TDD with a `NoopRedo` (image-base pass-through) so PG-3's tests keep passing; the FPI-base case returns without calling redo.

- [ ] **Step 2 (5b): materializing compaction + horizon GC**

Image creation: when a key's delta stack above its newest image exceeds `IMAGE_CREATE_THRESHOLD`, materialize (via `Redo`) every key in the range at the stack-top LSN into an image layer; register-then-deregister as in PG-3's compaction. GC: delete layers with `lsn_range.end < gc_horizon` whose key range is fully covered by a later image. **Tests:** a probe grid of `get_page(key, lsn ≥ horizon)` results is *identical* before/after image creation and after GC; GC refuses when coverage is incomplete.

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-page-store` → PASS (old + new).

```bash
git add crates/page-store/src
git commit -m "feat(page-store): Redo seam, get_page, image materialization + horizon GC"
```

---

## Task 6: `crabka-pageserver` — the Connect page service

**Files:**
- Create: `crates/pageserver/{Cargo.toml, build.rs, proto/crabka/pageserver/v1/pageserver.proto, src/lib.rs, src/service.rs, tests/serve.rs}`
- Modify: `release-plz.toml`

- [ ] **Step 1: The proto + build** (gateway idiom: `connectrpc_axum_build::compile_protos` + vendored protoc in `build.rs`):

```proto
service PageService {
  rpc GetPage(GetPageRequest) returns (GetPageResponse);
  rpc GetRelSize(GetRelSizeRequest) returns (GetRelSizeResponse);
}
message GetPageRequest { uint32 spc_oid = 1; uint32 db_oid = 2; uint32 rel_number = 3;
                         uint32 fork = 4; uint32 blkno = 5; uint64 lsn = 6; }
message GetPageResponse { bytes page = 1; }                       // exactly 8192
message GetRelSizeRequest { uint32 spc_oid = 1; uint32 db_oid = 2; uint32 rel_number = 3;
                            uint32 fork = 4; uint64 lsn = 5; }
message GetRelSizeResponse { uint32 nblocks = 1; bool exact = 2; } // v1: hint, exact=false
```

- [ ] **Step 2: Write the failing integration test** (`tests/serve.rs`): ingest the corpus on `InMemory`; serve the router in-process (`axum::serve` on an ephemeral port, the gateway test pattern); a Connect client `GetPage`s a covered page at `capture_lsn` → byte-equal to the standby file; an unknown relation → NotFound; a GIN-ish key (synthetic unsupported delta) → the explicit unsupported-rmgr error code surfaced.

- [ ] **Step 3: Implement** — `service.rs` holds `Arc<LayerMap>/ops/Arc<dyn Redo>` (wired to `crabka-postgres-redo`), handlers call `get_page`/the rel-size hint; `.build_connect()`. `publish = false` + release-plz entry.

- [ ] **Step 4: Verify + commit**

Run: `cargo test -p crabka-pageserver` → PASS; allowlist → exit 0.

```bash
git add crates/pageserver release-plz.toml
git commit -m "feat(pageserver): Connect page service (GetPage, GetRelSize hint)"
```

---

## Task 7: Fuzz harness + final gate

- [ ] **Step 1:** A fuzz/property harness in `postgres-redo` (`proptest`-driven arbitrary-bytes → decode → `apply`; assert no panic, only `Ok`/`RedoError`). Run a bounded corpus in CI (`proptest` cases), full fuzzing locally.
- [ ] **Step 2:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-postgres-redo -p crabka-page-store -p crabka-pageserver --all-targets -- -D warnings`; `cargo nextest run -p crabka-postgres-redo -p crabka-page-store -p crabka-pageserver`; `./tools/check-publish-allowlist.sh` — all green.
- [ ] **Step 3:** Commit.

```bash
git add -A
git commit -m "test(postgres-redo): panic-freedom fuzz harness; final PG-4 gate"
```

---

## Self-Review

**1. Spec coverage:** dispatch + loud refusal + FPI + checked page module (Task 1); HEAP/HEAP2 (Task 2); BTREE/SEQ (Task 3); the standby oracle + byte-exact gate + fsm exclusion (Task 4); `Redo` seam, `get_page`, image creation + horizon GC with read-equivalence (Task 5); the Connect service on the gateway idiom + error surfacing (Task 6); panic-freedom fuzzing (Task 7). Deferred set (PG-4b rmgrs/SLRU/SMGR, basebackup/cache, branching) untouched — Scope boundary. ✅

**2. Placeholder scan:** page-layout offsets, opcode masks, and per-arm semantics are stated with the self-correcting-constants pattern (verified against fixtures, like PG-2's magic); full test code for the decisive behaviors; large rmgr arm bodies are specified by layout + semantics + their per-arm differential tests rather than inlined page-length C transliterations — the standby gate is the arbiter. No `TBD`.

**3. Type consistency:** `apply(base, records) -> Result<PageImage, RedoError>` (Task 1) is the `trait Redo` shape (Task 5) the service wires (Task 6); `RedoError::{UnsupportedRmgr, BadRecord, BaseMissing}` used across Tasks 1–6; `get_page` composes PG-3's `ReconstructData` exactly (base `Option` + oldest-first deltas); the proto's `(spc,db,rel,fork,blkno)` mirrors `RelTag`/`PageKey`.

**4. Invariant check:** byte-exact per rmgr (Task 4 gate + per-arm tests); loud refusal (Task 1 + Task 6 surfacing); panic-free (Task 1 properties + Task 7 fuzz); `pd_lsn` discipline (Task 1 test); materialization read-equivalence (Task 5); seam cleanliness (`page-store` depends only on the trait); allowlist (Tasks 1, 6, 7). Each task green before commit.

**5. Prerequisites flagged:** PG-2 + PG-3 unlanded (header); local PG 17 once for the standby capture. Batching: 1 ∥ 5a → 2 → 3 → 4 → 5b → 6 → 7.
