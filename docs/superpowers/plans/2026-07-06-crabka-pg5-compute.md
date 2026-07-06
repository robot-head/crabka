# PG-5: Compute integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Boot a real Postgres 17 against the disaggregated stack. **PG-5a:** pageserver readiness — timeline seeding from an `initdb` dir, live topic ingest, LSN-wait on `GetPage`, a `Basebackup` RPC. **PG-5b:** the compute image — a minimal vendored smgr-hook patch over pinned PG-17 sources, a C extension whose smgr calls `crabka-compute-client` (a Rust cdylib behind a C ABI — the workspace's one sanctioned `unsafe` boundary). Gate: pgbench end-to-end (Tier 1, blocked on PG-4b) + page-image differential vs stock PG (Tier 2).

**Architecture:** 5a extends `crates/pageserver` in pure Rust; 5b adds `crates/compute-client` (cdylib) and a non-workspace `compute/` tree (patches, extension C, image build). Compute is a stock-shaped primary; PG-1's safekeeper attaches unchanged, closing the WAL loop.

**Tech Stack:** Rust 2024 (pinned stable 1.96.0), `prost` + a blocking HTTP client (no tokio in the cdylib), `cbindgen`, `tar`, `tokio` (pageserver), C + libpq-less extension against patched PG-17 headers, `testcontainers` (+ `postgres` module), `assert2`/`nextest`, `cargo +nightly fmt`, `clippy::pedantic`.

**Spec:** [`docs/superpowers/specs/2026-07-06-crabka-pg5-compute-design.md`](../specs/2026-07-06-crabka-pg5-compute-design.md).

**PREREQUISITES (unlanded):** PG-2, PG-3, PG-4 crates (5a builds on them); PG-1 (the WAL loop for the gate); **PG-4b blocks Task 7's boot gate only** (SLRUs in basebackup). A local/containerized PG 17 for fixtures.

---

## Invariants

1. **Seeded fidelity:** an imported relation file re-served page-by-page @ LSN₀ is byte-identical to the source file.
2. **Reads never race ingest:** `GetPage(lsn)` blocks until `last_ingested_lsn ≥ lsn` (bounded timeout → clear error), never serves stale-behind-request pages.
3. **The `unsafe` boundary is structural:** only `crates/compute-client/src/ffi.rs` contains `unsafe`; every block has `// SAFETY:`; `unsafe_op_in_unsafe_fn = deny`; the C header is cbindgen-generated; the style guide documents the exception.
4. **No forked repo:** PG-17 sources are pinned + vendored patch files; a CI check re-applies patches against the pin.
5. **Compute is disposable:** durable truth = the topic + the bucket; re-basebackup resumes without acked-transaction loss.
6. **New-crate hygiene:** `publish = false` + release-plz entries; every task ends green before its commit.

## Scope boundary

- **In scope (5a):** `seed_timeline`, live ingest, LSN-wait, `Basebackup` (SLRU content wired but gated on PG-4b); **(5b):** the cdylib client + FFI, the smgr-hook patch, the extension, the image, both gate tiers.
- **Deferred:** read replicas / multi-compute (per-page last-written LSN), pooling/suspend, batched-streamed GetPage, page-service TLS/auth, PG-6 branching UX.

---

## File Structure

- **`crates/pageserver/src/{seed.rs, live_ingest.rs, basebackup.rs}`** + LSN-wait in `service.rs`; proto gains `Basebackup`.
- **`crates/compute-client/`** (new, cdylib+rlib): `src/{lib.rs, client.rs, ffi.rs}`, `build.rs` (compiles the pageserver proto + runs cbindgen → `include/crabka_compute.h`), own `[lints]` table (the opt-out).
- **`compute/`** (new, non-workspace): `patches/pg17/0001-smgr-hook.patch`, `extension/{crabka.c, Makefile}`, `image/build.sh` (packaging idiom).
- **`docs/style_guides/code_style_guide.md`** — the exception paragraph.
- **`release-plz.toml`** — the `crabka-compute-client` private entry.

**Batching:** Tasks 1–4 (5a) are intra-`pageserver` — 1 → (2 ∥ 3) → 4. Task 5 (cdylib) is parallel with all of 5a. Task 6 (patch/extension/image) after 5. Task 7 (Tier 1, **PG-4b-gated**) and Task 8 (Tier 2 + final gate) last.

---

## Task 1 (5a): Timeline seeding

**Files:**
- Create: `crates/pageserver/src/seed.rs`

- [ ] **Step 1: Write the failing test** (integration profile; obtain an `initdb` data dir from a `postgres:17` container — run `initdb` to a bind-mounted temp dir, the workspace testcontainers pattern):

```rust
    #[tokio::test]
    async fn seeded_relation_pages_reserve_byte_identically() {
        let datadir = initdb_fixture().await;                    // container-produced, cached per run
        let (map, ops) = seed_timeline(&datadir, tl(), LSN0).await.unwrap();
        // Pick 2 relation files (a catalog table + its index) from base/<db>/:
        for rel in sample_relations(&datadir) {
            let file = std::fs::read(&rel.path).unwrap();
            for blk in 0..(file.len() / 8192) {
                let page = get_page(&map, &ops, &rel.key(blk as u32), LSN0, &NoopRedo).await.unwrap();
                assert!(page.as_ref() == &file[blk * 8192..(blk + 1) * 8192]);
            }
        }
        // Non-relation files (global/pg_control, pg_xact/, pg_multixact/, PG_VERSION) retained:
        assert!(nonrel_store_contains(&ops, "global/pg_control").await);
    }
```

- [ ] **Step 2: Implement** — walk `base/*/` + `global/` relation files (filenode naming → `RelTag`, fork suffixes `_fsm`/`_vm`; **fsm skipped** — not WAL-logged, PG-3 precedent), emit each 8 KB block as a `Value::Image` entry @ `LSN0` through PG-3's ingest + flush into **image layers** (the PG-4 writer); copy non-relation files (`pg_control`, `pg_xact/*`, `pg_multixact/*`, `pg_filenode.map`, `PG_VERSION`) into a `pg/<tenant>/<timeline>/nonrel/` prefix.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src/seed.rs crates/pageserver/src/lib.rs crates/pageserver/tests
git commit -m "feat(pageserver): timeline seeding from an initdb data directory"
```

---

## Task 2 (5a, ∥ Task 3): Live topic ingest

**Files:**
- Create: `crates/pageserver/src/live_ingest.rs`

- [ ] **Step 1: Write the failing test** — in-process broker; produce the PG-2 fixture corpus as `PGW1` frames (the PG-1 frame codec's layout, re-stated locally in the test) onto `__pg_wal.test`; run `spawn_live_ingest(consumer, ingest)`; assert `last_ingested_lsn` reaches the corpus end and a probe `get_page` equals the PG-3 direct-ingest result for the same corpus.
- [ ] **Step 2: Implement** — a consumer loop: poll → decode each record's `PGW1` frame → `WalStreamDecoder::feed` → `shard_record` → PG-3 `Ingest::put`; flush on size/time; publish `last_ingested_lsn` via `watch::Sender<Lsn>`. Frame contiguity re-checked (a gap halts ingest with the offending offsets).
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src/live_ingest.rs crates/pageserver/src/lib.rs
git commit -m "feat(pageserver): live WAL-topic ingest with contiguity checks"
```

---

## Task 3 (5a, ∥ Task 2): LSN-wait on the page service

**Files:**
- Modify: `crates/pageserver/src/service.rs`

- [ ] **Step 1: Write the failing test** — with `last_ingested_lsn = 100`: `GetPage(lsn=200)` blocks; advancing the watch to 200 releases it with the right page; a request past `wait_timeout` returns a `deadline_exceeded`-mapped error naming both LSNs.
- [ ] **Step 2: Implement** — `wait_for_lsn(watch, lsn, timeout)` before materialization in `GetPage`/`GetRelSize`; timeout configurable.
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src/service.rs
git commit -m "feat(pageserver): bounded LSN-wait before page materialization"
```

---

## Task 4 (5a): The `Basebackup` RPC

**Files:**
- Create: `crates/pageserver/src/basebackup.rs`; Modify: `proto/…/pageserver.proto` (+ `rpc Basebackup(BasebackupRequest) returns (BasebackupResponse)` — `bytes tar = 1` v1)

- [ ] **Step 1: Write the failing test** — request a basebackup at `capture_lsn`; untar; assert: `PG_VERSION` + seeded non-rel files present; `global/pg_control` **validates under `pg_controldata`** (run in the PG-17 container — the oracle) with its checkpoint/redo fields at `capture_lsn`; SLRU dirs present (content asserted only once PG-4b lands — the assertion is written now and `#[ignore]`d with a PG-4b reference).
- [ ] **Step 2: Implement** — tar the nonrel prefix; **patch the seeded `pg_control` template**: overwrite the checkpoint LSN/redo/time fields at PG-17's `pg_control.h` offsets and recompute its trailing CRC-32C (the `pg_controldata` oracle self-corrects any offset error). No relation files in the tarball (smgr serves them).
- [ ] **Step 3: Verify + commit**

```bash
git add crates/pageserver/src/basebackup.rs crates/pageserver/proto crates/pageserver/tests
git commit -m "feat(pageserver): Basebackup RPC (pg_control patching, pg_controldata-validated)"
```

---

## Task 5 (5b, ∥ Tasks 1–4): `crabka-compute-client` — the cdylib + the one unsafe boundary

**Files:**
- Create: `crates/compute-client/{Cargo.toml, build.rs, src/lib.rs, src/client.rs, src/ffi.rs, cbindgen.toml}`
- Modify: `release-plz.toml`, `docs/style_guides/code_style_guide.md`

- [ ] **Step 1: Write the failing tests**

```rust
// client.rs: safe, blocking, mock-server-tested
    #[test]
    fn get_page_round_trips_against_a_mock_pageserver() {
        let srv = mock_connect_unary("/crabka.pageserver.v1.PageService/GetPage", valid_page_response());
        let c = ComputeClient::connect(&srv.url()).unwrap();
        let page = c.get_page(rel_probe(), 0, Lsn(100)).unwrap();
        assert!(page.len() == 8192);
    }
    #[test]
    fn pageserver_error_maps_to_typed_error() { /* unsupported-rmgr Connect error -> ClientError::Pageserver{code,msg} */ }
```

FFI: a build-time C harness (`cc` crate, dev-only) compiling a caller of `ck_connect`/`ck_get_page` against the cbindgen header, run as a test against the mock server; a CI drift check that `cbindgen` output matches the committed header.

- [ ] **Step 2: Implement**

`client.rs` (safe): blocking HTTP/1.1 Connect unary (persistent connection, prost bodies, bounded retries) over the **same proto** as the pageserver (`build.rs` compiles `../pageserver/proto/...`). `ffi.rs` (the boundary): `#[no_mangle] pub unsafe extern "C" fn ck_get_page(handle, spc, db, rel, fork, blkno, lsn, out_page: *mut u8) -> i32` etc. — pointer marshalling only, `// SAFETY:` on every block, errors as negative codes + `ck_last_error_message`. `Cargo.toml`: `crate-type = ["cdylib", "rlib"]`, `publish = false`, **its own `[lints]` table** (the workspace set minus `unsafe_code = "forbid"`, plus `unsafe_op_in_unsafe_fn = "deny"`); release-plz entry. Style guide: add the exception paragraph (one crate, one file, cbindgen, SAFETY comments).

- [ ] **Step 3: Verify + commit**

Run: `cargo test -p crabka-compute-client` → PASS; `./tools/check-publish-allowlist.sh` → 0.

```bash
git add crates/compute-client release-plz.toml docs/style_guides/code_style_guide.md
git commit -m "feat(compute-client): blocking Connect cdylib with the sanctioned FFI boundary"
```

---

## Task 6 (5b): The patch, the extension, the image

**Files:**
- Create: `compute/patches/pg17/0001-smgr-hook.patch`, `compute/extension/{crabka.c, Makefile}`, `compute/image/build.sh`, a patch-apply CI check

- [ ] **Step 1: The core patch** — against pinned `postgres-17.x` sources: add a registration hook (`typedef const f_smgr *(*smgr_hook_type)(...); extern PGDLLIMPORT smgr_hook_type smgr_hook;`) consulted in the smgr-open dispatch so an extension can substitute the relation smgr table for non-temp relations (~50-line diff, the Neon/TDE fork shape). Vendor the reviewed diff; add the CI check: fetch-pin → `git apply --check`.
- [ ] **Step 2: The extension** — `crabka.c`: `_PG_init` reads GUCs (`crabka.pageserver_endpoint`, `crabka.tenant/timeline`), `ck_connect`s, installs the hook; the `f_smgr` table: `read → ck_get_page(…, lsn = GetFlushRecPtr())`, `nblocks → ck_get_rel_size` (+ a per-relation size cache updated by `extend`/`truncate`), `write/extend → data no-op + cache`, `exists → ck_get_rel_size ≥ 0`, `unlink → no-op`. Client errors → `ereport(ERROR, …)` naming the pageserver cause. Builds via PGXS against the patched tree.
- [ ] **Step 3: The image** — `build.sh` (packaging idiom): fetch pinned sources → apply patches → build PG → build the cdylib (`cargo build -p crabka-compute-client --release`) → build the extension → assemble the OCI image (entrypoint: basebackup-fetch into `$PGDATA` if empty, then `postgres`). Verify: the image builds; `postgres --version` runs; the extension loads (`shared_preload_libraries=crabka` against a mock endpoint fails *gracefully* with the documented error).
- [ ] **Step 4: Commit**

```bash
git add compute .github/workflows
git commit -m "feat(compute): smgr-hook patch, crabka extension, compute image build"
```

---

## Task 7: The boot gate (Tier 1) — **BLOCKED ON PG-4b**

- [ ] **Step 1:** Compose the stack (containerized): seeded pageserver + broker + safekeeper + the compute image (basebackup boot). `pgbench -i` then `pgbench -T 30` runs green; kill compute mid-run; re-basebackup; resume; no acked-transaction loss (audit vs the topic).
- [ ] **Step 2:** Un-`#[ignore]` Task 4's SLRU assertions. Commit.

```bash
git add compute crates/pageserver
git commit -m "test(pg5): Tier-1 boot gate — pgbench end-to-end on the disaggregated stack"
```

---

## Task 8: Tier-2 fidelity + final gate

- [ ] **Step 1:** The deterministic workload replayed on stock PG 17; compare relation page images at matched LSNs (PG-4's standby-oracle comparator + masking, reused system-level).
- [ ] **Step 2:** `cargo +nightly fmt --check`; `cargo clippy -p crabka-pageserver -p crabka-compute-client --all-targets -- -D warnings`; `cargo nextest run -p crabka-pageserver -p crabka-compute-client`; `./tools/check-publish-allowlist.sh`; the patch-apply + cbindgen drift checks — all green. Commit.

---

## Self-Review

**1. Spec coverage:** seeding (Task 1); live ingest (Task 2); LSN-wait (Task 3); `Basebackup` + `pg_control` patching with the `pg_controldata` oracle (Task 4); the cdylib client + FFI containment + style-guide codification (Task 5); the patch/extension/image with the no-fork strategy + CI drift checks (Task 6); Tier-1 gate incl. disposability, explicitly PG-4b-blocked (Task 7); Tier-2 fidelity + hygiene (Task 8). Deferred set (replicas, pooling, streaming reads, TLS, PG-6) untouched — Scope boundary. ✅

**2. Placeholder scan:** the genuinely-authored-at-execution artifacts (the ~50-line patch, the extension C) are specified by exact shape (hook typedef, `f_smgr` table semantics, GUCs, error mapping) with behavior gates, not inlined C dumps; `pg_control` patching uses the self-correcting-oracle pattern; test bodies given for the decisive Rust behaviors. No `TBD`.

**3. Type consistency:** `seed_timeline`/`get_page`/`NoopRedo` (Task 1) are PG-3/4's real seams; `last_ingested_lsn: watch::Sender<Lsn>` (Task 2) is what Task 3 waits on; the proto shared by `build.rs` (Task 5) is Task 4's extended service; `ck_get_page`'s `(spc,db,rel,fork,blkno,lsn)` mirrors `GetPageRequest`.

**4. Invariant check:** seeded fidelity (Task 1 byte test); no read-ingest races (Task 3); the unsafe boundary structural (Task 5 lint table + drift checks + style guide); no forked repo (Task 6 + CI apply-check); disposability (Task 7); hygiene (Tasks 5, 8). Each task green before commit.

**5. Prerequisites flagged:** PG-2/3/4 (5a's substrate), PG-1 (the loop), PG-4b gating Task 7 only — all named in the header; 5a and Task 5 proceed without PG-4b. Batching: 1 → (2 ∥ 3) → 4, with Task 5 parallel throughout → 6 → 7 → 8.
