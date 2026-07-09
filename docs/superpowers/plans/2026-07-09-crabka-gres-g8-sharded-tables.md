# Chapter Gres G-8: Sharded Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One table spans ranges: global-visibility tables on the existing g-timeline, scatter-gather statement execution (0A000 lifted), interval rowid placement, and online checkpoint-fork splits — with the commit-rate ceiling measured against its stated envelope.

**Architecture:** G-8a makes sharded tables *semantically* distributed with zero new clocks: every write is g-stamped (leased g-blocks; batched range-0 decisions), every read runs under the G-7 gsnap+barrier, and a new `RangeScanner` seam in the executor scatter-gathers scans with owning-range visibility evaluation. G-8b makes the layout *dynamic*: versioned RangeMap v2 with `(table_id, rowid)` boundaries, splits as checkpoint forks with filtered restore and parked predecessor topics.

**Tech Stack:** everything G-7 built (transport, coordinator, models, harnesses), G-3 checkpoint machinery (filtered restore), G-5 parking, `crabka-pgparser` (`SHARDED`), stateright.

## Global Constraints

- **Prerequisites:** all of G-7 landed (G-8a); plus G-5's parking (G-8b splits park predecessor topics). This plan is the furthest from the tree — every task begins by re-verifying the seams it names; where this plan and the landed code disagree, the code wins and the plan step adapts, recording the divergence in the task's commit message.
- **Spec:** [2026-07-09-crabka-gres-g8-sharded-tables-design.md](../specs/2026-07-09-crabka-gres-g8-sharded-tables-design.md). The two spec invariants every task defends: **sharded-table visibility is gsnap-only** (no code path may consult a foreign range's local clog), and **fence-first ordering** extends to splits (both successor fences precede any read of the predecessor's end).
- **Correctness bar:** the corpus-through-sharding conformance gate (corpus tables `SHARDED` across 2 ranges must match the parity baseline) is the semantic backstop for everything; it runs from G-8a's first executable milestone onward.
- Lints/format/commit/test conventions as in the G-2 plan; donor test names kept where suites are re-targeted.

---

## Batch 1 — G-8a semantics (serial: Task 1 then Task 2 — both modify `crates/pgexec` and `crates/gres-ranges`, so they are NOT a disjoint parallel batch; panel amendment I8)

### Task 1: Global-visibility tables — catalog flag, write-path escalation, g-block leases

**Files:** Modify `crates/pgparser` (`SHARDED` in CREATE/ALTER TABLE grammar + AST), `crates/pgcatalog` (a `sharded: bool` on the table record — versioned encoding bump, greenfield rules), `crates/pgexec` (session write path: first write to a sharded table escalates the transaction to global exactly as a second-range touch does today; all its row batches carry `Prepared(Li→g)` via the existing `effective_global_xid` machinery), `crates/gres-ranges` (GTM g-block leasing: `lease_block(k) -> Range<u64>` — one range-0 append per block; allocation local thereafter; SP23 reseed lifts past every leased block, pinned by extending the ported GTM-reuse model with a `LeaseBlock` action).

Steps: parser TDD (parse/deparse golden + reject on unsupported positions); catalog round-trip; escalation TDD in-process (a single-range write to a sharded table produces a g-stamped batch and a range-0 decision; an unsharded table's path is byte-identical to before — pinned); lease TDD (blocks disjoint, reseed-past-leases model teeth). Commit `feat(gres): global-visibility tables and g-block leases`.

### Task 2: The `RangeScanner` seam + ScanRange RPC

**Files:** Modify `crates/pgexec/src/exec.rs` (`scan_live` routes through a `RangeScanner` trait; the default impl is the existing local scan — unsharded behavior byte-identical), `crates/gres-ranges` (the distributed impl: local-interval scan + `ScanRange { table, interval, gsnap, filter: Option<…> }` RPC on the transport; the owning range evaluates visibility under the caller's gsnap and returns rowid-ordered visible rows; gateway concatenates interval-ordered results), `transport/protocol.rs` (the new message pair).

Steps: TDD the seam (unsharded = old path, whole-struct scan-result equality); TDD remote visibility (hand-built histories: committed/aborted/in-doubt g's — remote scan returns exactly what a local scan of a co-located range returns under the same gsnap); lift `0A000` in `pinning_range` for statements whose every table is sharded/global-visibility (router change + the donor walker tests extended); filter pushdown basic case (predicate evaluated remotely when present, result equality vs unpushed). Commit `feat(gres): scatter-gather scans with owning-range visibility`.

---

## Batch 2 — G-8a proof (serial)

### Task 3: Visibility-equivalence property suite + corpus-through-sharding

**Files:** Create `crates/gres-ranges/tests/sharded_visibility.rs` (property test: random op histories against (a) a sharded 2-range table and (b) a single-range control; every read under every captured gsnap equal), re-run the ported bank/Elle suites with the workload tables `SHARDED`, and add the **corpus-through-sharding leg** to the gres-conformance CI job (provision a tenant whose corpus tables are sharded across 2 ranges; run the harness against the same `baseline.json`).

Steps: property suite first (it will find seam bugs the units missed — budget for iteration); then the suites; then the CI leg. Commit `test(gres): sharded-table visibility equivalence and corpus parity`.

### Task 4: The ceiling, measured

**Files:** Extend `scripts/gres-range-scaling.sh` with a sharded-table mode: single table across 1/2/4 ranges — measure ingest (must scale ~linearly) and aggregate commit rate as concurrency rises (must approach, and be reported against, the stated batched-decision envelope); publish both curves in the per-PR artifact.

Commit `ci: sharded-table ingest scaling and decision-ceiling measurement`.

---

## Batch 3 — G-8b splits (serial: Tasks 5 → 6 → 7)

### Task 5: RangeMap v2 — `(table_id, rowid)` boundaries, versioned and barrier-gated

**Files:** Modify `crates/gres-ranges/src/range/map.rs` (boundary type `(TableId, RowId)`; `range_for_key(table, rowid)`; descriptor format v2 — the donor reserved the room), `range/meta.rs` (the blob becomes versioned: monotonically numbered, written through range 0's Committer; every reader caches `(version, map)` and refreshes behind the Range0Barrier on fresh-snapshot statements — the upgrade the donor's own comment demanded), router + scanner interval logic.

Steps: TDD map v2 (lookup, encode/decode round-trip incl. v1-rejection — greenfield, no migration), TDD versioned reload (a map bump becomes visible to a barriered reader, never to a mid-snapshot statement), router/scanner re-pointed. Commit `feat(gres): mutable rowid-interval range map`.

### Task 6: The split orchestrator — checkpoint forks

**Files:** Create `crates/gres-ranges/src/split.rs`; modify `crates/gres-substrate` (filtered restore: interval-scoped ingestion over checkpoint parts — key-sorted parts make this a range-bounded read), `crates/gres-control` + operator + CLI (`crabka gres split <tenant> <table> <rowid>` and `move`; layout mutations; new-compute placement).

The orchestration (each step durable-or-idempotent, crash-anywhere): force checkpoint of r (G-3) → pause r's writes at covered offset (writer control message) → commit map v(n+1) on range 0 → successors restore filtered views + filtered tail replay → successors fence their fresh topics and run the prologue (inheriting in-doubt markers by interval) → park r (G-5 mechanism, generation bump) → un-pause = the sides are serving. A crash at any step leaves either r serving (map not yet bumped) or the sides recoverable (map bumped; successors' recovery is deterministic from checkpoint+tail) — never neither, never both-for-one-key; Task 7's model is the proof obligation.

Steps: filtered-restore TDD (restore [b, hi) from a full checkpoint equals the fold filtered); orchestrator happy path in-process; kill-at-every-step suite (the G-3 crash-anywhere pattern extended with the map-version dimension); move as degenerate split. Commit `feat(gres): online splits as checkpoint forks`.

### Task 7: The split model + split-under-nemesis (the G-8b gate)

**Files:** Create `crates/gres-ranges/tests/split_model.rs` (state: journal-per-range, map versions, checkpoints, in-doubt markers, computes with epochs; actions: every split step, every crash, 2PC decide racing the split, fence races; invariants: exactly-one-serving-owner per key at every map version, decisions honored on both sides, no in-doubt marker stranded, successor folds partition the predecessor fold) and `tests/split_nemesis.rs` (real split under live sharded load + writer kills; bounded write-pause asserted).

Commit `test(gres): split model and split-under-nemesis`.

## Completion checklist (maps to the G-8 gates)

- Visibility equivalence property suite + corpus-through-sharding at baseline parity (Task 3).
- Bank/Elle green on sharded tables (Task 3); ingest ~linear and the decision ceiling measured/published (Task 4).
- Split model exhaustive within bounds; kill-anywhere and nemesis suites green; write-pause bounded and asserted (Tasks 6–7).
- Unsharded tables byte-identical throughout (pinned in Tasks 1–2).
- G-8c remains research: the plan deliberately contains no work toward distributed decisions.
