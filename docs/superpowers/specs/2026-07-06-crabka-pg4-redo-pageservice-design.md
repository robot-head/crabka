# PG-4: Native redo, `get_page@LSN`, and the page service — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. Third slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — the read/materialization half of the pageserver track, plus the compute-facing service. **Carries the chapter's two crux decisions, both resolved by the user in this cycle.**

## Context — where this sits, and the two decisions

PG-4 turns PG-3's reconstruction *plans* into **pages**: a redo engine applies per-page WAL records over a base image; `get_page@LSN` serves the result; image-layer creation and GC (re-homed here from PG-3, because both require redo) keep read amplification and storage bounded; and a page service exposes it all to future compute (PG-5).

**Decision 1 — redo is full native Rust.** No sandboxed Postgres walredo sidecar, no hybrid: the rmgr redo logic is reimplemented in Rust. This buys a pure-Rust runtime (no C process, no patched-Postgres dependency for the *pageserver*) at the price of owning the most correctness-critical surface in the chapter — a redo bug is silent page corruption. The design therefore treats two mitigations as **load-bearing, not optional**:
- **Bounded rmgr scope with loud faults.** v1 implements exactly the set a pgbench-class workload needs — `XLOG` (FPI, `FPI_FOR_HINT`), `HEAP`, `HEAP2`, `BTREE`, `SEQ` — and returns `RedoError::UnsupportedRmgr { rmid, lsn }` for anything else (GIN/GiST/SP-GiST/BRIN/hash → a PG-4b slice; a relation using them is unservable and says so). The correctness surface grows one differentially-proven rmgr at a time.
- **The standby differential oracle.** Redo output is compared byte-for-byte against a **WAL-replayed stock-Postgres standby** — whose pages are themselves pure redo output, so both sides diverge from the primary identically (hint bits, unlogged mutations). Redo-vs-redo makes byte-exactness achievable; masked primary comparison (tuple hint bits, checksum field) is the fallback where a standby capture is impractical.

**Decision 2 — the page service is a Crabka-native Connect RPC** (not Neon-pagestream-compatible). A clean protocol versioned by us, on the same connectrpc-axum stack as the gateway. **Consequence carried honestly into PG-5:** compute cannot vendor Neon's client pieces; PG-5 must build its own smgr↔Connect client (likely a small Rust cdylib behind a C ABI — the `unsafe`-boundary tension that implies is resolved in PG-5's own cycle, not silently here).

## Design Goals

- **Pure, sans-IO redo:** `apply(base: Option<PageImage>, records: &[(Lsn, Bytes)]) → Result<PageImage, RedoError>` — per-rmgr dispatch over PG-2's decoded envelope, panic-free (fuzzed), PG 17 pinned (per-major tables like PG-2).
- **`get_page@LSN`:** PG-3's `get_reconstruct_data` ⊕ redo = the page; `will_init`/zeroed-base semantics correct.
- **Materializing compaction + GC** (re-homed): image layers created via redo when delta stacks cross a threshold; horizon GC drops layers fully covered by a later image — both behind a `Redo` trait seam so `crabka-page-store` never depends on the redo crate.
- **The page service:** unary Connect RPCs — `GetPage(rel, fork, blkno, lsn)`, `GetRelSize(rel, fork, lsn)` — served in-process against a store + redo, following the gateway's connectrpc-axum idiom.
- **The differential gate:** every covered page in the fixture corpus, materialized at the capture LSN, is byte-identical to the standby's file.

## Non-goals

- **The rmgr long tail** (GIN/GiST/SP-GiST/BRIN/hash), **SLRU/CLOG materialization** (commit-status pages — required before PG-5's boot gate; explicitly PG-4b together with the meta-lane interpretation PG-3 retained), **logical-message/origin rmgrs**.
- **Basebackup, `DbSize`, streaming/batched page RPCs, page-lease semantics** — PG-5's cycle, where compute boot requirements become concrete.
- **A materialized-page cache** — correctness first; caching is a measured follow-up.
- **Live ingest** (PG-1), **branching** (PG-6), **multi-node**.

## Architecture Overview

```
crates/postgres-redo   (crabka-postgres-redo — pure, sans-IO, fuzzed)
│   dispatch by (rmid, info) over PG-2's DecodedRecord envelope
│   v1 rmgrs: XLOG(FPI, FPI_FOR_HINT) · HEAP · HEAP2 · BTREE · SEQ
│   apply(base, records) -> PageImage | RedoError::{UnsupportedRmgr, BadRecord, BaseMissing}
│
crates/page-store      (PG-3, extended)
│   trait Redo { fn apply(...) }                      ← the seam; no dep on postgres-redo
│   get_page(key, lsn, &dyn Redo) -> PageImage        = get_reconstruct_data ⊕ redo
│   materializing compaction: delta-stack threshold → image layers (via Redo)
│   horizon GC: drop layers below gc_horizon fully covered by a later image
│
crates/pageserver      (crabka-pageserver — the service)
│   proto/crabka/pageserver/v1/pageserver.proto:
│     service PageService { rpc GetPage(...); rpc GetRelSize(...); }
│   connectrpc-axum (gateway idiom: build.rs codegen, .build_connect(), h2c-capable)
│   wires postgres-redo into page-store's Redo seam
│
oracle: stock PG 17 primary (fixture WAL) ──► pg_basebackup standby replayed to LSN
        └── captured relation files == our get_page output, byte-for-byte
```

## Key Design Decisions

### Per-`(rmid, info)` dispatch with loud refusal

The redo entry point dispatches on the decoded envelope's `(rmid, info)`; every unimplemented arm is `RedoError::UnsupportedRmgr` — never a silent skip (a skipped record is a corrupt page). This is also the growth seam: PG-4b adds arms, each landing only with its own differential coverage. FPI application is the degenerate arm (the base *is* the image) and `will_init` records apply against a zeroed page — both semantics fixed by PG-3's `ReconstructData` contract.

### The standby oracle (redo-vs-redo)

The fixture generator (extending PG-2's) additionally: takes a `pg_basebackup` of the primary, replays it as a standby to a target LSN, cleanly shuts it down, and captures the **relation files of the covered tables/indexes** (a few MB — the test table, its btree index, its sequence) plus a `(rel, fork, nblocks, capture_lsn)` manifest. The gate materializes every covered page at `capture_lsn` through `get_page` and compares byte-for-byte. Because both sides are pure redo, no masking is needed on the primary-divergent fields; the masked-primary comparator is retained as a secondary check and documented fallback.

### Materialization behind a `Redo` trait

`crabka-page-store` gains `trait Redo` and the compaction/GC drivers generic over it; `crabka-postgres-redo` implements the trait; `crabka-pageserver` wires them. Dependency arrows stay clean (`page-store` ⟂ `postgres-redo`), and PG-3's structural tests keep running with a no-op redo. Image-layer creation policy v1: when a key range's delta stack above the newest image exceeds a threshold, materialize an image layer at the stack's top LSN; GC v1: delete layers whose `lsn_range` ends below `gc_horizon` **and** whose key range is fully covered by a later image layer.

### The Connect page service follows the gateway idiom

Same stack, same build discipline as `crates/grpc-gateway`: a `.proto` compiled by `connectrpc-axum-build` in `build.rs`, handlers behind `.build_connect()`, h2c-capable serving (the MSG-5 listener work generalizes). Unary `GetPage`/`GetRelSize` suffice for v1 — no streaming until PG-5 measures the need. `GetRelSize` v1 returns PG-3's `max(blkno)+1` hint (exactness arrives with SMGR-record interpretation in PG-4b/PG-5).

### Panic-freedom as a tested property

Redo runs server-side on untrusted-shaped input (any bytes a WAL could contain). All page arithmetic (line pointers, item offsets, special space) is bounds-checked returning `RedoError::BadRecord`; a record-level fuzz target (decode → apply) asserts no panics; pure helpers (item insertion, page init, compaction of line pointer arrays) get property tests, and Creusot lemmas where they fit the verified-kernel pattern (e.g., offset arithmetic never exceeding `BLCKSZ`).

## Integration

- **`crates/postgres-redo`** (new, `crabka-postgres-redo`) — **`publish = false` + private release-plz entry**; deps: `crabka-postgres-wal` (envelope types), `bytes`, `thiserror`. No tokio.
- **`crates/page-store`** — `trait Redo`, `get_page`, materializing compaction, GC (extends PG-3).
- **`crates/pageserver`** (new, `crabka-pageserver`) — proto + Connect service + wiring; **`publish = false`** likewise.
- **Fixtures** — the PG-2 generator grows the standby capture; corpus shared from `crates/postgres-wal/tests/fixtures`.
- **Roadmap consequence:** PG-5 gains the bespoke smgr↔Connect client work (and PG-4b gains SLRU) — the roadmap doc is updated alongside this spec.

## Kafka / wire compliance

Not a Kafka surface. The byte-exactness bar transfers whole: **a materialized page must equal real Postgres redo output byte-for-byte** — enforced by the standby gate, per covered rmgr, before that rmgr is considered shipped.

## Testing

- **Per-arm unit tests:** each `(rmid, info)` arm against fixture-extracted records with known before/after pages (heap insert/update/delete/HOT, multi-insert, prune/visibility, btree leaf insert + split, sequence bump, FPI/FPI_FOR_HINT).
- **`will_init`/zeroed-base:** an init record with `base: None` produces the correct initialized page; `BaseMissing` when a non-init chain lacks a base.
- **UnsupportedRmgr:** a GIN-bearing fixture record refuses loudly with rmid + LSN.
- **The standby gate:** every covered page at `capture_lsn` byte-identical to the standby's files (the slice gate).
- **Materializing compaction + GC:** reads identical before/after image creation; GC never deletes a layer still needed by any LSN ≥ `gc_horizon` (property: reconstruct-at-horizon unchanged).
- **Fuzz/property:** decode→apply never panics; page-helper invariants (bounds, ordering).
- **Service integration:** in-process pageserver over the ingested corpus serves `GetPage` == the gate's pages; unknown relation → NotFound; unsupported-rmgr page → the explicit error surfaced through the RPC.

## Risks (carried into the plan)

- **Full-native redo correctness (the chapter's top risk, accepted by decision):** mitigations are the bounded rmgr set, the standby oracle, per-arm differential growth, fuzzing/property tests. Any gate mismatch is a redo bug — fix the engine, never the oracle.
- **Btree split fidelity** is the hardest v1 arm (multi-page, ordering-sensitive) — its per-arm tests get the densest fixtures.
- **Hint-bit residue** if the primary comparator is used where the standby capture is impractical — masked comparison documented (tuple infomask hint bits, `pd_checksum`).
- **`GetRelSize` is a hint** until SMGR/truncate interpretation (PG-4b) — compute-facing exactness deferred, stated.
- **PG-5 client burden** (Decision 2's cost): the smgr↔Connect client is now bespoke — flagged forward, sized in PG-5's cycle.

## Resolved decisions

- **Redo:** full native Rust (user decision); bounded v1 rmgr set {XLOG-FPI, HEAP, HEAP2, BTREE, SEQ}; loud `UnsupportedRmgr`; PG-4b for SLRU + the index long tail.
- **Oracle:** WAL-replayed standby, byte-exact; masked-primary fallback.
- **Protocol:** Crabka-native Connect RPC (user decision); unary `GetPage`/`GetRelSize`; gateway build idiom; PG-5 owns the bespoke client consequence.
- **Seams:** `trait Redo` in `page-store`; materializing compaction + GC live there, driven by the trait.
- **Crates:** `crabka-postgres-redo` (pure) + `crabka-pageserver` (service), both `publish = false`.
