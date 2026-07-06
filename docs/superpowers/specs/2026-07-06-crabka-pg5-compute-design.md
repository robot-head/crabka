# PG-5: Compute integration — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The keystone slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md): a real Postgres boots and runs against the disaggregated stack. Structured as **PG-5a** (pageserver readiness, pure Rust) + **PG-5b** (the compute image). **The compute client decision — a Rust cdylib behind a C ABI — was resolved by the user in this cycle**, establishing the workspace's one sanctioned `unsafe` boundary.

## Context — what compute actually forces

"Boot Postgres against the pageserver" decomposes into two halves with different natures:

**PG-5a — pageserver readiness (Rust, on landed designs).** Four service-side pieces compute cannot run without:
1. **Timeline seeding:** `initdb`'s WAL predates any replication slot, so a timeline is born by **importing** an `initdb`-produced data directory — every relation file becomes image entries @ LSN₀ in the layer store; non-relation files (control file, SLRU segments, config skeleton) are retained for basebackup. (Neon's bootstrap shape.)
2. **Live topic ingest:** a consumer loop `__pg_wal.<cluster>` → PG-2 decoder → PG-3 ingest, advancing `last_ingested_lsn`. Pure composition of landed designs; sequential — no LSN→offset index needed (as PG-1 anticipated).
3. **`GetPage`/`GetRelSize` LSN-wait:** compute requests pages at its flushed-WAL LSN; the pageserver **waits** (bounded, configurable timeout → clear error) until `last_ingested_lsn ≥ request.lsn` before materializing. Without this, reads race ingest.
4. **A `Basebackup` RPC:** the minimal boot bundle at an LSN — `pg_control` (checkpoint pointing at the LSN), SLRU segments (**PG-4b's materialization** — hence the boot-gate dependency), the seeded non-relation files — as a tarball stream.

**PG-5b — the compute image (the C/fork half).**
- **A minimal PG-17 core patch:** upstream Postgres through 17 has **no pluggable smgr**, so a small vendored patch adds an smgr-registration hook (the shape the Neon/TDE forks use) — maintained per supported major, applied to pinned PG 17.x sources in the image build. Everything else lives in an extension.
- **The `crabka` extension (C):** registers the smgr — reads → `GetPage(lsn = flushed WAL position)`, `nblocks` → `GetRelSize` (+ a local size cache maintained by `extend`/`truncate`), `write`/`extend` → **no-ops for data** (WAL is the truth; evicted buffers drop), `exists` via `GetRelSize`. GUCs: pageserver endpoint, tenant/timeline/cluster id.
- **The client — the resolved crux:** **`crabka-compute-client`**, a Rust **cdylib** exposing a C ABI (`ck_get_page`, `ck_get_rel_size`, `ck_connect`, …) over a **blocking** Connect-unary HTTP/1.1 client (prost + a sync HTTP dep) — smgr calls are synchronous, so no async runtime, no h2, no streaming. Proto codegen is shared with the pageserver: the client can never drift.
- **The write path composes what exists:** the patched compute is a stock-shaped *primary* (`wal_level=replica`, `full_page_writes=on` — FPIs remain the redo bases, as PG-3/4 assume); **PG-1's safekeeper attaches to it as a replica**, unchanged.

## The sanctioned `unsafe` boundary (user decision, encoded)

The workspace forbids `unsafe`; an `extern "C"` ABI cannot exist without it. The exception is **narrow and structural**:
- Exactly **one** crate (`crabka-compute-client`) declines the workspace lint set; `unsafe` code is confined to a single thin `src/ffi.rs` (pointer/CStr marshalling only — every other module remains `#![forbid(unsafe_code)]` at the module level via lint config), with `unsafe_op_in_unsafe_fn = "deny"` and every `unsafe` block carrying a `// SAFETY:` justification.
- The FFI surface is C-header-generated (`cbindgen`) so the C side never hand-declares signatures.
- The code style guide gains a paragraph recording this exception and its rules — the precedent is documented, not implicit.

## Design Goals

- **The keystone conformance gate:** `initdb`-import → seed → basebackup → boot the patched compute → safekeeper attached → live ingest → **pgbench runs**; then differential page-image checks vs stock Postgres on the same workload (the PG-differential analogue of the JVM-differential culture).
- **Compute is disposable:** all durable state lives in the topic + the bucket; killing compute and re-basebackup-ing resumes.
- **Every RPC bounded:** LSN-waits time out with actionable errors; the client surfaces pageserver errors (incl. PG-4's `UnsupportedRmgr`) as PG `ERROR`s naming the cause.

## Non-goals

- **SLRU/CLOG materialization itself** — **PG-4b** (a named prerequisite of the boot gate; 5a's basebackup consumes it).
- **Multiple computes / read replicas** (needs per-relation last-written-LSN tracking), **compute pooling/suspend-resume**, **branching UX** (PG-6), **sharded pageservers**, **TLS/auth on the page service** (in-cluster plaintext v1, flagged), **performance work** (batched/streamed GetPage — revisit with evidence; the cdylib choice keeps that path safe Rust).

## Architecture Overview

```
PG-5a (pageserver additions)                       PG-5b (compute image)
─────────────────────────────                      ─────────────────────────────
seed_timeline(initdb_dir)                          pinned PG 17.x sources
  rel files → image entries @ LSN₀                   + vendored smgr-hook patch (~small, per major)
  nonrel files → basebackup store                    + crabka extension (C):
live ingest: consume __pg_wal.<cluster>                smgr read    → ck_get_page(rel, blk, lsn)
  → WalStreamDecoder → page-store ingest               smgr nblocks → ck_get_rel_size (+ size cache)
  → last_ingested_lsn                                  write/extend → data no-op (WAL is truth)
GetPage/GetRelSize: wait last_ingested ≥ lsn         + crabka-compute-client cdylib (Rust):
Basebackup(lsn) → tar: pg_control @ lsn,               blocking Connect unary over h1 (prost)
  SLRUs (PG-4b), seeded nonrel files                   ffi.rs = the one unsafe boundary (cbindgen)

boot: basebackup → datadir → start compute ── safekeeper attaches (PG-1, unchanged) ──► __pg_wal topic ──► live ingest
gate: pgbench green end-to-end + page-image differential vs stock PG                      (the loop closes)
```

## Key Design Decisions

### The 5a/5b split with a hard interface between them

5a is buildable/testable entirely in Rust against fixtures (seed a timeline from a real `initdb` dir; serve basebackup; ingest the PG-1 topic; LSN-wait under a lagging ingester) — no C, no fork. 5b consumes 5a only through the public RPCs + the tarball. The split keeps the riskiest work (the fork/extension) isolated with the smallest possible contract, and lets 5a land while PG-4b (its SLRU input) is still in flight everywhere except the basebackup's SLRU content.

### Blocking-unary client, deliberately boring

smgr calls block a Postgres backend; the client is therefore synchronous end-to-end: one persistent HTTP/1.1 connection per backend (lazy), Connect unary POSTs, prost-encoded, bounded retries on transport errors, no runtime. Everything above `ffi.rs` is ordinary safe Rust sharing the pageserver's generated types — the drift-free property that motivated the cdylib decision.

### The compute fork is patch-files-plus-pin, not a repo fork

The image build fetches pinned `postgres-17.x` sources, applies vendored `patches/pg17/*.patch` (the smgr-registration hook; reviewed diffs in-tree), builds, and layers the extension + cdylib — the `packaging/` idiom (apko/melange precedents: creusot-toolchain, demo images). No long-lived forked repository to drift; bumping PG = re-pinning + re-verifying patches apply.

### The gate is two-tiered

**Tier 1 (functional):** pgbench initializes and runs against the stack — proves boot, reads, writes, the WAL loop, LSN-waits. **Tier 2 (fidelity):** replay the same deterministic workload on a stock Postgres; compare relation page images at matched LSNs (the PG-4 standby-oracle machinery reused at system level, same masking rules). Tier 2 is what makes "it works" mean "it's byte-faithful".

## Integration

- **`crates/pageserver`** (extends PG-4's crate) — seeding, live ingest, LSN-wait, `Basebackup` RPC.
- **`crates/compute-client`** (new, `crabka-compute-client`, cdylib+rlib) — **`publish = false` + release-plz entry**; the lint opt-out + `ffi.rs` + cbindgen header.
- **`compute/`** (new top-level, outside the Cargo workspace — like `sdks/`) — `patches/pg17/`, the extension C sources, the image build (`packaging/` conventions).
- **`docs/style_guides/code_style_guide.md`** — the documented `unsafe`-boundary exception.
- **Prerequisites:** PG-1…PG-4 designs; **PG-4b lands before the boot gate** (SLRUs in basebackup). PG-5a itself needs only PG-2/3/4's crates.

## Kafka / wire compliance

The safekeeper leg is unchanged Kafka wire (PG-1). The compute↔pageserver leg is the PG-4 Connect service, extended compatibly (`Basebackup` added; `GetPage` gains wait semantics, not shape changes). Page fidelity inherits PG-4's byte-exactness bar, now proven at system level by the Tier-2 gate.

## Testing

- **5a units/integration (Rust only):** seeding round-trip (imported rel files re-served byte-identically @ LSN₀); live ingest advances `last_ingested_lsn` from a real PG-1 topic; `GetPage` blocks-then-serves under a lagging ingester and times out cleanly past the bound; basebackup tarball contains a valid `pg_control` + seeded files (structure-validated; SLRU content once PG-4b lands).
- **Client units:** encode/decode goldens shared with the pageserver proto; FFI-layer tests via the C header (a tiny C test harness calling `ck_get_page` against a mock server); `// SAFETY:` review + `cbindgen` drift check in CI.
- **The boot gate (Tier 1):** end-to-end pgbench on the composed stack (containerized; gated on PG-4b).
- **The fidelity gate (Tier 2):** deterministic workload, page-image differential vs stock PG at matched LSNs.
- **Disposability:** kill compute mid-pgbench; re-basebackup; resume; no acked-transaction loss (the safekeeper topic is the truth).

## Risks (carried into the plan)

- **The core patch is the sharpest tool:** kept minimal (a registration hook), vendored as reviewable diffs, re-verified per PG point release. A patch-drift CI check (apply-against-pinned-sources) guards it.
- **The `unsafe` boundary:** structurally confined (one file, one crate, cbindgen, SAFETY comments, style-guide codification) — the containment *is* the mitigation.
- **PG-4b sequencing:** the boot gate cannot pass without SLRUs; 5a/5b land everything else first, the gate task is explicitly blocked on PG-4b.
- **LSN-wait liveness:** a stalled safekeeper stalls compute reads at fresh LSNs — bounded timeouts + clear errors v1; backpressure/HA later.
- **`full_page_writes=on` WAL volume** — accepted v1 (bases for redo); revisit with pageserver-side image coverage evidence.

## Resolved decisions

- **Client:** Rust cdylib + C ABI (user decision); blocking Connect unary; the one sanctioned `unsafe` boundary, structurally confined and style-guide-documented.
- **Structure:** PG-5a (seeding, live ingest, LSN-wait, basebackup) / PG-5b (patch + extension + cdylib + image).
- **Fork strategy:** vendored patch files over pinned sources; no forked repo.
- **Compute posture:** stock-shaped primary, FPW on; safekeeper attaches unchanged; data-write no-ops.
- **Gate:** Tier 1 pgbench + Tier 2 page-image differential; blocked on PG-4b.
