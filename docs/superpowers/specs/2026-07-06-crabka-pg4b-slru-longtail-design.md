# PG-4b: SLRU materialization, relation lifecycle, and the index-rmgr long tail — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The follow-on slice of [PG-4](2026-07-06-crabka-pg4-redo-pageservice-design.md) in the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — **the sole blocker on PG-5's boot gate**. Three concerns: (a) SLRU/CLOG materialization, (b) relation-lifecycle interpretation + exact `GetRelSize`, (c) the index-rmgr redo long tail.

## Context — what "a booting Postgres" actually needs

PG-4 deliberately bounded redo to the pgbench-class rmgr set and left three gaps that PG-5's gate cannot pass without:

1. **SLRUs.** MVCC visibility reads transaction status from **clog** (`pg_xact`, 2 bits/xid) and row-lock state from **multixact** — paged files that are *not relations* (no block refs; they never pass through smgr on the pageserver side, and a booting compute reads them from its data directory before smgr is ever consulted). PG-3 retained the raw records in the meta lane, uninterpreted; PG-4b interprets them.
2. **Relation lifecycle.** `SMGR` create/truncate records, commit-time relation drops (carried *inside* XACT commit records), and `DBASE` create/drop — without them, `GetRelSize` stays a hint (`exact=false`), truncated tails remain readable (wrong), and dropped relations never become `NotFound` or GC-eligible.
3. **The index long tail.** GIN, GiST, SP-GiST, BRIN, and hash redo arms — any workload using those index types currently hits `UnsupportedRmgr` (correct, loud, and a hard stop for real applications).

## Design Goals

- **SLRUs ride the existing machinery:** SLRU pages become first-class layer-store keys with delta values and redo arms — reusing PG-3's layers/compaction/GC and PG-4's dispatch wholesale, not a parallel store.
- **Exact `GetRelSize`:** a per-relation size projection (`exact = true`), seeded from file sizes at import, updated by extends/truncates/creates/drops; reads beyond the size error; dropped relations read `NotFound`.
- **The long tail lands per-arm, differentially:** each index family ships only with its own fixture workload + standby byte-comparison, exactly PG-4's discipline.
- **The oracle extends cleanly:** the standby's SLRU segments are themselves pure redo output — `pg_xact`/`pg_multixact` files byte-compare at the capture LSN just like relation pages.

## Non-goals

- **`pg_commit_ts`** (`track_commit_timestamp = off` v1), **`pg_subtrans`/`pg_serial`/`pg_notify`** (reset at startup, never materialized — documented).
- **`CREATE DATABASE … STRATEGY FILE_COPY`** — refused loudly (`UnsupportedRecord`); the PG-15+ default `WAL_LOG` strategy works through ordinary block refs already.
- **GENERIC/LOGICALMSG/REPLORIGIN rmgrs** (extension WAL, logical decoding concerns) — retained-uninterpreted, refused at redo if a page read requires them.
- **Space reclamation from drops/truncates** (GC *eligibility* is marked; the reclamation pass rides PG-4's GC unchanged).
- Branching (PG-6), compute (PG-5b), live-ingest HA.

## Architecture Overview

```
KEY-SPACE EXTENSION (amends PG-3's contract — greenfield, no shims):
  Key::Rel   { rel: RelTag, blk: u32 }                  (was PageKey — unchanged semantics)
  Key::Slru  { kind: SlruKind{Clog, MultiXactOff, MultiXactMem}, segno: u32, blk: u32 }
  Key::RelMeta { rel: RelTag }                          (the size/lifecycle projection)
  — one total order; layer names/container entries encode the tagged key.

INGEST (extends PG-5a's live ingest + PG-3's fixture ingest):
  Sharded::Meta records now pass a LIGHT interpreter (in crabka-postgres-redo):
    XACT commit/abort  → touched clog page keys (xid + subxids may span pages)
                       + commit-carried rel drops → RelMeta lifecycle deltas
    CLOG zeropage/trunc, MULTIXACT zero/create → Slru keys
    SMGR create/trunc, DBASE (WAL_LOG passthrough | FILE_COPY refuse) → RelMeta deltas
  → delta entries (raw record bytes) at those keys; everything else stays retained-uninterpreted.

REDO (new arms beside PG-4's):
  Slru arms: fold commit/abort status bits into 8 KB clog pages; multixact writes.
  RelMeta "redo": fold size/lifecycle events → { nblocks, dropped_at } @ LSN.

SERVE:
  GetRelSize → the projection (exact=true); beyond-size → error; dropped → NotFound.
  Basebackup → materialize Slru pages ≤ LSN → pg_xact / pg_multixact segment files (32 pages/segment).
```

## Key Design Decisions

### SLRU deltas are raw records; interpretation splits ingest-light / redo-full

The ingest-time interpreter answers only *"which pages does this record touch?"* (an XACT commit's `xid` + subxids map to clog pages arithmetically; subxid arrays may span pages — one delta entry per touched page, sharing the record `Arc`, exactly PG-2's multi-block fan-out shape). The *full* interpretation ("set these 2-bit statuses") happens at redo time in a per-`(rmid, info)` arm, like every other rmgr. This keeps ingest thin and stateless, and reuses layers/compaction/GC/materialization for SLRUs with zero new storage machinery. *Alternative rejected — eager SLRU materialization at ingest:* a second stateful write path with its own crash-consistency story; the layer store already solves all of that.

### The key space becomes a tagged enum (an explicit PG-3 amendment)

PG-3 specced `PageKey(RelTag, blkno)`; PG-4b amends it to the three-variant `Key` **before any of it is built** (greenfield — no migration, no shims; PG-3's plan executors implement the enum from the start). `RelMeta` values are tiny (`nblocks` + lifecycle marker @ LSN) but flow through the same delta/image/`get_reconstruct_data` path, so "size at LSN" and "dropped at LSN" are ordinary versioned reads — which is exactly what PG-6 branching will want too.

### Exact rel-size with an honest seam

`GetRelSize` reads the `RelMeta` projection: seeded relations get **exact** sizes at import (file sizes are known); subsequent extends (max-blkno from ordinary page deltas), `SMGR` truncates/creates, and commit-carried drops update it. `exact=true` from this slice on; a read beyond `nblocks` errors (`BlockBeyondEof`), a read of a dropped relation is `NotFound`. This also finally makes trims *meaningful*: truncated/dropped ranges become GC-eligible under PG-4's existing horizon rule.

### The long tail is five fixture-gated arm families

BRIN and hash first (simplest structures), then GiST, SP-GiST, GIN (posting trees + pending lists — the densest). Each family lands as: a fixture-generator extension (`CREATE INDEX USING <am>` + insert/update/delete/vacuum traffic), the redo arms, and its standby byte-comparison over the index relation's pages. No family ships without its differential; until it lands, that family keeps failing loudly — the PG-4 contract, unchanged.

## Integration

- **`crates/page-store`** — the `Key` enum + ordered encoding (amends PG-3); no other storage change.
- **`crates/postgres-redo`** — the meta interpreter (`shard_meta(record) -> Vec<(Key, …)>`), the SLRU + RelMeta redo arms, the five index arm families.
- **`crates/pageserver`** — ingest wires `shard_meta`; `GetRelSize` reads the projection (`exact=true`); `Basebackup` renders SLRU segment files (un-`#[ignore]`s PG-5a's SLRU assertions); **PG-5's Task-7 boot gate unblocks when this lands**.
- **`tools/gen-pg-wal-fixtures.sh`** — index workloads, multixact traffic (concurrent `SELECT … FOR SHARE`), `TRUNCATE`/`DROP TABLE`, `CREATE DATABASE` (WAL_LOG), post-checkpoint touches.
- **Roadmap** — PG-4b's entry updated to this concrete scope.

## Kafka / wire compliance

Not a wire surface. The byte-exactness bar extends to SLRUs: **`pg_xact`/`pg_multixact` segment bytes at the capture LSN must equal the standby's** — the standby's SLRU state is itself pure redo output, so the comparison is exact (no masking).

## Testing

- **Clog arithmetic units:** xid→(page, offset) mapping; subxid arrays spanning page boundaries fan out to every touched page; status-bit folding (commit/abort/sub-commit) produces the documented 2-bit encodings.
- **Multixact units:** offsets/members page writes; zero-page arms.
- **Lifecycle units:** truncate → `nblocks` drops at the record's LSN (reads beyond → `BlockBeyondEof`; reads *below* the truncate LSN still serve the old tail — versioned reads); drop → `NotFound` at ≥ LSN; `DBASE` FILE_COPY → loud refusal.
- **`GetRelSize` exactness:** seeded size exact at LSN₀; grows with extends; truncate/drop respected; `exact=true` asserted end-to-end through the RPC.
- **Per-family index differentials:** each index family's fixture workload byte-compares its index pages vs the standby — the family's shipping gate.
- **The extended standby gate:** relation pages (PG-4's) **plus** `pg_xact`/`pg_multixact` segment bytes at the capture LSN; basebackup SLRU content assertions (PG-5a's `#[ignore]`s) enabled.

## Risks (carried into the plan)

- **GIN is the hardest arm family** (posting-tree splits, pending-list merges) — sequenced last, with the densest fixtures; its differential is the arbiter as always.
- **Subxid page-spanning fan-out** is the subtle ingest correctness point — property-tested (any xid+subxid set → the exact touched-page set).
- **Commit-record parsing breadth:** XACT commit/abort has many optional payload sections (subxids, drops, invalidations, origin) gated by `xinfo` flags — the parser takes what PG-4b needs (subxids, drops) and length-skips the rest, differentially validated.
- **Basebackup SLRU rendering** must produce byte-complete segments including zeroed never-touched tails — validated by the standby file comparison, not by construction.
- **Key-space amendment discipline:** if PG-3 execution has started before PG-4b lands, the enum change is a mechanical refactor (greenfield, no compat) — noted so executors sequence it deliberately.

## Resolved decisions

- **SLRUs in the layer store** via the tagged `Key` enum (PG-3 amended pre-build); raw-record deltas, ingest-light/redo-full interpretation split.
- **Scope:** clog + multixact only (commit_ts/subtrans/serial/notify out, documented); DBASE WAL_LOG passthrough, FILE_COPY refused.
- **Rel lifecycle:** `RelMeta` projection; `GetRelSize` `exact=true`; `BlockBeyondEof`/`NotFound` semantics; GC eligibility via the existing horizon rule.
- **Long tail order:** BRIN → hash → GiST → SP-GiST → GIN, each fixture-gated per-family.
- **Gate:** the extended standby comparison (relations + SLRU segments) unblocks PG-5 Task 7.
