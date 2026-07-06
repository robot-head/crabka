# PG-3: The versioned-page layer store — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. Second slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — the storage half of the pageserver track, sitting between PG-2's decoded stream and PG-4's redo.

## Context — where this sits

PG-3 stores what PG-2 emits and serves what PG-4 needs: an object-bucket-native store of **versioned page history** — per-page WAL deltas and page images keyed `(RelTag, block) @ LSN` — whose read API answers *"give me everything needed to reconstruct page K at LSN L."* The grounding verdict drives the design: the blockstore's **infrastructure** (the `ObjectOps` bucket surface, index-persistence patterns) is reusable, but its **data model** (wall-clock-pruned, scan-oriented, latest-only) cannot express LSN-versioned point reads — so the layer data model is net-new, patterned on Neon's delta/image layers and on Crabka's own footer-manifest object idiom (the diskless-WAL flush objects).

**One roadmap refinement (explicit):** the roadmap listed "compaction/GC" under PG-3. Designing it surfaced that **image-layer creation and real GC require redo** — a page cannot be materialized from delta records without applying them. So PG-3 ships the *formats* for both layer kinds, ingest, the read plan, and **structural** (non-materializing) compaction; **materializing** compaction (image-layer creation) and GC land in PG-4 alongside redo. PG-4's surface grows accordingly; nothing is lost, only honestly re-homed.

## Design Goals

- **Versioned-page data model:** immutable **delta layers** (a key-range × LSN-range of `(key, lsn) → value` entries) and **image layers** (a key-range at one LSN of `key → 8 KB page`), indexed by an in-memory **layer map**.
- **The read plan, not the page:** `get_reconstruct_data(key, lsn) → { base: Option<(Lsn, PageImage)>, deltas: Vec<(Lsn, WalEntry)> }` — the newest image (or `will_init` record) at ≤ LSN plus every delta above it, oldest-first. Materialization (redo) is PG-4; the one case PG-3 answers outright is a page whose newest entry at ≤ LSN is already an image (an FPI) with no deltas above.
- **Bucket-native:** layers are immutable objects written once and read by byte range (`ObjectOps::{put_from_path, get_range, list}`); the layer map is rebuildable from an object listing alone (crash recovery without a metadata service).
- **Idempotent ingest:** re-feeding WAL below `disk_consistent_lsn` (after a crash, or when PG-1 replays) is a no-op — same `(key, lsn)` ⇒ same value.
- **Timeline-shaped from day one:** every path and type carries `(tenant, timeline)` so PG-6 branching is additive, even though v1 runs a single timeline.

## Non-goals

- **Redo / page materialization, image-layer *creation*, GC** — PG-4 (the refinement above). PG-3 defines the image-layer format + reader/writer so PG-4 adds only the materialization driver.
- **SLRU/CLOG and rmgr-interpreted metadata** (relation truncate/create, commit-timestamp indexing): interpreting rmgr payloads is redo-adjacent. PG-3 **retains** PG-2's `Sharded::Meta` records verbatim in a per-timeline meta lane — kept, not interpreted — so PG-4+ can interpret later without re-ingesting.
- **Relation-size (`nblocks`) service** — PG-5's smgr needs it; v1 records `max(blkno)+1` per relation as a derived hint only, refined when truncate records are interpreted (PG-4+).
- **Branching/PITR** (PG-6), **live ingest** (PG-1), **a network service** (PG-4's page service).
- **Sharding across pageserver nodes** — single-process v1.

## Architecture Overview

```
crates/page-store  (crabka-page-store — async, tokio; consumes crabka-postgres-wal, crabka-object-store)
│
│  INGEST (single writer per timeline)
│  Sharded::Page { key, lsn, rec } ──► OpenLayer (BTreeMap<(PageKey, Lsn), Value>)
│       Value::Image(8 KB)   ← block ref carried an FPI (hole already reconstructed by PG-2)
│       Value::Wal { will_init, bytes } ← ordinary per-page record
│  Sharded::Meta ──► meta lane (verbatim retention, uninterpreted)
│       size threshold / checkpoint ──► flush: L0 delta layer object + advance disk_consistent_lsn
│
│  LAYERS on the bucket (immutable; shared container format)
│  pg/<tenant>/<timeline>/<keyrange>__<lsnrange>.{delta|image}
│  [header | sorted entries | sparse key index | footer{index_off, count, crc, magic}]
│       read = get_range(footer) → get_range(index window) → get_range(entry block)
│
│  LAYER MAP (in-memory; rebuilt from list() + name parsing)
│  query(key, lsn): layers intersecting key with lsn ≤ L, newest-first
│       └─► get_reconstruct_data(key, lsn) -> ReconstructData { base, deltas }   ── PG-4 redo consumes
│
│  STRUCTURAL COMPACTION: N × L0 (full-key-range) ──► key-partitioned L1 deltas (pure re-sort, no redo)
```

## Key Design Decisions

### Two layer kinds, one container format, Crabka's footer idiom

Delta and image layers share one immutable container: `header (magic, version, kind, tenant/timeline, key-range, LSN-range) · entries sorted by (key, lsn) · a sparse key→offset index · footer (index offset, entry count, CRC, footer magic)`. Point reads never download a layer: `get_range` the fixed-size footer, binary-search the sparse index window, `get_range` the entry block — the same footer-manifest + byte-range idiom as the diskless-WAL flush objects, applied to a sorted key space. *Alternative rejected — Parquet layers:* the blockstore grounding showed row-group granularity and time-oriented stats are wrong for single-8 KB-page point reads; forcing Parquet here would be reuse-theater. The image-layer format lands now (PG-4 writes them); only FPI-bearing *delta* entries carry images in PG-3.

### `Value::{Image, Wal{will_init}}` — reconstruction bases inside the delta stream

An FPI **is** a materialized page at its LSN, and a `will_init` record (`BKPBLOCK_WILL_INIT`) reinitializes a page ignoring prior state — both terminate the backward search. Encoding them as first-class entry values (exactly Neon's `Value::Image`/`Value::WalRecord`) means `get_reconstruct_data` works correctly in PG-3 even though no image *layers* exist yet: post-checkpoint FPIs seed the bases. This is also what makes the slice testable without redo (see Testing).

### The layer map is rebuildable from the bucket alone

Layer object names encode `(key-range, LSN-range, kind)`; startup lists the timeline prefix and parses names — no manifest object, no metadata service, no recovery WAL for the store itself. `disk_consistent_lsn` = the max flushed LSN-range end; ingest resumes there and tolerates replayed overlap idempotently. v1's map is a sorted-by-LSN vector with linear intersection scan — correct and upgradeable to an interval structure when layer counts demand it. *Alternative rejected:* a manifest object — an optimization with a consistency obligation, deferred until listing cost bites.

### Structural compaction only (the refinement)

When L0 count crosses a threshold, N full-key-range L0 deltas are merge-sorted into key-partitioned L1 deltas — pure data movement that bounds read fan-in (a read touches ≤ L0-threshold + O(1) L1 layers per LSN band) with **no redo required**, and `get_reconstruct_data` results are provably identical before/after (tested). Image creation + GC arrive with PG-4's redo.

### Single-writer ingest, concurrent readers

One ingest task per timeline owns the open layer; readers share the layer map behind `Arc<RwLock<…>>` and read immutable objects. Matches the substrate (one WAL group in, many `get_page` readers) without inventing concurrency the slice doesn't need.

## Integration

- **`crates/page-store`** (new, `crabka-page-store`) — **`publish = false` + private release-plz entry** (allowlist gate).
- **Consumes:** `crabka-postgres-wal` (`Lsn`, `PageKey`, `RelTag`, `Sharded`, `PageImage`) — PG-2's output contract; `crabka-object-store` (`ObjectOps`: `put_from_path`, `get_range`, `list`) — the bucket surface, unchanged.
- **Produces for PG-4:** `get_reconstruct_data` + the image-layer writer it will drive; the meta lane for later interpretation.
- **Object layout:** `pg/<tenant>/<timeline>/…` — beside, never entangled with, `diskless-wal/…` and the blockstore prefixes: one bucket, distinct prefixes.

## Kafka / wire compliance

Not a wire surface. The byte-exactness discipline applies to **page fidelity**: an FPI stored and returned as a base must be byte-identical to what the WAL carried (tested); layer CRCs guard object corruption.

## Testing

- **Container round-trip:** write a delta layer (mixed `Image`/`Wal` values) via `ObjectOps` to `InMemory`; point-read entries back through footer→index→range reads byte-identically; a corrupted footer/CRC errors with the object key.
- **Reconstruct plan:** synthetic layers → `get_reconstruct_data(K, L)` returns the newest base ≤ L and exactly the deltas in `(base, L]` oldest-first; a `will_init` record terminates like an image; a key with no entries errors cleanly; an LSN below the oldest base errors "history trimmed".
- **FPI byte-match oracle (the slice gate):** ingest the PG-2 fixture corpus end-to-end (decoder → ingest → flush); for every FPI-bearing page, `get_reconstruct_data(key, fpi_lsn)` returns that FPI as base, **byte-identical** to the WAL's (hole-reconstructed) image, with zero deltas above.
- **Idempotent re-ingest:** re-feed a fixture LSN overlap; layer contents and reconstruct results unchanged.
- **Crash recovery:** drop the layer map; rebuild from `list()`; identical query results.
- **Structural compaction:** reconstruct results identical before/after L0→L1; read fan-in bounded.

## Risks (carried into the plan)

- **Pages with no FPI base in the fixture window:** a page whose history starts mid-stream (no checkpoint FPI in the corpus) has delta-only history — `get_reconstruct_data` correctly returns `base: None` + deltas, but only PG-4 can validate the *content*. The fixture generator's post-checkpoint traffic guarantees FPI coverage for the gate; delta-only pages are asserted structurally only.
- **Layer-name key encoding:** `RelTag` is 4 × u32 + fork — fixed-width hex encoding must sort consistently with the in-memory key order (tested property).
- **Read amplification before image layers exist:** long delta chains until PG-4 materializes images — acceptable for a fixture-scale v1; the L0→L1 bound + the PG-4 handoff are the mitigations.
- **Meta-lane growth:** verbatim retention is unbounded until PG-4+ interprets/GCs it — fixture-scale acceptable, flagged.

## Resolved decisions

- **Model:** delta + image layers, one container format, footer + sparse index, byte-range reads; `Value::{Image, Wal{will_init}}`.
- **Read API:** `get_reconstruct_data` (plan, not page); FPIs/`will_init` are bases.
- **Recovery:** layer map from `list()` + name parsing; `disk_consistent_lsn`; idempotent re-ingest.
- **Compaction:** structural L0→L1 only; image creation + GC re-homed to PG-4 (roadmap refinement).
- **Meta records:** retained verbatim, uninterpreted.
- **Crate:** `crates/page-store`, async, `publish = false`; single-writer/multi-reader.
