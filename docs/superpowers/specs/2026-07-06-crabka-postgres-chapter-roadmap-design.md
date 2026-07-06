# Chapter C — Serverless Postgres: the PG-slice roadmap

**Date:** 2026-07-06
**Status:** Approved
**Type:** Chapter decomposition. Orders the [serverless-backend vision](2026-07-06-crabka-serverless-backend-vision-design.md)'s Chapter C (the keystone) into buildable design-cycle slices, grounded against the actual tree.

## Grounding — what the tree actually holds

Three verified facts shape every slice boundary:

1. **`connect-postgres` is pure logical decoding.** pgoutput v1 only (Begin/Commit/Relation/Insert/Update/Delete; Truncate/Type/Origin rejected), 10 scalar types, protobuf `EntityKey`/`EntityDifference` records with `crabka.pg.{table,lsn,operation}` headers, auto slot+publication, LSN peek-then-advance resume (`pgoutput.rs:64-105`, `source.rs:264-315`, `offset.rs`). **Zero physical-WAL machinery exists anywhere** — no `XLogRecord` parsing, no `START_REPLICATION PHYSICAL`, no walreceiver (repo-wide grep confirmed). The CDC integration story is real; the storage half is at zero.
2. **The approved WAL substrate is payload-blind but offset-sequenced — and spec-only.** `WalStore::append_durable` takes verbatim opaque bytes; `QuorumStateMachine`'s `LogView` crosses only offset/epoch metadata; the flush-to-bucket + `__diskless_wal_index` path records only `(first_offset, last_offset, byte_start, byte_len)` — all of it reusable by a Postgres WAL group *unchanged*. But the addressing model is strictly gap-free sequential offsets (no LSN/byte addressing), and none of it is implemented (`crates/broker/src/wal/` absent).
3. **The blockstore's infrastructure is reusable; its data model is not.** Object-store plumbing (`ObjectStoreConfig`/`build_object_store`/`ObjectOps`), Parquet streaming, and index-snapshot persistence patterns carry over. The data model — wall-clock-time-pruned, scan-oriented, latest-only append — is exactly wrong for versioned pages: no LSN indexing, no point-read-by-key, no as-of visibility.

## The structural insight that orders the slices

**Redo belongs to the read path, not ingest** (Neon's actual architecture). Delta layers store raw per-page WAL records; image layers store materialized pages; `get_page@LSN` materializes a page as *(latest image ≤ LSN) ⊕ (redo over deltas ≤ LSN)*. So decode/shard (ingest) and redo (read) are separate slices, and the layer store sits between them.

## The slices

### PG-1 — Safekeeper ingest (physical WAL → quorum WAL group)
A walreceiver front-end speaking `START_REPLICATION … PHYSICAL` to a **stock, unpatched** Postgres primary; the byte-addressed LSN stream wrapped as sequential opaque records (LSN range carried per record) appended via `append_durable` into a per-database WAL group; an LSN→offset index for reads; standby feedback (`restart_lsn` advance) driven by the quorum-durable watermark so the primary can recycle WAL. **Reuses:** the payload-blind kernel, the flush/index path — unchanged. **Net-new:** the replication-protocol client, the LSN↔offset framing adapter, the feedback loop. **Gated on** the diskless WAL slices (1–6, spec-only); the front-end + framing can be built earlier against the slice-1 `LocalFsyncWal` shape.

### PG-2 — WAL decode + page-shard *(the first spec — no unbuilt prerequisites)*
A sans-IO `XLogRecord` parser over an LSN-addressed byte stream (segment/page framing, contrecords, CRC-32C, block references, FPIs) and a page-shard router keying decoded records by `(RelTag, block) @ LSN` — the exact shape PG-3's delta layers ingest. Differentially verified against `pg_waldump` over fixture WAL. 100% net-new, pure Rust, buildable today.

### PG-3 — Layer store (versioned pages on the bucket)
Image + delta layer *formats*, the layer map, LSN-visibility queries ("layers covering key K at LSN L"), ingest with idempotent flush, and **structural** (non-materializing) L0→L1 compaction. Reuses object-store plumbing and index-persistence *patterns*; the versioned-page data model is net-new (grounded: the blockstore's cannot express it). Developable today atop PG-2's output + a local/`InMemory` bucket. *(Refined during the PG-3 design: image-layer **creation** and GC require redo, so they live in PG-4.)*

### PG-4 — Redo + `get_page@LSN` + page service (+ materializing compaction, GC)
Materialization: (latest image ≤ LSN) ⊕ (deltas ≤ LSN via redo), served over a pagestream RPC — plus image-layer creation and GC (both need redo; re-homed from PG-3). **Carries the chapter's crux decision** — sandboxed Postgres walredo sidecar vs native Rust redo vs Neon's hybrid — presented as approaches in its own design cycle. Differential page-image verification against stock Postgres.

### PG-5 — Compute integration
The patched-Postgres compute image (`smgr` → pagestream), one maintained patch set per supported PG major (containerized — operationally an OCI image, like Neon's compute). **The keystone conformance gate:** pgbench boots and runs against the disaggregated stack (PG-1 ingest live), with differential page-image checks vs stock Postgres — the PG-differential analogue of the JVM-differential culture.

### PG-6 — Branching / PITR
Copy-on-write timelines at LSN via layer-map indirection. Pure layer-store leverage once PG-3/PG-4 exist.

## Dependency graph & the two-track schedule

```
diskless WAL slices 1–6 (separate track, spec-only) ──► PG-1 ──┐
                                                               ├──► PG-5 (end-to-end gate) ──► PG-6
fixture WAL (stock PG) ──► PG-2 ──► PG-3 ──► PG-4 ─────────────┘
```

**PG-2 → PG-3 → PG-4 — the hard 80% — have no unbuilt prerequisites.** They develop against fixture WAL and a local bucket while the diskless WAL slices land independently; PG-1 joins the tracks when the substrate exists. This is what makes the "multi-quarter net-new IP" schedule honest rather than serialized behind the WAL program.

## Honest framing (carried from the vision doc, non-negotiable)

- **Never pitch "parity-or-behind Neon"** — that implies a working product; this chapter is *at the starting line* on the storage half. The pageserver + `smgr` is Neon's hard core IP, rebuilt here.
- **The moat is shared-substrate co-location, never database quality:** pages on the same bucket and same quorum-WAL groups as topics/blobs/lakehouse; CDC-as-a-topic already landed (`connect-postgres`); post-Ch.2, an Iceberg table of every DB change for free.
- **DB-column-typed filtering (Chapter G's DB half) and RLS-aware realtime (Chapters D/E)** ride this chapter's compute — they stay gated, not promised.

## Resolved decisions

- **Slice order:** decode → layers → redo (redo is a read-path concern); safekeeper on the parallel WAL-gated track.
- **PG-1 targets stock Postgres** (physical replication client) — no compute patching until PG-5.
- **First spec: PG-2**, with the walredo crux deferred to PG-4's own cycle.
- **Differential culture:** `pg_waldump` is PG-2's oracle; stock-Postgres page images are PG-4/PG-5's.
