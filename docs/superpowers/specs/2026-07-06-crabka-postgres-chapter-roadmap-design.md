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

### PG-1 — Safekeeper ingest (physical WAL → an internal topic)
A standalone component speaking `START_REPLICATION … PHYSICAL` to a **stock, unpatched** Postgres primary and **producing** contiguity-guarded `PGW1`-framed WAL records to an internal topic `__pg_wal.<cluster>` with `acks=all` over the ordinary Kafka wire — zero broker changes. *(Refined during the PG-1 design — the WAL-slice gate dissolved: the produce path IS the `WalStore::append_durable` path once diskless slice 1 lands, entered over the wire; the durability tier is inherited from the topic and upgrades transparently — classic today (dev-grade, documented), fsync at slice 1, quorum at 6a — with no safekeeper code change. The LSN→offset index moved out to the future live-pageserver-ingest slice; feedback is tier-qualified.)* **Buildable today**; gate: the stored stream decodes cleanly through PG-2's decoder.

### PG-2 — WAL decode + page-shard *(the first spec — no unbuilt prerequisites)*
A sans-IO `XLogRecord` parser over an LSN-addressed byte stream (segment/page framing, contrecords, CRC-32C, block references, FPIs) and a page-shard router keying decoded records by `(RelTag, block) @ LSN` — the exact shape PG-3's delta layers ingest. Differentially verified against `pg_waldump` over fixture WAL. 100% net-new, pure Rust, buildable today.

### PG-3 — Layer store (versioned pages on the bucket)
Image + delta layer *formats*, the layer map, LSN-visibility queries ("layers covering key K at LSN L"), ingest with idempotent flush, and **structural** (non-materializing) L0→L1 compaction. Reuses object-store plumbing and index-persistence *patterns*; the versioned-page data model is net-new (grounded: the blockstore's cannot express it). Developable today atop PG-2's output + a local/`InMemory` bucket. *(Refined during the PG-3 design: image-layer **creation** and GC require redo, so they live in PG-4.)*

### PG-4 — Redo + `get_page@LSN` + page service (+ materializing compaction, GC)
Materialization: (latest image ≤ LSN) ⊕ (deltas ≤ LSN via redo), plus image-layer creation and GC (both need redo; re-homed from PG-3). **Both crux decisions resolved in PG-4's cycle:** redo is **full native Rust** (bounded v1 rmgr set — XLOG-FPI/HEAP/HEAP2/BTREE/SEQ — with loud `UnsupportedRmgr` refusal; the correctness mechanism is a byte-exact differential against a **WAL-replayed standby**, redo-vs-redo); the page service is a **Crabka-native Connect RPC** (`GetPage`/`GetRelSize`), not Neon-pagestream-compatible. **PG-4b** (follow-on): the index-rmgr long tail (GIN/GiST/SP-GiST/BRIN/hash), SLRU/CLOG materialization, and SMGR-record interpretation — required before PG-5's boot gate.

### PG-5 — Compute integration
Structured as **PG-5a** (pageserver readiness: timeline seeding from an `initdb` import, live topic ingest, LSN-wait on `GetPage`, a `Basebackup` RPC) + **PG-5b** (the compute image: a minimal vendored smgr-hook patch over pinned PG-17 sources — no forked repo — plus a C extension whose smgr calls **`crabka-compute-client`, a Rust cdylib behind a C ABI**; that client shape was **resolved in PG-5's cycle** and establishes the workspace's one sanctioned `unsafe` boundary — one crate, one `ffi.rs`, cbindgen, style-guide-codified). The compute is a stock-shaped primary (FPW on); PG-1's safekeeper attaches unchanged, closing the WAL loop. **The keystone conformance gate:** pgbench boots and runs against the disaggregated stack (Tier 1, **blocked on PG-4b** for SLRUs), then page-image differential vs stock Postgres at matched LSNs (Tier 2) — the PG-differential analogue of the JVM-differential culture.

### PG-6 — Branching / PITR
Copy-on-write timelines at LSN via layer-map indirection. Pure layer-store leverage once PG-3/PG-4 exist.

## Dependency graph & the two-track schedule

```
PG-1 (buildable today; durability tier inherits from the topic —
      classic now, fsync at diskless slice 1, quorum at 6a) ──┐
                                                              ├──► PG-5 (end-to-end gate, needs PG-4b) ──► PG-6
fixture WAL (stock PG) ──► PG-2 ──► PG-3 ──► PG-4 ────────────┘
```

**Every slice through PG-4 — including PG-1 after its design refinement — has no unbuilt prerequisites.** PG-2→3→4 develop against fixture WAL and a local bucket; PG-1 produces over the ordinary Kafka wire and inherits durability upgrades from the diskless track without code changes. This is what makes the "multi-quarter net-new IP" schedule honest rather than serialized behind the WAL program.

## Honest framing (carried from the vision doc, non-negotiable)

- **Never pitch "parity-or-behind Neon"** — that implies a working product; this chapter is *at the starting line* on the storage half. The pageserver + `smgr` is Neon's hard core IP, rebuilt here.
- **The moat is shared-substrate co-location, never database quality:** pages on the same bucket and same quorum-WAL groups as topics/blobs/lakehouse; CDC-as-a-topic already landed (`connect-postgres`); post-Ch.2, an Iceberg table of every DB change for free.
- **DB-column-typed filtering (Chapter G's DB half) and RLS-aware realtime (Chapters D/E)** ride this chapter's compute — they stay gated, not promised.

## Resolved decisions

- **Slice order:** decode → layers → redo (redo is a read-path concern); safekeeper on the parallel WAL-gated track.
- **PG-1 targets stock Postgres** (physical replication client) — no compute patching until PG-5.
- **First spec: PG-2**, with the walredo crux deferred to PG-4's own cycle.
- **Differential culture:** `pg_waldump` is PG-2's oracle; stock-Postgres page images are PG-4/PG-5's.
