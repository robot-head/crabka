# PG-2: Postgres WAL decode + page-shard — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. First buildable slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — the ingest half of the pageserver track, with **no unbuilt prerequisites**.

## Context — where this sits

PG-2 builds the parser every downstream slice consumes: a **sans-IO decoder** for Postgres physical WAL (the `XLogRecord` stream) plus a **page-shard router** that keys decoded records by `(relation, block) @ LSN` — the exact shape PG-3's delta layers ingest and PG-4's redo replays. Nothing like it exists in-tree (verified: no `XLogRecord`/walreceiver/physical-replication code anywhere; `connect-postgres` is logical-only). It is 100% net-new, pure Rust, and buildable today against fixture WAL from a stock Postgres — which is what lets the hard-80% track start without waiting on the diskless WAL slices.

**Deliberately rmgr-agnostic.** The decoder extracts the record *envelope* — `(xid, rmid, info, block references, full-page images, data payloads)` — without interpreting resource-manager semantics (heap vs btree vs …). Interpreting records is **redo**, which is PG-4's read-path concern. This boundary is what keeps PG-2 small and finishable.

## Design Goals

- **Byte-faithful decode of the physical WAL stream:** segment/page framing (short + long page headers), records spanning pages and segments (contrecords), CRC-32C validation, and the full record body grammar (block headers, `RelFileLocator`, block numbers, FPI headers with hole reconstruction, short/long main-data headers).
- **Sans-IO pull parser:** feed LSN-addressed bytes in, poll complete records out — so the same decoder serves fixture segment files today and PG-1's record-wrapped quorum stream later, unchanged.
- **Page-shard routing:** each decoded record fans out per block reference to `(PageKey { RelTag, blkno }, lsn)`-keyed entries; records with no block references (commit, checkpoint, standby…) route to a pass-through metadata lane keyed by rmgr. PG-2 routes; PG-3 stores.
- **Differential verification:** decode output is checked field-by-field against `pg_waldump` over the same fixture segments — Crabka's differential culture applied to Postgres.

## Non-goals

- **Redo / rmgr semantics** — PG-4. PG-2 never materializes a page.
- **Compressed FPIs** (`wal_compression = pglz|lz4|zstd`) — deferred; the default is `off`, fixtures pin it off, and the decoder rejects compressed images with a clear error naming the constraint. Hole reconstruction (default behavior) **is** in scope.
- **Live replication ingest** (`START_REPLICATION`, keepalives, feedback) — PG-1.
- **Multi-version support** — v1 pins one Postgres major (**17**, the current stable); the page-header magic is validated against it and decode constants are structured per-major so a second major is additive later.
- **Layer storage, LSN→offset indexing, timelines** — PG-3 / PG-1.

## Architecture Overview

```
crates/postgres-wal  (crabka-postgres-wal — new, sans-IO, no tokio)
│
├── Lsn                u64 newtype; segment/page arithmetic; Display "X/Y" form
├── WalStreamDecoder   sans-IO pull parser:
│     feed(&mut self, lsn: Lsn, bytes: &[u8])          // LSN-addressed input
│     poll_record(&mut self) -> Option<DecodedRecord>  // complete records out
│   framing: XLogPageHeader (short/long), xlp_magic check, contrecord
│   reassembly (XLP_FIRST_IS_CONTRECORD / xlp_rem_len), CRC-32C validation
│
├── DecodedRecord { start_lsn, end_lsn, xid, rmid, info,
│                   blocks: Vec<BlockRef>, main_data: Bytes }
│   BlockRef { rel: RelTag { spc_oid, db_oid, rel_number, fork }, blkno: u32,
│              image: Option<PageImage /* 8 KB, hole re-zeroed */>, data: Bytes }
│
└── shard_record(rec) -> impl Iterator<Item = Sharded>
      Sharded::Page { key: PageKey(RelTag, blkno), lsn, blk_idx, rec: Arc<DecodedRecord> }
      Sharded::Meta { rmid, lsn, rec }                 // commit/checkpoint/… pass-through
```

## Key Design Decisions

### Sans-IO pull parser over an LSN-addressed byte stream

The decoder owns no I/O: callers feed byte runs tagged with their starting LSN and poll for completed records. This is the same discipline as `kraft-core`'s sans-IO consensus and makes the two input paths trivially symmetric — a fixture-file reader now, PG-1's `read_raw`-style verbatim record runs later. Contrecord reassembly (a record whose tail continues on the next page/segment under `XLP_FIRST_IS_CONTRECORD` + `xlp_rem_len`) is internal buffering, invisible to callers. *Alternative rejected:* an async reader-driven decoder — welds the parser to an I/O model before PG-1 exists and makes fixture-based differential tests heavier.

### rmgr-agnostic envelope decode

The record body grammar (block headers `0..=32`, `BKPBLOCK_*` flags, `SAME_REL` folding, `XLogRecordBlockImageHeader`, short/long data headers) is **uniform across all resource managers** — only the *interpretation* of payloads differs per rmgr. Decoding the envelope for every rmgr costs nothing extra and gives PG-3 complete per-page delta streams from day one; interpreting payloads is deferred to PG-4's redo. FPI hole reconstruction (re-zeroing `hole_offset..hole_offset+hole_length` into a full 8 KB image) is envelope work and lands here.

### Page-shard routing is the module's output contract

`(PageKey, lsn) → record` is precisely the delta-layer ingest key PG-3 needs and the replay key PG-4's redo consumes; block-less records (commit — which PG-3+ needs for visibility, checkpoint, standby/running-xacts) flow through a metadata lane keyed by rmgr rather than being dropped — routing policy stays here, storage policy stays downstream. A multi-block record (e.g. a btree split touching 2–3 pages) fans out once per block reference sharing one `Arc<DecodedRecord>`.

### Version pinning with per-major structure

`xlp_magic` is validated against the pinned major (PG 17); header sizes/flag constants live in a per-major constants module so adding PG 18 later is a new table, not a rewrite. Fixtures are generated by the pinned major and record it in their manifest.

### Differential oracle: committed `pg_waldump` output

Fixtures are generated once by a script (stock `initdb --wal-segsize=1` + crafted traffic) and **committed** — small 1 MB segments plus the corresponding `pg_waldump` text — so CI verifies the decoder hermetically (no Postgres in CI): parse the committed segments, compare `(lsn, rmgr, tot_len, block refs (rel/fork/blk), FPI presence)` line-for-line against the committed oracle text. The generation script requires a local Postgres 17 and is re-run only to extend the corpus. The crafted traffic must cover: multi-block records (index splits), a contrecord crossing a **segment** boundary, FPIs with and without holes, commit records, and `wal_level=replica` defaults.

## Integration

- **`crates/postgres-wal`** (new) — `crabka-postgres-wal`; **`publish = false` + a private release-plz entry** (the publication-allowlist gate applies to every new crate).
- **Consumes:** fixture segment files now; PG-1's verbatim LSN-framed record runs later (same `feed` seam).
- **Produces for PG-3:** the `Sharded::Page`/`Meta` stream — the delta-layer ingest contract.
- **Reuse check:** CRC-32C — reuse `crabka-protocol`'s Castagnoli implementation if exported; else the `crc32c` crate (workspace-pinned). No tokio dependency (sans-IO).

## Kafka / wire compliance

Not a Kafka-wire surface — but the same byte-exactness discipline applies to the *Postgres* wire: the decoder must accept exactly what Postgres 17 emits (validated differentially via `pg_waldump`), and fixture bytes are never hand-crafted — always generated by a real Postgres.

## Testing

- **Framing units:** long header at segment start (sysid/seg-size/blcksz echoed), short headers thereafter; wrong-magic → clear versioned error.
- **Contrecord reassembly:** a record split across pages and across the 1 MB segment boundary reassembles byte-identically; a truncated tail yields "incomplete", not a panic.
- **CRC:** a corrupted byte anywhere in a record fails validation with the offending LSN.
- **Body grammar:** multi-block record fans out N `BlockRef`s with correct `RelTag`/fork/blkno; `SAME_REL` folding; FPI hole re-zeroed to exactly 8 KB; short vs long main-data headers.
- **Shard routing:** multi-block → N `Sharded::Page` sharing one `Arc`; commit record → `Sharded::Meta`; ordering by LSN preserved per key.
- **Differential (the gate):** full committed-fixture parse matches the committed `pg_waldump` oracle line-for-line; compressed-FPI fixture (generated separately) is rejected with the documented error.

## Risks (carried into the plan)

- **Grammar fidelity:** the body grammar has fiddly alignment/ordering details (block-header sequence, image sub-headers, data-length accounting). Mitigated by the line-for-line `pg_waldump` differential — any drift fails loudly.
- **Fixture size in git:** 1 MB segments × a handful — comparable to the existing protocol corpus; noted, accepted.
- **CRC polynomial/seed exactness:** PG's CRC-32C covers body-then-header-prefix in a specific order; encoded exactly in the plan and cross-checked by the differential (a wrong CRC recipe fails every record).
- **Compressed-FPI deferral:** deployments running `wal_compression=on` are rejected loudly until the pglz/lz4/zstd decode lands — a documented constraint, not a silent gap.

## Resolved decisions

- **Scope:** envelope decode + shard routing only; redo is PG-4; live ingest is PG-1.
- **Parser:** sans-IO pull (`feed`/`poll_record`), LSN-addressed input.
- **Version:** PG 17 pinned; per-major constants structure.
- **FPIs:** hole reconstruction in scope; compressed images deferred with a loud error.
- **Oracle:** committed fixtures + committed `pg_waldump` text; hermetic CI; regeneration script needs local PG.
- **Crate:** `crates/postgres-wal`, `publish = false`, no tokio.
