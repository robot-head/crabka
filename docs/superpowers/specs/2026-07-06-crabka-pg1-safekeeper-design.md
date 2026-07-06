# PG-1: The safekeeper — physical WAL ingest — design

**Date:** 2026-07-06
**Status:** Approved
**Type:** Subsystem design. The ingest slice of the [Chapter C roadmap](2026-07-06-crabka-postgres-chapter-roadmap-design.md) — a stock, unpatched Postgres primary streams its physical WAL into a Crabka topic, durably, with correct feedback.

## Context — where this sits, and a roadmap correction

PG-1 plays Neon's safekeeper role: speak `START_REPLICATION … PHYSICAL` to a **stock Postgres 17** primary, land the byte-addressed LSN stream durably, and report `flushed_lsn` back so the primary can recycle WAL. The roadmap originally gated PG-1 on the unbuilt diskless-WAL slices, assuming an in-broker `WalStore` linkage. Designing it dissolved that gate:

**The safekeeper is a standalone component that *produces* framed WAL records to an internal topic with `acks=all` over the ordinary Kafka wire.** The produce path *is* the `WalStore::append_durable` path once diskless slice 1 lands (produce → partition writer → `append_durable`) — entered over the wire instead of by linking the broker. Consequences:

- **Zero broker changes; buildable today** against the landed broker + `crabka-client-producer`.
- **The durability tier is inherited and upgrades transparently:** classic topic today (`acks=all` = replicated, page-cache durability), fsync-durable when slice 1 lands, fsync-quorum at 6a — with **no safekeeper code change**. `flushed_lsn` feedback is therefore *tier-qualified*: production use of the feedback (letting the primary discard WAL) is gated on slice 1+, and the docs say so.
- **Flush-to-bucket, indexing, and Fetch-consumption come free** — the WAL group is just a topic; PG-3's future live-ingest is a consumer.

The roadmap doc is updated alongside this spec.

## Design Goals

- **Stock-primary ingest:** physical replication protocol (`IDENTIFY_SYSTEM`, physical slot create/reuse, `START_REPLICATION`, CopyBoth: `XLogData`/keepalive in, standby-status-update out). No compute patching (that's PG-5).
- **Contiguous, self-describing framing:** each produced record's value is `[magic "PGW1" | start_lsn u64 LE | wal bytes]`; chunking aligned to `XLogData` boundaries (≤ 512 KiB targets); **contiguity enforced** — the next record's `start_lsn` must equal the previous `end_lsn`, at produce time and at restart.
- **Correct feedback:** `write_lsn` = highest enqueued, `flushed_lsn` = highest **acked** end-LSN, sent on keepalive-reply-requested and on a timer; a physical slot pins WAL on the primary until confirmed.
- **Crash-safe resume:** on restart, read the topic tail for the last frame's `end_lsn` and resume `START_REPLICATION` there — the slot guarantees availability.
- **PG-2 as the validity oracle:** the stored stream, consumed back and fed through `crabka-postgres-wal`'s decoder, must decode cleanly (framing continuity, record CRCs) across every chunk boundary — the slice gate.

## Non-goals

- **LSN→offset random-access index** — dropped from PG-1 (the roadmap listed it): sequential consumption plus tail-scan resume covers everything this slice serves; a random-LSN seek index is deferred to the live pageserver-ingest slice that will actually need it.
- **Timeline switches / failover of the primary** — v1 is single-timeline; a timeline change halts with a clear error.
- **WAL trimming** on the topic (needs the pageserver's `disk_consistent_lsn` handshake — deferred), **multi-cluster management**, **safekeeper HA** (one instance per cluster v1), **logical anything** (`connect-postgres` owns that).
- **Fsync/quorum durability itself** — inherited from the topic tier (diskless slices); PG-1 neither implements nor blocks on it.

## Architecture Overview

```
stock Postgres 17 primary
  └─ replication conn (replication=true): IDENTIFY_SYSTEM → slot crabka_sk_<cluster> (physical,
     create-if-missing) → START_REPLICATION … PHYSICAL <resume_lsn> TIMELINE <tli>
       CopyBoth stream:
         XLogData('w': wal_start, wal_end, bytes) ──► chunker (XLogData-aligned, ≤512 KiB, contiguity guard)
         keepalive('k': end, reply?)              ──► feedback scheduler
         ◄── standby status('r': write, flush, apply=flush, reply?)   flush = highest ACKED end_lsn
                                    │
                                    ▼
  crabka-client-producer, acks=all ──► internal topic  __pg_wal.<cluster>  (1 partition)
       record value = PGW1 | start_lsn | wal bytes      (v2 batch framing = free flush/index/Fetch reuse)
                                    │ acks (offsets) ──► flushed_lsn advance
  restart: client-consumer reads the tail → last end_lsn → resume_lsn
  gate:   consume all → crabka-postgres-wal decoder → clean decode across chunk boundaries
```

## Key Design Decisions

### Produce-path ingest (the gate-dissolving decision)

The safekeeper writes through the broker's front door instead of linking its internals: `acks=all` produce to `__pg_wal.<cluster>`. Rationale: (1) it reaches the *same* `append_durable` seam the in-broker design would, once slice 1 lands, because that seam sits on the partition-writer path the wire already drives; (2) it decouples PG-1's schedule from the diskless program entirely; (3) topic-ness buys flush/index/Fetch for free and makes the WAL stream observable with stock tooling. *Alternative rejected — in-broker `WalStore` linkage:* couples PG-1 to unlanded code, adds a broker surface, and buys nothing the wire doesn't.

### Tier-qualified feedback honesty

`flushed_lsn` tells the primary "you may discard this WAL." Its truth equals the topic's durability tier: today `acks=all` means replicated-not-fsynced; slice 1 makes it fsync-durable; 6a quorum-fsync. PG-1 reports acked offsets as flushed **and documents the tier dependency loudly** — the code is final, the *claim* strengthens as the substrate lands. Until slice 1, deployments are dev-grade by definition and the docs say exactly that.

### Framing: self-contained values, XLogData-aligned chunks

The frame carries its own `start_lsn`; `end_lsn = start + len`. Chunk boundaries align to `XLogData` message boundaries (never mid-message), which need **not** align to WAL record boundaries — PG-2's decoder reassembles contrecords across arbitrary run splits (designed for exactly this seam). The contiguity guard (next start == prev end) is checked before every produce and re-verified during restart tail-reads; a violation halts (a gap in stored WAL is unrecoverable corruption of the stream's meaning).

### Replication connection: `tokio-postgres` first, minimal wire fallback

`connect-postgres` already depends on workspace `tokio-postgres 0.7`, which provides `copy_both_simple`. **Verify-first step in the plan:** whether the workspace version supports the `replication=true` startup parameter; if it does not, the fallback is a minimal in-crate replication session (startup message + auth + CopyBoth submessages over `postgres-protocol` primitives) — a small pgwire subset, squarely in the culture of a codebase that implements the Kafka protocol from scratch. Only the connection setup differs; the message parsing (`XLogData`/keepalive/status-update) is ours either way.

### The PG-2 decode gate

The end-to-end proof: run real traffic on a containerized PG 17, let the safekeeper ingest it, then consume `__pg_wal.<cluster>` and feed the reassembled byte stream through `crabka-postgres-wal::WalStreamDecoder`. Every record must decode with valid CRCs and contiguous LSNs across every chunk boundary — the two crates verify each other (the decoder was designed for LSN-addressed runs; the safekeeper produces them).

## Integration

- **`crates/safekeeper`** (new, `crabka-safekeeper`) — **`publish = false` + private release-plz entry**. Deps: `crabka-client-producer` (acks=all produce), `crabka-client-consumer` (tail resume + the gate), `crabka-client-admin` (ensure-topic), `tokio-postgres`/`postgres-protocol`, `bytes`, `thiserror`, `tokio`. Dev-dep: `crabka-postgres-wal` (the decode gate).
- **Topic convention:** `__pg_wal.<cluster_id>`, 1 partition, created if missing (`CreateTopicSpec`); retention effectively infinite until the trim slice.
- **Upgrades free:** slice 1 / 6a change the topic's durability tier, not this crate.

## Kafka / wire compliance

- **The safekeeper is a normal Kafka-wire client** — produce/consume/admin over the public protocol; nothing broker-internal.
- **Postgres wire:** the replication protocol subset is implemented against PG 17's documented streaming-replication protocol and tested against a real PG 17 (never a mock).

## Testing

- **Frame codec + chunker units:** round-trip; XLogData-boundary alignment; contiguity guard rejects a gap and an overlap; ≤ target-size splitting.
- **Protocol message units:** parse `XLogData`/keepalive from captured bytes; encode standby-status-update byte-exactly (field order, LSN+1 conventions per protocol docs).
- **Integration (containerized PG 17 via the workspace `testcontainers`/`testcontainers-modules` pattern — the harness schema-registry et al. already use; `connect-postgres` itself has no container harness):** slot created idempotently; traffic streams into the topic; keepalive reply-requested answered; `flushed_lsn` advances only with acks.
- **Restart/resume:** kill mid-stream; restart; tail-read resume produces **no gap and no overlap** (contiguity verified across the restart seam); the slot retained WAL.
- **The decode gate (the slice gate):** full consume → `WalStreamDecoder` → every record CRC-valid, LSN-contiguous, across all chunk and restart boundaries.
- **Timeline-switch halt:** a promoted-standby fixture (or forged tli in a unit test) halts with the documented error.

## Risks (carried into the plan)

- **`tokio-postgres` replication support** — the verify-first step; the minimal wire fallback is scoped and named. Auth for v1 fixtures: password/trust (SCRAM via `postgres-protocol` if needed).
- **Feedback truthfulness before slice 1** — tier-qualified, documented; the primary keeps WAL via the slot regardless, so the worst case pre-slice-1 is re-streaming after a Crabka data loss, not primary corruption.
- **Unbounded topic growth** until the trim slice — retention documented as operator-managed v1.
- **Single-safekeeper availability** — an outage pauses ingest (the slot pins WAL; primary disk grows) — stated operational constraint until an HA slice.

## Resolved decisions

- **Ingest path:** standalone producer over the Kafka wire (`acks=all`, `__pg_wal.<cluster>`); zero broker changes; durability tier inherited — the WAL-slice gate dissolved (roadmap updated).
- **Framing:** `PGW1 | start_lsn | bytes`, XLogData-aligned ≤ 512 KiB chunks, contiguity enforced; no LSN index in this slice.
- **Feedback:** flush = acked end-LSN, tier-qualified; physical slot `crabka_sk_<cluster>`; single timeline, halt on switch.
- **Resume:** tail-read on restart.
- **Gate:** the stored stream decodes cleanly through PG-2's decoder.
- **Crate:** `crates/safekeeper`, `publish = false`.
