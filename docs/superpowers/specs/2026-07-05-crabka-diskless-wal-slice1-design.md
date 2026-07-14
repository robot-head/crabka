# Diskless WAL — Slice 1: WAL seam + durability abstraction — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (first slice of a 6-slice milestone). Slice 1 is a **scaffolding slice** — it installs the seam and the durability-boundary move; it does **not** ship diskless on its own.

## Context — where this sits

The [north-star roadmap](2026-07-05-crabka-north-star-roadmap-design.md) Chapter 1 (flagship) is a diskless / object-storage-native broker; its Milestone 1 is a shared low-latency WAL. That milestone decomposes into six independently-specifiable slices:

1. **WAL seam + durability abstraction (this spec)** — the `WalStore` seam + moving the `acks=all` boundary off ISR-HW onto WAL-durability, backed by the simplest medium. Proves Delta A (an acked record never becomes a loss).
2. KRaft-assigned offset sequencer for diskless partitions.
3. Shared object-store flush + object/offset-index layout.
4. Hot-tail read cache + diskless fetch path.
5. Crash-mid-flush atomicity + stateless-broker recovery.
6. Quorum-durable multi-AZ WAL + concurrent-sequencer proof + re-composed end-to-end gate + Jepsen.

This spec covers **Slice 1 only**. The later slices are named here as explicit non-goals so the seam is designed to receive them.

The whole diskless produce path swaps in **behind one narrow channel**. The wire handler `process_partition` ([`crates/broker/src/handlers/produce.rs`](../../../crates/broker/src/handlers/produce.rs)) sends each batch down an mpsc `writer_tx`, awaits an `Offset`, and for `acks=all` blocks until a watermark reaches `base_offset + last_offset_delta + 1` (`produce.rs:778-784`; `ACKS_ALL = -1` at `produce.rs:45`). Slice 1 changes *what makes a record durable* and *what advances that watermark* **behind that channel** — the wire-facing handler stays byte-identical.

## Design Goals

Slice 1 exists to de-risk the load-bearing seam and the central durability invariant **before** the hard slices (offsets, flush, quorum) build on them. Concretely it must:

- **Install a `WalStore` seam** behind `writer_tx` that abstracts "assign an offset, make these bytes durable, and expose a monotonic durable watermark," so later slices swap the durable medium without touching the wire path or the ack gate.
- **Move the `acks=all` boundary** for a diskless-mode topic off the ISR high-watermark (page-cache presence across the ISR) onto a **WAL durable-commit** watermark, reusing the existing `ReplicaState` HWM machinery as the client-facing surface (only the *advance source* changes).
- **Deliver one concrete, testable new semantic:** a diskless-mode `RF=1, acks=all` topic becomes **crash-safe** (durable by `fsync`), where a classic `RF=1, acks=all` topic is crash-*unsafe* (page-cache only, `LogConfig::flush_on_append` defaults to `false` — `crates/log/src/config.rs:45,90`).
- **Establish the Delta A proof:** extend the stateright durability model with a `wal_acked` ghost distinct from `committed`, and machine-check that a `wal_acked` record is never lost.

### Non-goals (Slice 1)

- No object storage, no shared/batched objects, no offset→object index (Slice 3).
- No KRaft-assigned offsets — offsets stay locally assigned via `Log::log_end_offset()` (Slice 2).
- No change to the Fetch read path — bytes stay in the local `Log` where Fetch already reads them (Slice 4).
- No quorum / multi-AZ durability — the single-node medium does **not** claim to survive node/disk *loss*, only crash-restart (Slice 6).
- No crash-mid-flush atomicity, no stateless-broker recovery (Slice 5).
- No Creusot kernel this slice — stateright only.

## Architecture Overview

Everything lands behind the existing narrow channel; the wire-facing handler and the ack gate are unchanged.

```
process_partition (produce.rs)                    ← WIRE-FACING, UNCHANGED (byte-exact)
      │  writer_tx.send(ProduceJob{ data, ack })              partition.rs:50-69, :193
      ▼
partition_writer::run                             ← branch on per-topic `diskless` flag
      ├── classic topic:  Log::append(+ISR-HW)    ← UNCHANGED (partition_writer.rs:101, :250-264)
      └── diskless topic: WalStore::append_durable(part, ProduceData)
                                │  assigns Offset (local, log_end_offset in slice 1)
                                │  persists durably (LocalFsyncWal: append to Log + fsync)
                                ▼
                          ReplicaState durable watermark advance
                          (recompute_hw_for_wal_durable — sibling of
                           recompute_hw_for_leader_append; hybrid reuse)
                                │  fires hw_advance_notify
                                ▼
finalize_ack: await_hw_at_least(base+delta+1)     ← UNCHANGED gate (produce.rs:778-784,
                                                     partition.rs:538)
```

### The `WalStore` seam

A new broker-internal module (`crates/broker/src/wal/`, `mod.rs` + `local_fsync.rs`) defines:

- **`trait WalStore` (async, `Send + Sync`):** `append_durable(&self, partition, ProduceData) -> Result<WalAppend, BrokerError>`, returning the assigned `base_offset` and driving a **monotonic per-partition durable-offset watermark**. The trait's contract is: *offsets it returns are strictly increasing and gap-free per partition; the durable watermark advances to `offset` only once that offset's bytes are durable; the watermark never regresses.* This is the seam every later slice re-implements (replicated WAL, object-store flush) without changing its callers.
- **`struct LocalFsyncWal` (Slice-1 impl):** wraps the partition's existing `Arc<Mutex<Log>>`. `append_durable` appends the verbatim/owned batch via `Log::append_verbatim` / `Log::append` (`crates/log/src/log.rs:522,479` — offsets from `log_end_offset()`, `log.rs:394`), then `fsync`s the active segment (`Segment::flush` → `sync_data`, `crates/log/src/segment.rs:939-940`), then advances the durable watermark. Because the bytes are written to the same local `Log` a classic topic uses, **the Fetch path is untouched this slice.**

The `partition_writer::run` group-commit loop (`crates/broker/src/partition_writer.rs`) keeps its batching shape (one lock, one commit, one watermark advance, one `hw_advance_notify` per group — `partition_writer.rs:250-264`); the diskless branch substitutes the append+durability step with a `WalStore` call and advances the watermark from WAL-durable rather than leader-append LEO.

### Hybrid HWM reuse

`ReplicaState` (`crates/broker/src/replica_state.rs`) remains the single client-facing watermark surface. Today its HW advances from leader append (`recompute_hw_for_leader_append`) and follower fetch LEO updates (`crates/broker/src/handlers/fetch.rs:403-412`). Slice 1 adds a sibling advance path — `recompute_hw_for_wal_durable(offset)` — driven by `WalStore`'s durable watermark. For a diskless `RF=1` topic the ISR is `{leader}`, so HW = leader's WAL-durable offset; `await_hw_at_least` (`crates/broker/src/partition.rs:538`) resolves exactly when that offset is `fsync`-durable. Client-observable HWM/ISR (Fetch, Metadata, admin) stay byte-exact because they read the same `ReplicaState`.

## Key Design Decisions

### Everything lands behind `writer_tx`; the wire path is untouched

`process_partition` and `finalize_ack` already only assign/await an `Offset` then gate on a monotonic watermark — they do not care *how* durability is achieved. Slice 1 changes nothing above the channel, so Kafka wire-compat (Produce/Fetch/Metadata byte-exactness) is preserved by construction. *Alternative rejected:* a parallel diskless produce handler — needless duplication of the wire-facing gate and a second thing to keep byte-identical.

### `WalStore` abstracts durability, not storage layout

The trait's contract is offset-assignment + a durable watermark, deliberately *not* "where the bytes live." That lets Slice 3 (object-store flush) and Slice 6 (quorum medium) reimplement durability without the writer, the ack gate, or the model composition changing. *Alternative rejected:* threading object-store/flush concepts into the seam now — over-couples Slice 1 to decisions (object layout, offset→object index) that belong to later slices.

### `LocalFsyncWal` reuses the local `Log`; offsets stay local

Writing to the existing `Log` (rather than a separate store) keeps offsets local (`log_end_offset()`) and the Fetch path unchanged, isolating the *only* change to "what makes the record durable and what advances the watermark." This is what keeps the proof burden to Delta A alone. *Alternatives rejected:* (a) a separate durable log — forces a Fetch-path change (Slice 4) into Slice 1; (b) KRaft offsets now — drags in the sequencer-placement tension and its gap-free proof (Slice 2 / Delta C).

### `fsync`-durability, not replication, is Slice 1's demonstrable semantic

A diskless-mode topic's durability comes from `WalStore` (a local `fsync`), so a `RF=1, acks=all` diskless topic is crash-safe where classic `RF=1, acks=all` is not. This is a real, observable behavior that exercises the full seam (offset assignment + durable watermark + ack gate + proof) with the least machinery. *Alternative considered:* keep Slice 1 a pure no-op-semantics refactor (diskless flag routes through `WalStore` but durability is still page-cache) — rejected because it would leave the durability-watermark move untested and the `wal_acked` ghost vacuous.

### Proof scope: Delta A, stateright only

Slice 1 extends `data_path_model.rs` (`crates/broker/src/data_path_model.rs`) with a `wal_acked` ghost distinct from `committed` (`data_path_model.rs:66-67`) and re-establishes the no-loss property (`committed_durable`, `data_path_model.rs:454`) with durability sourced from the WAL. The three harder deltas — crash-mid-flush atomicity (B; the partial-durability case the crash model declares out of scope at `model/mod.rs:163`), gap-free concurrent offsets (C), and the re-composed end-to-end gate (D) — are deferred with their slices. No Creusot kernel this slice (nothing in `crates/verified` models persistence yet; a durability-watermark kernel is a Slice-6 sibling to `recompute_high_watermark`, `crates/verified/src/consensus.rs:267`). *Alternative rejected:* a new verified kernel up front — premature before the seam's shape is validated.

### The `diskless` topic flag is internal

A per-topic `diskless` boolean, resolved from the metadata image (greenfield: added directly, no wire exposure required for Slice 1), selects the branch in `partition_writer::run`. It is **not** a Kafka-client-visible topic config surface in Slice 1 — clients cannot tell a topic is diskless, per the wire-compat invariant. *Alternative deferred:* exposing it via `kafka-configs`/CreateTopics belongs with the operator/admin surface, not this slice.

## Integration

- **`crates/broker/src/handlers/produce.rs`** — unchanged. The ack gate (`finalize_ack`, `:778-784`; parallel gate `:651-656`) already awaits the watermark.
- **`crates/broker/src/partition_writer.rs`** — branch the group-commit step on the `diskless` flag; diskless routes to `WalStore::append_durable` and advances the watermark via `recompute_hw_for_wal_durable`.
- **`crates/broker/src/partition.rs`** — `ProduceData`/`ProduceJob`/`WriterMessage` (`:50-69`) and `await_hw_at_least` (`:538`) unchanged; the `Partition` gains a handle to its `WalStore` (a `dyn WalStore` for diskless topics).
- **`crates/broker/src/replica_state.rs`** — add `recompute_hw_for_wal_durable(offset)` alongside `recompute_hw_for_leader_append`; `compute_hw` (`:135`) unchanged.
- **`crates/broker/src/wal/`** — new module: `WalStore` trait + `LocalFsyncWal`.
- **`crates/log`** — one small addition: a public `Log::sync()` that `fsync`s the active segment on demand (mirroring the existing `flush_on_append` flush at `log.rs:579`), independent of the `flush_on_append` config. `LocalFsyncWal` calls it. Offset assignment (`Log::append*`) is otherwise unchanged.
- **Metadata/config** — a `diskless` per-topic flag on the topic config in the metadata image.
- **`crates/broker/src/data_path_model.rs`** — add the `wal_acked` ghost, `WalAppend`/`WalCommit` model actions, and the no-loss invariant; keep `data_clean` (`:540`) green.

## Kafka / KIP compliance

- **Wire-compat inviolable.** No Produce/Fetch/Metadata/admin response bytes change; the client cannot observe that a topic is diskless. Guaranteed structurally by leaving everything above `writer_tx` untouched.
- **KIP-1150 (Diskless Topics / "Inkless") relationship.** Slice 1 is the seam that the KIP-1150-shaped medium (Slices 2–6) plugs into. It deliberately does **not** implement the leaderless sequencer or object-store WAL yet.
- **Durability semantics.** Classic `acks=all` = replicated to ISR page cache (Kafka-standard). Slice 1 diskless `acks=all` = `fsync`-durable on the acking node. Both are exposed identically on the wire (HW-gated ack); the difference is internal and, for `RF=1`, strictly stronger.

## Testing

- **Stateright (`data_path_model.rs`):** add a `wal_acked` ghost and `WalAppend`/`WalCommit` actions; assert the always-property *"no `wal_acked` record is ever lost"* across crash-restart interleavings, and keep the existing `data_clean` no-loss property green. This is the Slice-1 shipping-gate check.
- **Unit — `LocalFsyncWal`:** offsets are strictly increasing and gap-free per partition; the durable watermark advances only *after* `fsync`, never before; the watermark never regresses.
- **Unit — ack boundary:** for a diskless partition, `await_hw_at_least(target)` resolves exactly when the WAL-durable watermark reaches `target`, and not before (drive the WAL commit deterministically and assert the oneshot resolution point).
- **Behavioral — the new semantic:** a diskless `RF=1, acks=all` produce whose bytes are `fsync`-durable survives a simulated crash-restart (the record is present after recovery), where a classic `RF=1, acks=all` produce in the same harness would be lost. Exercises behavior, not source text.

## Resolved decisions (from brainstorming)

- **Slice-1 scope:** thinnest — seam + durability only. (Not merged with offsets or flush.)
- **Durability medium:** single-node local durable log (`fsync`).
- **Offsets:** keep local `log_end_offset()` assignment.
- **HWM/ISR surface:** hybrid — reuse `ReplicaState`, WAL-sourced advance.
- **Proof:** stateright only; extend `data_path_model` with a `wal_acked` ghost.
