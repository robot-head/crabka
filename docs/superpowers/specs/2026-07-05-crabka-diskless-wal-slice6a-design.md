# Diskless WAL — Slice 6a: quorum-durable WAL medium — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (first sub-slice of Slice 6, the capstone). Foundation slice — it upgrades the durability medium; leaderless serving, the concurrent sequencer, and the shipping gate are later sub-slices.

## Context — where this sits

Slice 6 (the capstone) turns the crash-safe **single-node** diskless topic (Slices 1–5) into a **production-shippable** one: a quorum-replicated WAL that survives node/disk loss, leaderless serving, a concurrent sequencer, and the re-composed no-acked-loss gate + Jepsen. It decomposes into four sub-slices:

- **6a — Quorum WAL medium (this spec):** `QuorumWalStore` (2f+1 AZ replicas, fsync-quorum-commit) replaces `LocalFsyncWal` behind the same `WalStore` seam.
- **6c — Concurrent/leaderless offset sequencer + authority handoff** (generalizes Slice 2; a new verified offset-allocator kernel).
- **6b — Leaderless serving + hot-tail cache** (flip the produce leadership gate; `ReplicaState.hw` from the quorum watermark on every serving broker).
- **6d — Re-composed durability gate + Jepsen** — the shipping gate; turns Slice-5's out-of-scope `NodeLoss` in-scope.

This spec covers **6a only** — the foundation everything else composes over (the quorum-durable watermark). It is the **maximally-incremental** step off the Slice-1 `WalStore` seam: it *quorum-multiplies* the fsync ack gate rather than replacing it.

**Prerequisites (unlanded):** Slices 1–5. `QuorumWalStore` implements the Slice-1 `WalStore` trait and re-sources the Slice-1 WAL-durable HW.

## Design Goals

- **Make the ack quorum-durable:** a record is acked only once `fsync`'d on a majority (f+1) of **AZ-distributed** disks — surviving f node/AZ losses **and** full-quorum simultaneous power loss.
- **Reuse the sans-IO consensus core:** one `QuorumStateMachine` per WAL group; no new consensus algorithm.
- **Change nothing above the seam:** the produce/ack path, offset assignment (Slice 2), fetch (Slice 4), and flush (Slice 3) are untouched — only the `WalStore` impl and the HW advance *source* change.

### Non-goals (6a)

- **No leaderless serving / write path.** 6a keeps single-writer-per-partition; any-broker-accepts-writes is 6b/6c.
- **No concurrent sequencer.** Offsets still come from the Slice-2 single-authority sequencer (6c generalizes it).
- **No re-composed gate / Jepsen** (6d) — 6a carries only a first proof delta (the quorum frontier).
- **No shard-consolidation.** Per-partition quorum groups for the foundation; consolidating many partitions into few groups (to bound consensus overhead) is a deferred optimization.
- **No RAM-quorum relaxed tier** (fsync-after-ack) — a later opt-in behind the same seam if bench-driver shows the strong medium misses the P99 target.

## Architecture Overview

```
partition_writer (diskless branch, Slice 1)
   WalStore::append_durable(batch)  ──►  QuorumWalStore (6a)  [replaces LocalFsyncWal]
        │  replicate the verbatim v2 batch to the partition's 2f+1 AZ-placed WAL group
        │     each replica: LocalFsyncWal step — Log::append_verbatim + fsync (segment.rs:939)
        │  QuorumStateMachine core (kraft-core) per group tracks presence/HWM via LogView
        │  commit when f+1 have fsync-acked  → quorum-durable watermark
        ▼
   recompute_hw_for_wal_durable(quorum_watermark)  → hw_advance_notify   [Slice 1, unchanged]
        ▼
   finalize_ack: await_hw_at_least(...)   [produce.rs:778-784, UNCHANGED]

Flush (Slice 3): read_raw(flushed_offset, hw, budget)  — hw now = fsync-quorum-committed  [UNCHANGED]
```

## Key Design Decisions

### Reuse the sans-IO core; build a new per-shard engine

The pure `crabka-kraft-core` is **reusable verbatim, one per WAL group**: `QuorumStateMachine` (`crates/kraft-core/src/core.rs:29-36`) holds only `me`/`state`/`role`/`election_timeout_ms` — payload-blind, driven by `on_event(Event, &dyn LogView, SimInstant) -> Vec<Action>` (`:115`); `LogView` (`types.rs:40-49`) crosses only offset/epoch metadata (`end_offset`/`last_epoch`/`end_offset_for_epoch`) — **no record payloads**; `Action` (`action.rs:12-47`) is abstract effects. It already runs headless under `sim.rs` and compiles for wasm — proof it is not welded to the metadata engine. **The async metadata engine (`crates/raft/src/kraft/controller.rs`) is NOT reusable**: monomorphized on `MetadataImage`/`MetadataRecord` (`:97-100`, apply at `:1445-1448`), single `@metadata-0` log (`log.rs:29-34`), single mpsc actor (`:445`), full-byte replication to voters (`serve_fetch_records :1759-1772`), wire pinned to `__cluster_metadata`/partition 0 (`transport.rs:229-260`). So 6a builds a **new, leaner per-shard WAL engine** whose "apply" is a trivial durable-watermark advance (not a metadata-image reduction), a `ShardId → engine` registry (none exists — one `QuorumStateMachine` per broker today, `controller.rs:417`), and shard-addressed routing (the KIP-595 codecs already carry `topics[]`/`partitions[]`, `transport.rs:452-456` — field population, not a new codec). *This matches the roadmap directive to build the WAL "on the existing sans-IO KRaft engine … rather than a new consensus layer."*

### `QuorumWalStore` behind the Slice-1 seam

`QuorumWalStore` implements `WalStore::append_durable` (Slice 1): it submits the verbatim v2 batch to the partition's WAL group and returns once quorum-durable. Each of the 2f+1 replicas runs the **Slice-1 `LocalFsyncWal` step per replica** — `Log::append_verbatim` (`log.rs:522`) + `Segment::flush → sync_data` (`segment.rs:939-940`). The group's `QuorumStateMachine` advances the durable watermark using the **verified** `recompute_high_watermark` (`crates/verified/src/consensus.rs:267`) — the quorum-durable offset *is* the majority-th-largest replicated offset, so the existing kernel applies unchanged. Commit (f+1 fsync-acks) advances the per-partition WAL-durable watermark, which drives `recompute_hw_for_wal_durable` (Slice 1). *This keeps and quorum-multiplies the Slice-1 fsync gate — the callers never change.*

### AZ placement + failure budget

The 2f+1 replicas (default 3, f=1) are placed across **≥3 AZs** via `replica_selector.rs` (`RackAware`, `:11`). Acked ⇒ `fsync`'d on f+1 AZ-distributed disks: **no-acked-loss under f node/AZ losses and under total power loss of the quorum** (a surviving f+1 disks retain the record). The unrecoverable boundary is loss of a **quorum** (f+1 nodes) — out of scope, asserted for flushed offsets only (the object tier covers those). This fixes AutoMQ's documented single-AZ-EBS AZ-loss gap by replicating across AZs, with no block-store dependency.

### Durable-reload seam (new)

`QuorumStateMachine` has **no `reset`/`reload`/`from_durable` API** (confirmed absent) — needed for a WAL replica to reconstruct its consensus state (role, quorum state, HWM) on restart from its persisted `PersistQuorumState` + its durable WAL segment. 6a adds this seam (the sim/model also needs it to faithfully model restart). A restarting replica reloads durable state and rejoins the group; the leader re-advances the watermark from the reloaded quorum view.

### Per-partition quorum groups (foundation)

Each diskless partition's WAL is its own 2f+1 group — the cleanest incremental step (parallels classic per-partition replication) and it keeps a partition's tail in one group (simple per-partition watermark + flush read). *Deferred optimization:* consolidating many partitions into few WAL shards to bound consensus overhead (elections/heartbeats scale with group count) — a real scaling concern, addressed after the foundation is correct.

## Integration

- **`crates/broker/src/wal/quorum.rs`** (new) — `QuorumWalStore` (impl `WalStore`); the per-partition WAL group management (one `QuorumStateMachine` per group + the `ShardId → engine` registry).
- **`crates/broker/src/wal/`** — reuse `LocalFsyncWal` as the per-replica durable step.
- **New per-shard WAL engine** (async wrapper over `crabka-kraft-core`) + shard-addressed wire routing (extend the KIP-595 codec field population; a group discriminator in dispatch, since `server.rs:342-368` routes by `api_key` only today).
- **`crates/kraft-core/src/core.rs`** — add the durable-reload seam on `QuorumStateMachine`.
- **`crates/verified/src/consensus.rs`** — reuse `recompute_high_watermark` for the quorum frontier (no change).
- **`replica_selector.rs`** — AZ placement of the WAL group.
- **Slice-1 `recompute_hw_for_wal_durable` / `await_hw_at_least` / `finalize_ack`** — untouched; only the watermark's source moves to quorum-commit.
- **Slices 2–4** — untouched (offset assignment, fetch, flush all read the same HW).

## Kafka / KIP compliance

- **Wire-compat inviolable.** The quorum WAL, the per-shard engine, and its wire messages are entirely internal; clients see a partition whose `acks=all` is now quorum-fsync-durable. No Produce/Fetch/Metadata change.
- **Stronger `acks=all`.** Classic `acks=all` = replicated to ISR page cache (min LEO). 6a diskless `acks=all` = `fsync`'d on an AZ-distributed majority — strictly stronger, exposed identically on the wire (HW-gated ack).
- **Consensus reuse.** The WAL groups run the same KIP-595-shaped consensus core as KRaft, so leader election / epoch fencing / truncation semantics are the proven ones.

## Testing

- **Quorum-durable ack (behavior):** a diskless `acks=all` produce is acknowledged only after f+1 replicas fsync; with a 3-replica group, kill 1 replica (f=1) — the ack still fires and no acked record is lost; kill 2 (quorum lost) — the write does not ack (availability loss, not silent acked-loss).
- **Full-quorum power loss:** all 3 replicas crash-restart; every acked offset is present after recovery (each surviving disk retains its fsync'd prefix; the leader re-advances the watermark from the reloaded quorum).
- **Watermark = majority-th-largest:** unit-test that the group's durable watermark advances to `o` only once f+1 replicas hold `o`, driven by the reused `recompute_high_watermark` (property-check against its oracle).
- **Seam unchanged:** the produce/ack path, offset assignment, fetch, and flush tests (Slices 1–4) stay green with `QuorumWalStore` swapped in for `LocalFsyncWal` behind the seam.
- **First proof delta (stateright):** extend the Slice-5 diskless crash model so the WAL frontier advances on majority presence; `NodeLoss` of a *minority* of WAL replicas is **in-scope** and `wal_acked_durable` holds; a `sometimes` witness reaches "acked-but-unflushed offset survives minority WAL-node loss" (the state that flips Slice-5's out-of-scope note). The full re-composition (concurrent appenders + leader change + Jepsen) is 6d.
- **Durable reload:** a replica restarts, reloads its consensus + WAL state, and rejoins without the group losing or double-counting any offset.

## Risks (carried into the plan)

- **Per-partition consensus overhead** — N groups' elections/heartbeats scale with partition count; shard-consolidation is the deferred mitigation. The foundation must not bake in assumptions that block consolidation.
- **Data-plane replication cost** — the metadata engine full-byte-replicates to every voter; a naive WAL reuse would N×-replicate all traffic permanently. 6a replicates for *durability* and offloads to objects (Slice 3), so the WAL retains only the un-flushed tail — the replication window is bounded by the flush cadence, not permanent. This must hold in the design (trim the per-replica WAL on flush).
- **Durable-reload correctness** — the new reload seam must reconstruct the exact quorum view (role/epoch/HWM); a wrong reload could ack-then-lose. Covered by the reload test + the model.
- **Latency budget** — fsync-before-ack on f+1 AZ disks adds cross-AZ round-trips; if bench-driver shows it misses single-digit-ms P99, the RAM-quorum relaxed tier (opt-in, same seam) is the escape hatch.
- **Quorum-loss boundary** — loss of f+1 nodes is genuinely unrecoverable for the un-flushed tail; the spec asserts no-acked-loss only *within* quorum and relies on the object tier for flushed offsets.

## Resolved decisions (from brainstorming)

- **Decomposition:** Slice 6 → 6a (this) → 6c → 6b → 6d; spec 6a first (the foundation).
- **Medium:** local-durable-log-per-node, **fsync-before-ack**, 2f+1 AZ replicas, commit on f+1 fsync.
- **Reuse:** the sans-IO `QuorumStateMachine` core per group + verified `recompute_high_watermark`; build a new per-shard engine + registry + routing + durable-reload seam.
- **Grouping:** per-partition quorum groups; shard-consolidation deferred.
- **Scope:** medium only — single-writer preserved; leaderless (6b), concurrent sequencer (6c), full gate + Jepsen (6d) deferred.
- **Deferred:** RAM-quorum relaxed tier; shard-consolidation.
