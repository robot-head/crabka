# Diskless WAL — Slice 6c: concurrent sequencer + leaderless writes — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (sub-slice of Slice 6). Generalizes Slice-2 offset assignment to concurrent stateless appenders and flips the write path leaderless.

## Context — where this sits

Sub-slice of Slice 6 (see the [6a spec](2026-07-05-crabka-diskless-wal-slice6a-design.md) for the decomposition). Slice 2 assigned offsets via a **single-authority** `ControllerSequencer` (commit `V1PartitionOffsetAdvance`, then read-after-commit) — correct only when one sequencer per partition never interleaves. 6a made the WAL a quorum; 6b made reads leaderless. 6c makes **writes** leaderless: **any broker accepts a diskless write**, sequences an offset safely under **N concurrent appenders**, and appends to the quorum WAL — with the nominal leader reduced to a wire fiction. It also owns the leaderless-write produce-gate flip deferred from 6b.

**Design decision — correctness now, throughput later.** 6c generalizes the sequencer for *concurrency correctness* by **widening the commit path to return the applied base offset** (the alternative Slice 2 explicitly deferred as "the S6-era robustification") — the controller serializes concurrent advances and hands each appender its correct contiguous range. The offset authority stays KRaft-committed. **Moving offset assignment onto the WAL-quorum-commit path** (eliminating the per-produce controller round-trip — the Slice-2-flagged bottleneck) is a **named deferred throughput optimization**, not needed for the correctness shipping gate (6d).

**Prerequisites (unlanded):** Slices 1–5 + 6a (+ 6b for full leaderless serving of the writes it enables). Builds on Slice-2's `OffsetSequencer` seam.

## Design Goals

- **Concurrent-safe offset assignment:** generalize `OffsetSequencer` from single-authority read-after-commit to N stateless appenders, each receiving a gap-free, unique, strictly-monotonic contiguous range — by returning the applied base **from the commit**, not a racy post-commit image read.
- **Sequencer-authority handoff:** on a KRaft controller leader change, the new authority re-derives the next-offset from the committed image with no offset regress or duplicate.
- **Leaderless write path:** flip the produce leadership gate (`produce.rs:459-476`) so any broker accepts a diskless write and sequences via the concurrent sequencer + quorum WAL; keep advertising a nominal leader on the wire.
- **A verified offset-allocator kernel:** the Slice-1-named Creusot sibling proving concurrent range assignment is a gap-free/unique/monotonic partition of `[0, next)`.

### Non-goals (6c)

- **No throughput move to WAL-commit assignment** (the controller round-trip stays this slice — a deferred optimization; correctness first).
- **No re-composed durability gate / Jepsen** (6d) — 6c adds the concurrent-offset unit + Creusot proofs; the composed no-acked-loss gate is 6d.
- **No client routing changes** beyond the produce-gate flip (Metadata still names a nominal leader; consumer routing to non-leaders is 6b/KIP-392).

## Architecture Overview

```
Produce for a diskless partition arrives on ANY broker (handlers/produce.rs)
   leadership gate (:459-476):  REJECT if leader!=self   ──►  [diskless] ACCEPT + sequence
        │
        ▼
   OffsetSequencer::assign(topic, part, N)  ──►  ConcurrentSequencer (6c)
        │  submit "advance P by N" to the controller; the controller SERIALIZES
        │  concurrent advances and RETURNS the applied base (widened commit path)
        │     base = pre-increment partition_next_offsets slot value  [no racy read-after-commit]
        ▼
   append at `base` to the QUORUM WAL (6a QuorumWalStore)  → fsync-quorum-commit → ack
        │  (verified concurrent offset-allocator kernel: assign_ranges / is_gap_free_partition)
        ▼
   nominal leader still advertised on the wire (Metadata/current_leader hint) — a fiction
```

## Key Design Decisions

### Widen the commit to return the applied base (concurrency correctness)

Slice 2's read-after-commit (`base = image.next_offset − count`) races under concurrency: two appenders both read the image and compute the same base. 6c fixes it by threading the **applied base through the commit path** — when the controller applies a `V1PartitionOffsetAdvance`, it records the pre-increment slot value and returns it to *that* submitter (a dedicated `AssignOffsets` command / widened `submit_change` reply). The controller's single serialized apply order defines a total order over concurrent advances, so each appender gets a unique, contiguous, gap-free range. `ConcurrentSequencer` implements the same Slice-2 `OffsetSequencer::assign` trait (the seam is unchanged; only the impl swaps). *Alternative rejected:* read-after-commit (Slice 2's single-authority approach) — races under concurrency, the exact gap Slice 2 flagged.

### Sequencer-authority handoff on leader change

The durable offset authority is the **committed** `partition_next_offsets` in the KRaft image (Slice 2). On a controller leader change, the new leader already holds the committed image, so handoff = it resumes assigning from the committed frontier. The correctness obligation: no in-flight advance is lost or double-applied across the handoff — guaranteed because an advance is either committed (visible to the new leader) or not (the appender's ack never fired; it retries). This mirrors the existing KRaft `CommitWaiter` fail-on-leadership-loss (`controller.rs:1180`): parked advances fail on the old leader; the appender retries against the new leader. *No offset regress or duplicate across handoff.*

### Leaderless write path (the produce-gate flip)

For a diskless partition, replace the `leader != self.node_id` rejection (`produce.rs:459-476`, `NOT_LEADER_OR_FOLLOWER` + KIP-951 hint) with **accept-and-sequence**: the accepting broker calls `ConcurrentSequencer::assign`, appends to the quorum WAL (6a), and acks on quorum-commit. The metadata **still names a nominal leader** and the wire **still reports it** (Metadata `current_leader`, epoch fencing) — the leader becomes a fiction decoupled from where data lands. The `min.insync.replicas` preflight (`produce.rs:513-521`, reads image ISR) is reinterpreted against the **WAL quorum's** durability budget (f+1) for diskless, not the classic ISR. The classic path (non-diskless) keeps the exact leader-enforcing gate.

### The verified concurrent offset-allocator kernel

A new Creusot kernel in `crates/verified/src/` (the Slice-1-named sibling to `recompute_high_watermark`): `assign_ranges` / `is_gap_free_partition` over concurrently-assigned `[base, base+count)` ranges. Property: the assigned ranges form a **partition of `[0, next)` with no gap, no overlap, and strict monotonicity**. Pure, small, arithmetic — the shape Creusot handles (built like `count_ge_prefix` + its monotone/nonnegative lemmas, `consensus.rs:20-57`), with a sort-and-check-contiguity oracle (mirroring `hwm_sort_oracle`). It has a production call site: the `ConcurrentSequencer`'s range validation behind the `OffsetSequencer` seam.

## Integration

- **`crates/raft/src/kraft/controller.rs` / `transport.rs`** — widen the commit reply to carry the applied base (a dedicated `AssignOffsets` command or a widened `submit_change` reply threading the pre-increment slot value from the apply path).
- **`crates/broker/src/wal/…`** (Slice-2 `OffsetSequencer` seam) — `ConcurrentSequencer` impl (submit + return-from-commit); replaces `ControllerSequencer` for diskless.
- **`crates/broker/src/handlers/produce.rs`** — the leaderless-write gate flip for diskless (`:459-476`); `min.insync.replicas` reinterpreted against the WAL quorum (`:513-521`).
- **`crates/verified/src/`** — the `assign_ranges`/`is_gap_free_partition` kernel + oracle; called from `ConcurrentSequencer`.
- **6a `QuorumWalStore`** — the append target (unchanged; 6c supplies the base).
- **Ack gate, fetch, flush** — untouched.

## Kafka / KIP compliance

- **Contiguous byte-exact offsets** across concurrent appenders (the kernel proves it); clients see `0,1,2,…` exactly.
- **Wire still names a leader.** Metadata/`current_leader`/epoch fencing are preserved even though the write path no longer enforces leadership — clients cannot tell the write was leaderless.
- **Idempotence/`acks=all`** unchanged on the wire; `acks=all` for diskless = WAL-quorum-durable (6a).

## Testing

- **Concurrent gap-free offsets:** N appenders concurrently produce to one diskless partition; assert the assigned ranges are contiguous, unique, gap-free, strictly monotonic (the kernel's property, exercised end-to-end).
- **Handoff:** force a controller leader change mid-produce; assert no offset regresses or duplicates, and in-flight advances either commit (visible to the new leader) or fail-and-retry.
- **Leaderless accept:** a produce sent to a **non-leader** broker for a diskless topic is accepted, sequenced, quorum-committed, and acked; the same produce to a non-leader classic topic is still rejected (`NOT_LEADER_OR_FOLLOWER`).
- **Creusot:** `cargo creusot` proves `is_gap_free_partition`/`assign_ranges` (CI replay, like the existing kernels).
- **Seam unchanged:** the `OffsetSequencer::assign` trait and its callers are byte-identical; only the impl swapped.

## Risks (carried into the plan)

- **Controller throughput bottleneck** (the deferred optimization): per-produce advances still funnel through the single `@metadata-0` engine — correct but a throughput ceiling. The WAL-commit-assignment optimization is the named follow-up; 6c must not bake in assumptions that block it.
- **Handoff races:** an advance committed just before a leader change must be visible to the new leader (KRaft commit durability guarantees this); the fail-waiters path must not double-apply.
- **min.insync reinterpretation:** the diskless durability budget is the WAL quorum (f+1), not the classic ISR — mixing them would mis-gate acks.

## Resolved decisions (from brainstorming)

- **Concurrency correctness via widen-commit-return** (not racy read-after-commit); the `OffsetSequencer` seam unchanged.
- **Handoff:** the committed KRaft frontier is the authority; the new leader resumes; no regress/duplicate.
- **Leaderless writes:** flip the produce gate for diskless; wire still names a nominal leader.
- **Creusot:** the concurrent offset-allocator kernel (Slice-1-named sibling).
- **Deferred:** the throughput move to WAL-commit assignment; the re-composed gate + Jepsen (6d).
