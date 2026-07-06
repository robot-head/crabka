# Diskless WAL — Slice 2: KRaft-assigned offset sequencer — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (second slice of a 6-slice milestone). Like Slice 1, a **scaffolding slice** — it moves the offset authority; it does not ship diskless on its own.

## Context — where this sits

Second slice of the diskless-broker WAL milestone (see [Slice 1](2026-07-05-crabka-diskless-wal-slice1-design.md) for the decomposition and the seam it builds on). Slice 1 installed the `WalStore` durability seam behind the partition writer and moved the `acks=all` boundary onto a WAL `fsync`, but **deliberately kept offsets local** (`Log::log_end_offset()`). Slice 2 replaces that local assignment with a **controller-committed** offset for diskless topics, so offsets come from a single KRaft authority rather than each broker's local log — the step toward stateless brokers.

**Decision (from brainstorming):** the offset authority is **controller-committed / commit-time** — each produce group's offset range is assigned by committing an "advance partition P by N" record through the KRaft controller (reusing the controller's commit-and-ack machinery), returning the base offset. This is the leaderless commit-time-assignment shape the end-state (WarpStream / Confluent Freight / KIP-1150) wants, so Slice 6 *generalizes* it rather than unwinding it. **Forward path deferred:** Slice 2 assigns via a **local** `submit_change` only (the assigning node is a controller/voter); the broker→controller forward path (and the RPC offset field) is a later follow-up. Since diskless does not ship until Slice 6, the interim combined-node restriction costs nothing.

## Design Goals

- **Replace the local offset authority** for diskless topics: source the base offset from a KRaft-committed `V1PartitionOffsetAdvance` record instead of `Log::log_end_offset()` (`crates/log/src/log.rs:394`, read at `:483`/`:524`).
- **Introduce the `OffsetSequencer` seam** (mirroring Slice 1's `WalStore`) so Slice 6 can swap in a concurrent/leaderless sequencer without touching the writer or the ack gate.
- **Preserve Kafka byte-exactness:** the assigned base is stamped via the existing verbatim path whose offset patch is *below the CRC region*, so client-visible offsets are contiguous and require no re-CRC.
- **Prove the sequencer correctness (Delta C-lite):** offsets are gap-free, strictly monotonic, and unique per partition under a **single** sequencer, via a stateright model.

### Non-goals (Slice 2)

- **No broker→controller forward path.** No change to `CrabkaSubmitChangeResponse` (`crates/raft/src/wire.rs`); assigning runs where the image is local (combined/controller node). Deferred follow-up.
- **No crash atomicity.** The controller commits the offset *before* the local `fsync`, opening a `[B, B+N)` committed-but-not-durable window on crash. Excluded by a no-crash assumption; this is exactly **Slice 5** (commit↔fsync atomicity + recovery).
- **No concurrency / leader-change.** Single sequencer per partition only; concurrent stateless appenders and sequencer-authority handoff are **Slice 6**.
- **No throughput optimization.** The per-group controller commit through the single `@metadata-0` engine is a known, accepted bottleneck for this correctness slice (see Risks).
- No object-store flush (Slice 3), no diskless fetch (Slice 4).

## Architecture Overview

The offset authority moves from the local log to KRaft, but everything above `writer_tx` (the wire handler, the `acks=all` gate at `crates/broker/src/handlers/produce.rs:778-784` and `partition.rs:538`) stays byte-identical, and the classic path is untouched.

```
partition_writer::run  ── diskless Produce group (N = Σ(last_offset_delta+1))
      │
      ├─ 1. OffsetSequencer::assign(topic, part, N) ─► ControllerSequencer
      │        submit_change([V1PartitionOffsetAdvance{topic,part,count=N}])   (local)
      │        → controller commits → image.apply bumps partition_next_offsets
      │        → read post-commit image: base = next_offset(topic,part) − N
      │
      ├─ 2. append each batch at its sub-range of [base, base+N)
      │        Log::append_verbatim_at(batch, base)   (new public; base == log_end_offset() guard)
      │        → Segment patches base_offset below the CRC region → NO re-CRC
      │
      ├─ 3. fsync                  (Slice 1: WalStore / Log::sync)
      └─ 4. recompute_hw_for_wal_durable → hw_advance_notify   (Slice 1)
```

## Key Design Decisions

### The `OffsetSequencer` seam

`trait OffsetSequencer { async fn assign(&self, topic: &str, partition: PartitionIndex, count: u32) -> Result<Offset, BrokerError>; }` returns the base offset of a contiguous `count`-wide range. It replaces the two `let assigned_base = self.log_end_offset();` sites *for diskless topics only* (classic topics keep local assignment). The seam is swappable: Slice 6's concurrent/leaderless sequencer is a different impl behind the same trait. *Alternative rejected:* inlining the controller call in the writer — couples the writer to KRaft and blocks the Slice-6 swap.

### `ControllerSequencer` — commit then read-after-commit

The Slice-2 impl commits one `V1PartitionOffsetAdvance` via a **local** `Controller::submit_change` (`crates/raft/src/controller.rs:230` → `crates/raft/src/kraft/controller.rs:605`), awaits its commit, then reads the post-commit `MetadataImage`'s `partition_next_offset(topic, partition)` and returns `base = next − count`.

- Why read-after-commit: `submit_change` returns `Result<(), RaftError>` — it surfaces no data-plane offset (the `CommitWaiter.base_offset` is the *raft-log* offset, not the partition's data offset). Reading the image after the commit resolves is correct because `submit_change`'s reply fires from `try_resolve_waiters` (`controller.rs:1540`), which runs *after* `advance_and_apply` (`:1403`) applies the record and updates the image. Under a single sequencer per partition, no other advance for that partition interleaves between commit and read, so `next − count` is exactly this group's base.
- *Alternative deferred:* widening the commit path to return the applied data-plane base directly (a dedicated `AssignOffsets` command threading the base through the waiter) — more robust under concurrency, but that robustification belongs with Slice 6; read-after-commit is correct and minimal for the single-sequencer slice.

### The `V1PartitionOffsetAdvance` metadata record — a delta via the private carrier

A new `MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord { topic: String, partition: i32, count: i64 })` (`crates/metadata/src/records.rs`, enum at `:264-292`, template `V1PartitionDirAssignmentRecord` at `:62-70`). It is applied by `MetadataImage::apply` (`crates/metadata/src/image.rs:470-658`) as a **delta** — bumping a new `partition_next_offsets: HashMap<(String,i32), i64>` field on the image (`image.rs:85-111`), returning the pre-increment value as the base — mirroring the existing delta arm `V1PartitionDirAssignment` (`image.rs:648-657`). There is no KIP-631 wire record for this Crabka-internal concept, so it rides the **Crabka-private carrier** (a new private apiKey `1003` next to `kraft_translate.rs:661-665`; encode via `wincode_carrier` at `:670-681`, decode guard at `:895-902`), exactly as `V1PartitionDirAssignment` does.

*Why a delta, not a full-record replace:* the delta is order-independent — `apply` runs single-threaded on the committed log in commit order, so sequential deltas produce a gap-free, strictly-monotonic, unique offset sequence. This is the property the Slice-2 proof rests on, and the same rationale the dir-assignment carrier documents (`kraft_translate.rs:633-645`). *`apply` is infallible by contract* (`image.rs:463-467`); any pre-checks go in `validate` (`:844-933`, catch-all arm `:892-931`).

### Byte-exact offset stamping — `Log::append_verbatim_at`

The base flows into the append via a **new public** `Log::append_verbatim_at(&VerbatimBatch, base: Offset)`, which wraps the existing private `append_verbatim_preserving_offset` (`log.rs:544`) *plus* the leader-epoch checkpoint bookkeeping `append_verbatim` does (`log.rs:527-534`), and asserts `base == log_end_offset()`. The segment layer patches `base_offset` + `partition_leader_epoch` into a header copy below byte 16 (`Segment::append_verbatim` / `patch_base_offset_and_leader_epoch`, `segment.rs:839/869`); the CRC covers bytes 21+, so **the assigned offset is stamped with no CRC recompute** — the record bytes clients fetch are byte-exact Kafka with a contiguous, sequencer-assigned offset. *Alternative rejected:* re-encoding to stamp the offset — needless CPU and breaks the zero-copy verbatim path.

### The `base == log_end_offset()` guard is the gap-free witness

`append_verbatim_at` reuses `append_at`'s invariant (`log.rs:623`): the caller-supplied base must equal the local LEO. Under a correct single sequencer this always holds — the KRaft `partition_next_offsets` counter and the local log advance in lockstep (both by `N` per committed group, from a shared start of 0). A mismatch means the sequencer and the local log diverged (a bug, or the deferred crash window) and the append fails loudly rather than silently creating a hole.

## Integration

- **`crates/metadata/src/records.rs`** — `PartitionOffsetAdvanceRecord` struct + `V1PartitionOffsetAdvance` enum variant.
- **`crates/metadata/src/lib.rs`** — re-export the struct (`:74-81`).
- **`crates/metadata/src/image.rs`** — new `partition_next_offsets` field (+ init in `new`, `:120-142`) + `partition_next_offset(topic, part)` accessor (mirror `partition()` at `:175-177`) + `apply` arm (`:470-658`) + `record_variant` arm (`:60-83`) + `validate` arm (`:892-931`) + snapshot emit in `to_records` (`:683-825`).
- **`crates/metadata/src/kraft_translate.rs`** — carrier encode arm + private apiKey `1003` + decode guard.
- **`crates/log/src/log.rs`** — new public `append_verbatim_at(&VerbatimBatch, Offset)`.
- **`crates/broker/src/wal/`** — `OffsetSequencer` trait + `ControllerSequencer` (holds a `Controller` handle + a read handle to the current `MetadataImage`).
- **`crates/broker/src/partition_writer.rs`** — the diskless Produce branch: assign `N` per group, append at the base via `append_verbatim_at`, then the Slice-1 fsync + HW advance. Classic path unchanged.
- **`crates/broker/src/partition.rs`** — diskless partitions construct a `ControllerSequencer` and pass it (alongside the Slice-1 `WalStore`) to the writer.
- **Wire/RPC** — **unchanged** (forward path deferred).

## Kafka / KIP compliance

- **Wire-compat inviolable.** Client-visible offsets are contiguous per partition and byte-exact (no re-CRC). Produce/Fetch/Metadata responses are unchanged; the `diskless` flag and the advance record are internal.
- **Offset semantics.** A diskless partition's offsets are still `0, 1, 2, …` contiguous, identical to a classic partition — only the *authority* that assigns them moved from the local log to KRaft.
- **KIP-1150 relationship.** Controller-committed / commit-time assignment is the leaderless end-state's shape; Slice 2 implements the single-authority case, Slice 6 the concurrent/stateless generalization.

## Testing

- **Unit — `ControllerSequencer`:** against an in-process single-voter controller, `assign(topic, p, N)` returns a base equal to the partition's prior committed next-offset, and successive assigns return contiguous, non-overlapping ranges (`base_k+1 == base_k + N_k`). Reuses the inline single-voter commit path (`controller.rs:1341-1343`).
- **Unit — `MetadataImage` apply:** applying a sequence of `V1PartitionOffsetAdvance` deltas produces a strictly-monotonic per-partition `next_offset`; interleaving advances for *different* partitions are independent; snapshot `to_records`/`from_records` round-trips the `partition_next_offsets` map. (Extend `image.rs:1044`, `records.rs:302`.)
- **Unit — `Log::append_verbatim_at`:** stamps the supplied base byte-exactly (assert `bytes[17..21]` — the CRC — is unchanged, `bytes[21..]` verbatim, mirroring `partition_writer.rs:818-819`); rejects a base `!= log_end_offset()` with `OffsetMismatch`.
- **Integration — diskless writer:** a diskless produce group is assigned a contiguous offset range from the controller, appended at that base, fsynced, and acked; the client-observed base offset equals the controller-assigned base. Exercises behavior end-to-end through `writer_tx`.
- **Stateright — Delta C-lite (the shipping-gate check):** model the sequencer as a committed per-partition `next` counter + an `Assign(N)` action (returns `base = next`, then `next += N`) composed with the local append; assert an always-property that the assigned offsets are **contiguous, strictly increasing, and never reused** per partition across every interleaving the checker explores — under a single sequencer and no crash. Model it as a sibling to (or an extension of) `crates/broker/src/data_path_model.rs`; do not weaken the property to force a pass.

## Risks (carried into the plan)

- **Throughput bottleneck (dominant).** Every diskless produce group across all partitions commits one record through the single controller engine task and the single `@metadata-0` log — no batching/coalescing. This serializes data-plane offset assignment through a control-plane singleton and contends with classic `AlterPartition`/topic/ACL traffic on the same engine. Accepted for the correctness slice; optimization (batched/commit-at-flush assignment) is deferred.
- **Commit-before-fsync gap.** Real durability hole under crash in `[B, B+N)`; safe only under the no-crash assumption. Must be loudly documented so it is never mistaken for production durability (closed in Slice 5).
- **Read-after-commit correctness** depends on the single-sequencer-per-partition assumption; it breaks under concurrency (Slice 6, which replaces it).

## Resolved decisions (from brainstorming)

- **Offset authority:** controller-committed / commit-time.
- **Forward path:** deferred — local `submit_change` only; no RPC offset field this slice.
- **Base-return mechanism:** read-after-commit (`base = image.next_offset − count`).
- **Commit granularity:** one advance per produce **group** (`N = Σ(last_offset_delta+1)`).
- **Sequencer state:** a new `partition_next_offsets` map in the existing `MetadataImage`.
- **Record shape:** a **delta** (`advance by N`) on the Crabka-private carrier (apiKey 1003).
- **Proof:** single-sequencer, no-crash, no-concurrency; gap-free + monotonic + unique; stateright.
