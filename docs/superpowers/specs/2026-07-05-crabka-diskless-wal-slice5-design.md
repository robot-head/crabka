# Diskless WAL — Slice 5: crash-restart atomicity + recovery — design

**Date:** 2026-07-05
**Status:** Approved
**Type:** Subsystem design (fifth slice of a 6-slice milestone). A **correctness-hardening slice** — it proves no-acked-loss across crash-restart and makes a diskless partition recover cleanly; it is not a feature.

## Context — where this sits

Fifth slice of the diskless-broker WAL milestone (see [Slice 1](2026-07-05-crabka-diskless-wal-slice1-design.md) for the decomposition). Slices 1–4 built the single-node diskless data path (fsync-durable ack → KRaft offsets → object flush + index → cold fetch → trim). Each slice introduced an ordering with a crash window. Slice 5 handles a crash at every seam, recovers the partition on restart, and extends the durability model to **inject partial durability** and prove **no acknowledged record is ever lost across a crash-restart of the same node**.

**Scope boundary (medium-dictated).** With the Slices 1–4 medium (a single node's local `fsync`), an acked record is by construction on that node's fsync'd local `Log` (`ack` fires strictly *after* `fsync`). So **no-acked-loss is provable for crash-restart of the same node**, and a *different* broker can recover **flushed** data from objects+index. But the **un-flushed acked tail lives only on the accepting node's disk** — node/disk loss or failover to a stateless broker cannot recover it until Slice 6's quorum-replicated WAL. That is a property the medium cannot provide, not a Slice-5 gap.

**Prerequisites (unlanded):** Slices 1–4. The model builds on the Slice-1 `wal_acked` ghost (`data_path_model.rs` today has only `committed`/`lost`, `:66-67`). Note: the Slice-1 spec's `model/mod.rs:163` "partial-durability out of scope" cite is a **phantom path** — no such file exists; the real model is `crates/broker/src/data_path_model.rs`, and there is no code marker to flip, only the *absence* of a crash action to add.

## Design Goals

- **Recover a diskless partition cleanly on restart:** torn-tail truncation, KRaft↔local re-anchor, index rebuild, producer-sequence dedup rebuild.
- **Prove no-acked-loss across crash-restart** via a partial-durability crash-injection stateright model.
- **Close the one window that could lose acked data** (trim racing ahead of index durability) by enforcing `trim ≤ committed-index-frontier`.
- **Preserve Kafka semantics:** crash-induced offset gaps are benign (consumers skip self-delimiting batches); idempotent producers don't duplicate or misfire after restart.

### Non-goals (Slice 5)

- **No node/disk-loss durability** of the un-flushed acked tail (Slice 6's quorum medium). Modeled as an explicit out-of-scope injection.
- **No object-side producer-state snapshots** (needed only for *failover*-node rebuild — Slice 6).
- **No durable/KRaft-backed PID allocation.** The volatile `ProducerIdManager` (`producer_id_manager.rs`) PID-collision-across-restart is a **pre-existing classic-path gap**, not diskless-specific — flagged as a follow-up.
- **No orphan-object GC** (a crash between PUT and index-publish leaks an object; a storage-cost leak, not a correctness loss — deferred).
- **No transactional/LSO recovery** (diskless is non-transactional per Slice 4; `LSO = HW`).
- **No Creusot kernel** (stateright-only this slice).

## Architecture Overview — the crash seams

Timeline of one diskless produce group, then the async flush lifecycle:
`t0` KRaft commit `V1PartitionOffsetAdvance` → `t1` local `append_verbatim_at(base)` → `t2` `fsync` → `t3` HW advance → **ack** — then async: `PUT` → index publish → `flushed_frontier` advance → trim.

| Seam | Crash window | Acked loss? | Status |
| --- | --- | --- | --- |
| **A** | `[KRaft commit, fsync)` | No (pre-ack) — benign offset gap | **NEW: re-anchor** |
| **B** | mid-`fsync` (torn trailing batch) | No (pre-ack) | **NEW: validate + epoch-checkpoint truncate** |
| C | `[PUT, index publish)` | No — orphan object; data still local | safe (GC deferred) |
| D | index publish uncommitted | No — frontier derives from committed index | safe (proof owed) |
| E1 | `[index-durable, trim-issued)` | No — local+object overlap | safe |
| **E2** | `[trim, index-durability)` | **Yes if unguarded** | **NEW: the sharpest edge** |

## Key Design Decisions

### Recovery extends `Log::open` (`crates/log/src/log.rs:215-286`)

**Seam B — torn-tail truncation.** `Log::open` opens the last segment via `Segment::open_active(..., validate_on_open)` (`:251-255`); `recover_active_tail` (`segment.rs:236-275`) decodes forward, breaks on the first undecodable batch (`:255-257`), `set_len` physically truncates the torn tail (`:266-269`), and sets `lso = active.last_offset()+1` (`log.rs:267`). Slice 5 **forces `validate_on_open = true` for diskless partitions** (else a torn tail reads as valid) and **adds `LeaderEpochCheckpoint::truncate_from_end(recovered_LEO)`** (`leader_epoch_checkpoint.rs:104`) after tail recovery — `Log::open` opens the checkpoint (`:265`) but never truncates it today, leaving an epoch entry dangling past `log_end_offset` (a real bug that corrupts epoch→offset lookups). `log_start_override` is `None` at open (`log.rs:280`), so the local floor is re-derived from the first on-disk segment base — trimmed offsets stay below-floor and route cold, consistent with Slice 4.

### Seam A — re-anchor the local append cursor to the KRaft frontier

On restart, `recover_active_tail` may leave `log_end_offset() < KRaft partition_next_offsets` (the `[B,B+N)` crash window: KRaft committed the offset at `t0`, but `fsync` never completed). Slice 2's `append_verbatim_at` asserts `base == log_end_offset()` (the gap-free witness, reusing `append_at`'s `OffsetMismatch` at `log.rs:623`), so the *next* produce would fail **on every attempt**. Slice 5 reconciles the two authorities on diskless-partition open: read the KRaft frontier from the metadata image, record a durable `reconciled_frontier`, and relax the guard to `base == max(log_end_offset(), reconciled_frontier)` (keeping the `base ≥ log_end_offset()` half — never mask a *real* gap). This turns the window into a **benign consumer-visible offset gap** (Kafka consumers skip). *Alternative rejected:* gap-fill placeholder batches — pollutes the log with synthetic records and complicates the read path. **KRaft is the offset authority; the local log is a cache of a suffix; restart re-anchors the append cursor to the authority.** No existing code bridges KRaft↔local `Log`.

### Seam E2 — `trim ≤ committed-index-frontier` (the sharpest edge)

Slice 4 gated trim on the *in-memory* `WalIndexCache.flushed_frontier`. Slice 5 tightens it to **index durability**: trim only up to an offset whose `WalFlushRecord` is durably committed to the `__diskless_wal_index` topic (so the entry is reconstructable from the committed projection on restart). A trim that races ahead of index durability leaves `[trimmed,…)` below the local floor **and** absent from the rebuilt cache — a permanent `[below-floor ∧ cache-miss]` loss of acked data. The enforced ordering is **index-topic-durable → then issue `TrimToOffset`**. This is the one window that *can* lose acked data; the model must exercise it.

### Producer-sequence dedup rebuild (greenfield)

`ProducerState` (`producer_state.rs`) is an in-memory `DashMap` always built empty via `ProducerState::new()` (`partition.rs:679`, `remote_log_manager.rs:925`, …) — there is **no** rebuild path, and `recover_active_tail` reads only `last_offset`/`max_timestamp` (`segment.rs:259-261`), not sequence state. On crash-restart the fsync'd log recovers the *records* but not the dedup map, so an idempotent retry of an already-acked batch would be treated as fresh → a duplicate. Slice 5 adds a rebuild: scan the recovered WAL tail's batch headers for `(producer_id, producer_epoch, base_sequence, last_offset_delta)` and repopulate `last_sequence`, keyed strictly to the **post-truncation** `log_end_offset()`; drop entries above the recovered LEO via `ProducerState::truncate` (`:176`). Natural home: a new routine in `crates/log/src/recovery.rs` (sibling to `swap_orphan_recover`, `:29`), wired into `Log::open`, populating the shared `ProducerState`. *PID-allocation durability is out of scope* (see non-goals).

### Seams C, D, E1 — already crash-safe by Slices 3–4 design

- **C (orphan object):** on restart the flusher re-reads the same `[flushed_frontier, hw)` tail and re-PUTs to a **new immutable `flush_uuid`** key; the orphan is GC-able. (GC deferred.)
- **D (uncommitted publish):** `flushed_frontier` derives from the *committed* projection, so an uncommitted publish contributes nothing. Slice 5 owes a **proof** that the projection is monotone and idempotent under crash-replay (re-consuming from the last committed offset reconstructs the identical frontier), not new code.
- **E1 (index-durable / trim-issued):** the range is in both the local WAL and the object — a benign overlap; trim re-issues later.

### The proof — a separate diskless-only stateright model

Rather than bolt onto `data_path_model`'s clean-replication actions (state-explosion: bounds already near limits, `:46-49`), add a **tighter diskless-only model** (small `MAX_LEN`, few brokers, no ISR/replication actions). It **injects partial durability** by splitting the currently-atomic durability step (`AdvanceHwm`, `:402-415`) into ordered sub-step actions — `WalAppend → WalFsync → KraftAssign → ObjectPut → IndexPublish → Trim` — each mutating a distinct frontier ghost, plus a `Crash`/`Recover` pair (the log **persists** across restart, mirroring `Die`/`Revive` at `:438-442` which touch only the `live` mask). `Crash(b)` rolls the *non-durable* ghosts back to the last durable frontier; `Recover` re-derives every frontier from durable state only (`wal_acked` from the fsync'd tail, `flushed_offset` from the committed index, local floor from trim). **Assert** an always-property `wal_acked_durable` (every acked offset stays recoverable across every interleaving) with **`sometimes` witnesses** so each mid-sequence crash is actually reached (not vacuous), plus a producer-dedup ghost asserting recovery never regresses `last_sequence`. `NodeLoss(b)` (clears the log) is modeled as an **explicit out-of-scope** injection: `wal_acked_durable` is asserted for *flushed* offsets only under node loss, with the un-flushed-tail case marked the Slice-6 obligation. Keep `committed_durable`/`data_clean` green.

## Integration

- **`crates/log/src/log.rs`** (`Log::open`) — force `validate_on_open` for diskless; call `LeaderEpochCheckpoint::truncate_from_end(recovered_LEO)` after tail recovery; wire the producer-state + reconciled-frontier rebuild.
- **`crates/log/src/recovery.rs`** — new producer-sequence rebuild routine (sibling to `swap_orphan_recover`).
- **`crates/broker/src/…`** (Slice-2 `append_verbatim_at`) — relax the guard to `base == max(log_end_offset, reconciled_frontier)`; the diskless-open KRaft re-anchor.
- **`crates/broker/src/diskless/flusher.rs`** (Slice 4) — tighten the trim gate to committed-index durability.
- **`crates/broker/src/producer_state.rs`** — a rebuild/populate entry point.
- **A new diskless crash-model file** (alongside `data_path_model.rs`).
- **Ack/hot-read paths** — untouched.

## Kafka / KIP compliance

- **Crash offset gaps are benign.** The `[B,B+N)` gap and any torn-tail gap are legal — batches self-delimit, consumers skip; `ListOffsets`/HW stay consistent with the re-anchored frontier.
- **Idempotence preserved.** Producer-sequence dedup is rebuilt on restart, so an idempotent retry after a crash is neither duplicated nor spuriously rejected (`OutOfOrderSequence`).
- **No wire change.** Recovery is internal; clients see a partition that came back with its acked data intact.

## Testing

- **No-acked-loss (the shipping gate):** the stateright model asserts `wal_acked_durable` across every `{WalAppend, WalFsync, KraftAssign, ObjectPut, IndexPublish, Trim, Crash, Recover}` interleaving, with `sometimes` witnesses for crash-in-`[KraftAssign,WalFsync)`, crash-`[PUT,IndexPublish)`, and crash-mid-fsync.
- **Torn-tail recovery (behavior):** append batches, physically truncate the active segment mid-batch, reopen — `log_end_offset` = one past the last fully-written batch; the epoch checkpoint has no entry past it.
- **KRaft re-anchor:** simulate `log_end_offset() < KRaft frontier` on open; the next `append_verbatim_at` succeeds at `base == reconciled_frontier` (not `OffsetMismatch`); the gap is consumer-visible-benign.
- **Producer dedup rebuild:** produce idempotent batches, crash-restart, replay the last batch → deduped (no duplicate in the log), and a fresh sequence is accepted — keyed to the recovered LEO.
- **Trim ≤ index-durable:** a trim is never issued past an offset absent from the committed index; a fetch below the trimmed floor is always cold-served (no `[below-floor ∧ cache-miss]`).
- **Node-loss is out of scope, explicitly:** the model's `NodeLoss(b)` shows the un-flushed tail is *not* recoverable (Slice-6 obligation), while flushed offsets remain recoverable.

## Risks (carried into the plan)

- **Trim racing ahead of index durability** — the one window that loses acked data; enforce + prove `trim ≤ committed-index-frontier`.
- **Producer dedup breaking after restart** — the rebuild must key `last_sequence` to the post-truncation LEO or it wrongly dedups/duplicates.
- **Dangling leader-epoch entry** past the recovered LEO — add the missing checkpoint truncation.
- **Validation-off silently disabling tail recovery** — force `validate_on_open` for diskless.
- **Re-anchor masking a real gap** — derive `reconciled_frontier` only from the committed KRaft frontier, persist it, and keep `base ≥ log_end_offset`.
- **Vacuous proof** — mandatory `sometimes` witnesses so crashes actually land mid-sequence.
- **Mis-drawn scope** — the spec confines "stateless recovery" to flushed data and models node-loss as out-of-scope.

## Resolved decisions (from brainstorming)

- **Scope:** crash-restart atomicity + no-acked-loss (single node); node-loss + object-side producer snapshots → Slice 6.
- **Producer state:** rebuild sequence dedup from the WAL; **defer** durable PID allocation (a pre-existing classic gap).
- **Seam A:** re-anchor the append cursor to the KRaft frontier (not gap-fill placeholders).
- **Seam E2:** `trim ≤ committed-index-frontier` (tighten Slice 4's in-memory gate to durability).
- **Proof:** a separate, tighter diskless-only stateright model with partial-durability injection; `NodeLoss` explicitly out of scope; stateright-only.
- **Deferred:** orphan-object GC, transactional/LSO recovery, Creusot.
