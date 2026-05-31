# KIP-595 Slice 3b — KRaft replicated log over crabka-log

Date: 2026-05-31
Status: Approved (brainstorming) — pending spec review

## Context

Slice 3 replaces openraft with a hand-rolled KRaft engine, decomposed 3a–3d
(incremental; openraft stays live until the 3c cutover). 3a delivered the pure
consensus core (`crates/raft/src/kraft/`) with an injected `LogView` seam. 3b
supplies the real log behind that seam.

Exploration of `crabka_log::Log` (`crates/log/src/`) confirmed it already
provides every primitive needed: `append` / `append_at` (the latter preserves a
leader-assigned offset on followers), `read_raw(fetch_offset, limit_offset,
max_bytes)` (verbatim wire bytes), `truncate_to(offset)`, `log_start_offset` /
`log_end_offset`, and a per-partition leader-epoch checkpoint
(`epoch_checkpoint().end_offset_for_epoch(epoch, log_end)`, with per-batch
`partition_leader_epoch` recorded on append). crabka-log does NOT track a
high-watermark — that is consensus state the facade adds.

## Goal & scope

Build `KraftLog`, a thin facade over `crabka_log::Log` that adds the consensus
semantics the 3a core needs (HWM tracking, committed-read filtering for `Fetch`,
divergence lookup) and implements the 3a `LogView` trait. Prove it by re-backing
the 3a multi-node simulation with real `KraftLog` instances (tempdirs).

**In scope:** the `KraftLog` facade + `LogView` impl; standalone unit tests; the
core-over-real-log integration test.

**Out of scope:** the production async/TCP driver, `quorum-state` file
persistence, and removing openraft / `log_store.rs` — all Slice 3c. Snapshot /
purge interaction — Slice 4. openraft remains the live engine; 3b is additive.

## Components

### `KraftLog` facade (`crates/raft/src/kraft/log.rs`)

Wraps `crabka_log::Log` plus an in-memory `hwm: i64`:

- `open(dir, config) -> Result<KraftLog, RaftError>` — opens/creates the log;
  initializes `hwm = log.log_start_offset()`.
- `append(&mut self, batch: &mut RecordBatch) -> Result<i64, RaftError>` —
  leader path; `log.append` assigns the offset and records the batch's
  `partition_leader_epoch` in the epoch checkpoint. Returns the assigned offset.
- `append_at(&mut self, batch: &mut RecordBatch, offset: i64) -> Result<(), RaftError>`
  — follower path; preserves the leader-assigned offset (`log.append_at`,
  which validates `offset == log_end_offset`).
- `read_committed(&self, offset: i64, max_bytes: usize) -> Result<RawRead, RaftError>`
  — serves KIP-595 `Fetch`: `log.read_raw(offset, self.hwm.min(log_end),
  max_bytes)`. Verbatim wire bytes, never beyond the committed (HWM) range.
- `truncate_to(&mut self, offset: i64) -> Result<(), RaftError>` —
  `log.truncate_to(offset)`, then `hwm = hwm.min(offset)`.
- `advance_hwm(&mut self, new_hwm: i64)` — `hwm = hwm.max(new_hwm).min(log_end)`
  (monotonic; never past the log end).
- Accessors: `hwm()`, `log_end_offset()`, `log_start_offset()`.

### `LogView` impl for `KraftLog`

Bridges the 3a core's `LogView` to crabka-log:

- `end_offset()` → `log.log_end_offset()`
- `last_epoch()` → `epoch_checkpoint().latest_epoch().unwrap_or(0)` (i32 →
  `LeaderEpoch` u32 conversion at the boundary)
- `end_offset_for_epoch(epoch)` →
  `epoch_checkpoint().end_offset_for_epoch(epoch as i32, log_end)`, mapping the
  `-1` (UNDEFINED_OFFSET) sentinel to `None`.

## Data flow

```
3a core Action          KraftLog call                       crabka_log
-------------------     -------------------------------     -----------------
AppendLeaderChange  ->  append(leader_change_batch)     ->  Log::append
(leader appends)        append(record_batch)
SendFetch/replicate ->  read_committed(off, max)        ->  Log::read_raw
(follower applies)  ->  append_at(batch, off)           ->  Log::append_at
AdvanceHighWatermark->  advance_hwm(n)                  ->  (facade state)
TruncateTo(off)     ->  truncate_to(off)               ->  Log::truncate_to
core reads          ->  LogView {end_offset,last_epoch,  ->  log_end_offset,
                          end_offset_for_epoch}              epoch_checkpoint
```

## Acceptance / testing

- **Standalone unit tests (`KraftLog`):** append varied-epoch batches → read
  back → `LogView` queries (`end_offset`, `last_epoch`, `end_offset_for_epoch`
  including unknown→`None`) → `truncate_to` at an epoch boundary (assert log-end
  + hwm both drop) → `read_committed` never returns bytes past HWM →
  `advance_hwm` monotonic + clamped to log end.
- **Core-over-real-log integration (headline):** generalize the 3a simulation
  harness so each node's log is a real `KraftLog` on a tempdir. The harness maps
  the core's log-related `Action`s onto the real log (`AppendLeaderChange` /
  `leader_append` → `append`; `SendFetch`/`ReceiveFetch` → `read_committed` on
  the leader + `append_at` on the follower; `AdvanceHighWatermark` →
  `advance_hwm`; `TruncateTo` → `truncate_to`). Assert, on top of the 3a
  invariants:
  - after replication settles, **all voters' on-disk logs are byte-identical up
    to the HWM** (compare `read_committed` bytes);
  - HWM agrees across voters and never exceeds any voter's log end;
  - a forced-divergence scenario (a follower with a conflicting-epoch tail)
    drives a `TruncateTo` that actually shortens the follower's `KraftLog`, after
    which it re-converges to the leader's log.
- Deterministic: tempdirs + the logical clock; file IO is deterministic.

## Error handling

`crabka_log::LogError` is surfaced through `RaftError` (the crate's existing
error). Invariants guarded with `debug_assert!` (`hwm <= log_end_offset`;
`truncate_to` target `>= log_start_offset`). A `Fetch` below `log_start_offset`
(records compacted away) returns the available range plus a flag the caller maps
to "needs a snapshot" — but snapshots are Slice 4, so 3b only returns the
available range and records the gap; it does not build snapshot responses.

## Disposition

Permanent. `KraftLog` is the log the 3c driver wires into `ControllerHandle`,
ultimately replacing `log_store.rs`. It carries no openraft dependency and is
unit-testable in isolation.
