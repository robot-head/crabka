# Crabka tiered storage 48c — Local retention split (design)

**Date:** 2026-05-26
**Status:** Slice design. Follows slice 48b (copy path). Part of the
KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Once a sealed segment has been durably offloaded to the remote tier
(`CopySegmentFinished` in the RLMM), let the broker delete the local
copy on a **local-retention** schedule that is independent of (and
typically tighter than) the topic's total retention. Introduce
`local-log-start-offset` distinct from `log-start-offset`, and stop
the standard `Log::tick()` retention from clobbering uncopied segments
on tiered topics.

This is the **local-retention path only**. The remote read path on
`Fetch` (48d) and remote-tier retention / partition delete (48e) are
deferred to later sub-slices. Until 48d ships, a client trying to
fetch below `local_log_start_offset()` will see `OFFSET_OUT_OF_RANGE`
just as if the data had been deleted by total retention — operators
who want the "local-tighter-than-total" window observable have to wait
for 48d.

## Config (consumed in this slice)

Kafka topic-level configs, validated + applied by
`crates/broker/src/config_keys.rs`:

- **`local.retention.ms`** — i64 ≥ `-2`. Kafka semantics:
  - `-2` (default): inherit `retention.ms`.
  - `-1`: unlimited (treat the same as `-2` here — see "Greenfield
    simplification" below).
  - `≥ 0`: that many milliseconds.
- **`local.retention.bytes`** — i64 ≥ `-2`. Same semantics, bytes.

Both map to new `LogConfig` fields:

```rust
/// Slice 48c (KIP-405): when a partition has `remote_storage_enable = true`,
/// local copies of segments that have been offloaded (`CopySegmentFinished`)
/// are deleted on this schedule. `None` = inherit `retention_ms`. Default
/// `None` (Kafka's `-2`).
pub local_retention_ms: Option<Duration>,

/// Slice 48c (KIP-405): size budget for the local copy of a tiered
/// partition. `None` = inherit `retention_bytes`. Default `None`.
pub local_retention_bytes: Option<u64>,
```

**Effective values** (computed at retention time):

- `effective_local_ms = local_retention_ms.or(retention_ms)` — `None` =
  no time-based local cleanup.
- `effective_local_bytes = local_retention_bytes.or(retention_bytes)` —
  `None` = no size-based local cleanup.

**Greenfield simplification:** Kafka distinguishes `local.retention.ms
= -1` ("unlimited local") from `-2` ("inherit"). For tiered topics the
practical difference is negligible (you'd never set local retention
*longer* than total retention), so 48c collapses both to "inherit"
(`None`). The wire-side validate accepts both for compatibility with
`kafka-configs`, and the apply path maps both to `None`. Documented in
the slice notes.

## Log-crate surface (new / changed)

### `LogConfig` fields (above).

### New methods on `Log`

```rust
impl Log {
    /// Slice 48c: first absolute offset still present on this broker's
    /// local disk. Equals [`log_start_offset`] until remote-tier retention
    /// (48e) starts evicting segments that are still locally present —
    /// at which point the two split.
    pub fn local_log_start_offset(&self) -> i64 { ... }

    /// Slice 48c: physically delete every sealed segment whose
    /// `last_offset < target`, then bump `local_log_start_override` so
    /// the next reader sees `target` as the floor. Active segment never
    /// touched. Returns the count of segments removed. No-op if `target
    /// <= local_log_start_offset()`.
    ///
    /// The caller is responsible for verifying the segments are safely
    /// in the remote tier (`CopySegmentFinished`) before invoking this;
    /// `Log` itself enforces no tiered-storage invariants. See
    /// `crates/broker/src/remote_log_manager.rs` for the production
    /// caller.
    pub fn delete_local_segments_through(&mut self, target: i64) -> Result<usize, LogError> { ... }
}
```

`local_log_start_offset()` is the new accessor. For 48c it returns
`log_start_offset()` — `local_log_start_override` is a separate field
that tracks the local-only pointer, but the *current* invariant is
that `local_log_start_offset() == log_start_offset()`. Both pointers
co-evolve in 48c; they only diverge in 48e (remote-retention can
advance `log_start_offset` past `local_log_start_offset` when the
broker has data locally that is being evicted from the remote tier,
or the other way around).

Concretely: `delete_local_segments_through(target)` bumps both
`log_start_override` (via the existing `set_log_start_offset`) and
`local_log_start_override` to `target`. Today `local_log_start_offset()`
just delegates to `log_start_offset()`; the split is wired in 48e.
Naming the accessor now gives 48d a stable handle to use for the
"remote read floor" without a follow-up rename.

### `Log::tick()` change

For tiered topics, the standard retention path **must not** delete
segments — that's the local-retention task's job and only after copy
has finished. Concretely: when `config.remote_storage_enable` is
`true`, `Log::tick()` returns early (no time/size eviction). The
active-roll-on-age path stays in `tick()` for non-tiered and tiered
alike (no change there).

`Log::tick()` for tiered topics becomes a no-op for retention. The
`RemoteLogManager` is the sole driver of segment deletion on tiered
topics.

## Broker — `RemoteLogManager` extension

`crates/broker/src/remote_log_manager.rs` grows a local-retention
pass that runs after the copy pass on each tick.

```rust
async fn tick_all(...) {
    // ... existing snapshot ...
    for partition in snapshot {
        if !leader || !remote_storage_enable { continue; }
        let exports = log.tierable_segments();
        copy_eligible(...).await;     // existing
        local_retention_pass(...).await;   // NEW
    }
}

/// Compute the highest base_offset whose segment is safely
/// (`CopySegmentFinished`) in the remote tier **and** exceeds the
/// per-topic local-retention window; delete every local segment whose
/// `last_offset < target`.
async fn local_retention_pass(
    tp: &TopicIdPartition,
    partition: &Partition,
    exports: &[SegmentExport],
    rlmm: &dyn RemoteLogMetadataManager,
    now: SystemTime,
) { ... }
```

Algorithm (per partition):

1. Snapshot the topic's `LogConfig` for `local_retention_ms`,
   `local_retention_bytes`, `retention_ms`, `retention_bytes`.
2. Build `effective_local_ms`, `effective_local_bytes`.
3. Walk the local sealed segments (from `log.tierable_segments()`,
   oldest first):
   - For each segment, look up `rlmm.list_remote_log_segments(tp)`
     and check that **some** entry with the same `start_offset` is in
     `CopySegmentFinished` (matching by base offset is fine — slice
     48b's per-segment UUID guarantees no collision).
   - Compute `delete_by_time = effective_local_ms.is_some()
     && now_ms - seg.max_timestamp > effective_local_ms`.
   - Compute `delete_by_size` greedily: maintain a running total of
     sealed+active bytes; mark oldest-first as deletable until total
     ≤ `effective_local_bytes`.
   - The segment is **deletable** when it is `CopySegmentFinished`
     **AND** (`delete_by_time` OR `delete_by_size`).
4. Determine `target = max(delete_through_last_offset) + 1` across
   all deletable segments (i.e. the first offset to keep).
5. Call `log.delete_local_segments_through(target)`.

Important invariants:

- Only `CopySegmentFinished` segments are eligible. A segment in
  `CopySegmentStarted` (in-flight copy) or unknown to the RLMM is
  retained locally.
- The active segment is never deleted (`Log` enforces this).
- The deletion path is greedy oldest-first; if segment N is not
  yet `CopySegmentFinished`, segments N+1, N+2, ... are NOT
  considered for deletion (keeps the local prefix contiguous;
  matches Kafka's behavior).

### Pure-logic helper

To make this testable independent of tokio + DashMap, extract:

```rust
pub(crate) fn local_retention_target(
    exports: &[SegmentExport],
    finished_bases: &HashSet<i64>,
    active_size: u64,
    effective_local_ms: Option<i64>,
    effective_local_bytes: Option<u64>,
    now_ms: i64,
) -> Option<i64> { ... }
```

Returns the `target` to pass to `delete_local_segments_through`, or
`None` if nothing is deletable. Unit-tested against synthetic
`SegmentExport` vectors. The async `local_retention_pass` is then a
thin wrapper that gathers inputs and calls the helper.

## Out of scope (48d / 48e+)

- Remote read path on `Fetch` / `ListOffsets` (48d). Fetch below
  `local_log_start_offset()` still returns `OFFSET_OUT_OF_RANGE` in
  this slice — `log_start_offset()` and `local_log_start_offset()`
  are equal for the time being.
- Remote-tier retention + segment eviction (`DeleteSegmentStarted`
  by total-retention) — 48e.
- Topic-delete cascade through remote tier — 48e.
- `TopicBasedRemoteLogMetadataManager` — 48f.
- Object-store RSM — 48f / later.
- Operator CRD surface — 48g.

## Test plan

- **`crates/log/src/config.rs`** — `LogConfig` Default has both
  `local_retention_ms` and `local_retention_bytes` = `None`. New
  test: defaults are `None`.
- **`crates/log/src/log.rs`**:
  - `local_log_start_offset_matches_log_start_offset` (the 48c
    invariant).
  - `delete_local_segments_through_drops_sealed_below_target`.
  - `delete_local_segments_through_keeps_active_segment`.
  - `delete_local_segments_through_advances_local_start_pointer`.
  - `delete_local_segments_through_is_noop_at_or_below_current_start`.
  - `tick_skips_retention_when_remote_storage_enable_is_true` —
    appends, rolls, calls `tick(far_future)`; sealed segments survive
    because tiered-topic retention is the RemoteLogManager's job.
- **`crates/broker/src/config_keys.rs`**:
  - `validate_local_retention_ms_accepts_minus_one_minus_two_and_positive`.
  - `validate_local_retention_ms_rejects_below_minus_two`.
  - `is_recognized_includes_local_retention_keys`.
  - `apply_local_retention_ms_minus_two_means_inherit`.
  - `apply_local_retention_ms_positive_propagates`.
  - `apply_local_retention_bytes_propagates`.
- **`crates/broker/src/remote_log_manager.rs`**:
  - `local_retention_target_returns_none_when_no_finished_segments`.
  - `local_retention_target_time_based_eviction`.
  - `local_retention_target_size_based_eviction`.
  - `local_retention_target_skips_unfinished_segments_and_stops`.
  - `local_retention_target_falls_back_to_retention_ms_when_local_is_none`.
  - `local_retention_pass_deletes_copied_segments` — integration:
    real `Log` + `LocalTieredStorage` + `InmemoryRemoteLogMetadataManager`;
    copy then run the retention pass with a tight local.retention.ms;
    assert sealed copies are gone, active segment + uncopied ones stay,
    `local_log_start_offset()` advances.
  - `local_retention_pass_noop_when_remote_storage_disabled` — sanity
    check that the new path is gated.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-log -p crabka-broker`
- `cargo build --workspace`
- No CRD drift (no CRDs touched).
