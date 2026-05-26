# Crabka tiered storage 48e — Remote retention + partition delete (design)

**Date:** 2026-05-26
**Status:** Slice design. Follows slice 48d (remote read path). Part of
the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Two complementary lifecycle paths through the
[`DeleteSegmentStarted` → `DeleteSegmentFinished`] and
[`DeletePartitionMarked` → `DeletePartitionStarted` →
`DeletePartitionFinished`] state machines that 48a defined but no caller
has driven yet:

1. **Remote retention.** The broker periodically evicts remote-tier
   segments whose age exceeds the topic's `retention.ms` (the total
   retention, not the local-only one) or that fall outside its
   `retention.bytes` budget, mirroring what `Log::tick` does for local
   segments on non-tiered topics.
2. **Topic-delete cascade.** When `DeleteTopics` removes a tiered topic,
   the broker tears down the local partition dirs (existing 48b path)
   AND deletes every remote-tier segment that partition ever offloaded.

Together they make tiered storage operationally complete: a tiered
topic's remote-tier footprint shrinks under retention pressure and is
reclaimed on topic delete instead of leaking forever.

## Non-goals (still deferred)

- `TopicBasedRemoteLogMetadataManager` — 48f.
- Object-store RSM — 48f / later.
- Operator CRD surface — 48g.
- Read-committed aborted-transaction filtering on remote batches
  (slice-48d note).

## Remote retention — `RemoteLogManager` extension

`crates/broker/src/remote_log_manager.rs` grows a remote-retention
pass that runs after the local-retention pass on each tick.

```rust
async fn tick_all(...) {
    // ... existing snapshot ...
    for partition in snapshot {
        if !leader || !remote_storage_enable { continue; }
        copy_eligible(...).await;        // existing (48b)
        local_retention_pass(...).await; // existing (48c)
        remote_retention_pass(...).await; // NEW
    }
}
```

### Algorithm (per partition)

1. Snapshot the topic's `LogConfig.retention_ms` /
   `LogConfig.retention_bytes` (the **total** retention, same fields the
   non-tiered `Log::tick` consults).
2. List all `CopySegmentFinished` segments from the RLMM, ordered by
   `start_offset`.
3. Walk oldest-first. A segment is deletable when **either**:
   - `now_ms - seg.max_timestamp_ms > retention_ms`, or
   - the running sum of segment sizes from the oldest forward exceeds
     `total_bytes - retention_bytes` (greedy size-based eviction; same
     shape as `local_retention_target`).
4. The walk **stops** at the first segment that is not deletable —
   keeps the remote prefix contiguous, matches the local-retention pass
   and Kafka's behavior.
5. For each deletable segment, run the full lifecycle:
   - `update_remote_log_segment_metadata(state =
     DeleteSegmentStarted)`
   - `rsm.delete_log_segment_data(metadata)` (on `spawn_blocking`)
   - `update_remote_log_segment_metadata(state =
     DeleteSegmentFinished)`
   Any failure logs at WARN and short-circuits the partition's pass;
   leftover `DeleteSegmentStarted` metadata is harmless (the next tick
   sees it via list but it is filtered out of the
   `CopySegmentFinished`-only readable set).

### Pure-logic helper

To keep the algorithm testable without tokio + RSM, extract:

```rust
pub(crate) fn remote_retention_eviction_set(
    finished: &[RemoteLogSegmentMetadata],
    retention_ms: Option<i64>,
    retention_bytes: Option<u64>,
    now_ms: i64,
) -> Vec<RemoteLogSegmentMetadata>;
```

Returns the segments (in oldest-first order) the pass should delete.
Empty when nothing qualifies. Unit-tested against synthetic metadata
vectors.

### Effective retention values

Total `retention.ms` / `retention.bytes` on tiered topics already flow
through `LogConfig` (slice 11/12 admin handlers; same fields the
non-tiered `Log::tick` uses). 48e reads them via
`partition.log.lock().config_snapshot()`, no new config keys.

The local-tighter-than-total invariant is enforced operationally, not
by Crabka: if an operator sets `local.retention.ms > retention.ms`, the
remote retention pass will evict segments that the local-retention pass
hasn't yet deleted locally. This is unusual but well-defined — fetches
below the new remote floor still try the remote tier, get `None`
(segment gone), and surface `OFFSET_OUT_OF_RANGE`.

### `log_start_offset` does NOT split in 48e

48c noted "the two pointers only split in 48e". On further inspection,
the split is not actually needed for remote retention to be correct:

- After local retention (48c), `log_start_offset()` ==
  `local_log_start_offset()`, both set to the local floor. The remote
  tier holds segments below that floor.
- `ListOffsets EARLIEST` (48d) already returns
  `min(local_log_start, remote_earliest)`, so the public earliest stays
  correct.
- After remote retention deletes the oldest remote segments, the
  remote tier's earliest moves up. `EARLIEST` recomputes on every
  request — no log_start mutation required.
- `Fetch` below the (still-elevated) local floor consults the remote
  tier; if the segment was just evicted, the RLMM lookup returns
  `None`, and the existing fall-through emits `OFFSET_OUT_OF_RANGE`.
  No log_start mutation required.

So `local_log_start_offset()` continues to delegate to
`log_start_offset()` in 48e. The split (truly distinct pointers) is
only needed if/when we want `Fetch.log_start_offset` in the response
to reflect the remote floor rather than the local one — but Kafka
clients tolerate the elevated local floor (`EARLIEST` is the source of
truth they actually consult). Leaving the split out of 48e keeps the
slice tight; documented in the slice notes.

## Topic-delete cascade — `DeleteTopics` extension

`crates/broker/src/handlers/delete_topics.rs`. Before tearing down each
deleted topic's partition dirs (existing 48b path), capture the
`(topic_id, partition_id, remote_storage_enable)` triples. For each
partition with `remote_storage_enable = true`, spawn a detached task
that runs the partition-delete cascade against the broker's
`RemoteReader`:

```rust
async fn cascade_remote_partition_delete(
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
    tp: TopicIdPartition,
    broker_id: i32,
) -> Result<(), RemoteStorageError> {
    write_partition_delete(&*rlmm, &tp, DeletePartitionMarked,  broker_id)?;
    write_partition_delete(&*rlmm, &tp, DeletePartitionStarted, broker_id)?;
    for md in rlmm.list_remote_log_segments(&tp)? {
        delete_one(&*rsm, &*rlmm, &md, broker_id).await;
    }
    write_partition_delete(&*rlmm, &tp, DeletePartitionFinished, broker_id)?;
    Ok(())
}
```

`delete_one` mirrors `rollback()` from 48b: transition the segment
to `DeleteSegmentStarted`, then call `rsm.delete_log_segment_data` on
the blocking pool, then transition to `DeleteSegmentFinished`. Failures
are logged at WARN — leftover delete-started segments are not
re-discovered on a tiered-topic-recreate (Kafka regenerates topic_id
on every create, and `TopicIdPartition` equality is by id +
partition).

Spawning is detached because the response should not wait on remote
I/O; in-progress deletes that outlive the broker process restart are
self-recoverable for the production
`TopicBasedRemoteLogMetadataManager` (the marker survives in the
internal topic). For 48e's in-memory RLMM, a restart loses both the
markers and the segments-on-the-RSM together (the in-mem manager
forgets everything anyway), so detached fire-and-forget is fine.

### Why not call `RemoteReader` directly?

`RemoteReader` exposes read-side helpers. The cascade needs both rsm
and rlmm. The simplest plumbing: expose `RemoteReader.rsm` /
`RemoteReader.rlmm` (already `pub(crate)`) and call a free
`cascade_remote_partition_delete` defined alongside the remote-retention
helper in `remote_log_manager.rs`. Keeps the read path and write path
in their respective modules.

### Tier-state probe order

The handler captures `remote_storage_enable` from
`partition.log.lock().config_snapshot()` BEFORE removing the partition
from the `DashMap` and BEFORE the `submit_change` to the controller
(both succeed before any teardown). After teardown, the snapshot is the
sole record of which partitions need the cascade.

## Test plan

### Pure-logic helper (`remote_log_manager.rs::remote_retention_eviction_set`)

- `returns_empty_when_no_segments` — empty input, any settings.
- `time_based_eviction_picks_oldest_until_first_in_window` — synthetic
  segments with strictly increasing `max_timestamp_ms`; retention
  picks the prefix older than the window; stops at the first in-window
  one.
- `size_based_eviction_evicts_oldest_first` — total > budget; picks
  oldest segments until cumulative-from-newest fits.
- `time_and_size_combined_takes_union_of_either_match` — a segment
  qualifies if **either** condition holds; verify a segment chosen by
  time but not size is included.
- `none_settings_disable_each_axis` — `retention_ms = None` + finite
  bytes: no time eviction. Vice versa.
- `walk_stops_at_first_non_deletable` — gap in the middle (an
  in-window segment between two old ones); the helper stops at the
  in-window one even though a later segment is also deletable
  (preserves contiguous remote prefix).

### Integration: `remote_retention_pass` against real RSM + RLMM

- `remote_retention_pass_evicts_old_segments_through_lifecycle` —
  populate 3 finished segments; drive the pass with a tight
  `retention_ms`; assert all 3 are gone from `list_remote_log_segments`
  (DeleteFinished drops them) and `rsm.fetch_log_segment` errors with
  `SegmentNotFound`.
- `remote_retention_pass_noop_when_nothing_qualifies` — fresh segments
  + large retention; assert zero evictions.
- `remote_retention_pass_handles_partial_delete_failure` — wire an RSM
  whose `delete_log_segment_data` errors on the second call; verify the
  first segment is fully cleaned, the second is left in
  `DeleteSegmentStarted` (still listed by `list_remote_log_segments`
  but not in the finished-only set).

### Integration: partition delete cascade

- `cascade_remote_partition_delete_drops_every_segment` — populate
  several finished segments; run the cascade; assert `list…` is empty,
  partition-delete state is `DeletePartitionFinished`.
- `cascade_remote_partition_delete_is_noop_on_empty_partition` — no
  segments; cascade walks all three partition states without error.

### DeleteTopics integration (`crates/broker/tests/`)

- `tests/remote_partition_delete.rs::delete_topic_cascades_to_remote_segments`
  — one broker, one tiered topic, produce + wait for copy + delete
  topic; assert the RSM-backed directory for that partition is empty
  AND `list_remote_log_segments(tp)` returns `[]`. Since the broker
  test fixture uses `LocalTieredStorage`, the RSM dir is observable on
  disk.

  This is the only broker-level integration test in the slice — the
  remote-retention pass is tested at the lib level (same pattern 48c
  used for `local_retention_drive`).

### No-regression sweep

- Existing tests under `remote_log_manager::tests` cover the copy +
  local-retention paths. The new `remote_retention_pass` runs after
  them; tests that previously asserted "no remote segment deleted"
  must keep their `now_ms` within the retention window. Verified by
  re-running the slice-48b/c tests unchanged.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-broker -p crabka-remote-storage -p crabka-log`
- `cargo build --workspace`
- No CRD drift (no CRDs touched).
