# KIP-932 Share Groups — Slice D (Admin Offset RPCs) Design

**Date:** 2026-05-31
**Status:** Approved. Builds on Slices A (membership), B (persister), C (leader).
**KIP:** KIP-932.

## Goal

The three client-facing admin offset RPCs, served by the share **group** coordinator
(found via `FindCoordinator GROUP`), proxying to the Slice-B persister:
`DescribeShareGroupOffsets(90)`, `AlterShareGroupOffsets(91)`, `DeleteShareGroupOffsets(92)`.
Lets operators inspect, reset, and delete a share group's per-partition queue head (SPSO).

## RPC semantics

- **DescribeShareGroupOffsets(90, v0-1)** — ACL `Describe` on `Group`. Multi-group. Per
  requested `(topicName, partitions[])`: resolve name→`topic_id` via the metadata image; if
  `partitions` is empty, enumerate the group's initialized partitions from
  `ShareGroupStatePartitionMetadata`. For each, `persister.read_state` → `StartOffset` (SPSO),
  `LeaderEpoch`, and best-effort `Lag` (`high_watermark − SPSO` when the partition is local on
  this broker, else `-1`). Response carries `TopicId` (resolved). Unknown topic →
  `UNKNOWN_TOPIC_OR_PARTITION` per partition; missing state → `StartOffset = -1`.
- **AlterShareGroupOffsets(91, v0)** — ACL `Alter` on `Group`. Reset SPSO. Require the group
  **empty** (`find_share` + `Describe` → `members.is_empty()`; an absent group is acceptable);
  else top-level `NON_EMPTY_GROUP(68)`. Per `(topic, partition, startOffset)`: read current
  `state_epoch` via `persister.read_state` (default 0 if absent), then
  `persister.initialize(group, topic_id, partition, state_epoch + 1, startOffset)` — the
  bumped epoch passes the persister's fence and writes a fresh snapshot at the new SPSO,
  discarding prior in-flight state. Then **invalidate the leader cache** for that partition.
- **DeleteShareGroupOffsets(92, v0)** — ACL `Delete` on `Group`. Require group empty. Per
  requested topic: enumerate the group's initialized partitions for that `topic_id` (from
  `ShareGroupStatePartitionMetadata`); `persister.delete(group, topic_id, partition)` each;
  remove the topic from the group's `ShareGroupStatePartitionMetadata.initialized` (persist the
  updated v14 record via the share actor / offsets log); invalidate the leader cache for each.
  Unknown/never-initialized topic → `UNKNOWN_TOPIC_OR_PARTITION` (or success with no-op — match
  Kafka; default to error for an unknown topic).

## Dispatch & wiring

Inline-intercepted in `network/dispatch.rs` (mirror `delete_groups`/`describe_groups`) to get
`RequestContext { principal, peer, client_id }` for the ACL check. Add `Some(90/91/92)` arms +
frame handlers; add `handler_body_flexible` arms (all flexible from v0); advertise
`v!(describe_share_group_offsets_request)` / `alter…` / `delete…` in `api_catalog.rs`.

## New seams (added this slice)

1. `GroupCoordinator::share_state_partition_metadata(group) -> Option<ShareGroupStatePartitionMetadataValue>` — read a group's initialized `(topic_id, partitions)` + `deleting` from `share_seeds_cache` (none exists today). Used by Describe (enumerate-all) and Delete.
2. `SharePartitionLeaderManager::invalidate(group, topic_id, partition)` — drop the cached `AcquisitionState` so the next `ShareFetch` reloads from the persister. Called by Alter/Delete after mutating persisted state. Safe because the group is empty (no in-flight acquisitions). (Single-broker: the manager is local; cross-broker invalidation of a remote leader's cache is a Slice-F follow-up — note it.)

## Error handling

- ACL deny → top-level (Alter/Delete) or per-group (Describe) `GROUP_AUTHORIZATION_FAILED(30)`.
- Alter/Delete on a non-empty group → `NON_EMPTY_GROUP(68)`.
- Unknown topic name → `UNKNOWN_TOPIC_OR_PARTITION(3)` (per partition/topic row).
- Persister unreachable → `COORDINATOR_NOT_AVAILABLE(15)`.
- Feature disabled (`group.share.enable == false`) → `UNSUPPORTED_VERSION` (top-level).

## Non-goals

- Cross-broker leader-cache invalidation (single-broker tested; Slice F).
- `Lag` precision when the partition isn't local (best-effort `-1`).

## Testing (`tests/share_admin_offsets.rs`)

Typed-client integration (reuse `share_groups`/`share_state`/`share_consume` harness + the
share-state bootstrap + produce + join helpers):
1. **describe reflects SPSO:** produce + consume + accept some records (advance SPSO via the
   Slice-C path), then `DescribeShareGroupOffsets` returns the advanced `StartOffset` (and `Lag`
   = HWM − SPSO since the partition is local).
2. **alter resets (empty group):** initialize a partition; with no members, `AlterShareGroupOffsets`
   to `StartOffset = N`; then `DescribeShareGroupOffsets` shows `N`; a subsequent `ShareFetch`
   acquires starting at `N` (proves leader-cache invalidation).
3. **alter on non-empty group → NON_EMPTY_GROUP:** join (member present), `AlterShareGroupOffsets`
   → top-level `error_code == 68`.
4. **delete removes a topic:** initialize a topic's state; `DeleteShareGroupOffsets`; a later
   `DescribeShareGroupOffsets` for it shows `StartOffset = -1` / not-initialized.
5. **describe unknown topic → UNKNOWN_TOPIC_OR_PARTITION.**

## Acceptance gate

1. fmt clean. 2. `clippy --workspace --all-targets -- -D warnings` clean. 3. `cargo test --workspace`
green. 4. no drift. 5. 90/91/92 advertised + flexible arms. 6. All `tests/share_admin_offsets.rs`
cases green.

## Decomposition
- **D1:** `share_state_partition_metadata` accessor + `manager.invalidate` + the three handlers
  (`handlers/{describe,alter,delete}_share_group_offsets.rs`) + dispatch/flexible/api_catalog.
- **D2:** `tests/share_admin_offsets.rs`.
