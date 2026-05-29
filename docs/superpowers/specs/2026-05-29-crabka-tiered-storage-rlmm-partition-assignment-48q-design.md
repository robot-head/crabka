# Crabka tiered storage 48q — Per-broker metadata-partition assignment (design)

**Date:** 2026-05-29
**Status:** Slice design. Depends on 48o (assign + seek) and 48p
(snapshot/resume). Closes a 48f follow-up. Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Stop every broker from consuming all `__remote_log_metadata` partitions.
A broker should consume only the metadata partitions that carry metadata
for user-topic-partitions it leads or follows, and adjust that set
dynamically as leadership changes — matching Kafka's TBRLMM, where
consumer load scales with a broker's share rather than the whole cluster.

## The gap

Today every broker consumes the entire metadata topic with a unique group
id (`kafka_log.rs:200`, `subscribe(vec![topic])`). The 48f design records
this as a deliberate first cut: *"consumes all metadata-topic partitions
on every broker. Partition-set assignment is an optimization deferred to a
follow-up."* 48o made the consumer assignment-driven; 48q supplies the
assignment.

## Approach (dynamic, leadership-driven)

### Deriving the needed set

The needed metadata-partition set for this broker is:

```
{ metadata_partition_for(tp, N)
  : tp ∈ user-partitions this broker leads or follows }
```

`metadata_partition_for` already exists
(`crates/remote-storage-topic/src/partitioning.rs`), hashing
`(topic_id, partition)` into `[0, N)`. The broker reconciles the
`MetadataImage` continuously; compute the set from the image's partition
assignments for this `node_id` and publish it on a `watch` channel that
re-emits whenever the image changes.

### Driving the consumer assignment

A reconciler task in `manager.rs` (or the broker bootstrap) subscribes to
the watch and diffs against the current `AssignmentHandle::assigned()`:

- **added** partition → `handle.add(PartitionStart { partition,
  start_offset: snapshot_committed + 1 })` (reuses 48p's resume offsets;
  falls back to 0 when the snapshot has nothing for it). It then catches
  up from that offset to the partition's current HWM.
- **removed** partition → `handle.remove(partition)`.

### Readiness gate (correctness)

A user-partition the broker has *newly* become leader for must not serve
remote reads from a metadata partition that hasn't caught up yet —
otherwise `remote_log_segment_metadata` would return a misleading "no
segment" and the consumer would see a spurious end-of-tier.

Track per-metadata-partition readiness: a partition is *ready* once the
pump's `applied[partition]` (from `manager.rs`) reaches the HWM observed
at assignment time. Add to the RLMM read surface a way to distinguish
"not ready" from "no segment":

```rust
/// Err(NotReady) when the metadata partition for `tp` is assigned but
/// not yet caught up; Ok(None) only when caught up and no segment
/// covers the offset.
fn remote_log_segment_metadata(&self, tp, epoch, offset)
    -> Result<Option<RemoteLogSegmentMetadata>, RemoteStorageError>;
```

`RemoteReader::fetch_batch` propagates `NotReady`; `try_remote_read`
(`fetch.rs`) treats it as retryable — leave `OFFSET_OUT_OF_RANGE` so the
consumer retries — rather than a definitive miss. `ListOffsets`
(`list_offsets.rs`) treats `NotReady` the same as its existing remote-error
branch (warn + conservative answer). A metadata partition not assigned at
all (the broker neither leads nor follows the user-partition) is a genuine
`Ok(None)`.

### Bootstrap interaction

`bootstrap_topic_rlmm` (`crates/broker/src/broker.rs:2101`) currently starts
the RLMM and swaps it in behind `SwappableRlmm`. With 48q, the initial
assignment is the leadership-derived set (not all partitions); the watch
keeps it current thereafter.

## Files

- `crates/broker/src/broker.rs` — derive the needed set from
  `MetadataImage`, publish the watch, run the assignment reconciler, wire
  readiness into bootstrap.
- `crates/remote-storage-topic/src/manager.rs` — consume the watch, drive
  `AssignmentHandle`, track per-partition readiness, surface `NotReady`.
- `crates/remote-storage/src/metadata_manager.rs` /
  `crates/remote-storage/src/error.rs` — `NotReady` error variant on the
  read path.
- `crates/broker/src/handlers/{fetch.rs, list_offsets.rs}` — treat
  `NotReady` as retryable / conservative.
- `crates/remote-storage-topic/src/partitioning.rs` — reused as-is.

## Testing

- Assignment derivation: a replica set for `node_id` maps to the expected
  metadata-partition set via `metadata_partition_for`.
- Gaining a partition: `add` triggers catch-up; reads return `NotReady`
  (retryable `OFFSET_OUT_OF_RANGE`) until `applied` reaches HWM, then real
  segments.
- Losing a partition: `remove` stops consumption; subsequent reads for it
  are `Ok(None)`.
- Multi-broker loopback: two brokers split a topic's partitions; assert
  each consumes only its share of metadata partitions and both serve
  remote reads correctly for their own partitions.

## Non-goals

- No consumer-group-based assignment — the RLMM owns assignment directly
  (no rebalance protocol).
- No change to `metadata_partition_for` hashing (stable, rename-invariant).

## Dependencies & sequencing

Requires 48o (assignment API) and 48p (resume offsets + readiness HWM
tracking). Last of the topic-RLMM slices. Touches `manager.rs` (shared
with 48p), so sequence after 48p.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-remote-storage-topic -p crabka-broker`
- `cargo test --workspace` (no regressions)
- No CRD drift.
