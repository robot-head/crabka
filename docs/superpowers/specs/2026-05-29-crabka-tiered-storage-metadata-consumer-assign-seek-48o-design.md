# Crabka tiered storage 48o — Metadata-log consumer: assign + seek (design)

**Date:** 2026-05-29
**Status:** Slice design. Foundation for 48p (snapshot) and 48q
(per-broker partition assignment). Part of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Give the `__remote_log_metadata` consumer the two capabilities the
topic-based RLMM scaling work needs and that the high-level group
`Consumer` does not provide: **consume a chosen subset of partitions**,
each **starting at a chosen offset**, with the subset **mutable at
runtime**. This slice builds the mechanism; behavior stays equivalent to
today (assign all partitions from offset 0), so it lands and is tested in
isolation before 48p/48q exploit it.

## Why a foundation slice

`KafkaMetadataEventLog` (`crates/remote-storage-topic/src/kafka_log.rs`)
consumes via the high-level group `Consumer`:

```rust
// kafka_log.rs:325
Consumer::builder()
    .bootstrap(bootstrap)
    .group_id(group_id)                 // unique per subscriber
    .subscribe(vec![topic])             // ALL partitions
    .auto_offset_reset(AutoOffsetReset::Earliest)  // always from 0
    .build()
```

The group `Consumer` is subscription-based; manual partition assignment
and offset-seek live at the `crabka-client-core` layer
(`crates/client-consumer/src/lib.rs:39`: *"assign() (manual partition
consumption) — use crabka-client-core"*). 48p needs to resume from a
committed offset; 48q needs to consume only assigned partitions. Both
require dropping below the group consumer.

## Approach

### New `MetadataEventLog` subscription API

Today (`crates/remote-storage-topic/src/log.rs:51`):

```rust
pub trait MetadataEventLog: Send + Sync {
    fn partition_count(&self) -> i32;
    async fn publish(&self, partition: i32, event: Bytes) -> Result<i64, MetadataLogError>;
    fn subscribe(&self) -> MetadataEventStream;
    async fn high_water_marks(&self) -> Result<Vec<i64>, MetadataLogError>;
}
```

Crabka is greenfield — change the trait freely. Replace the all-partitions
`subscribe` with an assignment-driven one:

```rust
/// Start consuming the given partitions, each from its start offset
/// (inclusive). Returns the event stream plus a handle to mutate the
/// live assignment.
fn subscribe(&self, assignment: Vec<PartitionStart>) -> (MetadataEventStream, AssignmentHandle);

pub struct PartitionStart { pub partition: i32, pub start_offset: i64 }

pub trait AssignmentHandle: Send + Sync {
    /// Begin consuming `partition` from `start_offset`; no-op if already
    /// assigned. Newly-added partitions emit their backlog into the
    /// existing stream.
    fn add(&self, start: PartitionStart);
    /// Stop consuming `partition` and stop emitting its events.
    fn remove(&self, partition: i32);
    /// Current assigned partition set.
    fn assigned(&self) -> Vec<i32>;
}
```

`MetadataEventRecord` (partition, offset, payload) is unchanged, so the
`manager.rs` pump's per-record handling and `applied[partition]` tracking
are unaffected — only how the stream is constructed changes.

### `KafkaMetadataEventLog` on client-core

Rework the consumer off the group `Consumer` onto `crabka-client-core`
manual `Fetch` loops. Internally maintain one task per assigned partition
(or one multiplexed task driven by an assignment map): for each
`(partition, next_offset)`, issue `Fetch` to the partition leader, decode
batches into `MetadataEventRecord`s, advance `next_offset`, emit into the
shared `mpsc`. `AssignmentHandle::add`/`remove` spawn/cancel partition
tasks. No consumer group, no offset commit to the broker — the read
position is owned by the RLMM (in memory now; persisted by 48p).

If client-core lacks a small "fetch one partition from offset N" helper,
add it there (a thin wrapper over the existing `Connection` + `Fetch`
path); keep it minimal and reusable.

### Manager + fixture

- `manager.rs::start` calls `subscribe` with the full assignment from
  offset 0 (behavior-preserving) and keeps the `AssignmentHandle` for
  48p/48q to drive. `pump_loop` is unchanged.
- `InProcessMetadataEventLog` (`log.rs:149`) implements the new API: its
  `subscribe` filters the in-memory backlog to the assigned partitions
  starting at each `start_offset`, then forwards live appends only for
  assigned partitions; `add`/`remove` mutate the filter set.

## Files

- `crates/remote-storage-topic/src/log.rs` — trait change,
  `PartitionStart`, `AssignmentHandle`, InProcess fixture.
- `crates/remote-storage-topic/src/kafka_log.rs` — client-core
  manual-fetch consumer + assignment handle.
- `crates/remote-storage-topic/src/manager.rs` — call the new
  `subscribe`; hold the handle.
- `crates/client-core` — manual single-partition fetch helper, if not
  already present.

## Testing

- InProcess fixture: subscribe to a subset → only those partitions'
  events arrive; `start_offset > 0` skips earlier events; `add` mid-stream
  delivers the new partition's backlog then live; `remove` stops delivery.
- Loopback integration (`KafkaMetadataEventLog` against a real broker, as
  in `tiered_storage_topic_rlmm.rs`): publish across partitions, subscribe
  to a subset from a non-zero offset, assert exactly the expected records.
- Regression: existing topic-RLMM round-trip tests pass unchanged (the
  manager still assigns all partitions from 0).

## Non-goals

- No snapshot/resume yet (48p) — start offsets are still 0 here.
- No leadership-derived assignment yet (48q) — the manager assigns all
  partitions.
- No security changes (48r).

## Dependencies & sequencing

Lands after the 48m/48n batch. Prerequisite for 48p and 48q. Touches
`kafka_log.rs`, so it cannot run in the same batch as 48r (which also
touches `kafka_log.rs`); sequence 48r after 48o.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-remote-storage-topic -p crabka-broker`
- `cargo test --workspace` (no regressions)
- No CRD drift.
