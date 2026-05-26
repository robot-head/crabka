# Crabka tiered storage 48d — Remote read path (design)

**Date:** 2026-05-26
**Status:** Slice design. Follows slice 48c (local-retention split). Part
of the KIP-405 umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Serve `Fetch` requests below `local_log_start_offset()` from the remote
tier, using `RemoteLogMetadataManager::remote_log_segment_metadata` to
locate the right finished segment and `RemoteStorageManager::fetch_index`
+ `fetch_log_segment` to position and read. Surface remote earliest
offsets on `ListOffsets` (EARLIEST + by-timestamp).

After 48c, sealed segments copied to the remote tier are deleted from
local disk, but a consumer fetching below `local_log_start_offset()`
still sees `OFFSET_OUT_OF_RANGE` — that's the gap this slice closes.

## Broker wiring

Slice 48b/c construct `Arc<dyn RemoteStorageManager>` +
`Arc<dyn RemoteLogMetadataManager>` inside `Broker::start` and move them
into the `remote_log_manager::run` task. 48d hoists construction out so
the handlers (`Fetch`, `ListOffsets`) can reach the same instances:

- New `Broker.remote_reader: Option<Arc<RemoteReader>>` (None when the
  broker's `remote_log_storage_dir` is unset). The remote-log-manager
  task receives the same `RemoteReader`'s rsm/rlmm so there's a single
  shared pair per broker.
- New `crates/broker/src/remote_reader.rs` owning the rsm + rlmm pair
  and the read-side helpers below.

## `RemoteReader` API

```rust
pub(crate) struct RemoteReader {
    rsm: Arc<dyn RemoteStorageManager>,
    rlmm: Arc<dyn RemoteLogMetadataManager>,
}

impl RemoteReader {
    /// Fetch a single record batch covering `offset` from the remote
    /// tier. Looks up the finished segment in the RLMM, then uses the
    /// segment's offset index to position into the `.log` data and
    /// reads one batch. Returns `None` when no finished segment in the
    /// RLMM covers `(leader_epoch, offset)`.
    pub(crate) async fn fetch_batch(
        &self,
        tp: &TopicIdPartition,
        leader_epoch: i32,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Option<RecordBatch>, RemoteStorageError>;

    /// Lowest `start_offset` across finished segments for `tp`, or
    /// `None` when none exist. Drives `ListOffsets` EARLIEST below
    /// `local_log_start_offset()`.
    pub(crate) fn earliest_offset(
        &self,
        tp: &TopicIdPartition,
    ) -> Result<Option<i64>, RemoteStorageError>;

    /// First offset in the remote tier whose record timestamp is ≥
    /// `target_timestamp`. Walks finished segments oldest-first, picks
    /// the first whose `max_timestamp ≥ target_timestamp`, and uses
    /// the segment's time index to land on the offset. Returns `None`
    /// when no remote segment has data at or after the target.
    pub(crate) async fn offset_for_timestamp(
        &self,
        tp: &TopicIdPartition,
        target_timestamp: i64,
    ) -> Result<Option<i64>, RemoteStorageError>;
}
```

The RSM SPI is synchronous + blocking; calls go via
`tokio::task::spawn_blocking` exactly as in 48b's copy path.

### Pure-logic helpers (testable without a real RSM)

```rust
/// Parse `OffsetIndex` bytes (8B/entry: rel_offset BE + position BE) into a
/// sorted Vec.
pub(crate) fn parse_offset_index(bytes: &[u8]) -> Vec<(u32, u32)>;

/// Binary-search the largest entry whose `rel <= target_rel`. Returns 0 if
/// `entries` is empty or `target_rel` is below the first entry.
pub(crate) fn position_for_relative_offset(
    entries: &[(u32, u32)],
    target_rel: u32,
) -> u32;

/// Parse `TimeIndex` bytes (12B/entry: timestamp BE + rel_offset BE).
pub(crate) fn parse_time_index(bytes: &[u8]) -> Vec<(i64, u32)>;

/// First entry whose `ts >= target_ts`, or `None` if none qualify.
pub(crate) fn relative_offset_for_timestamp(
    entries: &[(i64, u32)],
    target_ts: i64,
) -> Option<u32>;
```

These mirror `crates/log/src/index.rs::{OffsetIndex,TimeIndex}::lookup`
byte-for-byte (the on-the-wire and on-disk formats are identical — the
copy path in 48b just streams the local index files verbatim into the
RSM). Living in `remote_reader.rs` keeps the broker from depending on
`crabka_log`'s private index module while preserving format parity.

## Fetch handler integration

`crates/broker/src/handlers/fetch.rs::do_read` currently emits
`OFFSET_OUT_OF_RANGE` whenever `fetch_offset < log.log_start_offset()`.
The new behavior:

1. Snapshot under the log lock as before, plus
   `remote_storage_enable = log.config_snapshot().remote_storage_enable`.
2. When `fetch_offset < log_start` AND `remote_storage_enable` AND the
   broker has a `remote_reader`:
   - Resolve `topic_id` from the metadata image (Fetch already does
     this at the request level for v ≥ 13).
   - Drop the log lock (a remote fetch can be slow; the handler must
     not hold the per-partition mutex across blocking I/O).
   - Call `remote_reader.fetch_batch(&tp, leader_epoch, fetch_offset,
     max_bytes)`. `leader_epoch` is taken from the partition's
     `current_leader_epoch` atomic (the fetch already pins this for
     KIP-101). If the consumer supplied
     `current_leader_epoch` ≥ 0 (KIP-320) it's been validated above to
     match, so either value is correct.
   - On `Ok(Some(batch))`: fill `out.records`, set
     `out.error_code = NONE`, `out.log_start_offset = log_start`, and
     the HW/LSO fields per the existing rules (read-committed filtering
     is not in scope for the remote path in 48d — the remote tier only
     holds sealed segments, all transactions on those segments have
     long since committed or aborted, but the aborted-txn index from
     `IndexType::Transaction` is not consulted here; documented).
   - On `Ok(None)` or any error: fall through to the existing
     `OFFSET_OUT_OF_RANGE` behavior (logged at WARN).
3. Otherwise, current behavior unchanged.

### Read-committed scoping note

Slice 48d serves remote batches without aborted-transaction filtering.
Apache Kafka itself does support this — it fetches the segment's
`.txnindex` via `RSM::fetch_index(IndexType::Transaction)` and runs the
same `aborted_in_range` filter. Crabka's reference RSM
(`LocalTieredStorage`) already preserves the txn index on copy; the
filter glue is the missing piece. Adding it is mechanical but out of
scope for 48d — flagged in the slice notes.

For consumer fetches with `isolation_level = 1`, the remote path
returns the batch unfiltered. This is safe (sealed remote segments by
definition contain only committed or fully-aborted transactions —
in-flight transactions live on the active local segment which is
never tiered), but a strict read-committed consumer could see a batch
from a transaction that was aborted *before* the segment was sealed.
The follow-up slice closes this gap.

## ListOffsets handler integration

`crates/broker/src/handlers/list_offsets.rs::handle` currently:

- EARLIEST → `log.log_start_offset()`
- LATEST → `log.log_end_offset()`
- positive timestamp → `-1` (stub)

New behavior, gated on
`log.config_snapshot().remote_storage_enable && broker.remote_reader.is_some()`:

- **EARLIEST**: returns `min(local_log_start_offset, remote_start)`
  where `remote_start` is `remote_reader.earliest_offset(tp)`. When the
  remote tier is empty for `tp`, falls back to `local_log_start_offset`
  (== `log_start_offset` per 48c invariant).

  Non-tiered topics keep returning `log_start_offset()`.

- **By-timestamp** (`timestamp > 0`): consult
  `remote_reader.offset_for_timestamp(tp, ts)` first; if it returns
  `Some`, return that offset. Otherwise, return `-1` (the existing
  no-local-timeindex behavior — adding local-segment timestamp lookup
  is its own future cleanup and is out of scope here).

  Non-tiered topics keep returning `-1`.

- LATEST is unchanged: `log_end_offset()` is the partition's true
  LEO regardless of tiering.

### Topic id resolution

The handler currently key-looks up `partitions` by `(name, idx)`. For
the remote path, the metadata image's topic id is needed to build the
`TopicIdPartition`. ListOffsets v ≥ 1 carries the topic name; for
both Fetch and ListOffsets, the topic id is resolved via
`broker.controller.current_image().topic(name).map(|t| t.topic_id)`.
When the topic id can't be resolved (e.g. topic just got deleted), the
remote path is skipped and the existing behavior is preserved.

## Out of scope (48e+)

- Read-committed aborted-transaction filtering on remote batches
  (sketched above; mechanical follow-up).
- Local-segment timestamp index lookup for `ListOffsets` (today's `-1`
  stub is preserved on the local path; remote lookup is layered on
  top).
- Remote-tier total-retention eviction of segments that have aged out
  on the remote side (48e).
- `RemotePartitionDeleteMetadata` cascade on `DeleteTopics` (48e).
- `TopicBasedRemoteLogMetadataManager` — still in-memory (48f).
- Object-store RSM — still `LocalTieredStorage` (48f / later).
- Operator CRD surface (48g).

## Test plan

### Pure-logic helpers in `remote_reader.rs`

- `parse_offset_index_round_trips_known_entries` — encode 3 entries the
  way `OffsetIndex::append` does, decode via `parse_offset_index`, get
  the same pairs.
- `position_for_relative_offset_returns_floor` — exact match, between
  entries, before-first, after-last.
- `parse_time_index_round_trips_known_entries`.
- `relative_offset_for_timestamp_returns_first_ge` — exact match,
  between entries (returns next), after-last (None), empty (None).

### `RemoteReader` against `LocalTieredStorage` + `InmemoryRemoteLogMetadataManager`

- `fetch_batch_finds_segment_and_returns_first_batch` — append batches
  to a real `Log`, roll, copy via slice 48b's `copy_eligible`, then
  call `remote_reader.fetch_batch` for an offset inside a copied
  segment; assert the returned batch's `base_offset` is the largest
  `base_offset` ≤ target.
- `fetch_batch_returns_none_when_segment_not_in_rlmm` — call against
  an offset that has no finished segment; expect `Ok(None)`.
- `fetch_batch_returns_none_for_in_progress_segment` — add metadata
  in state `CopySegmentStarted` (not finished); expect `Ok(None)`.
- `earliest_offset_returns_lowest_finished_start` — copy 2 sealed
  segments; assert `earliest_offset` returns the first one's
  `start_offset`. With no finished segments, returns `None`.
- `offset_for_timestamp_locates_remote_segment` — copy sealed segments
  with known max timestamps; query a timestamp in the middle of the
  range; expect the first relative offset ≥ ts.
- `offset_for_timestamp_returns_none_when_past_last` — query timestamp
  greater than every segment's `max_timestamp`; expect `None`.

### Fetch handler — broker integration tests (`crates/broker/tests/`)

- `tests/remote_fetch.rs::fetch_below_local_log_start_serves_from_remote`
   — single broker, single-partition tiered topic, append + roll +
   wait for copy + advance local retention so old offsets are gone
   locally; consumer fetch at offset 0 returns a record batch.
- `tests/remote_fetch.rs::fetch_below_local_start_returns_oor_when_not_tiered`
   — same shape but `remote.storage.enable=false`; consumer fetch at
   offset 0 returns `OFFSET_OUT_OF_RANGE` (regression guard).
- `tests/remote_fetch.rs::list_offsets_earliest_returns_remote_floor`
   — after retention deletes local copies, `EARLIEST` returns 0 (the
   remote tier's earliest), not `local_log_start_offset`.

### Existing tests

- No changes expected — slice 48d adds new behavior gated on
  `remote_storage_enable && remote_reader.is_some()`. Non-tiered
  topics, and tiered topics with no remote_reader (configured-out),
  see no behavior change.

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-broker -p crabka-remote-storage`
- `cargo build --workspace`
- No CRD drift (no CRDs touched).
