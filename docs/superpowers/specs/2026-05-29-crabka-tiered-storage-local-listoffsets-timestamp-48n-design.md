# Crabka tiered storage 48n — Local ListOffsets by-timestamp (design)

**Date:** 2026-05-29
**Status:** Slice design. Closes a 48d follow-up. Part of the KIP-405
umbrella
(`docs/superpowers/specs/2026-05-25-crabka-tiered-storage-roadmap-design.md`).

## Goal

Resolve `ListOffsets` by-timestamp against the **local** log so non-tiered
topics (and the local portion of tiered topics) return a real offset
instead of the `-1` stub. Also wire the two tiered/extended sentinels that
are cheap to support: earliest-local (`-4`, KIP-405) and max-timestamp
(`-3`, KIP-734).

## The gap

`crates/broker/src/handlers/list_offsets.rs` only resolves positive
timestamps via the remote tier:

```rust
// list_offsets.rs:100
ts if ts > 0 => {
    if let (Some(reader), Some(tid)) = (remote_reader.as_ref(), topic_id) {
        match reader.offset_for_timestamp(&tp, ts).await { ... }
    } else {
        -1            // <-- non-tiered topics never resolve by timestamp
    }
}
_ => -1,
```

`Log` has the machinery (`TimeIndex::lookup`, `index.rs:231`; per-segment
`max_timestamp`) but exposes no timestamp→offset method.

## Approach

### `Log::offset_for_timestamp`

Add to `crates/log/src/log.rs`:

```rust
/// Earliest offset whose record timestamp is >= `target_ts`, searching
/// local segments oldest-first. `None` when no local record qualifies.
pub fn offset_for_timestamp(&self, target_ts: i64) -> Option<i64>;
```

Walk `self.segments` (sealed) then `self.active`, oldest-first; find the
first segment whose `max_timestamp >= target_ts`; within it use
`TimeIndex::lookup(target_ts)` to get a floor position, then scan batches
forward from that position for the first record with `timestamp >=
target_ts`, returning its absolute offset. The time index is sparse, so
the post-index scan is required for an exact answer (matching Kafka's
`LogSegment.findOffsetByTimestamp`).

`Segment::time_index` is currently private; add a narrow
`Segment::offset_for_timestamp(target_ts) -> Option<i64>` that does the
index lookup + log scan internally, so `Log` iterates segments and
delegates. This keeps the index private and the scan logic next to the
segment's own `.log` file handle.

### `Log::offset_of_max_timestamp` (for `-3`)

```rust
/// Offset of the record carrying the partition's largest timestamp, or
/// `log_start_offset()` when empty. KIP-734 MAX_TIMESTAMP.
pub fn offset_of_max_timestamp(&self) -> i64;
```

Track the max across segments (each `Segment` already records
`max_timestamp`); within the winning segment, the time index's last entry
points at the floor for the max-timestamp record — scan from there for the
record whose timestamp equals the segment max and return its offset. The
active segment participates too.

### Handler wiring (`list_offsets.rs`)

Introduce the sentinels and resolve them:

```rust
const EARLIEST: i64 = -2;
const LATEST: i64 = -1;
const MAX_TIMESTAMP: i64 = -3;       // KIP-734
const EARLIEST_LOCAL: i64 = -4;      // KIP-405

let offset = match part.timestamp {
    EARLIEST => /* existing: min(local_start, remote earliest) */,
    LATEST => local_end,
    EARLIEST_LOCAL => local_log_start_offset(),   // local tier floor
    MAX_TIMESTAMP => log.offset_of_max_timestamp(),
    ts if ts > 0 => {
        // Remote holds older data: a remote hit is the earliest
        // qualifying offset. Otherwise fall back to the local log.
        remote_result.or_else(|| log.offset_for_timestamp(ts)).unwrap_or(-1)
    }
    _ => -1,
};
```

For tiered topics the remote lookup runs first (it covers the oldest
records); `remote_result.or(local)` yields the earliest offset across the
whole partition. For non-tiered topics `remote_result` is `None` and the
local lookup answers. `EARLIEST_LOCAL` needs `local_log_start_offset()`,
already on `Log`.

The response `timestamp` field: for positive-timestamp and MAX_TIMESTAMP
queries Kafka echoes the matched record's timestamp. Return the resolved
record timestamp where the lookup already has it in hand (extend the
segment helper to return `(offset, timestamp)`); for sentinel queries the
field stays `-1` as today.

## Files

- `crates/log/src/log.rs` — `offset_for_timestamp`,
  `offset_of_max_timestamp`.
- `crates/log/src/segment.rs` — `offset_for_timestamp` /
  max-timestamp helper (keeps `time_index` private).
- `crates/broker/src/handlers/list_offsets.rs` — new sentinels +
  local/remote combination + response timestamp.

No config; no wire-format changes (these sentinels already exist in the
`ListOffsets` request schema).

## Testing

- Log unit: `offset_for_timestamp` across multiple sealed segments + the
  active segment; target before first / between / after last record;
  empty log → `None`; exact-match and gap cases against the sparse index.
- Log unit: `offset_of_max_timestamp` with the max in a sealed segment and
  in the active segment; ties resolve to the earliest offset (Kafka).
- Broker: non-tiered topic returns a real by-timestamp offset (was `-1`);
  tiered topic returns the remote answer when the timestamp predates the
  local floor and the local answer otherwise; `EARLIEST_LOCAL` returns
  `local_log_start_offset()`; `MAX_TIMESTAMP` returns the expected offset.

## Non-goals

- No change to EARLIEST/LATEST behavior.
- No remote MAX_TIMESTAMP scan — `-3` is a local-log query in Kafka
  (the local log always holds the newest records).

## Dependencies & sequencing

Independent. First parallel batch alongside 48m (disjoint files:
`log.rs`/`segment.rs`/`list_offsets.rs` vs `fetch.rs`/`remote_reader.rs`).

## Acceptance gates

- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test -p crabka-log -p crabka-broker`
- `cargo test --workspace` (no regressions)
- `kafka-get-offsets` / `ListOffsets` by-timestamp behavior matches
  Apache Kafka; no CRD drift.
