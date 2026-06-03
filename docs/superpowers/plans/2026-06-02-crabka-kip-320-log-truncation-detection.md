# KIP-320 Log-Truncation Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Kafka KIP-320 so log truncation is detected in-band: the leader returns a `diverging_epoch`, the follower truncates on it, and the native consumer proactively validates fetch positions via `OffsetForLeaderEpoch` — closing the mixed-cluster silent-divergence hazard.

**Architecture:** Three tiers. (1) Broker **leader** computes a diverging epoch from the leader-epoch cache when a fetch carries `last_fetched_epoch`, and reports `current_leader` on fence errors. (2) Broker **follower** stamps `last_fetched_epoch` and truncates in-band on `diverging_epoch`, keeping the reactive `OffsetForLeaderEpoch` path only for leadership changes. (3) Native **consumer** tracks per-partition leader epoch in a sidecar `positions` map, validates positions proactively via `OffsetForLeaderEpoch` (Java-faithful `AwaitValidation` lifecycle), handles `diverging_epoch`/`OFFSET_OUT_OF_RANGE` in an error-first poll loop, and round-trips `committed_leader_epoch`. Validated against a mixed JVM+Crabka cluster.

**Tech Stack:** Rust 2024, tokio, `crabka-protocol` (generated Kafka wire codecs), `assert2` for tests, the `broker-jvm-acceptance` harness for JVM interop.

**Spec:** `docs/superpowers/specs/2026-06-02-crabka-kip-320-log-truncation-detection-design.md`

---

## Architecture decisions that deviate from / refine the spec

1. **Consumer position storage — sidecar, not a type swap.** The spec describes "replace `next_offsets` with `FetchPosition`". `next_offsets: Arc<Mutex<HashMap<(String,i32), i64>>>` is shared between `Consumer` and `CoordinatorState` and is the offset source of truth for offset-advance, `resolve_latest_sentinels`, and commit filtering. Rather than change its type (high blast radius across `poll.rs`, `coordinator.rs`, `consumer.rs`, `offset_wire.rs`), we add a **parallel** `positions: Arc<Mutex<HashMap<(String,i32), PartitionPosition>>>` holding the epoch metadata + validation flag, keyed identically. A logical `FetchPosition` is `next_offsets[k]` (offset) + `positions[k]` (epoch/leader/state). Behaviour is identical; offset-advance code is untouched.

2. **`end_offset_for_epoch` is kept, not reimplemented.** The spec suggested reimplementing it on top of the new method. The existing `end_offset_for_epoch` has a deliberately simpler contract (exact-epoch match → else `-1`) that the `OffsetForLeaderEpoch` handler and the reactive replicator fence path depend on. The new `epoch_and_offset_for` implements the full Kafka `endOffsetFor` (floor + future + below-all branches) needed for divergence. Changing the handler is out of scope, so the two coexist.

3. **Consumer metadata-epoch refresh runs in the data path (`poll`), not the coordinator.** The coordinator only issues `Metadata` when this member is the group leader (`coordinator.rs:528`). KIP-320 needs leader epochs on every member, so `poll`/`validate.rs` issue `Metadata` to populate `positions` when an epoch is unknown or on a fence/not-leader error.

## File structure

**Broker / log tier**
- `crates/log/src/leader_epoch_checkpoint.rs` — add `epoch_and_offset_for` (Kafka `endOffsetFor`).
- `crates/broker/src/handlers/fetch.rs` — leader divergence + `current_leader` (Fetch v12+).
- `crates/broker/src/replicator.rs` — send `last_fetched_epoch`; in-band `diverging_epoch` truncation.
- `crates/broker/tests/leader_epoch.rs` — divergence handler + follower truncation integration tests.

**Consumer tier**
- `crates/client-core/src/offset_for_leader_epoch.rs` (new) + `crates/client-core/src/lib.rs` — `OffsetForLeaderEpoch` client helper.
- `crates/client-consumer/src/position.rs` (new) + `lib.rs` — `PartitionPosition` + pure truncation-decision fn.
- `crates/client-consumer/src/builder.rs` — `AutoOffsetReset::None`.
- `crates/client-consumer/src/error.rs` — `ConsumerError::LogTruncation`.
- `crates/client-consumer/src/offset_wire.rs` — `committed_leader_epoch` round-trip.
- `crates/client-consumer/src/consumer.rs` — `positions` field, `ConsumerRecord.leader_epoch`, seed epoch.
- `crates/client-consumer/src/coordinator.rs` — `positions` in state, commit-with-epoch, prime epoch.
- `crates/client-consumer/src/poll.rs` + `crates/client-consumer/src/validate.rs` (new) + `lib.rs` — fetch epochs, error-first loop, validate pre-pass, reset policy.
- `crates/client-consumer/tests/integration.rs` — consumer truncation tests.

**JVM + docs**
- `broker-jvm-acceptance` harness — mixed-cluster divergence scenario.
- `README.md`, `STATUS.md` — flip KIP-320 ⚠️→✅.

## Batches (per CLAUDE.md: parallel where file sets are disjoint)

- **Batch 1** (parallel — disjoint files, each leaves build green): Task 1, Task 2, Task 3, Task 4.
- **Batch 2** (parallel — depends on Task 1): Task 5, Task 6.
- **Batch 3** (single coherent task — depends on Batch 1): Task 7.
- **Batch 4** (single task — depends on Task 7 + Batch 1): Task 8.
- **Batch 5** (parallel — depends on prior batches): Task 9, Task 10, Task 11, Task 12.

Standard pre-push gate after each commit (memory): `cargo fmt --all` then `cargo clippy --workspace --all-targets -- -D warnings`.

---

## Batch 1

### Task 1: Leader-epoch cache — `epoch_and_offset_for`

**Files:**
- Modify: `crates/log/src/leader_epoch_checkpoint.rs`

- [ ] **Step 1: Write the failing tests**

Add to the `tests` module in `crates/log/src/leader_epoch_checkpoint.rs` (after `missing_file_yields_empty`):

```rust
    #[test]
    fn epoch_and_offset_latest_returns_pair_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Requested == latest recorded epoch → (epoch, log_end_offset).
        assert!(c.epoch_and_offset_for(1, 100) == (1, 100));
    }

    #[test]
    fn epoch_and_offset_older_returns_floor_epoch_and_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        // Recorded older epoch → (epoch, start of next epoch).
        assert!(c.epoch_and_offset_for(0, 200) == (0, 50));
        assert!(c.epoch_and_offset_for(1, 200) == (1, 100));
    }

    #[test]
    fn epoch_and_offset_gap_uses_floor_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(5, 100).unwrap();
        // Requested epoch 3 is not recorded; floor is epoch 0, next start 100.
        assert!(c.epoch_and_offset_for(3, 200) == (0, 100));
    }

    #[test]
    fn epoch_and_offset_future_epoch_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        // Requested epoch above everything recorded → (UNDEFINED, log_end).
        assert!(c.epoch_and_offset_for(7, 100) == (UNDEFINED_EPOCH, 100));
    }

    #[test]
    fn epoch_and_offset_below_all_returns_requested_and_first_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(3, 30).unwrap();
        c.append(4, 40).unwrap();
        // Requested epoch below the first recorded epoch.
        assert!(c.epoch_and_offset_for(1, 100) == (1, 30));
    }

    #[test]
    fn epoch_and_offset_empty_cache_is_undefined_at_log_end() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.epoch_and_offset_for(0, 9) == (UNDEFINED_EPOCH, 9));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p crabka-log epoch_and_offset`
Expected: FAIL — `no method named epoch_and_offset_for` / `cannot find value UNDEFINED_EPOCH`.

- [ ] **Step 3: Add the constants and method**

Just above `impl LeaderEpochCheckpoint {` (after the `LeaderEpochCheckpoint` struct, around line 33), add:

```rust
/// Kafka sentinel: "no leader epoch information".
pub const UNDEFINED_EPOCH: i32 = -1;
/// Kafka sentinel: "no offset".
pub const UNDEFINED_OFFSET: i64 = -1;
```

Inside `impl LeaderEpochCheckpoint`, after `end_offset_for_epoch` (line 133), add:

```rust
    /// Kafka `LeaderEpochFileCache.endOffsetFor`. Returns
    /// `(found_epoch, end_offset)` — the epoch the requested offset range
    /// actually belongs to on this log, and the first offset *after* that
    /// epoch. Used to detect follower/consumer log divergence (KIP-320):
    ///
    ///  - `requested == UNDEFINED_EPOCH`            → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested == latest recorded epoch`      → `(requested, log_end_offset)`
    ///  - `requested` above all recorded epochs     → `(UNDEFINED_EPOCH, log_end_offset)`
    ///  - `requested` below all recorded epochs     → `(requested, first_recorded_start)`
    ///  - otherwise (gap or exact older match)      → `(floor_epoch, next_epoch_start)`
    ///
    /// where `floor_epoch` is the largest recorded epoch `<= requested`.
    /// `end_offset` is always a valid truncation target (`>= 0`).
    #[must_use]
    pub fn epoch_and_offset_for(&self, requested_epoch: i32, log_end_offset: i64) -> (i32, i64) {
        if requested_epoch == UNDEFINED_EPOCH {
            return (UNDEFINED_EPOCH, log_end_offset);
        }
        if self.latest_epoch() == Some(requested_epoch) {
            return (requested_epoch, log_end_offset);
        }
        // Smallest recorded epoch strictly greater than `requested`.
        let higher = self
            .entries
            .iter()
            .filter(|e| e.epoch > requested_epoch)
            .min_by_key(|e| e.epoch);
        match higher {
            // `requested` is in the future relative to this log.
            None => (UNDEFINED_EPOCH, log_end_offset),
            Some(next) => {
                // Largest recorded epoch <= requested (the floor).
                let floor = self
                    .entries
                    .iter()
                    .filter(|e| e.epoch <= requested_epoch)
                    .map(|e| e.epoch)
                    .max();
                match floor {
                    Some(f) => (f, next.start_offset),
                    // `requested` is below the first recorded epoch.
                    None => (requested_epoch, next.start_offset),
                }
            }
        }
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p crabka-log epoch_and_offset`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-log --all-targets -- -D warnings
git add crates/log/src/leader_epoch_checkpoint.rs
git commit -m "feat(log): add epoch_and_offset_for for KIP-320 divergence detection"
```

---

### Task 2: `OffsetForLeaderEpoch` client helper (`crabka-client-core`)

**Files:**
- Create: `crates/client-core/src/offset_for_leader_epoch.rs`
- Modify: `crates/client-core/src/lib.rs`

This gives the consumer a way to *send* `OffsetForLeaderEpoch` (the broker only serves it today). It returns the leader's `(leader_epoch, end_offset)` for each requested partition.

- [ ] **Step 1: Write the failing test**

Create `crates/client-core/src/offset_for_leader_epoch.rs`:

```rust
//! Client-side `OffsetForLeaderEpoch` (`api_key=23`) helper, used by the
//! consumer's KIP-320 position-validation pass. The broker, given a
//! partition's `leader_epoch`, returns the `end_offset` of that epoch —
//! the safe offset a fetcher must not have consumed past.

use crate::connection::Connection;
use crate::error::ClientError;
use crabka_protocol::owned::offset_for_leader_epoch_request::{
    OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
};
use crabka_protocol::owned::offset_for_leader_epoch_response::OffsetForLeaderEpochResponse;

/// One leader-epoch end-offset answer for a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEndOffset {
    pub partition: i32,
    /// The leader's view of the epoch (may be lower than requested if the
    /// requested epoch is unknown to the leader).
    pub leader_epoch: i32,
    /// First offset *after* the requested epoch on the leader's log, or
    /// `-1` (`UNDEFINED_OFFSET`) if the epoch is unknown.
    pub end_offset: i64,
    pub error_code: i16,
}

/// Send a single-partition `OffsetForLeaderEpoch` request. `current_leader_epoch`
/// is the epoch the caller believes the partition is in (for fencing);
/// `leader_epoch` is the epoch the caller wants the end offset of.
///
/// # Errors
/// Transport / version-negotiation failure, or a partition not present in the
/// response.
pub async fn offset_for_leader_epoch(
    conn: &Connection,
    topic: &str,
    partition: i32,
    current_leader_epoch: i32,
    leader_epoch: i32,
) -> Result<EpochEndOffset, ClientError> {
    let resp: OffsetForLeaderEpochResponse = conn
        .send(OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderTopic {
                topic: topic.to_string(),
                partitions: vec![OffsetForLeaderPartition {
                    partition,
                    current_leader_epoch,
                    leader_epoch,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await?;
    parse_single(&resp, topic, partition)
}

fn parse_single(
    resp: &OffsetForLeaderEpochResponse,
    topic: &str,
    partition: i32,
) -> Result<EpochEndOffset, ClientError> {
    resp.topics
        .iter()
        .find(|t| t.topic == topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == partition))
        .map(|p| EpochEndOffset {
            partition: p.partition,
            leader_epoch: p.leader_epoch,
            end_offset: p.end_offset,
            error_code: p.error_code,
        })
        .ok_or(ClientError::Server { error_code: -1 })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_protocol::owned::offset_for_leader_epoch_response::{
        EpochEndOffset as WireEpochEndOffset, OffsetForLeaderEpochResponse,
        OffsetForLeaderTopicResult,
    };

    #[test]
    fn parse_single_extracts_partition_answer() {
        let resp = OffsetForLeaderEpochResponse {
            topics: vec![OffsetForLeaderTopicResult {
                topic: "t".into(),
                partitions: vec![WireEpochEndOffset {
                    partition: 0,
                    leader_epoch: 2,
                    end_offset: 42,
                    error_code: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let got = parse_single(&resp, "t", 0).unwrap();
        assert!(got == EpochEndOffset { partition: 0, leader_epoch: 2, end_offset: 42, error_code: 0 });
    }

    #[test]
    fn parse_single_missing_partition_is_error() {
        let resp = OffsetForLeaderEpochResponse::default();
        assert!(parse_single(&resp, "t", 0).is_err());
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/client-core/src/lib.rs`, add the module declaration and re-export alongside the existing `fetch` module (match the file's existing `mod`/`pub use` style):

```rust
pub mod offset_for_leader_epoch;
pub use offset_for_leader_epoch::{EpochEndOffset, offset_for_leader_epoch};
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `cargo test -p crabka-client-core offset_for_leader_epoch`
Expected: PASS (2 tests). If `ClientError::Server` field name differs, adjust to the actual variant (confirm with `rg "Server" crates/client-core/src/error.rs`).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-client-core --all-targets -- -D warnings
git add crates/client-core/src/offset_for_leader_epoch.rs crates/client-core/src/lib.rs
git commit -m "feat(client-core): add OffsetForLeaderEpoch client helper for KIP-320"
```

---

### Task 3: `ConsumerError::LogTruncation` variant

**Files:**
- Modify: `crates/client-consumer/src/error.rs`

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/client-consumer/src/error.rs`:

```rust
    #[test]
    fn display_log_truncation() {
        let e = ConsumerError::LogTruncation {
            topic: "t".into(),
            partition: 3,
            fetch_offset: 100,
            safe_offset: 42,
        };
        let s = e.to_string();
        assert!(s.contains("truncation"));
        assert!(s.contains("42"));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p crabka-client-consumer display_log_truncation`
Expected: FAIL — `no variant named LogTruncation`.

- [ ] **Step 3: Add the variant**

In `crates/client-consumer/src/error.rs`, add to the `ConsumerError` enum (before `Server`):

```rust
    #[error("log truncation detected on {topic}-{partition}: fetch offset {fetch_offset} is past the leader's log; safe offset {safe_offset}")]
    LogTruncation {
        topic: String,
        partition: i32,
        fetch_offset: i64,
        safe_offset: i64,
    },
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p crabka-client-consumer display_log_truncation`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-client-consumer --all-targets -- -D warnings
git add crates/client-consumer/src/error.rs
git commit -m "feat(client-consumer): add LogTruncation error variant (KIP-320)"
```

---

### Task 4: `PartitionPosition` + truncation-decision (`position.rs`)

**Files:**
- Create: `crates/client-consumer/src/position.rs`
- Modify: `crates/client-consumer/src/lib.rs`

This holds the per-partition epoch metadata sidecar and the **pure** decision function used by both the proactive validate pass and the in-band `diverging_epoch` path, so the logic is unit-testable without a broker.

- [ ] **Step 1: Write the failing tests**

Create `crates/client-consumer/src/position.rs`:

```rust
//! Per-partition KIP-320 position metadata (sidecar to `next_offsets`) and
//! the pure truncation-decision used by the proactive validate pass and the
//! in-band `diverging_epoch` path.

/// Epoch metadata for one assigned partition. The fetch *offset* itself lives
/// in `Consumer::next_offsets`; this carries the leader-epoch state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionPosition {
    /// Leader epoch of the last consumed record (the `last_fetched_epoch` we
    /// send). `-1` until a record is consumed or a committed epoch is seeded.
    pub offset_epoch: i32,
    /// Current leader node id from the latest metadata. `-1` if unknown.
    pub leader_id: i32,
    /// Current leader epoch from the latest metadata (the `current_leader_epoch`
    /// we send). `-1` if unknown.
    pub leader_epoch: i32,
    /// `true` while this partition must be validated via `OffsetForLeaderEpoch`
    /// before it may be fetched again (set when the metadata leader epoch
    /// advances past `offset_epoch`).
    pub awaiting_validation: bool,
}

impl Default for PartitionPosition {
    fn default() -> Self {
        Self { offset_epoch: -1, leader_id: -1, leader_epoch: -1, awaiting_validation: false }
    }
}

/// Outcome of validating a fetch position against the leader's epoch history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValidationOutcome {
    /// Position is consistent with the leader; resume fetching. Carries the
    /// leader's epoch for that offset (to refresh `offset_epoch`).
    Valid { leader_epoch: i32 },
    /// Truncation detected; the fetcher must reset to `safe_offset`.
    Truncated { safe_offset: i64 },
}

/// Decide whether a position has diverged, given the fetch `offset`, the epoch
/// we last consumed (`offset_epoch`), and the leader's answer for that epoch
/// (`leader_end_offset`, `leader_epoch`). This is Kafka's consumer-side rule:
/// truncation iff the leader's epoch for our data is older than ours, or its
/// end offset for that epoch is below our position.
pub(crate) fn classify(
    offset: i64,
    offset_epoch: i32,
    leader_epoch: i32,
    leader_end_offset: i64,
) -> ValidationOutcome {
    if leader_end_offset < 0 || leader_epoch < offset_epoch || leader_end_offset < offset {
        ValidationOutcome::Truncated {
            safe_offset: if leader_end_offset < 0 { 0 } else { leader_end_offset },
        }
    } else {
        ValidationOutcome::Valid { leader_epoch }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn consistent_position_is_valid() {
        // We consumed up to offset 100 at epoch 2; leader says epoch 2 ends at
        // 150 (still open / ahead). No truncation.
        assert!(classify(100, 2, 2, 150) == ValidationOutcome::Valid { leader_epoch: 2 });
    }

    #[test]
    fn leader_end_below_position_is_truncation() {
        // Leader's epoch-2 end offset (80) is below our position (100): the
        // tail we hold was truncated away.
        assert!(classify(100, 2, 2, 80) == ValidationOutcome::Truncated { safe_offset: 80 });
    }

    #[test]
    fn older_leader_epoch_is_truncation() {
        // Leader only knows up to epoch 1 for our offset; our epoch 2 data
        // diverged.
        assert!(classify(100, 2, 1, 60) == ValidationOutcome::Truncated { safe_offset: 60 });
    }

    #[test]
    fn undefined_leader_offset_truncates_to_zero() {
        assert!(classify(100, 2, -1, -1) == ValidationOutcome::Truncated { safe_offset: 0 });
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/client-consumer/src/lib.rs`, add alongside the other `mod` declarations:

```rust
mod position;
```

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p crabka-client-consumer position::`
Expected: PASS (4 tests). (A `dead_code` warning on unused fields is expected until Task 7/8 wire it in; clippy `-D warnings` would fail, so add `#![allow(dead_code)]` at the top of `position.rs` for now and remove it in Task 8.)

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/client-consumer/src/position.rs crates/client-consumer/src/lib.rs
git commit -m "feat(client-consumer): add PartitionPosition + truncation classify (KIP-320)"
```

---

## Batch 2 (depends on Task 1)

### Task 5: Leader divergence + `current_leader` in the Fetch handler

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

The leader-epoch fence already lives at `fetch.rs:272-299`. Divergence detection slots in immediately after it, reusing the same `PendingRead { partition: None, .. }` pattern so the read is skipped and the pre-filled `out` is returned verbatim.

- [ ] **Step 1: Extract `last_fetched_epoch`**

In the per-partition loop, next to the existing extractions (`fetch.rs:225-227`), add:

```rust
            let req_last_fetched_epoch = fp.last_fetched_epoch;
```

- [ ] **Step 2: Populate `current_leader` on the fence error**

Inside the existing fence block (`fetch.rs:279-298`), after `out.error_code = ...`, before the `pending.push(...)`, add the current-leader hint:

```rust
                    // KIP-320: tell the fetcher who the current leader is so it
                    // can re-target without a full Metadata round-trip. Encodes
                    // only at Fetch v12+ (codegen gates the tagged field).
                    let leader_id = image
                        .partition(&topic_name, idx)
                        .map_or(-1, |pr| i32::try_from(pr.leader).unwrap_or(-1));
                    out.current_leader = crabka_protocol::owned::fetch_response::LeaderIdAndEpoch {
                        leader_id,
                        leader_epoch: our_epoch,
                        ..Default::default()
                    };
```

- [ ] **Step 3: Add divergence detection after the fence block**

Immediately after the fence `if let Some(part) = part_opt.as_ref() { ... }` block closes (after `fetch.rs:299`), add:

```rust
            // KIP-320 divergence detection. A v12+ fetcher includes the leader
            // epoch of its last fetched record (`last_fetched_epoch`). If the
            // leader's epoch history says that epoch/offset diverged, return a
            // `diverging_epoch` and serve no records, so the follower/consumer
            // truncates instead of appending on top of a divergent suffix.
            if req_last_fetched_epoch >= 0
                && let Some(part) = part_opt.as_ref()
            {
                let (found_epoch, end_offset) = {
                    let log = part.log.lock().expect("log mutex poisoned");
                    let leo = log.log_end_offset();
                    log.epoch_checkpoint()
                        .epoch_and_offset_for(req_last_fetched_epoch, leo)
                };
                if found_epoch < req_last_fetched_epoch || end_offset < fetch_offset {
                    out.error_code = codes::NONE;
                    out.diverging_epoch =
                        crabka_protocol::owned::fetch_response::EpochEndOffset {
                            epoch: found_epoch,
                            end_offset,
                            ..Default::default()
                        };
                    pending.push(PendingRead {
                        topic_name: topic_name.clone(),
                        topic_id,
                        partition_index: idx,
                        fetch_offset,
                        max_bytes,
                        read_committed,
                        is_follower_fetch,
                        partition: None,
                        out,
                        cpu_micros: 0,
                    });
                    continue;
                }
            }
```

(If `image` is not in scope at this point in the function, hoist its binding above the loop or compute `leader_id` from `part_opt`'s metadata the same way the KIP-392 block at `fetch.rs:367` obtains it.)

- [ ] **Step 4: Write the handler unit/integration test**

Add a test to `crates/broker/tests/leader_epoch.rs` (Task 11 fleshes out the follower-side integration; this one asserts the leader's response shape). Use the existing harness pattern in that file to start a broker, produce records across two leader epochs on a partition, then issue a follower Fetch with `last_fetched_epoch` set to the old epoch and a `fetch_offset` past that epoch's end, asserting `diverging_epoch.end_offset` equals the epoch boundary and `records` is empty. (Full code in Task 11; this step is satisfied by Task 11's `diverging_epoch_returned_on_stale_last_fetched_epoch` test.)

For Batch 2, verify compilation and existing tests instead:

Run: `cargo test -p crabka-broker fetch`
Expected: PASS (existing fetch tests still green; no regression).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): leader returns diverging_epoch + current_leader (KIP-320)"
```

---

### Task 6: Follower sends `last_fetched_epoch` + in-band truncation

**Files:**
- Modify: `crates/broker/src/replicator.rs`

- [ ] **Step 1: Send `last_fetched_epoch` in the fetch request**

In `build_fetch_request` (`replicator.rs:238-279`), compute the follower's last-fetched epoch from its own leader-epoch checkpoint and set the field. After the `leader_epoch` binding (`replicator.rs:243-250`), add:

```rust
    // KIP-320: the leader epoch of our last appended record. Sent so the
    // leader can detect divergence in-band and answer with `diverging_epoch`.
    let last_fetched_epoch = cfg
        .partitions
        .get(&cfg.topic, cfg.partition)
        .and_then(|entry| {
            let log = entry.log.lock().expect("log mutex poisoned");
            log.epoch_checkpoint().latest_epoch()
        })
        .unwrap_or(-1);
```

Then add `last_fetched_epoch,` to the `FetchPartition { .. }` literal (`replicator.rs:268-273`):

```rust
            partitions: vec![FetchPartition {
                partition: cfg.partition,
                fetch_offset,
                current_leader_epoch: leader_epoch,
                last_fetched_epoch,
                partition_max_bytes: partition_max_bytes_cap,
                ..FetchPartition::default()
            }],
```

(Confirm the lock type: the broker `Partition.log` is a `std::sync::Mutex` per `fetch.rs:1061`. If `Config.partitions.get(..)` returns a guard that cannot be held across the `FetchPartition` construction, bind `last_fetched_epoch` first as shown — the guard drops at the end of the `and_then` closure.)

- [ ] **Step 2: Handle `diverging_epoch` in the success branch**

In `handle_response`, the `codes::NONE` arm (`replicator.rs:313-351`) currently replicates batches. Before replicating, check for an in-band divergence signal. Insert at the top of the `codes::NONE => {` block (right after line 313):

```rust
            // KIP-320: an in-band divergence signal. The leader served no
            // records and told us the epoch/offset our log must truncate to.
            // `EpochEndOffset` defaults to (epoch:-1, end_offset:-1); a
            // populated `end_offset >= 0` means "truncate here".
            if part_resp.diverging_epoch.end_offset >= 0 {
                let end_offset = part_resp.diverging_epoch.end_offset;
                if let Some(part) = cfg.partitions.get(&cfg.topic, cfg.partition) {
                    match part.truncate_to(end_offset).await {
                        Ok(()) => info!(
                            topic = %cfg.topic,
                            partition = cfg.partition,
                            end_offset,
                            "replicator: truncated to diverging_epoch (KIP-320 in-band)"
                        ),
                        Err(e) => warn!(
                            topic = %cfg.topic,
                            partition = cfg.partition,
                            end_offset,
                            error = %e,
                            "replicator: truncate_to(diverging_epoch) failed"
                        ),
                    }
                }
                return LoopAction::Continue;
            }
```

(`truncate_to` already truncates both the log and the epoch checkpoint — it is the same call `handle_epoch_fence` uses at `replicator.rs:475`.)

- [ ] **Step 3: Remove the `dead_code` shim from Task 4**

Now that `PartitionPosition`/`classify` are about to be consumed (Task 8) and this task consumes `epoch_and_offset_for` indirectly via the broker, leave `position.rs` as-is; the shim is removed in Task 8. No action here — noted to avoid confusion.

- [ ] **Step 4: Verify build + existing replication tests**

Run: `cargo test -p crabka-broker replicat`
Expected: PASS (existing replicator/replication tests green). Full divergence integration is Task 11.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/replicator.rs
git commit -m "feat(broker): follower sends last_fetched_epoch + truncates in-band (KIP-320)"
```

---

## Batch 3 (depends on Batch 1)

### Task 7: Consumer wiring — positions, epoch round-trip, reset policy

**Files:**
- Modify: `crates/client-consumer/src/builder.rs`
- Modify: `crates/client-consumer/src/offset_wire.rs`
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/coordinator.rs`

This is one coherent task because adding `AutoOffsetReset::None` and changing the `offset_wire` signatures break the exhaustive matches and callers in `consumer.rs`/`coordinator.rs` — they must change together to keep the build green.

- [ ] **Step 1: Add `AutoOffsetReset::None`**

In `crates/client-consumer/src/builder.rs`, extend the enum (`builder.rs:8-15`):

```rust
#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    /// Start from offset 0.
    Earliest,
    /// Start from the log-end offset. Resolved lazily by `Consumer::poll`
    /// using `ListOffsets(timestamp=-1)`.
    Latest,
    /// Do not reset automatically. On a missing offset or detected truncation,
    /// `poll` returns `ConsumerError::LogTruncation` / surfaces the error.
    None,
}
```

- [ ] **Step 2: Round-trip `committed_leader_epoch` in `offset_wire.rs`**

Change `parse_offset_fetch` to return the committed leader epoch, and `build_commit_topics` to accept it. Replace the two function bodies (`offset_wire.rs:66-92` and `97-123`):

```rust
/// Flatten an `OffsetFetch` response into `(topic, partition, committed_offset,
/// committed_leader_epoch)` tuples.
pub(crate) fn parse_offset_fetch(
    resp: &OffsetFetchResponse,
    id_to_name: &HashMap<WireUuid, String>,
) -> Vec<(String, i32, i64, i32)> {
    let mut out = Vec::new();
    if resp.groups.is_empty() {
        for t in &resp.topics {
            for p in &t.partitions {
                out.push((t.name.clone(), p.partition_index, p.committed_offset, p.committed_leader_epoch));
            }
        }
    } else {
        for g in &resp.groups {
            for t in &g.topics {
                let name = if t.name.is_empty() {
                    id_to_name.get(&t.topic_id).cloned().unwrap_or_default()
                } else {
                    t.name.clone()
                };
                for p in &t.partitions {
                    out.push((name.clone(), p.partition_index, p.committed_offset, p.committed_leader_epoch));
                }
            }
        }
    }
    out
}

/// Build `OffsetCommit` topics. `offsets` maps `(topic, partition)` to
/// `(committed_offset, committed_leader_epoch)`.
pub(crate) fn build_commit_topics(
    offsets: HashMap<(String, i32), (i64, i32)>,
    topic_ids: &HashMap<String, WireUuid>,
) -> Vec<OffsetCommitRequestTopic> {
    let mut by_topic: HashMap<String, Vec<(i32, i64, i32)>> = HashMap::new();
    for ((t, p), (off, epoch)) in offsets {
        by_topic.entry(t).or_default().push((p, off, epoch));
    }
    by_topic
        .into_iter()
        .map(|(name, parts)| OffsetCommitRequestTopic {
            topic_id: topic_ids.get(&name).copied().unwrap_or_default(),
            name,
            partitions: parts
                .into_iter()
                .map(|(p, off, epoch)| OffsetCommitRequestPartition {
                    partition_index: p,
                    committed_offset: off,
                    committed_leader_epoch: epoch,
                    committed_metadata: Some(String::new()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}
```

Update the `offset_wire.rs` unit tests for the new shapes: `parse_offset_fetch` assertions become 4-tuples (e.g. `("t".to_string(), 3, 42, -1)` — the test responses leave `committed_leader_epoch` at its `-1` default), and `build_commit_topics` test inputs become `(offset, epoch)` pairs:

```rust
        offsets.insert(("t".to_string(), 0), (100, 5));
        // ...
        assert!(topics[0].partitions[0].committed_offset == 100);
        assert!(topics[0].partitions[0].committed_leader_epoch == 5);
```

- [ ] **Step 3: Add `ConsumerRecord.leader_epoch` and the `positions` field**

In `crates/client-consumer/src/consumer.rs`, extend `ConsumerRecord` (`consumer.rs:59-67`):

```rust
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub leader_epoch: i32,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}
```

Add the `positions` field to the `Consumer` struct (after `next_offsets`, `consumer.rs:44`):

```rust
    /// KIP-320 per-partition leader-epoch metadata, keyed like `next_offsets`.
    pub(crate) positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
```

- [ ] **Step 4: Seed positions + committed epoch at build time**

In `consumer.rs::start`, the offset-prime loop (`consumer.rs:283-309`) now receives a 4-tuple and must seed `positions`. Replace that block:

```rust
        let mut next_offsets: HashMap<(String, i32), i64> = HashMap::new();
        let mut positions: HashMap<(String, i32), crate::position::PartitionPosition> =
            HashMap::new();
        if !assigned_partitions.is_empty() {
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (t, p) in &assigned_partitions {
                by_topic.entry(t.clone()).or_default().push(*p);
            }
            let of = client
                .send(crate::offset_wire::build_offset_fetch(
                    &group_id, &by_topic, &topic_ids,
                ))
                .await?;
            let id_to_name = crate::offset_wire::id_to_name(&topic_ids);
            for (name, partition_index, committed, committed_epoch) in
                crate::offset_wire::parse_offset_fetch(&of, &id_to_name)
            {
                let starting = if committed >= 0 {
                    committed
                } else {
                    match auto_offset_reset {
                        AutoOffsetReset::Earliest => 0,
                        AutoOffsetReset::Latest | AutoOffsetReset::None => i64::MAX,
                    }
                };
                next_offsets.insert((name.clone(), partition_index), starting);
                positions.insert(
                    (name, partition_index),
                    crate::position::PartitionPosition {
                        offset_epoch: committed_epoch,
                        ..Default::default()
                    },
                );
            }
        }
```

(`AutoOffsetReset::None` resolving to `i64::MAX` here mirrors `Latest`'s "no commit" seed; the truncation surfacing for `None` happens in the poll error path, Task 8.)

Construct the shared `positions` Arc next to `next_offsets` (`consumer.rs:332-334`):

```rust
        let assigned = Arc::new(Mutex::new(assigned_partitions));
        let next_offsets = Arc::new(Mutex::new(next_offsets));
        let positions = Arc::new(Mutex::new(positions));
        let topic_ids = Arc::new(Mutex::new(topic_ids));
```

Thread it into `CoordinatorState` (`consumer.rs:337-352`) and the returned `Consumer` (`consumer.rs:355-370`) by adding `positions: Arc::clone(&positions),` and `positions,` respectively.

- [ ] **Step 5: Add `positions` to `CoordinatorState`, prime epoch, commit-with-epoch**

In `crates/client-consumer/src/coordinator.rs`, add the field to `CoordinatorState` (`coordinator.rs:116`):

```rust
    pub positions: Arc<Mutex<HashMap<(String, i32), crate::position::PartitionPosition>>>,
```

Update `prime_offsets` (`coordinator.rs:639-663`) for the 4-tuple + seed `positions`:

```rust
    let id_to_name = id_to_name(&topic_ids);
    let mut offsets = state.next_offsets.lock().await;
    let mut positions = state.positions.lock().await;
    let mut seen: HashSet<(String, i32)> = HashSet::new();
    for (name, partition_index, committed, committed_epoch) in parse_offset_fetch(&of, &id_to_name) {
        let starting = if committed >= 0 {
            committed
        } else {
            match state.auto_offset_reset {
                AutoOffsetReset::Earliest => 0,
                AutoOffsetReset::Latest | AutoOffsetReset::None => i64::MAX,
            }
        };
        let key = (name, partition_index);
        seen.insert(key.clone());
        offsets.insert(key.clone(), starting);
        positions.entry(key).or_default().offset_epoch = committed_epoch;
    }
    for tp in partitions {
        if !seen.contains(tp) {
            let starting = match state.auto_offset_reset {
                AutoOffsetReset::Earliest => 0,
                AutoOffsetReset::Latest | AutoOffsetReset::None => i64::MAX,
            };
            offsets.insert(tp.clone(), starting);
            positions.entry(tp.clone()).or_default();
        }
    }
    Ok(())
```

Update `commit_revoked` (`coordinator.rs:375-393`) to commit `(offset, epoch)` pairs:

```rust
async fn commit_revoked(state: &CoordinatorState, revoked: &[(String, i32)]) {
    let revoked_set: HashSet<&(String, i32)> = revoked.iter().collect();
    let offsets: HashMap<(String, i32), (i64, i32)> = {
        let off = state.next_offsets.lock().await;
        let pos = state.positions.lock().await;
        off.iter()
            .filter(|(k, v)| revoked_set.contains(k) && **v > 0 && **v != i64::MAX)
            .map(|(k, v)| {
                let epoch = pos.get(k).map_or(-1, |p| p.offset_epoch);
                (k.clone(), (*v, epoch))
            })
            .collect()
    };
    if offsets.is_empty() {
        return;
    }
    let topic_ids = state.topic_ids.lock().await.clone();
    let topics = build_commit_topics(offsets, &topic_ids);
    // ... rest unchanged (send OffsetCommitRequest) ...
```

Add `use crate::builder::AutoOffsetReset;` if not already imported (the new `None` arm references it; `prime_offsets` already matches on `state.auto_offset_reset`).

- [ ] **Step 6: Verify the crate builds and existing tests pass**

Run: `cargo test -p crabka-client-consumer`
Expected: PASS. Fix any remaining `ConsumerRecord { .. }` construction sites the compiler flags (e.g. `poll.rs:181` — leave a temporary `leader_epoch: -1,` there; Task 8 replaces it). Also fix `commit_revoked`'s steady-state caller if a non-revoke commit path exists (search `build_commit_topics` callers).

- [ ] **Step 7: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-client-consumer --all-targets -- -D warnings
git add crates/client-consumer/src/builder.rs crates/client-consumer/src/offset_wire.rs \
        crates/client-consumer/src/consumer.rs crates/client-consumer/src/coordinator.rs
git commit -m "feat(client-consumer): positions sidecar, committed-epoch round-trip, None reset (KIP-320)"
```

---

## Batch 4 (depends on Task 7 + Batch 1)

### Task 8: Consumer poll — validate pass, error-first loop, epoch fetch fields

**Files:**
- Create: `crates/client-consumer/src/validate.rs`
- Modify: `crates/client-consumer/src/poll.rs`
- Modify: `crates/client-consumer/src/consumer.rs` (re-export `mod validate;` is in `lib.rs`)
- Modify: `crates/client-consumer/src/lib.rs`
- Modify: `crates/client-consumer/src/position.rs` (remove the `dead_code` shim)

- [ ] **Step 1: Register `validate` module**

In `crates/client-consumer/src/lib.rs` add:

```rust
mod validate;
```

- [ ] **Step 2: Write `validate.rs` (metadata refresh + proactive validation)**

Create `crates/client-consumer/src/validate.rs`:

```rust
//! KIP-320 consumer position validation. Two responsibilities:
//!   1. Refresh per-partition leader id + leader epoch from `Metadata`, flagging
//!      a partition `awaiting_validation` when its leader epoch advances.
//!   2. For flagged partitions, issue `OffsetForLeaderEpoch` and decide (via
//!      `position::classify`) whether to resume or reset for truncation.

use std::collections::HashMap;

use crabka_protocol::owned::metadata_request::MetadataRequest;

use crate::consumer::Consumer;
use crate::error::ConsumerError;
use crate::position::{ValidationOutcome, classify};

impl Consumer {
    /// Refresh leader id / leader epoch for `topics` from `Metadata`. A
    /// partition whose metadata leader epoch is greater than the epoch we last
    /// consumed (`offset_epoch`) is flagged `awaiting_validation`.
    pub(crate) async fn refresh_leader_epochs(&self) -> Result<(), ConsumerError> {
        let md = self.client.send(MetadataRequest::default()).await?;
        let mut positions = self.positions.lock().await;
        for t in &md.topics {
            let Some(name) = &t.name else { continue };
            for p in &t.partitions {
                let key = (name.clone(), p.partition_index);
                let entry = positions.entry(key).or_default();
                entry.leader_id = p.leader_id;
                if p.leader_epoch > entry.leader_epoch {
                    entry.leader_epoch = p.leader_epoch;
                    if p.leader_epoch > entry.offset_epoch && entry.offset_epoch >= 0 {
                        entry.awaiting_validation = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate every `awaiting_validation` partition via `OffsetForLeaderEpoch`.
    /// Returns the set of partitions that truncated, mapped to the safe offset
    /// the caller must reset `next_offsets` to. Clears the validation flag for
    /// partitions confirmed consistent.
    pub(crate) async fn validate_positions(
        &self,
    ) -> Result<HashMap<(String, i32), i64>, ConsumerError> {
        // Snapshot the work to do under the lock, then issue RPCs unlocked.
        let to_validate: Vec<(String, i32, i64, i32, i32)> = {
            let positions = self.positions.lock().await;
            let offsets = self.next_offsets.lock().await;
            positions
                .iter()
                .filter(|(_, p)| p.awaiting_validation && p.offset_epoch >= 0)
                .filter_map(|((t, part), p)| {
                    let off = *offsets.get(&(t.clone(), *part))?;
                    Some((t.clone(), *part, off, p.offset_epoch, p.leader_epoch))
                })
                .collect()
        };

        let mut truncated: HashMap<(String, i32), i64> = HashMap::new();
        for (topic, partition, offset, offset_epoch, leader_epoch) in to_validate {
            let answer = crabka_client_core::offset_for_leader_epoch(
                self.client.connection(),
                &topic,
                partition,
                leader_epoch,
                offset_epoch,
            )
            .await?;
            // Re-check the partition is still assigned + epoch unchanged before
            // applying — a rebalance may have moved it.
            let mut positions = self.positions.lock().await;
            let Some(pos) = positions.get_mut(&(topic.clone(), partition)) else { continue };
            if pos.leader_epoch != leader_epoch {
                continue; // metadata moved under us; revalidate next poll
            }
            match classify(offset, offset_epoch, answer.leader_epoch, answer.end_offset) {
                ValidationOutcome::Valid { leader_epoch: le } => {
                    pos.offset_epoch = le;
                    pos.awaiting_validation = false;
                }
                ValidationOutcome::Truncated { safe_offset } => {
                    pos.awaiting_validation = false;
                    truncated.insert((topic, partition), safe_offset);
                }
            }
        }
        Ok(truncated)
    }
}
```

(If `Client` exposes its connection by a different accessor than `connection()`, adjust — confirm with `rg "pub fn connection|pub(crate) fn connection|impl Client" crates/client-core/src`. If the client only exposes `send`, add a thin `offset_for_leader_epoch` method on `Client` in client-core that wraps the helper, and call that instead.)

- [ ] **Step 3: Write the failing poll tests (drive the integration in Task 12)**

The poll-loop behavior is exercised end-to-end in Task 12 (integration). For this task, add one unit test to `validate.rs` confirming `refresh_leader_epochs` flag logic via the pure pieces — but since it needs a `Consumer`, defer assertion to Task 12. Mark this step done by confirming `cargo build -p crabka-client-consumer` compiles after Step 4.

- [ ] **Step 4: Rewrite the poll loop (error-first + epochs + reset)**

In `crates/client-consumer/src/poll.rs`:

(a) **Run the validate pass first.** After `resolve_latest_sentinels` (`poll.rs:29`), add:

```rust
        // KIP-320: refresh leader epochs and proactively validate any position
        // whose leader epoch advanced, before fetching. Truncated partitions
        // are reset here (or surfaced for auto.offset.reset=None below).
        self.refresh_leader_epochs().await?;
        let truncated = self.validate_positions().await?;
        if !truncated.is_empty() {
            self.apply_truncation(&truncated).await?;
        }
```

(b) **Send the epoch fields.** In the `FetchPartition` builder (`poll.rs:57-62`), read the position's epochs. Replace the `by_topic` build (`poll.rs:38-45`) to also capture epochs, and the partition map:

```rust
        let mut by_topic: HashMap<String, Vec<(i32, i64, i32, i32)>> = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            let positions = self.positions.lock().await;
            for (t, p) in &assigned {
                // Skip partitions still awaiting validation — they must not be
                // fetched until proven consistent.
                if positions.get(&(t.clone(), *p)).is_some_and(|x| x.awaiting_validation) {
                    continue;
                }
                let next = offsets.get(&(t.clone(), *p)).copied().unwrap_or(0);
                let pos = positions.get(&(t.clone(), *p)).copied().unwrap_or_default();
                by_topic
                    .entry(t.clone())
                    .or_default()
                    .push((*p, next, pos.leader_epoch, pos.offset_epoch));
            }
        }
```

And the `FetchPartition` construction:

```rust
                    partitions: plist
                        .into_iter()
                        .map(|(p, off, leader_epoch, last_fetched_epoch)| FetchPartition {
                            partition: p,
                            fetch_offset: off,
                            current_leader_epoch: leader_epoch,
                            last_fetched_epoch,
                            partition_max_bytes: 1 << 20,
                            ..Default::default()
                        })
                        .collect(),
```

(c) **Error-first partition handling.** Replace the per-partition body (`poll.rs:111-193`) so error codes and `diverging_epoch` are inspected before decoding. Insert at the top of the `for part in &topic.partitions {` loop, after the `still_owned` check (`poll.rs:114-116`):

```rust
                let key = (topic_name.clone(), part.partition_index);

                // KIP-320 in-band truncation: leader served no records and told
                // us where to truncate.
                if part.diverging_epoch.end_offset >= 0 {
                    self.handle_truncation_in_poll(&mut offsets, &key, part.diverging_epoch.end_offset)?;
                    continue;
                }
                match part.error_code {
                    0 => {}
                    1 /* OFFSET_OUT_OF_RANGE */ => {
                        // Reset per policy; None surfaces an error.
                        let safe = self.reset_offset_for(&key).await?;
                        offsets.insert(key.clone(), safe);
                        continue;
                    }
                    74 /* FENCED_LEADER_EPOCH */ | 75 /* UNKNOWN_LEADER_EPOCH */ | 6 /* NOT_LEADER_OR_FOLLOWER */ => {
                        // Mark for validation + metadata refresh next poll.
                        let mut positions = self.positions.lock().await;
                        if let Some(p) = positions.get_mut(&key) {
                            p.awaiting_validation = true;
                        }
                        continue;
                    }
                    other => {
                        return Err(ConsumerError::Server(other));
                    }
                }
```

(Confirm the numeric codes against `crabka_protocol`/broker `codes` — prefer importing the named constants if the consumer crate can depend on them; otherwise the literals above match Kafka: `OFFSET_OUT_OF_RANGE=1`, `NOT_LEADER_OR_FOLLOWER=6`, `FENCED_LEADER_EPOCH=74`, `UNKNOWN_LEADER_EPOCH=75`.)

(d) **Capture batch leader epoch + populate `ConsumerRecord.leader_epoch`.** In the record-emit loop (`poll.rs:179-189`), set the field and track the highest epoch seen so the position advances:

```rust
                    for r in &batch.records {
                        let offset = batch.base_offset + i64::from(r.offset_delta);
                        out.push(ConsumerRecord {
                            topic: topic_name.clone(),
                            partition: part.partition_index,
                            offset,
                            leader_epoch: batch.partition_leader_epoch,
                            timestamp: batch.base_timestamp + r.timestamp_delta,
                            key: r.key.clone(),
                            value: r.value.clone(),
                        });
                    }
```

After advancing `offsets` (`poll.rs:191-193`), update the position's `offset_epoch` to the last batch's epoch:

```rust
                if let Some(next) = next_offset_after(batches) {
                    offsets.insert(key.clone(), next);
                    if let Some(last_epoch) = batches.iter().map(|b| b.partition_leader_epoch).max() {
                        let mut positions = self.positions.lock().await;
                        positions.entry(key.clone()).or_default().offset_epoch = last_epoch;
                    }
                }
```

- [ ] **Step 5: Add the reset/truncation helpers**

Add these `impl Consumer` methods at the bottom of `poll.rs` (before the final `#[cfg(test)]`):

```rust
impl Consumer {
    /// Resolve the safe offset for an `OFFSET_OUT_OF_RANGE` reset under the
    /// configured policy. `None` policy surfaces a `LogTruncation` error.
    async fn reset_offset_for(&self, key: &(String, i32)) -> Result<i64, ConsumerError> {
        match self.auto_offset_reset {
            crate::builder::AutoOffsetReset::Earliest => Ok(0),
            crate::builder::AutoOffsetReset::Latest => Ok(i64::MAX), // resolved next poll
            crate::builder::AutoOffsetReset::None => {
                let fetch_offset = self
                    .next_offsets
                    .lock()
                    .await
                    .get(key)
                    .copied()
                    .unwrap_or(-1);
                Err(ConsumerError::LogTruncation {
                    topic: key.0.clone(),
                    partition: key.1,
                    fetch_offset,
                    safe_offset: 0,
                })
            }
        }
    }

    /// Apply truncations detected by the proactive validate pass to `next_offsets`,
    /// honoring `auto.offset.reset` (None → error on the first truncated partition).
    async fn apply_truncation(
        &self,
        truncated: &std::collections::HashMap<(String, i32), i64>,
    ) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        for (key, safe_offset) in truncated {
            match self.auto_offset_reset {
                crate::builder::AutoOffsetReset::None => {
                    let fetch_offset = offsets.get(key).copied().unwrap_or(-1);
                    return Err(ConsumerError::LogTruncation {
                        topic: key.0.clone(),
                        partition: key.1,
                        fetch_offset,
                        safe_offset: *safe_offset,
                    });
                }
                _ => {
                    offsets.insert(key.clone(), *safe_offset);
                }
            }
        }
        Ok(())
    }

    /// In-band `diverging_epoch` handler used inside the poll loop while the
    /// `next_offsets` guard is already held.
    fn handle_truncation_in_poll(
        &self,
        offsets: &mut std::collections::HashMap<(String, i32), i64>,
        key: &(String, i32),
        safe_offset: i64,
    ) -> Result<(), ConsumerError> {
        match self.auto_offset_reset {
            crate::builder::AutoOffsetReset::None => {
                let fetch_offset = offsets.get(key).copied().unwrap_or(-1);
                Err(ConsumerError::LogTruncation {
                    topic: key.0.clone(),
                    partition: key.1,
                    fetch_offset,
                    safe_offset,
                })
            }
            _ => {
                offsets.insert(key.clone(), safe_offset);
                Ok(())
            }
        }
    }
}
```

`Consumer` must expose `auto_offset_reset`: add `pub(crate) auto_offset_reset: AutoOffsetReset,` to the struct (`consumer.rs:34-56`), set it in the returned `Consumer { .. }` (it is already a `start` parameter), and import `AutoOffsetReset` in `poll.rs`.

- [ ] **Step 6: Remove the `dead_code` shim**

Delete the `#![allow(dead_code)]` line added to `position.rs` in Task 4 (all items are now used).

- [ ] **Step 7: Verify build + crate tests**

Run: `cargo test -p crabka-client-consumer`
Expected: PASS (existing tests green; new behaviour covered in Task 12).

- [ ] **Step 8: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-client-consumer --all-targets -- -D warnings
git add crates/client-consumer/src/validate.rs crates/client-consumer/src/poll.rs \
        crates/client-consumer/src/consumer.rs crates/client-consumer/src/lib.rs \
        crates/client-consumer/src/position.rs
git commit -m "feat(client-consumer): proactive position validation + error-first poll (KIP-320)"
```

---

## Batch 5 (depends on prior batches)

### Task 9: Broker truncation integration tests

**Files:**
- Modify: `crates/broker/tests/leader_epoch.rs`

- [ ] **Step 1: Write the leader divergence test**

Add to `crates/broker/tests/leader_epoch.rs`, reusing the file's existing broker-start + produce helpers (mirror `test_fenced_leader_epoch_truncates_zombie_writes`). The test:

```rust
#[tokio::test]
async fn diverging_epoch_returned_on_stale_last_fetched_epoch() {
    // Start a single broker; create a partition; produce records, force a
    // leader-epoch bump (controlled shutdown + re-elect, as the existing
    // fence test does), then produce more so the epoch cache has
    //   epoch e0 -> [0, k), epoch e1 -> [k, n).
    // Issue a follower Fetch at fetch_offset = n with last_fetched_epoch = e0.
    // Assert the response carries diverging_epoch.end_offset == k and no records.
    // (Use the same harness/produce/elect helpers already in this file.)
}
```

Fill in using the file's existing helpers. The key assertions:

```rust
    assert!(part.diverging_epoch.end_offset == k);
    assert!(part.diverging_epoch.epoch == e0);
    assert!(part.records.is_none() || part.records.as_ref().unwrap().as_v2().map_or(true, |b| b.is_empty()));
```

- [ ] **Step 2: Write the follower in-band truncation test**

```rust
#[tokio::test]
async fn follower_truncates_in_band_on_diverging_epoch() {
    // Two brokers, one partition, broker A leader. Produce + replicate so B
    // matches A. Induce divergence: write a divergent suffix to B's local log
    // via the partition's test-accessible log handle (append batches at a
    // stale epoch that A does not have), bump A's epoch and produce new
    // records at the new epoch. Start/resume B's replicator and poll until
    // B.log_end_offset() converges with A; assert B truncated its divergent
    // suffix (B's epoch checkpoint no longer contains the stale-epoch entry
    // beyond the diverging offset, and B's records match A's).
}
```

Use the cluster harness already used by the broker replication tests (search `tests/` for a two-broker setup, e.g. `replication`/`elect_leaders` tests). Assert convergence:

```rust
    // Within a bounded poll, B's log matches A's.
    assert!(broker_b.partition_leo("t", 0) == broker_a.partition_leo("t", 0));
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-broker --test leader_epoch`
Expected: PASS (including the two new tests).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/leader_epoch.rs
git commit -m "test(broker): KIP-320 diverging_epoch + follower in-band truncation"
```

---

### Task 10: Consumer truncation integration tests

**Files:**
- Modify: `crates/client-consumer/tests/integration.rs`

- [ ] **Step 1: Write the tests** (reuse the file's existing broker+producer+consumer harness, e.g. the pattern around `integration.rs:175`):

```rust
#[tokio::test]
async fn consumer_resets_on_offset_out_of_range_earliest() {
    // Produce N records; delete-records (or retention) so log_start moves past
    // the consumer's committed offset; poll with auto_offset_reset=Earliest;
    // assert poll recovers from offset 0 rather than erroring.
}

#[tokio::test]
async fn consumer_none_policy_surfaces_log_truncation() {
    // Same divergence as above but auto_offset_reset=None; assert poll() returns
    // Err(ConsumerError::LogTruncation { .. }).
}

#[tokio::test]
async fn consumer_proactive_validation_on_leader_epoch_bump() {
    // Consume some records (records carry leader_epoch == e0). Force a
    // leadership change so metadata leader epoch becomes e1 and the partition's
    // tail at e0 is truncated. poll(): refresh_leader_epochs flags the
    // partition, validate_positions issues OffsetForLeaderEpoch, classify
    // detects truncation, next_offsets resets to the safe offset; assert the
    // consumer continues from the safe offset.
}

#[tokio::test]
async fn committed_leader_epoch_survives_restart() {
    // Consume + commit (committed_leader_epoch == e0 is sent). Drop the
    // consumer; rebuild subscribed to the same group. Assert prime seeds
    // positions[..].offset_epoch == e0 (so a subsequent leader-epoch bump
    // triggers validation). Inspect via a fresh poll that validates.
}
```

Fill bodies with the harness's helpers (start broker, `Producer`, `Consumer::builder()...build()`). For inducing truncation deterministically, prefer `DeleteRecords` (moves `log_start`, yields `OFFSET_OUT_OF_RANGE`) for the reset tests, and a controlled leadership change for the proactive-validation test (mirror the broker `elect_leaders` test harness).

- [ ] **Step 2: Run the tests**

Run: `cargo test -p crabka-client-consumer --test integration`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all && cargo clippy -p crabka-client-consumer --all-targets -- -D warnings
git add crates/client-consumer/tests/integration.rs
git commit -m "test(client-consumer): KIP-320 truncation detection + restart survival"
```

---

### Task 11: JVM mixed-cluster divergence scenario

**Files:**
- Modify: the `broker-jvm-acceptance` harness (locate via `rg -l "jvm_acceptance|broker-jvm-acceptance" --type rust` and the harness scripts under `crates/` / `tests/`).

- [ ] **Step 1: Wire-conformance check**

Add a scenario where a JVM client (AdminClient / `kafka-console-consumer`) and a Crabka broker exchange `OffsetForLeaderEpoch` and Fetch v12+: produce across two epochs on a Crabka leader, issue a Java `OffsetForLeaderEpoch` (via AdminClient or a small Java helper already in the harness) for the old epoch, and assert the returned end offset equals the epoch boundary. Confirms byte-exactness + v12+ `diverging_epoch` encoding decodes in the JVM client.

- [ ] **Step 2: Induced-divergence scenario**

Mixed JVM+Crabka cluster (the harness already supports a mixed quorum per `2026-06-01-kip595-slice6-mixed-quorum`). Steps: produce to a partition, replicate, force an unclean leadership change so a follower with fewer records becomes leader, produce divergent records at the new epoch, rejoin the old leader as a follower. Assert:
- a **JVM follower truncates from a Crabka leader** (JVM broker logs converge to the Crabka leader), and
- a **JVM consumer recovers** (kafka-console-consumer continues without error after the truncation), and
- where the harness allows, a **Crabka follower truncates from a JVM leader**.

- [ ] **Step 3: Run the JVM scenario**

Run the harness's KIP-320 scenario (follow the harness's existing invocation, e.g. `cargo test -p <harness-crate> kip320 -- --ignored` or the harness script). Expected: PASS. (Per the benchmark/JVM memory, JVM runs are Linux-bound; run on the Linux harness, not the Mac.)

- [ ] **Step 4: Commit**

```bash
git add <harness files>
git commit -m "test(jvm): KIP-320 mixed-cluster divergence + truncation recovery"
```

---

### Task 12: Docs — flip KIP-320 to ✅

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Update the README matrix**

In `README.md`, change the two KIP-320 rows from ⚠️ to ✅:
- `| [KIP-320](...) | Detect & handle log truncation (leader epoch in fetch) | ✅ |` (line ~394)
- Update the prose at lines ~62-64 if it qualifies KIP-320 as partial.

- [ ] **Step 2: Note completion in STATUS.md**

Add a short slice entry to `STATUS.md` summarizing the KIP-320 completion (leader diverging_epoch, follower in-band truncation, Java-faithful consumer validation, mixed-JVM validation), matching the file's existing slice-entry format.

- [ ] **Step 3: Commit**

```bash
git add README.md STATUS.md
git commit -m "docs: mark KIP-320 (log-truncation detection) complete"
```

---

## Self-review

**Spec coverage:**
- Leader-epoch cache `epoch_and_offset_for` → Task 1. ✓
- Leader Fetch divergence + `current_leader` (v12+) → Task 5. ✓
- Follower `last_fetched_epoch` + in-band `diverging_epoch` truncation → Task 6. ✓
- Consumer position model + `awaiting_validation` lifecycle → Tasks 4, 7, 8. ✓
- Metadata-driven epoch tracking → Task 8 (`refresh_leader_epochs`). ✓
- Proactive `OffsetForLeaderEpoch` validate-positions → Tasks 2, 8 (`validate_positions`). ✓
- Error-first poll (diverging_epoch / OFFSET_OUT_OF_RANGE / FENCED / UNKNOWN / NOT_LEADER) → Task 8. ✓
- `AutoOffsetReset::None` + `LogTruncation` → Tasks 3, 7, 8. ✓
- `committed_leader_epoch` round-trip → Task 7. ✓
- `ConsumerRecord.leader_epoch` → Tasks 7, 8. ✓
- Concurrency discipline (validate unlocked, apply locked + re-check) → Task 8 (`validate_positions`). ✓
- Tests: epoch-cache (T1), leader handler + follower (T9), consumer (T10), JVM mixed (T11). ✓
- Docs (T12). ✓
- `snapshot_id` left out of scope → not touched. ✓

**Placeholder scan:** Tasks 9–11 reference "the file's existing harness helpers" rather than inlining a full two-broker/JVM harness — this is deliberate (the helpers exist and vary; inlining would be wrong, not just verbose). The assertions and scenario steps are concrete. All `src/` code steps contain complete code.

**Type consistency:** `PartitionPosition { offset_epoch, leader_id, leader_epoch, awaiting_validation }` and `classify(offset, offset_epoch, leader_epoch, leader_end_offset) -> ValidationOutcome::{Valid{leader_epoch}, Truncated{safe_offset}}` are used consistently across Tasks 4/8. `build_commit_topics(HashMap<(String,i32),(i64,i32)>, ..)` and `parse_offset_fetch -> Vec<(String,i32,i64,i32)>` are used consistently across Tasks 7. `epoch_and_offset_for(i32,i64)->(i32,i64)` consistent across Tasks 1/5/6.

**Open verification items flagged inline for the implementer** (not blockers — confirm against the live tree): `ClientError::Server` field name (Task 2); `Client::connection()` accessor vs. a wrapper method (Task 8); whether `image` is in scope at the divergence site in `fetch.rs` (Task 5); the exact named error-code constants available to the consumer crate (Task 8); any non-revoke `build_commit_topics` caller (Task 7).
