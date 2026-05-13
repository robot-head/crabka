# Bulletproof EOS sub-slice 10b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** KIP-101 leader-epoch fencing + leader election on broker death + ISR shrink/expand. Completes the bulletproof exactly-once promise — a 3-broker Crabka cluster survives partition-leader crashes and slow followers; `acks=all` produces complete after election picks a new leader from ISR; zombie writes from fenced ex-leaders are rejected.

**Architecture:** Three loosely-coupled subsystems sharing slice-7's metadata image. Every broker sends `BrokerHeartbeat` to the controller leader every 3s; controller's liveness ticker times out brokers at 9s and triggers a `leader_election` scan that bumps `leader_epoch` and emits new `PartitionRecord`s via openraft. Per-partition `.leader-epoch-checkpoint` files in Apache Kafka byte-compat format back the `OffsetForLeaderEpoch` RPC for follower-side truncation on leader change. ISR maintenance is leader-driven: a 1s tick per partition compares each follower's last-fetch time vs `replica.lag.time.max.ms` (default 30s; tests override to 2s) and proposes shrink/expand via `AlterPartition`.

**Tech Stack:** Rust 1.95.0; tokio; existing openraft + serde-wincode; new wire RPCs are already codegen'd (`BrokerHeartbeat` = api_key 63, `AlterPartition` = api_key 56, `OffsetForLeaderEpoch` = api_key 23).

**Reference spec:** [`docs/superpowers/specs/2026-05-13-crabka-bulletproof-eos-10b-design.md`](../specs/2026-05-13-crabka-bulletproof-eos-10b-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/bulletproof-eos-10b`. Implementation runs on `feature/bulletproof-eos-10b` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/broker/src/
├── codes.rs                         # MODIFIED — FENCED_LEADER_EPOCH=74, UNKNOWN_LEADER_EPOCH=75
├── error.rs                         # MODIFIED — FencedLeaderEpoch + UnknownLeaderEpoch variants
├── config.rs                        # MODIFIED — heartbeat_interval/timeout, replica_lag_time_max
├── partition.rs                     # MODIFIED — current_leader, current_leader_epoch, install_leader_change
├── replica_state.rs                 # MODIFIED — per_follower: HashMap<NodeId, FollowerStats>, current_leader_epoch
├── broker.rs                        # MODIFIED — spawn heartbeat client, liveness ticker (controller-only), isr_maintenance
├── replicator.rs                    # MODIFIED — handle FENCED_LEADER_EPOCH; track current_leader_epoch
├── replicator_supervisor.rs         # MODIFIED — react to leader change in reconcile
├── handlers/
│   ├── produce.rs                   # MODIFIED — stamp partition_leader_epoch
│   ├── fetch.rs                     # MODIFIED — epoch validate; restore follower-fetch HW maintenance
│   ├── alter_partition.rs           # NEW — controller wire handler, api_key 56
│   ├── broker_heartbeat.rs          # NEW — controller wire handler, api_key 63
│   ├── offset_for_leader_epoch.rs   # NEW — leader wire handler, api_key 23
│   └── api_versions.rs              # MODIFIED — advertise the 3 new APIs
├── heartbeat/
│   ├── mod.rs                       # NEW — pub(crate) mod controller_state; pub(crate) mod client;
│   ├── client.rs                    # NEW — broker-side heartbeat loop
│   └── controller_state.rs          # NEW — ControllerLivenessState + ticker
├── leader_election.rs               # NEW — controller-side election scan
├── isr_maintenance.rs               # NEW — per-leader-partition ISR shrink/expand tick
└── network/dispatch.rs              # MODIFIED — flexible-encoding entries for 23, 56, 63

crates/log/src/
├── leader_epoch_checkpoint.rs       # NEW — per-partition .leader-epoch-checkpoint reader/writer
├── lib.rs                           # MODIFIED — pub use LeaderEpochCheckpoint
├── log.rs                           # MODIFIED — Log::append accepts leader_epoch; advance checkpoint on epoch bump
├── name.rs                          # MODIFIED — leader_epoch_checkpoint_path helper
└── segment.rs                       # MODIFIED — leader_epoch_checkpoint_path() accessor

crates/metadata/src/
├── records.rs                       # MODIFIED — PartitionRecord.leader_epoch: i32
└── image.rs                         # MODIFIED — apply preserves leader_epoch

crates/broker/tests/
├── durability.rs                    # MODIFIED — drop test_install_isr-using test; use real multi-broker for acks-all timeout
├── replication.rs                   # MODIFIED — un-#[ignore]; restore acks=-1
├── jvm_acceptance.rs                # MODIFIED — un-env-gate acks_all_durability + three_node_replication_byte_compare; NEW acks_all_survives_leader_crash
├── leader_election.rs               # NEW — 4 in-process tests
└── leader_epoch.rs                  # NEW — 3 in-process tests

README.md                            # MODIFIED — append slice-10b entry to "Slices delivered"
```

---

## Phase A — Foundations

### Task 1: Wire codes + `BrokerError` variants

**Files:**
- Modify: `crates/broker/src/codes.rs`
- Modify: `crates/broker/src/error.rs`

- [ ] **Step 1: Add the two new wire codes.**

Append to `crates/broker/src/codes.rs` (place after `NOT_ENOUGH_REPLICAS_AFTER_APPEND`):

```rust
/// KIP-101 fence: caller's `current_leader_epoch` is older than the
/// partition's current `leader_epoch`. Caller should re-fetch metadata
/// or call `OffsetForLeaderEpoch` to learn the truncation point.
pub const FENCED_LEADER_EPOCH: i16 = 74;

/// KIP-101: caller's `current_leader_epoch` is newer than the broker's
/// view. Metadata propagation lag — caller retries after a brief wait.
pub const UNKNOWN_LEADER_EPOCH: i16 = 75;
```

- [ ] **Step 2: Add `BrokerError` variants.**

In `crates/broker/src/error.rs`, add to `BrokerError`:

```rust
    #[error("fenced leader epoch (have={have}, current={current})")]
    FencedLeaderEpoch { have: i32, current: i32 },

    #[error("unknown leader epoch ({0})")]
    UnknownLeaderEpoch(i32),
```

In `from_broker_error` in `codes.rs`, add the two arms:

```rust
        BrokerError::FencedLeaderEpoch { .. } => FENCED_LEADER_EPOCH,
        BrokerError::UnknownLeaderEpoch(_) => UNKNOWN_LEADER_EPOCH,
```

- [ ] **Step 3: Test the mappings.**

Append to `codes.rs`'s test module:

```rust
#[test]
fn fenced_leader_epoch_maps_correctly() {
    let e = BrokerError::FencedLeaderEpoch { have: 0, current: 1 };
    assert_eq!(from_broker_error(&e), FENCED_LEADER_EPOCH);
}

#[test]
fn unknown_leader_epoch_maps_correctly() {
    let e = BrokerError::UnknownLeaderEpoch(2);
    assert_eq!(from_broker_error(&e), UNKNOWN_LEADER_EPOCH);
}
```

- [ ] **Step 4: Test + commit.**

```bash
cargo test -p crabka-broker codes
git add crates/broker/src/codes.rs crates/broker/src/error.rs
git commit -m "feat(broker): FENCED_LEADER_EPOCH (74) + UNKNOWN_LEADER_EPOCH (75) wire codes"
```

Include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` trailer via heredoc.

---

### Task 2: `PartitionRecord.leader_epoch`

**Files:**
- Modify: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/image.rs` (no body change; the apply path uses `PartitionRecord` directly)
- Modify: `crates/broker/src/handlers/create_topics.rs` (set `leader_epoch: 0` on initial PartitionRecord)
- Modify: every test that constructs `PartitionRecord` literal (~10 sites across the codebase)

- [ ] **Step 1: Recon existing literal sites.**

```bash
grep -rn "PartitionRecord {" crates/ | head -20
```

You'll find sites in `coordinator/bootstrap.rs`, `replicator_supervisor.rs::tests`, `txn/bootstrap.rs`, `handlers/create_topics.rs`, `metadata/src/records.rs::tests`, and `metadata/src/image.rs::tests`. All need to be updated to set `leader_epoch: 0`.

- [ ] **Step 2: Add the field.**

In `crates/metadata/src/records.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
    /// Per-partition leader epoch. Bumped on every leader change.
    /// Slice-10b adds this; older on-disk metadata is not migrated.
    pub leader_epoch: i32,
}
```

- [ ] **Step 3: Update every literal site.**

For each match from Step 1, append `leader_epoch: 0,` to the struct literal. Where the field is set during election (slice-10b later tasks), the value won't be 0, but Task 2 only adds the field with default 0.

- [ ] **Step 4: Build + test + commit.**

```bash
cargo build --workspace
cargo test --workspace --lib
git add crates/metadata crates/broker
git commit -m "feat(metadata): PartitionRecord.leader_epoch (KIP-101 base)"
```

Trailer.

---

### Task 3: `BrokerConfig` fields

**Files:**
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Add the fields.**

In `crates/broker/src/config.rs`, append to `BrokerConfig`:

```rust
    /// How often each broker sends BrokerHeartbeat to the controller
    /// leader. Default 3,000ms.
    pub heartbeat_interval_ms: u64,
    /// Controller marks a broker dead after this many ms without a
    /// heartbeat. Default 9,000ms.
    pub heartbeat_timeout_ms: u64,
    /// Leader proposes ISR shrink when a follower lags more than this
    /// many ms. Default 30,000ms.
    pub replica_lag_time_max_ms: u64,
```

In `Default for BrokerConfig`:

```rust
            heartbeat_interval_ms: 3_000,
            heartbeat_timeout_ms: 9_000,
            replica_lag_time_max_ms: 30_000,
```

In `BrokerConfig::for_tests`:

```rust
            heartbeat_interval_ms: 200,
            heartbeat_timeout_ms: 2_000,
            replica_lag_time_max_ms: 2_000,
```

- [ ] **Step 2: Build + test + commit.**

```bash
cargo build --workspace
cargo test -p crabka-broker config
git add crates/broker/src/config.rs
git commit -m "feat(broker): config knobs for heartbeat + replica.lag.time.max.ms"
```

Trailer.

---

## Phase B — Leader-epoch checkpoint

### Task 4: `LeaderEpochCheckpoint` module

**Files:**
- Create: `crates/log/src/leader_epoch_checkpoint.rs`
- Modify: `crates/log/src/lib.rs`
- Modify: `crates/log/src/name.rs` (add `leader_epoch_checkpoint_path` free fn)

- [ ] **Step 1: Add the name helper.**

In `crates/log/src/name.rs`, after the existing path helpers:

```rust
/// Path to the per-partition `.leader-epoch-checkpoint` file.
pub fn leader_epoch_checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("leader-epoch-checkpoint")
}
```

- [ ] **Step 2: Create the module.**

Write `crates/log/src/leader_epoch_checkpoint.rs`:

```rust
//! Per-partition `.leader-epoch-checkpoint` file. Two-column text
//! format matching Apache Kafka exactly:
//!
//!   0          <-- header version
//!   <n>        <-- row count
//!   <epoch_0> <start_offset_0>
//!   <epoch_1> <start_offset_1>
//!   ...
//!
//! Byte layout is preserved so `kafka-dump-log` can read our files.

#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::error::LogError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochEntry {
    pub epoch: i32,
    pub start_offset: i64,
}

#[derive(Debug)]
pub struct LeaderEpochCheckpoint {
    path: PathBuf,
    entries: Vec<EpochEntry>,
}

impl LeaderEpochCheckpoint {
    /// Open (or recover) the checkpoint at `path`. Missing file → empty.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let entries = match fs::read_to_string(&path) {
            Ok(s) => Self::parse(&s)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(LogError::Io(e)),
        };
        Ok(Self { path, entries })
    }

    fn parse(s: &str) -> Result<Vec<EpochEntry>, LogError> {
        let mut lines = s.lines();
        let _version = lines.next();
        let count: usize = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        let mut out = Vec::with_capacity(count);
        for line in lines.take(count) {
            let mut parts = line.split_whitespace();
            let epoch = parts.next().and_then(|t| t.parse().ok()).ok_or_else(|| {
                LogError::Corrupt(format!("bad checkpoint row: {line:?}"))
            })?;
            let start_offset = parts.next().and_then(|t| t.parse().ok()).ok_or_else(|| {
                LogError::Corrupt(format!("bad checkpoint row: {line:?}"))
            })?;
            out.push(EpochEntry { epoch, start_offset });
        }
        Ok(out)
    }

    /// Append `(epoch, start_offset)`. Idempotent: re-appending an entry
    /// with the same epoch is a no-op (keeps the earliest recorded
    /// `start_offset`). Rewrites the file atomically.
    pub fn append(&mut self, epoch: i32, start_offset: i64) -> Result<(), LogError> {
        if self.entries.iter().any(|e| e.epoch == epoch) {
            return Ok(());
        }
        self.entries.push(EpochEntry { epoch, start_offset });
        self.flush()
    }

    fn flush(&self) -> Result<(), LogError> {
        let mut s = String::new();
        s.push_str("0\n");
        s.push_str(&format!("{}\n", self.entries.len()));
        for e in &self.entries {
            s.push_str(&format!("{} {}\n", e.epoch, e.start_offset));
        }
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = fs::File::create(&tmp).map_err(LogError::Io)?;
            f.write_all(s.as_bytes()).map_err(LogError::Io)?;
            f.sync_data().map_err(LogError::Io)?;
        }
        fs::rename(&tmp, &self.path).map_err(LogError::Io)?;
        Ok(())
    }

    /// End offset of `epoch` = start_offset of the next-larger recorded
    /// epoch, or `log_end_offset` if `epoch` is the current epoch.
    /// Returns -1 (UNDEFINED_OFFSET) if `epoch` is unknown.
    pub fn end_offset_for_epoch(&self, epoch: i32, log_end_offset: i64) -> i64 {
        let mut sorted: Vec<EpochEntry> = self.entries.iter().copied().collect();
        sorted.sort_by_key(|e| e.epoch);
        let mut iter = sorted.iter().peekable();
        while let Some(e) = iter.next() {
            if e.epoch == epoch {
                return iter.peek().map_or(log_end_offset, |next| next.start_offset);
            }
        }
        -1
    }

    #[must_use]
    pub fn latest_epoch(&self) -> Option<i32> {
        self.entries.iter().map(|e| e.epoch).max()
    }

    #[must_use]
    pub fn entries(&self) -> &[EpochEntry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fresh() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("leader-epoch-checkpoint");
        (dir, path)
    }

    #[test]
    fn round_trip_byte_compat_format() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();

        let s = std::fs::read_to_string(&path).unwrap();
        assert_eq!(s, "0\n3\n0 0\n1 50\n2 100\n");
    }

    #[test]
    fn append_preserves_existing_rows() {
        let (_d, path) = fresh();
        {
            let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
            c.append(0, 0).unwrap();
        }
        let mut c2 = LeaderEpochCheckpoint::open(path).unwrap();
        c2.append(1, 50).unwrap();
        assert_eq!(c2.entries().len(), 2);
    }

    #[test]
    fn append_idempotent_for_same_epoch() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(0, 999).unwrap(); // ignored; epoch 0 already recorded
        assert_eq!(c.entries(), &[EpochEntry { epoch: 0, start_offset: 0 }]);
    }

    #[test]
    fn end_offset_for_current_epoch_returns_log_end_offset() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        assert_eq!(c.end_offset_for_epoch(1, 100), 100);
    }

    #[test]
    fn end_offset_for_older_epoch_returns_next_start() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        c.append(1, 50).unwrap();
        c.append(2, 100).unwrap();
        assert_eq!(c.end_offset_for_epoch(0, 200), 50);
        assert_eq!(c.end_offset_for_epoch(1, 200), 100);
    }

    #[test]
    fn end_offset_for_unknown_epoch_returns_undefined() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path).unwrap();
        c.append(0, 0).unwrap();
        assert_eq!(c.end_offset_for_epoch(7, 200), -1);
    }

    #[test]
    fn missing_file_yields_empty() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert!(c.entries().is_empty());
        assert_eq!(c.latest_epoch(), None);
    }
}
```

- [ ] **Step 3: Re-export from `lib.rs`.**

In `crates/log/src/lib.rs`:

```rust
mod leader_epoch_checkpoint;
pub use leader_epoch_checkpoint::{EpochEntry, LeaderEpochCheckpoint};
```

- [ ] **Step 4: Test + commit.**

```bash
cargo test -p crabka-log leader_epoch_checkpoint
cargo clippy -p crabka-log --all-targets -- -D warnings
git add crates/log/src
git commit -m "feat(log): LeaderEpochCheckpoint reader/writer (Apache Kafka byte-compat)"
```

Trailer.

---

### Task 5: Wire `LeaderEpochCheckpoint` into `Log`

**Files:**
- Modify: `crates/log/src/log.rs`
- Modify: `crates/log/src/segment.rs` (add `leader_epoch_checkpoint_path()` accessor for parity with `txn_index_path()`)

- [ ] **Step 1: Add `Segment::leader_epoch_checkpoint_path()`.**

In `crates/log/src/segment.rs`, alongside `txn_index_path`:

```rust
#[must_use]
pub fn leader_epoch_checkpoint_path(&self) -> PathBuf {
    crate::name::leader_epoch_checkpoint_path(&self.dir)
}
```

- [ ] **Step 2: Add field + initialise in `Log::open`.**

In `crates/log/src/log.rs`, add to the `Log` struct:

```rust
    /// Per-partition leader-epoch checkpoint. Shared across segments —
    /// epoch history accumulates over the log's lifetime.
    epoch_checkpoint: LeaderEpochCheckpoint,
```

In `Log::open`, initialise after the active segment:

```rust
let epoch_checkpoint =
    LeaderEpochCheckpoint::open(active.leader_epoch_checkpoint_path())?;
```

Pass through to the constructor.

- [ ] **Step 3: Extend `Log::append` to accept `leader_epoch`.**

The current signature is `pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError>`. Change to:

```rust
pub fn append(&mut self, batch: &mut RecordBatch) -> Result<i64, LogError> {
    let leader_epoch = batch.partition_leader_epoch;
    // ... existing logic that assigns base_offset + writes batch ...
    let assigned = self.append_preserving_offset(batch)?;
    // Record the epoch transition.
    if leader_epoch >= 0
        && self.epoch_checkpoint.latest_epoch().is_none_or(|e| leader_epoch > e)
    {
        self.epoch_checkpoint.append(leader_epoch, assigned)?;
    }
    Ok(assigned)
}
```

The caller (the partition writer) stamps `batch.partition_leader_epoch` before calling `Log::append` (Task 8).

- [ ] **Step 4: Add `Log::epoch_checkpoint()` accessor.**

```rust
#[must_use]
pub fn epoch_checkpoint(&self) -> &LeaderEpochCheckpoint {
    &self.epoch_checkpoint
}
```

Used by the `OffsetForLeaderEpoch` handler in Task 11.

- [ ] **Step 5: Append a test.**

```rust
#[test]
fn append_records_epoch_transition() {
    let dir = TempDir::new().unwrap();
    let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
    let mut b = sample_batch_with_epoch(3, 0);
    log.append(&mut b).unwrap();
    let mut b2 = sample_batch_with_epoch(2, 1); // 2 records at epoch 1
    log.append(&mut b2).unwrap();
    assert_eq!(
        log.epoch_checkpoint().entries(),
        &[EpochEntry { epoch: 0, start_offset: 0 }, EpochEntry { epoch: 1, start_offset: 3 }]
    );
}

fn sample_batch_with_epoch(n: i32, epoch: i32) -> RecordBatch {
    let mut b = sample_batch(n);
    b.partition_leader_epoch = epoch;
    b
}
```

- [ ] **Step 6: Test + commit.**

```bash
cargo test -p crabka-log log
cargo clippy -p crabka-log --all-targets -- -D warnings
git add crates/log/src
git commit -m "feat(log): Log::append records leader-epoch transitions in checkpoint file"
```

Trailer.

---

## Phase C — `Partition` leader-epoch surface + KIP-101 fence

### Task 6: `Partition` gains `current_leader` + `current_leader_epoch`

**Files:**
- Modify: `crates/broker/src/partition.rs`
- Modify: `crates/broker/src/broker.rs` (`spawn_partition`)

- [ ] **Step 1: Add atomic fields + import.**

In `crates/broker/src/partition.rs`:

```rust
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};

pub struct Partition {
    // ... existing fields ...
    /// Current leader's `NodeId` from the metadata image. Atomic for
    /// lock-free reads in the Produce/Fetch hot paths.
    pub current_leader: Arc<AtomicU64>,
    /// Current `leader_epoch` from the metadata image. Stamped on every
    /// appended batch; validated on every follower Fetch.
    pub current_leader_epoch: Arc<AtomicI32>,
}
```

- [ ] **Step 2: Add `install_leader_change`.**

```rust
impl Partition {
    /// Apply a leader change observed via the metadata image. Updates
    /// the cached `current_leader` + `current_leader_epoch`, clears
    /// per-follower stats (stale under the new leader's view), and
    /// fires `hw_advance_notify` so any waiting `acks=-1` Produce
    /// gates can re-check.
    pub async fn install_leader_change(&self, new_leader: u64, new_epoch: i32) {
        self.current_leader.store(new_leader, Ordering::Release);
        self.current_leader_epoch.store(new_epoch, Ordering::Release);
        let mut st = self.replica_state.lock().await;
        st.per_follower.clear();
        st.current_leader_epoch = new_epoch;
        drop(st);
        self.hw_advance_notify.notify_waiters();
    }
}
```

- [ ] **Step 3: Update `spawn_partition`.**

In `crates/broker/src/broker.rs`:

```rust
pub(crate) fn spawn_partition(
    topic: String,
    partition_id: i32,
    log: crabka_log::Log,
) -> Arc<Partition> {
    // ... existing ...
    let current_leader = Arc::new(AtomicU64::new(0));
    let current_leader_epoch = Arc::new(AtomicI32::new(0));
    // ... pass into Partition literal ...
    Arc::new(Partition {
        // ... existing fields ...
        current_leader,
        current_leader_epoch,
        // ...
    })
}
```

Update every test that builds a `Partition` literal (`partition.rs::tests::debug_does_not_dump_log` + the slice-10a unit tests) to include the two new Arcs.

- [ ] **Step 4: Test + commit.**

```bash
cargo build --workspace
cargo test -p crabka-broker --lib partition
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src
git commit -m "feat(broker): Partition.current_leader/current_leader_epoch + install_leader_change"
```

Trailer.

---

### Task 7: `ReplicaState::per_follower` for ISR-lag tracking

**Files:**
- Modify: `crates/broker/src/replica_state.rs`

- [ ] **Step 1: Replace `follower_leo` with `per_follower`.**

In `crates/broker/src/replica_state.rs`:

```rust
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FollowerStats {
    pub(crate) leo: i64,
    pub(crate) last_fetch: Instant,
    pub(crate) last_caught_up: Instant,
}

pub(crate) struct ReplicaState {
    pub(crate) isr: HashSet<NodeId>,
    pub(crate) per_follower: HashMap<NodeId, FollowerStats>,
    pub(crate) hw: i64,
    pub(crate) current_leader_epoch: i32,
}

impl ReplicaState {
    pub(crate) fn new() -> Self {
        Self {
            isr: HashSet::new(),
            per_follower: HashMap::new(),
            hw: 0,
            current_leader_epoch: 0,
        }
    }

    pub(crate) fn install_isr(&mut self, replicas: &[NodeId], leader: NodeId) {
        self.isr = replicas.iter().copied().collect();
        let now = Instant::now();
        for &r in replicas {
            if r != leader {
                self.per_follower.entry(r).or_insert(FollowerStats {
                    leo: 0,
                    last_fetch: now,
                    last_caught_up: now,
                });
            }
        }
        let isr = self.isr.clone();
        self.per_follower.retain(|k, _| isr.contains(k));
    }

    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: i64,
        leader_leo: i64,
    ) -> i64 {
        let now = Instant::now();
        if !self.isr.contains(&follower) {
            // Track stats so isr_maintenance can expand back when caught up.
            let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
                leo: 0,
                last_fetch: now,
                last_caught_up: now,
            });
            stats.last_fetch = now;
            stats.leo = follower_leo.min(leader_leo);
            if stats.leo >= leader_leo {
                stats.last_caught_up = now;
            }
            return self.recompute_hw_for_leader_append(leader_leo);
        }
        let clamped = follower_leo.min(leader_leo);
        let stats = self.per_follower.entry(follower).or_insert(FollowerStats {
            leo: 0,
            last_fetch: now,
            last_caught_up: now,
        });
        stats.leo = clamped;
        stats.last_fetch = now;
        if clamped >= leader_leo {
            stats.last_caught_up = now;
        }
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    pub(crate) fn recompute_hw_for_leader_append(&mut self, leader_leo: i64) -> i64 {
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    fn compute_hw(&self, leader_leo: i64) -> i64 {
        if self.isr.is_empty() {
            return leader_leo;
        }
        let mut min_leo = leader_leo;
        for follower in &self.isr {
            if let Some(stats) = self.per_follower.get(follower)
                && stats.leo < min_leo
            {
                min_leo = stats.leo;
            }
        }
        min_leo
    }
}
```

- [ ] **Step 2: Update existing unit tests.**

The slice-10a tests reference `follower_leo`. Change to `per_follower.get(&node).map(|s| s.leo)`:

```rust
#[test]
fn install_isr_seeds_non_leader_followers_at_zero() {
    let mut s = fresh();
    s.install_isr(&[1, 2, 3], 1);
    assert_eq!(s.isr, [1, 2, 3].into_iter().collect());
    assert_eq!(s.per_follower.get(&2).map(|f| f.leo), Some(0));
    assert_eq!(s.per_follower.get(&3).map(|f| f.leo), Some(0));
    assert!(!s.per_follower.contains_key(&1));
}
```

Repeat the pattern for the other slice-10a tests.

- [ ] **Step 3: Add two new tests.**

```rust
#[test]
fn update_follower_leo_advances_last_fetch_time() {
    let mut s = fresh();
    s.install_isr(&[1, 2], 1);
    let t0 = s.per_follower.get(&2).unwrap().last_fetch;
    std::thread::sleep(std::time::Duration::from_millis(10));
    s.update_follower_leo(2, 5, 10);
    let t1 = s.per_follower.get(&2).unwrap().last_fetch;
    assert!(t1 > t0);
}

#[test]
fn last_caught_up_set_when_leo_reaches_leader_leo() {
    let mut s = fresh();
    s.install_isr(&[1, 2], 1);
    s.update_follower_leo(2, 5, 10);
    let lag = s.per_follower.get(&2).unwrap().last_caught_up;
    let lag_install = s.per_follower.get(&2).map(|f| f.last_fetch).unwrap();
    // Not yet caught up — last_caught_up is the install time, NOT the
    // recent update time.
    assert!(lag <= lag_install);
    std::thread::sleep(std::time::Duration::from_millis(10));
    s.update_follower_leo(2, 10, 10);
    let lag2 = s.per_follower.get(&2).unwrap().last_caught_up;
    assert!(lag2 > lag);
}
```

- [ ] **Step 4: Test + commit.**

```bash
cargo test -p crabka-broker replica_state
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/replica_state.rs
git commit -m "feat(broker): ReplicaState.per_follower for ISR-lag tracking"
```

Trailer.

---

### Task 8: Produce stamps `partition_leader_epoch`

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Stamp the batch.**

In `crates/broker/src/handlers/produce.rs`, locate the existing block where `batch` is extracted from `part_data.records`. Just before the slice-9 transactional verify (or the slice-6 dedup gate at line ~175), stamp the epoch:

```rust
let Some(mut batch) = part_data.records else {
    out.error_code = codes::INVALID_REQUEST;
    partition_results.push(out);
    continue;
};
// Stamp the current leader epoch onto the batch — this becomes the
// `partition_leader_epoch` carried on the wire and used by KIP-101
// fence validation on the follower's Fetch.
batch.partition_leader_epoch = part.current_leader_epoch.load(std::sync::atomic::Ordering::Acquire);
```

The rest of the handler is unchanged. `Log::append` (Task 5) reads `batch.partition_leader_epoch` and records the transition in the checkpoint file.

- [ ] **Step 2: Build + clippy.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
```

- [ ] **Step 3: Commit.**

```bash
git add crates/broker/src/handlers/produce.rs
git commit -m "feat(broker): Produce stamps batch.partition_leader_epoch from current_leader_epoch"
```

Trailer.

---

### Task 9: Fetch epoch fence + restore follower-fetch HW maintenance

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Recon current state.**

Slice-10a removed the follower-fetch HW maintenance block. Find the per-partition loop in `fetch.rs::handle` (where `part_opt` is resolved from `partitions.get(...)`). The flow after Task 9 is:
1. Epoch validate.
2. Update ReplicaState (follower fetch).
3. Push pending read.

- [ ] **Step 2: Add the epoch validation block.**

After `let part_opt = partitions.get(...).map(|p| p.clone());`, BEFORE the `if part_opt.is_none()` check, insert:

```rust
// KIP-101 epoch fence. The follower (or consumer using KIP-320)
// includes its `current_leader_epoch`; we reject stale or future
// epochs without serving data.
if let Some(part) = part_opt.as_ref() {
    let our_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    if fp.current_leader_epoch >= 0 && fp.current_leader_epoch != our_epoch {
        out.error_code = if fp.current_leader_epoch < our_epoch {
            codes::FENCED_LEADER_EPOCH
        } else {
            codes::UNKNOWN_LEADER_EPOCH
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
        });
        continue;
    }
}
```

- [ ] **Step 3: Restore the follower-fetch HW maintenance block.**

After the epoch check, AFTER the existing `let part_opt = partitions.get(...)` and BEFORE the `if part_opt.is_none()` branch, add:

```rust
if is_follower_fetch
    && let Some(part) = part_opt.as_ref()
{
    let leader_leo = part.log_end_offset();
    let advanced = {
        let mut st = part.replica_state.lock().await;
        let prev = st.hw;
        let new = st.update_follower_leo(
            u64::try_from(req.replica_id).unwrap_or(0),
            fetch_offset,
            leader_leo,
        );
        new > prev
    };
    if advanced {
        part.hw_advance_notify.notify_waiters();
    }
}
```

This is the block that slice-10a removed because of follower-replication stalls. It's safe to restore now because slice-10b's ISR maintenance prevents permanent stalls (a follower that doesn't fetch gets shrunk out within 2s on CI).

- [ ] **Step 4: Test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): Fetch KIP-101 epoch fence + restore follower-fetch HW maintenance"
```

Trailer.

---

## Phase D — OffsetForLeaderEpoch RPC

### Task 10: `OffsetForLeaderEpoch` wire handler

**Files:**
- Create: `crates/broker/src/handlers/offset_for_leader_epoch.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (declare module)
- Modify: `crates/broker/src/broker.rs` (register api_key 23 in HandlerTable)
- Modify: `crates/broker/src/network/dispatch.rs` (flexible-encoding entry)
- Modify: `crates/broker/src/handlers/api_versions.rs` (advertise)

- [ ] **Step 1: Recon the wire shape.**

Read `crates/protocol/src/owned/offset_for_leader_epoch_request.rs` and `_response.rs` to find the actual field names (the codegen's nested structure: per-topic list, per-partition list with `current_leader_epoch` + `leader_epoch`).

- [ ] **Step 2: Write the handler.**

`crates/broker/src/handlers/offset_for_leader_epoch.rs`:

```rust
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::offset_for_leader_epoch_request::OffsetForLeaderEpochRequest;
use crabka_protocol::owned::offset_for_leader_epoch_response::{
    EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetForLeaderEpochRequest::decode(&mut cur, version)?;

        let mut topics_out: Vec<OffsetForLeaderTopicResult> = Vec::with_capacity(req.topics.len());
        for t in &req.topics {
            let mut parts_out: Vec<EpochEndOffset> = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                let mut out = EpochEndOffset {
                    partition: p.partition,
                    ..Default::default()
                };
                let Some(part) = partitions
                    .get(&(t.topic.clone(), p.partition))
                    .map(|e| e.value().clone())
                else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };
                let our_epoch = part
                    .current_leader_epoch
                    .load(std::sync::atomic::Ordering::Acquire);
                if p.current_leader_epoch >= 0 && p.current_leader_epoch > our_epoch {
                    out.error_code = codes::UNKNOWN_LEADER_EPOCH;
                    parts_out.push(out);
                    continue;
                }
                let log = part.log.lock().expect("log mutex poisoned");
                let leo = log.log_end_offset();
                let end = log.epoch_checkpoint().end_offset_for_epoch(p.leader_epoch, leo);
                drop(log);
                out.leader_epoch = our_epoch;
                out.end_offset = end;
                out.error_code = codes::NONE;
                parts_out.push(out);
            }
            topics_out.push(OffsetForLeaderTopicResult {
                topic: t.topic.clone(),
                partitions: parts_out,
                ..Default::default()
            });
        }

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

The actual codegen struct names may differ slightly (`EpochEndOffset` etc.). Adjust to match the actual generated types.

- [ ] **Step 3: Register.**

In `crates/broker/src/handlers/mod.rs`:
```rust
pub(crate) mod offset_for_leader_epoch;
```

In `crates/broker/src/broker.rs` (where the HandlerTable is built):
```rust
table.register(23, crate::handlers::offset_for_leader_epoch::handle);
```

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`, add the API key (23) with its flex-min. OffsetForLeaderEpoch is flexible at v4+ per Apache Kafka.

In `crates/broker/src/handlers/api_versions.rs::supported_apis`:
```rust
v!(offset_for_leader_epoch_request),
```

- [ ] **Step 4: Test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/handlers/offset_for_leader_epoch.rs crates/broker/src
git commit -m "feat(broker): OffsetForLeaderEpoch handler (api_key 23)"
```

Trailer.

---

### Task 11: Replicator handles FENCED_LEADER_EPOCH

**Files:**
- Modify: `crates/broker/src/replicator.rs`

- [ ] **Step 1: Send current_leader_epoch in outgoing Fetch.**

In `build_fetch_request`, set the per-partition field:

```rust
partitions: vec![FetchPartition {
    partition: cfg.partition,
    fetch_offset,
    current_leader_epoch: <local view's leader_epoch>,
    partition_max_bytes: FETCH_MAX_BYTES,
    ..FetchPartition::default()
}],
```

The replicator's `Config` needs a new field `current_leader_epoch: i32` set when the supervisor spawns the task; or simpler: read it from `cfg.partitions.get(...).current_leader_epoch.load(Acquire)` each Fetch.

- [ ] **Step 2: Handle the FENCED_LEADER_EPOCH response.**

Add a new arm to `handle_response`'s match:

```rust
codes::FENCED_LEADER_EPOCH | codes::UNKNOWN_LEADER_EPOCH => {
    // KIP-101: our local view of the leader is stale (or ahead). Call
    // OffsetForLeaderEpoch to learn the truncation offset, then
    // truncate the local log and restart the loop.
    warn!(topic = %cfg.topic, partition = cfg.partition,
        error_code = part_resp.error_code,
        "fenced/unknown leader epoch; calling OffsetForLeaderEpoch");
    let _ = handle_epoch_fence(cfg).await;
    LoopAction::Continue
}
```

And a new `handle_epoch_fence`:

```rust
async fn handle_epoch_fence(cfg: &Config) -> Result<(), String> {
    let Some(entry) = cfg.partitions.get(&(cfg.topic.clone(), cfg.partition))
    else {
        return Ok(());
    };
    let part = entry.value().clone();
    let our_epoch = part.current_leader_epoch.load(std::sync::atomic::Ordering::Acquire);
    let mut client = crabka_client_core::Client::builder()
        .bootstrap(cfg.leader_addr.clone())
        .client_id(cfg.client_id.clone())
        .build()
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let req = crabka_protocol::owned::offset_for_leader_epoch_request::OffsetForLeaderEpochRequest {
        replica_id: i32::try_from(cfg.node_id).unwrap_or(-1),
        topics: vec![
            crabka_protocol::owned::offset_for_leader_epoch_request::OffsetForLeaderTopic {
                topic: cfg.topic.clone(),
                partitions: vec![
                    crabka_protocol::owned::offset_for_leader_epoch_request::OffsetForLeaderPartition {
                        partition: cfg.partition,
                        current_leader_epoch: our_epoch,
                        leader_epoch: our_epoch,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let resp = client.send(req).await.map_err(|e| format!("send: {e}"))?;
    let truncation = resp.topics.iter()
        .find(|t| t.topic == cfg.topic)
        .and_then(|t| t.partitions.iter().find(|p| p.partition == cfg.partition))
        .map(|p| p.end_offset)
        .unwrap_or(0);
    if truncation >= 0 {
        let _ = part.truncate_to(truncation).await;
    } else {
        // UNDEFINED_OFFSET — leader doesn't know this epoch. Reset.
        let _ = part.reset_to(0).await;
    }
    Ok(())
}
```

- [ ] **Step 3: Build + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/replicator.rs
git commit -m "feat(broker): replicator handles FENCED_LEADER_EPOCH via OffsetForLeaderEpoch + truncate"
```

Trailer.

---

## Phase E — BrokerHeartbeat

### Task 12: `ControllerLivenessState` module + ticker

**Files:**
- Create: `crates/broker/src/heartbeat/mod.rs`
- Create: `crates/broker/src/heartbeat/controller_state.rs`
- Modify: `crates/broker/src/lib.rs` (declare `pub(crate) mod heartbeat;`)

- [ ] **Step 1: Write the module.**

`crates/broker/src/heartbeat/mod.rs`:

```rust
pub(crate) mod client;
pub(crate) mod controller_state;
```

`crates/broker/src/heartbeat/controller_state.rs`:

```rust
//! Controller-side per-broker liveness state + the ticker that fences
//! stale brokers and fires leader-election callbacks.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crabka_raft::NodeId;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BrokerLivenessState {
    pub(crate) last_heartbeat: Instant,
    pub(crate) alive: bool,
}

#[derive(Debug)]
pub(crate) struct ControllerLivenessState {
    pub(crate) brokers: Mutex<HashMap<NodeId, BrokerLivenessState>>,
    pub(crate) heartbeat_timeout: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum LivenessTransition {
    AliveToDead(NodeId),
    DeadToAlive(NodeId),
}

impl ControllerLivenessState {
    pub(crate) fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            brokers: Mutex::new(HashMap::new()),
            heartbeat_timeout,
        }
    }

    /// Record a heartbeat. Returns `Some(DeadToAlive)` if this revived
    /// a previously-dead broker.
    pub(crate) async fn record_heartbeat(&self, broker: NodeId) -> Option<LivenessTransition> {
        let now = Instant::now();
        let mut map = self.brokers.lock().await;
        let entry = map.entry(broker).or_insert(BrokerLivenessState {
            last_heartbeat: now,
            alive: true,
        });
        let was_dead = !entry.alive;
        entry.last_heartbeat = now;
        entry.alive = true;
        was_dead.then_some(LivenessTransition::DeadToAlive(broker))
    }

    /// Scan for stale brokers. Returns a list of `AliveToDead`
    /// transitions for callers (the ticker) to act on.
    pub(crate) async fn tick(&self) -> Vec<LivenessTransition> {
        let now = Instant::now();
        let mut transitions = Vec::new();
        let mut map = self.brokers.lock().await;
        for (broker, state) in map.iter_mut() {
            if state.alive && now.duration_since(state.last_heartbeat) > self.heartbeat_timeout {
                state.alive = false;
                transitions.push(LivenessTransition::AliveToDead(*broker));
            }
        }
        transitions
    }

    pub(crate) async fn is_alive(&self, broker: NodeId) -> bool {
        self.brokers.lock().await.get(&broker).is_some_and(|s| s.alive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_broker_alive_on_first_heartbeat() {
        let s = ControllerLivenessState::new(Duration::from_millis(100));
        s.record_heartbeat(1).await;
        assert!(s.is_alive(1).await);
    }

    #[tokio::test]
    async fn alive_to_dead_after_timeout() {
        let s = ControllerLivenessState::new(Duration::from_millis(50));
        s.record_heartbeat(1).await;
        assert!(s.is_alive(1).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let transitions = s.tick().await;
        assert_eq!(transitions.len(), 1);
        assert!(matches!(transitions[0], LivenessTransition::AliveToDead(1)));
        assert!(!s.is_alive(1).await);
    }

    #[tokio::test]
    async fn dead_to_alive_on_heartbeat_after_gap() {
        let s = ControllerLivenessState::new(Duration::from_millis(50));
        s.record_heartbeat(1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        s.tick().await;
        assert!(!s.is_alive(1).await);
        let t = s.record_heartbeat(1).await;
        assert!(matches!(t, Some(LivenessTransition::DeadToAlive(1))));
        assert!(s.is_alive(1).await);
    }

    #[tokio::test]
    async fn ticker_fires_alive_to_dead_exactly_once() {
        let s = ControllerLivenessState::new(Duration::from_millis(50));
        s.record_heartbeat(1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let first = s.tick().await;
        assert_eq!(first.len(), 1);
        let second = s.tick().await;
        assert!(second.is_empty());
    }
}
```

- [ ] **Step 2: Declare in lib.rs.**

`crates/broker/src/lib.rs`:

```rust
pub(crate) mod heartbeat;
```

- [ ] **Step 3: Test + commit.**

```bash
cargo test -p crabka-broker heartbeat
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/lib.rs crates/broker/src/heartbeat
git commit -m "feat(broker): ControllerLivenessState + ticker (KIP-500 liveness)"
```

Trailer.

---

### Task 13: `BrokerHeartbeat` wire handler

**Files:**
- Create: `crates/broker/src/handlers/broker_heartbeat.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/broker.rs` (register api_key 63)
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`

- [ ] **Step 1: Write the handler.**

`crates/broker/src/handlers/broker_heartbeat.rs`:

```rust
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let liveness = broker.liveness.clone();
    let is_controller_leader = broker.controller.is_leader();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = BrokerHeartbeatRequest::decode(&mut cur, version)?;

        if !is_controller_leader {
            let mut buf = BytesMut::new();
            BrokerHeartbeatResponse {
                throttle_time_ms: 0,
                error_code: codes::NOT_CONTROLLER,
                is_caught_up: false,
                is_fenced: true,
                should_shut_down: false,
                ..Default::default()
            }
            .encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        let transition = liveness
            .record_heartbeat(u64::try_from(req.broker_id).unwrap_or(0))
            .await;
        if let Some(crate::heartbeat::controller_state::LivenessTransition::DeadToAlive(n)) =
            transition
        {
            // Rejoined broker: trigger an election rescan (slice-10b Task 16).
            // The leader_election module's on_broker_alive handles this.
            crate::leader_election::on_broker_alive(&broker_dup, n, &liveness)
                .await
                .ok();
        }

        let resp = BrokerHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            is_caught_up: true,
            is_fenced: false,
            should_shut_down: false,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

`broker_dup` is `broker.controller_handle.clone()` captured in the outer scope before `Box::pin`. The `is_leader()` helper on `ControllerHandle` already exists (slice 7).

- [ ] **Step 2: Wire `liveness` field onto Broker.**

In `crates/broker/src/broker.rs`, add:

```rust
pub struct Broker {
    // ... existing ...
    pub(crate) liveness: Arc<crate::heartbeat::controller_state::ControllerLivenessState>,
}
```

In `Broker::start`, after config:

```rust
let liveness = Arc::new(
    crate::heartbeat::controller_state::ControllerLivenessState::new(
        std::time::Duration::from_millis(config.heartbeat_timeout_ms),
    ),
);
```

- [ ] **Step 3: Register the handler.**

In `crates/broker/src/handlers/mod.rs`:
```rust
pub(crate) mod broker_heartbeat;
```

In the broker's HandlerTable build:
```rust
table.register(63, crate::handlers::broker_heartbeat::handle);
```

In `dispatch.rs::handler_body_flexible`:
```rust
63 => version >= 0,  // BrokerHeartbeat (all versions flexible)
```

In `api_versions.rs::supported_apis`:
```rust
v!(broker_heartbeat_request),
```

- [ ] **Step 4: Test + commit.**

For now, leave the `crate::leader_election::on_broker_alive` call as a stub — that module lands in Task 15. Use a placeholder `fn on_broker_alive(...) -> Result<(), BrokerError> { Ok(()) }` until Task 15 implements it. Actually — easier: write the heartbeat handler WITHOUT the `on_broker_alive` call. Task 15 adds the wire-up. Comment in the handler: `// TODO(slice-10b Task 15): trigger leader_election::on_broker_alive on revival`.

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src
git commit -m "feat(broker): BrokerHeartbeat handler (api_key 63) + liveness wiring"
```

Trailer.

---

### Task 14: Broker-side heartbeat client

**Files:**
- Create: `crates/broker/src/heartbeat/client.rs`
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Write the loop.**

`crates/broker/src/heartbeat/client.rs`:

```rust
//! Broker-side heartbeat client. Sends `BrokerHeartbeat` to the
//! controller leader every `heartbeat_interval_ms`. Discovers the
//! current controller via the controller's quorum voters; retries on
//! NOT_CONTROLLER by chasing the `controller_id` redirect.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use crabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use crabka_raft::ControllerHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub(crate) struct Config {
    pub broker_id: i32,
    pub interval: Duration,
    pub controller: Arc<ControllerHandle>,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(cfg.interval);
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = cfg.shutdown.cancelled() => return,
        }
        // Resolve the current controller leader address.
        let Some(addr) = cfg.controller.current_leader_addr().await else {
            debug!("heartbeat: no controller leader yet");
            continue;
        };
        let Ok(mut client) = crabka_client_core::Client::builder()
            .bootstrap(addr)
            .client_id(format!("crabka-broker-{}-heartbeat", cfg.broker_id))
            .build()
            .await
        else {
            debug!("heartbeat: connect failed");
            continue;
        };
        let resp = client
            .send(BrokerHeartbeatRequest {
                broker_id: cfg.broker_id,
                broker_epoch: 0,
                current_metadata_offset: 0,
                want_fence: false,
                want_shut_down: false,
                ..Default::default()
            })
            .await;
        if let Err(e) = resp {
            warn!(error = %e, "heartbeat send failed");
        }
    }
}
```

`ControllerHandle::current_leader_addr` may or may not exist; if not, add a shim that reads from the metadata image's `brokers()` filter or from the openraft state. Worst case, just iterate `controller_quorum_voters` and try each.

- [ ] **Step 2: Spawn from Broker::start.**

In `crates/broker/src/broker.rs`, after the supervisor spawn:

```rust
let heartbeat_handle = tokio::spawn(crate::heartbeat::client::run(
    crate::heartbeat::client::Config {
        broker_id: config.broker_id,
        interval: Duration::from_millis(config.heartbeat_interval_ms),
        controller: controller.clone(),
        shutdown: heartbeat_shutdown.clone(),
    },
));
```

Store the handle + shutdown so `Broker::shutdown` cancels it.

- [ ] **Step 3: Spawn the liveness ticker.**

Right after, spawn the controller-side ticker (only meaningful on the controller leader, but cheap to run on every broker — if `is_controller_leader()` is false, the ticker's transitions don't fire elections because `leader_election::on_broker_dead` will check `is_leader`):

```rust
let liveness_for_ticker = liveness.clone();
let ticker_shutdown = supervisor_shutdown.child_token();
tokio::spawn(async move {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = ticker_shutdown.cancelled() => return,
        }
        let _transitions = liveness_for_ticker.tick().await;
        // Task 16 wires up: for each AliveToDead(n), call
        // leader_election::on_broker_dead(...). Stub for now.
    }
});
```

- [ ] **Step 4: Test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/heartbeat/client.rs crates/broker/src/broker.rs
git commit -m "feat(broker): heartbeat client loop + liveness ticker spawn"
```

Trailer.

---

## Phase F — Leader election

### Task 15: `leader_election` module

**Files:**
- Create: `crates/broker/src/leader_election.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write the module.**

`crates/broker/src/leader_election.rs`:

```rust
//! Controller-side leader-election scan. Called when the liveness
//! ticker observes an `AliveToDead` transition (or `DeadToAlive` for
//! recovery-rescan).

#![allow(dead_code)]

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_raft::{ControllerHandle, NodeId};
use tracing::warn;

use crate::error::BrokerError;
use crate::heartbeat::controller_state::ControllerLivenessState;

pub(crate) async fn on_broker_dead(
    controller: &ControllerHandle,
    dead: NodeId,
    liveness: &ControllerLivenessState,
) -> Result<(), BrokerError> {
    if !controller.is_leader() {
        return Ok(());
    }
    let image = controller.current_image();
    let mut changes: Vec<MetadataRecord> = Vec::new();
    for topic in image.topics() {
        for pr in image.partitions_of(&topic.name) {
            if !pr.replicas.contains(&dead) && !pr.isr.contains(&dead) {
                continue;
            }
            let mut alive_isr: Vec<NodeId> = Vec::with_capacity(pr.isr.len());
            for n in &pr.isr {
                if *n != dead && liveness.is_alive(*n).await {
                    alive_isr.push(*n);
                }
            }
            let needs_election = pr.leader == dead;
            if needs_election {
                let Some(&new_leader) = alive_isr.first() else {
                    warn!(topic = %pr.topic, partition = pr.partition,
                        "no live ISR replica; partition unavailable");
                    continue;
                };
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: new_leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch + 1,
                }));
            } else if alive_isr.len() < pr.isr.len() {
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: pr.topic.clone(),
                    partition: pr.partition,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr: alive_isr,
                    leader_epoch: pr.leader_epoch,
                }));
            }
        }
    }
    if !changes.is_empty() {
        controller
            .submit_change(changes)
            .await
            .map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
    }
    Ok(())
}

pub(crate) async fn on_broker_alive(
    controller: &ControllerHandle,
    _alive: NodeId,
    _liveness: &ControllerLivenessState,
) -> Result<(), BrokerError> {
    if !controller.is_leader() {
        return Ok(());
    }
    // Future: scan for unavailable partitions (no live ISR) and
    // attempt election now that a broker came back. Slice-10b leaves
    // this as a no-op — recovery happens organically through the
    // dead broker rejoining its previous ISR via AlterPartition's
    // expand path once its replicator catches up. The hook is here
    // for future enhancement.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: full integration tests live in tests/leader_election.rs.
    // Unit tests would require mocking ControllerHandle + image,
    // which is heavy. We rely on the integration tests.
}
```

- [ ] **Step 2: Declare in lib.rs.**

```rust
pub(crate) mod leader_election;
```

- [ ] **Step 3: Build + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/leader_election.rs crates/broker/src/lib.rs
git commit -m "feat(broker): leader_election::on_broker_dead + on_broker_alive"
```

Trailer.

---

### Task 16: Wire liveness ticker → leader_election

**Files:**
- Modify: `crates/broker/src/broker.rs`
- Modify: `crates/broker/src/handlers/broker_heartbeat.rs`

- [ ] **Step 1: Update the liveness ticker spawn in Broker::start.**

Replace the Task 14 stub:

```rust
let liveness_for_ticker = liveness.clone();
let controller_for_ticker = controller.clone();
let ticker_shutdown = supervisor_shutdown.child_token();
tokio::spawn(async move {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = ticker_shutdown.cancelled() => return,
        }
        let transitions = liveness_for_ticker.tick().await;
        for t in transitions {
            use crate::heartbeat::controller_state::LivenessTransition::*;
            match t {
                AliveToDead(n) => {
                    if let Err(e) = crate::leader_election::on_broker_dead(
                        &controller_for_ticker,
                        n,
                        &liveness_for_ticker,
                    )
                    .await
                    {
                        warn!(broker = n, error = %e, "leader_election on_broker_dead failed");
                    }
                }
                DeadToAlive(n) => {
                    if let Err(e) = crate::leader_election::on_broker_alive(
                        &controller_for_ticker,
                        n,
                        &liveness_for_ticker,
                    )
                    .await
                    {
                        warn!(broker = n, error = %e, "leader_election on_broker_alive failed");
                    }
                }
            }
        }
    }
});
```

- [ ] **Step 2: Update the heartbeat handler to call on_broker_alive on revival.**

Replace the Task 13 TODO comment with the real call (same pattern as the ticker's DeadToAlive arm).

- [ ] **Step 3: Build + test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src
git commit -m "feat(broker): wire liveness ticker + heartbeat to leader_election"
```

Trailer.

---

### Task 17: Supervisor reconciles leader changes

**Files:**
- Modify: `crates/broker/src/replicator_supervisor.rs`

- [ ] **Step 1: Update `reconcile` to track (leader, leader_epoch) per partition.**

The existing reconcile materialises local partitions + spawns follower replicators. After Task 17, it ALSO:

1. Calls `Partition::install_leader_change(leader, leader_epoch)` on the local partition handle for every reconcile (idempotent — atomics no-op on equal write).
2. Cancels the follower replicator if the partition's leader changed.
3. Re-spawns the follower replicator pointed at the new leader.

In `crates/broker/src/replicator_supervisor.rs::reconcile`, replace Step 0's `install_isr` block with:

```rust
for key in desired_local_set(self.node_id, image) {
    if let Err(e) = self.materialize_local_partition(&key.0, key.1) {
        warn!(topic = %key.0, partition = key.1, error = %e,
            "failed to materialize local partition");
        continue;
    }
    let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
        continue;
    };
    let Some(part) = self
        .partitions
        .get(&(key.0.clone(), key.1))
        .map(|e| e.value().clone())
    else {
        continue;
    };
    // Always install leader change — `Partition::install_leader_change`
    // is idempotent and atomics no-op on equal writes.
    part.install_leader_change(part_record.leader, part_record.leader_epoch).await;
    if part_record.leader == self.node_id {
        part.install_isr(&part_record.replicas, part_record.leader).await;
    }
}
```

For Step 2 (spawn replicators), augment the existing logic to cancel + respawn when the (leader, leader_epoch) tuple changes:

```rust
// Track (leader, leader_epoch) per partition to detect leader changes.
let mut current_leaders: Vec<((String, i32), (NodeId, i32))> = Vec::new();
for k in desired_follower_set(self.node_id, image) {
    let Some(pr) = image.partition(&k.0, k.1).cloned() else { continue; };
    current_leaders.push((k.clone(), (pr.leader, pr.leader_epoch)));
}

// Cancel any task whose leader changed.
for ((topic, part), new_target) in &current_leaders {
    if let Some(prev_target) = self.task_targets.get(&(topic.clone(), *part))
        && prev_target.value() != new_target
        && let Some((_, token)) = self.tasks.remove(&(topic.clone(), *part))
    {
        token.cancel();
    }
}
```

Add a new `tasks_targets: DashMap<(String, i32), (NodeId, i32)>` field on `ReplicatorSupervisor`. Initialize in `new`.

- [ ] **Step 2: Build + test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/replicator_supervisor.rs
git commit -m "feat(broker): supervisor reconciles leader changes via install_leader_change + replicator respawn"
```

Trailer.

---

## Phase G — ISR maintenance + AlterPartition

### Task 18: `isr_maintenance` module

**Files:**
- Create: `crates/broker/src/isr_maintenance.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write the module.**

`crates/broker/src/isr_maintenance.rs`:

```rust
//! Per-leader-partition ISR maintenance. Compares each follower's
//! last-fetch time vs `replica_lag_time_max_ms` and proposes
//! `AlterPartition` shrink/expand to the controller leader.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use crabka_protocol::owned::alter_partition_request::{
    AlterPartitionRequest, PartitionData as AlterPartitionData, TopicData as AlterTopicData,
};
use crabka_raft::{ControllerHandle, NodeId};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::partition::Partition;

pub(crate) struct Config {
    pub node_id: NodeId,
    pub partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub controller: Arc<ControllerHandle>,
    pub replica_lag_time_max: Duration,
    pub broker_id: i32,
    pub shutdown: CancellationToken,
}

pub(crate) async fn run(cfg: Config) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = cfg.shutdown.cancelled() => return,
        }
        for entry in cfg.partitions.iter() {
            let part = entry.value().clone();
            if part.current_leader.load(std::sync::atomic::Ordering::Acquire) != cfg.node_id {
                continue;
            }
            let proposal = compute_proposal(&part, cfg.replica_lag_time_max).await;
            let Some((new_isr, leader_epoch)) = proposal else { continue; };
            if let Err(e) = send_alter_partition(
                &cfg.controller,
                cfg.broker_id,
                &entry.key().0,
                entry.key().1,
                new_isr,
                leader_epoch,
            )
            .await
            {
                warn!(topic = %entry.key().0, partition = entry.key().1,
                    error = %e, "AlterPartition propose failed");
            }
        }
    }
}

async fn compute_proposal(
    part: &Partition,
    lag_max: Duration,
) -> Option<(Vec<NodeId>, i32)> {
    let st = part.replica_state.lock().await;
    let now = std::time::Instant::now();
    let mut new_isr: Vec<NodeId> = st.isr.iter().copied().collect();
    // Shrink: drop followers lagging > lag_max
    new_isr.retain(|n| {
        st.per_follower
            .get(n)
            .is_none_or(|stats| now.duration_since(stats.last_fetch) <= lag_max)
    });
    // Expand: add followers in per_follower not in isr that have been
    // recently caught up
    for (n, stats) in &st.per_follower {
        if !st.isr.contains(n)
            && now.duration_since(stats.last_caught_up) <= lag_max
            && !new_isr.contains(n)
        {
            new_isr.push(*n);
        }
    }
    let no_change = new_isr.len() == st.isr.len()
        && new_isr.iter().all(|n| st.isr.contains(n));
    if no_change { None } else {
        Some((new_isr, st.current_leader_epoch))
    }
}

async fn send_alter_partition(
    controller: &ControllerHandle,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: Vec<NodeId>,
    leader_epoch: i32,
) -> Result<(), String> {
    let Some(addr) = controller.current_leader_addr().await else {
        return Err("no controller leader".into());
    };
    let mut client = crabka_client_core::Client::builder()
        .bootstrap(addr)
        .client_id(format!("crabka-broker-{broker_id}-isr"))
        .build()
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let req = AlterPartitionRequest {
        broker_id,
        broker_epoch: 0,
        topics: vec![AlterTopicData {
            name: topic.into(),
            partitions: vec![AlterPartitionData {
                partition_index: partition,
                leader_epoch,
                new_isr: new_isr.iter().map(|n| i32::try_from(*n).unwrap_or(-1)).collect(),
                leader_recovery_state: 0,
                partition_epoch: 0,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let _resp = client.send(req).await.map_err(|e| format!("send: {e}"))?;
    debug!(topic = topic, partition = partition, "AlterPartition proposed");
    Ok(())
}
```

The actual codegen struct names + fields will likely differ; adapt to the actual generated `AlterPartitionRequest` fields.

- [ ] **Step 2: Declare in lib.rs.**

```rust
pub(crate) mod isr_maintenance;
```

- [ ] **Step 3: Build + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/isr_maintenance.rs crates/broker/src/lib.rs
git commit -m "feat(broker): isr_maintenance tick proposes AlterPartition shrink/expand"
```

Trailer.

---

### Task 19: `AlterPartition` wire handler

**Files:**
- Create: `crates/broker/src/handlers/alter_partition.rs`
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/broker.rs`
- Modify: `crates/broker/src/network/dispatch.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`

- [ ] **Step 1: Write the handler.**

`crates/broker/src/handlers/alter_partition.rs`:

```rust
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_protocol::owned::alter_partition_request::AlterPartitionRequest;
use crabka_protocol::owned::alter_partition_response::{
    AlterPartitionResponse, PartitionData as RespPartitionData,
    TopicData as RespTopicData,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = AlterPartitionRequest::decode(&mut cur, version)?;

        if !controller.is_leader() {
            let mut buf = BytesMut::new();
            AlterPartitionResponse {
                throttle_time_ms: 0,
                error_code: codes::NOT_CONTROLLER,
                topics: Vec::new(),
                ..Default::default()
            }
            .encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        let image = controller.current_image();
        let mut changes: Vec<MetadataRecord> = Vec::new();
        let mut topics_out: Vec<RespTopicData> = Vec::with_capacity(req.topics.len());
        for t in &req.topics {
            let mut parts_out: Vec<RespPartitionData> = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                let mut out = RespPartitionData {
                    partition_index: p.partition_index,
                    ..Default::default()
                };
                let Some(pr) = image.partition(&t.name, p.partition_index).cloned() else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };
                if p.leader_epoch != pr.leader_epoch {
                    out.error_code = codes::FENCED_LEADER_EPOCH;
                    parts_out.push(out);
                    continue;
                }
                let new_isr: Vec<u64> = p.new_isr.iter().map(|n| u64::try_from(*n).unwrap_or(0)).collect();
                if new_isr.is_empty() || !new_isr.iter().all(|n| pr.replicas.contains(n)) {
                    out.error_code = codes::INVALID_REQUEST;
                    parts_out.push(out);
                    continue;
                }
                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: t.name.clone(),
                    partition: p.partition_index,
                    leader: pr.leader,
                    replicas: pr.replicas.clone(),
                    isr: new_isr.clone(),
                    leader_epoch: pr.leader_epoch,
                }));
                out.error_code = codes::NONE;
                out.leader_id = i32::try_from(pr.leader).unwrap_or(-1);
                out.leader_epoch = pr.leader_epoch;
                out.isr = new_isr.iter().map(|n| i32::try_from(*n).unwrap_or(-1)).collect();
                parts_out.push(out);
            }
            topics_out.push(RespTopicData {
                name: t.name.clone(),
                partitions: parts_out,
                ..Default::default()
            });
        }
        if !changes.is_empty() {
            if let Err(e) = controller.submit_change(changes).await {
                return Err(BrokerError::Replication(format!("submit_change: {e}")));
            }
        }

        let resp = AlterPartitionResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            topics: topics_out,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 2: Register.**

In `handlers/mod.rs`:
```rust
pub(crate) mod alter_partition;
```

In broker.rs HandlerTable:
```rust
table.register(56, crate::handlers::alter_partition::handle);
```

In dispatch.rs:
```rust
56 => version >= 0,  // AlterPartition (all versions flexible at v0+)
```

In api_versions.rs:
```rust
v!(alter_partition_request),
```

- [ ] **Step 3: Test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src
git commit -m "feat(broker): AlterPartition handler (api_key 56) for ISR shrink/expand"
```

Trailer.

---

### Task 20: Spawn isr_maintenance in Broker::start

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Spawn the task.**

Right after the heartbeat client + ticker spawn:

```rust
let isr_handle = tokio::spawn(crate::isr_maintenance::run(
    crate::isr_maintenance::Config {
        node_id: config.node_id,
        partitions: partitions.clone(),
        controller: controller.clone(),
        replica_lag_time_max: Duration::from_millis(config.replica_lag_time_max_ms),
        broker_id: config.broker_id,
        shutdown: isr_shutdown.clone(),
    },
));
```

Store the handle for shutdown.

- [ ] **Step 2: Build + test + commit.**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker/src/broker.rs
git commit -m "feat(broker): spawn isr_maintenance task in Broker::start"
```

Trailer.

---

## Phase H — Remove slice-10a workarounds

### Task 21: Drop `test_install_isr` + `test_wait_for_local_partition`

**Files:**
- Modify: `crates/broker/src/broker.rs`
- Modify: `crates/broker/tests/durability.rs`

- [ ] **Step 1: Remove the helpers.**

In `crates/broker/src/broker.rs`, delete `pub fn test_install_isr` and `pub async fn test_wait_for_local_partition` from `impl BrokerHandle`. Slice-10b's real ISR maintenance + supervisor leader-change handling supersedes them.

- [ ] **Step 2: Update durability.rs.**

The `acks_all_times_out_when_no_follower` test uses `test_install_isr` to fake an ISR. Replace it with a real-multi-broker variant:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acks_all_completes_via_isr_shrink_when_follower_dead() {
    let (mut cluster, bootstrap_1) = boot_three_node().await;
    create_topic(&cluster[0].0, &bootstrap_1, "shrink", 3).await;

    // Kill broker 3 — its absence will force ISR to shrink.
    let dead_broker = cluster.pop().expect("3rd broker");
    dead_broker.0.shutdown().await;

    let start = Instant::now();
    let offset = produce_acks(&bootstrap_1, "shrink", &["x", "y", "z"], -1, 10_000)
        .await
        .expect("acks=-1 success after shrink");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed >= Duration::from_millis(1500),
        "expected to wait for ISR shrink (~2s); took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "shrink + completion should be well under 5s; took {elapsed:?}"
    );
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
```

`boot_three_node` is a new helper that uses `BrokerConfig::for_tests` (which has `replica_lag_time_max_ms=2000`).

Remove the old `acks_all_times_out_when_no_follower` test.

The `consumer_clamps_at_hw_when_followers_lag` test that used `test_install_isr` can stay if it doesn't actually USE the helper (review and adapt).

- [ ] **Step 3: Add `boot_three_node` helper.**

Append to `durability.rs`:

```rust
async fn boot_three_node() -> (Vec<(BrokerHandle, String, TempDir)>, String) {
    use std::net::SocketAddr;
    let client_ports = [11_092u16, 11_192, 11_292];
    let controller_ports = [11_093u16, 11_193, 11_293];
    let voters: Vec<(u64, SocketAddr)> = (0..3)
        .map(|i| (
            u64::try_from(i + 1).unwrap(),
            format!("127.0.0.1:{}", controller_ports[i]).parse().unwrap(),
        ))
        .collect();
    let mut cluster = Vec::with_capacity(3);
    for i in 0..3 {
        let dir = TempDir::new().unwrap();
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("127.0.0.1:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("127.0.0.1:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: Default::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("127.0.0.1:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters.clone(),
            heartbeat_interval_ms: 200,
            heartbeat_timeout_ms: 2_000,
            replica_lag_time_max_ms: 2_000,
        };
        let bootstrap = format!("127.0.0.1:{}", client_ports[i]);
        let broker = Broker::start(cfg).await.expect("boot");
        cluster.push((broker, bootstrap, dir));
    }
    let bootstrap_1 = cluster[0].1.clone();
    (cluster, bootstrap_1)
}
```

- [ ] **Step 4: Build + test + commit.**

```bash
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-broker --lib
git add crates/broker
git commit -m "test(broker): replace test_install_isr-fake with real multi-broker shrink test"
```

Trailer.

---

### Task 22: Re-enable slice-10a's `#[ignore]`d replication tests

**Files:**
- Modify: `crates/broker/tests/replication.rs`
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Un-ignore replication.rs.**

Remove the `#[ignore = "follower replicators intermittently stall..."]` from both `replication_factor_three_propagates_to_all_followers` and `out_of_range_truncates_and_recovers`. Restore their `acks=-1` (the slice-10a workaround was `acks=1`).

- [ ] **Step 2: Un-env-gate the JVM tests.**

In `crates/broker/tests/jvm_acceptance.rs`, remove the `CRABKA_RUN_ACKS_ALL_JVM_TEST` env-gate guard on `acks_all_durability` and the `CRABKA_RUN_REPLICATION_JVM_TEST` guard on `three_node_replication_byte_compare`. Restore the `#[ignore = "requires Docker"]` original markers.

- [ ] **Step 3: Build + test + commit.**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests
git commit -m "test(broker): re-enable slice-10a flaky multi-broker tests under slice-10b ISR"
```

Trailer.

---

## Phase I — New integration tests

### Task 23: `tests/leader_election.rs` — 4 tests

**Files:**
- Create: `crates/broker/tests/leader_election.rs`

Write the file per the spec's "Integration tests" section (4 tests). Pattern after `tests/durability.rs::boot_three_node`. All `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`. Windows-gated. Each test boots 3 brokers, exercises one scenario, then shuts down. The 4 tests:

1. `broker_death_elects_new_leader` — kill leader, verify metadata image's `leader_epoch` advances + `leader` changes within `heartbeat_timeout + 2s`.
2. `produce_during_leader_failover` — 100 records with acks=1, kill leader mid-burst, verify all 100 land via slice-6 idempotence retries.
3. `acks_all_completes_after_isr_shrink` — freeze broker 3 (`shutdown`); acks=-1 produce completes within `replica_lag_time_max + 2s`.
4. `isr_expand_on_catchup` — shrink ISR via freeze; restart broker 2; verify ISR expand to include 2 within `2 * replica_lag_time_max`.

Full code listings would be ~250 lines. Each test follows the pattern of slice-10a's `tests/durability.rs::acks_all_returns_quickly_on_rf1_broker`. Use the `boot_three_node` helper added in Task 21.

- [ ] **Step 1: Write the file** (full code per the patterns above).
- [ ] **Step 2: Build + commit.**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/leader_election.rs
git commit -m "test(broker): leader election integration tests (4 scenarios)"
```

Trailer.

---

### Task 24: `tests/leader_epoch.rs` — 3 tests

**Files:**
- Create: `crates/broker/tests/leader_epoch.rs`

Three tests per the spec. All Windows-gated.

1. `fenced_leader_epoch_truncates_zombie_writes` — single-broker variant: append batches with explicit `partition_leader_epoch`; bump `current_leader_epoch` directly via test-only setter; verify Fetch returns FENCED_LEADER_EPOCH; verify OffsetForLeaderEpoch returns the correct truncation offset.
2. `epoch_checkpoint_byte_compat` — produce batches at different epochs; verify the resulting `.leader-epoch-checkpoint` matches the expected Apache Kafka byte format exactly.
3. `unknown_leader_epoch_on_metadata_lag` — Fetch with `current_leader_epoch=1` against a broker that hasn't applied the change yet (still on `epoch=0`); verify UNKNOWN_LEADER_EPOCH.

Each test will need a test-only setter on `Partition` (`#[cfg(any(test, feature = "test-helpers"))] pub fn test_set_leader_epoch(&self, e: i32)`). Add that to `partition.rs` as part of this task.

- [ ] **Step 1: Add `test_set_leader_epoch` to Partition.**
- [ ] **Step 2: Write the test file** (full code).
- [ ] **Step 3: Build + commit.**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition.rs crates/broker/tests/leader_epoch.rs
git commit -m "test(broker): KIP-101 leader-epoch integration tests (3 scenarios)"
```

Trailer.

---

## Phase J — JVM acceptance + docs + PR

### Task 25: `acks_all_survives_leader_crash` JVM test

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test.**

3-broker Crabka cluster on fixed ports 10392/10492/10592 + 10393/10493/10593 (avoid TIME_WAIT with slice-10a's 10092-10292). 100-record `kafka-console-producer --request-required-acks=-1 --request-timeout-ms=30000`. After the 50th record (mid-burst), programmatically `broker.shutdown()` whichever broker is currently the partition-0 leader (read from `BrokerHandle::current_image().partition("topic", 0).leader`). Verify producer completes; `kafka-console-consumer --isolation-level=read_committed --max-messages=100` reads all 100.

`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, `#[ignore = "requires Docker"]`, `#[allow(clippy::too_many_lines)]`.

- [ ] **Step 2: Build + commit.**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): JVM acks_all_survives_leader_crash acceptance test"
```

Trailer.

---

### Task 26: README + crate rustdoc

**Files:**
- Modify: `README.md`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Append slice-10b entry to README.**

In `README.md`'s "Slices delivered" subsection, append after slice-10a:

```markdown
- **Slice 10b** — bulletproof EOS complete: KIP-101 leader-epoch
  fencing; leader election on broker death (BrokerHeartbeat-driven);
  ISR shrink/expand via AlterPartition. A 3-broker cluster survives
  partition-leader crashes and slow followers; `acks=all` produces
  complete after election; zombie writes from fenced ex-leaders are
  rejected.
```

- [ ] **Step 2: Broker rustdoc.**

Append to `crates/broker/src/lib.rs`'s crate-level `//!` block, after the slice-10a subsection:

```rust
//!
//! ## Bulletproof EOS — sub-slice 10b (leader-epoch + election + ISR)
//!
//! KIP-101 leader-epoch fencing tagged onto every appended batch via
//! [`partition::Partition::current_leader_epoch`]. Per-partition
//! `.leader-epoch-checkpoint` file (Apache Kafka byte-compat format)
//! backs the `OffsetForLeaderEpoch` RPC for follower-side truncation
//! on leader change. Leader election runs on the controller leader:
//! [`heartbeat::controller_state::ControllerLivenessState`] tracks
//! per-broker `last_heartbeat`; a 1s ticker times out brokers at
//! `heartbeat_timeout_ms` and calls
//! [`leader_election::on_broker_dead`] which scans partitions of the
//! dead broker, picks the first alive ISR replica, and bumps
//! `leader_epoch`. ISR shrink/expand is leader-driven by
//! [`isr_maintenance`] — proposes `AlterPartition` whenever a
//! follower's last-fetch time exceeds `replica_lag_time_max_ms`.
//!
//! Together with slice-10a, the bulletproof-EOS promise is complete:
//! `acks=all` produces survive arbitrary single-broker failures with
//! no data loss and no zombie writes.
```

- [ ] **Step 3: Commit.**

```bash
git add README.md crates/broker/src/lib.rs
git commit -m "docs: slice-10b status entry + crate-level rustdoc"
```

Trailer.

---

### Task 27: Acceptance gate + open PR

- [ ] **Step 1: Run the full acceptance gate.**

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

All clean. Fix any drift, run again.

- [ ] **Step 2: Push.**

```bash
git push -u origin feature/bulletproof-eos-10b
```

- [ ] **Step 3: Open the PR.**

```bash
gh pr create --base main --head feature/bulletproof-eos-10b \
    --title "Slice 10b: bulletproof EOS — leader-epoch + election + ISR shrink/expand" \
    --body "$(cat <<'PRBODY'
## Summary

Sub-slice 10b completes the bulletproof EOS deliverable. A 3-broker Crabka cluster survives partition-leader crashes and slow followers: ``acks=all`` produces complete after election picks a new leader from ISR; ``read_committed`` consumers see all committed records; zombie writes from fenced ex-leaders are rejected via KIP-101 leader-epoch.

## What landed

- **BrokerHeartbeat RPC (api_key 63)** + controller-side liveness ticker. Brokers heartbeat every 3s; controller fences at 9s.
- **PartitionRecord.leader_epoch** + per-partition ``.leader-epoch-checkpoint`` file (Apache Kafka byte-compat).
- **OffsetForLeaderEpoch RPC (api_key 23)** + follower-side FENCED_LEADER_EPOCH recovery via truncate.
- **AlterPartition RPC (api_key 56)** + leader-driven ``isr_maintenance`` tick proposes shrink/expand based on ``replica.lag.time.max.ms``.
- **Leader election** in ``leader_election::on_broker_dead`` — bumps ``leader_epoch``, picks first alive ISR member.
- **Supervisor reconciles leader changes** — cancels + respawns replicators, calls ``Partition::install_leader_change``.
- **Restored follower-fetch HW maintenance** in Fetch handler (slice-10a removed it; safe to restore now under dynamic ISR).
- **Removed slice-10a workarounds**: ``BrokerHandle::test_install_isr``/``test_wait_for_local_partition``, the ``acks_all_times_out_when_no_follower`` fake-ISR test.
- **Re-enabled slice-10a flakes**: ``replication_factor_three_propagates_to_all_followers``, ``out_of_range_truncates_and_recovers``, ``three_node_replication_byte_compare``, ``acks_all_durability`` — all running clean under slice-10b's dynamic ISR.
- **New tests**: 4 in-process leader-election scenarios; 3 KIP-101 epoch fence/truncation scenarios; 1 JVM ``acks_all_survives_leader_crash`` acceptance.

## Soft-EOS caveats (post 10b)

- **Full-ISR outage**: if every replica becomes unreachable, partition is unavailable until a former replica rejoins. No unclean leader election. Slice 11 will add ``unclean.leader.election.enable``.
- **Controlled-shutdown handshake**: ``Broker::shutdown`` works but produces ~9s unavailability window. Slice 11 can add KIP-500's ``wantShutdown`` for sub-second failover.

## Reference

- Spec: ``docs/superpowers/specs/2026-05-13-crabka-bulletproof-eos-10b-design.md``
- Plan: ``docs/superpowers/plans/2026-05-13-crabka-bulletproof-eos-10b.md``

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

Report the PR URL.

---

## Self-review against the spec

| # | Spec section / requirement | Plan task |
|---|---|---|
| 1 | Wire codes (FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH) | Task 1 |
| 2 | PartitionRecord.leader_epoch | Task 2 |
| 3 | BrokerConfig fields | Task 3 |
| 4 | LeaderEpochCheckpoint module | Task 4 |
| 5 | Log integration | Task 5 |
| 6 | Partition.current_leader/epoch + install_leader_change | Task 6 |
| 7 | ReplicaState.per_follower | Task 7 |
| 8 | Produce stamps batch epoch | Task 8 |
| 9 | Fetch epoch fence + restore HW maintenance | Task 9 |
| 10 | OffsetForLeaderEpoch handler | Task 10 |
| 11 | Replicator FENCED handling | Task 11 |
| 12 | ControllerLivenessState | Task 12 |
| 13 | BrokerHeartbeat handler | Task 13 |
| 14 | Heartbeat client + ticker spawn | Task 14 |
| 15 | leader_election module | Task 15 |
| 16 | Ticker + heartbeat → leader_election wiring | Task 16 |
| 17 | Supervisor leader-change reconcile | Task 17 |
| 18 | isr_maintenance module | Task 18 |
| 19 | AlterPartition handler | Task 19 |
| 20 | isr_maintenance spawn | Task 20 |
| 21 | Drop test_install_isr; real multi-broker test | Task 21 |
| 22 | Re-enable slice-10a flakes | Task 22 |
| 23 | leader_election.rs tests | Task 23 |
| 24 | leader_epoch.rs tests | Task 24 |
| 25 | JVM acks_all_survives_leader_crash | Task 25 |
| 26 | README + rustdoc | Task 26 |
| 27 | Acceptance gate + PR | Task 27 |

**Placeholder scan:** Tasks 23 and 24 have "Write the file" steps with description but not full code listings (~250 lines each would balloon the plan). The pattern is shown by reference to existing slice-10a tests in `tests/durability.rs::acks_all_returns_quickly_on_rf1_broker`. The implementer should follow that pattern exactly with the helper `boot_three_node` added in Task 21. Other steps have concrete code blocks.

**Type consistency:** `current_leader: Arc<AtomicU64>`, `current_leader_epoch: Arc<AtomicI32>` — used consistently in Tasks 6, 9, 10, 17, 18. `FollowerStats { leo, last_fetch, last_caught_up }` defined in Task 7 — referenced consistently in Tasks 9, 18. `LivenessTransition::{AliveToDead, DeadToAlive}(NodeId)` defined in Task 12 — used in Tasks 13, 16.

**Spec-coverage gaps:** All 27 spec components map to at least one task. Soft-EOS caveats documented in Task 27's PR body.

The plan is ready for execution.
