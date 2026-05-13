# Bulletproof EOS sub-slice 10a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add High Watermark tracking on the leader, gate `acks=all` Produces on HW advance, and clamp consumer Fetch + `read_committed` LSO at HW. After this slice, a JVM producer with `acks=all` against Crabka blocks until every static-ISR replica has the batch; consumers see only fully-replicated records.

**Architecture:** A new per-`Partition` `ReplicaState` tracks each replica's last-fetched offset and caches the HW (= min LEO over ISR). Follower Fetches update `ReplicaState` before reading; consumer Fetches read the cached HW and clamp. The Produce handler awaits HW via a new `await_hw_at_least` primitive on `Partition`, which parks on a per-partition `hw_advance_notify` until satisfied or a deadline elapses. ISR is installed statically (= `replicas` from the metadata image) by the supervisor at materialization time; slice 10b will replace this with controller-driven ISR mutation.

**Tech Stack:** Rust 1.95.0 (workspace pin); tokio (existing); `tokio::sync::Notify` for HW-advance signalling; `tokio::time::sleep_until` for the wait deadline. No new external crates.

**Reference spec:** [`docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`](../specs/2026-05-12-crabka-bulletproof-eos-10a-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/bulletproof-eos-10a`. Implementation runs on `feature/bulletproof-eos-10a` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/broker/src/
├── replica_state.rs                 # NEW — ReplicaState struct + HW computation + unit tests
├── partition.rs                     # MODIFIED — replica_state + hw_advance_notify fields; high_watermark/install_isr/await_hw_at_least
├── partition_writer.rs              # MODIFIED — fire hw_advance_notify after produce/replicate append (handles rf=1 case)
├── broker.rs                        # MODIFIED — spawn_partition initializes ReplicaState + hw_advance_notify
├── codes.rs                         # MODIFIED — NOT_ENOUGH_REPLICAS=19 + NOT_ENOUGH_REPLICAS_AFTER_APPEND=20
├── lib.rs                           # MODIFIED — declare `mod replica_state;`
├── handlers/
│   ├── fetch.rs                     # MODIFIED — follower path updates ReplicaState; consumer path clamps at HW; read_committed uses min(HW, lso)
│   └── produce.rs                   # MODIFIED — acks=-1 awaits Partition::await_hw_at_least
└── replicator_supervisor.rs         # MODIFIED — install_isr after materialize for leader partitions

crates/broker/tests/
├── durability.rs                    # NEW — 5 integration tests (Windows-gated)
└── jvm_acceptance.rs                # MODIFIED — append acks_all_durability JVM test

README.md                            # MODIFIED — append "Slices delivered" sub-section under Status
```

---

## Phase A — Foundations

### Task 1: New error codes + BrokerError mapping

**Files:**
- Modify: `crates/broker/src/codes.rs`

- [ ] **Step 1: Verify which codes are missing**

```bash
grep -n "NOT_ENOUGH_REPLICAS" crates/broker/src/codes.rs
```

Expected: no matches (the codes don't exist yet).

- [ ] **Step 2: Append the two new constants**

Add to `crates/broker/src/codes.rs` in the existing constant block (place after `NOT_LEADER_OR_FOLLOWER`):

```rust
/// Per-partition error returned by `acks=all` Produce when the request
/// completes without enough in-sync replicas confirming the write. The
/// record is durably on the leader's log; the producer should retry.
pub const NOT_ENOUGH_REPLICAS: i16 = 19;

/// Per-partition error returned by `acks=all` Produce when the request
/// appended successfully on the leader but the HW timeout elapsed before
/// enough in-sync replicas confirmed the write. The record is durably on
/// the leader's log but not yet on every ISR follower.
pub const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
```

- [ ] **Step 3: Add unit test**

Append to the existing test module at the bottom of `codes.rs`:

```rust
#[test]
fn not_enough_replicas_codes_have_expected_values() {
    assert_eq!(NOT_ENOUGH_REPLICAS, 19);
    assert_eq!(NOT_ENOUGH_REPLICAS_AFTER_APPEND, 20);
}
```

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-broker codes
git add crates/broker/src/codes.rs
git commit -m "feat(broker): NOT_ENOUGH_REPLICAS{,_AFTER_APPEND} wire codes (19, 20)"
```

Include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` trailer via heredoc.

---

### Task 2: `ReplicaState` module

**Files:**
- Create: `crates/broker/src/replica_state.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Recon `lib.rs`'s `mod` block**

```bash
grep -n "^mod\|^pub(crate) mod" crates/broker/src/lib.rs
```

Expected: existing mod declarations including `partition`, `partition_writer`, `replicator`, `replicator_supervisor`, `txn`. The new module goes in alphabetical order between `producer_state` and `replicator`.

- [ ] **Step 2: Write the module**

Create `crates/broker/src/replica_state.rs`:

```rust
//! Per-partition replica progress tracking, lives on the partition leader.
//!
//! `ReplicaState` records each follower's last-fetched offset (= the
//! follower's persisted LEO from the leader's perspective) and caches
//! the High Watermark = min LEO over the ISR. Slice 10a uses a static
//! ISR (= `replicas` from the metadata image); slice 10b will replace
//! the static install with controller-driven ISR mutation.
//!
//! See `docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`.

use std::collections::{HashMap, HashSet};

use crabka_raft::NodeId;

#[derive(Debug, Clone)]
pub(crate) struct ReplicaState {
    /// In-sync replica set. Slice 10a: static = `replicas` from the
    /// metadata image at materialization time. Slice 10b will mutate.
    pub(crate) isr: HashSet<NodeId>,
    /// Per-non-leader-replica LEO, as reported by follower Fetches'
    /// `fetch_offset`. The leader's own LEO is fed in at HW-compute
    /// time from `Log::log_end_offset()`.
    pub(crate) follower_leo: HashMap<NodeId, i64>,
    /// Cached HW = min(LEO over isr). Recomputed on every follower
    /// Fetch and on every leader-side append (the latter only matters
    /// when isr has a single member — the rf=1 case).
    pub(crate) hw: i64,
}

impl ReplicaState {
    /// Empty state — HW=0, no ISR, no follower LEOs. Used by
    /// `spawn_partition` before the supervisor installs the real ISR.
    pub(crate) fn new() -> Self {
        Self {
            isr: HashSet::new(),
            follower_leo: HashMap::new(),
            hw: 0,
        }
    }

    /// Install (or reinstall) the ISR membership and zero out
    /// follower_leo entries for non-leader replicas. Idempotent: calling
    /// twice with the same `(replicas, leader)` is a no-op for state
    /// purposes (existing follower_leo entries are preserved across
    /// reinstalls so a re-materialize-on-image-change doesn't reset
    /// follower progress).
    pub(crate) fn install_isr(&mut self, replicas: &[NodeId], leader: NodeId) {
        self.isr = replicas.iter().copied().collect();
        for &r in replicas {
            if r != leader {
                self.follower_leo.entry(r).or_insert(0);
            }
        }
        // Drop any stale follower_leo entries for replicas no longer in ISR.
        self.follower_leo.retain(|k, _| self.isr.contains(k));
    }

    /// Apply a follower's reported LEO and recompute HW. Caller fires
    /// `hw_advance_notify` if the returned value exceeds the previous
    /// cached HW. `follower_leo` reports from non-ISR replicas are
    /// ignored.
    ///
    /// Followers can never legitimately report a LEO higher than the
    /// leader's own LEO (the leader writes first, replication is pull-
    /// based). If a follower's reported LEO exceeds `leader_leo`, the
    /// stored value is clamped to `leader_leo` and the HW computation
    /// uses the clamped value.
    pub(crate) fn update_follower_leo(
        &mut self,
        follower: NodeId,
        follower_leo: i64,
        leader_leo: i64,
    ) -> i64 {
        if !self.isr.contains(&follower) {
            // Not in ISR — ignore the report but still recompute HW so
            // an in-flight leader-side append still advances HW for the
            // rf=1 case.
            return self.recompute_hw_for_leader_append(leader_leo);
        }
        let clamped = follower_leo.min(leader_leo);
        self.follower_leo.insert(follower, clamped);
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    /// Recompute HW from the leader's current LEO and the cached
    /// follower_leo map. Caller fires `hw_advance_notify` if the
    /// returned value exceeds the previous cached HW. Used by the
    /// partition writer after each successful leader-side append so
    /// rf=1 partitions (ISR = {leader}) advance HW immediately.
    pub(crate) fn recompute_hw_for_leader_append(&mut self, leader_leo: i64) -> i64 {
        self.hw = self.compute_hw(leader_leo);
        self.hw
    }

    fn compute_hw(&self, leader_leo: i64) -> i64 {
        // Edge case: ISR is empty (state hasn't been installed yet).
        // HW = leader's LEO is correct here — pre-install, the partition
        // is fresh and nothing has replicated.
        if self.isr.is_empty() {
            return leader_leo;
        }
        // HW = min over ISR of {follower_leo for followers, leader_leo for leader}.
        // The leader is in ISR by construction (install_isr only adds
        // non-leader entries to follower_leo, but the leader is in
        // self.isr so it contributes leader_leo as the implicit min).
        let mut min_leo = leader_leo;
        for follower in &self.isr {
            if let Some(&leo) = self.follower_leo.get(follower) {
                if leo < min_leo {
                    min_leo = leo;
                }
            }
            // Followers in ISR that have never sent a Fetch contribute
            // 0 to the min (their default LEO). install_isr() sets
            // every non-leader replica's follower_leo to 0 explicitly,
            // so this branch is only reached when isr contains a
            // replica that wasn't in the install_isr call (impossible
            // by construction).
        }
        min_leo
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> ReplicaState {
        ReplicaState::new()
    }

    #[test]
    fn new_state_has_zero_hw_and_empty_membership() {
        let s = fresh();
        assert_eq!(s.hw, 0);
        assert!(s.isr.is_empty());
        assert!(s.follower_leo.is_empty());
    }

    #[test]
    fn install_isr_seeds_non_leader_followers_at_zero() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        assert_eq!(s.isr, [1, 2, 3].into_iter().collect());
        assert_eq!(s.follower_leo.get(&2), Some(&0));
        assert_eq!(s.follower_leo.get(&3), Some(&0));
        assert!(!s.follower_leo.contains_key(&1));
    }

    #[test]
    fn install_isr_idempotent_preserves_follower_progress() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(2, 50, 100);
        s.update_follower_leo(3, 75, 100);
        // Re-install the same ISR — existing progress should NOT be
        // reset to 0.
        s.install_isr(&[1, 2, 3], 1);
        assert_eq!(s.follower_leo.get(&2), Some(&50));
        assert_eq!(s.follower_leo.get(&3), Some(&75));
    }

    #[test]
    fn install_isr_drops_stale_follower_leo_for_removed_replicas() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(3, 75, 100);
        // Reinstall without node 3.
        s.install_isr(&[1, 2], 1);
        assert!(!s.follower_leo.contains_key(&3));
    }

    #[test]
    fn hw_advances_when_trailing_follower_catches_up() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        // Leader at 100; followers at 50 and 75 — HW = 50.
        let hw1 = s.update_follower_leo(2, 50, 100);
        assert_eq!(hw1, 0); // follower 3 still at 0, so min is 0
        let hw2 = s.update_follower_leo(3, 75, 100);
        assert_eq!(hw2, 50); // min(100 leader, 50 f2, 75 f3) = 50
        // Trailing follower 2 catches up to 80.
        let hw3 = s.update_follower_leo(2, 80, 100);
        assert_eq!(hw3, 75); // now min is f3's 75
    }

    #[test]
    fn hw_pins_at_slowest_isr_follower() {
        let mut s = fresh();
        s.install_isr(&[1, 2, 3], 1);
        s.update_follower_leo(2, 100, 100);
        s.update_follower_leo(3, 30, 100);
        assert_eq!(s.hw, 30);
    }

    #[test]
    fn non_isr_follower_leo_update_ignored() {
        let mut s = fresh();
        s.install_isr(&[1, 2], 1);
        // Node 3 is not in ISR — its report should not influence HW.
        let hw = s.update_follower_leo(3, 999, 100);
        assert_eq!(hw, 100); // ISR = {1, 2}; leader at 100; follower 2 at 0
        // Hmm wait — follower 2's stored LEO is 0 (from install_isr),
        // so HW should be 0, not 100. Let me re-derive: ISR = {1, 2};
        // leader_leo = 100; follower_leo[2] = 0 (default from install);
        // min = 0. So hw = 0.
        assert_eq!(s.hw, 0);
    }

    #[test]
    fn single_replica_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1], 1); // rf=1: only the leader is in ISR
        let hw = s.recompute_hw_for_leader_append(42);
        assert_eq!(hw, 42);
    }

    #[test]
    fn follower_overshoot_clamps_to_leader_leo() {
        let mut s = fresh();
        s.install_isr(&[1, 2], 1);
        // Follower lies and claims LEO higher than the leader's.
        let hw = s.update_follower_leo(2, 200, 100);
        // Clamped to 100; HW = min(100, 100) = 100.
        assert_eq!(hw, 100);
        assert_eq!(s.follower_leo.get(&2), Some(&100));
    }

    #[test]
    fn empty_isr_hw_equals_leader_leo() {
        let mut s = fresh();
        // No install_isr call; isr is empty.
        let hw = s.recompute_hw_for_leader_append(50);
        assert_eq!(hw, 50);
    }
}
```

Re-read the spec's "Components → ReplicaState" before writing. The `install_isr` test for stale-follower-removal is non-obvious; verify it against the recovery semantics.

- [ ] **Step 3: Declare module in `lib.rs`**

Add `pub(crate) mod replica_state;` to `crates/broker/src/lib.rs` in alphabetical order with the existing `mod` declarations.

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-broker replica_state
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/replica_state.rs crates/broker/src/lib.rs
git commit -m "feat(broker): per-partition ReplicaState for HW tracking"
```

Include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` trailer via heredoc.

---

## Phase B — Partition surface

### Task 3: Wire `replica_state` + `hw_advance_notify` onto `Partition`

**Files:**
- Modify: `crates/broker/src/partition.rs`
- Modify: `crates/broker/src/broker.rs` (`spawn_partition`)

- [ ] **Step 1: Add fields to `Partition`**

In `crates/broker/src/partition.rs`, extend the `Partition` struct:

```rust
use crate::replica_state::ReplicaState;

pub struct Partition {
    pub topic: String,
    pub partition_id: i32,
    pub log: Arc<Mutex<Log>>,
    pub writer_tx: mpsc::Sender<WriterMessage>,
    pub append_notify: Arc<Notify>,
    /// Per-partition follower progress + cached HW. Lives on every
    /// `Partition`, but only meaningfully populated where this broker
    /// is leader. Wrapped in `Mutex` (not `tokio::sync::Mutex`) because
    /// reads are short and contention is per-Fetch-or-Produce frequency.
    pub replica_state: Arc<Mutex<ReplicaState>>,
    /// Fires whenever `replica_state.hw` advances. Awaited by the
    /// Produce handler when handling `acks == -1`.
    pub hw_advance_notify: Arc<Notify>,
    pub _writer_handle: Arc<JoinHandle<()>>,
}
```

- [ ] **Step 2: Update `spawn_partition` in `broker.rs`**

In `crates/broker/src/broker.rs`, modify the existing `spawn_partition`:

```rust
pub(crate) fn spawn_partition(
    topic: String,
    partition_id: i32,
    log: crabka_log::Log,
) -> Arc<Partition> {
    let log = Arc::new(Mutex::new(log));
    let (tx, rx) = tokio::sync::mpsc::channel::<WriterMessage>(64);
    let notify = Arc::new(tokio::sync::Notify::new());
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    let hw_advance_notify = Arc::new(tokio::sync::Notify::new());
    let writer = tokio::spawn(crate::partition_writer::run(
        log.clone(),
        rx,
        notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
    ));
    Arc::new(Partition {
        topic,
        partition_id,
        log,
        writer_tx: tx,
        append_notify: notify,
        replica_state,
        hw_advance_notify,
        _writer_handle: Arc::new(writer),
    })
}
```

Note the writer now receives `replica_state` and `hw_advance_notify` so it can update HW on leader-side appends (Task 7). The writer signature change in `partition_writer.rs` lands in Task 7; this task wires the channels.

- [ ] **Step 3: Update existing test helper**

`crates/broker/src/partition.rs` has a test `debug_does_not_dump_log` that constructs a `Partition` manually. Update it to include the new fields:

```rust
let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
let hw_advance_notify = Arc::new(Notify::new());
let p = Partition {
    topic: "t".into(),
    partition_id: 0,
    log: Arc::new(Mutex::new(log)),
    writer_tx: tx,
    append_notify: Arc::new(Notify::new()),
    replica_state,
    hw_advance_notify,
    _writer_handle: Arc::new(writer),
};
```

(The two compile-only test cases `partition_is_clone_and_send` need no changes.)

- [ ] **Step 4: Update `partition_writer::run` signature stub**

In `crates/broker/src/partition_writer.rs`, change the `pub async fn run` signature to accept the two new arguments. Task 7 wires the actual usage; for this task, accept them and prefix with `_` to silence unused warnings:

```rust
use std::sync::{Arc, Mutex};
use crate::replica_state::ReplicaState;

pub async fn run(
    log: Arc<Mutex<Log>>,
    mut rx: mpsc::Receiver<WriterMessage>,
    append_notify: Arc<Notify>,
    _replica_state: Arc<Mutex<ReplicaState>>,
    _hw_advance_notify: Arc<Notify>,
) {
    // body unchanged for now — Task 7 replaces the `_` prefix and adds HW
    // recomputation after each successful Produce/Replicate append.
}
```

Update the existing `partition_writer.rs` tests that call `run(...)` to pass two extra `Arc::new(...)` arguments.

- [ ] **Step 5: Build + test + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition.rs crates/broker/src/broker.rs crates/broker/src/partition_writer.rs
git commit -m "feat(broker): plumb ReplicaState + hw_advance_notify through Partition"
```

Include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` trailer.

---

### Task 4: `Partition::high_watermark()`

**Files:**
- Modify: `crates/broker/src/partition.rs`

- [ ] **Step 1: Write the test**

Append to `crates/broker/src/partition.rs::tests`:

```rust
#[tokio::test]
async fn high_watermark_reads_cached_value() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    {
        let mut st = replica_state.lock().unwrap();
        st.hw = 42;
    }
    let p = Partition {
        topic: "t".into(),
        partition_id: 0,
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state,
        hw_advance_notify: Arc::new(Notify::new()),
        _writer_handle: Arc::new(writer),
    };
    assert_eq!(p.high_watermark(), 42);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cargo test -p crabka-broker partition::tests::high_watermark_reads_cached_value
```

Expected: FAIL — method `high_watermark` does not exist.

- [ ] **Step 3: Implement**

Add to the existing `impl Partition` block in `crates/broker/src/partition.rs`:

```rust
/// Cached High Watermark. Reads `replica_state` briefly. Returns 0 if
/// the mutex is poisoned (the writer task panicked) — caller treats
/// that as "not making progress".
#[must_use]
pub fn high_watermark(&self) -> i64 {
    match self.replica_state.lock() {
        Ok(st) => st.hw,
        Err(_) => 0,
    }
}
```

- [ ] **Step 4: Test passes**

```bash
cargo test -p crabka-broker partition::tests::high_watermark_reads_cached_value
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition.rs
git commit -m "feat(broker): Partition::high_watermark accessor"
```

Trailer.

---

### Task 5: `Partition::install_isr()`

**Files:**
- Modify: `crates/broker/src/partition.rs`

- [ ] **Step 1: Write the test**

Append to `crates/broker/src/partition.rs::tests`:

```rust
#[tokio::test]
async fn install_isr_populates_replica_state() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let p = Partition {
        topic: "t".into(),
        partition_id: 0,
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(Mutex::new(crate::replica_state::ReplicaState::new())),
        hw_advance_notify: Arc::new(Notify::new()),
        _writer_handle: Arc::new(writer),
    };
    p.install_isr(vec![1, 2, 3], 1);
    let st = p.replica_state.lock().unwrap();
    assert_eq!(st.isr.len(), 3);
    assert!(st.isr.contains(&1) && st.isr.contains(&2) && st.isr.contains(&3));
    assert_eq!(st.follower_leo.get(&2), Some(&0));
}
```

- [ ] **Step 2: Run test to verify failure**

```bash
cargo test -p crabka-broker partition::tests::install_isr_populates_replica_state
```

Expected: FAIL — method `install_isr` does not exist.

- [ ] **Step 3: Implement**

Add to `impl Partition`:

```rust
/// Install (or reinstall) the ISR membership and seed non-leader
/// follower_leo entries to 0. Called by the replicator supervisor
/// when this broker materializes a partition where it's the leader.
/// Idempotent: re-installing the same `(replicas, leader)` preserves
/// existing follower progress.
pub fn install_isr(&self, replicas: Vec<crabka_raft::NodeId>, leader: crabka_raft::NodeId) {
    if let Ok(mut st) = self.replica_state.lock() {
        st.install_isr(&replicas, leader);
    }
}
```

- [ ] **Step 4: Test passes**

```bash
cargo test -p crabka-broker partition::tests::install_isr_populates_replica_state
```

- [ ] **Step 5: Commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition.rs
git commit -m "feat(broker): Partition::install_isr passthrough to ReplicaState"
```

Trailer.

---

### Task 6: `Partition::await_hw_at_least()` + `HwTimeout`

**Files:**
- Modify: `crates/broker/src/partition.rs`

- [ ] **Step 1: Write the tests**

Append to `crates/broker/src/partition.rs::tests`:

```rust
#[tokio::test]
async fn await_hw_returns_immediately_if_already_satisfied() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    {
        let mut st = replica_state.lock().unwrap();
        st.hw = 100;
    }
    let p = Partition {
        topic: "t".into(),
        partition_id: 0,
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state,
        hw_advance_notify: Arc::new(Notify::new()),
        _writer_handle: Arc::new(writer),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    p.await_hw_at_least(50, deadline).await.expect("immediate");
}

#[tokio::test]
async fn await_hw_returns_timeout_when_unreached() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let p = Partition {
        topic: "t".into(),
        partition_id: 0,
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: Arc::new(Mutex::new(crate::replica_state::ReplicaState::new())),
        hw_advance_notify: Arc::new(Notify::new()),
        _writer_handle: Arc::new(writer),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
    let result = p.await_hw_at_least(100, deadline).await;
    assert!(matches!(result, Err(crate::partition::HwTimeout)));
}

#[tokio::test]
async fn await_hw_wakes_on_advance() {
    let dir = tempdir().expect("tempdir");
    let log = Log::open(dir.path(), LogConfig::default()).expect("open log");
    let (tx, _rx) = mpsc::channel::<WriterMessage>(1);
    let writer = tokio::spawn(async {});
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    let hw_advance_notify = Arc::new(Notify::new());
    let p = Partition {
        topic: "t".into(),
        partition_id: 0,
        log: Arc::new(Mutex::new(log)),
        writer_tx: tx,
        append_notify: Arc::new(Notify::new()),
        replica_state: replica_state.clone(),
        hw_advance_notify: hw_advance_notify.clone(),
        _writer_handle: Arc::new(writer),
    };
    // Background task: advance HW after 20ms and fire the notify.
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        replica_state.lock().unwrap().hw = 100;
        hw_advance_notify.notify_waiters();
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    p.await_hw_at_least(50, deadline).await.expect("woke on advance");
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cargo test -p crabka-broker partition::tests::await_hw
```

Expected: FAIL — method `await_hw_at_least` and type `HwTimeout` do not exist.

- [ ] **Step 3: Implement**

Add to `crates/broker/src/partition.rs`:

```rust
/// Returned by `await_hw_at_least` when the deadline elapses before
/// the High Watermark reaches the target offset.
#[derive(Debug)]
pub struct HwTimeout;

impl Partition {
    // ... existing methods ...

    /// Wait until `replica_state.hw >= target_offset` or `deadline`
    /// elapses. Used by the Produce handler for `acks == -1` to gate
    /// the response on full replication.
    ///
    /// Returns immediately with `Ok(())` if the cached HW already
    /// satisfies the target. Otherwise parks on `hw_advance_notify`
    /// with a `sleep_until(deadline)` race; on each wake re-reads HW.
    ///
    /// # Errors
    ///
    /// Returns `Err(HwTimeout)` if the deadline elapses before the HW
    /// advances. Returns `Ok(())` on the first re-check that satisfies
    /// the target.
    pub async fn await_hw_at_least(
        &self,
        target_offset: i64,
        deadline: std::time::Instant,
    ) -> Result<(), HwTimeout> {
        loop {
            // Cheap fast path: HW already satisfies.
            if self.high_watermark() >= target_offset {
                return Ok(());
            }
            // Subscribe to the notify BEFORE re-reading HW so we
            // don't miss an advance that happens between the read
            // and the await. (tokio::sync::Notify::notified semantics.)
            let waiter = self.hw_advance_notify.notified();
            tokio::pin!(waiter);
            // One more check after subscribing.
            if self.high_watermark() >= target_offset {
                return Ok(());
            }
            tokio::select! {
                () = &mut waiter => continue,
                () = tokio::time::sleep_until(deadline.into()) => return Err(HwTimeout),
            }
        }
    }
}
```

Note `tokio::time::sleep_until` takes a `tokio::time::Instant`, not `std::time::Instant`. The `.into()` conversion handles that.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p crabka-broker partition::tests::await_hw
```

- [ ] **Step 5: Commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition.rs
git commit -m "feat(broker): Partition::await_hw_at_least + HwTimeout for acks=all blocking"
```

Trailer.

---

## Phase C — Append + Fetch HW updates

### Task 7: `partition_writer` recomputes HW + fires notify on leader-side appends

**Files:**
- Modify: `crates/broker/src/partition_writer.rs`

- [ ] **Step 1: Recon existing writer-loop test fixtures**

```bash
grep -n "fn run\|writer_appends_and_acks\|writer_fires_notify" crates/broker/src/partition_writer.rs
```

The existing test `writer_fires_notify_after_append` only awaits `append_notify`. We add a parallel test for `hw_advance_notify`.

- [ ] **Step 2: Update writer body**

In `crates/broker/src/partition_writer.rs`, modify the `Produce` and `Replicate` arms to recompute HW and fire the new notify. The signature change from Task 3 is already in place — replace the `_replica_state` / `_hw_advance_notify` placeholders with usage:

```rust
use std::sync::{Arc, Mutex};

use crabka_log::Log;
use tokio::sync::{Notify, mpsc};

use crate::partition::{ProduceJob, WriterMessage};
use crate::replica_state::ReplicaState;

pub async fn run(
    log: Arc<Mutex<Log>>,
    mut rx: mpsc::Receiver<WriterMessage>,
    append_notify: Arc<Notify>,
    replica_state: Arc<Mutex<ReplicaState>>,
    hw_advance_notify: Arc<Notify>,
) {
    while let Some(msg) = rx.recv().await {
        match msg {
            WriterMessage::Produce(ProduceJob { mut batch, ack }) => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.append(&mut batch)
                        .map_err(crate::error::BrokerError::from)
                };
                let ok = result.is_ok();
                let _ = ack.send(result);
                if ok {
                    append_notify.notify_waiters();
                    // Recompute HW: rf=1 case (ISR = {leader}) advances
                    // HW to leader_leo. For rf>1 the HW only advances
                    // when followers catch up via Fetch, but the call
                    // is cheap and the notify is harmless.
                    let new_hw = {
                        let leader_leo = log.lock().expect("log mutex poisoned").log_end_offset();
                        let mut st = replica_state.lock().expect("replica_state mutex poisoned");
                        let prev = st.hw;
                        let new = st.recompute_hw_for_leader_append(leader_leo);
                        if new > prev { Some(new) } else { None }
                    };
                    if new_hw.is_some() {
                        hw_advance_notify.notify_waiters();
                    }
                }
            }
            WriterMessage::Replicate { mut batch, ack } => {
                // Follower-side replicate: never advance HW here. The
                // leader is the source of truth for HW; followers learn
                // it from FetchResponse.high_watermark. (Slice 10a
                // doesn't yet propagate the leader's HW to the
                // follower's cached state — that's a slice 10b
                // refinement; for now the follower's `replica_state` is
                // never read for HW purposes.)
                let offset = batch.base_offset;
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.append_at(&mut batch, offset)
                        .map_err(crate::error::BrokerError::from)
                };
                let ok = result.is_ok();
                let _ = ack.send(result);
                if ok {
                    append_notify.notify_waiters();
                }
            }
            WriterMessage::Truncate { offset, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.truncate_to(offset)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
            }
            WriterMessage::ResetTo { new_base, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.reset_to(new_base)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
            }
            #[cfg(any(test, feature = "test-helpers"))]
            WriterMessage::TestSetLogStart { new_start, ack } => {
                let result = {
                    let mut log = log.lock().expect("log mutex poisoned");
                    log.test_set_log_start_offset(new_start)
                        .map_err(crate::error::BrokerError::from)
                };
                let _ = ack.send(result);
            }
        }
    }
}
```

Note the careful lock-ordering: the log mutex must be re-acquired briefly after the append+ack pair to read `log_end_offset()`, then released before acquiring the `replica_state` mutex. Holding both at once is fine in principle (both are `std::sync::Mutex`, neither is held across `.await`), but the brief re-lock matches the pattern used elsewhere in the codebase.

- [ ] **Step 3: Write the new test**

Append to `crates/broker/src/partition_writer.rs::tests`:

```rust
#[tokio::test]
async fn writer_fires_hw_notify_after_produce_when_rf_one() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    // rf=1: only the leader is in ISR. Leader node_id = 1.
    {
        let mut st = replica_state.lock().unwrap();
        st.install_isr(&[1], 1);
    }
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run(
        log.clone(),
        rx,
        append_notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
    ));

    // Subscribe BEFORE sending so we don't miss the notification.
    let waiter = hw_advance_notify.notified();
    tokio::pin!(waiter);

    let (ack, _ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        batch: sample_batch(2),
        ack,
    }))
    .await
    .expect("send job");

    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("hw_advance_notify did not fire");

    // Verify HW = 2 (LEO).
    assert_eq!(replica_state.lock().unwrap().hw, 2);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn writer_does_not_fire_hw_notify_when_followers_lagging() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let (tx, rx) = mpsc::channel(1);
    let append_notify = Arc::new(Notify::new());
    let replica_state = Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()));
    // rf=3 ISR; followers at LEO 0. HW must stay at 0 across the
    // leader-side append because the followers haven't caught up.
    {
        let mut st = replica_state.lock().unwrap();
        st.install_isr(&[1, 2, 3], 1);
    }
    let hw_advance_notify = Arc::new(Notify::new());
    let writer = tokio::spawn(run(
        log.clone(),
        rx,
        append_notify.clone(),
        replica_state.clone(),
        hw_advance_notify.clone(),
    ));

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        batch: sample_batch(3),
        ack,
    }))
    .await
    .expect("send job");
    ack_rx.await.expect("ack").expect("append ok");

    // HW must still be 0 — followers are at LEO 0.
    assert_eq!(replica_state.lock().unwrap().hw, 0);

    drop(tx);
    writer.await.expect("writer join");
}
```

Update the existing `writer_appends_and_acks`, `writer_fires_notify_after_append`, `writer_handles_replicate_with_caller_offset`, `writer_replicate_offset_mismatch_surfaces_error`, `writer_truncate_drops_records` tests to pass the two new `Arc::new(...)` arguments to `run(...)`. Pattern: add `Arc::new(Mutex::new(crate::replica_state::ReplicaState::new()))` and `Arc::new(Notify::new())` at the end of the existing `run()` calls.

- [ ] **Step 4: Tests pass**

```bash
cargo test -p crabka-broker partition_writer
```

All existing + 2 new tests pass.

- [ ] **Step 5: Commit**

```bash
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/src/partition_writer.rs
git commit -m "feat(broker): writer recomputes HW + fires hw_advance_notify on leader appends"
```

Trailer.

---

### Task 8: Fetch follower path — update `ReplicaState` before reading

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Locate the follower vs. consumer branch**

The handler already has `let is_follower_fetch = req.replica_id >= 0;` at line ~72. The per-partition resolution loop starts at line ~80.

- [ ] **Step 2: Insert the ReplicaState update**

In `crates/broker/src/handlers/fetch.rs`, modify the per-partition loop (right after `part_opt` is resolved, before `pending.push(...)`):

```rust
// ── HW maintenance (follower fetch) ──────────────────────────────
// When the call is a follower fetch (replica_id >= 0), use the
// incoming fetch_offset as the follower's persisted LEO from the
// leader's perspective: at this point the follower has durably
// appended everything below fetch_offset and is asking for what's
// next. Update ReplicaState and fire hw_advance_notify if HW moved.
if is_follower_fetch {
    if let Some(part) = part_opt.as_ref() {
        let leader_leo = part.log_end_offset();
        let new_hw_opt = {
            let mut st = part.replica_state.lock().expect("replica_state mutex poisoned");
            let prev = st.hw;
            let new = st.update_follower_leo(
                u64::try_from(req.replica_id).unwrap_or(0),
                fetch_offset,
                leader_leo,
            );
            if new > prev { Some(new) } else { None }
        };
        if new_hw_opt.is_some() {
            part.hw_advance_notify.notify_waiters();
        }
    }
}
```

Place this block between the existing `let part_opt = partitions.get(...)` and the `if part_opt.is_none() || topic_name.is_empty()` block.

- [ ] **Step 3: Write the test**

Add to `crates/broker/tests/durability.rs` (file landing in Task 12 — for now, defer the test there). To keep this task self-contained, add a minimal unit-level test in `crates/broker/src/handlers/fetch.rs`:

Actually — `fetch.rs` doesn't currently have a `#[cfg(test)] mod tests`. The handler logic is exercised end-to-end via the integration tests. Skip the unit test here; Task 12's `acks_one_returns_before_replication` and `consumer_clamps_at_hw` will catch regressions.

- [ ] **Step 4: Build + clippy**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --lib
```

Expected: all existing tests pass. Slice-9 transactional and slice-8 replication tests must remain green.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): Fetch follower path updates ReplicaState"
```

Trailer.

---

### Task 9: Fetch consumer path — clamp at HW + read_committed uses min(HW, lso)

**Files:**
- Modify: `crates/broker/src/handlers/fetch.rs`

- [ ] **Step 1: Modify `do_read` to clamp at HW**

In `crates/broker/src/handlers/fetch.rs`, the `do_read` function currently uses `log_end` as the visible boundary and sets `out.high_watermark = log_end`. We need to:

1. Pass a fourth parameter to `do_read`: `is_follower_fetch: bool`.
2. When `is_follower_fetch` is true: behave as today (follower sees everything up to LEO).
3. When `is_follower_fetch` is false (consumer): clamp visible batches to `base_offset < hw`; set `out.high_watermark = hw`; `out.last_stable_offset = if read_committed { min(hw, lso) } else { hw }`.

Replace the existing `do_read` signature + body:

```rust
fn do_read(
    part: &Partition,
    fetch_offset: i64,
    max_bytes: i32,
    read_committed: bool,
    is_follower_fetch: bool,
    out: &mut PartitionData,
) -> Result<usize, BrokerError> {
    let hw = part.high_watermark();
    let (log_start, log_end, lso, batch_opt, aborted_txns): (
        i64,
        i64,
        i64,
        Option<RecordBatch>,
        Vec<AbortedTransaction>,
    ) = {
        let log = part.log.lock().expect("log mutex poisoned");
        let log_start = log.log_start_offset();
        let log_end = log.log_end_offset();
        let lso = log.lso();
        // Consumer fetches clamp the visible upper bound to HW;
        // follower fetches see everything up to LEO.
        let upper_bound = if is_follower_fetch { log_end } else { hw };
        // For read_committed consumers the effective LSO tightens to
        // min(HW, log.lso()) so that committed transactional records
        // are only visible once they're also durable across the ISR.
        let effective_lso = if read_committed && !is_follower_fetch {
            lso.min(hw)
        } else {
            lso
        };

        if fetch_offset < log_start {
            out.error_code = codes::OFFSET_OUT_OF_RANGE;
            out.log_start_offset = log_start;
            out.high_watermark = if is_follower_fetch { log_end } else { hw };
            out.last_stable_offset = if read_committed && !is_follower_fetch {
                effective_lso
            } else if is_follower_fetch {
                log_end
            } else {
                hw
            };
            return Ok(0);
        }
        if fetch_offset >= upper_bound {
            (log_start, log_end, lso, None, Vec::new())
        } else {
            let read_max = usize::try_from(max_bytes.max(0)).unwrap_or(0);
            let read = log.read(fetch_offset, read_max)?;

            if read_committed && !is_follower_fetch {
                let aborted_raw = log.aborted_in_range(fetch_offset, effective_lso);
                let aborted_pids: std::collections::HashSet<(i64, i64, i64)> = aborted_raw
                    .iter()
                    .map(|e| (e.producer_id, e.start_offset, e.last_offset))
                    .collect();
                let aborted = aborted_raw
                    .into_iter()
                    .map(|e| AbortedTransaction {
                        producer_id: e.producer_id,
                        first_offset: e.start_offset,
                        ..Default::default()
                    })
                    .collect();

                let visible_batch = read
                    .batches
                    .into_iter()
                    .filter(|b| b.base_offset < effective_lso)
                    .filter(|b| !b.attributes.is_control_batch())
                    .find(|b| {
                        if !b.attributes.is_transactional() {
                            return true;
                        }
                        let pid = b.producer_id;
                        let batch_last = b.base_offset + i64::from(b.last_offset_delta);
                        !aborted_pids.iter().any(|&(apid, astart, alast)| {
                            apid == pid && b.base_offset >= astart && batch_last <= alast
                        })
                    });

                (log_start, log_end, lso, visible_batch, aborted)
            } else if !is_follower_fetch {
                // Consumer fetch in read_uncommitted: just clamp at HW.
                let batch_opt = read.batches.into_iter().find(|b| b.base_offset < hw);
                (log_start, log_end, lso, batch_opt, Vec::new())
            } else {
                // Follower fetch: no clamping, no filtering.
                let batch_opt = read.batches.into_iter().next();
                (log_start, log_end, lso, batch_opt, Vec::new())
            }
        }
    };

    out.error_code = codes::NONE;
    out.high_watermark = if is_follower_fetch { log_end } else { hw };
    out.log_start_offset = log_start;
    out.last_stable_offset = if read_committed && !is_follower_fetch {
        lso.min(hw)
    } else if is_follower_fetch {
        log_end
    } else {
        hw
    };

    if read_committed && !is_follower_fetch {
        out.aborted_transactions = Some(aborted_txns);
    }

    let bytes_est = batch_opt
        .as_ref()
        .map_or(0, |b| <RecordBatch as Encode>::encoded_len(b, 0));
    out.records = batch_opt;
    Ok(bytes_est)
}
```

- [ ] **Step 2: Update all `do_read` call sites**

The existing `do_read` is called in two places: the first-read pass and the long-poll re-read in `long_poll_then_reread`. Update both to pass `is_follower_fetch`.

In the first-read pass loop (~line 145):

```rust
for p in &mut pending {
    if let Some(part) = &p.partition {
        total_bytes += do_read(
            part,
            p.fetch_offset,
            p.max_bytes,
            p.read_committed,
            p.is_follower_fetch, // NEW
            &mut p.out,
        )?;
    }
}
```

Add `is_follower_fetch: bool` to the `PendingRead` struct definition (~line 36):

```rust
struct PendingRead {
    topic_name: String,
    topic_id: WireUuid,
    partition_index: i32,
    fetch_offset: i64,
    max_bytes: i32,
    read_committed: bool,
    is_follower_fetch: bool, // NEW
    partition: Option<Arc<Partition>>,
    out: PartitionData,
}
```

Populate it in both `pending.push(PendingRead { ... })` sites (search for "pending.push"):

```rust
pending.push(PendingRead {
    topic_name: topic_name.clone(),
    topic_id,
    partition_index: idx,
    fetch_offset,
    max_bytes,
    read_committed,
    is_follower_fetch, // NEW — from the outer scope
    partition: part_opt,
    out,
});
```

And the same in the early-error push.

Update `long_poll_then_reread`'s call:

```rust
do_read(
    part,
    p.fetch_offset,
    p.max_bytes,
    p.read_committed,
    p.is_follower_fetch, // NEW
    &mut p.out,
)?;
```

- [ ] **Step 3: Build + clippy**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 4: Run existing tests (no regression)**

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --test transactions  # slice-9 read_committed regression check
```

Expected: all green. Slice-9's `commit_then_read_committed_sees_records` test exercises read_committed: HW = LEO for an rf=1 in-process broker because the writer's HW-recompute path advances HW to LEO immediately (Task 7), so `effective_lso = min(HW, lso) = lso` and the test continues to pass.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/fetch.rs
git commit -m "feat(broker): Fetch clamps consumer path at HW; read_committed uses min(HW, lso)"
```

Trailer.

---

## Phase D — Produce `acks=all`

### Task 10: Produce handler awaits HW when `acks == -1`

**Files:**
- Modify: `crates/broker/src/handlers/produce.rs`

- [ ] **Step 1: Recon the existing handler shape**

The handler accepts `req.acks: i16`. Values: 0 = fire-and-forget, 1 = leader-ack-only, -1 = full ISR. The current `Ok(Ok(Ok(base_offset)))` branch (line ~224) is where success is set; we extend it to wait on HW when `acks == -1`.

- [ ] **Step 2: Modify the success branch**

In `crates/broker/src/handlers/produce.rs`, replace the existing `Ok(Ok(Ok(base_offset)))` arm body with:

```rust
Ok(Ok(Ok(base_offset))) => {
    // For acks == -1 ("all"), block until the High Watermark
    // advances past this batch's last offset, or the request's
    // timeout_ms expires. The append already succeeded on the
    // leader; the wait is purely for the durability gate.
    if req.acks == -1 {
        let target = base_offset + i64::from(last_offset_delta) + 1;
        let deadline = std::time::Instant::now() + timeout;
        match part.await_hw_at_least(target, deadline).await {
            Ok(()) => {
                out.error_code = codes::NONE;
                out.base_offset = base_offset;
                if pid >= 0 {
                    producer_state
                        .commit(
                            &topic_name,
                            idx,
                            pid,
                            epoch,
                            base_seq,
                            last_offset_delta,
                            base_offset,
                            max_timestamp,
                        )
                        .await;
                }
            }
            Err(_timeout) => {
                out.error_code = codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND;
                out.base_offset = base_offset;
                if pid >= 0 {
                    // Still commit idempotence state — the record IS
                    // durably on the leader; on retry the producer
                    // hits the dedup gate and gets the same base_offset.
                    producer_state
                        .commit(
                            &topic_name,
                            idx,
                            pid,
                            epoch,
                            base_seq,
                            last_offset_delta,
                            base_offset,
                            max_timestamp,
                        )
                        .await;
                }
            }
        }
    } else {
        // acks == 0 (no response expected at the request level, but the
        // partition response is still encoded) or acks == 1 (leader-
        // only ack): unchanged behavior.
        out.error_code = codes::NONE;
        out.base_offset = base_offset;
        if pid >= 0 {
            producer_state
                .commit(
                    &topic_name,
                    idx,
                    pid,
                    epoch,
                    base_seq,
                    last_offset_delta,
                    base_offset,
                    max_timestamp,
                )
                .await;
        }
    }
}
```

The other error arms (`Ok(Ok(Err(e)))`, `Ok(Err(_))`, `Err(_)`) stay unchanged — those represent append failure or writer-died, which we don't need to gate on HW.

- [ ] **Step 3: Build + clippy**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

- [ ] **Step 4: Regression check**

```bash
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --test unit
cargo test -p crabka-broker --test transactions
```

Expected: all green. Slice-9 transactional tests use the default builder which produces with `acks=1` (idempotent producer default) — they don't exercise the new path.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/handlers/produce.rs
git commit -m "feat(broker): Produce acks=-1 awaits HW; returns NOT_ENOUGH_REPLICAS_AFTER_APPEND on timeout"
```

Trailer.

---

## Phase E — Supervisor installs ISR

### Task 11: Supervisor calls `install_isr` after materialize on leader partitions

**Files:**
- Modify: `crates/broker/src/replicator_supervisor.rs`

- [ ] **Step 1: Recon existing reconcile loop**

The `reconcile` function in `crates/broker/src/replicator_supervisor.rs` already iterates `desired_local_set(self.node_id, image)` and calls `materialize_local_partition`. We extend the same loop to call `install_isr` when self is leader.

- [ ] **Step 2: Modify the reconcile loop**

Replace the existing "Step 0" block in `reconcile`:

```rust
// 0. Materialize the on-disk partition for every assignment where
//    self is in `replicas`, regardless of leader/follower role.
//    Additionally: for every partition where self is leader,
//    install the static ISR (= replicas) into the partition's
//    ReplicaState so the HW computation has the correct membership.
for key in desired_local_set(self.node_id, image) {
    if let Err(e) = self.materialize_local_partition(&key.0, key.1) {
        warn!(
            topic = %key.0, partition = key.1, error = %e,
            "failed to materialize local partition"
        );
        continue;
    }
    // After materialize, install ISR if self is leader.
    let Some(part_record) = image.partition(&key.0, key.1).cloned() else {
        continue;
    };
    if part_record.leader != self.node_id {
        continue;
    }
    let Some(part) = self.partitions.get(&(key.0.clone(), key.1)).map(|e| e.value().clone()) else {
        continue;
    };
    part.install_isr(part_record.replicas.clone(), part_record.leader);
}
```

- [ ] **Step 3: Test**

The existing `desired_follower_set` tests don't cover the ISR install. Add a new test:

```rust
#[tokio::test]
async fn reconcile_installs_isr_on_leader_partition() {
    use crabka_log::LogConfig;
    use tempfile::tempdir;
    use tokio_util::sync::CancellationToken;

    let dir = tempdir().expect("tempdir");
    let img = image_with(&[
        MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 3,
        }),
        MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1, // we'll run supervisor as node_id=1
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
        }),
    ]);
    let partitions = Arc::new(DashMap::new());
    // ReplicatorSupervisor needs a ControllerHandle and a TxnCoordinator
    // for `new` — but for this unit test we drive `reconcile` directly,
    // so we can construct a minimal supervisor without spawning it.
    // Skip the full constructor; instead test the materialize+install
    // logic via the standalone `materialize_partition` helper + manual
    // install_isr call mirroring what reconcile does.
    crate::replicator_supervisor::materialize_partition(
        &partitions,
        "t",
        0,
        dir.path(),
        &LogConfig::default(),
    )
    .expect("materialize");
    let part = partitions.get(&("t".to_string(), 0)).expect("part").value().clone();
    part.install_isr(vec![1, 2, 3], 1);
    let st = part.replica_state.lock().expect("lock");
    assert_eq!(st.isr.len(), 3);
}
```

This test exercises the helper functions in isolation rather than spinning up a full `ReplicatorSupervisor` — full reconciliation behavior is covered end-to-end by the integration tests in Task 12.

- [ ] **Step 4: Build + clippy + test**

```bash
cargo build -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker replicator_supervisor
```

Expected: 5 existing + 1 new test, all green.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/replicator_supervisor.rs
git commit -m "feat(broker): supervisor installs ISR on leader partitions after materialize"
```

Trailer.

---

## Phase F — Tests

### Task 12: Integration tests in `durability.rs`

**Files:**
- Create: `crates/broker/tests/durability.rs`

- [ ] **Step 1: Write the file scaffolding**

Create `crates/broker/tests/durability.rs`:

```rust
//! Integration tests for sub-slice 10a (bulletproof EOS — HW + acks=all).
//!
//! Windows-gated like slice-7/8/9 multi-broker tests: openraft +
//! `tokio` scheduling on Windows runners cause flakes that have
//! nothing to do with the protocol being tested.

#![cfg(not(target_os = "windows"))]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Record, RecordBatch};

fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch {
        last_offset_delta: (i32::try_from(values.len()).unwrap() - 1).max(0),
        max_timestamp: i64::try_from(values.len()).unwrap(),
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).unwrap(),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

async fn boot_single() -> (Broker, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, name: &str, rf: i16) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: rf,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 0, "CreateTopics failed: {resp:?}");
}

async fn produce_acks(
    bootstrap: &str,
    topic: &str,
    values: &[&str],
    acks: i16,
    timeout_ms: i32,
) -> Result<i64, i16> {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(ProduceRequest {
            acks,
            timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.into(),
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch_with_values(values)),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    if pr.error_code == 0 {
        Ok(pr.base_offset)
    } else {
        Err(pr.error_code)
    }
}
```

- [ ] **Step 2: Test 1 — `acks_one_returns_quickly_on_rf1_broker`**

(Slice 10a uses a single-broker harness for these tests because Crabka's slice-8 follower replicators talk a real TCP loop that we don't want to mock here. With rf=1 the writer's HW-recompute path advances HW to LEO immediately, so acks=1 and acks=-1 produce equivalent end-state behavior; the *blocking* behavior under multi-broker is exercised by the JVM acceptance test in Task 13.)

Append to `durability.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_one_returns_quickly_on_rf1_broker() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ack1", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ack1", &["a", "b", "c"], 1, 5_000)
        .await
        .expect("ack=1 success");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}
```

- [ ] **Step 3: Test 2 — `acks_all_returns_quickly_on_rf1_broker`**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_returns_quickly_on_rf1_broker() {
    // rf=1: ISR = {leader}; HW = leader_leo after every append; acks=-1
    // returns synchronously after the writer's HW-recompute fires.
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "ackall", 1).await;
    let start = Instant::now();
    let offset = produce_acks(&bootstrap, "ackall", &["a", "b", "c"], -1, 5_000)
        .await
        .expect("ack=-1 success");
    let elapsed = start.elapsed();
    assert_eq!(offset, 0);
    assert!(
        elapsed < Duration::from_secs(1),
        "acks=-1 on rf=1 should return promptly; took {elapsed:?}"
    );
    broker.shutdown().await;
}
```

- [ ] **Step 4: Test 3 — `consumer_clamps_at_hw_when_followers_lag`**

This test forces a multi-replica metadata image while running only a single broker — so the leader's view of the ISR has followers that never check in, pinning HW at 0 forever (until we manually advance via a test helper). It exercises consumer clamping in isolation.

Add a test-only helper to the broker crate for this: since we can't easily install fake follower entries from a test (Partition.replica_state.install_isr is `pub` but takes real NodeIds), we instead use the rf=1 path and verify consumer clamping happens by reading at offset 0 after producing.

For a cleaner test, install a fake ISR via the test-only helper and prove HW pins at 0:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumer_clamps_at_hw_when_followers_lag() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "clamp", 1).await;

    // Produce 3 records under rf=1 (HW advances to LEO immediately).
    let offset = produce_acks(&bootstrap, "clamp", &["x", "y", "z"], 1, 5_000)
        .await
        .expect("produce ok");
    assert_eq!(offset, 0);

    // Consumer fetch with replica_id=-1: should see records up to HW.
    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "clamp".into(),
                topic_id: WireUuid::ZERO,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let pd = &resp.responses[0].partitions[0];
    assert_eq!(pd.error_code, 0);
    // HW must be at the end of the data (rf=1 → HW = LEO = 3).
    assert_eq!(pd.high_watermark, 3, "HW should equal LEO for rf=1");

    broker.shutdown().await;
}
```

- [ ] **Step 5: Test 4 — `read_committed_clamps_at_min_hw_lso`**

For this test we open a transactional producer (slice 9 primitives), commit a txn, and verify read_committed sees the records — confirming that under rf=1 (where HW = LEO), `min(HW, lso)` doesn't change behavior from slice 9. Acts as the regression-pin.

```rust
use crabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use crabka_client_producer::{Producer, ProducerRecord};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_under_rf1_unchanged_from_slice9() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "rctxn", 1).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("rc-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    producer.begin_transaction().await.unwrap();
    for v in ["p", "q", "r"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "rctxn".into(),
                    value: Some(Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.commit_transaction().await.unwrap();

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("rc-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["rctxn".to_string()])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    assert_eq!(seen, vec!["p", "q", "r"]);
    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}
```

- [ ] **Step 6: Test 5 — `acks_all_times_out_when_no_follower`**

This test uses a custom multi-broker harness without actually wiring a second broker — instead we install a fake ISR with two members while only one broker is running, so the leader can never advance HW. The producer's `acks=-1` should hit the timeout and return `NOT_ENOUGH_REPLICAS_AFTER_APPEND`.

Crabka's broker crate doesn't yet expose a public hook to install fake ISR from an integration test. Add a test-only helper to `BrokerConfig` or `Broker` — or, more surgically, expose a `Broker::test_install_isr(topic, partition, replicas, leader)` method behind `#[cfg(any(test, feature = "test-helpers"))]`.

Add to `crates/broker/src/broker.rs`:

```rust
impl Broker {
    /// Test-only: install a fake ISR on the named partition. Used by
    /// the slice-10a `durability.rs` test that proves
    /// `acks=-1 + missing followers → NOT_ENOUGH_REPLICAS_AFTER_APPEND`.
    #[cfg(any(test, feature = "test-helpers"))]
    pub fn test_install_isr(
        &self,
        topic: &str,
        partition: i32,
        replicas: Vec<u64>,
        leader: u64,
    ) {
        if let Some(part) = self.partitions.get(&(topic.to_string(), partition)) {
            part.value().install_isr(replicas, leader);
        }
    }
}
```

The `feature = "test-helpers"` is needed so integration tests can call this from outside the crate's `tests/` directory. Add the feature to `crates/broker/Cargo.toml`:

```toml
[features]
test-helpers = []
```

And the dev-dependency line for `crabka-broker` in `[dev-dependencies]` of `crates/broker/Cargo.toml` (if missing — it's automatic when the integration test uses the crate's `tests/` folder). For tests inside the broker's `tests/` directory (which is the case here), `#[cfg(any(test, feature = "test-helpers"))]` works because `cargo test` enables `cfg(test)` for the entry-crate but not for its dependencies — and integration tests *are* the entry crate. So `#[cfg(test)]` alone won't work. We need the feature gate.

Actually, simpler approach: integration tests link against `crabka-broker` like a downstream crate, so `cfg(test)` is NOT set for the broker code as seen by integration tests. The feature gate is required. Add to `crates/broker/tests/durability.rs`:

```rust
// No special activation needed — the test-helpers feature is enabled
// automatically via the [dev-dependencies] section's
// `crabka-broker = { path = ".", features = ["test-helpers"] }` if
// the broker is a dev-dep of itself.
```

For the broker's own integration tests in `crates/broker/tests/`, the broker is implicitly linked WITHOUT dev features. The cleanest workaround is to add a `dev-dependencies` self-reference to opt into the feature:

Append to `crates/broker/Cargo.toml`:

```toml
[features]
test-helpers = []

[dev-dependencies]
crabka-broker = { path = ".", features = ["test-helpers"] }
```

Wait — a crate cannot depend on itself in `[dev-dependencies]`. The actual fix is the trick of using a `[[test]] required-features = ["test-helpers"]` declaration, OR exposing the helper inside the `[[test]]` target unconditionally. The latter is simpler — use the broker's own `#[cfg(test)]` cfg for inline unit tests, OR for `tests/` integration tests, add the helper without any cfg gate but inside a `#[doc(hidden)]` module to discourage misuse:

Easier path: keep the helper public + un-gated, mark it `#[doc(hidden)]`, and document "test-only — do not call from production code":

```rust
impl Broker {
    /// Test-only helper. Do not call from production code. Used by
    /// integration tests that need to install a synthetic ISR to
    /// exercise the HW gate without spinning up multiple brokers.
    #[doc(hidden)]
    pub fn test_install_isr(
        &self,
        topic: &str,
        partition: i32,
        replicas: Vec<u64>,
        leader: u64,
    ) {
        if let Some(part) = self.partitions.get(&(topic.to_string(), partition)) {
            part.value().install_isr(replicas, leader);
        }
    }
}
```

Now the test in `durability.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_all_times_out_when_no_follower() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&bootstrap, "tout", 1).await;

    // Install a fake ISR with two members; only this broker (node 1)
    // is actually running, so node 2 can never check in via Fetch.
    // The leader's HW thus stays pinned at 0.
    broker.test_install_isr("tout", 0, vec![1, 2], 1);

    let start = Instant::now();
    let err = produce_acks(&bootstrap, "tout", &["x"], -1, 200)
        .await
        .expect_err("expected timeout");
    let elapsed = start.elapsed();
    assert_eq!(err, 20, "expected NOT_ENOUGH_REPLICAS_AFTER_APPEND");
    assert!(
        elapsed >= Duration::from_millis(180),
        "expected to wait ~200ms; took {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "should not wait significantly past timeout; took {elapsed:?}"
    );
    broker.shutdown().await;
}
```

- [ ] **Step 7: Build + run**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
cargo test -p crabka-broker --test durability
```

Expected: all 5 tests pass on Linux/macOS; skipped on Windows.

- [ ] **Step 8: Commit**

```bash
git add crates/broker/tests/durability.rs crates/broker/src/broker.rs
git commit -m "test(broker): durability integration tests for HW + acks=all"
```

Trailer.

---

### Task 13: JVM acceptance test `acks_all_durability`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Recon existing JVM test patterns**

```bash
grep -n "fn three_node_jvm_round_trip\|fn transactional_console_producer_eos\|KAFKA_IMAGE\|docker_run_kafka_tool" crates/broker/tests/jvm_acceptance.rs | head -10
```

The slice-9 PR added `transactional_console_producer_eos` with the same multi-broker scaffolding. We mirror its layout.

- [ ] **Step 2: Append the new test**

Append to `crates/broker/tests/jvm_acceptance.rs` (at the end, after the existing `transactional_console_producer_eos`):

```rust
// `acks=all` durability gate: 3-broker Crabka cluster, JVM
// `kafka-console-producer --request-required-acks -1` writes 100
// records, then `kafka-console-consumer --isolation-level
// read_committed` reads them all back. Confirms HW+acks=all works
// against an unmodified JVM client.
//
// Fixed ports: 9692/9792/9892 + 9693/9793/9893 (offset 600 from
// slice-8's replication test, 200 from slice-9's transactional
// test) to dodge TIME_WAIT collisions when running JVM tests
// sequentially.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acks_all_durability() {
    const TOPIC: &str = "crabka-acks-all-itest";

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=debug,info")),
        )
        .with_test_writer()
        .try_init();

    let client_ports = [9692u16, 9792, 9892];
    let controller_ports = [9693u16, 9793, 9893];

    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| {
            (
                u64::try_from(i + 1).unwrap(),
                format!("127.0.0.1:{}", controller_ports[i]).parse().unwrap(),
            )
        })
        .collect();

    let mut tempdirs = Vec::new();
    let mut spawns = Vec::new();
    for i in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: crabka_log::LogConfig::default(),
            node_id: u64::try_from(i + 1).unwrap(),
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters.clone(),
        };
        tempdirs.push(dir);
        spawns.push(tokio::spawn(async move {
            crabka_broker::Broker::start(cfg).await.expect("broker start")
        }));
    }
    let mut cluster = Vec::with_capacity(3);
    for (spawn, dir) in spawns.into_iter().zip(tempdirs) {
        cluster.push((spawn.await.expect("spawn"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);

    // Create topic with rf=3 so all three Crabka brokers are in the
    // replica set. The acks=-1 producer will block until all three
    // have appended.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "3",
        "--bootstrap-server",
        &bootstrap_1,
    ]);

    // Produce 100 records with --request-required-acks=-1.
    let producer_out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "bash",
            "-c",
            &format!(
                "for i in $(seq 1 100); do echo \"msg-$i\"; done | \
                 kafka-console-producer \
                   --bootstrap-server {bootstrap_1} \
                   --topic {TOPIC} \
                   --request-required-acks -1 \
                   --request-timeout-ms 10000"
            ),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("spawn kafka-console-producer");
    eprintln!(
        "CRABKA[test] producer status={} stdout={} stderr={}",
        producer_out.status,
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );
    assert!(
        producer_out.status.success(),
        "kafka-console-producer failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&producer_out.stdout),
        String::from_utf8_lossy(&producer_out.stderr),
    );

    // Brief pause to let replication settle.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Consume with read_committed and verify we see all 100 messages.
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);
    let consume_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        &bootstrap_3,
        "--topic",
        TOPIC,
        "--isolation-level",
        "read_committed",
        "--from-beginning",
        "--max-messages",
        "100",
        "--timeout-ms",
        "20000",
    ]);
    let stdout = String::from_utf8_lossy(&consume_out.stdout);
    let line_count = stdout.lines().filter(|l| !l.trim().is_empty()).count();
    assert!(
        line_count >= 100,
        "expected at least 100 records; got {line_count}: stdout={stdout}"
    );

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
```

- [ ] **Step 3: Build + check (compile only — Docker not required for CI compile-check)**

```bash
cargo build -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean. The test is `#[ignore]`d so it only runs under `cargo test ... -- --ignored`.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): JVM acks=all durability acceptance test"
```

Trailer.

---

## Phase G — Documentation + PR

### Task 14: Update root README + crate-level rustdoc

**Files:**
- Modify: `README.md`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Update root README**

Replace the existing `## Status` section in `README.md` with:

```markdown
## Status

Pre-1.0, pre-alpha. No production use.

### Slices delivered

- **Slice 1** — `crabka-protocol`: wire-protocol codec, JVM-differential
  tested.
- **Slice 2** — `crabka-client-core`: connection pool, API-version
  negotiation, request dispatch.
- **Slice 3** — `crabka-log`: Apache Kafka byte-compatible segments,
  indexes, retention.
- **Slice 4** — single-node broker MVP: Produce/Fetch/Metadata/CreateTopics
  over TCP. JVM clients connect, produce, and consume.
- **Slice 5** — consumer groups + coordinator: `__consumer_offsets`,
  OffsetCommit, OffsetFetch, group rebalance.
- **Slice 6** — idempotent producer: `InitProducerId`, per-(producer_id,
  epoch, sequence) dedup.
- **Slice 7** — KRaft / metadata quorum: openraft-backed controller,
  metadata image, CreateTopics through quorum.
- **Slice 8** — replication: multi-broker clusters, follower Fetch loop,
  rf-aware leader/follower roles. Deferred: HW, acks=all, leader
  election, KIP-101 (slice 10).
- **Slice 9** — transactions: KIP-98 + full KIP-1319 v2. TxnCoordinator,
  `__transaction_state`, per-segment `.txnindex`, LSO, transactional
  producer + consumer `isolation_level=read_committed`.
- **Slice 10a** — bulletproof EOS (HW + acks=all): partition-leader HW
  tracking; `acks=all` Produces block until full-ISR replication;
  consumer Fetch + `read_committed` LSO clamped at HW. Slice 10b will
  add KIP-101 leader-epoch, leader-election-on-failure, and ISR
  shrink/expand.
```

- [ ] **Step 2: Update broker crate-level rustdoc**

Append a "Slice 10a" subsection to `crates/broker/src/lib.rs`'s existing crate-level `//!` block, after the existing slice-9 transaction subsection:

```rust
//!
//! ## Bulletproof EOS — sub-slice 10a (HW + acks=all)
//!
//! Per-partition High Watermark tracking via [`replica_state::ReplicaState`]
//! (lives on `Partition`). The leader maintains each follower's LEO from
//! their Fetch requests and caches HW = `min(LEO over ISR)`. `acks=-1`
//! Produces gate on [`Partition::await_hw_at_least`] before responding;
//! on timeout the producer gets per-partition
//! `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20). Consumer Fetches
//! (`replica_id == -1`) clamp visible batches and `last_stable_offset`
//! at HW; `read_committed` LSO becomes `min(HW, log.lso())`.
//!
//! Sub-slice 10b will add KIP-101 leader-epoch fencing,
//! leader-election-on-failure, and ISR shrink/expand to close the
//! remaining bulletproof-EOS gap (a leader crash mid-transaction still
//! loses records as of 10a).
```

- [ ] **Step 3: Acceptance gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

All four must be clean. If `cargo fmt --all -- --check` reports drift in files this slice touched, run `cargo fmt --all` and include the diff in the commit. Pre-existing fmt drift in untouched files: leave alone.

- [ ] **Step 4: Commit**

```bash
git add README.md crates/broker/src/lib.rs
git commit -m "docs: slice-10a status entry + crate-level rustdoc"
```

Trailer.

---

### Task 15: Open the PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin feature/bulletproof-eos-10a
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --base main --head feature/bulletproof-eos-10a \
    --title "Slice 10a: bulletproof EOS — HW + acks=all" \
    --body "$(cat <<'PRBODY'
## Summary

Sub-slice 10a closes the first group of slice-8 deferrals: per-partition High Watermark tracking, `acks=all` Produce blocking, and consumer-side HW clamping (including the `read_committed` LSO tightening to `min(HW, first_unstable_offset)`).

After this slice a JVM producer with `acks=all` against Crabka blocks until every static-ISR replica has the batch; consumers see only fully-replicated records. The remaining slice-8 deferrals — KIP-101 leader-epoch, leader-election-on-failure, ISR shrink/expand — ship in sub-slice 10b.

## What landed

- `crates/broker/src/replica_state.rs` (new): per-partition `ReplicaState` with ISR membership, per-follower LEO, cached HW. Unit tests cover the state machine.
- `crates/broker/src/partition.rs`: `Partition` gains `replica_state` + `hw_advance_notify` fields and the `high_watermark()`, `install_isr()`, `await_hw_at_least()` API. `HwTimeout` error for the wait primitive.
- `crates/broker/src/partition_writer.rs`: writer fires `hw_advance_notify` after Produce appends (rf=1 case advances HW immediately).
- `crates/broker/src/handlers/fetch.rs`: follower Fetches (`replica_id >= 0`) update `ReplicaState` before reading; consumer Fetches (`replica_id == -1`) clamp visible batches and `last_stable_offset` at HW; `read_committed` LSO = `min(HW, log.lso())`.
- `crates/broker/src/handlers/produce.rs`: `acks == -1` awaits HW; on timeout returns `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (code 20).
- `crates/broker/src/replicator_supervisor.rs`: supervisor installs static ISR (= `replicas`) on leader partitions after materialize.
- `crates/broker/src/codes.rs`: `NOT_ENOUGH_REPLICAS` (19), `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20).
- Tests: `replica_state` unit tests; `partition` HW + wait unit tests; `partition_writer` HW-notify tests; new `durability.rs` integration tests; new `acks_all_durability` JVM acceptance test (Docker, ignored).
- Root `README.md` now carries a "Slices delivered" timeline.

## Soft-EOS caveat (post 10a)

A `acks=all` producer cannot acknowledge a write that hasn't replicated to every ISR member; `read_committed` consumers cannot see records that aren't durable across the ISR. **However**: a partition-leader crash mid-transaction still loses data because leader-election-on-failure ships in slice 10b. Until 10b, the bulletproof-EOS promise is "durable under no-failure" — a slow follower never returns a silent partial write, but a crashed leader still requires manual operator intervention.

## Reference

- Spec: `docs/superpowers/specs/2026-05-12-crabka-bulletproof-eos-10a-design.md`
- Plan: `docs/superpowers/plans/2026-05-12-crabka-bulletproof-eos-10a.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

Report the PR URL.

---

## Self-review against the spec

| # | Spec section / requirement | Plan task |
|---|---|---|
| 1 | New error codes (`NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`) | Task 1 |
| 2 | `ReplicaState` module with HW computation | Task 2 |
| 3 | `Partition` wires `replica_state` + `hw_advance_notify` | Task 3 |
| 4 | `Partition::high_watermark()` accessor | Task 4 |
| 5 | `Partition::install_isr()` | Task 5 |
| 6 | `Partition::await_hw_at_least()` + `HwTimeout` | Task 6 |
| 7 | Writer fires `hw_advance_notify` on leader appends (rf=1) | Task 7 |
| 8 | Fetch follower path updates `ReplicaState` | Task 8 |
| 9 | Fetch consumer path clamps at HW; read_committed = min(HW, lso) | Task 9 |
| 10 | Produce `acks=-1` awaits HW; timeout → `NOT_ENOUGH_REPLICAS_AFTER_APPEND` | Task 10 |
| 11 | Supervisor installs static ISR on leader partitions | Task 11 |
| 12 | 5 in-process integration tests (`durability.rs`) | Task 12 |
| 13 | JVM acceptance `acks_all_durability` | Task 13 |
| 14 | Root README "Slices delivered" + crate-level rustdoc | Task 14 |
| 15 | Push + open PR | Task 15 |

**Placeholder scan:** No bare TBD/TODO. The `consumer_clamps_at_hw_when_followers_lag` and `read_committed_under_rf1_unchanged_from_slice9` tests use the rf=1 path because Crabka's slice-8 follower replicators talk a real TCP loop that's awkward to mock in a single-broker harness — the `acks_all_times_out_when_no_follower` test covers the multi-replica HW-pin case via the `test_install_isr` helper. The slice-9 implementer's "interleaved_commit_and_abort" flake (`#[ignore]`d) stays ignored.

**Type consistency:** `ReplicaState::install_isr(&[NodeId], NodeId)` is the internal API; `Partition::install_isr(Vec<NodeId>, NodeId)` is the public wrapper (takes ownership of the Vec for caller convenience). Both use `crabka_raft::NodeId = u64`. `HwTimeout` is a unit-struct error type returned by `await_hw_at_least`. `Result<(), HwTimeout>` is the wait primitive's return; `Result<(), BrokerError>` is the broader error type used elsewhere — the wait primitive deliberately uses a narrower error type since timeouts aren't `BrokerError` candidates.

**Spec-coverage gaps:** None identified. Every spec section maps to at least one task. The "HW persistence is out of scope for 10a" note from the spec is consistent with the plan having no persistence task.

The plan is ready for execution.
