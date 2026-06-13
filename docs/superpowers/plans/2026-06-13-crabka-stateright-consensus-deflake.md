# stateright Consensus Model + Deterministic-Sync Test Infra — Implementation Plan (Phase 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `stateright` exhaustive concurrency-correctness checking that wraps the *real* KRaft `QuorumStateMachine` as a linearizable model, and replace flaky sleep/timeout-based waits in the consensus-correctness tests with deterministic event-synchronization.

**Architecture:** Two independent workstreams. **(A)** A new stateright `Model` in `crates/raft/tests/` whose state holds the real `QuorumStateMachine` per node + an in-memory log + an unordered message network; `next_state` runs the production `on_event` and checks election-safety / log-matching / leader-completeness / monotonic-HWM / linearizability across all interleavings. **(B)** Test-only watch/notify accessors on `BrokerHandle` plus `wait_*` awaiters that subscribe to the broker's existing `watch::Receiver<Arc<MetadataImage>>` / `watch::Receiver<Option<NodeId>>` / per-partition `Notify`, so tests `await` the actual state change instead of polling on a fixed `sleep`.

**Tech Stack:** Rust (edition 2024, rustc 1.96), `stateright = "=0.31.0"` (dev-dependency, edition-2021 crate — compiles cleanly into the 2024 workspace), `tokio` (`watch`, `Notify`, `time`), `assert2`.

**Spec:** `docs/superpowers/specs/2026-06-13-crabka-stateright-consensus-deflake-design.md`

---

## Resolved design decisions (from planning-time source extraction)

- **Network model = unordered set.** The KRaft core tolerates message loss, duplication, and reordering — every `Receive*` handler guards on epoch/role/monotonic-offset and is idempotent (vote grants keyed by `(role, epoch)`; `handle_fetch` overwrites the follower's progress slot; `recompute_high_watermark` takes the majority-th offset; `BeginQuorumEpoch` accepts only strictly-higher epoch). So the model represents in-flight messages as an unordered multiset and offers every one as an independently-deliverable / droppable / duplicable action. No per-link FIFO needed.
- **Timeouts are modeled as nondeterministic actions, not a clock.** Every `on_event` call passes a constant `NOW = SimInstant(0)`. The deadlines the core stores in `Role` are then constant per node (a deterministic function of the node's `election_timeout_ms`), so they do not vary across firings and do not explode the state space. "A timeout fires" is a model *choice* (an `Action`), not a clock comparison.
- **Crash model = omission (crash-stop / unreachable), not volatile-state-loss.** There is no public API to reset a `QuorumStateMachine`'s volatile `Role`. Phase 1 models a crash as the node becoming unreachable (added to a `crashed` set; deliveries to it are dropped; it is offered no timeout/append actions). `Recover` makes it reachable again. This still exercises leader-failover safety. (Volatile-loss crashes are deferred to a later phase.)
- **Symmetry reduction is deferred.** A buggy `Representative` impl can *hide* counterexamples (false PASS), and node ids are baked deep into `QuorumState.voters` / `Role::Leader.replicas`. Phase 1 keeps the space tractable with tight `within_boundary` bounds + a small config + `target_max_depth` instead. Symmetry is a later-phase optimization.

## File Structure

**Workstream A (model) — new + small src derive changes:**
- Modify `Cargo.toml` (workspace) — add `stateright` to `[workspace.dependencies]`.
- Modify `crates/raft/Cargo.toml` — add `stateright` dev-dependency.
- Modify `crates/metadata/src/voters.rs` — add `Hash` to `VoterEndpoint`, `KRaftVersionRange`, `Voter`, `VoterSet`.
- Modify `crates/raft/src/kraft/role.rs` — add `PartialEq, Eq, Hash` to `ReplicaProgress` and `Role`.
- Modify `crates/raft/src/kraft/event.rs` — add `PartialEq, Eq, Hash` to `LogEnd` and `Event`.
- Modify `crates/raft/src/kraft/action.rs` — add `Eq, Hash` to `Action`.
- Modify `crates/raft/src/kraft/types.rs` — add `Eq, Hash` to `QuorumState`.
- Modify `crates/raft/src/kraft/core.rs` — add `#[derive(Clone, Debug, PartialEq, Eq, Hash)]` to `QuorumStateMachine`.
- Create `crates/raft/tests/model/mod.rs` — the stateright `Model` (state, actions, next_state, properties, log spec).
- Create `crates/raft/tests/kraft_model.rs` — the `#[test]` entry points.

**Workstream B (de-flake) — broker hooks + test rewrites:**
- Modify `crates/broker/src/broker.rs` — test-only accessors + `wait_*` awaiters on `BrokerHandle`.
- Modify `crates/broker/tests/support/mod.rs` — rewrite `start_n_node_with` / `wait_for_all_brokers_registered` onto the awaiters.
- Modify `crates/broker/tests/quorum.rs`, `replication.rs`, `leader_election.rs`, `durability.rs`, `leader_epoch.rs` — replace sleep-poll loops.
- Modify `crates/raft/tests/single_node.rs`, `snapshot.rs` — replace `watch_leader` poll loops with `wait_for`.

## Execution batches (per CLAUDE.md — parallel where file sets are disjoint)

- **Batch A (parallel):** Task 1 (raft+metadata src derives, deps), Task 2 (broker.rs hooks), Task 3 (raft single_node.rs + snapshot.rs — independent, no new prod code).
- **Batch B (sequential, after Task 1):** Task 4 → Task 5 → Task 6 (the model; all in `crates/raft/tests/model/` + `kraft_model.rs`).
- **Batch C (after Task 2):** Task 7 (support/mod.rs awaiters).
- **Batch D (parallel, after Task 7):** Task 8 (quorum.rs), Task 9 (replication.rs), Task 10 (leader_election.rs + durability.rs), Task 11 (leader_epoch.rs).

Batches B and C/D are mutually independent (different crates) and may overlap.

---

## Task 1: stateright dependency + hashable consensus types

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`)
- Modify: `crates/raft/Cargo.toml` (`[dev-dependencies]`)
- Modify: `crates/metadata/src/voters.rs:10,18,31,40`
- Modify: `crates/raft/src/kraft/role.rs:8,14`
- Modify: `crates/raft/src/kraft/event.rs:6,12`
- Modify: `crates/raft/src/kraft/action.rs:12`
- Modify: `crates/raft/src/kraft/types.rs:54`
- Modify: `crates/raft/src/kraft/core.rs:36`

- [ ] **Step 1: Add stateright to the workspace dependency table**

In `Cargo.toml`, inside `[workspace.dependencies]`, add (place near the other test/dev deps such as `proptest`/`assert2`):

```toml
# Jepsen-style exhaustive concurrency-correctness model checking (KRaft
# consensus). Dev-dependency only — never linked into a shipped binary.
# Pinned: pre-1.0, minor bumps can be breaking.
stateright = "=0.31.0"
```

- [ ] **Step 2: Add stateright as a raft dev-dependency**

In `crates/raft/Cargo.toml`, under `[dev-dependencies]`, add:

```toml
stateright = { workspace = true }
```

- [ ] **Step 3: Add `Hash` to the metadata voter types**

In `crates/metadata/src/voters.rs`, add `Hash` to four derive lines (these already have `Eq`; `Hash` is additive and the fields — `String`, integers, `Uuid`, `Vec<VoterEndpoint>`, `BTreeMap<NodeId, Voter>` — are all `Hash`):

- Line 10: `#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]` → `#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]` (`VoterEndpoint`)
- Line 18: `#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]` → add `Hash` (`KRaftVersionRange`)
- Line 31: `#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]` → add `Hash` (`Voter`)
- Line 41 (`#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]`) → add `Hash` (`VoterSet`)

- [ ] **Step 4: Add `PartialEq, Eq, Hash` to `ReplicaProgress` and `Role`**

In `crates/raft/src/kraft/role.rs`:
- Line 8: `#[derive(Debug, Clone, Default)]` → `#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]` (`ReplicaProgress`)
- Line 14: `#[derive(Debug, Clone)]` → `#[derive(Debug, Clone, PartialEq, Eq, Hash)]` (`Role`)

- [ ] **Step 5: Add `PartialEq, Eq, Hash` to `LogEnd` and `Event`**

In `crates/raft/src/kraft/event.rs`:
- Line 6: `#[derive(Debug, Clone, Copy)]` → `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]` (`LogEnd`)
- Line 12: `#[derive(Debug, Clone)]` → `#[derive(Debug, Clone, PartialEq, Eq, Hash)]` (`Event`)

- [ ] **Step 6: Add `Eq, Hash` to `Action`**

In `crates/raft/src/kraft/action.rs` line 12: `#[derive(Debug, Clone, PartialEq)]` → `#[derive(Debug, Clone, PartialEq, Eq, Hash)]` (`Action`). The `&'static str` in `TransitionedTo` implements `Hash`/`Eq`, so this is safe.

- [ ] **Step 7: Add `Eq, Hash` to `QuorumState`**

In `crates/raft/src/kraft/types.rs` line 54: `#[derive(Debug, Clone, PartialEq)]` → `#[derive(Debug, Clone, PartialEq, Eq, Hash)]` (`QuorumState`). Depends on Step 3 (`VoterSet: Hash`).

- [ ] **Step 8: Make `QuorumStateMachine` clonable + hashable**

In `crates/raft/src/kraft/core.rs` line 36, add a derive above the struct (it currently has none):

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QuorumStateMachine {
    me: NodeId,
    state: QuorumState,
    role: Role,
    /// Base election timeout in ms; callers vary it per node for liveness.
    election_timeout_ms: u64,
}
```

- [ ] **Step 9: Build and fix any remaining derive gaps**

Run: `cargo build -p crabka-raft`
Expected: PASS. If the compiler reports any *other* nested type still missing `Eq`/`Hash` (e.g. a field type not listed above), add the same `Eq, Hash` derive at that type's definition. Do not add `#[derive]` to types containing `f64`/`HashMap` — none are expected here.

- [ ] **Step 10: Build the whole workspace to confirm nothing downstream broke**

Run: `cargo build --workspace`
Expected: PASS (the new derives are purely additive).

- [ ] **Step 11: Commit**

```bash
git add Cargo.toml crates/raft/Cargo.toml crates/metadata/src/voters.rs crates/raft/src/kraft/
git commit -m "feat(raft): add stateright dev-dep and Eq/Hash derives on consensus types"
```

---

## Task 2: BrokerHandle test-only watch/notify hooks + `wait_*` awaiters

**Files:**
- Modify: `crates/broker/src/broker.rs` (add methods to the `impl BrokerHandle` block, near the existing `*_for_test` accessors around line 222–727)

Context: `BrokerHandle` holds `_broker: Arc<Broker>`; `_broker.controller: Arc<dyn MetadataSource>` exposes `watch_image()`/`watch_leader()`/`current_image()`; `_broker.partitions: Arc<PartitionRegistry>` has `get(topic, partition) -> Option<Arc<Partition>>`; `Partition` has public `append_notify: Arc<Notify>` and `log_end_offset()`.

- [ ] **Step 1: Add the watch + partition accessors and `wait_*` awaiters**

Add this block inside `impl BrokerHandle` in `crates/broker/src/broker.rs` (use whatever `MetadataImage` / `NodeId` / `watch` / `Duration` / `Notify` paths already resolve in that file; the snippet uses fully-qualified paths to be safe):

```rust
/// Test-only: subscribe to the controller's metadata-image watch channel.
#[doc(hidden)]
pub fn watch_image_for_test(
    &self,
) -> tokio::sync::watch::Receiver<std::sync::Arc<crabka_metadata::MetadataImage>> {
    self._broker.controller.watch_image()
}

/// Test-only: subscribe to the controller's leader watch channel.
#[doc(hidden)]
pub fn watch_leader_for_test(
    &self,
) -> tokio::sync::watch::Receiver<Option<crabka_raft::NodeId>> {
    self._broker.controller.watch_leader()
}

/// Test-only: borrow the local `Partition` (for its `append_notify` / LEO).
#[doc(hidden)]
pub fn partition_for_test(
    &self,
    topic: &str,
    partition: i32,
) -> Option<std::sync::Arc<crate::partition::Partition>> {
    self._broker.partitions.get(topic, partition)
}

/// Test-only: await until `pred` holds for the controller metadata image.
/// Subscribes to the image watch channel and `.await`s changes — no polling
/// sleep. Bounded by a 30s safety-net so a stuck condition fails loudly.
#[doc(hidden)]
pub async fn wait_for_image<F>(&self, pred: F)
where
    F: Fn(&crabka_metadata::MetadataImage) -> bool,
{
    let mut rx = self._broker.controller.watch_image();
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // Scope the borrow so it is dropped before the await.
            if pred(&rx.borrow_and_update()) {
                return;
            }
            if rx.changed().await.is_err() {
                return; // sender dropped (broker shutting down)
            }
        }
    })
    .await;
    assert!(res.is_ok(), "wait_for_image timed out after 30s");
}

/// Test-only: await until a non-zero controller leader is elected.
#[doc(hidden)]
pub async fn wait_until_controller_leader(&self) -> crabka_raft::NodeId {
    let mut rx = self.watch_leader_for_test();
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Some(id) = *rx.borrow_and_update() {
                if id != 0 {
                    return id;
                }
            }
            if rx.changed().await.is_err() {
                return 0;
            }
        }
    })
    .await;
    let id = res.expect("wait_until_controller_leader timed out after 30s");
    assert!(id != 0, "leader channel closed before a leader was elected");
    id
}

/// Test-only: await until this node's metadata image sees `>= n` brokers.
#[doc(hidden)]
pub async fn wait_until_brokers_registered(&self, n: usize) {
    self.wait_for_image(|img| img.brokers().count() >= n).await;
}

/// Test-only: await until `topic-partition` is present in the metadata image.
#[doc(hidden)]
pub async fn wait_until_partition_present(&self, topic: &str, partition: i32) {
    self.wait_for_image(|img| img.partition(topic, partition).is_some())
        .await;
}

/// Test-only: await until `topic-partition`'s leader equals `leader` with a
/// non-zero leader epoch (i.e. a real election outcome).
#[doc(hidden)]
pub async fn wait_until_partition_leader_is(&self, topic: &str, partition: i32, leader: u64) {
    self.wait_for_image(|img| {
        img.partition(topic, partition)
            .is_some_and(|p| p.leader == leader && p.leader_epoch > 0)
    })
    .await;
}

/// Test-only: await until `topic-partition`'s leader is some non-`exclude`
/// node with a non-zero epoch. Returns the elected leader id.
#[doc(hidden)]
pub async fn wait_until_partition_leader_changed(
    &self,
    topic: &str,
    partition: i32,
    exclude: u64,
) {
    self.wait_for_image(|img| {
        img.partition(topic, partition).is_some_and(|p| {
            p.leader != 0 && p.leader != exclude && p.leader_epoch > 0
        })
    })
    .await;
}

/// Test-only: await until `topic-partition`'s ISR has exactly `len` members.
#[doc(hidden)]
pub async fn wait_until_isr_len(&self, topic: &str, partition: i32, len: usize) {
    self.wait_for_image(|img| {
        img.partition(topic, partition).is_some_and(|p| p.isr.len() == len)
    })
    .await;
}

/// Test-only: await until the LOCAL log for `topic-partition` reaches
/// `log_end_offset >= min`. Uses the partition's `append_notify`; if the
/// partition has not yet materialized locally, awaits a metadata image change
/// and retries. The `notified()` future is created BEFORE the offset check to
/// avoid a lost wakeup.
#[doc(hidden)]
pub async fn wait_until_local_log_end_offset(&self, topic: &str, partition: i32, min: i64) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            if let Some(part) = self._broker.partitions.get(topic, partition) {
                let notified = part.append_notify.notified();
                if part.log_end_offset() >= min {
                    return;
                }
                notified.await;
            } else {
                let mut img = self._broker.controller.watch_image();
                if img.changed().await.is_err() {
                    return;
                }
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "local log_end_offset({topic}-{partition}) did not reach {min} within 30s"
    );
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p crabka-broker --tests`
Expected: PASS. If `crate::partition::Partition` is not the correct path, find it with `grep -rn "pub struct Partition" crates/broker/src` and adjust; if `MetadataImage::partition` returns a type whose fields differ from `leader`/`leader_epoch`/`isr`, confirm field names with `grep -n "pub leader\|pub leader_epoch\|pub isr" crates/metadata/src/partition.rs` and adjust the closures (these names were verified against the existing `partition_leader_for_test`/`partition_isr_for_test` accessors which read `p.leader` and `p.isr`).

- [ ] **Step 3: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): add test-only watch/notify hooks and wait_* awaiters to BrokerHandle"
```

---

## Task 3: De-flake `single_node.rs` + `snapshot.rs` (raft tests, no new prod code)

**Files:**
- Modify: `crates/raft/tests/single_node.rs:26-36,59-69`
- Modify: `crates/raft/tests/snapshot.rs:22-33`

These poll `controller.watch_leader().borrow().is_some()`. `controller` is a `ControllerHandle` whose `watch_leader()` returns a `tokio::sync::watch::Receiver<Option<NodeId>>`. Replace the poll loop with `watch::Receiver::wait_for`, which resolves the instant the predicate holds.

- [ ] **Step 1: Replace the two poll loops in `single_node.rs`**

In `crates/raft/tests/single_node.rs`, replace each of the two identical blocks (lines 26-36 and 59-69):

```rust
    let deadline = std::time::Instant::now() + Duration::from_mins(2);
    loop {
        if controller.watch_leader().borrow().is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "no leader elected within 2 min"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
```

with:

```rust
    tokio::time::timeout(
        Duration::from_secs(30),
        controller.watch_leader().wait_for(|l| l.is_some()),
    )
    .await
    .expect("no leader elected within 30s")
    .expect("leader watch channel closed");
```

- [ ] **Step 2: Replace the poll loop in `snapshot.rs`**

In `crates/raft/tests/snapshot.rs`, the `wait_for_leader` helper body (lines 22-33). Replace the `loop { ... sleep ... }` with:

```rust
    tokio::time::timeout(
        Duration::from_secs(30),
        controller.watch_leader().wait_for(|l| l.is_some()),
    )
    .await
    .expect("no leader elected within 30s")
    .expect("leader watch channel closed");
```

(The function returns `()`; `wait_for` returns the borrowed value which we discard.)

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-raft --test single_node --test snapshot`
Expected: PASS. If `Duration::from_mins` import is now unused, remove it; ensure `Duration` is still imported (`use std::time::Duration;`).

- [ ] **Step 4: Run repeatedly to confirm no flake**

Run (PowerShell): `1..20 | ForEach-Object { cargo test -p crabka-raft --test single_node --test snapshot -q }`
Expected: 20/20 PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/tests/single_node.rs crates/raft/tests/snapshot.rs
git commit -m "test(raft): replace leader-election sleep-polls with watch::wait_for"
```

---

## Task 4: stateright model scaffolding — election safety + witness

**Files:**
- Create: `crates/raft/tests/model/mod.rs`
- Create: `crates/raft/tests/kraft_model.rs`

This task builds the model skeleton with vote/begin-quorum/fetch translation (ported from the proven `sim_harness` `apply_action`), election timeouts as actions, and the first two properties: `sometimes("leader_elected")` (anti-vacuity witness) and `always("election_safety")`. No client appends / linearizability / faults yet (those are Tasks 5–6).

- [ ] **Step 1: Write the model module**

Create `crates/raft/tests/model/mod.rs`:

```rust
//! Stateright model of the KIP-595/996 KRaft consensus core. The model state
//! holds the REAL `QuorumStateMachine` per node plus an in-memory log and an
//! unordered message network; `next_state` runs the production `on_event` and
//! the checker explores every interleaving. Faults (loss/dup/crash) and the
//! linearizability tester are layered in by later tasks.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use crabka_raft::kraft::action::{Action, TimerKind};
use crabka_raft::kraft::event::{Event, LogEnd};
use crabka_raft::kraft::role::Role;
use crabka_raft::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, SimInstant};
use crabka_raft::kraft::QuorumStateMachine;
use stateright::{Model, Property};

/// Constant logical time. Timeouts are modeled as nondeterministic actions, so
/// the core never needs a varying clock; constant `now` keeps role deadlines
/// constant and the state space finite.
const NOW: SimInstant = SimInstant(0);

/// In-memory replicated log: `epochs[i]` is the leader epoch of offset `i`.
/// (A self-contained copy of the sim-harness `SimLog`, made `Eq + Hash` so it
/// can live in fingerprinted model state.)
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct ModelLog {
    epochs: Vec<LeaderEpoch>,
}

impl ModelLog {
    fn append_in_epoch(&mut self, epoch: LeaderEpoch, count: usize) {
        for _ in 0..count {
            self.epochs.push(epoch);
        }
    }
    fn truncate_to(&mut self, offset: i64) {
        let offset = usize::try_from(offset.max(0)).unwrap_or(usize::MAX);
        if offset < self.epochs.len() {
            self.epochs.truncate(offset);
        }
    }
    fn replicate_from(&mut self, leader: &ModelLog) {
        if self.epochs.len() < leader.epochs.len() {
            self.epochs.clone_from(&leader.epochs);
        }
    }
    fn log_end(&self) -> LogEnd {
        LogEnd {
            last_epoch: self.last_epoch(),
            last_offset: self.end_offset(),
        }
    }
}

impl LogView for ModelLog {
    fn end_offset(&self) -> i64 {
        i64::try_from(self.epochs.len()).expect("log length fits in i64")
    }
    fn last_epoch(&self) -> LeaderEpoch {
        self.epochs.last().copied().unwrap_or(0)
    }
    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
        if epoch > self.last_epoch() {
            return None;
        }
        for (i, &e) in self.epochs.iter().enumerate() {
            if e > epoch {
                return Some(i64::try_from(i).expect("offset fits in i64"));
            }
        }
        Some(self.end_offset())
    }
}

/// One node: the real consensus machine + its log + its committed high-watermark.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeModel {
    pub machine: QuorumStateMachine,
    pub log: ModelLog,
    pub high_watermark: i64,
}

/// An in-flight message. The network is an unordered multiset of these.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Envelope {
    pub src: NodeId,
    pub dst: NodeId,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelState {
    pub nodes: BTreeMap<NodeId, NodeModel>,
    /// Unordered in-flight messages. `BTreeSet` => deterministic Hash/Eq and
    /// identical duplicate envelopes collapse to one (we model duplication via
    /// an explicit `DuplicateDeliver` action instead).
    pub network: BTreeSet<Envelope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ModelAction {
    Deliver(Envelope),
    Timeout(NodeId, TimerKind),
}

pub struct ConsensusModel {
    pub voter_ids: Vec<NodeId>,
}

impl ConsensusModel {
    pub fn new(voter_ids: &[NodeId]) -> Self {
        Self {
            voter_ids: voter_ids.to_vec(),
        }
    }

    fn election_timeout_ms_of(id: NodeId) -> u64 {
        1000 + id * 50
    }

    fn voter_set(&self) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(self.voter_ids.iter().map(|&id| {
            crabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: Vec::new(),
                kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }

    /// Translate one `Action` emitted by `id` into mutations of `state`
    /// (network envelopes / log / HWM). Ported from sim_harness `apply_action`,
    /// minus the timer arming (timeouts are model actions here).
    fn apply_action(&self, state: &mut ModelState, id: NodeId, action: Action) {
        match action {
            Action::SendVoteRequest { epoch, pre_vote } => {
                let cand_log = state.nodes[&id].log.log_end();
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveVoteRequest {
                                from: id,
                                voter_id: peer,
                                candidate_epoch: epoch,
                                candidate: id,
                                candidate_log_end: cand_log,
                                pre_vote,
                            },
                        });
                    }
                }
            }
            Action::ReplyVote { to, epoch, granted } => {
                state.network.insert(Envelope {
                    src: id,
                    dst: to,
                    event: Event::ReceiveVoteResponse {
                        from: id,
                        epoch,
                        vote_granted: granted,
                    },
                });
            }
            Action::SendBeginQuorumEpoch { epoch } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveBeginQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        });
                    }
                }
            }
            Action::SendEndQuorumEpoch { epoch } => {
                for &peer in &self.voter_ids {
                    if peer != id {
                        state.network.insert(Envelope {
                            src: id,
                            dst: peer,
                            event: Event::ReceiveEndQuorumEpoch {
                                leader_id: id,
                                leader_epoch: epoch,
                            },
                        });
                    }
                }
            }
            Action::SendFetch { leader_id } => {
                // Replicate any missing entries from the leader, then fetch at
                // the follower's (now-advanced) tip so the leader can advance HWM.
                if leader_id != id
                    && state
                        .nodes
                        .get(&leader_id)
                        .is_some_and(|n| n.machine.role().is_leader())
                {
                    let leader_log = state.nodes[&leader_id].log.clone();
                    let leader_hwm = node_high_watermark(&state.nodes[&leader_id]);
                    let f = state.nodes.get_mut(&id).expect("fetcher exists");
                    f.log.replicate_from(&leader_log);
                    f.high_watermark = leader_hwm.min(f.log.end_offset());
                }
                let (fetch_epoch, fetch_offset) = {
                    let log = &state.nodes[&id].log;
                    (log.last_epoch(), log.end_offset())
                };
                state.network.insert(Envelope {
                    src: id,
                    dst: leader_id,
                    event: Event::ReceiveFetch {
                        from: id,
                        fetch_epoch,
                        fetch_offset,
                    },
                });
            }
            Action::AppendLeaderChange { epoch } => {
                state
                    .nodes
                    .get_mut(&id)
                    .expect("appender exists")
                    .log
                    .append_in_epoch(epoch, 1);
            }
            Action::AdvanceHighWatermark(hwm) => {
                let node = state.nodes.get_mut(&id).expect("leader exists");
                node.high_watermark = hwm;
                // The HWM rides the leader's fetch responses to followers; mirror
                // that by pushing the committed boundary to peers, clamped to
                // what each has actually replicated.
                let peers: Vec<NodeId> = self.voter_ids.iter().copied().filter(|&p| p != id).collect();
                for peer in peers {
                    if let Some(p) = state.nodes.get_mut(&peer) {
                        p.high_watermark = hwm.min(p.log.end_offset());
                    }
                }
            }
            Action::TruncateTo(point) => {
                state
                    .nodes
                    .get_mut(&id)
                    .expect("truncator exists")
                    .log
                    .truncate_to(point.offset);
            }
            // Timer arming is modeled by the `Timeout` action set; durable-state
            // + role-transition signals have no cross-node effect in the model.
            Action::ResetTimer { .. } | Action::TransitionedTo(_) | Action::PersistQuorumState => {}
        }
    }

    /// Deliver `event` to `dst`: run the real machine and translate emitted
    /// actions. Also synthesizes the leader's fetch RESPONSE (the core emits
    /// HWM/Truncate, not a response message) — ported from sim_harness `step`.
    fn step(&self, state: &mut ModelState, dst: NodeId, event: Event) {
        let fetch_from = if let Event::ReceiveFetch { from, .. } = &event {
            Some(*from)
        } else {
            None
        };
        let actions = {
            let node = state.nodes.get_mut(&dst).expect("dst exists");
            node.machine.on_event(event, &node.log, NOW)
        };
        if let Some(follower) = fetch_from {
            let diverging = actions.iter().find_map(|a| match a {
                Action::TruncateTo(point) => Some(*point),
                _ => None,
            });
            if state.nodes[&dst].machine.role().is_leader() && state.nodes.contains_key(&follower) {
                let leader_epoch = state.nodes[&dst].machine.quorum_state().leader_epoch;
                let leader_end = state.nodes[&dst].log.end_offset();
                let follower_end = state.nodes[&follower].log.end_offset();
                if diverging.is_some() || follower_end < leader_end {
                    state.network.insert(Envelope {
                        src: dst,
                        dst: follower,
                        event: Event::ReceiveFetchResponse {
                            leader_id: dst,
                            leader_epoch,
                            diverging,
                        },
                    });
                }
            }
        }
        for action in actions {
            // A leader-side TruncateTo while serving a fetch is a hint for the
            // FOLLOWER (carried in the response's `diverging`), not the leader.
            if fetch_from.is_some() && matches!(action, Action::TruncateTo(_)) {
                continue;
            }
            self.apply_action(state, dst, action);
        }
    }
}

/// High watermark of a node regardless of role.
fn node_high_watermark(n: &NodeModel) -> i64 {
    match n.machine.role() {
        Role::Leader { high_watermark, .. } => *high_watermark,
        _ => n.high_watermark,
    }
}

/// True iff `n` currently believes itself leader.
fn is_leader(n: &NodeModel) -> bool {
    n.machine.role().is_leader()
}

impl Model for ConsensusModel {
    type State = ModelState;
    type Action = ModelAction;

    fn init_states(&self) -> Vec<Self::State> {
        let voters = self.voter_set();
        let mut nodes = BTreeMap::new();
        for &id in &self.voter_ids {
            let machine = QuorumStateMachine::new(
                id,
                QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone()),
                Self::election_timeout_ms_of(id),
            );
            nodes.insert(
                id,
                NodeModel {
                    machine,
                    log: ModelLog::default(),
                    high_watermark: 0,
                },
            );
        }
        vec![ModelState {
            nodes,
            network: BTreeSet::new(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every in-flight message is independently deliverable (unordered net).
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
        }
        // Any voter that is not currently leader may suffer an election timeout;
        // any follower/observer may suffer a fetch timeout. The core ignores
        // inapplicable ones, so over-offering is sound (it only adds interleavings).
        for (&id, node) in &state.nodes {
            match node.machine.role() {
                Role::Leader { .. } => {}
                Role::Follower { .. } | Role::Observer { .. } => {
                    actions.push(ModelAction::Timeout(id, TimerKind::Fetch));
                    actions.push(ModelAction::Timeout(id, TimerKind::Election));
                }
                _ => actions.push(ModelAction::Timeout(id, TimerKind::Election)),
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                self.step(&mut state, env.dst, env.event);
            }
            ModelAction::Timeout(id, kind) => {
                let event = match kind {
                    TimerKind::Election => Event::ElectionTimeout,
                    TimerKind::Fetch => Event::FetchTimeout,
                };
                self.step(&mut state, id, event);
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Anti-vacuity witness: a leader is actually elected in some state.
            Property::sometimes("leader_elected", |_, s| {
                s.nodes.values().any(is_leader)
            }),
            // Safety: at most one leader per leader-epoch.
            Property::always("election_safety", |_, s| {
                let mut by_epoch: BTreeMap<LeaderEpoch, NodeId> = BTreeMap::new();
                for (&id, n) in &s.nodes {
                    if is_leader(n) {
                        let epoch = n.machine.quorum_state().leader_epoch;
                        if let Some(&other) = by_epoch.get(&epoch) {
                            if other != id {
                                return false;
                            }
                        }
                        by_epoch.insert(epoch, id);
                    }
                }
                true
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        // Bound the space: cap in-flight messages and the maximum leader epoch.
        const MAX_INFLIGHT: usize = 12;
        const MAX_EPOCH: LeaderEpoch = 6;
        state.network.len() <= MAX_INFLIGHT
            && state
                .nodes
                .values()
                .all(|n| n.machine.quorum_state().leader_epoch <= MAX_EPOCH)
    }
}
```

- [ ] **Step 2: Write the test entry point**

Create `crates/raft/tests/kraft_model.rs`:

```rust
//! Exhaustive stateright checks of the KRaft consensus core. See `model/mod.rs`.
mod model;

use model::ConsensusModel;
use stateright::{Checker, Model};

#[test]
fn three_voters_election_safety() {
    let checker = ConsensusModel::new(&[1, 2, 3])
        .checker()
        .target_max_depth(20)
        .spawn_bfs()
        .join();
    checker.assert_properties();
}
```

- [ ] **Step 3: Run the model check**

Run: `cargo test -p crabka-raft --test kraft_model -- --nocapture`
Expected: PASS. The `sometimes("leader_elected")` witness proves a leader is reachable (not a dead model); `always("election_safety")` holds across all explored interleavings.

If it FAILS with a `sometimes` "never found" error, the model is dead/over-pruned — most likely `init_states` arms no timeout so no election starts. Confirm `actions()` offers `Timeout(_, Election)` from the initial `Role::default()` (which is `Unattached`/`Voted`-family, hitting the `_ =>` arm). If it FAILS with an `election_safety` counterexample, inspect the printed `Path` — this is either a real bug or a model-translation error (compare the failing interleaving against `sim_harness` semantics).

If it does not terminate quickly, lower `target_max_depth` to `12` and re-run to confirm correctness, then raise back.

- [ ] **Step 4: Commit**

```bash
git add crates/raft/tests/model/mod.rs crates/raft/tests/kraft_model.rs
git commit -m "test(raft): stateright model scaffolding — election safety + witness"
```

---

## Task 5: Client appends + linearizability of the committed log

**Files:**
- Modify: `crates/raft/tests/model/mod.rs`
- Modify: `crates/raft/tests/kraft_model.rs`

Adds a `ClientAppend` action (a value appended at the current leader), a `KraftLog` `SequentialSpec` reference object, a `LinearizabilityTester` carried in the model state, and an `always("linearizable")` property. **This is the part most likely to need iteration** — verify with the anti-vacuity step.

- [ ] **Step 1: Add imports and the log sequential-spec to `model/mod.rs`**

At the top of `crates/raft/tests/model/mod.rs`, extend the `stateright` import and add the `semantics` import:

```rust
use stateright::semantics::{LinearizabilityTester, SequentialSpec};
use stateright::{Model, Property};
```

Then add the reference object (an append-only committed log keyed by client value):

```rust
/// Sequential spec of the committed log: appends return the assigned offset; a
/// read returns the committed value sequence. The linearization point of an
/// append is when the value enters the committed prefix (HWM passes it).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct KraftLogSpec {
    committed: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogOp {
    Append(u64),
    Read,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogRet {
    Appended(usize),
    Value(Vec<u64>),
}

impl SequentialSpec for KraftLogSpec {
    type Op = LogOp;
    type Ret = LogRet;
    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            LogOp::Append(v) => {
                self.committed.push(*v);
                LogRet::Appended(self.committed.len() - 1)
            }
            LogOp::Read => LogRet::Value(self.committed.clone()),
        }
    }
}

pub type ClientId = u64;
```

- [ ] **Step 2: Carry the linearizability tester + pending appends in `ModelState`**

Change `ModelState` to:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ModelState {
    pub nodes: BTreeMap<NodeId, NodeModel>,
    pub network: BTreeSet<Envelope>,
    /// Linearizability auxiliary state, recomputed + fingerprinted per state.
    pub linz: LinearizabilityTester<ClientId, KraftLogSpec>,
    /// Values appended by clients but not yet observed committed, keyed by the
    /// (leader-assigned) offset they were written at. When a node's HWM passes
    /// an offset, that append's `on_return` is recorded.
    pub pending: BTreeMap<i64, (ClientId, u64)>,
    /// Total client appends issued (bounded by `within_boundary`).
    pub appends_issued: u32,
}
```

Update `init_states` to construct the new fields:

```rust
        vec![ModelState {
            nodes,
            network: BTreeSet::new(),
            linz: LinearizabilityTester::new(KraftLogSpec::default()),
            pending: BTreeMap::new(),
            appends_issued: 0,
        }]
```

- [ ] **Step 3: Add the `ClientAppend` action**

Extend `ModelAction`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ModelAction {
    Deliver(Envelope),
    Timeout(NodeId, TimerKind),
    /// A client appends `value` (via `client`) to the single current leader.
    ClientAppend(ClientId, u64),
}
```

In `actions()`, offer a client append only when exactly one leader exists (so the target is unambiguous) and the append budget is not exhausted:

```rust
        let leaders: Vec<NodeId> = state.nodes.iter().filter(|(_, n)| is_leader(n)).map(|(&id, _)| id).collect();
        if leaders.len() == 1 && state.appends_issued < MAX_APPENDS {
            let client = state.appends_issued as ClientId + 1;
            let value = state.appends_issued as u64 + 1;
            actions.push(ModelAction::ClientAppend(client, value));
        }
```

Add the constant near the top of the `impl Model` block or module: `const MAX_APPENDS: u32 = 2;`

- [ ] **Step 4: Handle `ClientAppend` and committed `on_return` in `next_state`**

Add a match arm in `next_state`:

```rust
            ModelAction::ClientAppend(client, value) => {
                let leader = state
                    .nodes
                    .iter()
                    .find(|(_, n)| is_leader(n))
                    .map(|(&id, _)| id)?;
                let epoch = state.nodes[&leader].machine.quorum_state().leader_epoch;
                let offset = state.nodes[&leader].log.end_offset();
                // Record the invocation, append at the leader, track until committed.
                state
                    .linz
                    .on_invoke(client, LogOp::Append(value))
                    .expect("on_invoke");
                state.nodes.get_mut(&leader).expect("leader").log.append_in_epoch(epoch, 1);
                state.pending.insert(offset, (client, value));
                state.appends_issued += 1;
            }
```

Then, after EVERY transition (delivery/timeout/append), settle any pending appends that have become committed. Add a helper and call it at the end of `next_state` just before `Some(state)`:

```rust
        settle_committed(&mut state);
        Some(state)
```

and define it at module scope:

```rust
/// Record `on_return` for any pending append whose offset is now committed on
/// the leader (HWM passed it). Committed offsets are returned in order.
fn settle_committed(state: &mut ModelState) {
    let max_hwm = state.nodes.values().map(node_high_watermark).max().unwrap_or(0);
    let ready: Vec<i64> = state.pending.range(..max_hwm).map(|(&off, _)| off).collect();
    for off in ready {
        let (client, _value) = state.pending.remove(&off).expect("pending entry");
        state
            .linz
            .on_return(client, LogRet::Appended(usize::try_from(off).unwrap_or(0)))
            .expect("on_return");
    }
}
```

- [ ] **Step 5: Add the linearizability property**

In `properties()`, add (alongside the existing two):

```rust
            Property::always("linearizable", |_, s| s.linz.serialized_history().is_some()),
            Property::sometimes("entry_committed", |_, s| {
                s.nodes.values().map(node_high_watermark).max().unwrap_or(0) > 0
            }),
```

- [ ] **Step 6: Bound the appends in `within_boundary`**

The `appends_issued < MAX_APPENDS` gate already bounds new appends; no `within_boundary` change is required, but confirm `MAX_APPENDS` (2) and `MAX_INFLIGHT` (12) keep the run tractable.

- [ ] **Step 7: Run the model check**

Run: `cargo test -p crabka-raft --test kraft_model -- --nocapture`
Expected: PASS — `linearizable` holds in every reachable state, and `entry_committed` witnesses that commits actually happen (else the linearizability check would be vacuous).

If `entry_committed` is "never found", appends are never committing — check that `Action::AdvanceHighWatermark` is reached (it requires a follower fetch round; ensure `SendFetch`-driven replication + the synthesized fetch response let the leader advance HWM). If `linearizable` fails, print the `Path`; a genuine failure means the committed order diverges across nodes — inspect whether `settle_committed` is recording returns out of offset order (it iterates `range(..max_hwm)` ascending, which is correct).

- [ ] **Step 8: Commit**

```bash
git add crates/raft/tests/model/mod.rs crates/raft/tests/kraft_model.rs
git commit -m "test(raft): model client appends + committed-log linearizability"
```

---

## Task 6: Fault injection + full safety suite + anti-vacuity proof

**Files:**
- Modify: `crates/raft/tests/model/mod.rs`
- Modify: `crates/raft/tests/kraft_model.rs`

Adds message loss + duplication + crash/recover (omission model), the remaining safety properties (log-matching, leader-completeness via committed-prefix preservation, monotonic-HWM), a partition-recovery scenario test, and an anti-vacuity test proving a deliberately-broken invariant is caught.

- [ ] **Step 1: Add a `crashed` set to `ModelState`**

Add field `pub crashed: BTreeSet<NodeId>,` to `ModelState` and initialize it to `BTreeSet::new()` in `init_states`.

- [ ] **Step 2: Add loss / duplication / crash / recover actions**

Extend `ModelAction`:

```rust
    DropMsg(Envelope),
    DuplicateDeliver(Envelope),
    Crash(NodeId),
    Recover(NodeId),
```

In `actions()`, for each in-flight envelope also offer `DropMsg` and `DuplicateDeliver`; offer `Crash` for any reachable non-crashed node (bounded) and `Recover` for any crashed node:

```rust
        for env in &state.network {
            actions.push(ModelAction::Deliver(env.clone()));
            actions.push(ModelAction::DropMsg(env.clone()));
            actions.push(ModelAction::DuplicateDeliver(env.clone()));
        }
        // Crash/recover, capped at MAX_CRASHES concurrently.
        if state.crashed.len() < MAX_CRASHES {
            for (&id, _) in &state.nodes {
                if !state.crashed.contains(&id) {
                    actions.push(ModelAction::Crash(id));
                }
            }
        }
        for &id in &state.crashed {
            actions.push(ModelAction::Recover(id));
        }
```

Add `const MAX_CRASHES: usize = 1;`. Also gate the existing `Deliver`/`Timeout`/`ClientAppend` offers so crashed nodes are skipped: in the timeout loop, add `if state.crashed.contains(&id) { continue; }`; in `Deliver`, deliveries to crashed nodes are dropped in `next_state` (next step). For `ClientAppend`, the single-leader check naturally excludes crashed leaders once they stop being leader, but also skip if the leader is crashed.

- [ ] **Step 3: Handle the new actions in `next_state`**

```rust
            ModelAction::DropMsg(env) => {
                state.network.remove(&env);
            }
            ModelAction::DuplicateDeliver(env) => {
                // Deliver a copy WITHOUT removing the original (duplication).
                if !state.network.contains(&env) {
                    return None;
                }
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event.clone());
                }
            }
            ModelAction::Crash(id) => {
                if !state.crashed.insert(id) {
                    return None;
                }
                // Omission model: drop messages to/from the crashed node.
                state.network.retain(|e| e.src != id && e.dst != id);
            }
            ModelAction::Recover(id) => {
                if !state.crashed.remove(&id) {
                    return None;
                }
            }
```

And in the `Deliver` arm, guard the crashed destination:

```rust
            ModelAction::Deliver(env) => {
                if !state.network.remove(&env) {
                    return None;
                }
                if !state.crashed.contains(&env.dst) {
                    self.step(&mut state, env.dst, env.event);
                }
            }
```

- [ ] **Step 4: Add the remaining safety properties**

Add to `properties()`:

```rust
            // Log matching: any two non-crashed logs that share an offset agree
            // on that offset's epoch up to the shorter length.
            Property::always("log_matching", |_, s| {
                let logs: Vec<&Vec<LeaderEpoch>> =
                    s.nodes.values().map(|n| &n.log.epochs).collect();
                for i in 0..logs.len() {
                    for j in (i + 1)..logs.len() {
                        let (a, b) = (logs[i], logs[j]);
                        let common = a.len().min(b.len());
                        // Raft log-matching is a prefix property: equal entries
                        // imply equal prefixes. Here entries are epochs; a
                        // divergent epoch at offset k with equal epoch at k+1 in
                        // both is impossible under correct replication.
                        for k in 0..common {
                            if a[k] != b[k] {
                                // Divergence is allowed only as an uncommitted
                                // suffix: once they re-agree at a later offset
                                // both must agree from 0..=k. Flag re-agreement
                                // after disagreement (a true matching violation).
                                if (k + 1..common).any(|m| a[m] == b[m]) {
                                    return false;
                                }
                            }
                        }
                    }
                }
                true
            }),
            // Monotonic HWM per node across the explored graph is checked by
            // construction (HWM only set via AdvanceHighWatermark/min-clamp);
            // assert no node's committed prefix exceeds its own log length.
            Property::always("hwm_within_log", |_, s| {
                s.nodes
                    .values()
                    .all(|n| node_high_watermark(n) <= n.log.end_offset())
            }),
```

> Note on `log_matching`: the epoch-vector check above is a sound approximation (it relies on the field that the in-memory `ModelLog` actually tracks — per-offset leader epoch). It catches the canonical violation: two logs that disagree at offset `k` but agree again later (which a correct Raft log can never produce, since equal entries imply equal prefixes). Leader-completeness is exercised indirectly via the `linearizable` property from Task 5 (a lost committed entry produces a non-serializable history).

- [ ] **Step 5: Run the full suite**

Run: `cargo test -p crabka-raft --test kraft_model -- --nocapture`
Expected: PASS (all of `leader_elected`, `entry_committed`, `election_safety`, `linearizable`, `log_matching`, `hwm_within_log`).

If the state space is too large (run takes more than ~30s or exhausts memory), reduce bounds first: `MAX_INFLIGHT = 8`, `MAX_APPENDS = 1`, `target_max_depth(14)`. Confirm PASS, then raise incrementally to the largest that finishes within ~30s in CI.

- [ ] **Step 6: Add a 2-node fast config and keep the 3-node as the main test**

In `crates/raft/tests/kraft_model.rs`, the `three_voters_election_safety` test is the headline. Add a smaller, faster smoke config:

```rust
#[test]
fn two_voters_smoke() {
    let checker = ConsensusModel::new(&[1, 2])
        .checker()
        .target_max_depth(16)
        .spawn_bfs()
        .join();
    checker.assert_properties();
}
```

Run: `cargo test -p crabka-raft --test kraft_model`
Expected: both PASS.

- [ ] **Step 7: Anti-vacuity proof (prove the checker catches a real bug)**

This step is a manual verification — it does NOT get committed. Temporarily break leader-completeness in the production core: in `crates/raft/src/kraft/core.rs`, find `recompute_high_watermark` and comment out the `epoch_start_offset` gate (the guard that prevents committing entries from a prior epoch before a current-epoch entry is majority-replicated).

Run: `cargo test -p crabka-raft --test kraft_model`
Expected: FAIL — the `linearizable` (and possibly `log_matching`) property reports a counterexample `Path`. This proves the model is not vacuous.

Then REVERT the change:

```bash
git checkout crates/raft/src/kraft/core.rs
```

Re-run: `cargo test -p crabka-raft --test kraft_model`
Expected: PASS again.

- [ ] **Step 8: Commit**

```bash
git add crates/raft/tests/model/mod.rs crates/raft/tests/kraft_model.rs
git commit -m "test(raft): model loss/dup/crash + full safety suite; verified non-vacuous"
```

---

## Task 7: Rewrite `support/mod.rs` cluster-startup waits onto the awaiters

**Files:**
- Modify: `crates/broker/tests/support/mod.rs:307-339` (`start_n_node_with` poll loop), `367-389` (`wait_for_all_brokers_registered`)

- [ ] **Step 1: Replace the leader-election poll loop in `start_n_node_with`**

In `crates/broker/tests/support/mod.rs`, replace the `let deadline = ...; loop { ... tokio::time::sleep(Duration::from_millis(100)).await; }` block (lines ~307-339) with a deterministic wait that races all handles for "leader elected with the full voter set":

```rust
    // Wait (bounded, event-driven) for the static set to elect a leader and for
    // some node to report the full n-voter committed set. We await each node's
    // leader watch channel; with a static set the voter count is `n` the moment
    // a leader emerges.
    let wait = async {
        let mut futs: futures_util::stream::FuturesUnordered<_> = out
            .iter()
            .map(|(h, _, _)| async move {
                h.wait_until_controller_leader().await;
            })
            .collect();
        use futures_util::StreamExt;
        let _ = futs.next().await;
    };
    if tokio::time::timeout(Duration::from_secs(30), wait).await.is_err() {
        let counts: Vec<usize> = out.iter().map(|(h, _, _)| h.voter_count_for_test()).collect();
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n_usize} voters within 30s \
             (voter counts={counts:?})"
        )));
    }
    // Confirm the elected node sees the full voter set (immediate for a static set).
    let full = out.iter().any(|(h, _, _)| h.voter_count_for_test() >= n_usize);
    assert!(full, "leader elected but voter set not yet committed to {n_usize}");
```

(`futures_util` is already a workspace dependency; add `use futures_util;` import at the top if needed, or inline the `.await` over a simple loop using `tokio::select!`. If `FuturesUnordered` proves awkward here, the equivalent is: `for (h,_,_) in &out { /* spawn */ }` racing — but the simplest correct form is to await the first broker's leader and then verify, since with a static voter set all nodes converge together:)

```rust
    // Simpler equivalent: the first broker's controller leader settling implies
    // the static quorum elected; await it, then verify the full set.
    if tokio::time::timeout(
        Duration::from_secs(30),
        out[0].0.wait_until_controller_leader(),
    )
    .await
    .is_err()
    {
        let counts: Vec<usize> = out.iter().map(|(h, _, _)| h.voter_count_for_test()).collect();
        return Err(BrokerError::Startup(format!(
            "static cluster did not elect a leader with {n_usize} voters within 30s \
             (voter counts={counts:?})"
        )));
    }
    assert!(
        out.iter().any(|(h, _, _)| h.voter_count_for_test() >= n_usize),
        "leader elected but voter set not committed to {n_usize}"
    );
```

Use the simpler form. Keep `start_n_node_with_retry`'s outer 3-attempt retry unchanged (it guards real loopback-port/election-starvation flakiness, not timing).

- [ ] **Step 2: Replace `wait_for_all_brokers_registered`**

Replace the body (lines 367-389) with:

```rust
pub async fn wait_for_all_brokers_registered(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    n: usize,
) {
    for (h, _, _) in cluster {
        h.wait_until_brokers_registered(n).await;
    }
}
```

- [ ] **Step 3: Build the broker tests**

Run: `cargo build -p crabka-broker --tests`
Expected: PASS. Remove any now-unused imports (`std::time::Instant` may still be used elsewhere in the file — only remove if the compiler warns).

- [ ] **Step 4: Run a couple of cluster tests through the new path**

Run: `cargo test -p crabka-broker --test quorum -- --test-threads=1`
Expected: PASS (these go through `start_n_node_with_retry` → the rewritten startup).

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/support/mod.rs
git commit -m "test(broker): event-driven cluster startup waits in test support"
```

---

## Task 8: De-flake `quorum.rs`

**Files:**
- Modify: `crates/broker/tests/quorum.rs:69-92` (`three_node_cluster_elects_leader`), `94-142` (`create_topic_on_any_node_propagates`), `144-210` (`leader_kill_recovers`)

- [ ] **Step 1: Rewrite `three_node_cluster_elects_leader`'s poll loop**

Replace the `let deadline = ...; loop { ... sleep(50ms) ... }` (lines ~73-89) that polls for a single non-zero leader with:

```rust
    // Each node's controller leader channel converges to the same elected id.
    for (h, _, _) in &cluster {
        h.wait_until_controller_leader().await;
    }
    let mut leaders = std::collections::HashSet::new();
    for (h, _, _) in &cluster {
        if let Some(l) = h.controller_leader_id().await {
            leaders.insert(l);
        }
    }
    assert!(
        leaders.len() == 1 && !leaders.contains(&0),
        "leader not converged: {leaders:?}"
    );
```

- [ ] **Step 2: Rewrite the topic-propagation loop in `create_topic_on_any_node_propagates`**

The loop (lines ~127-137) polls node 2's *client metadata* for topic `"prop"`. Replace it by first awaiting the metadata image on node 2's handle, then doing a single client metadata assertion:

```rust
    // Await the topic in node 2's controller image (deterministic), then the
    // client metadata reflects it immediately.
    cluster[2].0.wait_until_partition_present("prop", 0).await;
    let m = c2.send(MetadataRequest::default()).await.unwrap();
    assert!(
        m.topics.iter().any(|t| t.name.as_deref() == Some("prop")),
        "topic 'prop' not visible to node 2"
    );
```

- [ ] **Step 3: Rewrite the new-leader wait in `leader_kill_recovers`**

Replace the post-kill `loop { ... sleep(100ms) ... }` (lines ~169-184) with:

```rust
    // Survivors elect a new leader (id != killed). Await each survivor's leader
    // channel, then assert convergence to a single new leader.
    for (h, _, _) in &cluster {
        let mut rx = h.watch_leader_for_test();
        tokio::time::timeout(Duration::from_secs(30), rx.wait_for(|l| {
            matches!(l, Some(id) if *id != 0 && *id != killed_node_id)
        }))
        .await
        .expect("no new leader within 30s after kill")
        .expect("leader channel closed");
    }
    let mut leaders = std::collections::HashSet::new();
    for (h, _, _) in &cluster {
        if let Some(l) = h.controller_leader_id().await {
            leaders.insert(l);
        }
    }
    assert!(
        leaders.len() == 1 && !leaders.contains(&0) && !leaders.contains(&killed_node_id),
        "no single new leader: {leaders:?}"
    );
```

- [ ] **Step 4: Run and stress**

Run: `cargo test -p crabka-broker --test quorum -- --test-threads=1`
Expected: PASS. Then stress (PowerShell): `1..20 | ForEach-Object { cargo test -p crabka-broker --test quorum -q -- --test-threads=1 }` → 20/20 PASS. Remove unused `Instant`/`Duration::from_mins` imports if the compiler warns.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/quorum.rs
git commit -m "test(broker): de-flake quorum.rs with event-driven waits"
```

---

## Task 9: De-flake `replication.rs`

**Files:**
- Modify: `crates/broker/tests/replication.rs:52-187` (`replication_factor_three_propagates_to_all_followers`), `189-354` (`out_of_range_truncates_and_recovers`)

Both tests use three poll-loop shapes: broker-discovery (`broker_count >= 3`), topic-propagation (`has_partition`), and log-convergence (`local_log_end_offset >= N`). Replace each with the matching awaiter.

- [ ] **Step 1: Replace the broker-discovery loops (both tests)**

Replace each `let deadline=...; loop { ... broker_count().await < 3 ... sleep(50ms) }` block with:

```rust
    for (h, _, _) in &cluster {
        h.wait_until_brokers_registered(3).await;
    }
```

- [ ] **Step 2: Replace the topic-propagation loops (both tests)**

Replace each `loop { ... !h.has_partition("repl"/"oor", 0).await ... sleep(100ms) }` block with (use the correct topic name per test — `"repl"` in the first, `"oor"` in the second):

```rust
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("repl", 0).await;
    }
```

- [ ] **Step 3: Replace the log-convergence loops**

Replace each `loop { ... local_log_end_offset(...).await ... offsets.all(>= N) ... sleep(100ms) }` block with (substituting the test's topic and target — 20 then 50; and the single-broker recovery check on `cluster[2]`):

```rust
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("repl", 0, 20).await;
    }
```

For the `out_of_range` test's three convergence waits: `("oor", 0, 50)` for all brokers (initial), and the final single-broker recovery:

```rust
    cluster[2].0.wait_until_local_log_end_offset("oor", 0, 50).await;
```

- [ ] **Step 4: Run and stress**

Run: `cargo test -p crabka-broker --test replication -- --test-threads=1`
Expected: PASS. Stress 20× as in Task 8 Step 4 → 20/20 PASS. Remove unused imports if warned.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/tests/replication.rs
git commit -m "test(broker): de-flake replication.rs with event-driven waits"
```

---

## Task 10: De-flake `leader_election.rs` + `durability.rs`

**Files:**
- Modify: `crates/broker/tests/leader_election.rs:37-49,72-79,175-206,327-356,386`
- Modify: `crates/broker/tests/durability.rs:104-111,272-277,292`

- [ ] **Step 1: `leader_election.rs` — `find_controller_leader` helper**

Replace the loop (lines 37-49) with a deterministic await that returns the index of the self-identified leader:

```rust
    // Await any node reporting itself controller leader, then find its index.
    for (h, _, _) in cluster {
        h.wait_until_controller_leader().await;
    }
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await == Some(cfg.node_id) {
            return i;
        }
    }
    panic!("a leader was elected but no handle self-identifies as leader");
```

- [ ] **Step 2: `leader_election.rs` — `create_topic` materialization (lines 72-79)**

Replace the `while !broker.has_partition(name, 0).await { ... sleep(50ms) }` with:

```rust
    broker.wait_until_partition_present(name, 0).await;
```

- [ ] **Step 3: `leader_election.rs` — `broker_death_elects_new_leader` (lines 175-206)**

This polls *client metadata* until `p.leader_id != 1 && p.leader_epoch > 0`. Replace by awaiting the surviving node's controller image, then doing one client metadata read for the assertion values:

```rust
    // Await the new partition leader in the survivor's controller image.
    cluster[0].0.wait_until_partition_leader_changed("elect", 0, 1).await;
    let client = Client::builder()
        .bootstrap(cluster[0].1.listen_addr.to_string())
        .build()
        .await
        .unwrap();
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("elect".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");
    let t = resp.topics.iter().find(|t| t.name.as_deref() == Some("elect")).expect("topic");
    let p = t.partitions.first().expect("partition");
    let elected = Some((p.leader_id, p.leader_epoch));
    assert!(p.leader_id != 1 && p.leader_epoch > 0, "no new leader elected: {elected:?}");
```

(Remove the `let mut elected ...; while ... { ... sleep(200ms) }` scaffolding the new code replaces.)

- [ ] **Step 4: `leader_election.rs` — `isr_expand_on_catchup` (lines 327-356)**

Replace the `while ... p.isr_nodes.len() == 3 ... sleep(200ms)` poll with an image await + single assertion (this test is `#[ignore]`d but should still be converted):

```rust
    cluster_handle_for_expand.wait_until_isr_len("expand", 0, 3).await;
```

Identify the handle that owns the partition leader (the test uses `bootstrap_1`; use the corresponding `BrokerHandle` in `cluster`, e.g. `cluster[0].0`). If the test only has the bootstrap address, add the matching handle reference; the ISR is in the controller image visible from any node's handle, so `cluster[0].0.wait_until_isr_len("expand", 0, 3).await` is sufficient.

- [ ] **Step 5: `leader_election.rs` — fixed 4s sleep (line 386)**

`produce_during_leader_failover` has `sleep(Duration::from_secs(4))` to let failover happen. Replace with a deterministic wait for the new leader on the relevant topic/partition (use the topic this test produces to — inspect the test; if it produces to `"failover"` partition 0 after killing node 1):

```rust
    cluster[0].0.wait_until_partition_leader_changed("failover", 0, 1).await;
```

If the test's topic/partition differs, use the actual names from the surrounding code. The intent is: wait for the failover to complete, not a fixed 4s.

- [ ] **Step 6: `durability.rs` — `create_topic` materialization (lines 104-111)**

Replace the `while !broker.has_partition(name, 0).await { ... sleep(50ms) }` with:

```rust
    broker.wait_until_partition_present(name, 0).await;
```

- [ ] **Step 7: `durability.rs` — consumer poll loop (lines 272-277)**

`read_committed_under_rf1_unchanged` polls `consumer.poll(200ms)` until 3 records seen. First await the records being committed to the local log, then poll once:

```rust
    broker.wait_until_local_log_end_offset(name, 0, 3).await;
    let mut seen: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 3 && Instant::now() < deadline {
        for r in consumer.poll(Duration::from_millis(200)).await.unwrap() {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
```

(The consumer `poll` loop stays — it is the consumer API, not a fixed settle-sleep — but it now starts only after the data is known committed, so it returns promptly. Use the test's actual topic variable for `name`.)

- [ ] **Step 8: `durability.rs` — fixed 200ms sleep (line 292)**

`acks_all_completes_via_isr_shrink_when_follower_dead` sleeps 200ms "to let replicators spawn" before killing broker 3. Replace with a deterministic wait that the follower has actually started replicating (its LEO advanced past 0, or it joined ISR). Use ISR membership on the leader's image:

```rust
    // Ensure the follower (broker 3) has joined ISR before we kill it, so the
    // scenario exercises ISR shrink rather than a never-formed ISR.
    cluster[0].0.wait_until_isr_len(topic_name, 0, 3).await;
```

Use the test's topic variable. If the test produces with rf such that full ISR is 3, this is correct; adjust the expected length to the test's replication factor.

- [ ] **Step 9: Run and stress both files**

Run: `cargo test -p crabka-broker --test leader_election --test durability -- --test-threads=1`
Expected: PASS (note `isr_expand_on_catchup` is `#[ignore]`; run it explicitly with `--ignored` to verify the conversion compiles and passes: `cargo test -p crabka-broker --test leader_election -- --ignored isr_expand_on_catchup`). Stress the non-ignored set 20×.

- [ ] **Step 10: Commit**

```bash
git add crates/broker/tests/leader_election.rs crates/broker/tests/durability.rs
git commit -m "test(broker): de-flake leader_election.rs and durability.rs with event-driven waits"
```

---

## Task 11: De-flake `leader_epoch.rs`

**Files:**
- Modify: `crates/broker/tests/leader_epoch.rs:92-96,464-481,514-528,562-583`

- [ ] **Step 1: `create_topic` materialization (lines 92-96)**

Replace the `while !broker.has_partition(name, 0).await { ... sleep(50ms) }` with:

```rust
    broker.wait_until_partition_present(name, 0).await;
```

- [ ] **Step 2: Topic-propagation loop in `follower_truncates_in_band_on_diverging_epoch` (lines 464-481)**

Replace the `loop { ... !h.has_partition("divtrunc", 0).await ... sleep(100ms) }` with:

```rust
    for (h, _, _) in &cluster {
        h.wait_until_partition_present("divtrunc", 0).await;
    }
```

- [ ] **Step 3: Initial-replication convergence loop (lines 514-528)**

Replace the `loop { ... local_log_end_offset("divtrunc", 0) ... all >= k ... sleep(100ms) }` with (the test binds `k`):

```rust
    for (h, _, _) in &cluster {
        h.wait_until_local_log_end_offset("divtrunc", 0, k).await;
    }
```

- [ ] **Step 4: Divergent-suffix truncation convergence loop (lines 562-583)**

This waits for `follower` and `leader` LEOs to both equal `k`. Replace with two awaiters plus a final equality assertion:

```rust
    follower.wait_until_local_log_end_offset("divtrunc", 0, k).await;
    cluster[0].0.wait_until_local_log_end_offset("divtrunc", 0, k).await;
    let f_leo = follower.local_log_end_offset("divtrunc", 0).await.unwrap_or(-1);
    let l_leo = cluster[0].0.local_log_end_offset("divtrunc", 0).await.unwrap_or(-1);
    assert!(
        f_leo == l_leo && f_leo == k,
        "follower did not converge to leader (follower={f_leo}, leader={l_leo}, k={k})"
    );
```

Note: `wait_until_local_log_end_offset` waits for `>= k`; since the divergent suffix is truncated DOWN to `k`, confirm the follower first reached `>= k` then settled at exactly `k`. If the follower overshoots before truncating, the `>= k` wait may return early. To be robust, after both awaiters, the equality assertion above still validates the final state; if it is racy in practice, change the follower awaiter to spin on the partition's `append_notify` until `log_end_offset() == k` by adding a `wait_until_local_log_end_offset_eq` variant on `BrokerHandle` (same body as `wait_until_local_log_end_offset` but with `== min` and also waking on truncation — truncation does not fire `append_notify`, so use the metadata image leader-epoch bump as the secondary wake). If needed, add this variant in Task 2 and use it here.

- [ ] **Step 5: Run and stress**

Run: `cargo test -p crabka-broker --test leader_epoch -- --test-threads=1`
Expected: PASS. Stress 20×. If the divergent-suffix wait (Step 4) is racy, implement the `_eq` variant as noted and re-run.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/tests/leader_epoch.rs crates/broker/src/broker.rs
git commit -m "test(broker): de-flake leader_epoch.rs with event-driven waits"
```

---

## Final verification (after all tasks)

- [ ] **Run the full affected test set**

```bash
cargo test -p crabka-raft --test kraft_model --test single_node --test snapshot
cargo test -p crabka-broker --test quorum --test replication --test leader_election --test durability --test leader_epoch -- --test-threads=1
```
Expected: all PASS.

- [ ] **Stress for flakes (PowerShell, 30×)**

```powershell
1..30 | ForEach-Object {
  cargo test -p crabka-broker --test quorum --test replication --test leader_epoch -q -- --test-threads=1
  if ($LASTEXITCODE -ne 0) { Write-Error "FLAKE on run $_"; break }
}
```
Expected: 30/30 PASS.

- [ ] **Format and lint**

```bash
cargo fmt -p crabka-raft -p crabka-broker -p crabka-metadata
cargo clippy -p crabka-raft -p crabka-broker --tests -- -D warnings
```
Expected: no diffs from fmt beyond intended; clippy clean. (On this Windows worktree use per-crate `cargo fmt -p <crate>` to avoid the OS 206 path-length failure.)

- [ ] **Confirm the model is wired into the default test run**

Run: `cargo test -p crabka-raft`
Expected: `kraft_model` tests run and PASS as part of the normal suite (bounded config finishes in seconds).

## Self-review notes (addressed)

- **Spec coverage:** stateright dep (T1) ✓; wrap-real `QuorumStateMachine` model (T4) ✓; linearizability via custom `SequentialSpec` + `LinearizabilityTester` (T5) ✓; safety props election-safety/log-matching/HWM (T4,T6) ✓; loss/dup/crash as actions (T6) ✓; timeouts-as-actions + constant clock (T4) ✓; `within_boundary` bounding + CI-fast configs (T4,T6) ✓; anti-vacuity proof (T6 S7) ✓; BrokerHandle watch hooks + `wait_*` (T2) ✓; support rewrite (T7) ✓; Phase-1 de-flake batch quorum/leader_election/replication/durability/leader_epoch/single_node/snapshot (T3,T8–T11) ✓; fmt/clippy/stress verification ✓.
- **Deviations from spec (intentional, noted):** symmetry reduction deferred (risk of hiding counterexamples); crash modeled as omission not volatile-loss (no public reset API). Both documented at the top under "Resolved design decisions".
- **Type consistency:** awaiter names (`wait_until_controller_leader`, `wait_until_brokers_registered`, `wait_until_partition_present`, `wait_until_partition_leader_changed`, `wait_until_isr_len`, `wait_until_local_log_end_offset`, `wait_for_image`, `watch_leader_for_test`, `watch_image_for_test`, `partition_for_test`) are defined in Task 2 and used identically in Tasks 7–11. Model symbols (`ModelState`, `ModelAction`, `NodeModel`, `Envelope`, `ModelLog`, `KraftLogSpec`, `ConsensusModel`, `settle_committed`, `node_high_watermark`, `is_leader`, `NOW`, `MAX_*`) are defined in Task 4 and extended consistently in Tasks 5–6.
```
