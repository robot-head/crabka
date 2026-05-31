# KIP-595 Slice 3a — KRaft Consensus Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pure, deterministic, sans-IO KIP-595 + KIP-996 quorum state machine (roles, term/leader-epoch, pre-vote + real-vote election, vote granting, leader high-watermark, divergence decision) as an event/action core, validated by rule-level unit tests and an in-memory multi-node election/replication simulation.

**Architecture:** A new permanent module `crates/raft/src/kraft/` containing an `on_event(event, log_view, now) -> Vec<Action>` state machine over a `QuorumState` + `Role`. No IO, no async, no clock, no wire — time and jitter are passed in; the log is read through an injected `LogView` trait. openraft stays the live engine; this code is not wired to anything in 3a.

**Tech Stack:** Rust, `crabka_metadata::{VoterSet, Voter}` (reused), `NodeId = u64`, `uuid::Uuid`, `assert2` for tests. No new deps.

**Spec:** [docs/superpowers/specs/2026-05-31-kip595-slice3a-consensus-core-design.md](../specs/2026-05-31-kip595-slice3a-consensus-core-design.md)

---

## Background the implementer needs

- This is **pure logic**. Do not touch openraft, the controller, the wire, or `crabka-log`. Everything lives under `crates/raft/src/kraft/` and is reachable only from tests in 3a.
- Reuse `crabka_metadata::voters::{VoterSet, Voter}` for the voter set (`VoterSet::ids() -> BTreeSet<NodeId>`, `contains(id)`, `len()`). `NodeId = u64` (`crates/raft/src/types.rs:8`).
- A **`ReplicaKey`** is `(id: NodeId, directory_id: Uuid)` — Kafka identifies voters by id + directory id. Define it in `kraft/types.rs`.
- Internal ids are `NodeId = u64`; the wire uses `i32`. 3a is internal-only, so use `NodeId`/`u64` throughout; wire conversion is 3c's problem.
- Pre-vote (KIP-996): a `Vote` request carries `pre_vote: bool`. A **pre-vote** grant is non-binding — it does NOT persist `voted_key` nor change `leader_epoch`. A **standard** vote grant transitions to `Voted` and persists. Follow the KIP-595/996 algorithms below; the unit tests encode the truth table. (Exact JVM-quirk reconciliation happens in 3c against real peers — 3a's bar is internally-consistent KIP semantics that pass the simulation.)
- Time: pass `now: SimInstant` (a newtype over `u64` millis, defined in `kraft/types.rs`) into `on_event`; never call the real clock. Election jitter is supplied by the caller as a field on the machine (`election_timeout_ms` + a caller-chosen `jitter_ms`), never randomized internally.
- Determinism rule (repo-wide): no `Instant::now()`/`Date`/`rand` in this module.

## File Structure (`crates/raft/src/kraft/`)

| File | Responsibility |
|------|----------------|
| `mod.rs` | Module exports; `pub use` the public surface. |
| `types.rs` | `NodeId` alias re-export, `ReplicaKey`, `SimInstant`, `LeaderEpoch`, `QuorumState` (persistent), `LogView` trait, `LogOffsetMetadata`. |
| `role.rs` | `Role` enum + per-role state (votes-granted sets, leader fetch-offset map, timers). |
| `event.rs` | `Event` enum (inputs) + the request/response payload structs. |
| `action.rs` | `Action` enum (outputs). |
| `core.rs` | `QuorumStateMachine` struct + `on_event` dispatcher; election, vote-granting, begin/end-epoch, leader-HWM, divergence. |
| `tests/` (inline `#[cfg(test)]` in each file) | Rule-level unit tests. |
| `crates/raft/tests/kraft_sim.rs` | The deterministic multi-node simulation + acceptance tests. |
| `crates/raft/src/lib.rs` | add `mod kraft;` |

---

## Task 1: Scaffold module + core data types

**Files:** create `crates/raft/src/kraft/{mod,types,role,event,action,core}.rs`; modify `crates/raft/src/lib.rs`.

- [ ] **Step 1: Write the failing test**

In `crates/raft/src/kraft/types.rs`, append:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn quorum_state_starts_unattached_at_epoch_zero() {
        let voters = test_voter_set(&[1, 2, 3]);
        let qs = QuorumState::bootstrap(uuid::Uuid::nil(), voters.clone());
        assert!(qs.leader_epoch == 0);
        assert!(qs.leader_id.is_none());
        assert!(qs.voted_key.is_none());
        assert!(qs.voters.contains(2));
    }

    pub(crate) fn test_voter_set(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
            crabka_metadata::voters::Voter {
                id,
                directory_id: uuid::Uuid::nil(),
                endpoints: Vec::new(),
                kraft_version: crabka_metadata::voters::KRaftVersionRange::default(),
            }
        }))
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p crabka-raft kraft::types -- --nocapture`
Expected: FAIL — module/types not defined.

- [ ] **Step 3: Implement the types**

`crates/raft/src/kraft/types.rs`:

```rust
//! Core data types for the KRaft consensus state machine (KIP-595/996).
//! Pure, sans-IO: no clock, no wire, no log bytes.

use crabka_metadata::voters::VoterSet;
use uuid::Uuid;

pub use crate::types::NodeId;

/// A simulated/logical instant in milliseconds. Time is always injected, never
/// read from the system clock (keeps the state machine deterministic).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SimInstant(pub u64);

impl SimInstant {
    #[must_use]
    pub fn saturating_add_ms(self, ms: u64) -> Self {
        Self(self.0.saturating_add(ms))
    }
}

/// KRaft leader epoch (the i32 "leaderEpoch" on the wire; u32 internally is
/// fine because epochs only ever increase from 0).
pub type LeaderEpoch = u32;

/// Identifies a voter by node id + directory id (Kafka's ReplicaKey).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaKey {
    pub id: NodeId,
    pub directory_id: Uuid,
}

/// A log position: an offset together with the leader epoch that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogOffsetMetadata {
    pub offset: i64,
    pub epoch: LeaderEpoch,
}

/// Read-only view of the local replicated log the state machine reasons about.
/// 3b supplies the real `crabka-log`-backed impl; tests supply a fake.
pub trait LogView {
    /// Offset one past the last appended record (the log end offset).
    fn end_offset(&self) -> i64;
    /// Leader epoch of the last appended record (0 for an empty log).
    fn last_epoch(&self) -> LeaderEpoch;
    /// The end offset for `epoch`: the offset of the first record with a
    /// strictly greater epoch, or `end_offset()` if none. Used to compute the
    /// diverging-epoch hint. Returns `None` if `epoch` is unknown (> last).
    fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64>;
}

/// The durable quorum state — the logical content of the `quorum-state` file.
/// (File persistence itself is deferred to 3c; this is the in-memory model.)
#[derive(Debug, Clone, PartialEq)]
pub struct QuorumState {
    pub cluster_id: Uuid,
    pub leader_epoch: LeaderEpoch,
    pub leader_id: Option<NodeId>,
    pub voted_key: Option<ReplicaKey>,
    pub voters: VoterSet,
}

impl QuorumState {
    #[must_use]
    pub fn bootstrap(cluster_id: Uuid, voters: VoterSet) -> Self {
        Self { cluster_id, leader_epoch: 0, leader_id: None, voted_key: None, voters }
    }

    /// Majority size for the current voter set (`floor(n/2) + 1`).
    #[must_use]
    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }
}
```

`crates/raft/src/kraft/mod.rs`:

```rust
//! Hand-rolled KRaft consensus core (KIP-595 + KIP-996). Pure, deterministic,
//! sans-IO: an `on_event` state machine over `QuorumState` + `Role`. Not wired
//! to the controller/wire/log in slice 3a (openraft remains the live engine);
//! 3b/3c integrate it and ultimately replace openraft.

pub mod action;
pub mod core;
pub mod event;
pub mod role;
pub mod types;

pub use action::Action;
pub use core::QuorumStateMachine;
pub use event::Event;
pub use role::Role;
pub use types::{LeaderEpoch, LogOffsetMetadata, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};
```

Create `role.rs`, `event.rs`, `action.rs`, `core.rs` with a module doc line + the minimal types from Tasks 2–6 (start with `//! placeholder` and the enums defined as they're needed). Add `mod kraft;` to `crates/raft/src/lib.rs`.

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p crabka-raft kraft::types -- --nocapture`
Expected: PASS. Also `cargo build -p crabka-raft` succeeds.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft crates/raft/src/lib.rs
git commit -m "feat(raft): scaffold kraft consensus-core module + quorum-state types"
```

---

## Task 2: Roles, Events, Actions

**Files:** `crates/raft/src/kraft/{role,event,action}.rs`

- [ ] **Step 1: Write the failing test**

In `crates/raft/src/kraft/role.rs`, append:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    #[test]
    fn role_defaults_to_unattached() {
        let r = Role::default();
        assert!(matches!(r, Role::Unattached { .. }));
        assert!(!r.is_leader());
    }
}
```

- [ ] **Step 2: Run it** → FAIL (`Role` not defined). `cargo test -p crabka-raft kraft::role`.

- [ ] **Step 3: Implement the enums**

`crates/raft/src/kraft/role.rs`:

```rust
//! The replica's volatile role within the current epoch and its per-role state.

use std::collections::{BTreeMap, BTreeSet};

use crate::kraft::types::{NodeId, SimInstant};

/// Per-follower replication progress tracked by a leader (for HWM).
#[derive(Debug, Clone, Default)]
pub struct ReplicaProgress {
    /// Highest offset the follower has acknowledged fetching (its fetch offset).
    pub fetch_offset: i64,
}

#[derive(Debug, Clone)]
pub enum Role {
    /// Knows the epoch, no leader yet. May hold a non-binding pre-vote grant.
    Unattached { election_deadline: SimInstant },
    /// Cast a binding vote this epoch; waiting for a leader.
    Voted { election_deadline: SimInstant },
    /// Has a leader for the epoch; fetching from it.
    Follower { leader_id: NodeId, fetch_deadline: SimInstant },
    /// KIP-996 pre-vote candidate gathering non-binding grants.
    Prospective { granted: BTreeSet<NodeId>, election_deadline: SimInstant },
    /// Real candidacy (epoch bumped, self-voted).
    Candidate { granted: BTreeSet<NodeId>, election_deadline: SimInstant },
    /// Won the election; tracks follower progress for HWM.
    Leader { replicas: BTreeMap<NodeId, ReplicaProgress>, high_watermark: i64 },
    /// Stepping down; emitting EndQuorumEpoch.
    Resigned,
    /// Not in the voter set; only ever fetches.
    Observer { leader_id: Option<NodeId>, fetch_deadline: SimInstant },
}

impl Default for Role {
    fn default() -> Self {
        Role::Unattached { election_deadline: SimInstant(0) }
    }
}

impl Role {
    #[must_use]
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Role::Unattached { .. } => "Unattached",
            Role::Voted { .. } => "Voted",
            Role::Follower { .. } => "Follower",
            Role::Prospective { .. } => "Prospective",
            Role::Candidate { .. } => "Candidate",
            Role::Leader { .. } => "Leader",
            Role::Resigned => "Resigned",
            Role::Observer { .. } => "Observer",
        }
    }
}
```

`crates/raft/src/kraft/event.rs`:

```rust
//! Inputs to the consensus state machine.

use crate::kraft::types::{LeaderEpoch, NodeId};

/// A peer's view of its log tip, carried in Vote/Fetch requests.
#[derive(Debug, Clone, Copy)]
pub struct LogEnd {
    pub last_epoch: LeaderEpoch,
    pub last_offset: i64,
}

#[derive(Debug, Clone)]
pub enum Event {
    /// The election timer fired.
    ElectionTimeout,
    /// The fetch timer fired (follower/observer lost contact with the leader).
    FetchTimeout,
    /// A peer asks us for our vote.
    ReceiveVoteRequest {
        from: NodeId,
        candidate_epoch: LeaderEpoch,
        candidate: NodeId,
        candidate_log_end: LogEnd,
        pre_vote: bool,
    },
    /// A peer answered our Vote.
    ReceiveVoteResponse {
        from: NodeId,
        epoch: LeaderEpoch,
        vote_granted: bool,
        pre_vote: bool,
    },
    /// A leader announces its epoch to us.
    ReceiveBeginQuorumEpoch { leader_id: NodeId, leader_epoch: LeaderEpoch },
    /// A resigning leader tells us to start an election.
    ReceiveEndQuorumEpoch { leader_id: NodeId, leader_epoch: LeaderEpoch },
    /// (Leader side) a follower fetched at this position.
    ReceiveFetch { from: NodeId, fetch_epoch: LeaderEpoch, fetch_offset: i64 },
    /// (Follower side) the leader answered our Fetch.
    ReceiveFetchResponse {
        leader_id: NodeId,
        leader_epoch: LeaderEpoch,
        /// Set when the leader signalled log divergence.
        diverging: Option<crate::kraft::types::LogOffsetMetadata>,
    },
}
```

`crates/raft/src/kraft/action.rs`:

```rust
//! Outputs from the consensus state machine, executed by 3b/3c. In slice 3a
//! they are only inspected by tests.

use crate::kraft::types::{LeaderEpoch, LogOffsetMetadata, NodeId, SimInstant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerKind {
    Election,
    Fetch,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Broadcast a Vote request to all other voters (pre- or real vote).
    SendVoteRequest { epoch: LeaderEpoch, pre_vote: bool },
    /// Reply to a Vote request.
    ReplyVote { to: NodeId, epoch: LeaderEpoch, granted: bool, pre_vote: bool },
    /// New leader announces its epoch to all voters.
    SendBeginQuorumEpoch { epoch: LeaderEpoch },
    /// Resigning leader tells voters to elect.
    SendEndQuorumEpoch { epoch: LeaderEpoch },
    /// Follower/observer should fetch from this leader.
    SendFetch { leader_id: NodeId },
    /// We changed role (carries the new role name for observability/tests).
    TransitionedTo(&'static str),
    /// Persist the durable quorum state (epoch/votedKey/leaderId changed).
    PersistQuorumState,
    /// As new leader, append the LeaderChange control record for `epoch`.
    AppendLeaderChange { epoch: LeaderEpoch },
    /// Leader advanced the high watermark.
    AdvanceHighWatermark(i64),
    /// Follower must truncate its log to this diverging point.
    TruncateTo(LogOffsetMetadata),
    /// (Re)arm a timer to fire at `deadline`.
    ResetTimer { kind: TimerKind, deadline: SimInstant },
}
```

- [ ] **Step 4: Run** → PASS. `cargo test -p crabka-raft kraft::role`.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft
git commit -m "feat(raft): kraft Role/Event/Action types"
```

---

## Task 3: The state machine skeleton + vote granting

**Files:** `crates/raft/src/kraft/core.rs`

- [ ] **Step 1: Write the failing tests** (vote-granting truth table)

In `crates/raft/src/kraft/core.rs`, append a `#[cfg(test)] mod tests` with a fake `LogView` and these cases:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::kraft::event::{Event, LogEnd};
    use crate::kraft::types::*;
    use assert2::assert;

    struct FakeLog { end: i64, last_epoch: LeaderEpoch }
    impl LogView for FakeLog {
        fn end_offset(&self) -> i64 { self.end }
        fn last_epoch(&self) -> LeaderEpoch { self.last_epoch }
        fn end_offset_for_epoch(&self, epoch: LeaderEpoch) -> Option<i64> {
            if epoch <= self.last_epoch { Some(self.end) } else { None }
        }
    }
    fn voters(ids: &[NodeId]) -> crabka_metadata::voters::VoterSet {
        crabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id|
            crabka_metadata::voters::Voter { id, directory_id: uuid::Uuid::nil(),
                endpoints: vec![], kraft_version: Default::default() }))
    }
    fn machine(me: NodeId, ids: &[NodeId]) -> QuorumStateMachine {
        QuorumStateMachine::new(me, QuorumState::bootstrap(uuid::Uuid::nil(), voters(ids)), 1000)
    }

    #[test]
    fn grants_standard_vote_when_log_up_to_date_and_not_voted() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog { end: 5, last_epoch: 1 };
        let actions = m.on_event(Event::ReceiveVoteRequest {
            from: 2, candidate_epoch: 1, candidate: 2,
            candidate_log_end: LogEnd { last_epoch: 1, last_offset: 5 }, pre_vote: false,
        }, &log, SimInstant(0));
        assert!(actions.iter().any(|a| matches!(a,
            Action::ReplyVote { to: 2, granted: true, pre_vote: false, .. })));
        assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(2)); // binding
    }

    #[test]
    fn denies_standard_vote_when_candidate_log_behind() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog { end: 10, last_epoch: 2 };
        let actions = m.on_event(Event::ReceiveVoteRequest {
            from: 2, candidate_epoch: 2, candidate: 2,
            candidate_log_end: LogEnd { last_epoch: 1, last_offset: 3 }, pre_vote: false,
        }, &log, SimInstant(0));
        assert!(actions.iter().any(|a| matches!(a,
            Action::ReplyVote { granted: false, .. })));
    }

    #[test]
    fn pre_vote_grant_is_non_binding() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog { end: 5, last_epoch: 1 };
        m.on_event(Event::ReceiveVoteRequest {
            from: 2, candidate_epoch: 1, candidate: 2,
            candidate_log_end: LogEnd { last_epoch: 1, last_offset: 5 }, pre_vote: true,
        }, &log, SimInstant(0));
        assert!(m.quorum_state().voted_key.is_none()); // pre-vote does NOT persist
        assert!(m.quorum_state().leader_epoch == 0);    // epoch unchanged
    }

    #[test]
    fn denies_standard_vote_when_already_voted_for_other() {
        let mut m = machine(1, &[1, 2, 3]);
        let log = FakeLog { end: 5, last_epoch: 1 };
        // vote for 2 first
        m.on_event(Event::ReceiveVoteRequest { from: 2, candidate_epoch: 1, candidate: 2,
            candidate_log_end: LogEnd { last_epoch: 1, last_offset: 5 }, pre_vote: false }, &log, SimInstant(0));
        // now 3 asks in the same epoch
        let actions = m.on_event(Event::ReceiveVoteRequest { from: 3, candidate_epoch: 1, candidate: 3,
            candidate_log_end: LogEnd { last_epoch: 1, last_offset: 5 }, pre_vote: false }, &log, SimInstant(0));
        assert!(actions.iter().any(|a| matches!(a, Action::ReplyVote { to: 3, granted: false, .. })));
    }

    #[test]
    fn fenced_when_candidate_epoch_below_current() {
        let mut m = machine(1, &[1, 2, 3]);
        m.force_epoch(5); // test helper
        let log = FakeLog { end: 5, last_epoch: 5 };
        let actions = m.on_event(Event::ReceiveVoteRequest { from: 2, candidate_epoch: 3, candidate: 2,
            candidate_log_end: LogEnd { last_epoch: 5, last_offset: 5 }, pre_vote: false }, &log, SimInstant(0));
        assert!(actions.iter().any(|a| matches!(a, Action::ReplyVote { granted: false, .. })));
    }
}
```

- [ ] **Step 2: Run** → FAIL (`QuorumStateMachine` not defined).

- [ ] **Step 3: Implement the machine + vote handling**

`crates/raft/src/kraft/core.rs` (above the tests):

```rust
//! The KRaft quorum state machine: `on_event(event, log, now) -> Vec<Action>`.

use crate::kraft::action::{Action, TimerKind};
use crate::kraft::event::{Event, LogEnd};
use crate::kraft::role::Role;
use crate::kraft::types::{LeaderEpoch, LogView, NodeId, QuorumState, ReplicaKey, SimInstant};

pub struct QuorumStateMachine {
    me: NodeId,
    state: QuorumState,
    role: Role,
    /// Base election timeout in ms; callers vary it per node for liveness.
    election_timeout_ms: u64,
}

impl QuorumStateMachine {
    #[must_use]
    pub fn new(me: NodeId, state: QuorumState, election_timeout_ms: u64) -> Self {
        let observer = !state.voters.contains(me);
        let role = if observer {
            Role::Observer { leader_id: None, fetch_deadline: SimInstant(0) }
        } else {
            Role::default()
        };
        Self { me, state, role, election_timeout_ms }
    }

    #[must_use]
    pub fn quorum_state(&self) -> &QuorumState { &self.state }
    #[must_use]
    pub fn role(&self) -> &Role { &self.role }
    #[must_use]
    pub fn is_voter(&self) -> bool { self.state.voters.contains(self.me) }

    #[cfg(test)]
    pub(crate) fn force_epoch(&mut self, e: LeaderEpoch) { self.state.leader_epoch = e; }

    /// `true` if `candidate_log` is at least as up-to-date as ours
    /// (KIP-595: higher last epoch wins; on tie, higher/equal offset wins).
    fn log_is_up_to_date(&self, log: &dyn LogView, cand: LogEnd) -> bool {
        let my_epoch = log.last_epoch();
        let my_end = log.end_offset();
        cand.last_epoch > my_epoch || (cand.last_epoch == my_epoch && cand.last_offset >= my_end)
    }

    pub fn on_event(&mut self, event: Event, log: &dyn LogView, now: SimInstant) -> Vec<Action> {
        match event {
            Event::ReceiveVoteRequest { from, candidate_epoch, candidate, candidate_log_end, pre_vote } =>
                self.handle_vote_request(log, from, candidate_epoch, candidate, candidate_log_end, pre_vote),
            // remaining arms added in Tasks 4–6
            _ => Vec::new(),
        }
    }

    fn handle_vote_request(&mut self, log: &dyn LogView, from: NodeId, candidate_epoch: LeaderEpoch,
        candidate: NodeId, cand_log: LogEnd, pre_vote: bool) -> Vec<Action> {
        let mut actions = Vec::new();
        // Fenced: candidate is behind our epoch.
        if candidate_epoch < self.state.leader_epoch {
            actions.push(Action::ReplyVote { to: from, epoch: self.state.leader_epoch, granted: false, pre_vote });
            return actions;
        }
        // A standard vote at a higher epoch first advances us to that epoch
        // (Unattached), clearing any prior vote. Pre-vote never changes epoch.
        if !pre_vote && candidate_epoch > self.state.leader_epoch {
            self.transition_to_unattached(candidate_epoch, now_placeholder(), &mut actions);
        }
        let up_to_date = self.log_is_up_to_date(log, cand_log);
        let granted = if pre_vote {
            // Non-binding: grant if log is up to date and we don't already
            // follow a leader in this (or a higher) epoch.
            up_to_date && self.state.leader_id.is_none()
        } else {
            let not_voted_other = match self.state.voted_key {
                None => true,
                Some(k) => k.id == candidate,
            };
            up_to_date && not_voted_other && self.state.leader_id.is_none()
        };
        if granted && !pre_vote {
            // Binding: persist the vote, become Voted.
            self.state.voted_key = Some(ReplicaKey { id: candidate, directory_id: uuid::Uuid::nil() });
            self.role = Role::Voted { election_deadline: SimInstant(self.election_timeout_ms) };
            actions.push(Action::PersistQuorumState);
            actions.push(Action::TransitionedTo(self.role.name()));
        }
        actions.push(Action::ReplyVote { to: from, epoch: self.state.leader_epoch, granted, pre_vote });
        actions
    }

    fn transition_to_unattached(&mut self, epoch: LeaderEpoch, deadline: SimInstant, actions: &mut Vec<Action>) {
        self.state.leader_epoch = epoch;
        self.state.leader_id = None;
        self.state.voted_key = None;
        self.role = Role::Unattached { election_deadline: deadline };
        actions.push(Action::PersistQuorumState);
        actions.push(Action::TransitionedTo("Unattached"));
    }
}

// Helper retained only until Task 6 threads `now` everywhere; the vote path
// does not arm timers, so a zero deadline is harmless here.
fn now_placeholder() -> SimInstant { SimInstant(0) }
```

(The implementer should thread `now` into `handle_vote_request` instead of `now_placeholder()` if convenient; the placeholder only feeds an unused deadline on the epoch-advance path and is removed in Task 6.)

- [ ] **Step 4: Run** → all vote tests PASS. `cargo test -p crabka-raft kraft::core`.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/core.rs
git commit -m "feat(raft): kraft vote-granting (standard + pre-vote)"
```

---

## Task 4: Election lifecycle (timeout → Prospective → Candidate → Leader)

**Files:** `crates/raft/src/kraft/core.rs`

- [ ] **Step 1: Write failing tests**

Append to the `tests` module:

```rust
#[test]
fn election_timeout_starts_prevote_prospective() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 5, last_epoch: 1 };
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert!(matches!(m.role(), Role::Prospective { .. }));
    assert!(actions.iter().any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. })));
    assert!(m.quorum_state().leader_epoch == 0); // pre-vote: epoch not bumped yet
}

#[test]
fn prevote_majority_promotes_to_candidate_and_bumps_epoch() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 5, last_epoch: 1 };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000)); // Prospective
    // 1 (self) + grant from 2 = majority of 3
    let actions = m.on_event(Event::ReceiveVoteResponse {
        from: 2, epoch: 0, vote_granted: true, pre_vote: true }, &log, SimInstant(2001));
    assert!(matches!(m.role(), Role::Candidate { .. }));
    assert!(m.quorum_state().leader_epoch == 1);
    assert!(m.quorum_state().voted_key.map(|k| k.id) == Some(1)); // self-vote
    assert!(actions.iter().any(|a| matches!(a, Action::SendVoteRequest { pre_vote: false, epoch: 1 })));
}

#[test]
fn real_majority_promotes_to_leader_and_appends_leader_change() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 5, last_epoch: 1 };
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 0, vote_granted: true, pre_vote: true }, &log, SimInstant(2001));
    let actions = m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 1, vote_granted: true, pre_vote: false }, &log, SimInstant(2002));
    assert!(m.role().is_leader());
    assert!(m.quorum_state().leader_id == Some(1));
    assert!(actions.iter().any(|a| matches!(a, Action::AppendLeaderChange { epoch: 1 })));
    assert!(actions.iter().any(|a| matches!(a, Action::SendBeginQuorumEpoch { epoch: 1 })));
}

#[test]
fn observer_never_starts_election() {
    let mut m = machine(99, &[1, 2, 3]); // 99 is not a voter
    let log = FakeLog { end: 5, last_epoch: 1 };
    let actions = m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    assert!(matches!(m.role(), Role::Observer { .. }));
    assert!(!actions.iter().any(|a| matches!(a, Action::SendVoteRequest { .. })));
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement election arms**

Extend `on_event` to handle `Event::ElectionTimeout` and `Event::ReceiveVoteResponse`, and add the helpers:

- `ElectionTimeout`: if observer → no-op (return empty). Else → `Role::Prospective { granted: {self}, election_deadline: now + timeout }`, push `SendVoteRequest { epoch: current, pre_vote: true }` and `ResetTimer { Election, .. }`, `TransitionedTo("Prospective")`.
- `ReceiveVoteResponse { pre_vote: true, vote_granted: true }` while `Prospective`: insert `from` into `granted`; if `granted.len() >= majority` → become `Candidate`: bump `leader_epoch += 1`, set `voted_key = self`, `granted = {self}`, push `PersistQuorumState`, `SendVoteRequest { epoch: new, pre_vote: false }`, `TransitionedTo("Candidate")`.
- `ReceiveVoteResponse { pre_vote: false, vote_granted: true }` while `Candidate` AND `epoch == current`: insert into `granted`; if majority → become `Leader { replicas: <one entry per other voter, fetch_offset 0>, high_watermark: <current end offset> }`, set `leader_id = self`, push `AppendLeaderChange { epoch }`, `SendBeginQuorumEpoch { epoch }`, `PersistQuorumState`, `TransitionedTo("Leader")`.
- A `vote_granted: false` response carrying a higher `epoch` → step down via `transition_to_unattached(higher_epoch, ...)`.

Show the code in full in the implementation (mirror the structure of `handle_vote_request`). Use `self.state.majority()`.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/core.rs
git commit -m "feat(raft): kraft election lifecycle with KIP-996 pre-vote"
```

---

## Task 5: BeginQuorumEpoch / EndQuorumEpoch

**Files:** `crates/raft/src/kraft/core.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn begin_quorum_epoch_makes_us_follower() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 5, last_epoch: 1 };
    let actions = m.on_event(Event::ReceiveBeginQuorumEpoch { leader_id: 2, leader_epoch: 4 }, &log, SimInstant(10));
    assert!(matches!(m.role(), Role::Follower { leader_id: 2, .. }));
    assert!(m.quorum_state().leader_epoch == 4);
    assert!(m.quorum_state().leader_id == Some(2));
    assert!(actions.iter().any(|a| matches!(a, Action::SendFetch { leader_id: 2 })));
    assert!(actions.iter().any(|a| matches!(a, Action::PersistQuorumState)));
}

#[test]
fn end_quorum_epoch_triggers_immediate_election() {
    let mut m = machine(1, &[1, 2, 3]);
    // follow leader 2 @ epoch 4 first
    let log = FakeLog { end: 5, last_epoch: 1 };
    m.on_event(Event::ReceiveBeginQuorumEpoch { leader_id: 2, leader_epoch: 4 }, &log, SimInstant(10));
    let actions = m.on_event(Event::ReceiveEndQuorumEpoch { leader_id: 2, leader_epoch: 4 }, &log, SimInstant(11));
    // immediately start pre-vote (Prospective), not wait for timeout
    assert!(matches!(m.role(), Role::Prospective { .. }));
    assert!(actions.iter().any(|a| matches!(a, Action::SendVoteRequest { pre_vote: true, .. })));
}

#[test]
fn stale_begin_quorum_epoch_ignored() {
    let mut m = machine(1, &[1, 2, 3]);
    m.force_epoch(7);
    let log = FakeLog { end: 5, last_epoch: 7 };
    let actions = m.on_event(Event::ReceiveBeginQuorumEpoch { leader_id: 2, leader_epoch: 4 }, &log, SimInstant(10));
    assert!(actions.is_empty()); // lower epoch → ignored
    assert!(m.quorum_state().leader_id.is_none());
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

- `ReceiveBeginQuorumEpoch`: if `leader_epoch < current` → ignore (empty). Else set `leader_epoch`, `leader_id = Some(leader)`, clear `voted_key` if epoch advanced, become `Follower { leader_id, fetch_deadline: now + timeout }`; push `PersistQuorumState`, `SendFetch { leader_id }`, `ResetTimer { Fetch, .. }`, `TransitionedTo("Follower")`.
- `ReceiveEndQuorumEpoch`: if `leader_epoch < current` → ignore. Else, if we are a voter → behave exactly like `ElectionTimeout` *now* (start Prospective pre-vote immediately); if observer → just transition to `Unattached`/keep observing. Reuse the election-start helper from Task 4.

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/core.rs
git commit -m "feat(raft): kraft BeginQuorumEpoch/EndQuorumEpoch handling"
```

---

## Task 6: Leader replication — follower fetch tracking, HWM, divergence

**Files:** `crates/raft/src/kraft/core.rs`

- [ ] **Step 1: Write failing tests**

```rust
#[test]
fn leader_advances_hwm_at_majority_fetch_offset() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 10, last_epoch: 1 };
    // drive to leader
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 0, vote_granted: true, pre_vote: true }, &log, SimInstant(2001));
    m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 1, vote_granted: true, pre_vote: false }, &log, SimInstant(2002));
    // leader end offset 10. follower 2 fetches at 8, follower 3 at 4.
    let a2 = m.on_event(Event::ReceiveFetch { from: 2, fetch_epoch: 1, fetch_offset: 8 }, &log, SimInstant(2100));
    // majority of {self=10, 2=8} = 8 → HWM advances to 8
    assert!(a2.iter().any(|a| matches!(a, Action::AdvanceHighWatermark(8))));
    let _ = m.on_event(Event::ReceiveFetch { from: 3, fetch_epoch: 1, fetch_offset: 4 }, &log, SimInstant(2101));
    // sorted match offsets {10,8,4}; majority (2nd highest) = 8 → no regress
    if let Role::Leader { high_watermark, .. } = m.role() { assert!(*high_watermark == 8); } else { panic!() }
}

#[test]
fn leader_detects_divergence_and_returns_truncate() {
    let mut m = machine(1, &[1, 2, 3]);
    // log has last_epoch 2 ending at 10; epoch-1 ended at 5.
    struct L;
    impl LogView for L {
        fn end_offset(&self) -> i64 { 10 }
        fn last_epoch(&self) -> LeaderEpoch { 2 }
        fn end_offset_for_epoch(&self, e: LeaderEpoch) -> Option<i64> {
            match e { 0 => Some(0), 1 => Some(5), 2 => Some(10), _ => None }
        }
    }
    let log = L;
    m.on_event(Event::ElectionTimeout, &log, SimInstant(2000));
    m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 0, vote_granted: true, pre_vote: true }, &log, SimInstant(2001));
    m.on_event(Event::ReceiveVoteResponse { from: 2, epoch: 1, vote_granted: true, pre_vote: false }, &log, SimInstant(2002));
    // follower claims it fetched epoch 1 at offset 8, but epoch 1 ended at 5 → diverged.
    let actions = m.on_event(Event::ReceiveFetch { from: 2, fetch_epoch: 1, fetch_offset: 8 }, &log, SimInstant(2100));
    assert!(actions.iter().any(|a| matches!(a,
        Action::ReplyVote { .. }) == false)); // (sanity)
    assert!(actions.iter().any(|a| matches!(a,
        Action::TruncateTo(LogOffsetMetadata { offset: 5, epoch: 1 }))));
}

#[test]
fn follower_truncates_on_diverging_fetch_response() {
    let mut m = machine(1, &[1, 2, 3]);
    let log = FakeLog { end: 10, last_epoch: 2 };
    m.on_event(Event::ReceiveBeginQuorumEpoch { leader_id: 2, leader_epoch: 3 }, &log, SimInstant(10));
    let actions = m.on_event(Event::ReceiveFetchResponse {
        leader_id: 2, leader_epoch: 3,
        diverging: Some(LogOffsetMetadata { offset: 5, epoch: 1 }) }, &log, SimInstant(11));
    assert!(actions.iter().any(|a| matches!(a, Action::TruncateTo(LogOffsetMetadata { offset: 5, epoch: 1 }))));
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement**

- `ReceiveFetch` (only meaningful when `Leader`; otherwise reply/ignore):
  - **Divergence check:** if `fetch_offset > 0`, compare the follower's `fetch_epoch` against our log: `let div_end = log.end_offset_for_epoch(fetch_epoch)`. If `div_end` is `Some(e)` and `fetch_offset > e` (the follower is ahead of where that epoch ended in our log) OR `end_offset_for_epoch(fetch_epoch)` shows the follower's epoch didn't extend that far → push `Action::TruncateTo(LogOffsetMetadata { offset: e, epoch: fetch_epoch })` and return (do not count this fetch toward HWM).
  - **Otherwise (consistent):** record `replicas[from].fetch_offset = fetch_offset`; recompute HWM = the `majority()`-th largest value among `{ self_end_offset } ∪ { each replica.fetch_offset }` (include the leader's own `log.end_offset()`); if it increased, set `high_watermark` and push `Action::AdvanceHighWatermark(hwm)`. HWM never regresses; only commit offsets in the current leader epoch (the leader's own end offset is in-epoch by construction).
- `ReceiveFetchResponse` (follower side): if `diverging` is `Some(point)` → push `Action::TruncateTo(point)`; else (steady state) re-arm the fetch timer and `SendFetch`.

Provide full code mirroring earlier arms. Add a private `fn recompute_high_watermark(&self, log_end: i64) -> i64` that sorts the match offsets descending and returns the `majority()-1` index.

- [ ] **Step 4: Run** → PASS. Then run ALL kraft unit tests: `cargo test -p crabka-raft kraft`.

- [ ] **Step 5: Commit**

```bash
git add crates/raft/src/kraft/core.rs
git commit -m "feat(raft): kraft leader HWM advancement + log divergence handling"
```

---

## Task 7: Deterministic multi-node simulation + acceptance

**Files:** create `crates/raft/tests/kraft_sim.rs`

- [ ] **Step 1: Write the simulation harness + acceptance tests**

Build an in-memory harness: N `QuorumStateMachine`s each with its own growable fake log; a central scheduler holding a logical clock and a queue of in-flight messages (with per-node election timeouts staggered so elections converge). The harness translates each `Action` into `Event`s delivered to peers (e.g. `SendVoteRequest` → a `ReceiveVoteRequest` to every other voter; `ReplyVote` → `ReceiveVoteResponse` to the sender; `SendBeginQuorumEpoch` → `ReceiveBeginQuorumEpoch` to peers; `SendFetch`/`ReceiveFetch`/HWM bookkeeping). Drive `ElectionTimeout` for whichever node's deadline is earliest. Then assert:

```rust
#[test]
fn three_nodes_elect_exactly_one_leader() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(/*max_ticks*/ 10_000);
    assert!(sim.leaders().len() == 1, "expected exactly one leader, got {:?}", sim.leaders());
    // every voter agrees on the leader's epoch
    assert!(sim.distinct_epochs().len() == 1);
}

#[test]
fn re_elects_single_leader_after_leader_partition() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    let old_leader = sim.leaders()[0];
    sim.partition(old_leader);          // isolate the leader
    sim.run_until_stable(10_000);
    let new_leaders: Vec<_> = sim.leaders().into_iter().filter(|&l| l != old_leader).collect();
    assert!(new_leaders.len() == 1, "majority side must elect one new leader");
    sim.heal(old_leader);               // old leader rejoins
    sim.run_until_stable(10_000);
    assert!(sim.leaders().len() == 1);  // converges back to a single leader
    assert!(sim.leaders()[0] == new_leaders[0]); // old leader steps down to follower
}

#[test]
fn committed_high_watermark_agrees_across_voters() {
    let mut sim = Sim::new(&[1, 2, 3]);
    sim.run_until_stable(10_000);
    let leader = sim.leaders()[0];
    sim.leader_append(leader, /*records*/ 5); // grow the leader's log by 5
    sim.run_until_stable(10_000);
    // HWM reaches the appended offset and all voters fetch up to it
    assert!(sim.leader_high_watermark(leader) >= 5);
    assert!(sim.all_voters_fetched_to(sim.leader_high_watermark(leader)));
}
```

The `Sim` harness (election timeout staggering, message bus, partition/heal, leader_append growing a node's fake log, fetch loop) is implemented in the test file. Keep it deterministic: a fixed per-node `election_timeout_ms` (e.g. node id × 50 + 1000) so ties break deterministically; advance the clock to the next due timer when the message queue drains.

- [ ] **Step 2: Run the simulation**

Run: `cargo test -p crabka-raft --test kraft_sim -- --nocapture`
Expected: all three PASS. If a test hangs or elects 0/2 leaders, the election or HWM logic is wrong — debug against the rule-level tests. This simulation is the headline acceptance for 3a.

- [ ] **Step 3: Commit**

```bash
git add crates/raft/tests/kraft_sim.rs
git commit -m "test(raft): deterministic multi-node kraft election/replication simulation"
```

---

## Task 8: Capstone — fmt, clippy, regression

- [ ] **Step 1:** `cargo fmt --all && cargo fmt --all --check` → clean.
- [ ] **Step 2:** `cargo clippy -p crabka-raft --tests` → clean (the `kraft` module is hand-written; keep it warning-free).
- [ ] **Step 3:** `cargo test -p crabka-raft` (incl. `--features kraft-spike`) → all pass; openraft path and Slice-0 spike untouched and green.
- [ ] **Step 4:** Commit any fmt fixes: `git add -A && git commit -m "chore(raft): fmt kraft core" || echo "nothing to commit"`.

---

## Self-Review Notes

- **Spec coverage:** event/action core + `LogView` → Tasks 1–3; roles incl. Prospective/Observer → Task 2; vote granting (standard + pre-vote, fenced, already-voted, log-up-to-date) → Task 3; election w/ pre-vote → Task 4; Begin/EndQuorumEpoch → Task 5; leader HWM + divergence → Task 6; deterministic multi-node simulation (one leader, partition+heal, HWM agreement) → Task 7. All spec sections covered. Persistent `QuorumState` struct present (file IO correctly deferred to 3c).
- **Determinism:** time/jitter injected; no clock/rand in the module (enforced repo-wide).
- **Type consistency:** `QuorumStateMachine`, `on_event`, `QuorumState`, `Role`, `Event`, `Action`, `LogView`, `ReplicaKey`, `SimInstant`, `LogOffsetMetadata`, `LeaderEpoch`, `recompute_high_watermark`, `transition_to_unattached` are defined once and used consistently across tasks.
- **Logic bodies vs prose:** Tasks 4–6 specify some arm bodies as precise rules rather than full code because they share the machine's private state and read most naturally written in-place; the rule-level tests in each task encode the exact required behavior (truth tables / offsets), so "passes these tests" is an unambiguous bar. The data types, signatures, and representative bodies (Task 3) are given in full.
- **Isolation:** every task is additive under `crates/raft/src/kraft/` + one test file; openraft and the broker are untouched, so the tree is green at every commit.
