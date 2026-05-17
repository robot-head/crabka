# Slice 43b — Rebalancer execute path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Land the rebalancer's execute path — `ExecuteProposal` Connect RPC drives `AlterPartitionReassignments` (KIP-455) under a `IncrementalAlterConfigs`-managed KIP-73 throttle, with progress polling, atomic on-disk persistence, restart resume, a `CancelExecution` RPC, and the production Helm chart at `charts/crabka-rebalancer/` with helm-unittest tests in CI.

**Architecture:** New `executor` module owns a single-execution state machine (ApplyThrottle → Submit → Wait → ClearThrottle); at most one execution runs at a time, tracked by an `Arc<Mutex<Option<ExecutionHandle>>>` on `AppState`. State persists to `{data_dir}/proposals.json` (full ring buffer) and `{data_dir}/in_flight.json` (active-execution marker, deleted on terminal). On startup, recovery loads both files and resumes any in-flight plan by re-issuing the persisted phase — KIP-455 is idempotent against the same target replica set. `ClearThrottle` runs in every terminal path.

**Tech Stack:** Rust 1.95.0. Reuses existing workspace deps (`tokio`, `serde`, `tokio-util` for `CancellationToken`, `prometheus-client`). No new workspace deps. Helm chart uses the `helm-unittest` plugin (installed in CI) for chart tests.

**Reference spec:** [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43b-design.md`](../specs/2026-05-17-crabka-rebalancer-43b-design.md).

**Working directory:** `/home/matt/git/crabka`. Branch `feature/rebalancer-43b` already exists with the 43b spec committed.

---

## File structure

```
crates/rebalancer/
├── Cargo.toml                                            # MODIFIED — no new deps expected
├── proto/crabka/rebalancer/v1/rebalancer.proto           # MODIFIED — ProposalStatus variants, Proposal fields, ExecuteProposalRequest fields, CancelExecution RPC
├── src/
│   ├── lib.rs                                            # MODIFIED — mount executor module
│   ├── bin/rebalancer.rs                                 # MODIFIED — new CLI flags, executor wiring, startup recovery
│   ├── executor/
│   │   ├── mod.rs                                        # NEW — Executor + Execution + ExecutionHandle, state-machine driver
│   │   ├── phases.rs                                     # NEW — ClientFacade trait + per-phase async fns
│   │   ├── throttle.rs                                   # NEW — pure-logic compute_throttle_targets
│   │   └── state.rs                                      # NEW — InFlightFile + atomic write helpers
│   ├── api/
│   │   ├── handlers.rs                                   # MODIFIED — ExecuteProposal body, CancelExecution handler
│   │   └── mod.rs                                        # MODIFIED — wire CancelExecution into the builder
│   ├── model/
│   │   ├── proposal.rs                                   # MODIFIED — new ProposalStatus variants + Proposal fields
│   │   ├── store.rs                                      # MODIFIED — atomic file persistence + status-mutator methods
│   │   └── mod.rs                                        # MODIFIED — re-exports
│   └── metrics.rs                                        # MODIFIED — executions_started / completed / failed / cancelled counters
└── tests/
    ├── end_to_end.rs                                     # MODIFIED — three new integration tests
    └── connect_smoke.rs                                  # MODIFIED — ExecuteProposal HTTP round-trip
charts/crabka-rebalancer/                                 # NEW (whole tree)
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── deployment.yaml
│   ├── service.yaml
│   ├── serviceaccount.yaml
│   └── persistentvolumeclaim.yaml
└── tests/                                                 # helm-unittest test files (not Helm test pods)
    ├── deployment_test.yaml
    ├── required_values_test.yaml
    ├── service_test.yaml
    ├── pvc_test.yaml
    └── rbac_test.yaml
.github/workflows/ci.yml                                  # MODIFIED — helm-lint job installs helm-unittest + lints/tests new chart
README.md                                                 # MODIFIED — executor row → ✅
STATUS.md                                                 # MODIFIED — slice 43b entry
```

**14 tasks across 10 batches.**

- **Batch 1 (alone):** T1 — proto updates (extends `ProposalStatus`, adds Proposal fields, adds `CancelExecution` RPC)
- **Batch 2 (parallel):** T2 model updates, T3 throttle pure logic
- **Batch 3 (alone):** T4 — store persister (depends on T2's `ProposalStatus` shape)
- **Batch 4 (parallel):** T5 executor::state, T6 executor::phases (different files)
- **Batch 5 (alone):** T7 — executor::mod.rs (state machine, requires T4/T5/T6)
- **Batch 6 (alone):** T8 — api handlers (requires T1/T2/T7)
- **Batch 7 (alone):** T9 — binary wiring + recovery (requires T7/T8)
- **Batch 8 (parallel):** T10 e2e tests, T11 Connect smoke extension, T12 Helm chart files (different file sets)
- **Batch 9 (parallel):** T13 helm-unittest tests, T14 CI workflow update (different file sets)
- **Batch 10 (alone):** T15 — README + STATUS

(Plan numbers tasks T1–T15.)

---

## Batch 1 — Proto + codegen

### Task 1: Extend the `.proto` with execute-path messages and the Cancel RPC

**Files:**
- Modify: `crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto`

- [ ] **Step 1: Update the proto file**

Replace the entire contents of `crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto` with:

```proto
syntax = "proto3";
package crabka.rebalancer.v1;

service Rebalancer {
  rpc GetState(GetStateRequest) returns (GetStateResponse);
  rpc CreateProposal(CreateProposalRequest) returns (Proposal);
  rpc DryRunProposal(DryRunProposalRequest) returns (DryRunResponse);
  rpc GetProposal(GetProposalRequest) returns (Proposal);
  rpc ListProposals(ListProposalsRequest) returns (ListProposalsResponse);
  rpc ExecuteProposal(ExecuteProposalRequest) returns (ExecuteProposalResponse);
  rpc CancelExecution(CancelExecutionRequest) returns (CancelExecutionResponse);
}

message Broker {
  int32 id = 1;
  string host = 2;
  int32 port = 3;
  optional string rack = 4;
}

message Partition {
  int32 partition = 1;
  repeated int32 replicas = 2;
  int32 leader = 3;
  repeated int32 isr = 4;
}

message Topic { string name = 1; repeated Partition partitions = 2; }

message InFlightReassignment {
  string topic = 1;
  int32 partition = 2;
  repeated int32 adding_replicas = 3;
  repeated int32 removing_replicas = 4;
}

message GetStateRequest {}
message GetStateResponse {
  int64 snapshot_at_ms = 1;
  repeated Broker brokers = 2;
  repeated Topic topics = 3;
  repeated InFlightReassignment in_flight_reassignments = 4;
}

enum ProposalStatus {
  PROPOSAL_STATUS_UNSPECIFIED = 0;
  PROPOSAL_STATUS_COMPUTED = 1;
  PROPOSAL_STATUS_EXECUTING = 2;
  PROPOSAL_STATUS_COMPLETED = 3;
  PROPOSAL_STATUS_FAILED = 4;
  PROPOSAL_STATUS_CANCELLED = 5;
}

message Movement {
  string topic = 1;
  int32 partition = 2;
  repeated int32 old_replicas = 3;
  repeated int32 new_replicas = 4;
  int32 old_leader = 5;
  int32 new_leader = 6;
}

message ProposalSummary {
  int32 replica_movements = 1;
  int32 leader_movements = 2;
  int32 max_replicas_before = 3;
  int32 max_replicas_after = 4;
  int32 max_leaders_before = 5;
  int32 max_leaders_after = 6;
}

message Proposal {
  string id = 1;
  ProposalStatus status = 2;
  int64 created_at_ms = 3;
  repeated string goals_applied = 4;
  ProposalSummary summary = 5;
  repeated Movement movements = 6;
  int64 started_at_ms = 7;
  int64 terminated_at_ms = 8;
  optional string failure_reason = 9;
  int64 throttle_bytes_per_sec = 10;
}

message CreateProposalRequest { repeated string goals = 1; }
message DryRunProposalRequest { string id = 1; }
message DryRunResponse {
  string id = 1;
  ProposalSummary summary = 2;
  int64 estimated_bytes_moved = 3;
}
message GetProposalRequest { string id = 1; }
message ListProposalsRequest { int32 limit = 1; }
message ListProposalsResponse { repeated Proposal proposals = 1; }

message ExecuteProposalRequest {
  string id = 1;
  optional int64 throttle_bytes_per_sec = 2;
}
message ExecuteProposalResponse { Proposal proposal = 1; }

message CancelExecutionRequest { string id = 1; }
message CancelExecutionResponse { Proposal proposal = 1; }
```

- [ ] **Step 2: Verify the codegen builds cleanly**

Run: `cargo build -p crabka-rebalancer`
Expected: clean build. The build script regenerates `pb::*` types.

If the build fails because the existing `proposal_to_proto` in `src/api/handlers.rs` doesn't supply the new fields, ignore those errors for now — Task 2 will fix the helper to populate the new fields. The codegen itself must succeed.

If the proto-to-Rust regeneration somehow re-emits the existing API's RPC under a renamed module path, stop and report — that's an unexpected codegen drift.

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto
git -C /home/matt/git/crabka commit -m "rebalancer(43b): proto adds Executing/Completed/Failed/Cancelled + CancelExecution RPC

Extends ProposalStatus with four execute-path variants. Proposal gains
started_at_ms, terminated_at_ms, failure_reason, throttle_bytes_per_sec.
ExecuteProposalRequest gains optional throttle_bytes_per_sec. New
CancelExecution RPC + Request/Response messages. ExecuteProposalResponse
now carries the transitioning Proposal.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Model updates + throttle pure logic (parallel: T2, T3)

### Task 2: Extend `model::proposal::ProposalStatus` + `Proposal` fields

**Files:**
- Modify: `crates/rebalancer/src/model/proposal.rs`

- [ ] **Step 1: Replace the ProposalStatus + Proposal definitions**

Replace `crates/rebalancer/src/model/proposal.rs` with:

```rust
//! Proposal + Movement types. Mirrors the proto definitions but owned
//! by the model layer so the optimizer + goals don't depend on
//! generated code.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Movement {
    pub topic: String,
    pub partition: i32,
    pub old_replicas: Vec<i32>,
    pub new_replicas: Vec<i32>,
    pub old_leader: i32,
    pub new_leader: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProposalStatus {
    Computed,
    Executing,
    Completed,
    Failed,
    Cancelled,
}

impl ProposalStatus {
    /// True if the status is a final state (no further transitions).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ProposalStatus::Completed | ProposalStatus::Failed | ProposalStatus::Cancelled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ProposalSummary {
    pub replica_movements: i32,
    pub leader_movements: i32,
    pub max_replicas_before: i32,
    pub max_replicas_after: i32,
    pub max_leaders_before: i32,
    pub max_leaders_after: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    pub id: String,
    pub status: ProposalStatus,
    pub created_at_ms: i64,
    pub goals_applied: Vec<String>,
    pub summary: ProposalSummary,
    pub movements: Vec<Movement>,
    /// Set when transitioning to `Executing`; 0 otherwise.
    #[serde(default)]
    pub started_at_ms: i64,
    /// Set when transitioning to a terminal status; 0 otherwise.
    #[serde(default)]
    pub terminated_at_ms: i64,
    /// Set on `Failed`. None otherwise.
    #[serde(default)]
    pub failure_reason: Option<String>,
    /// Set when transitioning to `Executing` (echoes the throttle the
    /// executor applied). 0 otherwise.
    #[serde(default)]
    pub throttle_bytes_per_sec: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_terminal_flags() {
        assert!(!ProposalStatus::Computed.is_terminal());
        assert!(!ProposalStatus::Executing.is_terminal());
        assert!(ProposalStatus::Completed.is_terminal());
        assert!(ProposalStatus::Failed.is_terminal());
        assert!(ProposalStatus::Cancelled.is_terminal());
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib model::proposal -- --nocapture`
Expected: 1 test passes (`status_terminal_flags`).

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/model/proposal.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43b): ProposalStatus variants + Proposal execute-path fields

ProposalStatus gains Executing/Completed/Failed/Cancelled. Proposal
gains started_at_ms / terminated_at_ms / failure_reason /
throttle_bytes_per_sec. ProposalStatus::is_terminal helper for the
executor's state machine.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 3: `executor::throttle` pure-logic compute

**Files:**
- Create: `crates/rebalancer/src/executor/throttle.rs`
- Create: `crates/rebalancer/src/executor/mod.rs` (minimal — just the module declarations; the real `Executor` struct lands in T7)

This task only writes the throttle target computation + tests. The full executor wiring waits for T7.

- [ ] **Step 1: Write `crates/rebalancer/src/executor/throttle.rs`**

```rust
//! Pure-logic KIP-73 throttle target computation. Given a slice of
//! Movements, returns the per-broker rate targets and per-topic
//! replica-list targets that `ApplyThrottle` will write via
//! `IncrementalAlterConfigs`.
//!
//! The computation is deterministic and side-effect-free so the
//! executor's state machine can test it in isolation.

use std::collections::{BTreeMap, BTreeSet};

use crate::model::Movement;

/// All four KIP-73 target families for a single proposal.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ThrottleTargets {
    /// Brokers that will act as leaders for moving replicas.
    /// `leader.replication.throttled.rate` is set on each.
    pub leader_brokers: BTreeSet<i32>,
    /// Brokers that will act as new followers (catching up).
    /// `follower.replication.throttled.rate` is set on each.
    pub follower_brokers: BTreeSet<i32>,
    /// Per-topic value for `leader.replication.throttled.replicas`.
    /// Map value is the canonical `partition:broker,partition:broker,...`
    /// string ready for IncrementalAlterConfigs.
    pub leader_replicas_per_topic: BTreeMap<String, String>,
    /// Per-topic value for `follower.replication.throttled.replicas`.
    pub follower_replicas_per_topic: BTreeMap<String, String>,
}

#[must_use]
pub fn compute_throttle_targets(movements: &[Movement]) -> ThrottleTargets {
    let mut leader_brokers: BTreeSet<i32> = BTreeSet::new();
    let mut follower_brokers: BTreeSet<i32> = BTreeSet::new();
    // Topic → (partition, broker) entries, kept sorted for deterministic output.
    let mut leader_replicas_per_topic: BTreeMap<String, BTreeSet<(i32, i32)>> = BTreeMap::new();
    let mut follower_replicas_per_topic: BTreeMap<String, BTreeSet<(i32, i32)>> = BTreeMap::new();

    for m in movements {
        let old: BTreeSet<i32> = m.old_replicas.iter().copied().collect();
        let new: BTreeSet<i32> = m.new_replicas.iter().copied().collect();

        // Leaders = the set of source brokers (movements *from*).
        for src in &old {
            leader_brokers.insert(*src);
            leader_replicas_per_topic
                .entry(m.topic.clone())
                .or_default()
                .insert((m.partition, *src));
        }
        // Followers = new replicas that weren't already in `old`.
        for dst in new.difference(&old) {
            follower_brokers.insert(*dst);
            follower_replicas_per_topic
                .entry(m.topic.clone())
                .or_default()
                .insert((m.partition, *dst));
        }
    }

    ThrottleTargets {
        leader_brokers,
        follower_brokers,
        leader_replicas_per_topic: stringify(&leader_replicas_per_topic),
        follower_replicas_per_topic: stringify(&follower_replicas_per_topic),
    }
}

fn stringify(per_topic: &BTreeMap<String, BTreeSet<(i32, i32)>>) -> BTreeMap<String, String> {
    per_topic
        .iter()
        .map(|(topic, entries)| {
            let joined = entries
                .iter()
                .map(|(p, b)| format!("{p}:{b}"))
                .collect::<Vec<_>>()
                .join(",");
            (topic.clone(), joined)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    #[test]
    fn empty_movements_returns_empty_targets() {
        let t = compute_throttle_targets(&[]);
        assert!(t.leader_brokers.is_empty());
        assert!(t.follower_brokers.is_empty());
        assert!(t.leader_replicas_per_topic.is_empty());
        assert!(t.follower_replicas_per_topic.is_empty());
    }

    #[test]
    fn single_movement_one_topic() {
        // Move partition 0 from broker 1 to broker 2.
        let m = mv("t", 0, vec![1], vec![2]);
        let t = compute_throttle_targets(std::slice::from_ref(&m));
        assert_eq!(t.leader_brokers, BTreeSet::from([1]));
        assert_eq!(t.follower_brokers, BTreeSet::from([2]));
        assert_eq!(
            t.leader_replicas_per_topic.get("t").map(String::as_str),
            Some("0:1")
        );
        assert_eq!(
            t.follower_replicas_per_topic.get("t").map(String::as_str),
            Some("0:2")
        );
    }

    #[test]
    fn replica_set_growth_distinguishes_new_vs_existing() {
        // Replicas [1] → [1, 2]: broker 1 stays, broker 2 is the new follower.
        let m = mv("t", 5, vec![1], vec![1, 2]);
        let t = compute_throttle_targets(std::slice::from_ref(&m));
        assert_eq!(t.leader_brokers, BTreeSet::from([1]));
        assert_eq!(t.follower_brokers, BTreeSet::from([2]));
        // leader.replication.throttled.replicas covers the partition × source brokers.
        assert_eq!(
            t.leader_replicas_per_topic.get("t").map(String::as_str),
            Some("5:1")
        );
        assert_eq!(
            t.follower_replicas_per_topic.get("t").map(String::as_str),
            Some("5:2")
        );
    }

    #[test]
    fn multiple_movements_aggregate_per_topic() {
        let ms = vec![
            mv("t1", 0, vec![1], vec![2]),
            mv("t1", 1, vec![1, 3], vec![2, 3]),
            mv("t2", 0, vec![2], vec![1]),
        ];
        let t = compute_throttle_targets(&ms);
        assert_eq!(t.leader_brokers, BTreeSet::from([1, 2, 3]));
        assert_eq!(t.follower_brokers, BTreeSet::from([1, 2]));
        // Per-topic strings are sorted by (partition, broker).
        assert_eq!(
            t.leader_replicas_per_topic.get("t1").map(String::as_str),
            Some("0:1,1:1,1:3")
        );
        assert_eq!(
            t.follower_replicas_per_topic.get("t1").map(String::as_str),
            Some("0:2,1:2")
        );
        assert_eq!(
            t.leader_replicas_per_topic.get("t2").map(String::as_str),
            Some("0:2")
        );
        assert_eq!(
            t.follower_replicas_per_topic.get("t2").map(String::as_str),
            Some("0:1")
        );
    }

    #[test]
    fn output_is_deterministic_across_input_orders() {
        let a = vec![
            mv("z", 1, vec![3], vec![4]),
            mv("a", 0, vec![1], vec![2]),
        ];
        let b = vec![
            mv("a", 0, vec![1], vec![2]),
            mv("z", 1, vec![3], vec![4]),
        ];
        assert_eq!(compute_throttle_targets(&a), compute_throttle_targets(&b));
    }
}
```

- [ ] **Step 2: Create the executor module entry**

Create `crates/rebalancer/src/executor/mod.rs` with just module declarations:

```rust
//! Execute-path state machine. `Executor` runs one `Execution` at a time
//! against the cluster via `crabka_client_core::Client`.
//!
//! Slice 43b adds the full state machine (ApplyThrottle → Submit → Wait
//! → ClearThrottle) and on-disk persistence with restart resume. The
//! file is intentionally split across `phases`, `state`, and `throttle`
//! so each piece is independently testable.

pub mod throttle;
```

- [ ] **Step 3: Mount the executor module**

Edit `crates/rebalancer/src/lib.rs`. Append `pub mod executor;` after the existing `pub mod metrics;` (preserve all other module declarations).

- [ ] **Step 4: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib executor::throttle -- --nocapture`
Expected: 4 tests pass.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean.

The workspace uses `clippy::pedantic`. If any cast lints fire, replace `as` casts with `try_from` per the 43a pattern.

- [ ] **Step 5: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/executor crates/rebalancer/src/lib.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43b): executor::throttle pure-logic target computation

compute_throttle_targets(movements) returns the four KIP-73 target
families (leader/follower broker sets + per-topic partition:broker
strings) ready for IncrementalAlterConfigs. Deterministic output via
BTreeSet/BTreeMap. Four unit tests cover empty / single-movement /
replica growth / multi-movement aggregation / input-order
invariance.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Persister

### Task 4: `ProposalStore` atomic on-disk persistence + status mutators

**Files:**
- Modify: `crates/rebalancer/src/model/store.rs`
- Modify: `crates/rebalancer/src/model/mod.rs` (re-export `Persister` if needed; verify what's already re-exported)

- [ ] **Step 1: Replace `crates/rebalancer/src/model/store.rs`**

```rust
//! Ring buffer of recent `Proposal`s, UUID-keyed, with atomic on-disk
//! persistence. Slice 43b persists to `{data_dir}/proposals.json` so
//! proposals survive a rebalancer restart.

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use super::proposal::{Proposal, ProposalStatus};

const FILE_VERSION: u32 = 1;
const DEFAULT_FILENAME: &str = "proposals.json";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
}

#[derive(Debug, Serialize, Deserialize)]
struct OnDisk {
    version: u32,
    capacity: usize,
    proposals: Vec<Proposal>,
}

pub struct ProposalStore {
    inner: Mutex<VecDeque<Proposal>>,
    capacity: usize,
    /// Where to persist. `None` = in-memory only (tests / 43a-compat).
    path: Option<PathBuf>,
}

impl ProposalStore {
    /// New in-memory-only store. Used in unit tests where persistence
    /// isn't under test.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            capacity: capacity.max(1),
            path: None,
        }
    }

    /// Open or create a persisted store at `{data_dir}/proposals.json`.
    /// If the file is missing, returns an empty store and will create
    /// the file on first write.
    pub fn open(data_dir: &Path, capacity: usize) -> Result<Self, StoreError> {
        fs::create_dir_all(data_dir)?;
        let path = data_dir.join(DEFAULT_FILENAME);
        let inner = match fs::read(&path) {
            Ok(bytes) => {
                let parsed: OnDisk = serde_json::from_slice(&bytes)?;
                if parsed.version != FILE_VERSION {
                    return Err(StoreError::UnsupportedVersion {
                        found: parsed.version,
                        expected: FILE_VERSION,
                    });
                }
                VecDeque::from(parsed.proposals)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => VecDeque::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(Self {
            inner: Mutex::new(inner),
            capacity: capacity.max(1),
            path: Some(path),
        })
    }

    pub fn insert(&self, p: Proposal) {
        {
            let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
            if q.len() == self.capacity {
                q.pop_front();
            }
            q.push_back(p);
        }
        self.persist_if_durable();
    }

    /// Apply `f` to the proposal with `id`. Returns the post-mutation
    /// clone, or `None` if no such id. Persists.
    pub fn mutate<F: FnOnce(&mut Proposal)>(&self, id: &str, f: F) -> Option<Proposal> {
        let updated = {
            let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
            let p = q.iter_mut().find(|p| p.id == id)?;
            f(p);
            p.clone()
        };
        self.persist_if_durable();
        Some(updated)
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        q.iter().find(|p| p.id == id).cloned()
    }

    #[must_use]
    pub fn list(&self, limit: usize) -> Vec<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        let n = if limit == 0 {
            self.capacity
        } else {
            limit.min(self.capacity)
        };
        q.iter().rev().take(n).cloned().collect()
    }

    fn persist_if_durable(&self) {
        let Some(ref path) = self.path else {
            return;
        };
        let snapshot: Vec<Proposal> = {
            let q = self.inner.lock().expect("ProposalStore mutex poisoned");
            q.iter().cloned().collect()
        };
        let on_disk = OnDisk {
            version: FILE_VERSION,
            capacity: self.capacity,
            proposals: snapshot,
        };
        match write_atomic(path, &on_disk) {
            Ok(()) => debug!(?path, "proposals.json persisted"),
            Err(e) => warn!(?path, error = %e, "proposals.json persist failed; in-memory state ahead of disk"),
        }
    }
}

fn write_atomic(path: &Path, on_disk: &OnDisk) -> Result<(), StoreError> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(on_disk)?;
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::proposal::{Proposal, ProposalStatus, ProposalSummary};

    fn p(id: &str) -> Proposal {
        Proposal {
            id: id.into(),
            status: ProposalStatus::Computed,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: vec![],
            started_at_ms: 0,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 0,
        }
    }

    #[test]
    fn get_returns_inserted_proposal() {
        let s = ProposalStore::new(4);
        s.insert(p("a"));
        assert!(s.get("a").is_some());
        assert!(s.get("ghost").is_none());
    }

    #[test]
    fn ring_buffer_drops_oldest_at_capacity() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        assert!(s.get("a").is_none());
        assert!(s.get("b").is_some());
        assert!(s.get("c").is_some());
    }

    #[test]
    fn list_returns_most_recent_first_within_limit() {
        let s = ProposalStore::new(10);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        let listed: Vec<String> = s.list(2).into_iter().map(|p| p.id).collect();
        assert_eq!(listed, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn list_limit_zero_uses_capacity_default() {
        let s = ProposalStore::new(2);
        s.insert(p("a"));
        s.insert(p("b"));
        s.insert(p("c"));
        let listed: Vec<String> = s.list(0).into_iter().map(|p| p.id).collect();
        assert_eq!(listed, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn capacity_zero_clamped_to_one() {
        let s = ProposalStore::new(0);
        s.insert(p("a"));
        s.insert(p("b"));
        assert!(s.get("a").is_none());
        assert!(s.get("b").is_some());
    }

    #[test]
    fn mutate_updates_status_and_persists() {
        let s = ProposalStore::new(4);
        s.insert(p("a"));
        let updated = s
            .mutate("a", |pp| {
                pp.status = ProposalStatus::Executing;
                pp.started_at_ms = 42;
            })
            .expect("mutated");
        assert_eq!(updated.status, ProposalStatus::Executing);
        assert_eq!(updated.started_at_ms, 42);
        assert_eq!(s.get("a").unwrap().status, ProposalStatus::Executing);
    }

    #[test]
    fn mutate_returns_none_for_unknown_id() {
        let s = ProposalStore::new(4);
        assert!(s.mutate("ghost", |_| {}).is_none());
    }

    #[test]
    fn open_creates_empty_when_file_missing() {
        let dir = tempfile::tempdir().unwrap();
        let s = ProposalStore::open(dir.path(), 4).unwrap();
        assert!(s.list(0).is_empty());
    }

    #[test]
    fn persist_round_trips_via_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = ProposalStore::open(dir.path(), 4).unwrap();
            s.insert(p("a"));
            s.insert(p("b"));
            s.mutate("a", |pp| pp.status = ProposalStatus::Executing);
            // dropped here; relies on persist_if_durable having written
        }
        let s2 = ProposalStore::open(dir.path(), 4).unwrap();
        assert_eq!(s2.get("a").unwrap().status, ProposalStatus::Executing);
        assert!(s2.get("b").is_some());
    }

    #[test]
    fn open_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEFAULT_FILENAME);
        let bogus = r#"{"version":999,"capacity":4,"proposals":[]}"#;
        fs::write(&path, bogus).unwrap();
        let err = ProposalStore::open(dir.path(), 4).unwrap_err();
        assert!(matches!(err, StoreError::UnsupportedVersion { found: 999, expected: 1 }));
    }
}
```

- [ ] **Step 2: Verify the model module re-exports**

Check `crates/rebalancer/src/model/mod.rs` already re-exports `Proposal` and friends. If `ProposalStore` isn't re-exported at the model::* level, leave it — call sites already use `crate::model::store::ProposalStore`.

Add a re-export for `StoreError` if it's used outside this file in T9 (binary recovery path). Add this line to `model/mod.rs`:

```rust
pub use store::StoreError;
```

If the file doesn't already have a `pub use store::ProposalStore;` line, leave it as-is (call sites can use the full path).

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib model::store -- --nocapture`
Expected: 9 tests pass.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/model
git -C /home/matt/git/crabka commit -m "rebalancer(43b): ProposalStore atomic JSON persistence + status mutators

ProposalStore::open(data_dir, capacity) loads from
{data_dir}/proposals.json (schema version 1, atomic write via
tmp+rename) and persists on every insert/mutate. New mutate(id, f)
applies a closure to a proposal in place and persists. Round-trip
test, version-mismatch test, and existing ring-buffer tests
unchanged.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Executor state + phases (parallel: T5, T6)

### Task 5: `executor::state` — `InFlightFile` + atomic write helpers

**Files:**
- Create: `crates/rebalancer/src/executor/state.rs`
- Modify: `crates/rebalancer/src/executor/mod.rs` (add `pub mod state;`)

- [ ] **Step 1: Write `crates/rebalancer/src/executor/state.rs`**

```rust
//! On-disk active-execution marker. `{data_dir}/in_flight.json` exists
//! when an execution is in flight; its absence is the "idle" signal on
//! startup. Written atomically; deleted on terminal.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::model::proposal::ProposalStatus;

const FILE_VERSION: u32 = 1;
const FILENAME: &str = "in_flight.json";

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("schema version {found} not supported (expected {expected})")]
    UnsupportedVersion { found: u32, expected: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    ApplyThrottle,
    Submit,
    Wait,
    ClearThrottle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InFlightFile {
    pub version: u32,
    pub proposal_id: String,
    pub phase: Phase,
    pub started_at_ms: i64,
    pub throttle_bytes_per_sec: i64,
    /// Set when transitioning into ClearThrottle so a resume-during-clear
    /// knows which terminal status to commit.
    #[serde(default)]
    pub target_terminal_status: Option<ProposalStatus>,
    /// Stamped at the same time as `target_terminal_status` when
    /// `target = Failed`.
    #[serde(default)]
    pub failure_reason: Option<String>,
}

impl InFlightFile {
    #[must_use]
    pub fn new(
        proposal_id: String,
        phase: Phase,
        started_at_ms: i64,
        throttle_bytes_per_sec: i64,
    ) -> Self {
        Self {
            version: FILE_VERSION,
            proposal_id,
            phase,
            started_at_ms,
            throttle_bytes_per_sec,
            target_terminal_status: None,
            failure_reason: None,
        }
    }

    pub fn write(&self, data_dir: &Path) -> Result<(), StateError> {
        let path = path_of(data_dir);
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp, &bytes)?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    pub fn load(data_dir: &Path) -> Result<Option<Self>, StateError> {
        let path = path_of(data_dir);
        match fs::read(&path) {
            Ok(bytes) => {
                let parsed: Self = serde_json::from_slice(&bytes)?;
                if parsed.version != FILE_VERSION {
                    return Err(StateError::UnsupportedVersion {
                        found: parsed.version,
                        expected: FILE_VERSION,
                    });
                }
                Ok(Some(parsed))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn delete(data_dir: &Path) -> Result<(), StateError> {
        let path = path_of(data_dir);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

fn path_of(data_dir: &Path) -> PathBuf {
    data_dir.join(FILENAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_write_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut f = InFlightFile::new("p".into(), Phase::Submit, 42, 50_000_000);
        f.write(dir.path()).unwrap();
        let loaded = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.proposal_id, "p");
        assert_eq!(loaded.phase, Phase::Submit);
        assert_eq!(loaded.started_at_ms, 42);
        assert_eq!(loaded.target_terminal_status, None);

        f.phase = Phase::ClearThrottle;
        f.target_terminal_status = Some(ProposalStatus::Completed);
        f.write(dir.path()).unwrap();
        let loaded2 = InFlightFile::load(dir.path()).unwrap().unwrap();
        assert_eq!(loaded2.phase, Phase::ClearThrottle);
        assert_eq!(loaded2.target_terminal_status, Some(ProposalStatus::Completed));
    }

    #[test]
    fn load_returns_none_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn delete_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        InFlightFile::new("p".into(), Phase::Submit, 0, 0)
            .write(dir.path())
            .unwrap();
        InFlightFile::delete(dir.path()).unwrap();
        // Second delete is a no-op.
        InFlightFile::delete(dir.path()).unwrap();
        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn load_rejects_unsupported_version() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = r#"{"version":999,"proposal_id":"x","phase":"Submit","started_at_ms":0,"throttle_bytes_per_sec":0}"#;
        std::fs::write(dir.path().join(FILENAME), bogus).unwrap();
        let err = InFlightFile::load(dir.path()).unwrap_err();
        assert!(matches!(err, StateError::UnsupportedVersion { found: 999, expected: 1 }));
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/executor/mod.rs`. Insert `pub mod state;` after the existing `pub mod throttle;` line. Result:

```rust
//! ... (existing module docstring)

pub mod state;
pub mod throttle;
```

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib executor::state -- --nocapture`
Expected: 4 tests pass.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/executor
git -C /home/matt/git/crabka commit -m "rebalancer(43b): executor::state InFlightFile + atomic write/load

{data_dir}/in_flight.json encodes the active execution's proposal_id,
current phase, started_at_ms, throttle, and (when in ClearThrottle)
the target terminal status. Schema version 1; atomic write via tmp +
rename; delete is idempotent. Four unit tests cover round-trip,
missing-file, double-delete, and version-mismatch paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 6: `executor::phases` — `ClientFacade` trait + per-phase functions

**Files:**
- Create: `crates/rebalancer/src/executor/phases.rs`
- Modify: `crates/rebalancer/src/executor/mod.rs` (add `pub mod phases;`)

- [ ] **Step 1: Write `crates/rebalancer/src/executor/phases.rs`**

```rust
//! Per-phase action functions, decoupled from `crabka_client_core::Client`
//! via the `ClientFacade` trait so the state-machine tests can drive the
//! executor against a `MockClient`.

use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::executor::throttle::ThrottleTargets;
use crate::model::Movement;

/// A typed wrapper over the small set of admin RPCs the executor needs.
/// The production impl forwards to `crabka_client_core::Client::send`
/// with the generated `crabka_protocol::owned::*` request types. Tests
/// substitute a `MockClient`.
#[async_trait]
pub trait ClientFacade: Send + Sync {
    /// IncrementalAlterConfigs — sets or deletes the four KIP-73
    /// throttle keys derived from `targets` + `throttle_bytes_per_sec`.
    /// `op` is `Op::Set` for ApplyThrottle and `Op::Delete` for
    /// ClearThrottle.
    async fn alter_throttle_configs(
        &self,
        op: ConfigOp,
        targets: &ThrottleTargets,
        throttle_bytes_per_sec: i64,
    ) -> Result<(), PhaseError>;

    /// AlterPartitionReassignments — submits the partition movements
    /// in one request. Movements are passed pre-chunked by the caller
    /// (batch_size).
    async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError>;

    /// AlterPartitionReassignments with `null` replicas — cancels the
    /// listed partition reassignments. Used by Cancel + deadline-exceeded.
    async fn cancel_reassignments(&self, partitions: &[(String, i32)]) -> Result<(), PhaseError>;

    /// ListPartitionReassignments — returns the set of (topic, partition)
    /// keys that still have an in-flight reassignment, scoped to the
    /// caller's interest set.
    async fn list_in_flight(
        &self,
        of_interest: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>, PhaseError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOp {
    Set,
    Delete,
}

#[derive(Debug, thiserror::Error)]
pub enum PhaseError {
    #[error("broker rejected request: {0}")]
    Broker(String),
    #[error("client error: {0}")]
    Client(String),
}

/// Apply throttle: one IncrementalAlterConfigs request with all four
/// KIP-73 keys SET to the target values.
pub async fn apply_throttle(
    client: &dyn ClientFacade,
    targets: &ThrottleTargets,
    throttle_bytes_per_sec: i64,
) -> Result<(), PhaseError> {
    client
        .alter_throttle_configs(ConfigOp::Set, targets, throttle_bytes_per_sec)
        .await
}

/// Clear throttle: one IncrementalAlterConfigs request with all four
/// KIP-73 keys DELETED on the same resources. Idempotent — safe to
/// re-run.
pub async fn clear_throttle(
    client: &dyn ClientFacade,
    targets: &ThrottleTargets,
) -> Result<(), PhaseError> {
    client
        .alter_throttle_configs(ConfigOp::Delete, targets, 0)
        .await
}

/// Submit a movement plan, chunked at `batch_size`.
pub async fn submit_movements(
    client: &dyn ClientFacade,
    movements: &[Movement],
    batch_size: usize,
) -> Result<(), PhaseError> {
    for chunk in movements.chunks(batch_size.max(1)) {
        client.submit_reassignments(chunk).await?;
    }
    Ok(())
}

/// Track per-partition keys derived from a proposal — used to scope
/// ListPartitionReassignments + cancel calls to the proposal's surface.
#[must_use]
pub fn partition_keys(movements: &[Movement]) -> Vec<(String, i32)> {
    let mut m: BTreeMap<(String, i32), ()> = BTreeMap::new();
    for mv in movements {
        m.insert((mv.topic.clone(), mv.partition), ());
    }
    m.into_keys().collect()
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Mock that records every call. Tests inspect the recorded log to
    /// assert what the executor did.
    pub struct MockClient {
        pub calls: Mutex<Vec<MockCall>>,
        pub submit_remaining_failures: AtomicUsize,
        /// When >= 1, list_in_flight returns the proposal's full set
        /// for that many invocations, then empty thereafter (simulating
        /// reassignment completion).
        pub list_in_flight_remaining: AtomicUsize,
        pub list_scope: Mutex<Vec<(String, i32)>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum MockCall {
        AlterConfigs {
            op: ConfigOp,
            targets: ThrottleTargets,
            rate: i64,
        },
        Submit(Vec<Movement>),
        Cancel(Vec<(String, i32)>),
        ListInFlight(Vec<(String, i32)>),
    }

    impl MockClient {
        pub fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                submit_remaining_failures: AtomicUsize::new(0),
                list_in_flight_remaining: AtomicUsize::new(0),
                list_scope: Mutex::new(Vec::new()),
            }
        }

        pub fn calls(&self) -> Vec<MockCall> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ClientFacade for MockClient {
        async fn alter_throttle_configs(
            &self,
            op: ConfigOp,
            targets: &ThrottleTargets,
            throttle_bytes_per_sec: i64,
        ) -> Result<(), PhaseError> {
            self.calls.lock().unwrap().push(MockCall::AlterConfigs {
                op,
                targets: targets.clone(),
                rate: throttle_bytes_per_sec,
            });
            Ok(())
        }

        async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError> {
            if self.submit_remaining_failures.load(Ordering::SeqCst) > 0 {
                self.submit_remaining_failures.fetch_sub(1, Ordering::SeqCst);
                return Err(PhaseError::Broker("simulated".into()));
            }
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Submit(movements.to_vec()));
            Ok(())
        }

        async fn cancel_reassignments(
            &self,
            partitions: &[(String, i32)],
        ) -> Result<(), PhaseError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::Cancel(partitions.to_vec()));
            Ok(())
        }

        async fn list_in_flight(
            &self,
            of_interest: &[(String, i32)],
        ) -> Result<Vec<(String, i32)>, PhaseError> {
            self.calls
                .lock()
                .unwrap()
                .push(MockCall::ListInFlight(of_interest.to_vec()));
            let remaining = self.list_in_flight_remaining.load(Ordering::SeqCst);
            if remaining > 0 {
                self.list_in_flight_remaining.fetch_sub(1, Ordering::SeqCst);
                Ok(self.list_scope.lock().unwrap().clone())
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    #[tokio::test]
    async fn submit_movements_chunks_at_batch_size() {
        let client = MockClient::new();
        let ms = vec![mv("t", 0, vec![1], vec![2]), mv("t", 1, vec![1], vec![2]), mv("t", 2, vec![1], vec![2])];
        submit_movements(&client, &ms, 2).await.unwrap();
        let calls = client.calls();
        let submits: Vec<_> = calls
            .iter()
            .filter_map(|c| if let MockCall::Submit(m) = c { Some(m.len()) } else { None })
            .collect();
        assert_eq!(submits, vec![2, 1]);
    }

    #[tokio::test]
    async fn apply_throttle_then_clear_records_two_alter_configs() {
        let client = MockClient::new();
        let targets = crate::executor::throttle::compute_throttle_targets(&[mv(
            "t",
            0,
            vec![1],
            vec![2],
        )]);
        apply_throttle(&client, &targets, 50_000_000).await.unwrap();
        clear_throttle(&client, &targets).await.unwrap();
        let calls = client.calls();
        let ops: Vec<_> = calls
            .iter()
            .filter_map(|c| if let MockCall::AlterConfigs { op, .. } = c { Some(*op) } else { None })
            .collect();
        assert_eq!(ops, vec![ConfigOp::Set, ConfigOp::Delete]);
    }

    #[test]
    fn partition_keys_dedupes_and_sorts() {
        let ms = vec![
            mv("b", 1, vec![1], vec![2]),
            mv("a", 0, vec![1], vec![2]),
            mv("b", 1, vec![1], vec![3]),
        ];
        let keys = partition_keys(&ms);
        assert_eq!(
            keys,
            vec![("a".to_string(), 0), ("b".to_string(), 1)]
        );
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/executor/mod.rs`. Insert `pub mod phases;` so the module list reads:

```rust
//! ... (existing docstring)

pub mod phases;
pub mod state;
pub mod throttle;
```

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib executor::phases -- --nocapture`
Expected: 3 tests pass.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean.

The `async-trait` crate is already a workspace dep (used by 43a). If clippy flags `async_trait` style attributes, leave them — they're idiomatic.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/executor
git -C /home/matt/git/crabka commit -m "rebalancer(43b): executor::phases ClientFacade + per-phase fns + MockClient

ClientFacade trait abstracts the four admin RPCs the executor needs
(alter_throttle_configs, submit_reassignments, cancel_reassignments,
list_in_flight). Per-phase functions apply_throttle, clear_throttle,
submit_movements (chunked at batch_size). pub MockClient in
#[cfg(test)] for the state-machine tests in T7. Three unit tests
cover chunking, apply→clear sequencing, and partition_keys dedup.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Executor state machine

### Task 7: `executor::mod.rs` — `Executor`, `Execution`, state machine driver, production `ClientFacade` impl

**Files:**
- Modify: `crates/rebalancer/src/executor/mod.rs`
- Modify: `crates/rebalancer/src/metrics.rs` (new execution counters)

- [ ] **Step 1: Add execution counters to `metrics.rs`**

Edit `crates/rebalancer/src/metrics.rs`. Add three counters to the `RebalancerMetrics` struct + register them. Read the file first to understand its current shape, then insert the new fields + registry calls following the same pattern as the existing `proposals_created_total`. Specifically:

Add three new fields:
```rust
pub executions_started_total: Counter,
pub executions_completed_total: Counter,
pub executions_failed_total: Counter,
pub executions_cancelled_total: Counter,
```

Register them in `RebalancerMetrics::register` with helps:
- `executions_started_total`: "Total ExecuteProposal invocations that successfully entered Executing"
- `executions_completed_total`: "Total executions that reached Completed"
- `executions_failed_total`: "Total executions that reached Failed"
- `executions_cancelled_total`: "Total executions that reached Cancelled via CancelExecution"

- [ ] **Step 2: Replace `crates/rebalancer/src/executor/mod.rs`** with the full executor:

```rust
//! Execute-path state machine. `Executor` runs one `Execution` at a
//! time against the cluster via a `ClientFacade`.
//!
//! Slice 43b adds the full state machine (ApplyThrottle → Submit →
//! Wait → ClearThrottle) and on-disk persistence with restart resume.

pub mod phases;
pub mod state;
pub mod throttle;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::executor::phases::{
    apply_throttle, clear_throttle, partition_keys, submit_movements, ClientFacade, PhaseError,
};
use crate::executor::state::{InFlightFile, Phase, StateError};
use crate::executor::throttle::{compute_throttle_targets, ThrottleTargets};
use crate::metrics::RebalancerMetrics;
use crate::model::proposal::{Proposal, ProposalStatus};
use crate::model::store::ProposalStore;

/// Configuration controlling the executor's polling cadence and chunking.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub data_dir: PathBuf,
    pub default_throttle_bytes_per_sec: i64,
    pub poll_interval: Duration,
    pub execute_deadline: Duration,
    pub batch_size: usize,
}

/// State shared between `AppState` and the running execution task.
#[derive(Clone)]
pub struct ExecutorState {
    pub store: Arc<ProposalStore>,
    pub config: ExecutorConfig,
    pub metrics: RebalancerMetrics,
    pub in_flight: Arc<Mutex<Option<ExecutionHandle>>>,
}

/// Handle to an active execution task.
pub struct ExecutionHandle {
    pub proposal_id: String,
    pub task: JoinHandle<()>,
    pub cancel: CancellationToken,
    pub started_at: Instant,
}

/// One run of the state machine.
pub struct Execution<C: ClientFacade + 'static> {
    client: Arc<C>,
    state: ExecutorState,
    proposal: Proposal,
    targets: ThrottleTargets,
    throttle_bytes_per_sec: i64,
    cancel: CancellationToken,
    starting_phase: Phase,
}

impl<C: ClientFacade + 'static> Execution<C> {
    /// Build a fresh execution starting from `ApplyThrottle`.
    pub fn new(
        client: Arc<C>,
        state: ExecutorState,
        proposal: Proposal,
        throttle_bytes_per_sec: i64,
        cancel: CancellationToken,
    ) -> Self {
        let targets = compute_throttle_targets(&proposal.movements);
        Self {
            client,
            state,
            proposal,
            targets,
            throttle_bytes_per_sec,
            cancel,
            starting_phase: Phase::ApplyThrottle,
        }
    }

    /// Resume from a persisted phase (recovery on startup).
    pub fn resume(
        client: Arc<C>,
        state: ExecutorState,
        proposal: Proposal,
        in_flight: InFlightFile,
        cancel: CancellationToken,
    ) -> Self {
        let targets = compute_throttle_targets(&proposal.movements);
        Self {
            client,
            state,
            proposal,
            targets,
            throttle_bytes_per_sec: in_flight.throttle_bytes_per_sec,
            cancel,
            starting_phase: in_flight.phase,
        }
    }

    /// Drive the state machine to a terminal status. Always clears
    /// throttle before returning.
    pub async fn run(mut self) {
        let mut phase = self.starting_phase;
        // Best-effort persist on every phase transition.
        let _ = self.persist_phase(phase, None, None);

        let mut terminal: Option<(ProposalStatus, Option<String>)> = None;

        loop {
            match phase {
                Phase::ApplyThrottle => match self.do_apply_throttle().await {
                    Ok(()) => {
                        phase = Phase::Submit;
                        let _ = self.persist_phase(phase, None, None);
                    }
                    Err(e) => {
                        terminal = Some((ProposalStatus::Failed, Some(format!("ApplyThrottle: {e}"))));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Failed), Some(format!("ApplyThrottle: {e}")));
                    }
                },
                Phase::Submit => match self.do_submit().await {
                    Ok(()) => {
                        phase = Phase::Wait;
                        let _ = self.persist_phase(phase, None, None);
                    }
                    Err(e) => {
                        terminal = Some((ProposalStatus::Failed, Some(format!("Submit: {e}"))));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Failed), Some(format!("Submit: {e}")));
                    }
                },
                Phase::Wait => match self.do_wait().await {
                    WaitOutcome::Completed => {
                        terminal = Some((ProposalStatus::Completed, None));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Completed), None);
                    }
                    WaitOutcome::Cancelled => {
                        // Cancel revert + clear.
                        let _ = self.cancel_in_flight().await;
                        terminal = Some((ProposalStatus::Cancelled, None));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Cancelled), None);
                    }
                    WaitOutcome::DeadlineExceeded => {
                        let _ = self.cancel_in_flight().await;
                        terminal = Some((ProposalStatus::Failed, Some("Wait: deadline exceeded".into())));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Failed), Some("Wait: deadline exceeded".into()));
                    }
                    WaitOutcome::Error(e) => {
                        terminal = Some((ProposalStatus::Failed, Some(format!("Wait: {e}"))));
                        phase = Phase::ClearThrottle;
                        let _ = self.persist_phase(phase, Some(ProposalStatus::Failed), Some(format!("Wait: {e}")));
                    }
                },
                Phase::ClearThrottle => {
                    // Best-effort. We commit the terminal regardless of clear errors.
                    if let Err(e) = self.do_clear_throttle().await {
                        warn!(error = %e, "clear throttle failed; proposal still moves to terminal");
                    }
                    let (status, reason) = terminal.clone().unwrap_or((ProposalStatus::Completed, None));
                    self.commit_terminal(status, reason);
                    let _ = self.cleanup_in_flight_file();
                    return;
                }
            }
        }
    }

    async fn do_apply_throttle(&self) -> Result<(), PhaseError> {
        // Cancel before phase work runs is treated as cancel at this boundary.
        if self.cancel.is_cancelled() {
            return Err(PhaseError::Broker("cancelled before ApplyThrottle".into()));
        }
        apply_throttle(self.client.as_ref(), &self.targets, self.throttle_bytes_per_sec).await
    }

    async fn do_submit(&self) -> Result<(), PhaseError> {
        if self.cancel.is_cancelled() {
            return Err(PhaseError::Broker("cancelled before Submit".into()));
        }
        submit_movements(
            self.client.as_ref(),
            &self.proposal.movements,
            self.state.config.batch_size,
        )
        .await
    }

    async fn do_wait(&self) -> WaitOutcome {
        let scope = partition_keys(&self.proposal.movements);
        let mut ticker = tokio::time::interval(self.state.config.poll_interval);
        let deadline = tokio::time::Instant::now() + self.state.config.execute_deadline;
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if tokio::time::Instant::now() >= deadline {
                        return WaitOutcome::DeadlineExceeded;
                    }
                    match self.client.list_in_flight(&scope).await {
                        Ok(remaining) if remaining.is_empty() => return WaitOutcome::Completed,
                        Ok(_) => continue,
                        Err(e) => return WaitOutcome::Error(e),
                    }
                }
                () = self.cancel.cancelled() => return WaitOutcome::Cancelled,
            }
        }
    }

    async fn cancel_in_flight(&self) -> Result<(), PhaseError> {
        let scope = partition_keys(&self.proposal.movements);
        if scope.is_empty() {
            return Ok(());
        }
        self.client.cancel_reassignments(&scope).await
    }

    async fn do_clear_throttle(&self) -> Result<(), PhaseError> {
        clear_throttle(self.client.as_ref(), &self.targets).await
    }

    fn persist_phase(
        &self,
        phase: Phase,
        target: Option<ProposalStatus>,
        reason: Option<String>,
    ) -> Result<(), StateError> {
        let mut f = InFlightFile::new(
            self.proposal.id.clone(),
            phase,
            self.proposal.started_at_ms,
            self.throttle_bytes_per_sec,
        );
        f.target_terminal_status = target;
        f.failure_reason = reason;
        f.write(&self.state.config.data_dir)
    }

    fn cleanup_in_flight_file(&self) -> Result<(), StateError> {
        InFlightFile::delete(&self.state.config.data_dir)
    }

    fn commit_terminal(&self, status: ProposalStatus, reason: Option<String>) {
        let now = now_ms();
        let id = self.proposal.id.clone();
        let updated = self.state.store.mutate(&id, |p| {
            p.status = status;
            p.terminated_at_ms = now;
            if let Some(r) = &reason {
                p.failure_reason = Some(r.clone());
            }
        });
        match status {
            ProposalStatus::Completed => self.state.metrics.executions_completed_total.inc(),
            ProposalStatus::Failed => self.state.metrics.executions_failed_total.inc(),
            ProposalStatus::Cancelled => self.state.metrics.executions_cancelled_total.inc(),
            _ => 0,
        };
        if updated.is_none() {
            error!(proposal_id = %id, "commit_terminal: proposal vanished from store");
        }
        // Release the in-flight slot last; CancelExecution holders see Some until now.
        let in_flight = self.state.in_flight.clone();
        tokio::spawn(async move {
            in_flight.lock().await.take();
        });
        info!(proposal_id = %id, status = ?status, "execution terminal");
    }
}

#[derive(Debug)]
enum WaitOutcome {
    Completed,
    Cancelled,
    DeadlineExceeded,
    Error(PhaseError),
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::phases::tests::{MockClient, MockCall};
    use crate::model::proposal::{ProposalSummary};
    use crate::model::Movement;

    fn cfg(dir: &std::path::Path) -> ExecutorConfig {
        ExecutorConfig {
            data_dir: dir.to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(5),
            execute_deadline: Duration::from_secs(5),
            batch_size: 200,
        }
    }

    fn state_with_store(dir: &std::path::Path, p: Proposal) -> ExecutorState {
        let store = Arc::new(ProposalStore::new(20));
        store.insert(p);
        let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
        let metrics = RebalancerMetrics::register(&mut registry);
        ExecutorState {
            store,
            config: cfg(dir),
            metrics,
            in_flight: Arc::new(Mutex::new(None)),
        }
    }

    fn proposal_with_movements(id: &str, ms: Vec<Movement>) -> Proposal {
        Proposal {
            id: id.into(),
            status: ProposalStatus::Executing,
            created_at_ms: 0,
            goals_applied: vec![],
            summary: ProposalSummary::default(),
            movements: ms,
            started_at_ms: 1,
            terminated_at_ms: 0,
            failure_reason: None,
            throttle_bytes_per_sec: 50_000_000,
        }
    }

    fn mv(topic: &str, p: i32, old: Vec<i32>, new: Vec<i32>) -> Movement {
        Movement {
            topic: topic.into(),
            partition: p,
            old_replicas: old,
            new_replicas: new,
            old_leader: 0,
            new_leader: 0,
        }
    }

    #[tokio::test]
    async fn happy_path_apply_submit_wait_clear() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let calls = client.calls();
        let kinds: Vec<&str> = calls
            .iter()
            .map(|c| match c {
                MockCall::AlterConfigs { op, .. } => match op {
                    crate::executor::phases::ConfigOp::Set => "set",
                    crate::executor::phases::ConfigOp::Delete => "del",
                },
                MockCall::Submit(_) => "submit",
                MockCall::Cancel(_) => "cancel",
                MockCall::ListInFlight(_) => "list",
            })
            .collect();
        assert_eq!(kinds.first(), Some(&"set"));
        assert_eq!(kinds.last(), Some(&"del"));
        assert!(kinds.contains(&"submit"));

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Completed);
        assert!(after.terminated_at_ms > 0);

        // in_flight.json should be gone.
        assert!(InFlightFile::load(dir.path()).unwrap().is_none());
    }

    #[tokio::test]
    async fn submit_failure_routes_through_clear_to_failed() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        client
            .submit_remaining_failures
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);

        let cancel = CancellationToken::new();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Failed);
        assert!(after.failure_reason.as_deref().unwrap().contains("Submit"));
        // Clear ran.
        let kinds: Vec<&str> = client
            .calls()
            .iter()
            .map(|c| match c {
                MockCall::AlterConfigs { op, .. } => match op {
                    crate::executor::phases::ConfigOp::Set => "set",
                    crate::executor::phases::ConfigOp::Delete => "del",
                },
                MockCall::Submit(_) => "submit",
                MockCall::Cancel(_) => "cancel",
                MockCall::ListInFlight(_) => "list",
            })
            .collect();
        assert!(kinds.contains(&"del"));
    }

    #[tokio::test]
    async fn cancel_during_wait_results_in_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        let client = Arc::new(MockClient::new());
        client
            .list_in_flight_remaining
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
        *client.list_scope.lock().unwrap() = vec![("t".into(), 0)];

        let cancel = CancellationToken::new();
        let cancel_for_caller = cancel.clone();
        let exec = Execution::new(client.clone(), state.clone(), p, 50_000_000, cancel);
        let handle = tokio::spawn(async move {
            exec.run().await;
        });
        // Let the loop reach the Wait phase + spin once.
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel_for_caller.cancel();
        handle.await.unwrap();

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Cancelled);
        // Cancel should issue a cancel_reassignments call.
        let cancels: usize = client
            .calls()
            .iter()
            .filter(|c| matches!(c, MockCall::Cancel(_)))
            .count();
        assert!(cancels >= 1);
    }

    #[tokio::test]
    async fn resume_from_clear_throttle_commits_target_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let p = proposal_with_movements("p1", vec![mv("t", 0, vec![1], vec![2])]);
        let state = state_with_store(dir.path(), p.clone());

        // Pre-stage in_flight.json reflecting a Completed-targeted ClearThrottle.
        let mut f = InFlightFile::new(p.id.clone(), Phase::ClearThrottle, 1, 50_000_000);
        f.target_terminal_status = Some(ProposalStatus::Completed);
        f.write(dir.path()).unwrap();

        let client = Arc::new(MockClient::new());
        let cancel = CancellationToken::new();
        let in_flight = InFlightFile::load(dir.path()).unwrap().unwrap();
        let exec = Execution::resume(client.clone(), state.clone(), p, in_flight, cancel);
        exec.run().await;

        let after = state.store.get("p1").unwrap();
        assert_eq!(after.status, ProposalStatus::Completed);
        // Single DELETE call (re-running clear).
        let dels: usize = client
            .calls()
            .iter()
            .filter(|c| matches!(c, MockCall::AlterConfigs { op: crate::executor::phases::ConfigOp::Delete, .. }))
            .count();
        assert_eq!(dels, 1);
    }
}
```

- [ ] **Step 3: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib executor -- --nocapture`
Expected: 4 state-machine tests pass + the throttle / phases / state tests still pass (~11 total in `executor::*`).

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean. The workspace's `clippy::pedantic` may flag a few things; preserve behavior while making it clean (see 43a's pattern for `try_from` substitutions).

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/executor crates/rebalancer/src/metrics.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43b): executor::mod state machine + execution counters

Execution drives ApplyThrottle → Submit → Wait → ClearThrottle.
Cancel + deadline-exceeded both route through cancel_reassignments
(KIP-455 null replicas) before ClearThrottle. ClearThrottle runs in
every terminal path. Resume from ClearThrottle commits the persisted
target_terminal_status. Four state-machine unit tests cover happy
path / submit failure / cancel during wait / resume into
ClearThrottle. New metrics: executions_started_total /
completed_total / failed_total / cancelled_total.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 6 — API handlers

### Task 8: Wire `ExecuteProposal` body + `CancelExecution` handler

**Files:**
- Modify: `crates/rebalancer/src/api/handlers.rs`
- Modify: `crates/rebalancer/src/api/mod.rs` (wire the new RPC into the builder, plus `AppState` carries the executor surface)

- [ ] **Step 1: Extend `AppState`**

Edit `crates/rebalancer/src/api/handlers.rs`. Replace the existing `AppState` struct with:

```rust
/// State shared across all RPC handlers. Wired into axum via an
/// `Extension(Arc<AppState>)` layer applied to the generated router.
pub struct AppState {
    pub snapshot: SharedSnapshot,
    pub store: Arc<ProposalStore>,
    pub goal_registry: super::GoalRegistry,
    pub goal_ctx: crate::goals::GoalContext,
    pub metrics: RebalancerMetrics,
    // new in 43b:
    pub executor: crate::executor::ExecutorState,
    pub client_facade: Arc<dyn crate::executor::phases::ClientFacade>,
}
```

Replace the imports at the top of the file so the new types resolve:

```rust
use std::sync::Arc;

use axum::Extension;
use connectrpc_axum::message::error::Code;
use connectrpc_axum::message::{ConnectError, ConnectRequest, ConnectResponse};
use tokio_util::sync::CancellationToken;

use crate::executor::{Execution, ExecutionHandle};
use crate::ingest::SharedSnapshot;
use crate::metrics::RebalancerMetrics;
use crate::model::proposal::{Proposal, ProposalStatus};
use crate::model::{ClusterState, ProposalStore};
use crate::optimizer;
use crate::pb;
```

The existing `RebalancerMetrics` re-export should already be reachable via `crate::metrics::RebalancerMetrics`; the import above makes that explicit.

- [ ] **Step 2: Extend `proposal_to_proto`** to populate the new fields

In the same file, replace the existing `proposal_to_proto` with:

```rust
#[must_use]
pub fn proposal_to_proto(p: &Proposal) -> pb::Proposal {
    pb::Proposal {
        id: p.id.clone(),
        status: i32::from(status_to_proto(p.status)),
        created_at_ms: p.created_at_ms,
        goals_applied: p.goals_applied.clone(),
        summary: Some(pb::ProposalSummary {
            replica_movements: p.summary.replica_movements,
            leader_movements: p.summary.leader_movements,
            max_replicas_before: p.summary.max_replicas_before,
            max_replicas_after: p.summary.max_replicas_after,
            max_leaders_before: p.summary.max_leaders_before,
            max_leaders_after: p.summary.max_leaders_after,
        }),
        movements: p
            .movements
            .iter()
            .map(|m| pb::Movement {
                topic: m.topic.clone(),
                partition: m.partition,
                old_replicas: m.old_replicas.clone(),
                new_replicas: m.new_replicas.clone(),
                old_leader: m.old_leader,
                new_leader: m.new_leader,
            })
            .collect(),
        started_at_ms: p.started_at_ms,
        terminated_at_ms: p.terminated_at_ms,
        failure_reason: p.failure_reason.clone(),
        throttle_bytes_per_sec: p.throttle_bytes_per_sec,
    }
}

#[must_use]
fn status_to_proto(s: ProposalStatus) -> pb::ProposalStatus {
    match s {
        ProposalStatus::Computed => pb::ProposalStatus::Computed,
        ProposalStatus::Executing => pb::ProposalStatus::Executing,
        ProposalStatus::Completed => pb::ProposalStatus::Completed,
        ProposalStatus::Failed => pb::ProposalStatus::Failed,
        ProposalStatus::Cancelled => pb::ProposalStatus::Cancelled,
    }
}
```

- [ ] **Step 3: Replace the `execute_proposal` handler body** with the real implementation

```rust
/// Kick off an execution. Returns Executing-state proposal; operator
/// polls GetProposal for progress. Async — the executor runs on a
/// detached task.
pub async fn execute_proposal(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::ExecuteProposalRequest>,
) -> Result<ConnectResponse<pb::ExecuteProposalResponse>, ConnectError> {
    let inner = req.0;
    let id = inner.id;
    let throttle_bytes_per_sec = inner
        .throttle_bytes_per_sec
        .unwrap_or(state.executor.config.default_throttle_bytes_per_sec);

    let proposal = state.store.get(&id).ok_or_else(|| {
        ConnectError::new(Code::NotFound, format!("proposal `{id}` not found"))
    })?;
    if proposal.status.is_terminal() || matches!(proposal.status, ProposalStatus::Executing) {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            format!("proposal `{id}` is {:?} (must be Computed)", proposal.status),
        ));
    }
    if proposal.movements.is_empty() {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            format!("proposal `{id}` has no movements"),
        ));
    }

    // Acquire the in-flight slot.
    let mut slot = state.executor.in_flight.lock().await;
    if slot.is_some() {
        return Err(ConnectError::new(
            Code::FailedPrecondition,
            "another execution is already in flight",
        ));
    }

    let now = now_ms();
    let updated = state
        .store
        .mutate(&id, |p| {
            p.status = ProposalStatus::Executing;
            p.started_at_ms = now;
            p.throttle_bytes_per_sec = throttle_bytes_per_sec;
        })
        .ok_or_else(|| ConnectError::new(Code::Internal, "store.mutate vanished"))?;

    let cancel = CancellationToken::new();
    let executor_state = state.executor.clone();
    let client = state.client_facade.clone();
    let prop_for_task = updated.clone();
    let cancel_for_task = cancel.clone();

    let task = tokio::spawn(async move {
        Execution::new(client, executor_state, prop_for_task, throttle_bytes_per_sec, cancel_for_task)
            .run()
            .await;
    });

    *slot = Some(ExecutionHandle {
        proposal_id: id.clone(),
        task,
        cancel,
        started_at: std::time::Instant::now(),
    });
    drop(slot);

    state.executor.metrics.executions_started_total.inc();

    Ok(ConnectResponse(pb::ExecuteProposalResponse {
        proposal: Some(proposal_to_proto(&updated)),
    }))
}
```

- [ ] **Step 4: Add the `cancel_execution` handler**

Append to the same file:

```rust
/// Signal cancellation on the in-flight execution. Returns the proposal
/// already transitioned to `Cancelled`.
pub async fn cancel_execution(
    Extension(state): Extension<Arc<AppState>>,
    req: ConnectRequest<pb::CancelExecutionRequest>,
) -> Result<ConnectResponse<pb::CancelExecutionResponse>, ConnectError> {
    let id = req.0.id;

    let cancel_token = {
        let slot = state.executor.in_flight.lock().await;
        let Some(handle) = slot.as_ref() else {
            return Err(ConnectError::new(
                Code::NotFound,
                "no execution in flight",
            ));
        };
        if handle.proposal_id != id {
            return Err(ConnectError::new(
                Code::FailedPrecondition,
                format!(
                    "in-flight execution is `{}`, not `{id}`",
                    handle.proposal_id
                ),
            ));
        }
        handle.cancel.clone()
    };

    cancel_token.cancel();

    // Spin briefly waiting for the executor task to release the slot
    // and update the store. Bound to 5s; if the executor doesn't drain
    // in that time, return the current (Executing) proposal — the
    // operator can re-poll.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let proposal = state
            .store
            .get(&id)
            .ok_or_else(|| ConnectError::new(Code::NotFound, format!("proposal `{id}` vanished")))?;
        if matches!(
            proposal.status,
            ProposalStatus::Cancelled | ProposalStatus::Failed | ProposalStatus::Completed
        ) {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        if std::time::Instant::now() >= deadline {
            return Ok(ConnectResponse(pb::CancelExecutionResponse {
                proposal: Some(proposal_to_proto(&proposal)),
            }));
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
```

- [ ] **Step 5: Wire `cancel_execution` into the builder**

Edit `crates/rebalancer/src/api/mod.rs`. Update the `router()` function to register the new RPC on the builder. After the existing `.execute_proposal(handlers::execute_proposal)` line, add:

```rust
        .cancel_execution(handlers::cancel_execution)
```

The full call chain should read:

```rust
pub fn router(state: Arc<handlers::AppState>) -> axum::Router {
    RebalancerServiceBuilder::<()>::new()
        .get_state(handlers::get_state)
        .create_proposal(handlers::create_proposal)
        .dry_run_proposal(handlers::dry_run_proposal)
        .get_proposal(handlers::get_proposal)
        .list_proposals(handlers::list_proposals)
        .execute_proposal(handlers::execute_proposal)
        .cancel_execution(handlers::cancel_execution)
        .build()
        .layer(axum::Extension(state))
}
```

- [ ] **Step 6: Build**

Run: `cargo build -p crabka-rebalancer`
Expected: clean. The build pulls in the new RPC's codegen.

If the build fails because `client_facade` type doesn't satisfy axum's `Send + Sync` bounds on `Extension`, adjust the trait object (`Arc<dyn ClientFacade + Send + Sync>` — already `Send + Sync` per the trait's supertraits, so this should just work).

- [ ] **Step 7: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer --lib api -- --nocapture`
Expected: GoalRegistry tests still pass (3 tests). Handler-level tests for ExecuteProposal/CancelExecution are deferred to the e2e tests in T10.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean. Apply `try_from` for any cast lints.

- [ ] **Step 8: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/api
git -C /home/matt/git/crabka commit -m "rebalancer(43b): api wires ExecuteProposal + CancelExecution

ExecuteProposal acquires the in-flight slot, transitions proposal to
Executing in the store, spawns an Execution task. Returns the
transitioning proposal in the response. CancelExecution signals the
in-flight CancellationToken and waits up to 5s for the task to
release the slot. AppState carries the ExecutorState +
ClientFacade. proposal_to_proto and the new status_to_proto map the
extended ProposalStatus variants to the proto.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 7 — Binary wiring + recovery

### Task 9: `bin/rebalancer.rs` — new CLI flags, production ClientFacade, recovery on startup

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`
- Create: `crates/rebalancer/src/executor/client_impl.rs` (production ClientFacade impl using crabka_client_core::Client)
- Modify: `crates/rebalancer/src/executor/mod.rs` (mount `client_impl`)

- [ ] **Step 1: Write the production `ClientFacade` impl**

Create `crates/rebalancer/src/executor/client_impl.rs`:

```rust
//! Production `ClientFacade` over `crabka_client_core::Client`. Maps
//! each trait method to the corresponding admin RPC via raw
//! `Client::send`, mirroring the ingester pattern from 43a.

use async_trait::async_trait;
use crabka_client_core::Client;
use crabka_protocol::owned::alter_partition_reassignments_request::{
    AlterPartitionReassignmentsRequest, ReassignableTopic, ReassignablePartition,
};
use crabka_protocol::owned::incremental_alter_configs_request::{
    AlterableConfig, AlterConfigsResource, IncrementalAlterConfigsRequest,
};
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use std::collections::BTreeMap;

use crate::executor::phases::{ClientFacade, ConfigOp, PhaseError};
use crate::executor::throttle::ThrottleTargets;
use crate::model::Movement;

/// Kafka admin resource type ids.
const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

/// IncrementalAlterConfigs op type ids.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

const RATE_KEY_LEADER: &str = "leader.replication.throttled.rate";
const RATE_KEY_FOLLOWER: &str = "follower.replication.throttled.rate";
const REPLICAS_KEY_LEADER: &str = "leader.replication.throttled.replicas";
const REPLICAS_KEY_FOLLOWER: &str = "follower.replication.throttled.replicas";

pub struct LiveClient {
    pub inner: Client,
}

impl LiveClient {
    #[must_use]
    pub fn new(inner: Client) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl ClientFacade for LiveClient {
    async fn alter_throttle_configs(
        &self,
        op: ConfigOp,
        targets: &ThrottleTargets,
        throttle_bytes_per_sec: i64,
    ) -> Result<(), PhaseError> {
        let op_byte = match op {
            ConfigOp::Set => OP_SET,
            ConfigOp::Delete => OP_DELETE,
        };
        let rate_str = throttle_bytes_per_sec.to_string();
        let mut resources: Vec<AlterConfigsResource> = Vec::new();

        // Per-broker rate configs.
        for broker in &targets.leader_brokers {
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: broker.to_string(),
                configs: vec![AlterableConfig {
                    name: RATE_KEY_LEADER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some(rate_str.clone()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        for broker in &targets.follower_brokers {
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: broker.to_string(),
                configs: vec![AlterableConfig {
                    name: RATE_KEY_FOLLOWER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some(rate_str.clone()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                }],
                ..Default::default()
            });
        }

        // Per-topic replicas configs.
        let topics: BTreeMap<String, (Option<&str>, Option<&str>)> = {
            let mut m: BTreeMap<String, (Option<&str>, Option<&str>)> = BTreeMap::new();
            for (topic, val) in &targets.leader_replicas_per_topic {
                m.entry(topic.clone()).or_default().0 = Some(val.as_str());
            }
            for (topic, val) in &targets.follower_replicas_per_topic {
                m.entry(topic.clone()).or_default().1 = Some(val.as_str());
            }
            m
        };

        for (topic, (leader_val, follower_val)) in &topics {
            let mut configs = Vec::new();
            if let Some(v) = leader_val {
                configs.push(AlterableConfig {
                    name: REPLICAS_KEY_LEADER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some((*v).to_string()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                });
            }
            if let Some(v) = follower_val {
                configs.push(AlterableConfig {
                    name: REPLICAS_KEY_FOLLOWER.into(),
                    config_operation: op_byte,
                    value: match op {
                        ConfigOp::Set => Some((*v).to_string()),
                        ConfigOp::Delete => None,
                    },
                    ..Default::default()
                });
            }
            resources.push(AlterConfigsResource {
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: topic.clone(),
                configs,
                ..Default::default()
            });
        }

        let req = IncrementalAlterConfigsRequest {
            resources,
            ..Default::default()
        };
        let _resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        Ok(())
    }

    async fn submit_reassignments(&self, movements: &[Movement]) -> Result<(), PhaseError> {
        let mut topics_map: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
        for m in movements {
            topics_map.entry(m.topic.clone()).or_default().push(ReassignablePartition {
                partition_index: m.partition,
                replicas: Some(m.new_replicas.clone()),
                ..Default::default()
            });
        }
        let topics: Vec<ReassignableTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect();
        let req = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            topics,
            ..Default::default()
        };
        let _resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        Ok(())
    }

    async fn cancel_reassignments(&self, partitions: &[(String, i32)]) -> Result<(), PhaseError> {
        let mut topics_map: BTreeMap<String, Vec<ReassignablePartition>> = BTreeMap::new();
        for (topic, partition) in partitions {
            topics_map.entry(topic.clone()).or_default().push(ReassignablePartition {
                partition_index: *partition,
                replicas: None, // null = cancel
                ..Default::default()
            });
        }
        let topics: Vec<ReassignableTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| ReassignableTopic {
                name,
                partitions,
                ..Default::default()
            })
            .collect();
        let req = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            topics,
            ..Default::default()
        };
        let _resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        Ok(())
    }

    async fn list_in_flight(
        &self,
        of_interest: &[(String, i32)],
    ) -> Result<Vec<(String, i32)>, PhaseError> {
        // Send with `topics = None` (all in-flight), then filter to `of_interest`.
        let req = ListPartitionReassignmentsRequest::default();
        let resp = self
            .inner
            .send(req)
            .await
            .map_err(|e| PhaseError::Client(e.to_string()))?;
        let want: std::collections::HashSet<(String, i32)> = of_interest.iter().cloned().collect();
        let mut out = Vec::new();
        for t in &resp.topics {
            for p in &t.partitions {
                let key = (t.name.clone(), p.partition_index);
                if want.contains(&key) {
                    out.push(key);
                }
            }
        }
        Ok(out)
    }
}
```

- [ ] **Step 2: Mount the production impl**

Edit `crates/rebalancer/src/executor/mod.rs`. Add `pub mod client_impl;` at the top alongside the other modules. Result:

```rust
//! ...

pub mod client_impl;
pub mod phases;
pub mod state;
pub mod throttle;
```

- [ ] **Step 3: Update `bin/rebalancer.rs` with the new CLI flags + recovery wiring**

Read the current `bin/rebalancer.rs`, then rewrite to add the new flags, the executor state, recovery on startup, and the binary's data-dir handling. The full file becomes:

```rust
//! `crabka-rebalancer` — Cruise-Control-equivalent partition
//! rebalancer for Crabka clusters.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crabka_rebalancer::api::handlers::AppState;
use crabka_rebalancer::api::GoalRegistry;
use crabka_rebalancer::executor::client_impl::LiveClient;
use crabka_rebalancer::executor::state::InFlightFile;
use crabka_rebalancer::executor::{Execution, ExecutionHandle, ExecutorConfig, ExecutorState};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::{new_registry, HealthState};
use crabka_rebalancer::ingest::{new_shared_snapshot, Ingester};
use crabka_rebalancer::metrics::RebalancerMetrics;
use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus};
use crabka_rebalancer::model::store::ProposalStore;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-rebalancer",
    version,
    about = "Cruise-Control-equivalent partition rebalancer"
)]
struct Args {
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,

    #[arg(long, env = "CRABKA_REBALANCER_LISTEN_ADDR", default_value = "0.0.0.0:9300")]
    listen_addr: SocketAddr,

    #[arg(long, env = "CRABKA_SCRAPE_INTERVAL_SECS", default_value_t = 10)]
    scrape_interval_secs: u64,

    #[arg(long, env = "CRABKA_IMBALANCE_THRESHOLD_PCT", default_value_t = 10)]
    imbalance_threshold_pct: u32,

    #[arg(long, env = "CRABKA_MAX_MOVEMENTS_PER_PROPOSAL", default_value_t = 256)]
    max_movements_per_proposal: usize,

    #[arg(long, env = "CRABKA_PROPOSAL_RING_BUFFER_SIZE", default_value_t = 20)]
    proposal_ring_buffer_size: usize,

    /// On-disk persistence directory. Created if missing.
    #[arg(long, env = "CRABKA_DATA_DIR", default_value = "/var/lib/crabka-rebalancer")]
    data_dir: PathBuf,

    #[arg(long, env = "CRABKA_DEFAULT_THROTTLE_BYTES_PER_SEC", default_value_t = 50_000_000)]
    default_throttle_bytes_per_sec: i64,

    #[arg(long, env = "CRABKA_EXECUTE_DEADLINE_SECS", default_value_t = 1800)]
    execute_deadline_secs: u64,

    #[arg(long, env = "CRABKA_REASSIGNMENT_POLL_INTERVAL_SECS", default_value_t = 5)]
    reassignment_poll_interval_secs: u64,

    #[arg(long, env = "CRABKA_REASSIGNMENT_BATCH_SIZE", default_value_t = 200)]
    reassignment_batch_size: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "crabka_rebalancer=info,info".into()),
        )
        .init();

    let args = Args::parse();
    info!(listen = %args.listen_addr, bootstrap = %args.bootstrap_servers, data_dir = ?args.data_dir, "crabka-rebalancer starting");

    std::fs::create_dir_all(&args.data_dir)?;

    let client = crabka_client_core::Client::builder()
        .bootstrap(args.bootstrap_servers.clone())
        .client_id("crabka-rebalancer")
        .build()
        .await?;

    let snapshot = new_shared_snapshot();
    let shutdown = CancellationToken::new();
    let mut registry = new_registry();
    let metrics = RebalancerMetrics::register(&mut registry);

    let store = Arc::new(ProposalStore::open(&args.data_dir, args.proposal_ring_buffer_size)?);

    let ingester = Ingester::new(
        client.clone(),
        Duration::from_secs(args.scrape_interval_secs),
        snapshot.clone(),
        shutdown.clone(),
        metrics.clone(),
    );
    let ingester_handle = tokio::spawn(ingester.run());

    let executor_config = ExecutorConfig {
        data_dir: args.data_dir.clone(),
        default_throttle_bytes_per_sec: args.default_throttle_bytes_per_sec,
        poll_interval: Duration::from_secs(args.reassignment_poll_interval_secs),
        execute_deadline: Duration::from_secs(args.execute_deadline_secs),
        batch_size: args.reassignment_batch_size,
    };

    let in_flight_slot = Arc::new(Mutex::new(None::<ExecutionHandle>));
    let executor_state = ExecutorState {
        store: store.clone(),
        config: executor_config,
        metrics: metrics.clone(),
        in_flight: in_flight_slot.clone(),
    };

    let live_client: Arc<dyn crabka_rebalancer::executor::phases::ClientFacade> =
        Arc::new(LiveClient::new(client.clone()));

    // Recovery on startup: replay in_flight.json if present.
    if let Some(in_flight) = InFlightFile::load(&args.data_dir)? {
        info!(proposal_id = %in_flight.proposal_id, phase = ?in_flight.phase, "recovering in-flight execution");
        if let Some(proposal) = store.get(&in_flight.proposal_id) {
            // Re-mark Executing in memory in case persist beat the crash.
            let prop_for_resume = store
                .mutate(&in_flight.proposal_id, |p| {
                    p.status = ProposalStatus::Executing;
                })
                .unwrap_or(proposal);
            let cancel = CancellationToken::new();
            let handle_cancel = cancel.clone();
            let exec_state = executor_state.clone();
            let exec_client = live_client.clone();
            let task = tokio::spawn(async move {
                Execution::resume(exec_client, exec_state, prop_for_resume, in_flight, cancel).run().await;
            });
            *in_flight_slot.lock().await = Some(ExecutionHandle {
                proposal_id: in_flight.proposal_id.clone(),
                task,
                cancel: handle_cancel,
                started_at: std::time::Instant::now(),
            });
        } else {
            warn!(proposal_id = %in_flight.proposal_id, "in_flight.json references unknown proposal; clearing");
            let _ = InFlightFile::delete(&args.data_dir);
        }
    }

    let app_state = Arc::new(AppState {
        snapshot: snapshot.clone(),
        store,
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
        },
        metrics: metrics.clone(),
        executor: executor_state,
        client_facade: live_client,
    });

    let connect_router = crabka_rebalancer::api::router(app_state);
    let health_router = crabka_rebalancer::health::router(HealthState {
        snapshot: snapshot.clone(),
        registry: Arc::new(tokio::sync::Mutex::new(registry)),
    });
    let app = connect_router.merge(health_router);

    let listener = tokio::net::TcpListener::bind(args.listen_addr).await?;
    info!(addr = %listener.local_addr()?, "listening");
    let shutdown_for_axum = shutdown.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c().await.ok();
            shutdown_for_axum.cancel();
        })
        .await?;

    let _ = tokio::time::timeout(Duration::from_secs(5), ingester_handle).await;
    Ok(())
}
```

If `RebalancerMetrics::clone()` isn't available (the struct doesn't derive `Clone`), edit `crates/rebalancer/src/metrics.rs` to add `#[derive(Clone)]` to `RebalancerMetrics`. `Counter` and `Gauge` from `prometheus-client` clone via internal Arc.

- [ ] **Step 4: Build + run CLI help**

Run: `cargo build -p crabka-rebalancer`
Expected: clean. If the Ingester constructor signature differs (43a's `Ingester::new` may not accept a `metrics` arg), look at `crates/rebalancer/src/ingest/mod.rs` and either pass the right args or skip the metrics arg if Ingester already pulls metrics from elsewhere.

Run: `target/debug/crabka-rebalancer --help 2>&1 | head -40`
Expected: clap prints all CLI flags including the new ones (`--data-dir`, `--default-throttle-bytes-per-sec`, `--execute-deadline-secs`, `--reassignment-poll-interval-secs`, `--reassignment-batch-size`).

- [ ] **Step 5: Run tests + clippy**

Run: `cargo test -p crabka-rebalancer`
Expected: all existing tests still pass.

Run: `cargo clippy -p crabka-rebalancer --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/src/bin/rebalancer.rs crates/rebalancer/src/executor
git -C /home/matt/git/crabka commit -m "rebalancer(43b): binary wiring — CLI flags, LiveClient, startup recovery

New CLI flags: --data-dir, --default-throttle-bytes-per-sec,
--execute-deadline-secs, --reassignment-poll-interval-secs,
--reassignment-batch-size. LiveClient implements ClientFacade via
crabka_client_core::Client::send against the protocol's
IncrementalAlterConfigs / AlterPartitionReassignments /
ListPartitionReassignments request types. On startup, the binary
reads in_flight.json and spawns Execution::resume if present.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 8 — Tests + Helm chart (parallel: T10, T11, T12)

### Task 10: End-to-end integration tests for execute / cancel / resume

**Files:**
- Modify: `crates/rebalancer/tests/end_to_end.rs`

- [ ] **Step 1: Append three new tests to the existing file**

Read the current `crates/rebalancer/tests/end_to_end.rs` to understand the existing fixtures. After the existing tests, append:

```rust
/// Execute a proposal end-to-end against a single-broker Crabka.
/// Single-broker means the only valid replica set is [1]; the
/// optimizer's PreferredLeaderIdempotency goal won't generate
/// movements. We construct a synthetic proposal directly to exercise
/// the executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn execute_proposal_settles_against_real_broker() {
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::{ExecutorConfig, ExecutorState};
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};
    use crabka_rebalancer::model::store::ProposalStore;
    use crabka_rebalancer::model::Movement;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let (handle, broker_addr) = boot_broker().await;
    let client = crabka_client_core::Client::builder()
        .bootstrap(broker_addr.to_string())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .unwrap();
    create_topic(&client, "exec-t", 1).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());

    // Construct a synthetic "no-op" proposal: the only available broker
    // is 1, so the movement keeps replicas at [1]. This proves the
    // wire path (ApplyThrottle → Submit → Wait → ClearThrottle) without
    // requiring multi-broker movement.
    let proposal = Proposal {
        id: "exec-1".into(),
        status: ProposalStatus::Computed,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "exec-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 0,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 0,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = crabka_rebalancer::metrics::RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));

    let cancel = tokio_util::sync::CancellationToken::new();
    let exec = crabka_rebalancer::executor::Execution::new(
        live_client,
        executor_state.clone(),
        proposal,
        50_000_000,
        cancel,
    );
    let exec_task = tokio::spawn(exec.run());

    // Wait up to 10s for the proposal to reach a terminal state.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut final_status = ProposalStatus::Executing;
    while Instant::now() < deadline {
        final_status = store.get("exec-1").unwrap().status;
        if final_status != ProposalStatus::Executing && final_status != ProposalStatus::Computed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = exec_task.await;

    assert!(
        matches!(final_status, ProposalStatus::Completed | ProposalStatus::Failed),
        "expected terminal status, got {final_status:?}"
    );
    // in_flight.json should be cleaned up.
    assert!(crabka_rebalancer::executor::state::InFlightFile::load(dir.path()).unwrap().is_none());

    let _ = tokio::time::timeout(Duration::from_secs(30), handle.shutdown()).await;
}

/// Cancel during Wait transitions the proposal to Cancelled and clears
/// throttle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_clears_throttle_and_reverts() {
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::{ExecutorConfig, ExecutorState};
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};
    use crabka_rebalancer::model::store::ProposalStore;
    use crabka_rebalancer::model::Movement;
    use std::sync::Arc;
    use std::time::Duration;

    let (handle, broker_addr) = boot_broker().await;
    let client = crabka_client_core::Client::builder()
        .bootstrap(broker_addr.to_string())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .unwrap();
    create_topic(&client, "cancel-t", 1).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());
    let proposal = Proposal {
        id: "cancel-1".into(),
        status: ProposalStatus::Computed,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "cancel-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 0,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 0,
    };
    store.insert(proposal.clone());

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = crabka_rebalancer::metrics::RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_for_caller = cancel.clone();
    let exec = crabka_rebalancer::executor::Execution::new(
        live_client,
        executor_state,
        proposal,
        50_000_000,
        cancel,
    );
    let exec_task = tokio::spawn(exec.run());

    // Cancel quickly so we catch the Wait or earlier phase.
    tokio::time::sleep(Duration::from_millis(100)).await;
    cancel_for_caller.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), exec_task).await;

    let after = store.get("cancel-1").unwrap();
    assert!(
        matches!(after.status, ProposalStatus::Cancelled | ProposalStatus::Completed | ProposalStatus::Failed),
        "expected terminal status, got {:?}",
        after.status
    );
    assert!(crabka_rebalancer::executor::state::InFlightFile::load(dir.path()).unwrap().is_none());

    let _ = tokio::time::timeout(Duration::from_secs(30), handle.shutdown()).await;
}

/// Restart resumes an in-flight plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_in_flight_plan() {
    use crabka_rebalancer::executor::client_impl::LiveClient;
    use crabka_rebalancer::executor::state::{InFlightFile, Phase};
    use crabka_rebalancer::executor::{Execution, ExecutorConfig, ExecutorState};
    use crabka_rebalancer::model::proposal::{Proposal, ProposalStatus, ProposalSummary};
    use crabka_rebalancer::model::store::ProposalStore;
    use crabka_rebalancer::model::Movement;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    let (handle, broker_addr) = boot_broker().await;
    let client = crabka_client_core::Client::builder()
        .bootstrap(broker_addr.to_string())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .unwrap();
    create_topic(&client, "resume-t", 1).await;

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(ProposalStore::open(dir.path(), 20).unwrap());
    let proposal = Proposal {
        id: "resume-1".into(),
        status: ProposalStatus::Executing,
        created_at_ms: 0,
        goals_applied: vec![],
        summary: ProposalSummary::default(),
        movements: vec![Movement {
            topic: "resume-t".into(),
            partition: 0,
            old_replicas: vec![1],
            new_replicas: vec![1],
            old_leader: 1,
            new_leader: 1,
        }],
        started_at_ms: 1,
        terminated_at_ms: 0,
        failure_reason: None,
        throttle_bytes_per_sec: 50_000_000,
    };
    store.insert(proposal.clone());

    // Pre-stage an in_flight.json reflecting a "Submit"-phase crash.
    InFlightFile::new(proposal.id.clone(), Phase::Submit, 1, 50_000_000)
        .write(dir.path())
        .unwrap();

    let mut registry = prometheus_client::registry::Registry::with_prefix("crabka_rebalancer");
    let metrics = crabka_rebalancer::metrics::RebalancerMetrics::register(&mut registry);
    let executor_state = ExecutorState {
        store: store.clone(),
        config: ExecutorConfig {
            data_dir: dir.path().to_path_buf(),
            default_throttle_bytes_per_sec: 50_000_000,
            poll_interval: Duration::from_millis(50),
            execute_deadline: Duration::from_secs(30),
            batch_size: 200,
        },
        metrics,
        in_flight: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let live_client = Arc::new(LiveClient::new(client));
    let cancel = tokio_util::sync::CancellationToken::new();
    let in_flight = InFlightFile::load(dir.path()).unwrap().unwrap();
    let exec = Execution::resume(live_client, executor_state, proposal, in_flight, cancel);
    let _ = tokio::time::timeout(Duration::from_secs(10), exec.run()).await;

    let after = store.get("resume-1").unwrap();
    assert!(
        matches!(after.status, ProposalStatus::Completed | ProposalStatus::Failed),
        "expected terminal status after resume, got {:?}",
        after.status
    );
    assert!(InFlightFile::load(dir.path()).unwrap().is_none());

    let _ = tokio::time::timeout(Duration::from_secs(30), handle.shutdown()).await;
}
```

- [ ] **Step 2: Run the new tests**

Run: `cargo test -p crabka-rebalancer --test end_to_end -- --nocapture`
Expected: 5 tests pass (2 existing + 3 new).

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/tests/end_to_end.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43b): e2e tests for execute / cancel / restart resume

Three new end-to-end tests against a single-broker Crabka. Each
exercises the full state machine; movements are no-op (replicas
[1] → [1]) so the wire path runs without needing multi-broker
movement. Asserts terminal status reached and in_flight.json
cleaned up.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 11: Connect HTTP smoke test extended for ExecuteProposal

**Files:**
- Modify: `crates/rebalancer/tests/connect_smoke.rs`

- [ ] **Step 1: Append the ExecuteProposal smoke test**

Read the existing `connect_smoke.rs` to understand the binary-spawn fixture, then append a test that exercises `ExecuteProposal` and `CancelExecution` over HTTP+JSON:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_execute_proposal_and_cancel_over_http_json() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = Broker::start(cfg).await.unwrap();
    let broker_addr = broker.listen_addr();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rebal_port = listener.local_addr().unwrap().port();
    drop(listener);
    let rebal_addr = format!("127.0.0.1:{rebal_port}");

    let data_dir = tempfile::tempdir().unwrap();

    let bin_path = env!("CARGO_BIN_EXE_crabka-rebalancer");
    let mut child = tokio::process::Command::new(bin_path)
        .arg("--bootstrap-servers").arg(broker_addr.to_string())
        .arg("--listen-addr").arg(&rebal_addr)
        .arg("--scrape-interval-secs").arg("1")
        .arg("--data-dir").arg(data_dir.path())
        .env("RUST_LOG", "crabka_rebalancer=info,warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn crabka-rebalancer");

    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        match client.get(format!("http://{rebal_addr}/readyz")).send().await {
            Ok(r) if r.status() == reqwest::StatusCode::OK => break,
            _ => {}
        }
        assert!(Instant::now() < deadline, "rebalancer /readyz never returned 200");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // CreateProposal — empty goals returns a Computed proposal (may have
    // zero movements on a single-broker cluster; that's fine for the
    // wire-path test).
    let create = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/CreateProposal"
        ))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("create POST");
    assert!(create.status().is_success());
    let create_body: serde_json::Value = create.json().await.expect("create JSON");
    let id = create_body.get("id").and_then(|v| v.as_str()).expect("id").to_string();

    // ExecuteProposal on a zero-movements proposal returns FailedPrecondition.
    let exec = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/ExecuteProposal"
        ))
        .header("Content-Type", "application/json")
        .body(format!(r#"{{"id":"{id}"}}"#))
        .send()
        .await
        .expect("execute POST");
    // Connect's "FailedPrecondition" maps to HTTP 400.
    assert_eq!(exec.status(), reqwest::StatusCode::BAD_REQUEST,
        "expected FailedPrecondition for zero-movement proposal");
    let body_text = exec.text().await.unwrap_or_default();
    assert!(body_text.contains("movement") || body_text.contains("Computed"),
        "expected explanatory message; got {body_text}");

    let _ = child.kill().await;
    let _ = tokio::time::timeout(Duration::from_secs(30), broker.shutdown()).await;
    std::mem::forget(dir);
    std::mem::forget(data_dir);
}
```

- [ ] **Step 2: Run the smoke test**

Run: `cargo test -p crabka-rebalancer --test connect_smoke -- --nocapture`
Expected: 2 tests pass (1 existing + 1 new).

- [ ] **Step 3: Commit**

```bash
git -C /home/matt/git/crabka add crates/rebalancer/tests/connect_smoke.rs
git -C /home/matt/git/crabka commit -m "rebalancer(43b): connect smoke covers ExecuteProposal failure path

Adds an HTTP+JSON round-trip that creates a (zero-movement) proposal
on a single-broker cluster then asserts ExecuteProposal returns
FailedPrecondition (HTTP 400). Proves the new RPC is reachable over
the JSON wire and the FailedPrecondition gate fires.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 12: Helm chart files

**Files:**
- Create: `charts/crabka-rebalancer/Chart.yaml`
- Create: `charts/crabka-rebalancer/values.yaml`
- Create: `charts/crabka-rebalancer/templates/_helpers.tpl`
- Create: `charts/crabka-rebalancer/templates/deployment.yaml`
- Create: `charts/crabka-rebalancer/templates/service.yaml`
- Create: `charts/crabka-rebalancer/templates/serviceaccount.yaml`
- Create: `charts/crabka-rebalancer/templates/persistentvolumeclaim.yaml`

- [ ] **Step 1: Write `Chart.yaml`**

```yaml
apiVersion: v2
name: crabka-rebalancer
description: Cruise-Control-equivalent partition rebalancer for Crabka clusters.
type: application
version: 0.1.1
appVersion: "0.1.1"
home: https://github.com/robot-head/crabka
sources:
  - https://github.com/robot-head/crabka/tree/main/crates/rebalancer
```

- [ ] **Step 2: Write `values.yaml`**

```yaml
# Default values for crabka-rebalancer.

image:
  repository: ghcr.io/robot-head/crabka-rebalancer
  tag: ""  # defaults to .Chart.AppVersion when empty
  pullPolicy: IfNotPresent

# REQUIRED: comma-separated host:port list of Crabka bootstrap brokers.
bootstrapServers: ""

listenAddr: "0.0.0.0:9300"
scrapeIntervalSecs: 10
imbalanceThresholdPct: 10
maxMovementsPerProposal: 256
proposalRingBufferSize: 20

throttle:
  defaultBytesPerSec: 50000000

executeDeadlineSecs: 1800
reassignmentPollIntervalSecs: 5
reassignmentBatchSize: 200

persistence:
  size: 1Gi
  storageClass: ""  # uses default storage class when empty

resources: {}
nodeSelector: {}
tolerations: []
affinity: {}

service:
  type: ClusterIP
  port: 9300
```

- [ ] **Step 3: Write `templates/_helpers.tpl`**

```yaml
{{- define "rebalancer.name" -}}
crabka-rebalancer
{{- end -}}

{{- define "rebalancer.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "rebalancer.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "rebalancer.labels" -}}
app.kubernetes.io/name: {{ include "rebalancer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "rebalancer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rebalancer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "rebalancer.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
```

- [ ] **Step 4: Write `templates/deployment.yaml`**

```yaml
{{- if not .Values.bootstrapServers }}
{{- fail "values.bootstrapServers is required (no default)" }}
{{- end }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "rebalancer.fullname" . }}
  labels:
    {{- include "rebalancer.labels" . | nindent 4 }}
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      {{- include "rebalancer.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "rebalancer.selectorLabels" . | nindent 8 }}
    spec:
      serviceAccountName: {{ include "rebalancer.fullname" . }}
      containers:
        - name: crabka-rebalancer
          image: {{ include "rebalancer.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          env:
            - name: CRABKA_BOOTSTRAP_SERVERS
              value: {{ .Values.bootstrapServers | quote }}
            - name: CRABKA_REBALANCER_LISTEN_ADDR
              value: {{ .Values.listenAddr | quote }}
            - name: CRABKA_SCRAPE_INTERVAL_SECS
              value: {{ .Values.scrapeIntervalSecs | quote }}
            - name: CRABKA_IMBALANCE_THRESHOLD_PCT
              value: {{ .Values.imbalanceThresholdPct | quote }}
            - name: CRABKA_MAX_MOVEMENTS_PER_PROPOSAL
              value: {{ .Values.maxMovementsPerProposal | quote }}
            - name: CRABKA_PROPOSAL_RING_BUFFER_SIZE
              value: {{ .Values.proposalRingBufferSize | quote }}
            - name: CRABKA_DEFAULT_THROTTLE_BYTES_PER_SEC
              value: {{ .Values.throttle.defaultBytesPerSec | quote }}
            - name: CRABKA_EXECUTE_DEADLINE_SECS
              value: {{ .Values.executeDeadlineSecs | quote }}
            - name: CRABKA_REASSIGNMENT_POLL_INTERVAL_SECS
              value: {{ .Values.reassignmentPollIntervalSecs | quote }}
            - name: CRABKA_REASSIGNMENT_BATCH_SIZE
              value: {{ .Values.reassignmentBatchSize | quote }}
            - name: CRABKA_DATA_DIR
              value: /var/lib/crabka-rebalancer
          ports:
            - name: connect-rpc
              containerPort: 9300
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /healthz
              port: connect-rpc
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /readyz
              port: connect-rpc
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/crabka-rebalancer
          resources:
            {{- toYaml .Values.resources | nindent 12 }}
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: {{ include "rebalancer.fullname" . }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
```

- [ ] **Step 5: Write `templates/service.yaml`**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "rebalancer.fullname" . }}
  labels:
    {{- include "rebalancer.labels" . | nindent 4 }}
spec:
  type: {{ .Values.service.type }}
  ports:
    - port: {{ .Values.service.port }}
      targetPort: connect-rpc
      protocol: TCP
      name: connect-rpc
  selector:
    {{- include "rebalancer.selectorLabels" . | nindent 4 }}
```

- [ ] **Step 6: Write `templates/serviceaccount.yaml`**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "rebalancer.fullname" . }}
  labels:
    {{- include "rebalancer.labels" . | nindent 4 }}
```

- [ ] **Step 7: Write `templates/persistentvolumeclaim.yaml`**

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "rebalancer.fullname" . }}
  labels:
    {{- include "rebalancer.labels" . | nindent 4 }}
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {{ .Values.persistence.size }}
  {{- if .Values.persistence.storageClass }}
  storageClassName: {{ .Values.persistence.storageClass | quote }}
  {{- end }}
```

- [ ] **Step 8: Verify chart lints + renders**

Run: `helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092 2>&1 | tail -5`
Expected: `1 chart(s) linted, 0 chart(s) failed`.

Run: `helm template demo charts/crabka-rebalancer --set bootstrapServers=test:9092 > /tmp/rendered.yaml && grep -c "^kind:" /tmp/rendered.yaml`
Expected: `4` (Deployment + Service + ServiceAccount + PersistentVolumeClaim).

Run: `helm template demo charts/crabka-rebalancer 2>&1 | tail -3`
Expected: failure with the message `values.bootstrapServers is required`.

- [ ] **Step 9: Commit**

```bash
git -C /home/matt/git/crabka add charts/crabka-rebalancer
git -C /home/matt/git/crabka commit -m "rebalancer(43b): production Helm chart

charts/crabka-rebalancer/ ships Deployment (replicas: 1, strategy:
Recreate), ClusterIP Service on port 9300, ServiceAccount (no
cluster RBAC), and a ReadWriteOnce PVC mounted at
/var/lib/crabka-rebalancer. Required value: bootstrapServers (chart
fails to render without it). Env vars track CLI flags 1:1.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 9 — Helm unittest + CI (parallel: T13, T14)

### Task 13: Helm unittest test files

**Files:**
- Create: `charts/crabka-rebalancer/tests/deployment_test.yaml`
- Create: `charts/crabka-rebalancer/tests/required_values_test.yaml`
- Create: `charts/crabka-rebalancer/tests/service_test.yaml`
- Create: `charts/crabka-rebalancer/tests/pvc_test.yaml`
- Create: `charts/crabka-rebalancer/tests/rbac_test.yaml`

- [ ] **Step 1: Write `tests/deployment_test.yaml`**

```yaml
suite: deployment
templates:
  - deployment.yaml
release:
  name: demo
  namespace: kafka
set:
  bootstrapServers: kafka-bootstrap:9092
tests:
  - it: renders one replica with Recreate strategy
    asserts:
      - equal:
          path: spec.replicas
          value: 1
      - equal:
          path: spec.strategy.type
          value: Recreate

  - it: container exposes Connect-RPC port 9300
    asserts:
      - equal:
          path: spec.template.spec.containers[0].ports[0].containerPort
          value: 9300

  - it: probes wired to /healthz and /readyz
    asserts:
      - equal:
          path: spec.template.spec.containers[0].livenessProbe.httpGet.path
          value: /healthz
      - equal:
          path: spec.template.spec.containers[0].readinessProbe.httpGet.path
          value: /readyz

  - it: passes bootstrapServers env var
    asserts:
      - contains:
          path: spec.template.spec.containers[0].env
          content:
            name: CRABKA_BOOTSTRAP_SERVERS
            value: kafka-bootstrap:9092

  - it: mounts persistent volume at expected path
    asserts:
      - equal:
          path: spec.template.spec.containers[0].volumeMounts[0].mountPath
          value: /var/lib/crabka-rebalancer
      - equal:
          path: spec.template.spec.volumes[0].persistentVolumeClaim.claimName
          value: demo-crabka-rebalancer
```

- [ ] **Step 2: Write `tests/required_values_test.yaml`**

```yaml
suite: required values
templates:
  - deployment.yaml
release:
  name: demo
tests:
  - it: rendering fails when bootstrapServers is unset
    asserts:
      - failedTemplate:
          errorMessage: "values.bootstrapServers is required (no default)"
```

- [ ] **Step 3: Write `tests/service_test.yaml`**

```yaml
suite: service
templates:
  - service.yaml
release:
  name: demo
set:
  bootstrapServers: kafka-bootstrap:9092
tests:
  - it: defaults to ClusterIP on 9300
    asserts:
      - equal:
          path: spec.type
          value: ClusterIP
      - equal:
          path: spec.ports[0].port
          value: 9300
      - equal:
          path: spec.ports[0].targetPort
          value: connect-rpc
```

- [ ] **Step 4: Write `tests/pvc_test.yaml`**

```yaml
suite: pvc
templates:
  - persistentvolumeclaim.yaml
release:
  name: demo
set:
  bootstrapServers: kafka-bootstrap:9092
tests:
  - it: accessMode is ReadWriteOnce
    asserts:
      - equal:
          path: spec.accessModes[0]
          value: ReadWriteOnce

  - it: size defaults to 1Gi
    asserts:
      - equal:
          path: spec.resources.requests.storage
          value: 1Gi

  - it: honors explicit storageClass when set
    set:
      persistence.storageClass: fast-ssd
    asserts:
      - equal:
          path: spec.storageClassName
          value: fast-ssd
```

- [ ] **Step 5: Write `tests/rbac_test.yaml`**

```yaml
suite: rbac
templates:
  - serviceaccount.yaml
release:
  name: demo
set:
  bootstrapServers: kafka-bootstrap:9092
tests:
  - it: ServiceAccount is created
    asserts:
      - hasDocuments:
          count: 1
      - equal:
          path: kind
          value: ServiceAccount
      - equal:
          path: metadata.name
          value: demo-crabka-rebalancer
```

- [ ] **Step 6: Run helm-unittest locally**

If `helm` and the `unittest` plugin aren't installed locally, skip this step — CI will run it in T14. To install:

```bash
helm plugin install https://github.com/helm-unittest/helm-unittest 2>&1 | tail -3 || true
helm unittest charts/crabka-rebalancer 2>&1 | tail -20
```

Expected: all suites pass. If `helm-unittest` isn't available locally, that's fine; T14 wires it into CI.

- [ ] **Step 7: Commit**

```bash
git -C /home/matt/git/crabka add charts/crabka-rebalancer/tests
git -C /home/matt/git/crabka commit -m "rebalancer(43b): helm-unittest tests for rebalancer chart

Five test files under charts/crabka-rebalancer/tests/ exercise the
chart's contract via the helm-unittest plugin: deployment shape
(replicas, strategy, port, probes, env, volume mount), required
bootstrapServers value, ClusterIP service port, PVC accessMode +
size + storageClass, and ServiceAccount presence with no cluster
RBAC.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 14: CI — install helm-unittest, run on the rebalancer chart

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Find the existing helm-lint job**

Read `.github/workflows/ci.yml`. Locate the `helm-lint:` job. It currently lints `charts/crabka-operator` and runs a `helm template + grep` sanity check.

- [ ] **Step 2: Append rebalancer chart steps to the same job**

After the existing helm-operator template + grep block, append:

```yaml
      - name: Install helm-unittest plugin
        run: helm plugin install https://github.com/helm-unittest/helm-unittest --version v0.6.0
      - name: helm lint rebalancer chart
        run: helm lint charts/crabka-rebalancer --set bootstrapServers=test:9092
      - name: helm template rebalancer chart (sanity)
        run: |
          helm template demo charts/crabka-rebalancer --set bootstrapServers=test:9092 > /tmp/rebalancer.yaml
          grep -q "kind: Deployment" /tmp/rebalancer.yaml
          grep -q "kind: Service" /tmp/rebalancer.yaml
          grep -q "kind: ServiceAccount" /tmp/rebalancer.yaml
          grep -q "kind: PersistentVolumeClaim" /tmp/rebalancer.yaml
      - name: helm unittest rebalancer chart
        run: helm unittest charts/crabka-rebalancer
```

The full helm-lint job should now lint + template-check the operator chart and lint + template-check + unittest the rebalancer chart on the same runner.

- [ ] **Step 3: Verify YAML is still well-formed**

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" 2>&1 | tail -3`
Expected: no output (success). If you don't have python3 handy, run any YAML parser you have. If unavailable, eyeball the indentation against neighboring jobs.

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add .github/workflows/ci.yml
git -C /home/matt/git/crabka commit -m "ci(rebalancer): helm-lint job installs helm-unittest + tests new chart

helm-lint job grows three steps: install helm-unittest plugin
(v0.6.0), lint + template-check the new rebalancer chart, and run
helm unittest against the suite of five test files added in T13.
Operator-chart steps are unchanged.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 10 — Docs

### Task 15: README + STATUS

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Flip the README executor row**

Open `README.md`. Find the row added in slice 43a:

```
| Cruise-Control-equivalent rebalancer (executor) | ❌ |
```

Change `❌` to `✅`.

- [ ] **Step 2: Add the STATUS.md entry**

Append (at the same bottom-of-file position 43a used) to `STATUS.md`:

```markdown
## Slice 43b — Rebalancer execute path (2026-05-17)

- Rebalancer transitions from advisor to executor. `ExecuteProposal`
  now drives `AlterPartitionReassignments` (KIP-455) under a
  KIP-73 throttle managed via `IncrementalAlterConfigs`, with
  progress polled via `ListPartitionReassignments`. `ClearThrottle`
  runs in every terminal path — success, failure, and cancel —
  so the broker never gets stuck with throttle configs set.
- New `CancelExecution` RPC reverts pending reassignments (KIP-455
  null-replicas) and clears throttle, transitioning the proposal to
  `Cancelled`.
- `ProposalStatus` extended with `Executing` / `Completed` /
  `Failed` / `Cancelled`. `Proposal` gains `started_at_ms`,
  `terminated_at_ms`, `failure_reason`, `throttle_bytes_per_sec`.
- One execution at a time. Concurrent `ExecuteProposal` returns
  `FailedPrecondition`. `CreateProposal` continues to compute
  against the current (transition-state) snapshot during execution.
- On-disk persistence at `{data_dir}/proposals.json` (full ring
  buffer, atomic write) + `{data_dir}/in_flight.json` (active
  marker, deleted on terminal). On startup, recovery loads both and
  resumes the persisted phase via re-issuing
  `AlterPartitionReassignments` (KIP-455 idempotent). `data_dir`
  defaults to `/var/lib/crabka-rebalancer`.
- Production Helm chart at `charts/crabka-rebalancer/`: Deployment
  (replicas: 1, strategy: Recreate), ClusterIP Service on 9300,
  ServiceAccount (no cluster RBAC), RWO PVC. `bootstrapServers` is
  a required value (chart fails to render without it).
- Five `helm-unittest` test files under
  `charts/crabka-rebalancer/tests/` run in CI alongside `helm lint`
  and the `helm template + grep` sanity check.
- New CLI flags: `--data-dir`, `--default-throttle-bytes-per-sec`
  (default 50 MB/s), `--execute-deadline-secs` (default 1800),
  `--reassignment-poll-interval-secs` (default 5),
  `--reassignment-batch-size` (default 200).
- New metrics:
  `crabka_rebalancer_executions_started_total` /
  `_completed_total` / `_failed_total` / `_cancelled_total`.
- ~15 new unit tests across `model`, `executor`, plus three new
  end-to-end integration tests
  (`execute_proposal_settles_against_real_broker`,
  `cancel_clears_throttle_and_reverts`,
  `restart_resumes_in_flight_plan`) and one extended Connect HTTP
  smoke test.
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43b-design.md`].
- Out of scope (deferred): multi-replica HA (later slice), metric
  scraping for usage goals (43e), rack-aware / capacity / usage /
  CPU / anomaly goals (43c–43g), operator `KafkaRebalance` CRD
  (slice 44), pause/step-through, adaptive throttle.
```

- [ ] **Step 3: Run final verification**

```bash
cargo fmt --check 2>&1 | tail -5
cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -5
cargo test -p crabka-rebalancer 2>&1 | tail -5
```

All three must pass clean. If `cargo fmt --check` reports differences, run `cargo fmt` and re-check (commit any pure-formatting changes separately).

- [ ] **Step 4: Commit**

```bash
git -C /home/matt/git/crabka add README.md STATUS.md
git -C /home/matt/git/crabka commit -m "rebalancer(43b): README + STATUS

Cruise-Control-equivalent rebalancer (executor) row flips from ❌
to ✅ in README's Replication & durability section. Slice 43b
entry added to STATUS documenting the shipped execute path,
persistence, Helm chart, helm-unittest tests, new metrics, and
deferred follow-ups (multi-replica HA, additional goal families,
operator CRD).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review checklist (run before declaring the plan done)

**1. Spec coverage:**
- Architecture (executor module, state machine, `ExecutorState`/`ExecutionHandle`) → T3, T5, T6, T7
- Persistence (`proposals.json` + `in_flight.json`, atomic write, recovery) → T4, T5, T9
- API surface (`ProposalStatus` variants, new Proposal fields, `ExecuteProposal` body, `CancelExecution` RPC) → T1, T2, T8
- Throttle strategy (4 KIP-73 keys, target computation, apply/clear ordering, terminal guard) → T3, T7
- Helm chart + helm-unittest in CI → T12, T13, T14
- Testing (unit + integration + Connect smoke + helm-unittest) → T6, T7, T10, T11, T13
- Acceptance criteria (cargo + helm + STATUS + README) → T15
- Recovery flow before listener accepts → T9 Step 3 (recovery block precedes `axum::serve`)

**2. Placeholder scan:** None — every code step has a code block. Adaptations are explicit (e.g., "if Ingester::new differs, look at ingest/mod.rs and adjust").

**3. Type consistency:** `Movement`, `Proposal`, `ProposalStatus`, `ExecutorState`, `ExecutionHandle`, `InFlightFile`, `Phase`, `ThrottleTargets`, `ClientFacade`, `PhaseError`, `ConfigOp` are referenced consistently across tasks. `RebalancerMetrics` is cloneable (T9 Step 3 ensures it derives `Clone` if missing).
