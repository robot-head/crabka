# Slice 43a — Rebalancer foundation — Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Slice 43a — Rebalancer foundation (2026-05-17)

**Incomplete / deferred steps (out-of-scope follow-ups):**

- Execute path (closed by slice 43b)
- Persistence (closed by slice 43b)
- Metric scraping for usage goals (closed by slice 43e)
- Rack-aware / capacity / usage / CPU / anomaly goals (closed by slices 43c–43g)
- Operator KafkaRebalance CRD (closed by slice 44)

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Per `CLAUDE.md`, dispatch independent tasks within a batch in parallel.

**Goal:** Land a standalone `crabka-rebalancer` binary exposing a Connect-RPC service (`GetState` / `CreateProposal` / `DryRunProposal` / `GetProposal` / `ListProposals` / stub `ExecuteProposal`) that periodically snapshots cluster state and computes replica/leader-balance proposals.

**Architecture:** New workspace member `crates/rebalancer/`. Binary connects to the cluster via `crabka_client_core::Client`. Cluster state lives in an `ArcSwap<Option<ClusterState>>` updated every 10s by an `Ingester` task. Pure-logic `Goal` trait + `optimize()` produces `Proposal`s, stored in an in-memory UUID-keyed ring buffer. Connect-RPC service mounted via `connectrpc-axum`; `/healthz` `/readyz` `/metrics` on plain axum routes alongside.

**Tech Stack:** Rust 1.95.0. Workspace deps `connectrpc-axum = "0.1"`, `connectrpc-axum-build = "0.1"`, `prost = "0.13"`. Reuses `axum`, `arc-swap`, `prometheus-client`, `tokio`, `tracing`, `clap`, `uuid`, `anyhow`, `thiserror`, `serde_json` from the workspace. New `crabka-rebalancer` crate; no changes to existing crates other than workspace `Cargo.toml`.

**Reference spec:** [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43a-design.md`](../specs/2026-05-17-crabka-rebalancer-43a-design.md).

**Working directory:** `/home/matt/git/crabka`. Branch `feature/rebalancer-roadmap-43` already exists with the roadmap + 43a spec committed.

---

## File structure

```
Cargo.toml                                              # MODIFIED — add workspace deps
crates/rebalancer/
├── Cargo.toml                                          # NEW — crate manifest
├── build.rs                                            # NEW — connectrpc-axum-build codegen
├── proto/
│   └── crabka/rebalancer/v1/rebalancer.proto           # NEW — service definition
├── src/
│   ├── lib.rs                                          # NEW — top-level module mounts + public exports
│   ├── bin/rebalancer.rs                               # NEW — crabka-rebalancer binary
│   ├── api/
│   │   ├── mod.rs                                      # NEW — RebalancerService impl + axum router
│   │   └── handlers.rs                                 # NEW — one fn per RPC method
│   ├── ingest/
│   │   ├── mod.rs                                      # NEW — Ingester + snapshot loop
│   │   └── admin_client.rs                             # NEW — admin RPC wrappers
│   ├── model/
│   │   ├── mod.rs                                      # NEW — ClusterState / BrokerView / PartitionView
│   │   ├── proposal.rs                                 # NEW — Proposal / Movement / ProposalSummary / ProposalStatus
│   │   └── store.rs                                    # NEW — in-memory ring buffer
│   ├── goals/
│   │   ├── mod.rs                                      # NEW — Goal trait / GoalContext / GoalPriority
│   │   ├── preferred_leader_idempotency.rs             # NEW — hard goal
│   │   ├── replica_distribution.rs                     # NEW — soft goal
│   │   └── leader_distribution.rs                      # NEW — soft goal
│   ├── optimizer/
│   │   └── mod.rs                                      # NEW — optimize() function
│   └── health.rs                                       # NEW — /healthz /readyz /metrics
└── tests/
    └── end_to_end.rs                                   # NEW — in-process broker + ingester + service
README.md                                               # MODIFIED — add rebalancer row to feature matrix
STATUS.md                                               # MODIFIED — add slice-43a entry
```

**16 tasks across 8 batches.**

- **Batch 1 (alone):** T1 — crate scaffold (everything else depends on it compiling)
- **Batch 2 (alone):** T2 — proto + build.rs + codegen working
- **Batch 3 (parallel):** T3 model, T4 Goal trait, T5 ProposalStore
- **Batch 4 (parallel):** T6 PreferredLeaderIdempotency, T7 ReplicaDistribution, T8 LeaderDistribution
- **Batch 5 (alone):** T9 optimizer
- **Batch 6 (alone):** T10 Ingester (depends on T3)
- **Batch 7 (parallel):** T11 api handlers, T12 health module
- **Batch 8 (alone):** T13 binary entry, T14 end-to-end test, T15 Connect smoke test, T16 docs

(T13–T16 each depend on T11+T12, so dispatch sequentially.)

---

## Batch 1 — Crate scaffold

### Task 1: New `crates/rebalancer/` workspace member, bare crate that compiles

**Files:**
- Modify: `Cargo.toml` (workspace dependency additions)
- Create: `crates/rebalancer/Cargo.toml`
- Create: `crates/rebalancer/src/lib.rs`
- Create: `crates/rebalancer/src/bin/rebalancer.rs`

- [ ] **Step 1: Add workspace dependencies**

Edit the workspace `Cargo.toml`. Add to `[workspace.dependencies]` near the existing axum entry:

```toml
connectrpc-axum = "0.1"
connectrpc-axum-build = "0.1"
prost = "0.13"
```

- [ ] **Step 2: Create the crate manifest**

Write `crates/rebalancer/Cargo.toml`:

```toml
[package]
name = "crabka-rebalancer"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Cruise-Control-equivalent partition rebalancer for Crabka clusters"

[lints]
workspace = true

[[bin]]
name = "crabka-rebalancer"
path = "src/bin/rebalancer.rs"

[dependencies]
crabka-client-core = { version = "0.1", path = "../client-core" }
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
crabka-metadata = { version = "0.1", path = "../metadata" }
connectrpc-axum.workspace = true
prost.workspace = true
axum.workspace = true
arc-swap.workspace = true
prometheus-client.workspace = true
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "net", "macros", "signal", "time", "sync"] }
tokio-util = { workspace = true, features = ["rt"] }
tracing.workspace = true
tracing-subscriber.workspace = true
clap = { workspace = true, features = ["env", "derive"] }
uuid = { workspace = true }
anyhow.workspace = true
thiserror.workspace = true
serde = { workspace = true, features = ["derive"] }
serde_json.workspace = true
async-trait.workspace = true

[build-dependencies]
connectrpc-axum-build.workspace = true

[dev-dependencies]
crabka-broker = { version = "0.1", path = "../broker", features = ["test-helpers"] }
tempfile.workspace = true
tower.workspace = true
reqwest = { version = "0.12", default-features = false, features = ["json"] }
```

- [ ] **Step 3: Create empty `lib.rs` + binary stub**

Write `crates/rebalancer/src/lib.rs`:

```rust
//! Crabka rebalancer — Cruise-Control-equivalent partition placement
//! advisor (and, starting in slice 43b, executor).
//!
//! See `docs/superpowers/specs/2026-05-17-crabka-rebalancer-43a-design.md`
//! and the surrounding roadmap doc for the full slice plan.

// Module mounts come online as later tasks land them.
```

Write `crates/rebalancer/src/bin/rebalancer.rs`:

```rust
//! `crabka-rebalancer` — Cruise-Control-equivalent partition rebalancer.
//!
//! Slice 43a ships the advisor surface: connects to a cluster as an
//! admin client, snapshots state, exposes a Connect-RPC service for
//! `GetState` / `CreateProposal` / `DryRunProposal` (and a stub
//! `ExecuteProposal`).  Slice 43b wires execute.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("crabka-rebalancer slice-43a scaffold — no service wired yet");
    Ok(())
}
```

- [ ] **Step 4: Verify the crate builds**

Run: `cargo build -p crabka-rebalancer`
Expected: success, no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml crates/rebalancer/Cargo.toml crates/rebalancer/src/lib.rs crates/rebalancer/src/bin/rebalancer.rs
git commit -m "rebalancer(43a): crate scaffold

Empty workspace member that builds. Module mounts and wiring land
in later tasks. Workspace deps for connectrpc-axum + prost added.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Proto + codegen

### Task 2: Proto file + build.rs + generated module mount

**Files:**
- Create: `crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto`
- Create: `crates/rebalancer/build.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the `.proto` file**

Create `crates/rebalancer/proto/crabka/rebalancer/v1/rebalancer.proto`:

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
message ExecuteProposalRequest { string id = 1; }
message ExecuteProposalResponse {}
```

- [ ] **Step 2: Write build.rs that invokes `connectrpc-axum-build`**

Create `crates/rebalancer/build.rs`:

```rust
//! Build script — generates Connect-RPC server stubs + prost message
//! types from the `.proto` file. Outputs are written to `OUT_DIR` and
//! pulled in via the `pb::` module declared in `src/lib.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    connectrpc_axum_build::configure()
        .out_dir(std::env::var("OUT_DIR")?)
        .compile_protos(
            &["proto/crabka/rebalancer/v1/rebalancer.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/crabka/rebalancer/v1/rebalancer.proto");
    Ok(())
}
```

- [ ] **Step 3: Mount the generated code from `lib.rs`**

Edit `crates/rebalancer/src/lib.rs`. Append after the doc comment:

```rust

/// Generated protobuf + Connect server stubs. The actual content lives
/// in `OUT_DIR/crabka.rebalancer.v1.rs` and is produced by `build.rs`.
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/crabka.rebalancer.v1.rs"));
}
```

- [ ] **Step 4: Verify the crate still builds with codegen running**

Run: `cargo build -p crabka-rebalancer`
Expected: success; build.rs runs `protoc` (must be available on `$PATH`) and writes generated code to `OUT_DIR`. If protoc is missing, the build script errors with `protoc not found`. Install `protoc` and retry — or, as a fallback, enable the `fetch-protoc` feature on `connectrpc-axum-build` in `Cargo.toml` (this downloads a vendored protoc binary at build time).

- [ ] **Step 5: Smoke-check the generated module is importable**

Run: `cargo test -p crabka-rebalancer --lib -- --list 2>&1 | head -5`
Expected: succeeds without errors mentioning `pb::` or undefined symbols (zero tests is fine).

Also confirm the generated types exist by adding a *temporary* assert (revert before commit):

```rust
// in src/lib.rs, end of file:
#[cfg(test)]
mod _codegen_smoke {
    #[test]
    fn generated_types_compile() {
        let _ = super::pb::GetStateRequest {};
        let _ = super::pb::GetStateResponse::default();
    }
}
```

Run: `cargo test -p crabka-rebalancer --lib _codegen_smoke -- --nocapture`
Expected: 1 test passes. Then DELETE that block — it was a one-off smoke check.

- [ ] **Step 6: Commit**

```bash
git add crates/rebalancer/proto crates/rebalancer/build.rs crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): proto + connectrpc-axum codegen

Service definition (six RPCs) compiled to Rust via connectrpc-axum-build.
Generated module mounted as crabka_rebalancer::pb.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Pure-logic core (parallel: T3, T4, T5)

### Task 3: `model` module — ClusterState, Movement, validity helpers

**Files:**
- Create: `crates/rebalancer/src/model/mod.rs`
- Create: `crates/rebalancer/src/model/proposal.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the failing test for validity helpers**

Create `crates/rebalancer/src/model/mod.rs`:

```rust
//! In-memory data model for the rebalancer.
//!
//! `ClusterState` is the snapshot fed into the optimizer. `Movement`
//! is a single proposed change (replica-set update, leader change, or
//! both). Validity helpers reject malformed movements before they
//! reach the optimizer's accumulator.

pub mod proposal;

pub use proposal::{Movement, Proposal, ProposalStatus, ProposalSummary};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterState {
    pub cluster_id: Option<String>,
    pub snapshot_at_ms: i64,
    pub brokers: Vec<BrokerView>,
    pub partitions: Vec<PartitionView>,
    pub in_flight_reassignments: Vec<InFlightReassignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerView {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionView {
    pub topic: String,
    pub partition: i32,
    pub replicas: Vec<i32>,
    pub leader: i32,
    pub isr: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InFlightReassignment {
    pub topic: String,
    pub partition: i32,
    pub adding: Vec<i32>,
    pub removing: Vec<i32>,
}

/// Why a proposed movement was rejected. Returned by
/// [`validate_movement`]. The optimizer logs at debug + drops the
/// movement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MovementError {
    #[error("replication factor changed: old={old} new={new}")]
    ReplicationFactorChanged { old: usize, new: usize },
    #[error("new_leader {leader} not in new_replicas {replicas:?}")]
    LeaderNotInReplicas { leader: i32, replicas: Vec<i32> },
    #[error("new_replicas has duplicates: {replicas:?}")]
    DuplicateReplicas { replicas: Vec<i32> },
    #[error("new_replicas contains unknown broker id {id}")]
    UnknownBroker { id: i32 },
    #[error("target partition not found: {topic}-{partition}")]
    UnknownPartition { topic: String, partition: i32 },
}

/// Inspect `movement` against `state`'s broker + partition tables.
/// Returns `Ok(())` for movements the optimizer should accept,
/// `Err(MovementError)` for ones it should drop.
pub fn validate_movement(state: &ClusterState, mv: &Movement) -> Result<(), MovementError> {
    if mv.old_replicas.len() != mv.new_replicas.len() {
        return Err(MovementError::ReplicationFactorChanged {
            old: mv.old_replicas.len(),
            new: mv.new_replicas.len(),
        });
    }
    if !mv.new_replicas.contains(&mv.new_leader) {
        return Err(MovementError::LeaderNotInReplicas {
            leader: mv.new_leader,
            replicas: mv.new_replicas.clone(),
        });
    }
    let mut seen = std::collections::HashSet::new();
    for r in &mv.new_replicas {
        if !seen.insert(*r) {
            return Err(MovementError::DuplicateReplicas {
                replicas: mv.new_replicas.clone(),
            });
        }
    }
    let known: std::collections::HashSet<i32> = state.brokers.iter().map(|b| b.id).collect();
    for r in &mv.new_replicas {
        if !known.contains(r) {
            return Err(MovementError::UnknownBroker { id: *r });
        }
    }
    let part_known = state
        .partitions
        .iter()
        .any(|p| p.topic == mv.topic && p.partition == mv.partition);
    if !part_known {
        return Err(MovementError::UnknownPartition {
            topic: mv.topic.clone(),
            partition: mv.partition,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_state() -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![
                BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None },
                BrokerView { id: 2, host: "h2".into(), port: 9092, rack: None },
                BrokerView { id: 3, host: "h3".into(), port: 9092, rack: None },
            ],
            partitions: vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            }],
            in_flight_reassignments: vec![],
        }
    }

    #[test]
    fn validate_valid_movement_ok() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert!(validate_movement(&fixture_state(), &mv).is_ok());
    }

    #[test]
    fn validate_rejects_rf_change() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 2, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::ReplicationFactorChanged { .. })
        ));
    }

    #[test]
    fn validate_rejects_leader_not_in_replicas() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 2,
        };
        assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::LeaderNotInReplicas { .. })
        ));
    }

    #[test]
    fn validate_rejects_duplicate_replicas() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 1],
            old_leader: 1,
            new_leader: 1,
        };
        assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::DuplicateReplicas { .. })
        ));
    }

    #[test]
    fn validate_rejects_unknown_broker() {
        let mv = Movement {
            topic: "foo".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 99],
            old_leader: 1,
            new_leader: 1,
        };
        assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::UnknownBroker { id: 99 })
        ));
    }

    #[test]
    fn validate_rejects_unknown_partition() {
        let mv = Movement {
            topic: "ghost".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 3],
            old_leader: 1,
            new_leader: 1,
        };
        assert!(matches!(
            validate_movement(&fixture_state(), &mv),
            Err(MovementError::UnknownPartition { .. })
        ));
    }
}
```

- [ ] **Step 2: Write `proposal.rs`**

Create `crates/rebalancer/src/model/proposal.rs`:

```rust
//! Proposal + Movement types. Mirrors the proto definitions but owned
//! by the model layer so the optimizer + goals don't depend on
//! generated code.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Movement {
    pub topic: String,
    pub partition: i32,
    pub old_replicas: Vec<i32>,
    pub new_replicas: Vec<i32>,
    pub old_leader: i32,
    pub new_leader: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposalStatus {
    /// The optimizer computed the proposal but it has not been
    /// executed. Slice 43a only ever returns this state — execute
    /// lands in 43b.
    Computed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProposalSummary {
    pub replica_movements: i32,
    pub leader_movements: i32,
    pub max_replicas_before: i32,
    pub max_replicas_after: i32,
    pub max_leaders_before: i32,
    pub max_leaders_after: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub id: String,
    pub status: ProposalStatus,
    pub created_at_ms: i64,
    pub goals_applied: Vec<String>,
    pub summary: ProposalSummary,
    pub movements: Vec<Movement>,
}
```

- [ ] **Step 3: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Add (after the existing `pub mod pb;` block):

```rust
pub mod model;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib model::tests -- --nocapture`
Expected: 6 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/rebalancer/src/model crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): model module — ClusterState + Movement + validity

Pure data + the validate_movement helper used by the optimizer to
drop malformed proposals. Six unit tests cover every MovementError
variant.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 4: `goals` module — Goal trait, GoalContext, GoalPriority

**Files:**
- Create: `crates/rebalancer/src/goals/mod.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the trait + context types**

Create `crates/rebalancer/src/goals/mod.rs`:

```rust
//! `Goal` trait and shared context. Concrete goals live in sibling
//! modules (`preferred_leader_idempotency`, `replica_distribution`,
//! `leader_distribution`).

use crate::model::{ClusterState, Movement};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalPriority {
    /// Hard goals must be satisfied. If the optimizer truncates the
    /// movement list at `max_movements_per_proposal` and a hard goal
    /// still has unfulfilled movements, the optimizer returns
    /// `OptimizeError::HardGoalUnsatisfied`.
    Hard,
    /// Soft goals improve placement on a best-effort basis. Movements
    /// that don't fit under the cap are simply skipped.
    Soft,
}

#[derive(Debug, Clone, Copy)]
pub struct GoalContext {
    /// `(max - min) * 100 / total` must exceed this percentage for a
    /// soft goal to act. Hard goals ignore the threshold.
    pub imbalance_threshold_pct: u32,
    /// Safety cap on the total number of movements a single proposal
    /// can produce. Truncation drops soft-goal movements first.
    pub max_movements_per_proposal: usize,
}

pub trait Goal: Send + Sync {
    /// Stable identifier surfaced in `Proposal::goals_applied`. Must
    /// match the user-facing name accepted in
    /// `CreateProposalRequest::goals`.
    fn name(&self) -> &'static str;

    fn priority(&self) -> GoalPriority;

    /// Inspect `state` and return an ordered list of movements that
    /// improve (or satisfy) this goal. An empty `Vec` means the goal
    /// is already satisfied. Movements are intent; the optimizer
    /// validates and reconciles them across goals before producing
    /// the final proposal.
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal goal that returns a fixed movement list. Used by
    /// `optimizer::tests` to exercise the optimizer without depending
    /// on any concrete goal implementation.
    pub(crate) struct FixedGoal {
        pub(crate) name: &'static str,
        pub(crate) priority: GoalPriority,
        pub(crate) movements: Vec<Movement>,
    }

    impl Goal for FixedGoal {
        fn name(&self) -> &'static str {
            self.name
        }
        fn priority(&self) -> GoalPriority {
            self.priority
        }
        fn propose(&self, _: &ClusterState, _: &GoalContext) -> Vec<Movement> {
            self.movements.clone()
        }
    }

    #[test]
    fn priority_ordering_hard_before_soft() {
        assert!(matches!(GoalPriority::Hard, GoalPriority::Hard));
        assert_ne!(GoalPriority::Hard, GoalPriority::Soft);
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Append:

```rust
pub mod goals;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib goals -- --nocapture`
Expected: 1 test passes.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/goals crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): Goal trait + GoalContext + GoalPriority

Pure-logic abstraction the optimizer iterates over. Test-only
FixedGoal helper exposed via cfg(test) for optimizer tests.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 5: `ProposalStore` — in-memory ring buffer

**Files:**
- Create: `crates/rebalancer/src/model/store.rs`
- Modify: `crates/rebalancer/src/model/mod.rs`

- [ ] **Step 1: Write failing test + implementation**

Create `crates/rebalancer/src/model/store.rs`:

```rust
//! In-memory ring buffer of recent `Proposal`s, UUID-keyed.
//!
//! Slice 43a only persists proposals for the lifetime of the
//! `crabka-rebalancer` process. Restart drops them. Slice 43b adds
//! on-disk persistence.

use std::collections::VecDeque;
use std::sync::Mutex;

use super::proposal::Proposal;

pub struct ProposalStore {
    /// Most recent insertion at the back, oldest at the front. Bounded
    /// by `capacity`.
    inner: Mutex<VecDeque<Proposal>>,
    capacity: usize,
}

impl ProposalStore {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity: capacity.max(1),
        }
    }

    /// Append a proposal; drop the oldest if capacity is exceeded.
    pub fn insert(&self, p: Proposal) {
        let mut q = self.inner.lock().expect("ProposalStore mutex poisoned");
        if q.len() == self.capacity {
            q.pop_front();
        }
        q.push_back(p);
    }

    /// Fetch one proposal by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<Proposal> {
        let q = self.inner.lock().expect("ProposalStore mutex poisoned");
        q.iter().find(|p| p.id == id).cloned()
    }

    /// Return up to `limit` proposals, most recent first. `limit == 0`
    /// uses the store's capacity as the default.
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
        assert!(s.get("a").is_none(), "a should have been evicted");
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
        s.insert(p("c")); // evicts "a"
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
}
```

- [ ] **Step 2: Mount the module + re-export**

Edit `crates/rebalancer/src/model/mod.rs`. Locate the `pub mod proposal;` line at the top of the file and append a sibling line directly under it:

```rust
pub mod proposal;
pub mod store;

pub use proposal::{Movement, Proposal, ProposalStatus, ProposalSummary};
pub use store::ProposalStore;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib model::store -- --nocapture`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/model
git commit -m "rebalancer(43a): in-memory ProposalStore ring buffer

UUID-keyed, capacity-bounded (default 20), drops oldest on overflow.
Five unit tests cover get/list/eviction/zero-capacity-clamp.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Goals (parallel: T6, T7, T8)

### Task 6: `PreferredLeaderIdempotency` (hard)

**Files:**
- Create: `crates/rebalancer/src/goals/preferred_leader_idempotency.rs`
- Modify: `crates/rebalancer/src/goals/mod.rs`

- [ ] **Step 1: Write failing tests + implementation**

Create `crates/rebalancer/src/goals/preferred_leader_idempotency.rs`:

```rust
//! Hard goal: every partition's leader equals `replicas[0]` whenever
//! that broker is alive and in ISR.

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement};

pub struct PreferredLeaderIdempotency;

impl PreferredLeaderIdempotency {
    pub const NAME: &'static str = "PreferredLeaderIdempotency";
}

impl Goal for PreferredLeaderIdempotency {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Hard
    }
    fn propose(&self, state: &ClusterState, _ctx: &GoalContext) -> Vec<Movement> {
        let alive: std::collections::HashSet<i32> = state.brokers.iter().map(|b| b.id).collect();
        let mut out = Vec::new();
        for p in &state.partitions {
            let Some(&preferred) = p.replicas.first() else { continue };
            if p.leader == preferred {
                continue;
            }
            if !alive.contains(&preferred) {
                continue;
            }
            if !p.isr.contains(&preferred) {
                continue;
            }
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: p.replicas.clone(),
                new_replicas: p.replicas.clone(),
                old_leader: p.leader,
                new_leader: preferred,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BrokerView, PartitionView};

    fn state(parts: Vec<PartitionView>, alive_brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: alive_brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions: parts,
            in_flight_reassignments: vec![],
        }
    }

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        }
    }

    #[test]
    fn preferred_already_leader_no_op() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 1,
                isr: vec![1, 2, 3],
            }],
            vec![1, 2, 3],
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn preferred_alive_in_isr_but_not_leader_triggers_swap() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![1, 2, 3],
            }],
            vec![1, 2, 3],
        );
        let mvs = PreferredLeaderIdempotency.propose(&s, &ctx());
        assert_eq!(mvs.len(), 1);
        assert_eq!(mvs[0].new_leader, 1);
        assert_eq!(mvs[0].old_leader, 2);
        assert_eq!(mvs[0].old_replicas, mvs[0].new_replicas);
    }

    #[test]
    fn preferred_dead_skipped() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![2, 3],
            }],
            vec![2, 3], // broker 1 is missing — dead
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn preferred_out_of_isr_skipped() {
        let s = state(
            vec![PartitionView {
                topic: "foo".into(),
                partition: 0,
                replicas: vec![1, 2, 3],
                leader: 2,
                isr: vec![2, 3], // broker 1 alive but not in ISR
            }],
            vec![1, 2, 3],
        );
        assert!(PreferredLeaderIdempotency.propose(&s, &ctx()).is_empty());
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/goals/mod.rs`. Add (after the existing `use` block and before the `pub enum GoalPriority` declaration):

```rust
pub mod preferred_leader_idempotency;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib goals::preferred_leader_idempotency -- --nocapture`
Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/goals
git commit -m "rebalancer(43a): PreferredLeaderIdempotency goal (hard)

Emits a leader-swap movement for every partition where replicas[0]
is alive + in-ISR but isn't the current leader. Four unit tests
cover the four state combinations.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 7: `ReplicaDistribution` (soft)

**Files:**
- Create: `crates/rebalancer/src/goals/replica_distribution.rs`
- Modify: `crates/rebalancer/src/goals/mod.rs`

- [ ] **Step 1: Write the goal + tests**

Create `crates/rebalancer/src/goals/replica_distribution.rs`:

```rust
//! Soft goal: balance the count of replicas (any role) hosted on each
//! broker. Greedy heuristic — swap one replica at a time from the
//! most-loaded broker to the least-loaded.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct ReplicaDistribution;

impl ReplicaDistribution {
    pub const NAME: &'static str = "ReplicaDistribution";

    /// Count of replicas hosted per broker id.
    fn counts(state: &ClusterState) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
        for p in &state.partitions {
            for r in &p.replicas {
                *m.entry(*r).or_insert(0) += 1;
            }
        }
        m
    }

    fn imbalance_pct(counts: &HashMap<i32, usize>) -> u32 {
        let values: Vec<usize> = counts.values().copied().collect();
        let total: usize = values.iter().sum();
        if total == 0 {
            return 0;
        }
        let max = *values.iter().max().unwrap_or(&0);
        let min = *values.iter().min().unwrap_or(&0);
        ((max - min) * 100 / total) as u32
    }
}

impl Goal for ReplicaDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        // Working clone of partitions — we mutate replicas as we
        // accumulate movements so subsequent iterations see post-move
        // counts.
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        loop {
            // Recompute counts from `working`.
            let mut counts: HashMap<i32, usize> =
                state.brokers.iter().map(|b| (b.id, 0)).collect();
            for p in &working {
                for r in &p.replicas {
                    *counts.entry(*r).or_insert(0) += 1;
                }
            }
            if Self::imbalance_pct(&counts) <= ctx.imbalance_threshold_pct {
                break;
            }
            // Sort brokers by load: descending for "most loaded", ascending for "least loaded".
            let mut by_load: Vec<(i32, usize)> = counts.into_iter().collect();
            by_load.sort_by(|a, b| b.1.cmp(&a.1));
            let (hot, _hot_count) = *by_load.first().expect("at least one broker");
            let (cold, _cold_count) = *by_load.last().expect("at least one broker");
            if hot == cold {
                break;
            }
            // Find a partition on `hot` whose `replicas` set excludes `cold`.
            let candidate_idx = working.iter().position(|p| {
                p.replicas.contains(&hot)
                    && !p.replicas.contains(&cold)
                    // Skip if the replica set already covers every alive broker — no spare home.
                    && p.replicas.len() < state.brokers.len()
            });
            let Some(idx) = candidate_idx else {
                // No valid swap remains.
                break;
            };
            let p = &mut working[idx];
            let old_replicas = p.replicas.clone();
            let pos = p.replicas.iter().position(|r| *r == hot).unwrap();
            p.replicas[pos] = cold;
            // If the leader was `hot`, choose a new leader from the new replica set.
            // Prefer staying with whatever's left of the prior ISR; fall back to the
            // first member of the new replica set.
            let new_leader = if p.leader == hot {
                *p.replicas
                    .iter()
                    .find(|r| p.isr.contains(r))
                    .unwrap_or(&p.replicas[0])
            } else {
                p.leader
            };
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas,
                new_replicas: p.replicas.clone(),
                old_leader: p.leader,
                new_leader,
            });
            p.leader = new_leader;

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        }
    }

    fn state_with(partitions: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions,
            in_flight_reassignments: vec![],
        }
    }

    #[test]
    fn balanced_cluster_no_movements() {
        let parts = vec![
            PartitionView { topic: "t".into(), partition: 0, replicas: vec![1, 2], leader: 1, isr: vec![1, 2] },
            PartitionView { topic: "t".into(), partition: 1, replicas: vec![2, 3], leader: 2, isr: vec![2, 3] },
            PartitionView { topic: "t".into(), partition: 2, replicas: vec![1, 3], leader: 3, isr: vec![1, 3] },
        ];
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn one_hot_broker_produces_swaps() {
        // Every replica on broker 1 — broker 2 + 3 empty.
        let parts = (0..6)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1],
                leader: 1,
                isr: vec![1],
            })
            .collect();
        let s = state_with(parts, vec![1, 2, 3]);
        let mvs = ReplicaDistribution.propose(&s, &ctx());
        assert!(!mvs.is_empty(), "expected at least one swap");
        // RF preserved on every movement.
        for m in &mvs {
            assert_eq!(m.old_replicas.len(), m.new_replicas.len());
        }
    }

    #[test]
    fn partition_already_on_every_broker_skipped() {
        // RF == broker_count: no spare home for swaps.
        let parts = vec![PartitionView {
            topic: "t".into(),
            partition: 0,
            replicas: vec![1, 2, 3],
            leader: 1,
            isr: vec![1, 2, 3],
        }];
        let s = state_with(parts, vec![1, 2, 3]);
        assert!(ReplicaDistribution.propose(&s, &ctx()).is_empty());
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/goals/mod.rs`. Add directly after the `preferred_leader_idempotency` mount:

```rust
pub mod replica_distribution;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib goals::replica_distribution -- --nocapture`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/goals
git commit -m "rebalancer(43a): ReplicaDistribution goal (soft)

Greedy most-loaded → least-loaded swaps until the imbalance ratio
falls below threshold or no valid swap remains. RF preserved.
Three unit tests cover balanced / hot-broker / RF==broker_count.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 8: `LeaderDistribution` (soft)

**Files:**
- Create: `crates/rebalancer/src/goals/leader_distribution.rs`
- Modify: `crates/rebalancer/src/goals/mod.rs`

- [ ] **Step 1: Write the goal + tests**

Create `crates/rebalancer/src/goals/leader_distribution.rs`:

```rust
//! Soft goal: balance the count of partitions led per broker.
//! Movements are leader-only — replicas stay put — and only target
//! brokers already in the partition's replica set.

use std::collections::HashMap;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{ClusterState, Movement, PartitionView};

pub struct LeaderDistribution;

impl LeaderDistribution {
    pub const NAME: &'static str = "LeaderDistribution";

    fn leader_counts(state: &ClusterState) -> HashMap<i32, usize> {
        let mut m: HashMap<i32, usize> = state.brokers.iter().map(|b| (b.id, 0)).collect();
        for p in &state.partitions {
            *m.entry(p.leader).or_insert(0) += 1;
        }
        m
    }

    fn imbalance_pct(counts: &HashMap<i32, usize>) -> u32 {
        let values: Vec<usize> = counts.values().copied().collect();
        let total: usize = values.iter().sum();
        if total == 0 {
            return 0;
        }
        let max = *values.iter().max().unwrap_or(&0);
        let min = *values.iter().min().unwrap_or(&0);
        ((max - min) * 100 / total) as u32
    }
}

impl Goal for LeaderDistribution {
    fn name(&self) -> &'static str {
        Self::NAME
    }
    fn priority(&self) -> GoalPriority {
        GoalPriority::Soft
    }
    fn propose(&self, state: &ClusterState, ctx: &GoalContext) -> Vec<Movement> {
        let mut working: Vec<PartitionView> = state.partitions.clone();
        let mut out: Vec<Movement> = Vec::new();

        loop {
            let mut counts: HashMap<i32, usize> =
                state.brokers.iter().map(|b| (b.id, 0)).collect();
            for p in &working {
                *counts.entry(p.leader).or_insert(0) += 1;
            }
            if Self::imbalance_pct(&counts) <= ctx.imbalance_threshold_pct {
                break;
            }
            let mut by_load: Vec<(i32, usize)> = counts.into_iter().collect();
            by_load.sort_by(|a, b| b.1.cmp(&a.1));
            let (hot, _) = *by_load.first().expect("at least one broker");
            let (cold, _) = *by_load.last().expect("at least one broker");
            if hot == cold {
                break;
            }
            // Find a partition where:
            // - leader is `hot`
            // - `cold` is in the replica set (leader-only moves can
            //   only target an existing replica)
            // - `cold` is in ISR (leader must be in ISR per Kafka
            //   invariants)
            let idx = working.iter().position(|p| {
                p.leader == hot && p.replicas.contains(&cold) && p.isr.contains(&cold)
            });
            let Some(idx) = idx else {
                break;
            };
            let p = &mut working[idx];
            let old_leader = p.leader;
            let old_replicas = p.replicas.clone();
            p.leader = cold;
            out.push(Movement {
                topic: p.topic.clone(),
                partition: p.partition,
                old_replicas: old_replicas.clone(),
                new_replicas: old_replicas, // leader-only move
                old_leader,
                new_leader: cold,
            });

            if out.len() >= ctx.max_movements_per_proposal {
                break;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::BrokerView;

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        }
    }

    fn state_with(partitions: Vec<PartitionView>, brokers: Vec<i32>) -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: brokers
                .into_iter()
                .map(|id| BrokerView {
                    id,
                    host: format!("h{id}"),
                    port: 9092,
                    rack: None,
                })
                .collect(),
            partitions,
            in_flight_reassignments: vec![],
        }
    }

    #[test]
    fn balanced_no_movements() {
        let parts = vec![
            PartitionView { topic: "t".into(), partition: 0, replicas: vec![1, 2], leader: 1, isr: vec![1, 2] },
            PartitionView { topic: "t".into(), partition: 1, replicas: vec![1, 2], leader: 2, isr: vec![1, 2] },
        ];
        let s = state_with(parts, vec![1, 2]);
        assert!(LeaderDistribution.propose(&s, &ctx()).is_empty());
    }

    #[test]
    fn leader_only_movements_preserve_replicas() {
        // Every partition led by broker 1; broker 2 in every replica set.
        let parts = (0..4)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            })
            .collect();
        let s = state_with(parts, vec![1, 2]);
        let mvs = LeaderDistribution.propose(&s, &ctx());
        assert!(!mvs.is_empty());
        for m in &mvs {
            assert_eq!(m.old_replicas, m.new_replicas, "leader-only move");
            assert_eq!(m.old_leader, 1);
            assert_eq!(m.new_leader, 2);
        }
    }

    #[test]
    fn skips_when_cold_broker_not_in_replicas() {
        // Broker 3 is "cold" but isn't in any partition's replica set.
        let parts = (0..4)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 1,
                isr: vec![1, 2],
            })
            .collect();
        // 3 brokers, but every partition only has {1, 2} as replicas.
        let s = state_with(parts, vec![1, 2, 3]);
        // The leader counts are {1:4, 2:0, 3:0}. ImbalancePct = (4-0)*100/4 = 100%.
        // Threshold of 10% is exceeded but no partition has broker 3 in its replicas
        // — so no movement to broker 3 is valid. The goal can still propose 1→2 moves
        // until {1:2, 2:2, 3:0} (pct=(2-0)*100/4 = 50%) and further to {1:2, 2:2, 3:0}
        // and stop because the largest move to broker 3 isn't possible.
        // To prove the "skip when cold not in replicas" path *deterministically*,
        // we want broker 3 to *consistently* be the cold one — which it is, count 0
        // forever. The goal will still produce 1→2 moves until 1 and 2 are tied,
        // then loop with hot=1 or 2 (tied) and cold=3 → no valid move → break.
        let mvs = LeaderDistribution.propose(&s, &ctx());
        // No movement may target broker 3 as new_leader.
        for m in &mvs {
            assert_ne!(m.new_leader, 3, "broker 3 isn't in any replica set");
        }
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/goals/mod.rs`. Add directly after the `replica_distribution` mount:

```rust
pub mod leader_distribution;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib goals::leader_distribution -- --nocapture`
Expected: 3 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/goals
git commit -m "rebalancer(43a): LeaderDistribution goal (soft)

Leader-only swaps from most-loaded → least-loaded broker, restricted
to brokers already in the partition's replica set and ISR. Three
unit tests cover balanced, hot-leader, and cold-not-in-replicas.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — Optimizer

### Task 9: `optimize()` function

**Files:**
- Create: `crates/rebalancer/src/optimizer/mod.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the optimizer + tests**

Create `crates/rebalancer/src/optimizer/mod.rs`:

```rust
//! Optimizer: runs an ordered list of `Goal`s over a `ClusterState`,
//! coalesces their movements, and emits a `Proposal`.

use std::collections::HashMap;

use uuid::Uuid;

use crate::goals::{Goal, GoalContext, GoalPriority};
use crate::model::{validate_movement, ClusterState, Movement, PartitionView, Proposal,
                   ProposalStatus, ProposalSummary};

#[derive(Debug, thiserror::Error)]
pub enum OptimizeError {
    #[error("hard goal `{goal}` produced {extra} movements past the {cap} cap")]
    HardGoalUnsatisfied { goal: String, extra: usize, cap: usize },
}

pub struct OptimizeOutput {
    pub proposal: Proposal,
    pub state_after: ClusterState,
}

/// Run the goals over `state` and produce a `Proposal`. Goals are
/// applied in priority order (Hard before Soft). The cluster state
/// passed to each goal reflects the cumulative effect of prior goals'
/// movements — soft goals see post-hard-goal counts.
pub fn optimize(
    state: &ClusterState,
    goals: &[&dyn Goal],
    ctx: &GoalContext,
) -> Result<OptimizeOutput, OptimizeError> {
    // 1. Order: Hard first, ties broken by registration order.
    let mut ordered: Vec<(usize, &&dyn Goal)> = goals.iter().enumerate().collect();
    ordered.sort_by_key(|(idx, g)| {
        (
            match g.priority() {
                GoalPriority::Hard => 0,
                GoalPriority::Soft => 1,
            },
            *idx,
        )
    });

    // 2. Working clone of the state — each Movement updates it before
    //    the next goal sees it.
    let mut working = state.clone();

    // (topic, partition) → Movement. Last writer wins on coalesce.
    let mut accum: HashMap<(String, i32), Movement> = HashMap::new();
    let mut goals_applied: Vec<String> = Vec::new();
    let mut hard_overflow: Option<(String, usize)> = None;

    for (_idx, g) in &ordered {
        goals_applied.push(g.name().to_string());
        let movements = g.propose(&working, ctx);
        for m in movements {
            if validate_movement(&working, &m).is_err() {
                // Silently drop — the goal will see the unchanged state next iter.
                continue;
            }
            // Apply to working state immediately.
            apply_movement(&mut working, &m);
            let key = (m.topic.clone(), m.partition);
            accum.insert(key, m);
        }
        if accum.len() > ctx.max_movements_per_proposal {
            let extra = accum.len() - ctx.max_movements_per_proposal;
            if matches!(g.priority(), GoalPriority::Hard) {
                hard_overflow = Some((g.name().to_string(), extra));
            }
        }
    }

    if let Some((goal, extra)) = hard_overflow {
        return Err(OptimizeError::HardGoalUnsatisfied {
            goal,
            extra,
            cap: ctx.max_movements_per_proposal,
        });
    }

    // 3. Order the accumulated movements deterministically: by (topic, partition).
    let mut movements: Vec<Movement> = accum.into_values().collect();
    movements.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    // 4. Truncate to cap.
    movements.truncate(ctx.max_movements_per_proposal);

    // 5. Compute summary.
    let summary = compute_summary(state, &working, &movements);

    Ok(OptimizeOutput {
        proposal: Proposal {
            id: Uuid::new_v4().to_string(),
            status: ProposalStatus::Computed,
            created_at_ms: now_ms(),
            goals_applied,
            summary,
            movements,
        },
        state_after: working,
    })
}

fn apply_movement(state: &mut ClusterState, m: &Movement) {
    if let Some(p) = state
        .partitions
        .iter_mut()
        .find(|p| p.topic == m.topic && p.partition == m.partition)
    {
        p.replicas = m.new_replicas.clone();
        p.leader = m.new_leader;
        // ISR: drop replicas that left the set; otherwise leave.
        p.isr.retain(|r| p.replicas.contains(r));
        // If the new leader isn't in ISR, add it (we assume the
        // executor has caught up the replica; slice 43b's executor
        // will gate on real ISR catch-up).
        if !p.isr.contains(&p.leader) {
            p.isr.push(p.leader);
        }
    }
}

fn compute_summary(
    before: &ClusterState,
    after: &ClusterState,
    movements: &[Movement],
) -> ProposalSummary {
    let replica_movements = movements
        .iter()
        .filter(|m| m.old_replicas != m.new_replicas)
        .count() as i32;
    let leader_movements = movements
        .iter()
        .filter(|m| m.old_leader != m.new_leader)
        .count() as i32;

    ProposalSummary {
        replica_movements,
        leader_movements,
        max_replicas_before: max_per_broker(&before.partitions, |p| p.replicas.iter().copied()) as i32,
        max_replicas_after: max_per_broker(&after.partitions, |p| p.replicas.iter().copied()) as i32,
        max_leaders_before: max_per_broker(&before.partitions, |p| std::iter::once(p.leader)) as i32,
        max_leaders_after: max_per_broker(&after.partitions, |p| std::iter::once(p.leader)) as i32,
    }
}

fn max_per_broker<F, I>(parts: &[PartitionView], f: F) -> usize
where
    F: Fn(&PartitionView) -> I,
    I: IntoIterator<Item = i32>,
{
    let mut counts: HashMap<i32, usize> = HashMap::new();
    for p in parts {
        for b in f(p) {
            *counts.entry(b).or_insert(0) += 1;
        }
    }
    counts.values().copied().max().unwrap_or(0)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goals::tests::FixedGoal;
    use crate::model::{BrokerView, PartitionView};

    fn ctx() -> GoalContext {
        GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 256,
        }
    }

    fn state() -> ClusterState {
        ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![
                BrokerView { id: 1, host: "h1".into(), port: 9092, rack: None },
                BrokerView { id: 2, host: "h2".into(), port: 9092, rack: None },
            ],
            partitions: vec![PartitionView {
                topic: "t".into(),
                partition: 0,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            }],
            in_flight_reassignments: vec![],
        }
    }

    fn mv() -> Movement {
        Movement {
            topic: "t".into(),
            partition: 0,
            old_replicas: vec![1, 2],
            new_replicas: vec![1, 2],
            old_leader: 2,
            new_leader: 1,
        }
    }

    #[test]
    fn hard_runs_before_soft() {
        let soft = FixedGoal {
            name: "soft",
            priority: GoalPriority::Soft,
            movements: vec![mv()],
        };
        let hard = FixedGoal {
            name: "hard",
            priority: GoalPriority::Hard,
            movements: vec![],
        };
        // Soft first in `goals` list — but optimizer must call hard first.
        let goals: Vec<&dyn Goal> = vec![&soft, &hard];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert_eq!(out.proposal.goals_applied[0], "hard");
        assert_eq!(out.proposal.goals_applied[1], "soft");
    }

    #[test]
    fn empty_goals_returns_no_movements() {
        let goals: Vec<&dyn Goal> = vec![];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert!(out.proposal.movements.is_empty());
        assert_eq!(out.proposal.status, ProposalStatus::Computed);
    }

    #[test]
    fn duplicate_movements_coalesce_last_writer_wins() {
        let g1 = FixedGoal {
            name: "g1",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            }],
        };
        // g2 emits a movement with the SAME (topic, partition) but a
        // different new_leader — this would normally be rejected by
        // validate_movement against the post-g1 state because new_leader=2
        // and current leader=1 are both in the replica set, so it's valid.
        // After coalesce, g2's wins.
        let g2 = FixedGoal {
            name: "g2",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "t".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 1,
                new_leader: 2,
            }],
        };
        let goals: Vec<&dyn Goal> = vec![&g1, &g2];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert_eq!(out.proposal.movements.len(), 1);
        assert_eq!(out.proposal.movements[0].new_leader, 2);
    }

    #[test]
    fn invalid_movement_silently_dropped() {
        let bad = FixedGoal {
            name: "bad",
            priority: GoalPriority::Soft,
            movements: vec![Movement {
                topic: "ghost".into(),
                partition: 0,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 1,
                new_leader: 1,
            }],
        };
        let goals: Vec<&dyn Goal> = vec![&bad];
        let out = optimize(&state(), &goals, &ctx()).unwrap();
        assert!(out.proposal.movements.is_empty());
    }

    #[test]
    fn hard_goal_overflow_returns_error() {
        let mut movements = Vec::new();
        // 5 valid leader-flip movements would fit, but cap is 3.
        for i in 0..5 {
            movements.push(Movement {
                topic: "t".into(),
                partition: i,
                old_replicas: vec![1, 2],
                new_replicas: vec![1, 2],
                old_leader: 2,
                new_leader: 1,
            });
        }
        let mut s = state();
        // Multi-partition state.
        s.partitions = (0..5)
            .map(|i| PartitionView {
                topic: "t".into(),
                partition: i,
                replicas: vec![1, 2],
                leader: 2,
                isr: vec![1, 2],
            })
            .collect();
        let bulk = FixedGoal {
            name: "bulk",
            priority: GoalPriority::Hard,
            movements,
        };
        let ctx = GoalContext {
            imbalance_threshold_pct: 10,
            max_movements_per_proposal: 3,
        };
        let goals: Vec<&dyn Goal> = vec![&bulk];
        let err = optimize(&s, &goals, &ctx).unwrap_err();
        assert!(matches!(
            err,
            OptimizeError::HardGoalUnsatisfied { extra: 2, cap: 3, .. }
        ));
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Append:

```rust
pub mod optimizer;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib optimizer -- --nocapture`
Expected: 5 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/optimizer crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): optimize() — runs Goals, coalesces, summarises

Hard-first priority order, last-writer-wins coalesce per
(topic, partition), Hard-goal-overflow error path, deterministic
movement order (by topic+partition). Five unit tests covering the
priority, empty-goals, coalesce, invalid-drop, and overflow paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 6 — Ingest

### Task 10: `Ingester` + admin RPC wrappers + tests

**Files:**
- Create: `crates/rebalancer/src/ingest/admin_client.rs`
- Create: `crates/rebalancer/src/ingest/mod.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the admin client wrapper**

Create `crates/rebalancer/src/ingest/admin_client.rs`:

```rust
//! Thin typed wrappers over `crabka_client_core::Client` for the
//! three RPCs the ingester needs each tick. Returning typed responses
//! keeps the `Ingester` free of `crabka_protocol` imports.

use crabka_client_core::{Client, ClientError};
use crabka_protocol::owned::describe_cluster_request::DescribeClusterRequest;
use crabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;
use crabka_protocol::owned::list_partition_reassignments_request::ListPartitionReassignmentsRequest;
use crabka_protocol::owned::list_partition_reassignments_response::ListPartitionReassignmentsResponse;
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::metadata_response::MetadataResponse;

pub async fn fetch_metadata(client: &Client) -> Result<MetadataResponse, ClientError> {
    // v12: flexible, topic_id-aware. allow_auto_topic_creation = false.
    let req = MetadataRequest {
        topics: None,
        allow_auto_topic_creation: false,
        ..Default::default()
    };
    client.send(req).await
}

pub async fn fetch_describe_cluster(client: &Client) -> Result<DescribeClusterResponse, ClientError> {
    client.send(DescribeClusterRequest::default()).await
}

pub async fn fetch_list_reassignments(
    client: &Client,
) -> Result<ListPartitionReassignmentsResponse, ClientError> {
    // topics = None ⇒ all in-flight reassignments.
    client
        .send(ListPartitionReassignmentsRequest::default())
        .await
}
```

- [ ] **Step 2: Write the Ingester + snapshot loop**

Create `crates/rebalancer/src/ingest/mod.rs`:

```rust
//! Periodic cluster-state snapshotter. Spawned by the binary entry;
//! writes the latest snapshot into an `ArcSwap<Option<ClusterState>>`
//! that the RPC handlers read.

pub mod admin_client;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use crabka_client_core::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::model::{BrokerView, ClusterState, InFlightReassignment, PartitionView};

pub type SharedSnapshot = Arc<ArcSwap<Option<ClusterState>>>;

pub fn new_shared_snapshot() -> SharedSnapshot {
    Arc::new(ArcSwap::new(Arc::new(None)))
}

pub struct Ingester {
    client: Client,
    interval: Duration,
    snapshot: SharedSnapshot,
    shutdown: CancellationToken,
}

impl Ingester {
    #[must_use]
    pub fn new(
        client: Client,
        interval: Duration,
        snapshot: SharedSnapshot,
        shutdown: CancellationToken,
    ) -> Self {
        Self { client, interval, snapshot, shutdown }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        // First tick fires immediately — snapshot once at startup before
        // sleeping.
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                () = self.shutdown.cancelled() => {
                    info!("ingester shutting down");
                    return;
                }
            }
            match snapshot_once(&self.client).await {
                Ok(state) => {
                    debug!(brokers = state.brokers.len(), partitions = state.partitions.len(),
                           "snapshot ok");
                    self.snapshot.store(Arc::new(Some(state)));
                }
                Err(e) => {
                    warn!(error = %e, "snapshot tick failed; keeping prior state");
                }
            }
        }
    }
}

pub async fn snapshot_once(client: &Client) -> Result<ClusterState, anyhow::Error> {
    let md = admin_client::fetch_metadata(client).await?;
    let dc = admin_client::fetch_describe_cluster(client).await?;
    let lpr = admin_client::fetch_list_reassignments(client).await?;

    let brokers: Vec<BrokerView> = md
        .brokers
        .iter()
        .map(|b| BrokerView {
            id: b.node_id,
            host: b.host.clone(),
            port: b.port,
            rack: b.rack.clone(),
        })
        .collect();

    let mut partitions: Vec<PartitionView> = Vec::new();
    for t in &md.topics {
        let topic_name = t.name.clone().unwrap_or_default();
        for p in &t.partitions {
            partitions.push(PartitionView {
                topic: topic_name.clone(),
                partition: p.partition_index,
                replicas: p.replica_nodes.clone(),
                leader: p.leader_id,
                isr: p.isr_nodes.clone(),
            });
        }
    }

    let mut in_flight: Vec<InFlightReassignment> = Vec::new();
    for t in &lpr.topics {
        for p in &t.partitions {
            in_flight.push(InFlightReassignment {
                topic: t.name.clone(),
                partition: p.partition_index,
                adding: p.adding_replicas.clone(),
                removing: p.removing_replicas.clone(),
            });
        }
    }

    Ok(ClusterState {
        cluster_id: Some(dc.cluster_id.clone()).filter(|s| !s.is_empty()),
        snapshot_at_ms: now_ms(),
        brokers,
        partitions,
        in_flight_reassignments: in_flight,
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_starts_as_none() {
        let s = new_shared_snapshot();
        let g = s.load();
        assert!(g.as_ref().is_none());
    }

    #[test]
    fn swap_replaces_value() {
        let s = new_shared_snapshot();
        let state = ClusterState {
            cluster_id: Some("c".into()),
            snapshot_at_ms: 42,
            brokers: vec![],
            partitions: vec![],
            in_flight_reassignments: vec![],
        };
        s.store(Arc::new(Some(state.clone())));
        let g = s.load();
        let v = (*g).as_ref().expect("Some after swap");
        assert_eq!(v.snapshot_at_ms, 42);
        assert_eq!(v.cluster_id.as_deref(), Some("c"));
    }
}
```

Before continuing, verify the response field names match. Run:

```bash
grep -nE "pub node_id|pub host:|pub port:|pub rack:|pub partition_index|pub replica_nodes|pub leader_id|pub isr_nodes|pub cluster_id|pub adding_replicas|pub removing_replicas|pub topics" \
  /home/matt/git/crabka/crates/protocol/generated/MetadataResponse.owned.rs \
  /home/matt/git/crabka/crates/protocol/generated/DescribeClusterResponse.owned.rs \
  /home/matt/git/crabka/crates/protocol/generated/ListPartitionReassignmentsResponse.owned.rs | head -40
```

If any field name in the snapshot code above doesn't match the generated source, fix it before moving on. (The Kafka spec sometimes uses `name`, sometimes `topic` — check the actual generated struct.)

- [ ] **Step 3: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Append:

```rust
pub mod ingest;
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib ingest -- --nocapture`
Expected: 2 tests pass. (We can't unit-test `snapshot_once` without a live broker; that's covered by the end-to-end test in Task 14.)

- [ ] **Step 5: Commit**

```bash
git add crates/rebalancer/src/ingest crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): Ingester + admin RPC wrappers

Snapshot loop: Metadata + DescribeCluster + ListPartitionReassignments
every `interval`. Result written into ArcSwap<Option<ClusterState>>;
errors leave the prior snapshot in place. Two unit tests cover the
ArcSwap semantics.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 7 — API + health (parallel: T11, T12)

### Task 11: Connect-RPC service impl + axum mount

**Files:**
- Create: `crates/rebalancer/src/api/mod.rs`
- Create: `crates/rebalancer/src/api/handlers.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Discover the generated trait surface**

The exact trait name and method signatures depend on what `connectrpc-axum-build` emits. Before writing handlers, check the generated module:

```bash
find /home/matt/git/crabka/target -path "*build*rebalancer*out*crabka.rebalancer.v1.rs" -exec head -120 {} \;
```

(If multiple build artifacts exist, take the most recent one under `target/debug/build/crabka-rebalancer-<hash>/out/`.)

The generated code typically exposes:
- `pub mod rebalancer_server { pub trait Rebalancer { ... } pub fn router(svc: impl Rebalancer) -> Router { ... } }`
- Or under whatever module name `connectrpc-axum-build` chose (likely `pub mod rebalancer` or `pub mod rebalancer_server`).

Note the exact trait name and the `async fn` signatures. The rest of this task assumes:
- Trait: `pub trait Rebalancer: Send + Sync + 'static`
- Methods: `async fn get_state(&self, req: Request<GetStateRequest>) -> Result<Response<GetStateResponse>, Status>` (Connect uses tonic-style Request/Response/Status types from `connectrpc-axum`).

If the generated surface differs (e.g. different module path or function-style handlers), adapt the imports and signatures below to match. Do NOT fight the generator.

- [ ] **Step 2: Write the service impl**

Create `crates/rebalancer/src/api/handlers.rs`:

```rust
//! One Rust fn per Connect-RPC method. Each takes `&AppState` and
//! the typed request, returns a typed response or a `Status` error.

use std::sync::Arc;

use crate::ingest::SharedSnapshot;
use crate::model::{ProposalStore, ClusterState};
use crate::pb;

/// State shared across all RPC handlers.
pub struct AppState {
    pub snapshot: SharedSnapshot,
    pub store: Arc<ProposalStore>,
    pub goal_registry: crate::api::GoalRegistry,
    pub goal_ctx: crate::goals::GoalContext,
}

/// Convert a `ClusterState` into the proto `GetStateResponse`.
pub fn cluster_state_to_proto(state: &ClusterState) -> pb::GetStateResponse {
    let mut topics_by_name: std::collections::BTreeMap<String, Vec<pb::Partition>> =
        std::collections::BTreeMap::new();
    for p in &state.partitions {
        topics_by_name.entry(p.topic.clone()).or_default().push(pb::Partition {
            partition: p.partition,
            replicas: p.replicas.clone(),
            leader: p.leader,
            isr: p.isr.clone(),
        });
    }
    pb::GetStateResponse {
        snapshot_at_ms: state.snapshot_at_ms,
        brokers: state
            .brokers
            .iter()
            .map(|b| pb::Broker {
                id: b.id,
                host: b.host.clone(),
                port: b.port,
                rack: b.rack.clone(),
            })
            .collect(),
        topics: topics_by_name
            .into_iter()
            .map(|(name, partitions)| pb::Topic { name, partitions })
            .collect(),
        in_flight_reassignments: state
            .in_flight_reassignments
            .iter()
            .map(|r| pb::InFlightReassignment {
                topic: r.topic.clone(),
                partition: r.partition,
                adding_replicas: r.adding.clone(),
                removing_replicas: r.removing.clone(),
            })
            .collect(),
    }
}

pub fn proposal_to_proto(p: &crate::model::Proposal) -> pb::Proposal {
    pb::Proposal {
        id: p.id.clone(),
        status: pb::ProposalStatus::Computed as i32,
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
    }
}
```

Create `crates/rebalancer/src/api/mod.rs`:

```rust
//! Connect-RPC service implementation.
//!
//! The generated trait + router live in `crate::pb::rebalancer_server`
//! (or whatever module the codegen produces — confirm via Step 1 of
//! Task 11). `RebalancerService` implements it and adapts each method
//! to the rebalancer's in-memory state.

pub mod handlers;

use std::sync::Arc;

use crate::goals::{Goal, GoalContext};
use crate::ingest::SharedSnapshot;
use crate::model::ProposalStore;
use crate::optimizer;

/// Registry of `Goal` trait objects, name-keyed. Maps the
/// `CreateProposalRequest::goals` strings to concrete implementations.
pub struct GoalRegistry {
    /// Insertion order is preserved as the canonical priority order
    /// after stable-sorting by `GoalPriority`.
    goals: Vec<Box<dyn Goal>>,
}

impl GoalRegistry {
    #[must_use]
    pub fn default_registry() -> Self {
        Self {
            goals: vec![
                Box::new(crate::goals::preferred_leader_idempotency::PreferredLeaderIdempotency),
                Box::new(crate::goals::replica_distribution::ReplicaDistribution),
                Box::new(crate::goals::leader_distribution::LeaderDistribution),
            ],
        }
    }

    #[must_use]
    pub fn select<'a>(
        &'a self,
        names: &[String],
    ) -> Result<Vec<&'a dyn Goal>, GoalSelectError> {
        if names.is_empty() {
            return Ok(self.goals.iter().map(|b| b.as_ref()).collect());
        }
        let mut out = Vec::with_capacity(names.len());
        for n in names {
            let g = self
                .goals
                .iter()
                .find(|g| g.name() == n)
                .ok_or_else(|| GoalSelectError::Unknown(n.clone()))?;
            out.push(g.as_ref());
        }
        Ok(out)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GoalSelectError {
    #[error("unknown goal `{0}`")]
    Unknown(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_registry_has_three_goals() {
        let r = GoalRegistry::default_registry();
        let all = r.select(&[]).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn select_by_name() {
        let r = GoalRegistry::default_registry();
        let one = r.select(&["ReplicaDistribution".into()]).unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].name(), "ReplicaDistribution");
    }

    #[test]
    fn select_unknown_goal_errors() {
        let r = GoalRegistry::default_registry();
        let err = r.select(&["GhostGoal".into()]).unwrap_err();
        assert!(matches!(err, GoalSelectError::Unknown(ref n) if n == "GhostGoal"));
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Connect-RPC service trait impl. The exact trait path + signatures depend on
// what connectrpc-axum-build emitted into `crate::pb`. Confirm via Task 11
// Step 1 and adapt the impl below. The structure below assumes the codegen
// produces a `pb::rebalancer_server::Rebalancer` async trait with the six
// methods from the proto file and a `pb::rebalancer_server::router(svc)`
// helper that returns an `axum::Router`.
// ────────────────────────────────────────────────────────────────────────────

use connectrpc_axum::{Request, Response, Status, Code};

pub struct RebalancerService {
    pub state: Arc<handlers::AppState>,
}

#[async_trait::async_trait]
impl crate::pb::rebalancer_server::Rebalancer for RebalancerService {
    async fn get_state(
        &self,
        _req: Request<crate::pb::GetStateRequest>,
    ) -> Result<Response<crate::pb::GetStateResponse>, Status> {
        let g = self.state.snapshot.load();
        let Some(state) = (*g).as_ref() else {
            return Err(Status::new(Code::Unavailable, "no snapshot yet"));
        };
        Ok(Response::new(handlers::cluster_state_to_proto(state)))
    }

    async fn create_proposal(
        &self,
        req: Request<crate::pb::CreateProposalRequest>,
    ) -> Result<Response<crate::pb::Proposal>, Status> {
        let g = self.state.snapshot.load();
        let Some(snap) = (*g).as_ref() else {
            return Err(Status::new(Code::Unavailable, "no snapshot yet"));
        };
        let goals = self
            .state
            .goal_registry
            .select(&req.into_inner().goals)
            .map_err(|e| Status::new(Code::InvalidArgument, e.to_string()))?;
        let out = optimizer::optimize(snap, &goals, &self.state.goal_ctx)
            .map_err(|e| Status::new(Code::Internal, e.to_string()))?;
        self.state.store.insert(out.proposal.clone());
        Ok(Response::new(handlers::proposal_to_proto(&out.proposal)))
    }

    async fn dry_run_proposal(
        &self,
        req: Request<crate::pb::DryRunProposalRequest>,
    ) -> Result<Response<crate::pb::DryRunResponse>, Status> {
        let id = req.into_inner().id;
        let p = self
            .state
            .store
            .get(&id)
            .ok_or_else(|| Status::new(Code::NotFound, format!("proposal `{id}` not found")))?;
        let proto = handlers::proposal_to_proto(&p);
        Ok(Response::new(crate::pb::DryRunResponse {
            id: p.id,
            summary: proto.summary,
            estimated_bytes_moved: 0, // 43e
        }))
    }

    async fn get_proposal(
        &self,
        req: Request<crate::pb::GetProposalRequest>,
    ) -> Result<Response<crate::pb::Proposal>, Status> {
        let id = req.into_inner().id;
        let p = self
            .state
            .store
            .get(&id)
            .ok_or_else(|| Status::new(Code::NotFound, format!("proposal `{id}` not found")))?;
        Ok(Response::new(handlers::proposal_to_proto(&p)))
    }

    async fn list_proposals(
        &self,
        req: Request<crate::pb::ListProposalsRequest>,
    ) -> Result<Response<crate::pb::ListProposalsResponse>, Status> {
        let limit = req.into_inner().limit;
        let n = if limit <= 0 { 0 } else { limit as usize };
        let proposals = self
            .state
            .store
            .list(n)
            .iter()
            .map(handlers::proposal_to_proto)
            .collect();
        Ok(Response::new(crate::pb::ListProposalsResponse { proposals }))
    }

    async fn execute_proposal(
        &self,
        _req: Request<crate::pb::ExecuteProposalRequest>,
    ) -> Result<Response<crate::pb::ExecuteProposalResponse>, Status> {
        Err(Status::new(
            Code::Unimplemented,
            "execute path lands in slice 43b",
        ))
    }
}

/// Build the axum `Router` exposing the Connect-RPC service. The exact
/// `router` helper name comes from the generated server module.
#[must_use]
pub fn router(svc: RebalancerService) -> axum::Router {
    crate::pb::rebalancer_server::router(svc)
}
```

- [ ] **Step 3: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Append:

```rust
pub mod api;
```

- [ ] **Step 4: Compile + run the api unit tests**

Run: `cargo build -p crabka-rebalancer`

If the build fails because the generated module path or trait name differs from what's assumed in `api/mod.rs`, fix the references and re-run. The Cargo output points at the right location.

Run: `cargo test -p crabka-rebalancer --lib api -- --nocapture`
Expected: 3 tests pass (the `GoalRegistry` tests).

Note: handler-level tests (Unavailable / NotFound / Unimplemented / InvalidArgument) live in the end-to-end test (Task 14) where we have a real `AppState`. Adding handler unit tests now would require either mocking the entire `AppState` or duplicating fixture setup; deferred to the e2e suite.

- [ ] **Step 5: Commit**

```bash
git add crates/rebalancer/src/api crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): Connect-RPC service impl + GoalRegistry

RebalancerService implements the six RPCs; ExecuteProposal returns
Code::Unimplemented; pre-snapshot reads return Code::Unavailable;
unknown proposal ids return Code::NotFound. GoalRegistry maps the
CreateProposalRequest::goals strings to concrete Goal impls.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 12: `health` module — `/healthz`, `/readyz`, `/metrics`

**Files:**
- Create: `crates/rebalancer/src/health.rs`
- Modify: `crates/rebalancer/src/lib.rs`

- [ ] **Step 1: Write the module**

Create `crates/rebalancer/src/health.rs`:

```rust
//! Plain axum routes for `/healthz`, `/readyz`, `/metrics`. Mounted
//! alongside the Connect-RPC router by the binary entry.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus_client::registry::Registry;
use tokio::sync::Mutex;

use crate::ingest::SharedSnapshot;

#[derive(Clone)]
pub struct HealthState {
    pub snapshot: SharedSnapshot,
    pub registry: Arc<Mutex<Registry>>,
}

#[must_use]
pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(s): State<HealthState>) -> impl IntoResponse {
    let g = s.snapshot.load();
    if (*g).is_some() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "no snapshot yet")
    }
}

async fn metrics(State(s): State<HealthState>) -> impl IntoResponse {
    let mut buf = String::new();
    let r = s.registry.lock().await;
    if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &r) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
    }
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[must_use]
pub fn new_registry() -> Registry {
    Registry::with_prefix("crabka_rebalancer")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn fixture() -> HealthState {
        HealthState {
            snapshot: crate::ingest::new_shared_snapshot(),
            registry: Arc::new(Mutex::new(new_registry())),
        }
    }

    #[tokio::test]
    async fn healthz_ok() {
        let app = router(fixture());
        let resp = app
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_503_before_snapshot() {
        let app = router(fixture());
        let resp = app
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_200_after_snapshot() {
        use crate::model::ClusterState;
        let s = fixture();
        s.snapshot.store(std::sync::Arc::new(Some(ClusterState {
            cluster_id: None,
            snapshot_at_ms: 0,
            brokers: vec![],
            partitions: vec![],
            in_flight_reassignments: vec![],
        })));
        let app = router(s);
        let resp = app
            .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_returns_openmetrics() {
        let app = router(fixture());
        let resp = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(ct.starts_with("application/openmetrics-text"));
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        assert!(s.contains("# EOF"));
    }
}
```

- [ ] **Step 2: Mount the module**

Edit `crates/rebalancer/src/lib.rs`. Append:

```rust
pub mod health;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p crabka-rebalancer --lib health -- --nocapture`
Expected: 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/health.rs crates/rebalancer/src/lib.rs
git commit -m "rebalancer(43a): /healthz /readyz /metrics axum routes

Mirrors the operator + broker health-server pattern. /readyz gates on
the snapshot being non-None; /metrics serves OpenMetrics text from a
prefixed prometheus-client Registry. Four unit tests cover each route.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 8 — Binary entry + tests + docs (sequential: T13 → T14 → T15 → T16)

### Task 13: `bin/rebalancer.rs` — full CLI, spawn Ingester + axum server

**Files:**
- Modify: `crates/rebalancer/src/bin/rebalancer.rs`

- [ ] **Step 1: Write the full binary**

Replace `crates/rebalancer/src/bin/rebalancer.rs` with:

```rust
//! `crabka-rebalancer` — Cruise-Control-equivalent partition
//! rebalancer for Crabka clusters. Slice 43a: advisor surface only —
//! propose / dry-run / list / get. Execute lands in slice 43b.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crabka_rebalancer::api::{GoalRegistry, RebalancerService};
use crabka_rebalancer::api::handlers::AppState;
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::health::{HealthState, new_registry};
use crabka_rebalancer::ingest::{Ingester, new_shared_snapshot};
use crabka_rebalancer::model::ProposalStore;

#[derive(Debug, Parser)]
#[command(
    name = "crabka-rebalancer",
    version,
    about = "Cruise-Control-equivalent partition rebalancer (advisor, slice 43a)"
)]
struct Args {
    /// `host:port,host:port,...` of brokers to use for bootstrap.
    #[arg(long, env = "CRABKA_BOOTSTRAP_SERVERS")]
    bootstrap_servers: String,

    /// Bind address for the Connect-RPC + operational HTTP server.
    #[arg(long, env = "CRABKA_REBALANCER_LISTEN_ADDR", default_value = "0.0.0.0:9300")]
    listen_addr: SocketAddr,

    /// Cluster-state snapshot cadence.
    #[arg(long, env = "CRABKA_SCRAPE_INTERVAL_SECS", default_value_t = 10)]
    scrape_interval_secs: u64,

    /// `(max - min) * 100 / total` must exceed this for soft goals to act.
    #[arg(long, env = "CRABKA_IMBALANCE_THRESHOLD_PCT", default_value_t = 10)]
    imbalance_threshold_pct: u32,

    /// Safety cap on the total number of movements per proposal.
    #[arg(long, env = "CRABKA_MAX_MOVEMENTS_PER_PROPOSAL", default_value_t = 256)]
    max_movements_per_proposal: usize,

    /// In-memory ring buffer capacity for recent proposals.
    #[arg(long, env = "CRABKA_PROPOSAL_RING_BUFFER_SIZE", default_value_t = 20)]
    proposal_ring_buffer_size: usize,
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
    info!(listen = %args.listen_addr, bootstrap = %args.bootstrap_servers, "crabka-rebalancer starting");

    // Admin client.
    let client = crabka_client_core::Client::builder()
        .bootstrap(args.bootstrap_servers.clone())
        .client_id("crabka-rebalancer")
        .build()
        .await?;

    // Shared snapshot state.
    let snapshot = new_shared_snapshot();

    // Ingester.
    let shutdown = CancellationToken::new();
    let ingester = Ingester::new(
        client.clone(),
        Duration::from_secs(args.scrape_interval_secs),
        snapshot.clone(),
        shutdown.clone(),
    );
    tokio::spawn(ingester.run());

    // Service state.
    let registry = Arc::new(Mutex::new(new_registry()));
    let store = Arc::new(ProposalStore::new(args.proposal_ring_buffer_size));
    let app_state = Arc::new(AppState {
        snapshot: snapshot.clone(),
        store,
        goal_registry: GoalRegistry::default_registry(),
        goal_ctx: GoalContext {
            imbalance_threshold_pct: args.imbalance_threshold_pct,
            max_movements_per_proposal: args.max_movements_per_proposal,
        },
    });
    let svc = RebalancerService { state: app_state };
    let connect_router = crabka_rebalancer::api::router(svc);

    let health_router = crabka_rebalancer::health::router(HealthState {
        snapshot: snapshot.clone(),
        registry,
    });

    // Merge Connect + health onto one axum app.
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
    Ok(())
}
```

- [ ] **Step 2: Verify the binary builds**

Run: `cargo build -p crabka-rebalancer`
Expected: success.

- [ ] **Step 3: Verify CLI help works**

Run: `target/debug/crabka-rebalancer --help`
Expected: clap output enumerating the six flags with the documented defaults.

- [ ] **Step 4: Commit**

```bash
git add crates/rebalancer/src/bin/rebalancer.rs
git commit -m "rebalancer(43a): full CLI + axum server wiring

Spawns the Ingester, builds the AppState, mounts the Connect-RPC
service router + the /healthz /readyz /metrics router on one axum
app. Graceful shutdown on SIGINT cancels the ingester first.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 14: End-to-end integration test

**Files:**
- Create: `crates/rebalancer/tests/end_to_end.rs`

- [ ] **Step 1: Write the test**

Create `crates/rebalancer/tests/end_to_end.rs`:

```rust
//! Slice 43a end-to-end: spin up a single-broker Crabka in-process,
//! run an Ingester against it, drive the Connect-RPC service via its
//! generated trait, and assert the propose/get/list paths plus the
//! Unavailable / Unimplemented / NotFound / InvalidArgument error
//! codes.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use connectrpc_axum::{Code, Request};
use crabka_broker::{Broker, BrokerConfig};
use crabka_rebalancer::api::handlers::AppState;
use crabka_rebalancer::api::{GoalRegistry, RebalancerService};
use crabka_rebalancer::goals::GoalContext;
use crabka_rebalancer::ingest::{new_shared_snapshot, snapshot_once};
use crabka_rebalancer::model::ProposalStore;
use crabka_rebalancer::pb;
use crabka_rebalancer::pb::rebalancer_server::Rebalancer;

async fn boot_broker() -> (crabka_broker::BrokerHandle, std::net::SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.unwrap();
    let addr = handle.listen_addr();
    std::mem::forget(dir);
    (handle, addr)
}

async fn create_topic(client: &crabka_client_core::Client, name: &str, partitions: i32) {
    use crabka_protocol::owned::create_topics_request::{
        CreatableTopic, CreateTopicsRequest,
    };
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    client.send(req).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_proposal_on_balanced_cluster_returns_empty_movements() {
    let (handle, broker_addr) = boot_broker().await;

    let client = crabka_client_core::Client::builder()
        .bootstrap(broker_addr.to_string())
        .client_id("crabka-rebalancer-test")
        .build()
        .await
        .unwrap();

    // Create 3 topics × 4 partitions × RF=1 = 12 replicas, all on broker 1.
    for t in ["t0", "t1", "t2"] {
        create_topic(&client, t, 4).await;
    }

    // Take a single snapshot rather than spawning the Ingester.
    let snapshot = new_shared_snapshot();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match snapshot_once(&client).await {
            Ok(s) if s.partitions.len() == 12 => {
                snapshot.store(Arc::new(Some(s)));
                break;
            }
            Ok(_) => {} // partitions not yet created
            Err(e) => panic!("snapshot failed: {e}"),
        }
        assert!(Instant::now() < deadline, "topics not visible within 10s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Build a service against the snapshot.
    let svc = RebalancerService {
        state: Arc::new(AppState {
            snapshot: snapshot.clone(),
            store: Arc::new(ProposalStore::new(20)),
            goal_registry: GoalRegistry::default_registry(),
            goal_ctx: GoalContext {
                imbalance_threshold_pct: 10,
                max_movements_per_proposal: 256,
            },
        }),
    };

    // GetState → ok, 12 partitions.
    let state_resp = svc
        .get_state(Request::new(pb::GetStateRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        state_resp.topics.iter().map(|t| t.partitions.len()).sum::<usize>(),
        12
    );

    // CreateProposal (all goals) → 0 movements, all 3 goals applied.
    let proposal = svc
        .create_proposal(Request::new(pb::CreateProposalRequest { goals: vec![] }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(proposal.goals_applied.len(), 3);
    assert_eq!(proposal.movements.len(), 0);
    let summary = proposal.summary.expect("summary present");
    assert_eq!(summary.max_replicas_before, 12);
    assert_eq!(summary.max_replicas_after, 12);

    // CreateProposal (one named goal) → goals_applied has length 1.
    let one_goal = svc
        .create_proposal(Request::new(pb::CreateProposalRequest {
            goals: vec!["ReplicaDistribution".into()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(one_goal.goals_applied, vec!["ReplicaDistribution".to_string()]);

    // CreateProposal (unknown goal) → InvalidArgument.
    let err = svc
        .create_proposal(Request::new(pb::CreateProposalRequest {
            goals: vec!["GhostGoal".into()],
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    // GetProposal unknown id → NotFound.
    let err = svc
        .get_proposal(Request::new(pb::GetProposalRequest {
            id: "no-such-id".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // DryRunProposal on a real id → ok, estimated_bytes_moved = 0.
    let dry = svc
        .dry_run_proposal(Request::new(pb::DryRunProposalRequest {
            id: proposal.id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dry.estimated_bytes_moved, 0);

    // ListProposals → at least 2 (the two we created above).
    let list = svc
        .list_proposals(Request::new(pb::ListProposalsRequest { limit: 0 }))
        .await
        .unwrap()
        .into_inner();
    assert!(list.proposals.len() >= 2);

    // ExecuteProposal → Unimplemented.
    let err = svc
        .execute_proposal(Request::new(pb::ExecuteProposalRequest {
            id: proposal.id,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_state_returns_unavailable_before_first_snapshot() {
    let svc = RebalancerService {
        state: Arc::new(AppState {
            snapshot: new_shared_snapshot(),
            store: Arc::new(ProposalStore::new(20)),
            goal_registry: GoalRegistry::default_registry(),
            goal_ctx: GoalContext {
                imbalance_threshold_pct: 10,
                max_movements_per_proposal: 256,
            },
        }),
    };
    let err = svc
        .get_state(Request::new(pb::GetStateRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p crabka-rebalancer --test end_to_end -- --nocapture`
Expected: 2 tests pass.

- [ ] **Step 3: Commit**

```bash
git add crates/rebalancer/tests/end_to_end.rs
git commit -m "rebalancer(43a): end-to-end integration test

Single-broker Crabka, 3×4 RF=1 topics. Exercises every RPC method
plus the Unavailable / NotFound / Unimplemented / InvalidArgument
error paths.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 15: Connect protocol smoke test over real HTTP

**Files:**
- Create: `crates/rebalancer/tests/connect_smoke.rs`

- [ ] **Step 1: Write the test**

Create `crates/rebalancer/tests/connect_smoke.rs`:

```rust
//! Slice 43a Connect protocol smoke test. Builds the binary, runs it
//! against a temporary single-broker Crabka, hits the Connect endpoint
//! over HTTP+JSON, asserts a sane response. Proves the axum mount +
//! Connect-axum glue work end-to-end.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_get_state_over_http_json() {
    // 1. Boot a broker.
    let dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    let broker = Broker::start(cfg).await.unwrap();
    let broker_addr = broker.listen_addr();

    // 2. Pick an ephemeral local port for the rebalancer.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let rebal_port = listener.local_addr().unwrap().port();
    drop(listener);
    let rebal_addr = format!("127.0.0.1:{rebal_port}");

    // 3. Spawn the binary.
    let bin_path = env!("CARGO_BIN_EXE_crabka-rebalancer");
    let mut child = tokio::process::Command::new(bin_path)
        .arg("--bootstrap-servers").arg(broker_addr.to_string())
        .arg("--listen-addr").arg(&rebal_addr)
        .arg("--scrape-interval-secs").arg("1")
        .env("RUST_LOG", "crabka_rebalancer=info,warn")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn crabka-rebalancer");

    // 4. Wait for /readyz to become 200.
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

    // 5. POST {} as JSON to the Connect endpoint for GetState.
    let resp = client
        .post(format!(
            "http://{rebal_addr}/crabka.rebalancer.v1.Rebalancer/GetState"
        ))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("Connect POST");
    assert!(resp.status().is_success(), "got {}: {}", resp.status(), resp.text().await.unwrap_or_default());
    let body: serde_json::Value = resp.json().await.expect("JSON body");

    // 6. Sanity: response shape matches the proto.
    assert!(body.is_object());
    assert!(body.get("snapshotAtMs").is_some() || body.get("snapshot_at_ms").is_some(),
            "missing snapshotAtMs / snapshot_at_ms: {body}");

    let _ = child.kill().await;
    broker.shutdown().await;
    std::mem::forget(dir);
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p crabka-rebalancer --test connect_smoke -- --nocapture`
Expected: 1 test passes.

- [ ] **Step 3: Commit**

```bash
git add crates/rebalancer/tests/connect_smoke.rs
git commit -m "rebalancer(43a): Connect-RPC HTTP smoke test

Spawns the binary, waits for /readyz, POSTs JSON to GetState, asserts
the response shape. Proves the axum mount + connectrpc-axum glue work
end-to-end.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

### Task 16: README + STATUS docs

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Add the README feature-matrix row**

Open `README.md`. Find the `### Replication & durability` table (around line 105–120). The existing rows end with `| KIP-841 force-elect / unclean recovery toggle | ❌ |` or similar. Add this row at the end of that section, after the last replication-related row:

```markdown
| Cruise-Control-equivalent rebalancer (advisor) | ✅ |
| Cruise-Control-equivalent rebalancer (executor) | ❌ |
```

- [ ] **Step 2: Add the STATUS.md entry**

Edit `STATUS.md`. The file appends new slice sections at the top. Find the most recent `## Slice ...` heading (currently slice 67) and add a new section directly above it:

```markdown
## Slice 43a — Rebalancer foundation (2026-05-17)

- New workspace member `crates/rebalancer/` producing the
  `crabka-rebalancer` binary. Connects to a Crabka cluster as a
  regular admin client (`crabka_client_core::Client`), snapshots
  state every 10s via `Metadata` + `DescribeCluster` +
  `ListPartitionReassignments`, and exposes a Connect-RPC service
  on `:9300` for "what would balance this?" proposals.
- Connect-RPC service shape via `connectrpc-axum` 0.1 + prost. Six
  RPCs (`GetState`, `CreateProposal`, `DryRunProposal`,
  `GetProposal`, `ListProposals`, stub `ExecuteProposal`). Slice
  43a's `ExecuteProposal` returns `Code::Unimplemented` — execute
  lands in slice 43b. Clients can use JSON or protobuf per request
  (Connect content negotiation).
- Three goals: `PreferredLeaderIdempotency` (hard),
  `ReplicaDistribution` (soft), `LeaderDistribution` (soft). Pure
  trait-based plumbing — slices 43c–43g add rack-aware, capacity,
  usage, and anomaly goals against the same surface.
- Optimizer: hard-goals-first ordering, last-writer-wins coalesce on
  duplicate `(topic, partition)` keys, `OptimizeError::HardGoalUnsatisfied`
  when the cap drops a hard movement, deterministic post-coalesce
  movement order.
- In-memory `ProposalStore` (UUID-keyed VecDeque ring buffer,
  default capacity 20). No on-disk persistence in 43a — slice 43b
  adds it alongside the executor.
- Operational endpoints (`/healthz`, `/readyz`, `/metrics`) on the
  same axum listener. `/readyz` gates on the first successful
  snapshot. `/metrics` serves OpenMetrics text from a
  `crabka_rebalancer`-prefixed registry; metrics surface starts
  empty in 43a — populated as later slices ship counters and gauges.
- New workspace deps: `connectrpc-axum`, `connectrpc-axum-build`,
  `prost`. New dev-dep on `reqwest` for the Connect HTTP smoke test.
- 27 new unit tests across `model`, `goals/*`, `optimizer`,
  `ingest`, `api`, `health`. 2 in-process integration tests in
  `tests/end_to_end.rs` (balanced cluster proposal + pre-snapshot
  `Unavailable`). 1 binary-level Connect-protocol smoke test in
  `tests/connect_smoke.rs` (HTTP+JSON `GetState` round-trip).
- Reference doc:
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-43a-design.md`].
  Roadmap (slices 43a–43g + operator slice 44) in
  [`docs/superpowers/specs/2026-05-17-crabka-rebalancer-roadmap-design.md`].
- Out of scope (deferred): execute path (slice 43b), persistence
  (slice 43b), metric scraping for usage goals (slice 43e),
  rack-aware / capacity / usage / CPU / anomaly goals (slices
  43c–43g), operator `KafkaRebalance` CRD (slice 44).
```

- [ ] **Step 3: Run final verification**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-rebalancer
```

All three must pass clean.

- [ ] **Step 4: Commit**

```bash
git add README.md STATUS.md
git commit -m "rebalancer(43a): README + STATUS

Cruise-Control-equivalent rebalancer (advisor) row added to README's
Replication & durability section. Slice 43a entry added to STATUS
documenting the shipped surface and the slice 43b–43g follow-ups.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Self-review checklist

- **Spec coverage:** Every spec section has at least one task:
  - Architecture / crate layout → T1
  - Connect-RPC proto + codegen → T2
  - Optimizer + Goal trait → T3, T4, T9
  - The three slice-43a goals → T6, T7, T8
  - Cluster-state ingest → T10
  - ClusterState data model → T3
  - Unit tests → T3–T10 (one batch per module)
  - Integration test → T14
  - Connect protocol smoke test → T15
  - Acceptance criteria 5 + 6 (README + STATUS) → T16
  - Acceptance criteria 1–4 (build/run/curl/fmt-clippy) → T13 + T16 Step 3
- **No placeholders:** All `TODO`/`later` references in code blocks are intentional comments tied to the slice-43b → 43g follow-ups, not plan gaps.
- **Type consistency:** `Movement`, `Proposal`, `ClusterState`, `GoalContext`, `GoalPriority`, `MovementError`, `OptimizeError`, `ProposalStore`, `Ingester`, `RebalancerService`, `AppState`, `GoalRegistry`, `HealthState` — referenced consistently across all tasks. The plan calls out the one place where naming is generator-dependent (Task 11 Step 1: confirm the codegen module path) and instructs the implementer to adapt.
