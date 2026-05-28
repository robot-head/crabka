# KIP-853 Dynamic Voters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the controller quorum reconfigurable at runtime (add/remove/update voters, auto-join) with the voter set persisted in the metadata log, at full Apache Kafka wire/semantic fidelity.

**Architecture:** Two new serde `MetadataRecord` variants (`V1KRaftVersion`, `V1Voters`) make the voter set log-resident and authoritative; a reconfiguration coordinator on the leader drives openraft's `change_membership` in lockstep with those records. Three new RPCs (AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter, api keys 80/81/82) plus DescribeQuorum v2 expose reconfiguration to Kafka tooling. Dynamic-only: every controller bootstraps at `kraft.version=1` from `controller.quorum.bootstrap.servers`.

**Tech Stack:** Rust, openraft 0.9, `serde_wincode::SerdeCompat` for record framing, tokio, the crabka codegen'd protocol layer.

**Design spec:** `docs/superpowers/specs/2026-05-28-crabka-kip853-dynamic-voters-design.md`

---

## Conventions for every task

- Commit with identity overrides (local git identity is unset; never run `git config`):
  ```
  git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "..."
  ```
- Workspace build/test: `cargo build --workspace` / `cargo test --workspace`.
- Per-crate test: `cargo test -p <crate> --test <file>`.
- `Uuid` in `crates/metadata` and `crates/raft` is the `uuid` crate's `Uuid`. The protocol layer uses `crabka_protocol::primitives::uuid::Uuid`; convert at the handler boundary via `Uuid::from_bytes` / `.into_bytes()`.

## Batch / parallelism map

Tasks within a batch touch disjoint file sets and may be dispatched concurrently. Batches run in order (later batches depend on earlier ones compiling).

- **Batch 1:** Task 1 (metadata data model). Sequential foundation.
- **Batch 2:** Task 2 (raft `Node` + config refactor). Touches the whole raft crate — runs alone.
- **Batch 3 (parallel):** Task 3 (reconfiguration coordinator, raft crate) ‖ Task 4 (format + bootstrap, cli + broker/bootstrap.rs). Disjoint files.
- **Batch 4:** Task 5 (wire surface: handlers + dispatch + DescribeQuorum v2 + ApiVersions). Single task — all arms share `dispatch.rs`.
- **Batch 5:** Task 6 (auto-join, broker startup).
- **Batch 6:** Task 7 (integration tests).

---

## Task 1: Metadata data model — voter value types, records, image tracking

**Files:**
- Create: `crates/metadata/src/voters.rs`
- Modify: `crates/metadata/src/lib.rs` (add `pub mod voters;` + re-exports)
- Modify: `crates/metadata/src/records.rs` (new record structs + enum variants)
- Modify: `crates/metadata/src/image.rs` (new fields, apply/validate, accessors)
- Test: inline `#[cfg(test)]` in `voters.rs` and `records.rs`; image test in `image.rs`

- [ ] **Step 1: Write the failing test for VoterSet + record round-trip**

Add to a new file `crates/metadata/src/voters.rs`:

```rust
//! KIP-853 voter set value types: a voter is (id, directory-id, endpoints, kraft.version range).

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::NodeId;

/// A single listener endpoint advertised by a voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterEndpoint {
    pub name: String,
    pub host: String,
    pub port: u16,
}

/// Supported kraft.version range for a voter (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KRaftVersionRange {
    pub min: u16,
    pub max: u16,
}

impl Default for KRaftVersionRange {
    fn default() -> Self {
        Self { min: 0, max: 1 }
    }
}

/// One voter's full identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Voter {
    pub id: NodeId,
    pub directory_id: Uuid,
    pub endpoints: Vec<VoterEndpoint>,
    pub kraft_version: KRaftVersionRange,
}

/// The authoritative voter set (ordered by node id).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoterSet {
    voters: BTreeMap<NodeId, Voter>,
}

impl VoterSet {
    #[must_use]
    pub fn from_voters(voters: impl IntoIterator<Item = Voter>) -> Self {
        Self {
            voters: voters.into_iter().map(|v| (v.id, v)).collect(),
        }
    }

    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains_key(&id)
    }

    #[must_use]
    pub fn get(&self, id: NodeId) -> Option<&Voter> {
        self.voters.get(&id)
    }

    #[must_use]
    pub fn ids(&self) -> std::collections::BTreeSet<NodeId> {
        self.voters.keys().copied().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.voters.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Voter> {
        self.voters.values()
    }

    /// Return a copy with `voter` added or replaced.
    #[must_use]
    pub fn with_voter(&self, voter: Voter) -> Self {
        let mut next = self.clone();
        next.voters.insert(voter.id, voter);
        next
    }

    /// Return a copy with `id` removed.
    #[must_use]
    pub fn without_voter(&self, id: NodeId) -> Self {
        let mut next = self.clone();
        next.voters.remove(&id);
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: NodeId) -> Voter {
        Voter {
            id,
            directory_id: Uuid::from_u128(u128::from(id)),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9093,
            }],
            kraft_version: KRaftVersionRange::default(),
        }
    }

    #[test]
    fn add_remove_are_immutable_copies() {
        let base = VoterSet::from_voters([sample(1)]);
        let added = base.with_voter(sample(2));
        assert!(base.contains(1) && !base.contains(2));
        assert!(added.contains(1) && added.contains(2));
        let removed = added.without_voter(1);
        assert!(!removed.contains(1) && removed.contains(2));
    }

    #[test]
    fn ids_are_sorted() {
        let set = VoterSet::from_voters([sample(3), sample(1), sample(2)]);
        assert_eq!(set.ids().into_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails (module not wired)**

Run: `cargo test -p crabka-metadata voters:: 2>&1 | head -20`
Expected: compile error — `voters` module not declared in `lib.rs`.

- [ ] **Step 3: Wire the module and re-exports**

In `crates/metadata/src/lib.rs`, add alongside the other `pub mod` lines:

```rust
pub mod voters;
pub use voters::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p crabka-metadata voters:: -v`
Expected: PASS (2 tests).

- [ ] **Step 5: Add the record structs and enum variants (failing test first)**

In `crates/metadata/src/records.rs`, add the structs near the other record definitions:

```rust
/// KIP-853: finalizes the cluster-wide kraft.version feature level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KRaftVersionRecord {
    pub kraft_version: u16,
}

/// KIP-853: full snapshot of the controller voter set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VotersRecord {
    pub voters: crate::voters::VoterSet,
}
```

Add these variants to the `MetadataRecord` enum (after `V1UnregisterBroker`):

```rust
    V1KRaftVersion(KRaftVersionRecord),
    V1Voters(VotersRecord),
```

Add a round-trip test in the `records.rs` `#[cfg(test)]` module:

```rust
    #[test]
    fn voters_record_round_trips() {
        let rec = MetadataRecord::V1Voters(VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: 7,
                directory_id: uuid::Uuid::from_u128(7),
                endpoints: vec![crate::voters::VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "h".into(),
                    port: 1,
                }],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        });
        assert_eq!(round_trip(&rec), rec);
    }

    #[test]
    fn kraft_version_record_round_trips() {
        let rec = MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 });
        assert_eq!(round_trip(&rec), rec);
    }
```

- [ ] **Step 6: Run to verify failure then success**

Run: `cargo test -p crabka-metadata records:: 2>&1 | head -30`
Expected: first FAILS to compile because `apply`/`validate` in `image.rs` are non-exhaustive over the new variants (the enum is `#[non_exhaustive]` but internal matches still need arms). Proceed to Step 7 before re-running.

- [ ] **Step 7: Extend MetadataImage**

In `crates/metadata/src/image.rs`, add fields to the struct:

```rust
    kraft_version: u16,
    voters: crate::voters::VoterSet,
```

Add match arms in `apply()`:

```rust
        MetadataRecord::V1KRaftVersion(r) => {
            self.kraft_version = r.kraft_version;
        }
        MetadataRecord::V1Voters(r) => {
            self.voters = r.voters.clone();
        }
```

Add match arms in `validate()` (no precondition beyond feature gate):

```rust
        MetadataRecord::V1KRaftVersion(_) => Ok(()),
        MetadataRecord::V1Voters(_) => Ok(()),
```

Add accessors near the `brokers()` accessor:

```rust
    #[must_use]
    pub fn kraft_version(&self) -> u16 {
        self.kraft_version
    }

    #[must_use]
    pub fn voters(&self) -> &crate::voters::VoterSet {
        &self.voters
    }
```

Add a test in the `image.rs` `#[cfg(test)]` module:

```rust
    #[test]
    fn applies_voters_and_version() {
        let mut image = MetadataImage::default();
        image.apply(&MetadataRecord::V1KRaftVersion(
            crate::records::KRaftVersionRecord { kraft_version: 1 },
        ));
        image.apply(&MetadataRecord::V1Voters(crate::records::VotersRecord {
            voters: crate::voters::VoterSet::from_voters([crate::voters::Voter {
                id: 1,
                directory_id: uuid::Uuid::nil(),
                endpoints: vec![],
                kraft_version: crate::voters::KRaftVersionRange::default(),
            }]),
        }));
        assert_eq!(image.kraft_version(), 1);
        assert!(image.voters().contains(1));
    }
```

- [ ] **Step 8: Run the full metadata crate test suite**

Run: `cargo test -p crabka-metadata 2>&1 | tail -20`
Expected: PASS, no warnings about non-exhaustive matches.

- [ ] **Step 9: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/voters.rs crates/metadata/src/lib.rs crates/metadata/src/records.rs crates/metadata/src/image.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(metadata): VoterSet + VotersRecord/KRaftVersionRecord (KIP-853)"
```

---

## Task 2: Raft `Node` + config refactor

Replaces openraft's `BasicNode` with a custom `Node` carrying directory id + endpoints + version range, and switches `ControllerConfig` from a static voter list to dynamic-only inputs. Touches the whole raft crate — dispatch this task alone.

**Files:**
- Modify: `crates/raft/src/types.rs` (custom `Node`)
- Modify: `crates/raft/src/config.rs` (`bootstrap_servers`, `directory_id`, `auto_join`, `observer_lag_bound`; drop `voters`/`BootstrapMode` static semantics)
- Modify (compiler-guided ripples): `crates/raft/src/{controller.rs,network.rs,log_store.rs,server.rs}` — every `BasicNode`/`Node` construction or `.addr` access
- Test: existing `crates/raft/tests/single_node.rs` must still pass; add a `Node` codec test in `types.rs`

- [ ] **Step 1: Define the custom Node (failing build)**

In `crates/raft/src/types.rs`, replace `pub type Node = openraft::BasicNode;` with:

```rust
/// KIP-853 voter node identity used by openraft membership.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub directory_id: uuid::Uuid,
    pub endpoints: Vec<crabka_metadata::VoterEndpoint>,
    pub kraft_version: crabka_metadata::KRaftVersionRange,
}

impl Node {
    /// The controller RPC endpoint openraft dials. By convention the first
    /// endpoint named "CONTROLLER"; falls back to the first endpoint.
    #[must_use]
    pub fn controller_addr(&self) -> Option<std::net::SocketAddr> {
        self.endpoints
            .iter()
            .find(|e| e.name == "CONTROLLER")
            .or_else(|| self.endpoints.first())
            .and_then(|e| format!("{}:{}", e.host, e.port).parse().ok())
    }
}
```

openraft requires `Node: openraft::Node`. Add the impl (openraft 0.9's `Node` is a marker with `Clone + Default + ... `; `BasicNode` satisfied it). Confirm trait bounds by building; if openraft needs an explicit `impl openraft::Node for Node {}`, add it.

- [ ] **Step 2: Build to enumerate ripple sites**

Run: `cargo build -p crabka-raft 2>&1 | grep -E '^error' | head -40`
Expected: errors at each site constructing `BasicNode { addr }` or reading `node.addr`. These are the sites to fix in Steps 3–4.

- [ ] **Step 3: Fix network dial sites**

In `crates/raft/src/network.rs`, the dialer takes a target address. Replace `node.addr.parse()` style access with `node.controller_addr().ok_or(RaftError::Protocol("voter node has no controller endpoint".into()))?`. Keep the rest of the dial path unchanged.

- [ ] **Step 4: Fix controller/log_store/server construction sites**

Anywhere a `Node`/`BasicNode` is built from a `SocketAddr` (e.g. in `initialize`, `add_learner`), build the new `Node`:

```rust
let node = Node {
    directory_id: config.directory_id,
    endpoints: vec![crabka_metadata::VoterEndpoint {
        name: "CONTROLLER".into(),
        host: addr.ip().to_string(),
        port: addr.port(),
    }],
    kraft_version: crabka_metadata::KRaftVersionRange::default(),
};
```

`config.directory_id` is added in Step 5. For learner add paths where only an addr is known, use a freshly supplied directory id passed by the caller (the coordinator passes it in Task 3).

- [ ] **Step 5: Update ControllerConfig**

In `crates/raft/src/config.rs`:
- Remove `pub voters: Vec<(NodeId, SocketAddr)>`.
- Add:
  ```rust
  /// Endpoints used only to discover the leader at cold start (KIP-853 dynamic).
  pub bootstrap_servers: Vec<SocketAddr>,
  /// This replica's stable directory id (generated at format time).
  pub directory_id: uuid::Uuid,
  /// Issue AddVoter for self once caught up as an observer.
  pub auto_join: bool,
  /// Max allowed lag (in log entries) for an observer to be promotable.
  pub observer_lag_bound: u64,
  /// Initial voter set for the bootstrapping (`--standalone`/`--initial-controllers`)
  /// node only; empty for joiners.
  pub initial_voters: crabka_metadata::VoterSet,
  ```
- Keep `BootstrapMode` but redefine its meaning in doc comments: `Bootstrap` = this node holds the initial VotersRecord and calls `initialize` with `initial_voters`; `Join` = empty start, discover + auto-join; `Rejoin` = replay log.

- [ ] **Step 6: Update the initialize path**

In `controller.rs`, where `BootstrapMode::Bootstrap` currently calls `raft.initialize({(node_id, addr)})`, build the membership from `config.initial_voters`:

```rust
let members: std::collections::BTreeMap<NodeId, Node> = config
    .initial_voters
    .iter()
    .map(|v| {
        (
            v.id,
            Node {
                directory_id: v.directory_id,
                endpoints: v.endpoints.clone(),
                kraft_version: v.kraft_version,
            },
        )
    })
    .collect();
raft.initialize(members).await.map_err(/* existing mapping */)?;
```

- [ ] **Step 7: Update QuorumState to carry directory ids + endpoints**

In `controller.rs`, extend `QuorumState`:

```rust
    pub voter_nodes: BTreeMap<NodeId, Node>,
```

Populate it in `quorum_state()` from openraft's `membership_config().membership().nodes()`.

- [ ] **Step 8: Add a Node round-trip test and build/test**

Add to `types.rs` tests:

```rust
#[cfg(test)]
mod node_tests {
    use super::*;
    #[test]
    fn node_controller_addr_prefers_controller_listener() {
        let n = Node {
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![
                crabka_metadata::VoterEndpoint { name: "PLAINTEXT".into(), host: "127.0.0.1".into(), port: 9092 },
                crabka_metadata::VoterEndpoint { name: "CONTROLLER".into(), host: "127.0.0.1".into(), port: 9093 },
            ],
            kraft_version: Default::default(),
        };
        assert_eq!(n.controller_addr().unwrap().port(), 9093);
    }
}
```

Run: `cargo build -p crabka-raft && cargo test -p crabka-raft 2>&1 | tail -20`
Expected: builds clean; `single_node.rs` and new tests PASS. Fix any remaining call sites the compiler flags.

> Note: callers in the broker crate that construct `ControllerConfig` (e.g. `crates/broker/src/broker.rs`, test support) will now fail to build because `voters` was removed. Those are updated in Tasks 4 and 7. Build only the raft crate here.

- [ ] **Step 9: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/raft/src
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "refactor(raft): custom voter Node + dynamic-only ControllerConfig (KIP-853)"
```

---

## Task 3: Reconfiguration coordinator (raft crate) — Batch 3, parallel with Task 4

Owns voter-set mutations and KIP-853 safety. Drives openraft `change_membership` and emits the matching `V1Voters` record in lockstep.

**Files:**
- Create: `crates/raft/src/reconfig.rs`
- Modify: `crates/raft/src/controller.rs` (expose coordinator methods on `ControllerHandle`; observer-offset tracking)
- Modify: `crates/raft/src/lib.rs` (`mod reconfig;` + re-export the request/error types)
- Modify: `crates/raft/src/error.rs` (add reconfig error variants)
- Test: `crates/raft/tests/reconfig.rs` (NEW) + inline unit tests in `reconfig.rs`

- [ ] **Step 1: Define reconfig request/result types + errors (failing build)**

Create `crates/raft/src/reconfig.rs`:

```rust
//! KIP-853 reconfiguration coordinator: single-voter add/remove/update with safety guards.

use crabka_metadata::{Voter, VoterSet};

use crate::{NodeId, RaftError};

/// A request to add one voter. The candidate must already be a caught-up observer.
#[derive(Debug, Clone)]
pub struct AddVoter {
    pub voter: Voter,
}

/// A request to remove one voter.
#[derive(Debug, Clone)]
pub struct RemoveVoter {
    pub id: NodeId,
    pub directory_id: uuid::Uuid,
}

/// A request to update one voter's endpoints / supported version range.
#[derive(Debug, Clone)]
pub struct UpdateVoter {
    pub voter: Voter,
}

/// Outcome shared by all three operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconfigOutcome {
    Committed,
    NotLeader { leader: Option<NodeId> },
}
```

Add to `crates/raft/src/error.rs` `RaftError`:

```rust
    /// A reconfiguration violated a KIP-853 safety rule.
    ReconfigRejected(String),
    /// Another reconfiguration is already in progress.
    ReconfigInProgress,
    /// AddVoter candidate is not a caught-up observer yet.
    VoterNotCaughtUp { id: NodeId, lag: u64 },
```

- [ ] **Step 2: Write the coordinator logic**

Append to `reconfig.rs` a `Coordinator` that operates on a `ControllerHandle`-like surface. To avoid a cyclic dependency, define a small trait the controller implements:

```rust
/// The raft operations the coordinator needs. Implemented by ControllerHandle.
#[async_trait::async_trait]
pub trait ReconfigOps: Send + Sync {
    fn current_voters(&self) -> VoterSet;
    fn leader(&self) -> Option<NodeId>;
    fn is_leader(&self) -> bool;
    /// Highest log index the leader has; used for observer-lag checks.
    fn leader_last_index(&self) -> u64;
    /// Last replicated index for an observer/learner, if known.
    fn observer_index(&self, id: NodeId) -> Option<u64>;
    async fn add_learner(&self, id: NodeId, node: crate::Node) -> Result<(), RaftError>;
    async fn change_membership(&self, ids: std::collections::BTreeSet<NodeId>) -> Result<(), RaftError>;
    async fn submit_records(&self, records: Vec<crabka_metadata::MetadataRecord>) -> Result<(), RaftError>;
}

pub struct Coordinator<'a, O: ReconfigOps> {
    ops: &'a O,
    /// Guards single-change-at-a-time.
    lock: &'a tokio::sync::Mutex<()>,
    observer_lag_bound: u64,
}

impl<'a, O: ReconfigOps> Coordinator<'a, O> {
    pub fn new(ops: &'a O, lock: &'a tokio::sync::Mutex<()>, observer_lag_bound: u64) -> Self {
        Self { ops, lock, observer_lag_bound }
    }

    pub async fn add_voter(&self, req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        if !self.ops.is_leader() {
            return Ok(ReconfigOutcome::NotLeader { leader: self.ops.leader() });
        }
        let _guard = self
            .lock
            .try_lock()
            .map_err(|_| RaftError::ReconfigInProgress)?;

        let current = self.ops.current_voters();
        if current.contains(req.voter.id) {
            return Ok(ReconfigOutcome::Committed); // idempotent
        }
        // Observer must be caught up.
        let node = crate::Node {
            directory_id: req.voter.directory_id,
            endpoints: req.voter.endpoints.clone(),
            kraft_version: req.voter.kraft_version,
        };
        self.ops.add_learner(req.voter.id, node).await?;
        let lag = self
            .ops
            .leader_last_index()
            .saturating_sub(self.ops.observer_index(req.voter.id).unwrap_or(0));
        if lag > self.observer_lag_bound {
            return Err(RaftError::VoterNotCaughtUp { id: req.voter.id, lag });
        }
        // Lockstep: openraft membership first, then the authoritative VotersRecord.
        let next = current.with_voter(req.voter.clone());
        self.ops.change_membership(next.ids()).await?;
        self.ops
            .submit_records(vec![crabka_metadata::MetadataRecord::V1Voters(
                crabka_metadata::records::VotersRecord { voters: next },
            )])
            .await?;
        Ok(ReconfigOutcome::Committed)
    }

    pub async fn remove_voter(&self, req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        if !self.ops.is_leader() {
            return Ok(ReconfigOutcome::NotLeader { leader: self.ops.leader() });
        }
        let _guard = self.lock.try_lock().map_err(|_| RaftError::ReconfigInProgress)?;
        let current = self.ops.current_voters();
        if !current.contains(req.id) {
            return Ok(ReconfigOutcome::Committed); // idempotent
        }
        let next = current.without_voter(req.id);
        // Never let the surviving set be empty.
        if next.is_empty() {
            return Err(RaftError::ReconfigRejected(
                "cannot remove the last voter".into(),
            ));
        }
        self.ops.change_membership(next.ids()).await?;
        self.ops
            .submit_records(vec![crabka_metadata::MetadataRecord::V1Voters(
                crabka_metadata::records::VotersRecord { voters: next },
            )])
            .await?;
        Ok(ReconfigOutcome::Committed)
    }

    pub async fn update_voter(&self, req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        if !self.ops.is_leader() {
            return Ok(ReconfigOutcome::NotLeader { leader: self.ops.leader() });
        }
        let _guard = self.lock.try_lock().map_err(|_| RaftError::ReconfigInProgress)?;
        let current = self.ops.current_voters();
        if !current.contains(req.voter.id) {
            return Err(RaftError::ReconfigRejected("unknown voter".into()));
        }
        let next = current.with_voter(req.voter);
        // No membership change — only the VotersRecord is rewritten.
        self.ops
            .submit_records(vec![crabka_metadata::MetadataRecord::V1Voters(
                crabka_metadata::records::VotersRecord { voters: next },
            )])
            .await?;
        Ok(ReconfigOutcome::Committed)
    }
}
```

> If `crabka_metadata::records` is private, re-export `VotersRecord` from `crabka_metadata` (Task 1 already re-exports value types; add `pub use records::{KRaftVersionRecord, VotersRecord};` to `crates/metadata/src/lib.rs` and use that path).

- [ ] **Step 3: Implement `ReconfigOps` for ControllerHandle + expose methods**

In `controller.rs`:
- Add a `reconfig_lock: tokio::sync::Mutex<()>` to the handle's shared state (inside the existing `Arc<…>`).
- Track observer indices: openraft's metrics already expose replication match indices on the leader (the existing `per_voter_matched_index` plumbing). Extend it to also surface learner indices into a map readable by `observer_index`.
- Implement `ReconfigOps` for `ControllerHandle` mapping to existing `add_learner`, `change_membership`, `submit_change` (rename use as `submit_records`), `quorum_state()`.
- Add public async methods on `ControllerHandle`: `add_voter`, `remove_voter`, `update_voter`, each constructing a `Coordinator` and delegating.

- [ ] **Step 4: Unit-test the safety guards with a mock ReconfigOps**

Create `crates/raft/tests/reconfig.rs`:

```rust
// Tests the coordinator's safety guards against an in-memory mock (no real raft).
use crabka_metadata::{KRaftVersionRange, Voter, VoterEndpoint, VoterSet};
use crabka_raft::reconfig::{AddVoter, Coordinator, ReconfigOps, ReconfigOutcome, RemoveVoter};
use crabka_raft::{Node, NodeId, RaftError};
use std::collections::BTreeSet;
use std::sync::Mutex as StdMutex;

#[derive(Default)]
struct MockState {
    voters: VoterSet,
    leader_index: u64,
    observer_index: std::collections::HashMap<NodeId, u64>,
    is_leader: bool,
    submitted: Vec<crabka_metadata::MetadataRecord>,
    membership: Option<BTreeSet<NodeId>>,
}

struct Mock(StdMutex<MockState>);

#[async_trait::async_trait]
impl ReconfigOps for Mock {
    fn current_voters(&self) -> VoterSet { self.0.lock().unwrap().voters.clone() }
    fn leader(&self) -> Option<NodeId> { Some(1) }
    fn is_leader(&self) -> bool { self.0.lock().unwrap().is_leader }
    fn leader_last_index(&self) -> u64 { self.0.lock().unwrap().leader_index }
    fn observer_index(&self, id: NodeId) -> Option<u64> { self.0.lock().unwrap().observer_index.get(&id).copied() }
    async fn add_learner(&self, id: NodeId, _node: Node) -> Result<(), RaftError> {
        self.0.lock().unwrap().observer_index.entry(id).or_insert(0);
        Ok(())
    }
    async fn change_membership(&self, ids: BTreeSet<NodeId>) -> Result<(), RaftError> {
        self.0.lock().unwrap().membership = Some(ids);
        Ok(())
    }
    async fn submit_records(&self, records: Vec<crabka_metadata::MetadataRecord>) -> Result<(), RaftError> {
        self.0.lock().unwrap().submitted.extend(records);
        Ok(())
    }
}

fn voter(id: NodeId) -> Voter {
    Voter {
        id,
        directory_id: uuid::Uuid::from_u128(u128::from(id)),
        endpoints: vec![VoterEndpoint { name: "CONTROLLER".into(), host: "127.0.0.1".into(), port: 9093 }],
        kraft_version: KRaftVersionRange::default(),
    }
}

#[tokio::test]
async fn add_voter_rejects_lagging_observer() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        leader_index: 1000,
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let err = coord.add_voter(AddVoter { voter: voter(2) }).await.unwrap_err();
    assert!(matches!(err, RaftError::VoterNotCaughtUp { id: 2, .. }));
}

#[tokio::test]
async fn remove_last_voter_is_rejected() {
    let mock = Mock(StdMutex::new(MockState {
        voters: VoterSet::from_voters([voter(1)]),
        is_leader: true,
        ..Default::default()
    }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let err = coord
        .remove_voter(RemoveVoter { id: 1, directory_id: uuid::Uuid::from_u128(1) })
        .await
        .unwrap_err();
    assert!(matches!(err, RaftError::ReconfigRejected(_)));
}

#[tokio::test]
async fn add_voter_on_follower_reports_not_leader() {
    let mock = Mock(StdMutex::new(MockState { is_leader: false, ..Default::default() }));
    let lock = tokio::sync::Mutex::new(());
    let coord = Coordinator::new(&mock, &lock, 10);
    let out = coord.add_voter(AddVoter { voter: voter(2) }).await.unwrap();
    assert!(matches!(out, ReconfigOutcome::NotLeader { .. }));
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p crabka-raft --test reconfig -v`
Expected: 3 tests PASS (after `mod reconfig;` + re-exports are wired and `async_trait` is a dependency — add `async-trait` to `crates/raft/Cargo.toml` if absent).

- [ ] **Step 6: Build the raft crate**

Run: `cargo build -p crabka-raft 2>&1 | tail -10`
Expected: clean build.

- [ ] **Step 7: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/raft crates/metadata/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(raft): KIP-853 reconfiguration coordinator with safety guards"
```

---

## Task 4: Format + bootstrap (cli + broker bootstrap) — Batch 3, parallel with Task 3

Generates the replica directory id, persists it, and writes `KRaftVersionRecord` + initial `VotersRecord` into the bootstrap records. Adds `--standalone` / `--initial-controllers`. Updates broker startup to feed initial voters into `ControllerConfig` (Task 2 removed the static `voters`).

**Files:**
- Modify: `crates/cli/src/format.rs` (args, directory id, meta.properties, bootstrap records)
- Modify: `crates/broker/src/bootstrap.rs` (read meta.properties / directory id; derive initial `VoterSet` from bootstrap records)
- Modify: `crates/broker/src/broker.rs` (build `ControllerConfig` from new fields)
- Modify: `crates/broker/src/config.rs` (BrokerConfig: `bootstrap_servers`, `directory_id`, `auto_join`)
- Test: inline tests in `format.rs`; a bootstrap-roundtrip test in `bootstrap.rs`

- [ ] **Step 1: Add CLI args + a failing test**

In `crates/cli/src/format.rs` `FormatArgs`, add:

```rust
    /// Format this node as the sole initial controller voter.
    #[arg(long, conflicts_with = "initial_controllers")]
    standalone: bool,
    /// Explicit initial controllers: id@host:port:dir-uuid, comma-separated.
    #[arg(long, value_delimiter = ',')]
    initial_controllers: Vec<String>,
    /// This node's controller listener (host:port) — written into the VotersRecord
    /// when --standalone.
    #[arg(long)]
    controller_listener: Option<String>,
```

Add a parse-helper test in `format.rs` `#[cfg(test)]`:

```rust
    #[test]
    fn parses_initial_controller_spec() {
        let v = parse_initial_controller("3@host:9093:00000000-0000-0000-0000-000000000003").unwrap();
        assert_eq!(v.id, 3);
        assert_eq!(v.endpoints[0].port, 9093);
        assert_eq!(v.directory_id, uuid::Uuid::from_u128(3));
    }
```

- [ ] **Step 2: Implement the spec parser**

Add to `format.rs`:

```rust
fn parse_initial_controller(spec: &str) -> Result<crabka_metadata::Voter, String> {
    // id@host:port:dir-uuid
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id: crabka_raft::NodeId = id_part.parse().map_err(|_| "bad id")?;
    let parts: Vec<&str> = rest.rsplitn(2, ':').collect();
    // parts[0] = dir-uuid, parts[1] = host:port
    let dir: uuid::Uuid = parts[0].parse().map_err(|_| "bad directory uuid")?;
    let (host, port) = parts[1].rsplit_once(':').ok_or("missing host:port")?;
    let port: u16 = port.parse().map_err(|_| "bad port")?;
    Ok(crabka_metadata::Voter {
        id,
        directory_id: dir,
        endpoints: vec![crabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.to_string(),
            port,
        }],
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    })
}
```

- [ ] **Step 3: Generate + persist directory id (meta.properties)**

In `format.rs` `run()`, generate a directory id and write a `meta.properties`-equivalent next to the log dir:

```rust
let directory_id = uuid::Uuid::new_v4();
let meta = serde_json::json!({
    "cluster_id": cluster_id.to_string(),
    "directory_id": directory_id.to_string(),
    "version": 1,
});
std::fs::write(args.log_dir.join("meta.properties.json"), serde_json::to_vec_pretty(&meta)?)?;
```

- [ ] **Step 4: Write KRaftVersionRecord + VotersRecord into bootstrap records**

In `write_bootstrap_files()` (or its caller), prepend two records to the bootstrap record vector:

```rust
let mut records: Vec<MetadataRecord> = Vec::new();
records.push(MetadataRecord::V1KRaftVersion(
    crabka_metadata::KRaftVersionRecord { kraft_version: 1 },
));
let initial = if args.standalone {
    let listener = args.controller_listener.as_deref().ok_or("--standalone requires --controller-listener")?;
    let (host, port) = listener.rsplit_once(':').ok_or("bad --controller-listener")?;
    crabka_metadata::VoterSet::from_voters([crabka_metadata::Voter {
        id: /* this node's id, from existing format args/config */,
        directory_id,
        endpoints: vec![crabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.to_string(),
            port: port.parse().map_err(|_| "bad port")?,
        }],
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    }])
} else if !args.initial_controllers.is_empty() {
    let voters: Result<Vec<_>, _> = args.initial_controllers.iter().map(|s| parse_initial_controller(s)).collect();
    crabka_metadata::VoterSet::from_voters(voters?)
} else {
    crabka_metadata::VoterSet::default() // joiner: empty, relies on auto-join
};
if !initial.is_empty() {
    records.push(MetadataRecord::V1Voters(crabka_metadata::VotersRecord { voters: initial }));
}
// ... then existing SCRAM/ACL bootstrap records are appended as before
```

> If `format` does not currently know "this node's id", add a `--node-id` arg (KIP-853 `--standalone` needs it). Match the existing pattern for required args.

- [ ] **Step 5: Bootstrap read side — derive initial VoterSet + directory id**

In `crates/broker/src/bootstrap.rs`, add:

```rust
pub fn read_directory_id(log_dir: &Path) -> Result<uuid::Uuid, BrokerError> {
    let bytes = std::fs::read(log_dir.join("meta.properties.json"))?;
    let v: serde_json::Value = serde_json::from_slice(&bytes)?;
    v["directory_id"].as_str().and_then(|s| s.parse().ok())
        .ok_or_else(|| BrokerError::Config("missing directory_id in meta.properties.json".into()))
}

/// Extract the initial voter set from the bootstrap records (last V1Voters wins).
pub fn initial_voters(records: &[MetadataRecord]) -> crabka_metadata::VoterSet {
    records.iter().rev().find_map(|r| match r {
        MetadataRecord::V1Voters(v) => Some(v.voters.clone()),
        _ => None,
    }).unwrap_or_default()
}
```

Add a round-trip test in `bootstrap.rs` `#[cfg(test)]` that writes records via the same framing and asserts `initial_voters` returns the seeded set.

- [ ] **Step 6: Wire BrokerConfig + ControllerConfig**

In `crates/broker/src/config.rs` `BrokerConfig`: remove any static `voters` field; add `bootstrap_servers: Vec<SocketAddr>`, `directory_id: uuid::Uuid`, `auto_join: bool`, `observer_lag_bound: u64` (default e.g. 100).

In `crates/broker/src/broker.rs`, where `ControllerConfig` is constructed, set:
```rust
bootstrap_servers: config.bootstrap_servers.clone(),
directory_id: config.directory_id,
auto_join: config.auto_join,
observer_lag_bound: config.observer_lag_bound,
initial_voters: crabka_broker::bootstrap::initial_voters(&bootstrap_records),
```
and pick `BootstrapMode` from whether `initial_voters` contains `node_id` (Bootstrap) vs. empty (Join) vs. existing on-disk raft log (Rejoin).

- [ ] **Step 7: Build + test**

Run: `cargo build -p crabka-cli -p crabka-broker 2>&1 | tail -20 && cargo test -p crabka-cli format:: -v && cargo test -p crabka-broker bootstrap:: -v`
Expected: builds clean; format + bootstrap tests PASS.

- [ ] **Step 8: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/cli/src/format.rs crates/broker/src/bootstrap.rs crates/broker/src/broker.rs crates/broker/src/config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(format): seed kraft.version=1 + initial VotersRecord; dynamic bootstrap (KIP-853)"
```

---

## Task 5: Wire surface — RPC handlers, dispatch, DescribeQuorum v2, ApiVersions

**Files:**
- Create: `crates/broker/src/handlers/add_raft_voter.rs`, `remove_raft_voter.rs`, `update_raft_voter.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (declare modules)
- Modify: `crates/broker/src/handlers/describe_quorum.rs` (v2 fields)
- Modify: `crates/broker/src/network/dispatch.rs` (frame fns + match arms for 80/81/82; DescribeQuorum v2 routing)
- Modify: `crates/broker/src/handlers/api_versions.rs` (advertise 80/81/82 + DescribeQuorum max v2)
- Test: inline handler unit tests + a dispatch smoke test

- [ ] **Step 1: AddRaftVoter handler (failing build)**

Create `crates/broker/src/handlers/add_raft_voter.rs`:

```rust
use bytes::{Bytes, BytesMut};
use crabka_protocol::owned::{add_raft_voter_request::AddRaftVoterRequest, add_raft_voter_response::AddRaftVoterResponse};
use crabka_protocol::{Decodable, Encodable};

use crate::{codes, Broker, BrokerError};

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    _ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = AddRaftVoterRequest::decode(&mut cur, version)?;

    let voter = crabka_metadata::Voter {
        id: u64::try_from(req.voter_id).unwrap_or_default(),
        directory_id: uuid::Uuid::from_bytes(req.voter_directory_id.into_bytes()),
        endpoints: req.listeners.iter().map(|l| crabka_metadata::VoterEndpoint {
            name: l.name.clone(),
            host: l.host.clone(),
            port: u16::try_from(l.port).unwrap_or_default(),
        }).collect(),
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    };

    let error_code = match broker.controller.add_voter(crabka_raft::reconfig::AddVoter { voter }).await {
        Ok(crabka_raft::reconfig::ReconfigOutcome::Committed) => codes::NONE,
        Ok(crabka_raft::reconfig::ReconfigOutcome::NotLeader { .. }) => codes::NOT_LEADER_OR_FOLLOWER,
        Err(crabka_raft::RaftError::VoterNotCaughtUp { .. }) => codes::INVALID_REQUEST,
        Err(crabka_raft::RaftError::ReconfigInProgress) => codes::REQUEST_TIMED_OUT,
        Err(crabka_raft::RaftError::ReconfigRejected(_)) => codes::INVALID_REQUEST,
        Err(_) => codes::UNKNOWN_SERVER_ERROR,
    };

    let resp = AddRaftVoterResponse { error_code, ..Default::default() };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

> Match the exact module path used by codegen (the Explore report shows `crabka_protocol::owned::*` with `API_KEY`/`MIN_VERSION`/`MAX_VERSION` consts and `decode`/`encode`/`encoded_len`). Adjust `use` paths to the real generated names (`add_raft_voter_request` vs `AddRaftVoterRequest` module casing).

- [ ] **Step 2: RemoveRaftVoter + UpdateRaftVoter handlers**

Create `remove_raft_voter.rs` mirroring Step 1 but decoding `RemoveRaftVoterRequest` (fields `voter_id`, `voter_directory_id`) and calling `broker.controller.remove_voter(RemoveVoter { id, directory_id })`. Create `update_raft_voter.rs` decoding `UpdateRaftVoterRequest` and calling `update_voter(UpdateVoter { voter })`. Same error→code mapping (UpdateVoter has no `VoterNotCaughtUp`).

- [ ] **Step 3: Register modules**

In `crates/broker/src/handlers/mod.rs`, add:

```rust
pub(crate) mod add_raft_voter;
pub(crate) mod remove_raft_voter;
pub(crate) mod update_raft_voter;
```

- [ ] **Step 4: Dispatch frame fns + match arms**

In `crates/broker/src/network/dispatch.rs`, following the `handle_describe_quorum_frame` pattern, add `handle_add_raft_voter_frame` (api_key 80), `handle_remove_raft_voter_frame` (81), `handle_update_raft_voter_frame` (82). Add their arms to the main api-key match. Each parses the header, builds `RequestContext`, calls the handler, and `encode_response`s.

- [ ] **Step 5: DescribeQuorum v2**

In `describe_quorum.rs`, when `version >= 2`, populate per-voter `voter_directory_id` and the `Nodes` block (id + endpoints) from `quorum.voter_nodes` (added in Task 2 Step 7). Bump the response builder to read directory ids/endpoints from `QuorumState`.

> If the codegen'd `DescribeQuorumRequest`/`Response` `MAX_VERSION` is still `1`, the schema needs a v2 bump first: add the v2 fields (`VoterDirectoryId`, `Nodes`) to the DescribeQuorum schema JSON and regenerate. Treat that as Step 5a; run the codegen command the repo uses (check `crates/protocol/build.rs` / a `just codegen`/`make` target).

- [ ] **Step 6: Advertise in ApiVersions**

In `crates/broker/src/handlers/api_versions.rs` `supported_apis()`, add `v!(add_raft_voter_request)`, `v!(remove_raft_voter_request)`, `v!(update_raft_voter_request)`. DescribeQuorum's max version follows its const automatically once the schema is at v2.

- [ ] **Step 7: Handler unit tests**

Add an inline test per handler that builds a request, encodes it, calls `handle` against a single-node broker where the caller is the leader, and asserts `error_code == NONE`. Use the test harness from `crates/broker/tests/support` if a broker is needed, or assert the not-leader path against a follower. Minimum: a decode→encode round-trip test asserting the response encodes at v0 and v1.

Run: `cargo build -p crabka-broker 2>&1 | tail -20`
Expected: clean build.

- [ ] **Step 8: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/handlers crates/broker/src/network/dispatch.rs crates/protocol
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): AddRaftVoter/RemoveRaftVoter/UpdateRaftVoter RPCs + DescribeQuorum v2 (KIP-853)"
```

---

## Task 6: Auto-join

A controller absent from the voter set, with `auto_join = true`, discovers the leader via bootstrap servers, catches up as an observer, then issues `AddRaftVoter` for itself.

**Files:**
- Create: `crates/broker/src/auto_join.rs`
- Modify: `crates/broker/src/broker.rs` (spawn the auto-join task at startup)
- Modify: `crates/broker/src/lib.rs` (`mod auto_join;`)
- Test: covered by Task 7 integration; add a unit test for the "already a voter → no-op" guard

- [ ] **Step 1: Auto-join task**

Create `crates/broker/src/auto_join.rs`:

```rust
//! KIP-853 auto-join: promote self from observer to voter once caught up.

use std::time::Duration;

use crate::{Broker, BrokerError};

/// Runs in the background after startup. Idempotent and self-terminating.
pub(crate) async fn run(broker: std::sync::Arc<Broker>) {
    if !broker.config.auto_join {
        return;
    }
    let self_id = broker.config.node_id;
    loop {
        let image = broker.controller.current_image();
        if image.voters().contains(self_id) {
            return; // already a voter; nothing to do
        }
        match try_join(&broker, self_id).await {
            Ok(true) => return,
            Ok(false) | Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

async fn try_join(broker: &Broker, self_id: crabka_raft::NodeId) -> Result<bool, BrokerError> {
    // Build our own Voter from local listener config + directory id.
    let voter = crabka_metadata::Voter {
        id: self_id,
        directory_id: broker.config.directory_id,
        endpoints: broker.config.controller_endpoints(), // helper returning Vec<VoterEndpoint>
        kraft_version: crabka_metadata::KRaftVersionRange::default(),
    };
    // Send AddRaftVoter to the current leader (via the existing controller client /
    // bootstrap-server discovery used by submit_change forwarding).
    let outcome = broker.controller.add_voter(crabka_raft::reconfig::AddVoter { voter }).await;
    match outcome {
        Ok(crabka_raft::reconfig::ReconfigOutcome::Committed) => Ok(true),
        Ok(crabka_raft::reconfig::ReconfigOutcome::NotLeader { .. }) => Ok(false),
        Err(crabka_raft::RaftError::VoterNotCaughtUp { .. }) => Ok(false), // retry after catch-up
        Err(e) => Err(e.into()),
    }
}
```

> `add_voter` forwards to the leader internally (the coordinator returns `NotLeader` on a follower; the existing `submit_change` forwarding path already knows how to reach the leader — reuse it so a follower's auto-join request is forwarded rather than dropped). If forwarding for reconfig RPCs isn't already covered, add a forwarding arm mirroring `submit_change`.

- [ ] **Step 2: Spawn at startup**

In `broker.rs`, after the controller is started and the broker `Arc` exists, spawn:

```rust
tokio::spawn(crate::auto_join::run(broker.clone()));
```

- [ ] **Step 3: Build + unit test the no-op guard**

Add an inline test asserting `run` returns immediately when `auto_join = false`.

Run: `cargo build -p crabka-broker && cargo test -p crabka-broker auto_join:: -v`
Expected: clean build; test PASS.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/auto_join.rs crates/broker/src/broker.rs crates/broker/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(broker): KIP-853 controller auto-join"
```

---

## Task 7: Integration tests

**Files:**
- Modify: `crates/broker/tests/support/mod.rs` (update cluster boot to dynamic bootstrap — no static `voters`; format-seeded standalone + auto-join)
- Create: `crates/broker/tests/dynamic_voters.rs`
- Test command: `cargo test -p crabka-broker --test dynamic_voters`

- [ ] **Step 1: Update the test harness to dynamic bootstrap**

In `crates/broker/tests/support/mod.rs`, change `start_n_node` so broker 0 boots `--standalone` (initial voter set = {0}) and brokers 1..n boot empty with `auto_join = true` + `bootstrap_servers` pointing at broker 0's controller listener. Replace the Phase 3 manual `add_learner`/`change_membership` with reliance on auto-join (keep a bounded wait for the voter set to reach size `n`). Set `directory_id` per broker (generate at config build).

- [ ] **Step 2: Write the dynamic-membership test**

Create `crates/broker/tests/dynamic_voters.rs`:

```rust
mod support;
use std::time::Duration;

async fn wait_until<F: Fn() -> bool>(f: F, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if f() { return true; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    f()
}

#[tokio::test]
async fn auto_join_grows_quorum_to_three() {
    let cluster = support::start_n_node_with_retry(3).await;
    // Find the leader and assert its voter set converges to all 3.
    let leader = &cluster[0].0;
    let ok = wait_until(|| leader.controller.current_image().voters().len() == 3, Duration::from_secs(30)).await;
    assert!(ok, "voter set did not converge to 3 via auto-join");
}

#[tokio::test]
async fn remove_voter_shrinks_quorum() {
    let cluster = support::start_n_node_with_retry(3).await;
    let leader = &cluster[0].0;
    assert!(wait_until(|| leader.controller.current_image().voters().len() == 3, Duration::from_secs(30)).await);

    // Remove voter id 2 (a follower).
    let dir = leader.controller.current_image().voters().get(2).unwrap().directory_id;
    let out = leader.controller.remove_voter(crabka_raft::reconfig::RemoveVoter { id: 2, directory_id: dir }).await.unwrap();
    assert!(matches!(out, crabka_raft::reconfig::ReconfigOutcome::Committed));
    assert!(wait_until(|| leader.controller.current_image().voters().len() == 2, Duration::from_secs(15)).await);
}
```

> Adjust `cluster[i].0` field access and `controller` accessor to the real `BrokerHandle` API. If `BrokerHandle` doesn't expose `controller`/`add_voter` directly, add thin wrappers on `BrokerHandle` (mirrors the existing `change_membership` wrapper noted in the prior membership design).

- [ ] **Step 3: Run the integration tests**

Run: `cargo test -p crabka-broker --test dynamic_voters -- --nocapture 2>&1 | tail -30`
Expected: both tests PASS. If split-vote flakes occur on cold boot, the `start_n_node_with_retry` wrapper already retries.

- [ ] **Step 4: Full workspace verification**

Run: `cargo test --workspace 2>&1 | tail -30`
Expected: all green. Update any other call sites still referencing the removed static `voters` config (the compiler will flag them).

- [ ] **Step 5: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/tests
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-853 dynamic-voters integration (auto-join + remove)"
```

---

## Optional Task 8: JVM tooling compatibility check (if Docker/cp-kafka available)

Not a code task; a verification gate. Run a single crabka controller and point `kafka-metadata-quorum` at it:

```bash
kafka-metadata-quorum --bootstrap-controller <host:port> describe --status
kafka-metadata-quorum --bootstrap-controller <host:port> add-controller   # against a caught-up observer
kafka-metadata-quorum --bootstrap-controller <host:port> remove-controller --controller-id <id> --controller-directory-id <uuid>
```

Byte-compare the `DescribeQuorum` v2 response against a real cp-kafka KRaft controller. File a follow-up for any field mismatch.

---

## Self-review notes (author)

- **Spec coverage:** §Identity model → Task 1 (VoterSet/Voter) + Task 2 (Node). §Control records → Task 1. §Bootstrap/format → Task 4. §Reconfiguration coordinator (lockstep, single-change, observer catch-up, quorum-loss refusal, leader self-removal) → Task 3 (note: leader self-removal step-down is handled by openraft after `change_membership` removes the leader; the coordinator emits the record then openraft steps down — verify in Task 7 if time permits, otherwise file a follow-up). §Wire RPCs + DescribeQuorum v2 → Task 5. §Auto-join → Task 6. §Snapshots → explicitly out of scope (only bootstrap-record seeding of version+voters, done in Task 4); full FetchSnapshot transfer deferred per spec.
- **Known follow-ups carried from spec:** full snapshot transfer (`FetchSnapshot`/`InstallSnapshot`); byte-exact Kafka control-record codecs for `VotersRecord`/`KRaftVersionRecord` (only needed at the FetchSnapshot boundary, which is deferred).
- **Codegen dependency:** Task 5 Step 5 may require a DescribeQuorum schema v2 bump + regenerate; flagged inline.
