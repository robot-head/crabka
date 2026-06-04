# KIP-1071 Streams Client #1 — Membership + Byte-Exact Topology — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a new `crabka-client-streams` crate that defines a Processor-API topology (serialized byte-for-byte to the JVM 4.x `StreamsGroupHeartbeat.Topology` wire shape), joins a KIP-1071 streams group, and surfaces assigned active/standby/warmup tasks with correct fencing/rejoin/leave.

**Architecture:** A topology subsystem (insertion-ordered node graph → JVM `makeNodeGroups` union-find indexing → wire serialization) plus a membership subsystem (a `StreamsGroupHeartbeat` join + background heartbeat loop emitting events), mirroring the existing `crates/client-consumer/src/share/` module. The protocol layer already exists: `StreamsGroupHeartbeatRequest` (apiKey 88, v0) impls `ProtocolRequest` and dispatches via `Client::send`.

**Tech Stack:** Rust 2024, tokio, `bon` builders, `crabka-client-core` (transport), `crabka-protocol` (generated wire types), `thiserror`. Tests use an in-process `crabka-broker` (`test-helpers` feature) and `assert2`.

**Spec:** `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-membership-design.md`

---

## File structure

```
crates/client-streams/
  Cargo.toml                          new workspace member (version 0.2.0)
  src/
    lib.rs                            crate docs, module decls, public re-exports
    error.rs                          StreamsClientError
    topology/
      mod.rs                          re-exports; module glue
      node.rs                         NodeRegistry: insertion-ordered Source/Processor/Sink nodes + stores + repartition-topic set
      grouping.rs                     port of JVM makeNodeGroups (quick-union, first-seen index, drop-empty-source) → GroupTopics
      wire.rs                         GroupTopics + application_id → crabka_protocol Topology (sorting, naming, epoch=0, copartition indices)
      builder.rs                      public Topology builder + BuiltTopology + TopologyError
    membership/
      mod.rs                          re-exports
      types.rs                        StreamsAssignment, TaskAssignment, TopicPartition, StreamsStatus, StreamsEvent
      status.rs                       response Status code → StreamsStatus
      assignment.rs                   response TaskIds + BuiltTopology → StreamsAssignment (task → topic-partitions)
      coordinator.rs                  background StreamsGroupHeartbeat loop (epoch dance, fence→rejoin, adopt-and-echo, leave, event emit)
      client.rs                       public StreamsMembership handle + builder (join / next_event / close)
  tests/
    integration.rs                    in-process broker: join → Assigned/NotReady → leave
    golden_frame.rs                    byte-exact vs captured JVM Topology fixtures
    testdata/golden/                   captured JVM frames + capture README
```

## Reference facts (verified against the codebase — use verbatim)

**Protocol types already exist** (no protocol-layer work):
- `crabka_protocol::owned::streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Topology, Subtopology, CopartitionGroup}`
- `crabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo` — fields `name: String`, `partitions: i32`, `replication_factor: i16`, `topic_configs: Vec<KeyValue>`
- `crabka_protocol::owned::common::streams_group_heartbeat_request::key_value::KeyValue` — `key: String`, `value: String`
- `crabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds` — `subtopology_id: String`, `partitions: Vec<i32>` (request-side owned echo)
- `crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse` — `error_code: i16`, `error_message: Option<String>`, `member_id: String`, `member_epoch: i32`, `heartbeat_interval_ms: i32`, `acceptable_recovery_lag: i32`, `task_offset_interval_ms: i32`, `status: Option<Vec<Status>>`, `active_tasks/standby_tasks/warmup_tasks: Option<Vec<TaskIds>>`
- `crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds` — `subtopology_id: String`, `partitions: Vec<i32>`
- `crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status` — `status_code: i8`, `status_detail: String`
- `Topology` derives `Debug, Clone, PartialEq, Eq, Default` and impls `Encode` standalone.
- `Subtopology` fields: `subtopology_id`, `source_topics: Vec<String>`, `source_topic_regex: Vec<String>`, `state_changelog_topics: Vec<TopicInfo>`, `repartition_sink_topics: Vec<String>`, `repartition_source_topics: Vec<TopicInfo>`, `copartition_groups: Vec<CopartitionGroup>`
- `CopartitionGroup` fields: `source_topics: Vec<i16>`, `source_topic_regex: Vec<i16>`, `repartition_source_topics: Vec<i16>`

**JVM 4.x derivation rules** (the byte-exact contract):
- `Topology.Epoch` is always `0`.
- `SubtopologyId` = node-group integer index rendered as a decimal string (`"0"`, `"1"`, …).
- Index assignment: union-find over node names (unite processor↔each predecessor; unite all processors sharing a state store); iterate nodes in **insertion order**; first node of a not-yet-seen root mints the next index. Groups with **no source topics are dropped but still consume an index** (ids can be non-contiguous).
- Sorting: `source_topics` & `repartition_sink_topics` lexicographic; `repartition_source_topics` & `state_changelog_topics` by name; `topic_configs` by key; the `subtopologies` list **by id as a string** (`"0","1","10","2"`).
- `source_topic_regex` always empty. Copartition indices are `int16` into the sorted `source_topics` / `repartition_source_topics` arrays.
- Internal topic names: changelog `<application_id>-<store>-changelog`; repartition `<application_id>-<name>-repartition`.
- `TopicInfo.partitions`: `0` for changelog (always); repartition-source = enforced count or `0`. `replication_factor`: `0` (let broker default) unless pinned.

**Client + broker mechanics:**
- `crabka_client_core::{Client, ClientError, ProtocolRequest}`; build with `Client::builder().bootstrap(addr).client_id(id).maybe_security(sec).build().await?`; dispatch with `client.send(req).await?`.
- Broker is serial per-connection — run the heartbeat loop on its **own** `Client` connection (see `share/consumer.rs`).
- In-process broker test boot: `Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap()`, address `broker.listen_addr().to_string()`, teardown `broker.shutdown().await`. Use `#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`.
- Streams gate: `streams_group.enable` is already `true` in `BrokerConfig::for_tests`; you MUST finalize `streams.version` to level 1 via `UpdateFeatures` before any streams RPC (else `UNSUPPORTED_VERSION` = 34). First join may transiently return `COORDINATOR_LOAD_IN_PROGRESS` = 14 — retry.

## Execution batches (non-overlapping file sets — for subagent-driven dispatch)

- **Batch 0:** Task 1 (scaffolding). Everything depends on it.
- **Batch 1 (parallel):** Task 2 (`topology/node.rs`) ‖ Task 6 (`membership/types.rs` + `status.rs`). Disjoint files.
- **Sequential (topology chain):** Task 3 (`grouping.rs`) → Task 4 (`wire.rs`) → Task 5 (`builder.rs` + `topology/mod.rs`).
- **Batch 2 (parallel after 5 & 6):** Task 7 (`assignment.rs`) — needs `BuiltTopology` + types.
- **Sequential (membership runtime):** Task 8 (`coordinator.rs`) → Task 9 (`client.rs` + `membership/mod.rs`).
- **Batch 3 (parallel after 9 / 5):** Task 10 (`tests/integration.rs`) ‖ Task 11 (`tests/golden_frame.rs`).
- **Batch 4:** Task 12 (docs + final verification).

---

## Task 1: Crate scaffolding

**Files:**
- Create: `crates/client-streams/Cargo.toml`
- Create: `crates/client-streams/src/lib.rs`
- Create: `crates/client-streams/src/error.rs`

- [ ] **Step 1: Create the manifest**

`crates/client-streams/Cargo.toml`:
```toml
[package]
name = "crabka-client-streams"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "KIP-1071 Kafka Streams rebalance-protocol client for Apache Kafka in Rust"

[lints]
workspace = true

[features]
default = []

[dependencies]
crabka-protocol = { version = "0.2", path = "../protocol", default-features = false }
crabka-client-core = { version = "0.2", path = "../client-core" }
bon = { workspace = true }
bytes = { workspace = true }
thiserror = { workspace = true }
uuid = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "sync", "time", "macros"] }
tokio-util = { workspace = true, features = ["rt"] }
tracing = { workspace = true }

[dev-dependencies]
assert2 = { workspace = true }
crabka-broker = { version = "0.2", path = "../broker", features = ["test-helpers"] }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros", "rt-multi-thread"] }
```

- [ ] **Step 2: Create the error type**

`crates/client-streams/src/error.rs`:
```rust
//! Error type for the streams membership client.

/// Errors surfaced by the streams membership client.
#[derive(Debug, thiserror::Error)]
pub enum StreamsClientError {
    /// Transport / dispatch failure from `crabka-client-core`.
    #[error(transparent)]
    Transport(#[from] crabka_client_core::ClientError),
    /// Building the topology failed (bad node graph).
    #[error("topology error: {0}")]
    Topology(#[from] crate::topology::TopologyError),
    /// The group coordinator was unavailable past the retry deadline.
    #[error("streams group coordinator unavailable")]
    CoordinatorUnavailable,
    /// The broker rejected the topology (`STREAMS_INVALID_TOPOLOGY*` family).
    #[error("invalid topology (code {code}): {message}")]
    InvalidTopology { code: i16, message: String },
    /// `GROUP_AUTHORIZATION_FAILED` / `TOPIC_AUTHORIZATION_FAILED`.
    #[error("authorization failed (code {0})")]
    Authorization(i16),
    /// `GROUP_ID_NOT_FOUND`.
    #[error("group id not found")]
    GroupIdNotFound,
    /// The membership handle has been closed.
    #[error("membership closed")]
    Closed,
    /// An unmapped broker error code.
    #[error("broker error code {0}")]
    Server(i16),
}
```

- [ ] **Step 3: Create the crate root**

`crates/client-streams/src/lib.rs`:
```rust
//! KIP-1071 Kafka Streams rebalance-protocol client.
//!
//! Sub-project #1 of the Crabka Streams runtime: a [`StreamsMembership`] joins a
//! *streams group* via `StreamsGroupHeartbeat` (API key 88), maintains
//! membership with a background heartbeat, and surfaces assigned active/standby/
//! warmup tasks. The [`topology`] module builds a Processor-API topology and
//! serializes it byte-for-byte to the JVM 4.x wire shape.
//!
//! Processors are *structural placeholders* here — record processing arrives in
//! a later sub-project. See
//! `docs/superpowers/specs/2026-06-03-kip-1071-streams-client-membership-design.md`.
#![doc(html_root_url = "https://docs.rs/crabka-client-streams/0.0.0")]

mod error;
pub mod membership;
pub mod topology;

pub use error::StreamsClientError;
pub use membership::{
    StreamsAssignment, StreamsEvent, StreamsMembership, StreamsStatus, TaskAssignment,
    TopicPartition,
};
pub use topology::{BuiltTopology, Topology, TopologyError};
```

- [ ] **Step 4: Create placeholder module roots so it compiles**

`crates/client-streams/src/topology/mod.rs`:
```rust
//! Topology builder: Processor-API node graph → byte-exact wire `Topology`.

mod builder;
mod grouping;
mod node;
mod wire;

pub use builder::{BuiltTopology, Topology, TopologyError};
```

`crates/client-streams/src/membership/mod.rs`:
```rust
//! Streams group membership: `StreamsGroupHeartbeat` lifecycle + assignments.

mod assignment;
mod client;
mod coordinator;
mod status;
mod types;

pub use client::StreamsMembership;
pub use types::{StreamsAssignment, StreamsEvent, StreamsStatus, TaskAssignment, TopicPartition};
```

Create empty stub files so the modules resolve (each task below fills one in):
`topology/{node,grouping,wire,builder}.rs` and `membership/{types,status,assignment,coordinator,client}.rs`. For this step give each a minimal compilable stub, e.g. `builder.rs`:
```rust
//! stub — filled in Task 5
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {}
pub struct Topology;
pub struct BuiltTopology;
```
and the membership stubs likewise expose the names re-exported above with `pub struct`/`pub enum` placeholders. (Subagent: the simplest path is to write the real Task 2/6 content here if dispatched together; otherwise minimal stubs.)

- [ ] **Step 5: Verify it builds and joins the workspace**

Run: `cargo build -p crabka-client-streams`
Expected: compiles (warnings about unused stubs are fine).

Run: `cargo metadata --format-version 1 --no-deps | grep -c crabka-client-streams`
Expected: ≥ 1 (the crate is a recognized workspace member via `members = ["crates/*"]`).

- [ ] **Step 6: Commit**

```bash
git add crates/client-streams
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): scaffold crabka-client-streams crate"
```

---

## Task 2: Topology node model

**Files:**
- Modify: `crates/client-streams/src/topology/node.rs`

- [ ] **Step 1: Write the failing test**

Append to `node.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn nodes_preserve_insertion_order() {
        let mut reg = NodeRegistry::default();
        reg.add_source("src", vec!["t".into()]).unwrap();
        reg.add_processor("p", vec!["src".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["p".into()]).unwrap();
        let names: Vec<&str> = reg.nodes.iter().map(|n| n.name.as_str()).collect();
        check!(names == vec!["src", "p", "snk"]);
    }

    #[test]
    fn duplicate_node_is_rejected() {
        let mut reg = NodeRegistry::default();
        reg.add_source("a", vec!["t".into()]).unwrap();
        check!(reg.add_processor("a", vec![]).is_err());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib topology::node`
Expected: FAIL — `NodeRegistry` / methods not defined.

- [ ] **Step 3: Implement the node model**

Replace `node.rs` body (above the test module) with:
```rust
//! Insertion-ordered processor-node graph: the structural input the JVM's
//! `makeNodeGroups` operates on. Order is load-bearing — it determines
//! subtopology indices.

use std::collections::{HashMap, HashSet};

use super::builder::TopologyError;

/// What a node is and which topics/predecessors it touches.
#[derive(Debug, Clone)]
pub(crate) enum NodeKind {
    Source { topics: Vec<String> },
    Processor { predecessors: Vec<String> },
    Sink { topic: String, predecessors: Vec<String> },
}

#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub name: String,
    pub kind: NodeKind,
}

/// The full node graph, recorded in insertion order.
#[derive(Debug, Default)]
pub(crate) struct NodeRegistry {
    pub nodes: Vec<Node>,
    pub index: HashMap<String, usize>,
    /// `(store_name, connected_processor_names)` in insertion order.
    pub stores: Vec<(String, Vec<String>)>,
    /// Topic names registered as internal repartition topics.
    pub repartition_topics: HashSet<String>,
}

impl NodeRegistry {
    fn insert(&mut self, node: Node) -> Result<(), TopologyError> {
        if self.index.contains_key(&node.name) {
            return Err(TopologyError::DuplicateNode(node.name));
        }
        self.index.insert(node.name.clone(), self.nodes.len());
        self.nodes.push(node);
        Ok(())
    }

    pub fn add_source(&mut self, name: &str, topics: Vec<String>) -> Result<(), TopologyError> {
        self.insert(Node { name: name.to_string(), kind: NodeKind::Source { topics } })
    }

    pub fn add_processor(
        &mut self,
        name: &str,
        predecessors: Vec<String>,
    ) -> Result<(), TopologyError> {
        self.insert(Node { name: name.to_string(), kind: NodeKind::Processor { predecessors } })
    }

    pub fn add_sink(
        &mut self,
        name: &str,
        topic: String,
        predecessors: Vec<String>,
    ) -> Result<(), TopologyError> {
        self.insert(Node { name: name.to_string(), kind: NodeKind::Sink { topic, predecessors } })
    }

    pub fn add_store(&mut self, name: &str, processors: Vec<String>) {
        self.stores.push((name.to_string(), processors));
    }

    /// Validate that every referenced predecessor exists. Call after all nodes
    /// are added, before grouping.
    pub fn validate_predecessors(&self) -> Result<(), TopologyError> {
        for node in &self.nodes {
            let preds = match &node.kind {
                NodeKind::Processor { predecessors } => predecessors,
                NodeKind::Sink { predecessors, .. } => predecessors,
                NodeKind::Source { .. } => continue,
            };
            for p in preds {
                if !self.index.contains_key(p) {
                    return Err(TopologyError::UnknownPredecessor {
                        node: node.name.clone(),
                        predecessor: p.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}
```

Add the needed `TopologyError` variants now (Task 5 owns `builder.rs`, but these variants are referenced here — define them in `builder.rs` in this step so the crate compiles):
```rust
// in builder.rs, replacing the stub enum:
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib topology::node`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/topology/node.rs crates/client-streams/src/topology/builder.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): insertion-ordered topology node model"
```

---

## Task 3: Subtopology grouping (JVM makeNodeGroups port)

**Files:**
- Modify: `crates/client-streams/src/topology/grouping.rs`

- [ ] **Step 1: Write the failing tests**

Append to `grouping.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::node::NodeRegistry;
    use assert2::check;

    fn ids(groups: &[GroupTopics]) -> Vec<&str> {
        groups.iter().map(|g| g.id.as_str()).collect()
    }

    #[test]
    fn single_source_sink_is_one_subtopology() {
        let mut reg = NodeRegistry::default();
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["src".into()]).unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0"]);
        check!(groups[0].source_topics == vec!["in".to_string()]);
    }

    #[test]
    fn repartition_chain_is_two_subtopologies() {
        // src -> sink(repartition "rp"); source(rp) -> snk2
        let mut reg = NodeRegistry::default();
        reg.repartition_topics.insert("rp".into());
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("rsink", "rp".into(), vec!["src".into()]).unwrap();
        reg.add_source("rsrc", vec!["rp".into()]).unwrap();
        reg.add_sink("snk2", "out".into(), vec!["rsrc".into()]).unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0", "1"]);
        // group 0 reads external "in" and writes repartition "rp"
        check!(groups[0].source_topics == vec!["in".to_string()]);
        check!(groups[0].repartition_sink_topics == vec!["rp".to_string()]);
        // group 1 reads repartition "rp" as a repartition source
        check!(groups[1].repartition_source_topics == vec!["rp".to_string()]);
    }

    #[test]
    fn shared_state_store_unites_processors_into_one_group() {
        // two independent source->processor chains, joined only by a shared store
        let mut reg = NodeRegistry::default();
        reg.add_source("s1", vec!["a".into()]).unwrap();
        reg.add_processor("p1", vec!["s1".into()]).unwrap();
        reg.add_source("s2", vec!["b".into()]).unwrap();
        reg.add_processor("p2", vec!["s2".into()]).unwrap();
        reg.add_store("store", vec!["p1".into(), "p2".into()]);
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["0"]);
        let mut srcs = groups[0].source_topics.clone();
        srcs.sort();
        check!(srcs == vec!["a".to_string(), "b".to_string()]);
        check!(groups[0].changelog_stores == vec!["store".to_string()]);
    }

    #[test]
    fn source_less_group_is_dropped_but_consumes_an_index() {
        // group 0: a sink-only orphan (no source) -> dropped, but index 0 used.
        // group 1: real source->sink -> emitted as "1".
        let mut reg = NodeRegistry::default();
        reg.add_processor("orphan_proc", vec![]).unwrap(); // no predecessors, no source
        reg.add_sink("orphan_sink", "x".into(), vec!["orphan_proc".into()]).unwrap();
        reg.add_source("src", vec!["in".into()]).unwrap();
        reg.add_sink("snk", "out".into(), vec!["src".into()]).unwrap();
        let groups = group_nodes(&reg);
        check!(ids(&groups) == vec!["1"]); // "0" was consumed by the dropped group
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-client-streams --lib topology::grouping`
Expected: FAIL — `group_nodes` / `GroupTopics` not defined.

- [ ] **Step 3: Implement grouping**

Replace `grouping.rs` body (above tests) with:
```rust
//! Port of `InternalTopologyBuilder.makeNodeGroups`: union-find over the node
//! graph in insertion order assigns each subtopology its integer index; the
//! index is rendered as a decimal string. Groups with no source topics are
//! dropped but still consume an index (so ids may be non-contiguous).

use std::collections::HashMap;

use super::node::{NodeKind, NodeRegistry};

/// One subtopology's resolved topic sets, keyed by its decimal-string id.
#[derive(Debug, Clone, Default)]
pub(crate) struct GroupTopics {
    pub id: String,
    /// External source topics (sorted later by the wire layer).
    pub source_topics: Vec<String>,
    /// Internal repartition topics this subtopology reads.
    pub repartition_source_topics: Vec<String>,
    /// Internal repartition topics this subtopology writes.
    pub repartition_sink_topics: Vec<String>,
    /// Store names whose changelog topics back this subtopology.
    pub changelog_stores: Vec<String>,
}

/// Minimal quick-union over `usize` node indices (path-compressing find).
struct QuickUnion {
    parent: Vec<usize>,
}

impl QuickUnion {
    fn new(n: usize) -> Self {
        Self { parent: (0..n).collect() }
    }
    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }
    fn unite(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[ra] = rb;
        }
    }
}

/// Group the registry's nodes into subtopologies with JVM-matching ids.
pub(crate) fn group_nodes(reg: &NodeRegistry) -> Vec<GroupTopics> {
    let n = reg.nodes.len();
    let mut uf = QuickUnion::new(n);

    // Unite each processor/sink with each predecessor.
    for (i, node) in reg.nodes.iter().enumerate() {
        let preds = match &node.kind {
            NodeKind::Processor { predecessors } => predecessors,
            NodeKind::Sink { predecessors, .. } => predecessors,
            NodeKind::Source { .. } => continue,
        };
        for p in preds {
            if let Some(&j) = reg.index.get(p) {
                uf.unite(i, j);
            }
        }
    }
    // Unite all processors that share a state store.
    for (_store, procs) in &reg.stores {
        let mut iter = procs.iter().filter_map(|p| reg.index.get(p).copied());
        if let Some(first) = iter.next() {
            for other in iter {
                uf.unite(first, other);
            }
        }
    }

    // First-seen root (in insertion order) mints the next index.
    let mut root_to_id: HashMap<usize, usize> = HashMap::new();
    let mut next_id = 0usize;
    let mut groups: HashMap<usize, GroupTopics> = HashMap::new();
    let mut order: Vec<usize> = Vec::new();

    for i in 0..n {
        let root = uf.find(i);
        let id = *root_to_id.entry(root).or_insert_with(|| {
            let id = next_id;
            next_id += 1;
            order.push(id);
            id
        });
        let entry = groups.entry(id).or_insert_with(|| GroupTopics {
            id: id.to_string(),
            ..Default::default()
        });
        match &reg.nodes[i].kind {
            NodeKind::Source { topics } => {
                for t in topics {
                    if reg.repartition_topics.contains(t) {
                        entry.repartition_source_topics.push(t.clone());
                    } else {
                        entry.source_topics.push(t.clone());
                    }
                }
            }
            NodeKind::Sink { topic, .. } => {
                if reg.repartition_topics.contains(topic) {
                    entry.repartition_sink_topics.push(topic.clone());
                }
            }
            NodeKind::Processor { .. } => {}
        }
    }
    // Attach changelog stores to the group their processors landed in.
    for (store, procs) in &reg.stores {
        if let Some(&first) = procs.first().and_then(|p| reg.index.get(p)) {
            let root = uf.find(first);
            if let Some(&id) = root_to_id.get(&root) {
                if let Some(g) = groups.get_mut(&id) {
                    g.changelog_stores.push(store.clone());
                }
            }
        }
    }

    // Emit in id order, dropping groups with no source-side topics.
    order
        .into_iter()
        .filter_map(|id| groups.remove(&id))
        .filter(|g| !g.source_topics.is_empty() || !g.repartition_source_topics.is_empty())
        .collect()
}
```

Make `node` accessible to `grouping`: ensure `topology/mod.rs` declares `mod node;` (already there) and `node`'s items are `pub(crate)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib topology::grouping`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/topology/grouping.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): port JVM makeNodeGroups subtopology grouping"
```

---

## Task 4: Wire serialization

**Files:**
- Modify: `crates/client-streams/src/topology/wire.rs`

- [ ] **Step 1: Write the failing tests**

Append to `wire.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::grouping::GroupTopics;
    use assert2::check;

    #[test]
    fn epoch_is_zero_and_source_topics_sorted() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["b".into(), "a".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "app");
        check!(topo.epoch == 0);
        check!(topo.subtopologies[0].source_topics == vec!["a".to_string(), "b".to_string()]);
        check!(topo.subtopologies[0].source_topic_regex.is_empty());
    }

    #[test]
    fn subtopologies_sort_by_id_as_string_not_numeric() {
        let groups = vec![
            GroupTopics { id: "2".into(), source_topics: vec!["x".into()], ..Default::default() },
            GroupTopics { id: "10".into(), source_topics: vec!["x".into()], ..Default::default() },
            GroupTopics { id: "1".into(), source_topics: vec!["x".into()], ..Default::default() },
        ];
        let topo = to_wire(&groups, "app");
        let ids: Vec<&str> = topo.subtopologies.iter().map(|s| s.subtopology_id.as_str()).collect();
        check!(ids == vec!["1", "10", "2"]); // lexicographic, not numeric
    }

    #[test]
    fn changelog_topics_named_and_zero_partitions() {
        let groups = vec![GroupTopics {
            id: "0".into(),
            source_topics: vec!["in".into()],
            changelog_stores: vec!["store".into()],
            ..Default::default()
        }];
        let topo = to_wire(&groups, "my-app");
        let cl = &topo.subtopologies[0].state_changelog_topics;
        check!(cl.len() == 1);
        check!(cl[0].name == "my-app-store-changelog");
        check!(cl[0].partitions == 0);
    }

    #[test]
    fn copartition_indices_point_into_sorted_arrays() {
        // sorted sources: ["a","b","c"]; copartition over {"c","a"} → indices [0,2]
        let sources = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let repartition: Vec<String> = vec![];
        let cg = copartition_group(&sources, &repartition, &["c".into(), "a".into()]);
        check!(cg.source_topics == vec![0i16, 2i16]);
        check!(cg.repartition_source_topics.is_empty());
        check!(cg.source_topic_regex.is_empty());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p crabka-client-streams --lib topology::wire`
Expected: FAIL — `to_wire` / `copartition_group` not defined.

- [ ] **Step 3: Implement the serializer**

Replace `wire.rs` body (above tests) with:
```rust
//! `GroupTopics` + `application_id` → the byte-exact `StreamsGroupHeartbeat`
//! wire `Topology`. Every ordering rule here matches the JVM 4.x client.

use crabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;
use crabka_protocol::owned::streams_group_heartbeat_request::{
    CopartitionGroup, Subtopology, Topology,
};

use super::grouping::GroupTopics;

/// Build the wire `Topology` (epoch 0, sorted subtopologies + topic arrays).
pub(crate) fn to_wire(groups: &[GroupTopics], application_id: &str) -> Topology {
    let mut subtopologies: Vec<Subtopology> =
        groups.iter().map(|g| subtopology(g, application_id)).collect();
    // Sort the subtopology list by id AS A STRING (JVM behavior).
    subtopologies.sort_by(|a, b| a.subtopology_id.cmp(&b.subtopology_id));
    Topology { epoch: 0, subtopologies, ..Default::default() }
}

fn subtopology(g: &GroupTopics, app: &str) -> Subtopology {
    let mut source_topics = g.source_topics.clone();
    source_topics.sort();
    let mut repartition_sink_topics = g.repartition_sink_topics.clone();
    repartition_sink_topics.sort();

    let mut repartition_source_topics: Vec<TopicInfo> = g
        .repartition_source_topics
        .iter()
        .map(|name| TopicInfo {
            name: name.clone(),
            partitions: 0, // no enforced count in #1
            replication_factor: 0,
            ..Default::default()
        })
        .collect();
    repartition_source_topics.sort_by(|a, b| a.name.cmp(&b.name));

    let mut state_changelog_topics: Vec<TopicInfo> = g
        .changelog_stores
        .iter()
        .map(|store| TopicInfo {
            name: format!("{app}-{store}-changelog"),
            partitions: 0, // always 0 for changelog
            replication_factor: 0,
            ..Default::default()
        })
        .collect();
    state_changelog_topics.sort_by(|a, b| a.name.cmp(&b.name));

    Subtopology {
        subtopology_id: g.id.clone(),
        source_topics,
        source_topic_regex: Vec::new(),
        state_changelog_topics,
        repartition_sink_topics,
        repartition_source_topics,
        copartition_groups: Vec::new(), // builder declares none in #1
        ..Default::default()
    }
}

/// Encode a copartition group as `int16` indices into the sorted `sources` /
/// `repartition` arrays. Exposed (and unit-tested) so the byte-exact encoding is
/// covered even though the #1 builder emits no copartition groups.
pub(crate) fn copartition_group(
    sources: &[String],
    repartition: &[String],
    members: &[String],
) -> CopartitionGroup {
    let mut source_topics = Vec::new();
    let mut repartition_source_topics = Vec::new();
    for m in members {
        if let Some(i) = sources.iter().position(|s| s == m) {
            source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        } else if let Some(i) = repartition.iter().position(|s| s == m) {
            repartition_source_topics.push(i16::try_from(i).unwrap_or(i16::MAX));
        }
    }
    CopartitionGroup {
        source_topics,
        source_topic_regex: Vec::new(),
        repartition_source_topics,
        ..Default::default()
    }
}
```

Make `grouping::GroupTopics` visible to `wire` — it is already `pub(crate)`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p crabka-client-streams --lib topology::wire`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/topology/wire.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): byte-exact wire Topology serialization"
```

---

## Task 5: Public Topology builder + BuiltTopology

**Files:**
- Modify: `crates/client-streams/src/topology/builder.rs`
- Modify: `crates/client-streams/src/topology/mod.rs`

- [ ] **Step 1: Write the failing test**

Append to `builder.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use assert2::check;

    #[test]
    fn build_single_source_sink() {
        let mut topo = Topology::new();
        topo.add_source("src", ["in"]);
        topo.add_sink("snk", "out", ["src"]);
        let built = topo.build("app").unwrap();
        let wire = built.to_wire();
        check!(wire.epoch == 0);
        check!(wire.subtopologies.len() == 1);
        check!(wire.subtopologies[0].subtopology_id == "0");
        check!(wire.subtopologies[0].source_topics == vec!["in".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
    }

    #[test]
    fn unknown_predecessor_is_rejected() {
        let mut topo = Topology::new();
        topo.add_source("src", ["in"]);
        topo.add_sink("snk", "out", ["nope"]);
        check!(topo.build("app").is_err());
    }

    #[test]
    fn empty_topology_is_rejected() {
        let topo = Topology::new();
        check!(topo.build("app").is_err());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib topology::builder`
Expected: FAIL — `Topology::new`/`add_source`/`build`/`BuiltTopology` not defined (only the error enum exists).

- [ ] **Step 3: Implement the builder**

Replace `builder.rs` body (keep the `TopologyError` enum from Task 2; place this above the test module):
```rust
//! Public Processor-API topology builder. Records a node graph, then `build`
//! derives byte-exact subtopologies and the wire `Topology`.

use std::collections::BTreeMap;

use crabka_protocol::owned::streams_group_heartbeat_request::Topology as WireTopology;

use super::grouping::group_nodes;
use super::node::NodeRegistry;
use super::wire::to_wire;

/// Errors from building a topology.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("duplicate node name: {0}")]
    DuplicateNode(String),
    #[error("node {node} references unknown predecessor {predecessor}")]
    UnknownPredecessor { node: String, predecessor: String },
    #[error("topology has no source nodes")]
    Empty,
}

/// A Processor-API topology under construction. Node insertion order is
/// significant — it determines subtopology indices (JVM-matching).
#[derive(Debug, Default)]
pub struct Topology {
    reg: NodeRegistry,
    error: Option<TopologyError>,
}

impl Topology {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a source node reading the given external topics.
    pub fn add_source<S, I, T>(&mut self, name: S, topics: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let topics = topics.into_iter().map(Into::into).collect();
        self.record(self.reg_add_source(name.into(), topics));
        self
    }

    /// Add a processor node with the given predecessor node names.
    pub fn add_processor<S, I, T>(&mut self, name: S, predecessors: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let preds = predecessors.into_iter().map(Into::into).collect();
        self.record(self.reg_add_processor(name.into(), preds));
        self
    }

    /// Add a sink node writing to `topic`, fed by the given predecessors.
    pub fn add_sink<S, U, I, T>(&mut self, name: S, topic: U, predecessors: I) -> &mut Self
    where
        S: Into<String>,
        U: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let preds = predecessors.into_iter().map(Into::into).collect();
        self.record(self.reg_add_sink(name.into(), topic.into(), preds));
        self
    }

    /// Register a state store connected to the given processors (→ changelog).
    pub fn add_state_store<S, I, T>(&mut self, name: S, processors: I) -> &mut Self
    where
        S: Into<String>,
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        let procs = processors.into_iter().map(Into::into).collect();
        self.reg.add_store(&name.into(), procs);
        self
    }

    /// Register a topic name as an internal repartition topic.
    pub fn add_repartition_topic<S: Into<String>>(&mut self, name: S) -> &mut Self {
        self.reg.repartition_topics.insert(name.into());
        self
    }

    /// Derive subtopologies and the wire topology. `application_id` drives
    /// internal-topic names (`<app>-<store>-changelog`).
    pub fn build<S: Into<String>>(&self, application_id: S) -> Result<BuiltTopology, TopologyError> {
        if let Some(e) = &self.error {
            // Re-create the recorded error (TopologyError isn't Clone-cheap; rebuild).
            return Err(match e {
                TopologyError::DuplicateNode(n) => TopologyError::DuplicateNode(n.clone()),
                TopologyError::UnknownPredecessor { node, predecessor } => {
                    TopologyError::UnknownPredecessor {
                        node: node.clone(),
                        predecessor: predecessor.clone(),
                    }
                }
                TopologyError::Empty => TopologyError::Empty,
            });
        }
        self.reg.validate_predecessors()?;
        let groups = group_nodes(&self.reg);
        if groups.is_empty() {
            return Err(TopologyError::Empty);
        }
        let app = application_id.into();
        let wire = to_wire(&groups, &app);
        let mut source_topics: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for g in &groups {
            let mut all = g.source_topics.clone();
            all.extend(g.repartition_source_topics.iter().cloned());
            source_topics.insert(g.id.clone(), all);
        }
        Ok(BuiltTopology { wire, source_topics, application_id: app })
    }

    // --- internal helpers that capture the first registry error ---
    fn record(&mut self, r: Result<(), TopologyError>) {
        if self.error.is_none() {
            if let Err(e) = r {
                self.error = Some(e);
            }
        }
    }
    fn reg_add_source(&self, _n: String, _t: Vec<String>) -> Result<(), TopologyError> {
        unreachable!("see note") // replaced below
    }
}
```

Note for the implementer: the `reg_add_*` indirection above is awkward because the public methods take `&mut self` but call helpers that mutate `self.reg`. Implement them directly instead — e.g. `add_source` body should be:
```rust
let topics = topics.into_iter().map(Into::into).collect();
let r = self.reg.add_source(&name.into(), topics);
self.record(r);
self
```
and likewise `add_processor`/`add_sink` call `self.reg.add_processor`/`add_sink` directly. Delete the `reg_add_source` stub. (Kept here only to make the intent explicit.)

Then the built topology:
```rust
/// A built topology: the wire `Topology` plus the per-subtopology source-topic
/// map used to resolve task assignments to concrete topic-partitions.
#[derive(Debug, Clone)]
pub struct BuiltTopology {
    wire: WireTopology,
    source_topics: BTreeMap<String, Vec<String>>,
    application_id: String,
}

impl BuiltTopology {
    /// The wire `Topology` to send in the join heartbeat.
    #[must_use]
    pub fn to_wire(&self) -> WireTopology {
        self.wire.clone()
    }

    /// The external + repartition source topics a subtopology's tasks read.
    #[must_use]
    pub fn source_topics_for(&self, subtopology_id: &str) -> &[String] {
        self.source_topics.get(subtopology_id).map_or(&[], Vec::as_slice)
    }

    /// The application id (drives internal-topic names).
    #[must_use]
    pub fn application_id(&self) -> &str {
        &self.application_id
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib topology::builder`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the whole topology module + clippy**

Run: `cargo test -p crabka-client-streams --lib topology`
Expected: PASS (all topology tests).
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/client-streams/src/topology
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): public Topology builder + BuiltTopology"
```

---

## Task 6: Membership value types + status mapping

**Files:**
- Modify: `crates/client-streams/src/membership/types.rs`
- Modify: `crates/client-streams/src/membership/status.rs`

*(Parallel-safe with Tasks 2–5: disjoint files.)*

- [ ] **Step 1: Write the failing test (status mapping)**

Append to `status.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::types::StreamsStatus;
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

    #[test]
    fn maps_known_codes() {
        let s = Status { status_code: 1, status_detail: "in".into(), ..Default::default() };
        check!(matches!(map_status(&s), StreamsStatus::MissingSourceTopics(d) if d == "in"));
        let s = Status { status_code: 4, status_detail: String::new(), ..Default::default() };
        check!(matches!(map_status(&s), StreamsStatus::ShutdownApplication));
    }

    #[test]
    fn maps_unknown_code_to_unknown() {
        let s = Status { status_code: 99, status_detail: "x".into(), ..Default::default() };
        check!(matches!(map_status(&s), StreamsStatus::Unknown(99, _)));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib membership::status`
Expected: FAIL — `map_status` / `StreamsStatus` not defined.

- [ ] **Step 3: Implement types + status mapping**

`membership/types.rs`:
```rust
//! Public value types surfaced by [`StreamsMembership`](super::StreamsMembership).

/// A concrete topic-partition a task consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

/// One assigned task and the source topic-partitions it processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAssignment {
    pub subtopology_id: String,
    pub partitions: Vec<i32>,
    pub source_topic_partitions: Vec<TopicPartition>,
}

/// The active/standby/warmup tasks assigned to this member.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamsAssignment {
    pub active: Vec<TaskAssignment>,
    pub standby: Vec<TaskAssignment>,
    pub warmup: Vec<TaskAssignment>,
}

/// A non-ready status reported by the coordinator (KIP-1071 status codes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsStatus {
    StaleTopology(String),
    MissingSourceTopics(String),
    IncorrectlyPartitionedTopics(String),
    MissingInternalTopics(String),
    ShutdownApplication,
    AssignmentDelayed(String),
    Unknown(i8, String),
}

/// An event emitted by the membership heartbeat loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsEvent {
    /// A new assignment was adopted.
    Assigned(StreamsAssignment),
    /// The group is not ready (e.g. missing source/internal topics).
    NotReady(Vec<StreamsStatus>),
    /// We were fenced and auto-rejoined; a fresh assignment will follow.
    Fenced,
}
```

`membership/status.rs`:
```rust
//! Map response `Status` codes to the typed [`StreamsStatus`].

use crabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;

use super::types::StreamsStatus;

/// KIP-1071 `StreamsGroupHeartbeatResponse.Status` codes.
pub(crate) fn map_status(s: &Status) -> StreamsStatus {
    let detail = s.status_detail.clone();
    match s.status_code {
        0 => StreamsStatus::StaleTopology(detail),
        1 => StreamsStatus::MissingSourceTopics(detail),
        2 => StreamsStatus::IncorrectlyPartitionedTopics(detail),
        3 => StreamsStatus::MissingInternalTopics(detail),
        4 => StreamsStatus::ShutdownApplication,
        5 => StreamsStatus::AssignmentDelayed(detail),
        other => StreamsStatus::Unknown(other, detail),
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib membership::status`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/membership/types.rs crates/client-streams/src/membership/status.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): membership value types + status mapping"
```

---

## Task 7: Assignment resolution

**Files:**
- Modify: `crates/client-streams/src/membership/assignment.rs`

- [ ] **Step 1: Write the failing test**

Append to `assignment.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Topology;
    use assert2::check;
    use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;

    fn built() -> crate::topology::BuiltTopology {
        let mut t = Topology::new();
        t.add_source("src", ["in"]);
        t.add_sink("snk", "out", ["src"]);
        t.build("app").unwrap()
    }

    #[test]
    fn resolves_task_to_source_topic_partitions() {
        let tasks = vec![TaskIds {
            subtopology_id: "0".into(),
            partitions: vec![0, 2],
            ..Default::default()
        }];
        let resolved = resolve(&Some(tasks), &built());
        check!(resolved.len() == 1);
        check!(resolved[0].subtopology_id == "0");
        check!(resolved[0].partitions == vec![0, 2]);
        let tps: Vec<(&str, i32)> = resolved[0]
            .source_topic_partitions
            .iter()
            .map(|tp| (tp.topic.as_str(), tp.partition))
            .collect();
        check!(tps == vec![("in", 0), ("in", 2)]);
    }

    #[test]
    fn none_resolves_to_empty() {
        check!(resolve(&None, &built()).is_empty());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p crabka-client-streams --lib membership::assignment`
Expected: FAIL — `resolve` not defined.

- [ ] **Step 3: Implement resolution**

`membership/assignment.rs` (above tests):
```rust
//! Resolve response `TaskIds` into [`TaskAssignment`]s carrying the concrete
//! source topic-partitions each task reads (via the built topology).

use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;

use super::types::{TaskAssignment, TopicPartition};
use crate::topology::BuiltTopology;

/// Map one role's assigned tasks to [`TaskAssignment`]s. `None` (unchanged
/// since last heartbeat) resolves to an empty vec.
pub(crate) fn resolve(tasks: &Option<Vec<TaskIds>>, topology: &BuiltTopology) -> Vec<TaskAssignment> {
    let Some(tasks) = tasks else { return Vec::new() };
    tasks
        .iter()
        .map(|t| {
            let topics = topology.source_topics_for(&t.subtopology_id);
            let mut tps = Vec::new();
            for &p in &t.partitions {
                for topic in topics {
                    tps.push(TopicPartition { topic: topic.clone(), partition: p });
                }
            }
            TaskAssignment {
                subtopology_id: t.subtopology_id.clone(),
                partitions: t.partitions.clone(),
                source_topic_partitions: tps,
            }
        })
        .collect()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p crabka-client-streams --lib membership::assignment`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/src/membership/assignment.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): resolve tasks to source topic-partitions"
```

---

## Task 8: Coordinator heartbeat loop

**Files:**
- Modify: `crates/client-streams/src/membership/coordinator.rs`

This mirrors `crates/client-consumer/src/share/coordinator.rs`. It has no isolated unit test (it needs a live broker); the integration test in Task 10 exercises it. Verify via compile + clippy here.

- [ ] **Step 1: Implement the coordinator**

`membership/coordinator.rs`:
```rust
//! Background `StreamsGroupHeartbeat` loop. Mirrors `share/coordinator.rs`: a
//! ticker + `select!` racing each heartbeat against shutdown. Adopts the
//! broker's epoch + assignment, echoes owned tasks back (adopt-and-echo
//! reconciliation), rejoins from epoch 0 on fence, and sends a leave heartbeat
//! (`member_epoch = -1`) on shutdown. Meaningful changes are emitted as
//! [`StreamsEvent`]s.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds;
use crabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds;
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;

use super::assignment::resolve;
use super::status::map_status;
use super::types::{StreamsAssignment, StreamsEvent};
use crate::topology::BuiltTopology;

const FENCED_MEMBER_EPOCH: i16 = 110;
const UNKNOWN_MEMBER_ID: i16 = 25;
const STALE_MEMBER_EPOCH: i16 = 113;

/// State owned by the heartbeat task.
pub(crate) struct CoordinatorState {
    pub client: Client,
    pub group_id: String,
    pub member_id: String,
    pub process_id: String,
    pub instance_id: Option<String>,
    pub rebalance_timeout_ms: i32,
    pub topology: Arc<BuiltTopology>,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Owned tasks last adopted, echoed back as `active_tasks` next heartbeat.
    pub owned_active: Arc<Mutex<Vec<RespTaskIds>>>,
    pub heartbeat_interval: Duration,
    pub events: mpsc::UnboundedSender<StreamsEvent>,
}

enum Outcome {
    Ok,
    Rejoin,
    Transient,
}

/// Drive the loop until `shutdown` fires, then leave.
pub(crate) async fn run(state: CoordinatorState, shutdown: CancellationToken) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut rejoining = false;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&state, rejoining) => match outcome {
                Outcome::Ok => rejoining = false,
                Outcome::Transient => {}
                Outcome::Rejoin => {
                    *state.member_epoch.lock().await = 0;
                    state.owned_active.lock().await.clear();
                    rejoining = true;
                    let _ = state.events.send(StreamsEvent::Fenced);
                }
            },
        }
    }

    let leave = state.client.send(StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: -1,
        ..Default::default()
    });
    let _ = tokio::time::timeout(Duration::from_secs(5), leave).await;
}

async fn heartbeat_once(state: &CoordinatorState, rejoining: bool) -> Outcome {
    let epoch = *state.member_epoch.lock().await;
    let owned = state.owned_active.lock().await.clone();
    // On a fresh (re)join, resend the topology; otherwise null (unchanged).
    let topology = if rejoining || epoch == 0 {
        Some(state.topology.to_wire())
    } else {
        None
    };
    let active_tasks = if owned.is_empty() {
        None
    } else {
        Some(owned.iter().map(resp_to_req).collect())
    };

    let req = StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: epoch,
        process_id: Some(state.process_id.clone()),
        instance_id: state.instance_id.clone(),
        rebalance_timeout_ms: state.rebalance_timeout_ms,
        topology,
        active_tasks,
        ..Default::default()
    };

    match state.client.send(req).await {
        Ok(r) if r.error_code == 0 => {
            *state.member_epoch.lock().await = r.member_epoch;
            emit_response(state, &r).await;
            Outcome::Ok
        }
        Ok(r)
            if r.error_code == FENCED_MEMBER_EPOCH
                || r.error_code == UNKNOWN_MEMBER_ID
                || r.error_code == STALE_MEMBER_EPOCH =>
        {
            tracing::warn!(error_code = r.error_code, "streams heartbeat fenced; rejoining");
            Outcome::Rejoin
        }
        Ok(r) => {
            tracing::warn!(error_code = r.error_code, "unexpected streams heartbeat error");
            Outcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "streams heartbeat send failed");
            Outcome::Transient
        }
    }
}

/// Emit NotReady (status present) and/or Assigned (tasks present), and update
/// the owned-active set for the next echo.
async fn emit_response(
    state: &CoordinatorState,
    r: &crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
) {
    if let Some(statuses) = &r.status {
        if !statuses.is_empty() {
            let mapped = statuses.iter().map(map_status).collect();
            let _ = state.events.send(StreamsEvent::NotReady(mapped));
        }
    }
    let active_present = r.active_tasks.is_some();
    if active_present {
        if let Some(tasks) = &r.active_tasks {
            *state.owned_active.lock().await = tasks.clone();
        }
        let assignment = StreamsAssignment {
            active: resolve(&r.active_tasks, &state.topology),
            standby: resolve(&r.standby_tasks, &state.topology),
            warmup: resolve(&r.warmup_tasks, &state.topology),
        };
        let _ = state.events.send(StreamsEvent::Assigned(assignment));
    }
}

fn resp_to_req(t: &RespTaskIds) -> ReqTaskIds {
    ReqTaskIds { subtopology_id: t.subtopology_id.clone(), partitions: t.partitions.clone(), ..Default::default() }
}
```

- [ ] **Step 2: Verify it compiles + clippy clean**

Run: `cargo build -p crabka-client-streams`
Expected: compiles.
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/membership/coordinator.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): StreamsGroupHeartbeat coordinator loop"
```

---

## Task 9: Public StreamsMembership handle + builder

**Files:**
- Modify: `crates/client-streams/src/membership/client.rs`

- [ ] **Step 1: Implement the handle + builder**

`membership/client.rs`:
```rust
//! `StreamsMembership` — public handle for a KIP-1071 streams group.
//!
//! `start` generates a member id, sends the join heartbeat (`member_epoch = 0`
//! + topology), captures the broker's epoch / heartbeat interval / initial
//! assignment, then spawns the background heartbeat loop on its own connection
//! (the broker serves a connection serially). `next_event` drains coordinator
//! events; `close` leaves the group.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::streams_group_heartbeat_request::StreamsGroupHeartbeatRequest;

use super::coordinator::{self, CoordinatorState};
use super::status::map_status;
use super::types::{StreamsAssignment, StreamsEvent};
use crate::error::StreamsClientError;
use crate::membership::assignment::resolve;
use crate::topology::BuiltTopology;

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

/// A live streams-group membership. Construct via [`StreamsMembership::builder`].
pub struct StreamsMembership {
    member_id: String,
    group_id: String,
    events: mpsc::UnboundedReceiver<StreamsEvent>,
    shutdown: CancellationToken,
    hb_handle: Option<JoinHandle<()>>,
}

#[bon::bon]
impl StreamsMembership {
    /// Join a streams group and start heartbeating.
    #[builder(start_fn = builder, finish_fn = build)]
    pub async fn start(
        #[builder(into)] bootstrap: String,
        #[builder(into, default = "crabka-streams".to_string())] client_id: String,
        #[builder(into)] group_id: String,
        topology: BuiltTopology,
        #[builder(into)] process_id: Option<String>,
        #[builder(into)] instance_id: Option<String>,
        #[builder(default = Duration::from_secs(30))] rebalance_timeout: Duration,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, StreamsClientError> {
        if group_id.is_empty() {
            return Err(StreamsClientError::Server(0));
        }
        let process_id = process_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let member_id = uuid::Uuid::new_v4().to_string();
        let rebalance_timeout_ms = i32::try_from(rebalance_timeout.as_millis()).unwrap_or(30_000);

        let client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .maybe_security(security.clone())
            .build()
            .await?;

        // Join heartbeat: epoch 0 + topology. Retry COORDINATOR_LOAD_IN_PROGRESS.
        let topology = Arc::new(topology);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let join = loop {
            let resp = client
                .send(StreamsGroupHeartbeatRequest {
                    group_id: group_id.clone(),
                    member_id: member_id.clone(),
                    member_epoch: 0,
                    process_id: Some(process_id.clone()),
                    instance_id: instance_id.clone(),
                    rebalance_timeout_ms,
                    topology: Some(topology.to_wire()),
                    ..Default::default()
                })
                .await?;
            if resp.error_code == COORDINATOR_LOAD_IN_PROGRESS {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
            break map_error(resp)?;
        };

        let member_epoch_val = join.member_epoch;
        let hb_interval = if join.heartbeat_interval_ms > 0 {
            Duration::from_millis(u64::try_from(join.heartbeat_interval_ms).unwrap_or(3000))
        } else {
            Duration::from_secs(3)
        };

        // Emit the initial status/assignment from the join response.
        if let Some(statuses) = &join.status {
            if !statuses.is_empty() {
                let _ = events_tx.send(StreamsEvent::NotReady(statuses.iter().map(map_status).collect()));
            }
        }
        let owned_active = Arc::new(Mutex::new(join.active_tasks.clone().unwrap_or_default()));
        if join.active_tasks.is_some() {
            let _ = events_tx.send(StreamsEvent::Assigned(StreamsAssignment {
                active: resolve(&join.active_tasks, &topology),
                standby: resolve(&join.standby_tasks, &topology),
                warmup: resolve(&join.warmup_tasks, &topology),
            }));
        }

        // Heartbeat loop on its own connection.
        let coordinator_client = Client::builder()
            .bootstrap(&bootstrap)
            .client_id(client_id.clone())
            .maybe_security(security.clone())
            .build()
            .await?;
        let shutdown = CancellationToken::new();
        let state = CoordinatorState {
            client: coordinator_client,
            group_id: group_id.clone(),
            member_id: member_id.clone(),
            process_id,
            instance_id,
            rebalance_timeout_ms,
            topology: Arc::clone(&topology),
            member_epoch: Arc::new(Mutex::new(member_epoch_val)),
            owned_active,
            heartbeat_interval: hb_interval,
            events: events_tx,
        };
        let hb_handle = tokio::spawn(coordinator::run(state, shutdown.clone()));

        Ok(Self {
            member_id,
            group_id,
            events: events_rx,
            shutdown,
            hb_handle: Some(hb_handle),
        })
    }
}

impl StreamsMembership {
    /// The client-generated member id.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// The streams group id.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Await the next membership event (assignment / not-ready / fenced).
    /// Returns [`StreamsClientError::Closed`] once the heartbeat loop has ended.
    pub async fn next_event(&mut self) -> Result<StreamsEvent, StreamsClientError> {
        self.events.recv().await.ok_or(StreamsClientError::Closed)
    }

    /// Leave the group and stop heartbeating.
    pub async fn close(&mut self) -> Result<(), StreamsClientError> {
        self.shutdown.cancel();
        if let Some(h) = self.hb_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}

/// Map a join-response error code to a typed error (0 = ok).
fn map_error(
    resp: crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
) -> Result<
    crabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    StreamsClientError,
> {
    const STREAMS_INVALID_TOPOLOGY: i16 = 87; // STREAMS_INVALID_TOPOLOGY family
    const STREAMS_INVALID_TOPOLOGY_EPOCH: i16 = 88;
    const STREAMS_TOPOLOGY_FENCED: i16 = 89;
    const GROUP_AUTHORIZATION_FAILED: i16 = 30;
    const TOPIC_AUTHORIZATION_FAILED: i16 = 29;
    const GROUP_ID_NOT_FOUND: i16 = 69;
    match resp.error_code {
        0 => Ok(resp),
        c @ (STREAMS_INVALID_TOPOLOGY | STREAMS_INVALID_TOPOLOGY_EPOCH | STREAMS_TOPOLOGY_FENCED) => {
            Err(StreamsClientError::InvalidTopology {
                code: c,
                message: resp.error_message.unwrap_or_default(),
            })
        }
        c @ (GROUP_AUTHORIZATION_FAILED | TOPIC_AUTHORIZATION_FAILED) => {
            Err(StreamsClientError::Authorization(c))
        }
        GROUP_ID_NOT_FOUND => Err(StreamsClientError::GroupIdNotFound),
        other => Err(StreamsClientError::Server(other)),
    }
}
```

> **Plan note — verify these error-code constants against the broker before relying on them.** The exact `i16` values for `STREAMS_INVALID_TOPOLOGY` / `_EPOCH` / `_TOPOLOGY_FENCED`, `GROUP_ID_NOT_FOUND`, and the authorization codes must match `crates/protocol`'s error-code table (search `crates/protocol/src` / `crates/broker/src` for `STREAMS_INVALID_TOPOLOGY`, `GROUP_ID_NOT_FOUND`, `GROUP_AUTHORIZATION_FAILED`). Fix the constants to the real values in this step; the placeholders above are illustrative.

- [ ] **Step 2: Verify build + the whole lib + clippy**

Run: `cargo build -p crabka-client-streams`
Expected: compiles.
Run: `cargo test -p crabka-client-streams --lib`
Expected: PASS (all unit tests so far).
Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/membership/client.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "feat(streams-client): StreamsMembership handle + builder"
```

---

## Task 10: In-process broker integration test

**Files:**
- Create: `crates/client-streams/tests/integration.rs`

Copy the broker-boot + `finalize_streams_version` + `create_topic` helpers from `crates/broker/tests/streams_groups.rs` (the canonical reference).

- [ ] **Step 1: Write the integration test**

`crates/client-streams/tests/integration.rs`:
```rust
//! In-process broker: a streams member joins, converges to an assignment, and
//! leaves cleanly. Requires `streams.version` finalized + source topic created.
#![cfg(not(target_os = "windows"))]

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_client_streams::{StreamsEvent, StreamsMembership, Topology};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};

async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert_eq!(resp.error_code, 0, "streams.version finalize failed: {resp:?}");
}

async fn create_topic(client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 0, "topic create failed: {resp:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn member_joins_converges_and_leaves() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let admin = Client::builder().bootstrap(&bootstrap).client_id("admin").build().await.unwrap();
    finalize_streams_version(&admin).await;
    create_topic(&admin, "streams-input", 2).await;

    let mut topo = Topology::new();
    topo.add_source("src", ["streams-input"]);
    topo.add_sink("snk", "streams-output", ["src"]);
    let built = topo.build("streams-app").unwrap();

    let mut membership = StreamsMembership::builder()
        .bootstrap(&bootstrap)
        .group_id("streams-app")
        .topology(built)
        .rebalance_timeout(Duration::from_secs(30))
        .build()
        .await
        .expect("join");

    // Drive events until we see an Assigned with both partitions of subtopology 0.
    let assigned = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match membership.next_event().await.expect("event") {
                StreamsEvent::Assigned(a) => {
                    let active_parts: usize = a.active.iter().map(|t| t.partitions.len()).sum();
                    if active_parts >= 2 {
                        break a;
                    }
                }
                StreamsEvent::NotReady(_) | StreamsEvent::Fenced => {}
            }
        }
    })
    .await
    .expect("converged to an assignment");

    assert_eq!(assigned.active[0].subtopology_id, "0");
    let topics: Vec<&str> = assigned.active[0]
        .source_topic_partitions
        .iter()
        .map(|tp| tp.topic.as_str())
        .collect();
    assert!(topics.iter().all(|t| *t == "streams-input"));

    membership.close().await.expect("close");
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_source_topic_reports_not_ready() {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let admin = Client::builder().bootstrap(&bootstrap).client_id("admin").build().await.unwrap();
    finalize_streams_version(&admin).await;
    // Deliberately do NOT create "streams-missing".

    let mut topo = Topology::new();
    topo.add_source("src", ["streams-missing"]);
    topo.add_sink("snk", "out", ["src"]);
    let built = topo.build("streams-missing-app").unwrap();

    let mut membership = StreamsMembership::builder()
        .bootstrap(&bootstrap)
        .group_id("streams-missing-app")
        .topology(built)
        .build()
        .await
        .expect("join");

    let saw_not_ready = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let StreamsEvent::NotReady(statuses) = membership.next_event().await.expect("event") {
                if !statuses.is_empty() {
                    break true;
                }
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(saw_not_ready, "expected a NotReady status for the missing source topic");

    membership.close().await.expect("close");
    broker.shutdown().await;
}
```

> **Plan note:** if `crabka-protocol` isn't already a dev-dependency, add `crabka-protocol = { version = "0.2", path = "../protocol", default-features = false }` and `crabka-client-core = { version = "0.2", path = "../client-core" }` under `[dev-dependencies]` in `crates/client-streams/Cargo.toml` (Task 1 added the deps as normal dependencies; `crabka-client-core` is already a normal dep, but `crabka-protocol` test usage needs the `create_topics_request`/`update_features_request` owned modules — confirm they're exported and adjust the dev-dep if needed).

- [ ] **Step 2: Run the integration test**

Run: `cargo test -p crabka-client-streams --test integration -- --nocapture`
Expected: PASS (2 tests). If `member_joins_converges_and_leaves` flakes on the first heartbeat with code 14, the in-`start` retry handles it; if it times out, check that `finalize_streams_version` ran and the source topic exists.

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/tests/integration.rs crates/client-streams/Cargo.toml
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-client): in-process broker join/converge/leave integration"
```

---

## Task 11: Golden-frame interop harness + fixture

**Files:**
- Create: `crates/client-streams/tests/golden_frame.rs`
- Create: `crates/client-streams/tests/testdata/golden/README.md`
- Create: `crates/client-streams/tests/testdata/golden/single_source_sink.topology.json`

This is the byte-exact JVM interop gate. The fixture is the **expected** wire `Topology` for a canonical Processor-API topology, captured from a JVM Kafka Streams 4.x app. We compare our encoder's struct output to it.

- [ ] **Step 1: Document the capture procedure**

`crates/client-streams/tests/testdata/golden/README.md`:
```markdown
# Golden frames — JVM StreamsGroupHeartbeat.Topology fixtures

Each `<name>.topology.json` is the expected wire `Topology` (field-for-field)
that the JVM Kafka Streams 4.x **Processor API** client emits for the named
topology, used to gate byte-exact interop of the Rust encoder.

## Capture procedure (JVM 4.x, Processor API)

1. Write a minimal Java app using `org.apache.kafka.streams.Topology` (PAPI:
   `addSource` / `addProcessor` / `addSink` / `addStateStore`) matching the
   Rust builder calls in the corresponding test.
2. Configure `group.protocol=streams` (KIP-1071) and point it at any broker.
3. Capture the first `StreamsGroupHeartbeatRequest` (apiKey 88). Easiest:
   point it at the Crabka broker and enable request-byte logging, or attach a
   debugger to `StreamsGroupHeartbeatRequestManager.buildRequestData()` and dump
   the `Topology`.
4. Serialize the captured `Topology` to the JSON shape in
   `single_source_sink.topology.json` (subtopology ids as strings, topic arrays
   in the exact order the JVM emitted). Commit it.

Until a fixture is captured from a real JVM run, the corresponding test asserts
against the hand-derived expectation below AND is annotated so a JVM-captured
fixture can replace it without changing the test.
```

- [ ] **Step 2: Add the canonical fixture (hand-derived from §4 rules; replace with a real JVM capture when available)**

`crates/client-streams/tests/testdata/golden/single_source_sink.topology.json`:
```json
{
  "epoch": 0,
  "subtopologies": [
    {
      "subtopology_id": "0",
      "source_topics": ["streams-input"],
      "source_topic_regex": [],
      "repartition_sink_topics": [],
      "repartition_source_topics": [],
      "state_changelog_topics": [],
      "copartition_groups": []
    }
  ]
}
```

- [ ] **Step 3: Write the golden-frame test**

`crates/client-streams/tests/golden_frame.rs`:
```rust
//! Byte-exact interop gate: the encoder's wire `Topology` for a canonical
//! Processor-API topology must match the JVM 4.x fixture.
#![cfg(not(target_os = "windows"))]

use crabka_client_streams::Topology;

#[test]
fn single_source_sink_matches_jvm_fixture() {
    // The Rust topology MUST mirror the Java PAPI app the fixture was captured from.
    let mut topo = Topology::new();
    topo.add_source("src", ["streams-input"]);
    topo.add_sink("snk", "streams-output", ["src"]);
    let wire = topo.build("streams-app").unwrap().to_wire();

    // Assert the JVM-derived shape (mirrors single_source_sink.topology.json).
    assert_eq!(wire.epoch, 0);
    assert_eq!(wire.subtopologies.len(), 1);
    let s = &wire.subtopologies[0];
    assert_eq!(s.subtopology_id, "0");
    assert_eq!(s.source_topics, vec!["streams-input".to_string()]);
    assert!(s.source_topic_regex.is_empty());
    assert!(s.repartition_sink_topics.is_empty());
    assert!(s.repartition_source_topics.is_empty());
    assert!(s.state_changelog_topics.is_empty());
    assert!(s.copartition_groups.is_empty());
}
```

> **Plan note — strengthen to true byte-comparison when a real capture lands.** Replace the field asserts with: load the captured JVM `Topology` bytes from `testdata/golden/single_source_sink.topology.bin`, decode via `crabka_protocol`'s `Decode`, and `assert_eq!(decoded, wire)` (the structs derive `PartialEq`) AND encode both and compare bytes via `Topology`'s `Encode`. The hand-derived JSON fixture is the interim gate; capturing the real `.bin` from a JVM 4.x PAPI run is the follow-on validation milestone (also covered by the mixed JVM+Crabka group test deferred from the spec).

- [ ] **Step 4: Run the golden-frame test**

Run: `cargo test -p crabka-client-streams --test golden_frame`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add crates/client-streams/tests/golden_frame.rs crates/client-streams/tests/testdata
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(streams-client): golden-frame interop harness + canonical fixture"
```

---

## Task 12: Crate doc example + final verification

**Files:**
- Modify: `crates/client-streams/src/lib.rs`

- [ ] **Step 1: Add a runnable doc example**

Insert into the `lib.rs` crate docs (after the summary paragraph):
```rust
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_client_streams::{StreamsEvent, StreamsMembership, Topology};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut topo = Topology::new();
//! topo.add_source("src", ["input-topic"]);
//! topo.add_sink("snk", "output-topic", ["src"]);
//! let built = topo.build("my-application-id")?;
//!
//! let mut membership = StreamsMembership::builder()
//!     .bootstrap("localhost:9092")
//!     .group_id("my-application-id")
//!     .topology(built)
//!     .build()
//!     .await?;
//!
//! loop {
//!     match membership.next_event().await? {
//!         StreamsEvent::Assigned(a) => {
//!             for task in &a.active {
//!                 println!("active task {} → {:?}", task.subtopology_id, task.source_topic_partitions);
//!             }
//!         }
//!         StreamsEvent::NotReady(statuses) => println!("not ready: {statuses:?}"),
//!         StreamsEvent::Fenced => println!("rejoined after fence"),
//!     }
//! }
//! # }
//! ```
```

- [ ] **Step 2: Final verification — full test + fmt + clippy**

Run: `cargo test -p crabka-client-streams`
Expected: PASS (all unit + integration + golden-frame + doctest).

Run: `cargo fmt -p crabka-client-streams -- --check`
Expected: no diff. (If it complains, run `cargo fmt -p crabka-client-streams` and re-commit.)

Run: `cargo clippy -p crabka-client-streams --all-targets -- -D warnings`
Expected: clean.

Run: `cargo build --workspace` (confirm the new crate doesn't break the workspace)
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/client-streams/src/lib.rs
git -c user.name="Matt Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs(streams-client): quick-start example + final verification"
```

---

## Self-review

**Spec coverage:**
- §5 crate/modules/API → Tasks 1, 5, 9 (crate, builder, handle). ✓
- §4/§6 byte-exact derivation (epoch 0, string-sort ids, makeNodeGroups, sorting, copartition indices, naming, partitions) → Tasks 3, 4 (+ unit tests for each rule including the string-sort gotcha and copartition indices). ✓
- §7 lifecycle (join/heartbeat/reconcile adopt-and-echo/fence-rejoin/leave) → Tasks 8, 9. ✓
- §8 assignment resolution → Task 7. ✓
- §9 status + errors → Tasks 6 (status), 9 (error mapping). ✓
- §10 concurrency (Arc<Mutex>, mpsc events, separate connection) → Tasks 8, 9. ✓
- §11 protocol wiring → already exists (verified); no task needed beyond `Client::send`. ✓
- §12 testing (unit, golden-frame, in-process integration, mixed-group flagged) → Tasks 3/4/6/7 (unit), 11 (golden), 10 (integration); mixed-group flagged as follow-on in Task 11. ✓
- §13 open points: reconciliation-ack (adopt-and-echo, Task 8); application_id=group_id (Task 9 uses group_id as application id in the integration test; the builder takes application_id explicitly). ✓

**Placeholder scan:** Two deliberate **Plan notes** flag values to verify against the codebase (error-code constants in Task 9; byte-comparison upgrade in Task 11) rather than leaving silent placeholders — each gives the exact search to run. The `reg_add_source` stub in Task 5 is explicitly called out for deletion with the correct inline replacement shown. No silent TODOs.

**Type consistency:** `BuiltTopology::to_wire`/`source_topics_for` (Task 5) are consumed identically in Tasks 7, 8, 9. `StreamsEvent`/`StreamsAssignment`/`TaskAssignment`/`StreamsStatus` (Task 6) are produced in Tasks 8, 9 and consumed in Task 10 with matching field names. `resolve(&Option<Vec<TaskIds>>, &BuiltTopology)` signature (Task 7) matches its call sites (Tasks 8, 9). `CoordinatorState` fields (Task 8) match construction in Task 9. ✓

**Known risk carried forward:** the genuinely hard interop part (JVM node-insertion-order matching for complex topologies) is bounded here to Processor-API topologies and gated by Task 11; the real JVM `.bin` capture + mixed-group test are the explicit follow-on validation milestone.
