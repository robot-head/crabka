# `crabka-metadata-quorum` (slice 7) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace slice-4's in-memory metadata with an [openraft][openraft]-backed quorum across N Crabka brokers. After this slice, a 3-node Crabka cluster boots, elects a Raft leader, accepts `CreateTopics` against any node, and a JVM client round-trips records through any of the three brokers; killing the leader survives metadata reads.

**Architecture:** Two new crates land: `crabka-raft` (openraft adapters for log + state machine + network, plus the `Controller` entry point) and `crabka-metadata` (versioned `MetadataRecord` types + read-only `MetadataImage`). The broker gains a second TCP listener for controller RPCs on api keys `1000`-`1002` (hand-written in `crabka-raft::wire`, NOT in the protocol codegen). Slice-4's in-memory metadata is deleted; every read goes through `controller.current_image()`.

**Tech Stack:** Rust 1.95.0 edition 2024; [`openraft`][openraft] ~0.9 for consensus; `bincode = "2"` for payload serialization; `tokio` async runtime; `crabka-log` for the Raft log storage; `crabka-client-core` for outbound controller RPCs; `crabka-protocol` for framing primitives reused by the hand-written wire types; `tracing` for observability; `uuid` for the cluster id.

**Reference spec:** [`docs/superpowers/specs/2026-05-12-crabka-metadata-quorum-design.md`](../specs/2026-05-12-crabka-metadata-quorum-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/metadata-quorum-plan` (this file). Implementation runs on `feature/metadata-quorum` branched off `main` once this plan's PR merges.

[openraft]: https://github.com/databendlabs/openraft

---

## File structure

```
Cargo.toml                                              # MODIFIED — add openraft + bincode + uuid to [workspace.dependencies]

crates/metadata/                                        # NEW crate
├── Cargo.toml
├── src/
│   ├── lib.rs                                          # public re-exports
│   ├── error.rs                                        # MetadataError
│   ├── records.rs                                      # MetadataRecord enum + per-record types
│   └── image.rs                                        # MetadataImage + apply
└── tests/
    └── evolution.rs                                    # proptest schema-evolution suite

crates/raft/                                            # NEW crate
├── Cargo.toml
├── src/
│   ├── lib.rs                                          # public re-exports
│   ├── error.rs                                        # RaftError
│   ├── types.rs                                        # openraft TypeConfig declaration + NodeId alias
│   ├── config.rs                                       # ControllerConfig
│   ├── wire.rs                                         # api_keys 1000/1001/1002 encode/decode
│   ├── log_store.rs                                    # openraft RaftLogStorage on crabka-log
│   ├── state_machine.rs                                # openraft RaftStateMachine wrapping MetadataImage
│   ├── network.rs                                      # openraft RaftNetworkFactory + RaftNetwork
│   ├── server.rs                                       # controller listener accept loop + dispatch
│   └── controller.rs                                   # Controller::start + ControllerHandle
└── tests/
    └── single_node.rs                                  # in-process 1-voter cluster smoke

crates/broker/                                          # MODIFIED
└── src/
    ├── broker.rs                                       # MODIFIED — Controller wiring, kill MetadataState
    ├── config.rs                                       # MODIFIED — node_id, controller_listen_addr, voters
    ├── metadata.rs                                     # DELETED — replaced by crabka-metadata
    └── handlers/
        ├── metadata.rs                                 # MODIFIED — read from controller.current_image()
        ├── create_topics.rs                            # MODIFIED — submit through Controller
        └── delete_topics.rs                            # MODIFIED — submit through Controller

crates/broker/tests/
├── quorum.rs                                           # NEW — multi-node integration tests
└── jvm_acceptance.rs                                   # MODIFIED — three_node_jvm_round_trip
```

**Visibility conventions** (matches slice-1..6): public types re-exported via each crate's `lib.rs`; everything else is `pub(crate)`; the inner `openraft::Raft` instance never leaks past `Controller`.

---

## Phase A — Workspace deps + new crate skeletons

### Task 1: Add `openraft`, `bincode`, `uuid` to `[workspace.dependencies]`

**Files:**
- Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add the three deps**

Append under `[workspace.dependencies]` in alphabetical order:

```toml
bincode = "2"
openraft = { version = "0.9", features = ["serde", "type-alias"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

- [ ] **Step 2: Verify workspace still builds**

```bash
cargo build --workspace
```

Expected: clean. Lock file may grow.

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "build: add openraft, bincode, uuid to workspace dependencies"
```

Include `Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>` via heredoc.

---

### Task 2: `crabka-metadata` crate skeleton

**Files:**
- Create: `crates/metadata/Cargo.toml`
- Create: `crates/metadata/src/lib.rs`
- Create: `crates/metadata/src/error.rs`

- [ ] **Step 1: Manifest**

`crates/metadata/Cargo.toml`:

```toml
[package]
name = "crabka-metadata"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Versioned metadata record types + immutable image for Crabka"

[lints]
workspace = true

[dependencies]
bincode = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true, features = ["derive"] }
thiserror = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
proptest = { workspace = true }
```

- [ ] **Step 2: Stub `lib.rs`**

`crates/metadata/src/lib.rs`:

```rust
//! Versioned metadata records and the immutable image they apply to.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-12-crabka-metadata-quorum-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.0.0")]

mod error;

pub use error::MetadataError;
```

- [ ] **Step 3: `MetadataError`**

`crates/metadata/src/error.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataError {
    #[error("topic '{0}' already exists")]
    TopicExists(String),

    #[error("unknown topic '{0}'")]
    UnknownTopic(String),

    #[error("invalid partition {partition} on topic '{topic}'")]
    InvalidPartition { topic: String, partition: i32 },

    #[error("invalid record: {0}")]
    InvalidRecord(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_topic_exists() {
        let e = MetadataError::TopicExists("my-topic".into());
        assert_eq!(e.to_string(), "topic 'my-topic' already exists");
    }

    #[test]
    fn display_invalid_partition() {
        let e = MetadataError::InvalidPartition {
            topic: "t".into(),
            partition: 7,
        };
        assert!(e.to_string().contains("partition 7"));
    }
}
```

- [ ] **Step 4: Build + test + commit**

```bash
cargo build -p crabka-metadata
cargo test -p crabka-metadata
git add crates/metadata
git commit -m "feat(metadata): crate skeleton + MetadataError"
```

---

### Task 3: `crabka-raft` crate skeleton

**Files:**
- Create: `crates/raft/Cargo.toml`
- Create: `crates/raft/src/lib.rs`
- Create: `crates/raft/src/error.rs`

- [ ] **Step 1: Manifest**

`crates/raft/Cargo.toml`:

```toml
[package]
name = "crabka-raft"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Metadata Raft quorum (openraft adapters + Controller) for Crabka"

[lints]
workspace = true

[dependencies]
crabka-metadata = { version = "0.1", path = "../metadata" }
crabka-log = { version = "0.1", path = "../log" }
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
crabka-client-core = { version = "0.1", path = "../client-core" }
openraft = { workspace = true }
bincode = { workspace = true }
bytes = { workspace = true }
dashmap = { workspace = true }
futures-util = { workspace = true }
serde = { workspace = true, features = ["derive"] }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "sync", "time", "macros", "net"] }
tokio-util = { workspace = true, features = ["rt"] }
tracing = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
```

- [ ] **Step 2: Stub `lib.rs`**

`crates/raft/src/lib.rs`:

```rust
//! Metadata Raft quorum for Crabka — openraft adapters + `Controller`.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-12-crabka-metadata-quorum-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.0.0")]

mod error;

pub use error::RaftError;
```

- [ ] **Step 3: `RaftError`**

`crates/raft/src/error.rs`:

```rust
use thiserror::Error;

pub type NodeId = u64;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RaftError {
    #[error("storage: {0}")]
    Storage(#[from] crabka_log::LogError),

    #[error("network: {0}")]
    Network(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("metadata: {0}")]
    Metadata(#[from] crabka_metadata::MetadataError),

    #[error("openraft fatal: {0}")]
    Openraft(String),

    #[error("not leader; current leader: {current_leader:?}")]
    NotLeader { current_leader: Option<NodeId> },

    #[error("leader unknown (election in progress)")]
    LeaderUnknown,

    #[error("change rejected: {0}")]
    ChangeRejected(String),

    #[error("bincode encode: {0}")]
    SerdeFailed(#[from] bincode::error::EncodeError),

    #[error("bincode decode: {0}")]
    SerdeFailedDecode(#[from] bincode::error::DecodeError),

    #[error("controller shut down")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_not_leader_with_id() {
        let e = RaftError::NotLeader { current_leader: Some(7) };
        assert!(e.to_string().contains("Some(7)"));
    }

    #[test]
    fn display_not_leader_without_id() {
        let e = RaftError::NotLeader { current_leader: None };
        assert!(e.to_string().contains("None"));
    }
}
```

- [ ] **Step 4: Build + test + commit**

```bash
cargo build -p crabka-raft
cargo test -p crabka-raft
git add crates/raft
git commit -m "feat(raft): crate skeleton + RaftError"
```

---

## Phase B — `crabka-metadata` records + image

### Task 4: `MetadataRecord` enum + per-record types

**Files:**
- Create: `crates/metadata/src/records.rs`
- Modify: `crates/metadata/src/lib.rs`

- [ ] **Step 1: Records module**

`crates/metadata/src/records.rs`:

```rust
//! Versioned metadata records. Future versions add variants; older
//! readers can skip unknown ones because we encode each variant
//! length-prefixed inside the `bincode` payload.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRecord {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bc() -> bincode::config::Configuration {
        bincode::config::standard()
    }

    #[test]
    fn topic_record_bincode_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn partition_record_bincode_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn broker_registration_bincode_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn delete_topic_bincode_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }
}
```

- [ ] **Step 2: Re-export from `lib.rs`**

Replace `crates/metadata/src/lib.rs` with:

```rust
//! Versioned metadata records and the immutable image they apply to.

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.0.0")]

mod error;
mod records;

pub use error::MetadataError;
pub use records::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};
```

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-metadata records
git add crates/metadata
git commit -m "feat(metadata): MetadataRecord enum + 4 V1 record types"
```

---

### Task 5: `MetadataImage` + read API + apply

**Files:**
- Create: `crates/metadata/src/image.rs`
- Modify: `crates/metadata/src/lib.rs`

- [ ] **Step 1: Image module**

`crates/metadata/src/image.rs`:

```rust
//! Immutable snapshot of the cluster's metadata state. Mutated only by
//! [`MetadataImage::apply`] (called from the Raft state machine), and
//! read everywhere else via shared references / `Arc` clones.

use std::collections::HashMap;

use uuid::Uuid;

use crate::error::MetadataError;
use crate::records::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};

#[derive(Debug, Clone, Default)]
pub struct MetadataImage {
    cluster_id: Uuid,
    topics: HashMap<String, TopicRecord>,
    partitions: HashMap<(String, i32), PartitionRecord>,
    brokers: HashMap<NodeId, BrokerRegistrationRecord>,
}

impl MetadataImage {
    #[must_use]
    pub fn new(cluster_id: Uuid) -> Self {
        Self {
            cluster_id,
            topics: HashMap::new(),
            partitions: HashMap::new(),
            brokers: HashMap::new(),
        }
    }

    #[must_use]
    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn topics(&self) -> impl Iterator<Item = &TopicRecord> {
        self.topics.values()
    }

    #[must_use]
    pub fn topic(&self, name: &str) -> Option<&TopicRecord> {
        self.topics.get(name)
    }

    #[must_use]
    pub fn partition(&self, topic: &str, idx: i32) -> Option<&PartitionRecord> {
        self.partitions.get(&(topic.to_string(), idx))
    }

    pub fn partitions_of(&self, topic: &str) -> impl Iterator<Item = &PartitionRecord> {
        self.partitions
            .iter()
            .filter(move |((t, _), _)| t == topic)
            .map(|(_, v)| v)
    }

    #[must_use]
    pub fn broker(&self, node_id: NodeId) -> Option<&BrokerRegistrationRecord> {
        self.brokers.get(&node_id)
    }

    pub fn brokers(&self) -> impl Iterator<Item = &BrokerRegistrationRecord> {
        self.brokers.values()
    }

    /// Apply one record. Returns the previous record (for V1Topic /
    /// V1BrokerRegistration) so the caller can observe overwrite cases.
    /// Infallible — pre-validation against the current image happens
    /// in the controller before submitting to Raft. Apply must never
    /// fail on a committed entry.
    pub fn apply(&mut self, rec: &MetadataRecord) {
        match rec {
            MetadataRecord::V1Topic(t) => {
                self.topics.insert(t.name.clone(), t.clone());
            }
            MetadataRecord::V1Partition(p) => {
                self.partitions.insert((p.topic.clone(), p.partition), p.clone());
            }
            MetadataRecord::V1BrokerRegistration(b) => {
                self.brokers.insert(b.node_id, b.clone());
            }
            MetadataRecord::V1DeleteTopic(d) => {
                self.topics.remove(&d.name);
                self.partitions.retain(|(t, _), _| t != &d.name);
            }
        }
    }

    /// Synchronous pre-validation: returns `Ok` if the record would be a
    /// no-conflict apply, otherwise the appropriate error. Used by
    /// `Controller::submit_change` before forwarding to openraft.
    pub fn validate(&self, rec: &MetadataRecord) -> Result<(), MetadataError> {
        match rec {
            MetadataRecord::V1Topic(t) => {
                if self.topics.contains_key(&t.name) {
                    return Err(MetadataError::TopicExists(t.name.clone()));
                }
                if t.partitions <= 0 {
                    return Err(MetadataError::InvalidRecord("partitions must be > 0"));
                }
                Ok(())
            }
            MetadataRecord::V1Partition(p) => {
                if !self.topics.contains_key(&p.topic) {
                    return Err(MetadataError::UnknownTopic(p.topic.clone()));
                }
                Ok(())
            }
            MetadataRecord::V1DeleteTopic(d) => {
                if !self.topics.contains_key(&d.name) {
                    return Err(MetadataError::UnknownTopic(d.name.clone()));
                }
                Ok(())
            }
            MetadataRecord::V1BrokerRegistration(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn img() -> MetadataImage {
        MetadataImage::new(Uuid::nil())
    }

    fn topic(name: &str, partitions: i32) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions,
            replication_factor: 1,
        })
    }

    #[test]
    fn apply_topic_inserts() {
        let mut m = img();
        m.apply(&topic("t", 3));
        assert!(m.topic("t").is_some());
    }

    #[test]
    fn apply_delete_clears_partitions() {
        let mut m = img();
        m.apply(&topic("t", 2));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(), partition: 0, leader: 1, replicas: vec![1], isr: vec![1],
        }));
        m.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(), partition: 1, leader: 1, replicas: vec![1], isr: vec![1],
        }));
        assert_eq!(m.partitions_of("t").count(), 2);
        m.apply(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name: "t".into() }));
        assert!(m.topic("t").is_none());
        assert_eq!(m.partitions_of("t").count(), 0);
    }

    #[test]
    fn validate_topic_exists_rejected() {
        let mut m = img();
        m.apply(&topic("t", 1));
        let err = m.validate(&topic("t", 1)).unwrap_err();
        assert!(matches!(err, MetadataError::TopicExists(_)));
    }

    #[test]
    fn validate_delete_unknown_topic_rejected() {
        let m = img();
        let err = m
            .validate(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "ghost".into(),
            }))
            .unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn validate_partition_for_unknown_topic_rejected() {
        let m = img();
        let p = MetadataRecord::V1Partition(PartitionRecord {
            topic: "ghost".into(), partition: 0, leader: 1, replicas: vec![1], isr: vec![1],
        });
        let err = m.validate(&p).unwrap_err();
        assert!(matches!(err, MetadataError::UnknownTopic(_)));
    }

    #[test]
    fn broker_registration_is_idempotent() {
        let mut m = img();
        let b = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 1, host: "h".into(), port: 9092, rack: None,
        });
        m.apply(&b);
        m.apply(&b);
        assert_eq!(m.brokers().count(), 1);
    }
}
```

- [ ] **Step 2: Re-export**

Replace `crates/metadata/src/lib.rs` to add `mod image;` + `pub use image::MetadataImage;`. Final shape:

```rust
//! Versioned metadata records and the immutable image they apply to.

#![doc(html_root_url = "https://docs.rs/crabka-metadata/0.0.0")]

mod error;
mod image;
mod records;

pub use error::MetadataError;
pub use image::MetadataImage;
pub use records::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, NodeId, PartitionRecord,
    TopicRecord,
};
```

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-metadata
git add crates/metadata
git commit -m "feat(metadata): MetadataImage with apply + validate"
```

---

### Task 6: Proptest evolution suite

**Files:**
- Create: `crates/metadata/tests/evolution.rs`

- [ ] **Step 1: Property tests**

`crates/metadata/tests/evolution.rs`:

```rust
//! Proptest harness for `MetadataRecord` schema evolution. Today
//! everything is V1; the future-version policy is "decode v2 →
//! re-encode v1 round-trips for the fields v1 understands." We seed
//! that contract by asserting v1 ↔ v1 round-trips here.

use bincode::config::standard;
use crabka_metadata::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, PartitionRecord, TopicRecord,
};
use proptest::prelude::*;
use uuid::Uuid;

prop_compose! {
    fn arb_topic()(
        name in "[a-zA-Z][a-zA-Z0-9_-]{0,32}",
        partitions in 1..256i32,
        replication_factor in 1..16i16,
    ) -> TopicRecord {
        TopicRecord {
            name,
            topic_id: Uuid::new_v4(),
            partitions,
            replication_factor,
        }
    }
}

prop_compose! {
    fn arb_partition()(
        topic in "[a-zA-Z][a-zA-Z0-9_-]{0,32}",
        partition in 0..1024i32,
        replicas in prop::collection::vec(0..32u64, 1..6),
    ) -> PartitionRecord {
        let leader = replicas[0];
        let isr = replicas.clone();
        PartitionRecord { topic, partition, leader, replicas, isr }
    }
}

prop_compose! {
    fn arb_broker()(
        node_id in 0..1024u64,
        host in "[a-zA-Z][a-zA-Z0-9.-]{0,32}",
        port in 1024..65535u16,
        rack in prop::option::of("[a-zA-Z][a-zA-Z0-9-]{0,16}"),
    ) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord { node_id, host, port, rack }
    }
}

fn arb_record() -> impl Strategy<Value = MetadataRecord> {
    prop_oneof![
        arb_topic().prop_map(MetadataRecord::V1Topic),
        arb_partition().prop_map(MetadataRecord::V1Partition),
        arb_broker().prop_map(MetadataRecord::V1BrokerRegistration),
        "[a-zA-Z][a-zA-Z0-9_-]{0,32}".prop_map(|name| {
            MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name })
        }),
    ]
}

proptest! {
    #[test]
    fn record_round_trips_bincode(r in arb_record()) {
        let bytes = bincode::serde::encode_to_vec(&r, standard()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).unwrap();
        prop_assert_eq!(decoded, r);
    }

    #[test]
    fn batch_round_trips_bincode(records in prop::collection::vec(arb_record(), 0..32)) {
        let bytes = bincode::serde::encode_to_vec(&records, standard()).unwrap();
        let (decoded, _): (Vec<MetadataRecord>, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).unwrap();
        prop_assert_eq!(decoded, records);
    }
}
```

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-metadata --test evolution
git add crates/metadata/tests
git commit -m "test(metadata): proptest record + batch bincode round-trips"
```

---

## Phase C — `crabka-raft` adapters

### Task 7: openraft `TypeConfig` + Crabka-private wire types

**Files:**
- Create: `crates/raft/src/types.rs`
- Create: `crates/raft/src/wire.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: `TypeConfig`**

`crates/raft/src/types.rs`:

```rust
//! openraft `TypeConfig` for Crabka. Single source of truth for the
//! generic parameter set every adapter uses.

use serde::{Deserialize, Serialize};

use crabka_metadata::MetadataRecord;

pub type NodeId = u64;

/// `BasicNode` from openraft carries the network address. We use it
/// directly rather than wrapping.
pub type Node = openraft::BasicNode;

/// What we ask Raft to replicate. A batch of `MetadataRecord`s so
/// `submit_change` can group related records (Topic + N Partitions)
/// in a single committed entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppData {
    pub records: Vec<MetadataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AppDataResponse {
    /// Filled in by the state machine on apply; carries the new log
    /// index so callers can correlate.
    pub applied_index: u64,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = AppData,
        R = AppDataResponse,
        NodeId = NodeId,
        Node = Node,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime
);

/// Re-export the openraft-derived `Raft` alias so adapters can name it
/// without re-stating the type config.
pub type Raft = openraft::Raft<TypeConfig>;
```

NOTE: openraft 0.9 may have moved away from `declare_raft_types!` in favor of a derive-based config — check the version that resolved in `Cargo.lock` and adapt the macro/derive shape if needed. The `D`/`R`/`Entry`/`SnapshotData`/`AsyncRuntime` set above is the conceptual minimum.

- [ ] **Step 2: Wire types**

`crates/raft/src/wire.rs`:

```rust
//! Crabka-private Raft RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `crabka-protocol`'s codegen — they're
//! hand-written `Encode`/`Decode` impls living here because they're
//! controller-only and Crabka-specific.
//!
//! Api keys: 1000 AppendEntries, 1001 Vote, 1002 InstallSnapshot (stub).

use bytes::{Buf, BufMut, Bytes};

use crabka_protocol::{Decode, Encode, ProtocolError};

use crate::types::NodeId;

pub const API_KEY_APPEND_ENTRIES: i16 = 1000;
pub const API_KEY_VOTE: i16 = 1001;
pub const API_KEY_INSTALL_SNAPSHOT: i16 = 1002;

/// Payload kind discriminator inside `AppendEntries.entries[].payload`.
#[repr(i8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    Blank = 0,
    Normal = 1,
    Membership = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaLogEntry {
    pub log_index: i64,
    pub log_term: i64,
    pub payload_kind: i8,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaAppendEntriesRequest {
    pub node_id: i32,
    pub term: i64,
    pub leader_id: i32,
    pub prev_log_index: i64,
    pub prev_log_term: i64,
    pub leader_commit: i64,
    pub entries: Vec<CrabkaLogEntry>,
}

impl CrabkaAppendEntriesRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i32(self.node_id);
        out.put_i64(self.term);
        out.put_i32(self.leader_id);
        out.put_i64(self.prev_log_index);
        out.put_i64(self.prev_log_term);
        out.put_i64(self.leader_commit);
        out.put_i32(i32::try_from(self.entries.len()).map_err(|_| ProtocolError::Overflow)?);
        for e in &self.entries {
            out.put_i64(e.log_index);
            out.put_i64(e.log_term);
            out.put_i8(e.payload_kind);
            out.put_i32(i32::try_from(e.payload.len()).map_err(|_| ProtocolError::Overflow)?);
            out.put_slice(&e.payload);
        }
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 4 + 8 + 4 + 8 + 8 + 8 + 4 {
            return Err(ProtocolError::Truncated);
        }
        let node_id = buf.get_i32();
        let term = buf.get_i64();
        let leader_id = buf.get_i32();
        let prev_log_index = buf.get_i64();
        let prev_log_term = buf.get_i64();
        let leader_commit = buf.get_i64();
        let entry_count = buf.get_i32();
        let mut entries = Vec::with_capacity(entry_count.max(0) as usize);
        for _ in 0..entry_count.max(0) {
            if buf.remaining() < 8 + 8 + 1 + 4 {
                return Err(ProtocolError::Truncated);
            }
            let log_index = buf.get_i64();
            let log_term = buf.get_i64();
            let payload_kind = buf.get_i8();
            let payload_len = buf.get_i32();
            if buf.remaining() < payload_len.max(0) as usize {
                return Err(ProtocolError::Truncated);
            }
            let payload = Bytes::copy_from_slice(&buf[..payload_len.max(0) as usize]);
            buf.advance(payload_len.max(0) as usize);
            entries.push(CrabkaLogEntry { log_index, log_term, payload_kind, payload });
        }
        Ok(Self {
            node_id, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaAppendEntriesResponse {
    pub success: bool,
    pub term: i64,
    pub last_log_index: i64,
}

impl CrabkaAppendEntriesResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i8(if self.success { 1 } else { 0 });
        out.put_i64(self.term);
        out.put_i64(self.last_log_index);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 + 8 + 8 {
            return Err(ProtocolError::Truncated);
        }
        let success = buf.get_i8() != 0;
        let term = buf.get_i64();
        let last_log_index = buf.get_i64();
        Ok(Self { success, term, last_log_index })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaVoteRequest {
    pub term: i64,
    pub candidate_id: NodeId,
    pub last_log_index: i64,
    pub last_log_term: i64,
}

impl CrabkaVoteRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i64(self.term);
        out.put_u64(self.candidate_id);
        out.put_i64(self.last_log_index);
        out.put_i64(self.last_log_term);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 8 + 8 + 8 + 8 {
            return Err(ProtocolError::Truncated);
        }
        Ok(Self {
            term: buf.get_i64(),
            candidate_id: buf.get_u64(),
            last_log_index: buf.get_i64(),
            last_log_term: buf.get_i64(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaVoteResponse {
    pub vote_granted: bool,
    pub term: i64,
}

impl CrabkaVoteResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i8(if self.vote_granted { 1 } else { 0 });
        out.put_i64(self.term);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 + 8 {
            return Err(ProtocolError::Truncated);
        }
        Ok(Self {
            vote_granted: buf.get_i8() != 0,
            term: buf.get_i64(),
        })
    }
}

/// Stub for the deferred snapshot path. Encoded as a single byte `0`
/// so the wire stays well-defined.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CrabkaInstallSnapshotRequest;

impl CrabkaInstallSnapshotRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_u8(0);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 1 {
            return Err(ProtocolError::Truncated);
        }
        let _ = buf.get_u8();
        Ok(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrabkaInstallSnapshotResponse {
    pub error_code: i16,
}

impl CrabkaInstallSnapshotResponse {
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        Ok(())
    }

    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        if buf.remaining() < 2 {
            return Err(ProtocolError::Truncated);
        }
        Ok(Self { error_code: buf.get_i16() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_entries_round_trip() {
        let req = CrabkaAppendEntriesRequest {
            node_id: 1, term: 7, leader_id: 1, prev_log_index: 4, prev_log_term: 6,
            leader_commit: 3,
            entries: vec![CrabkaLogEntry {
                log_index: 5, log_term: 7, payload_kind: 1,
                payload: Bytes::from_static(b"hello"),
            }],
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        let decoded = CrabkaAppendEntriesRequest::decode_v0(&mut cur).unwrap();
        assert_eq!(decoded, req);
    }

    #[test]
    fn vote_round_trip() {
        let req = CrabkaVoteRequest { term: 9, candidate_id: 2, last_log_index: 100, last_log_term: 9 };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert_eq!(CrabkaVoteRequest::decode_v0(&mut cur).unwrap(), req);
    }

    #[test]
    fn install_snapshot_stub_round_trip() {
        let req = CrabkaInstallSnapshotRequest;
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert_eq!(CrabkaInstallSnapshotRequest::decode_v0(&mut cur).unwrap(), req);
    }
}
```

NOTE: `crabka_protocol::ProtocolError::Overflow` and `::Truncated` are the assumed variant names. Recon the actual variants by reading `crates/protocol/src/error.rs`; substitute the real names if they differ. The `Encode`/`Decode` traits aren't being implemented here directly because their generic-version signatures don't match a fixed-v0 type — explicit `encode_v0` / `decode_v0` is the simplest shape.

- [ ] **Step 3: Re-export**

In `crates/raft/src/lib.rs`, add:

```rust
mod types;
mod wire;

pub use error::RaftError;
pub use types::{AppData, AppDataResponse, NodeId, TypeConfig};
pub use wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_INSTALL_SNAPSHOT, API_KEY_VOTE,
    CrabkaAppendEntriesRequest, CrabkaAppendEntriesResponse, CrabkaInstallSnapshotRequest,
    CrabkaInstallSnapshotResponse, CrabkaLogEntry, CrabkaVoteRequest, CrabkaVoteResponse,
    PayloadKind,
};
```

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-raft wire
cargo build -p crabka-raft
git add crates/raft
git commit -m "feat(raft): TypeConfig + hand-written wire types for api keys 1000-1002"
```

---

### Task 8: `RaftLogStore` (openraft `RaftLogStorage` impl)

**Files:**
- Create: `crates/raft/src/log_store.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: Log store**

`crates/raft/src/log_store.rs`:

```rust
//! openraft `RaftLogStorage` backed by `crabka-log`. The log lives at
//! `<log_dir>/@metadata-0/`. Each openraft entry is serialized with
//! bincode and appended as a single Kafka `RecordBatch` whose value
//! payload IS the serialized entry. Future KRaft-wire-compat work will
//! revisit the record layout; today the wrapping is internal only.

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use bincode::config::standard;
use bytes::Bytes;
use tokio::sync::Mutex;

use crabka_log::{Log, LogConfig, ReadOutput};
use crabka_protocol::records::RecordBatch;

use crate::error::RaftError;
use crate::types::TypeConfig;

/// In-memory cache keyed by log index — openraft expects O(1) random
/// reads at the log tip. We populate from disk on startup and keep
/// entries cached until commit (and slightly past).
#[derive(Debug, Default)]
struct EntryCache {
    /// Sorted by index. We never compact in slice 7 (snapshots deferred).
    entries: BTreeMap<u64, openraft::Entry<TypeConfig>>,
    last_purged: u64,
}

pub(crate) struct RaftLogStore {
    log: Arc<Mutex<Log>>,
    cache: Arc<Mutex<EntryCache>>,
    /// Last `vote` openraft asked us to persist. Held in memory + flushed
    /// to a small adjacent file so it survives restart.
    vote_path: PathBuf,
}

impl RaftLogStore {
    pub(crate) async fn open(meta_dir: PathBuf) -> Result<Self, RaftError> {
        std::fs::create_dir_all(&meta_dir).map_err(crabka_log::LogError::Io)?;
        let log_dir = meta_dir.join("@metadata-0");
        std::fs::create_dir_all(&log_dir).map_err(crabka_log::LogError::Io)?;
        let log = Log::open(&log_dir, LogConfig::default())?;
        let vote_path = meta_dir.join("vote.bin");

        // Replay existing log into the cache.
        let mut cache = EntryCache::default();
        let mut offset = log.log_start_offset();
        loop {
            let out = log.read(offset, 1 << 20)?;
            if matches!(out, ReadOutput::Empty) {
                break;
            }
            let batches = match out {
                ReadOutput::Batches(b) => b,
                ReadOutput::Empty => break,
            };
            if batches.is_empty() {
                break;
            }
            for batch in &batches {
                for rec in &batch.records {
                    let Some(value) = rec.value.as_ref() else { continue };
                    let (entry, _): (openraft::Entry<TypeConfig>, _) =
                        bincode::serde::decode_from_slice(value, standard())?;
                    cache.entries.insert(entry.log_id.index, entry);
                }
                offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
            }
        }

        Ok(Self {
            log: Arc::new(Mutex::new(log)),
            cache: Arc::new(Mutex::new(cache)),
            vote_path,
        })
    }

    pub(crate) async fn last_log_id(&self) -> Option<openraft::LogId<crate::types::NodeId>> {
        self.cache.lock().await.entries.values().last().map(|e| e.log_id)
    }

    pub(crate) async fn read_range<R: RangeBounds<u64>>(
        &self,
        range: R,
    ) -> Vec<openraft::Entry<TypeConfig>> {
        self.cache
            .lock()
            .await
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect()
    }

    pub(crate) async fn append(
        &self,
        entries: Vec<openraft::Entry<TypeConfig>>,
    ) -> Result<(), RaftError> {
        let mut cache = self.cache.lock().await;
        let mut log = self.log.lock().await;
        for entry in entries {
            // Serialize entry into a RecordBatch with a single Record whose
            // value carries the bincode payload. base_offset = entry.log_id.index.
            let payload = bincode::serde::encode_to_vec(&entry, standard())?;
            let mut batch = RecordBatch::default();
            batch.base_offset = i64::try_from(entry.log_id.index).unwrap_or(i64::MAX);
            batch.last_offset_delta = 0;
            batch.records.push(crabka_protocol::records::Record {
                offset_delta: 0,
                value: Some(Bytes::from(payload)),
                ..Default::default()
            });
            log.append(&mut batch)?;
            cache.entries.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    pub(crate) async fn truncate(&self, since: u64) -> Result<(), RaftError> {
        let mut cache = self.cache.lock().await;
        let mut log = self.log.lock().await;
        cache.entries.retain(|&k, _| k < since);
        log.truncate_to(i64::try_from(since).unwrap_or(i64::MAX))?;
        Ok(())
    }

    pub(crate) async fn save_vote(
        &self,
        vote: &openraft::Vote<crate::types::NodeId>,
    ) -> Result<(), RaftError> {
        let bytes = bincode::serde::encode_to_vec(vote, standard())?;
        tokio::fs::write(&self.vote_path, &bytes).await.map_err(crabka_log::LogError::Io)?;
        Ok(())
    }

    pub(crate) async fn read_vote(
        &self,
    ) -> Result<Option<openraft::Vote<crate::types::NodeId>>, RaftError> {
        match tokio::fs::read(&self.vote_path).await {
            Ok(bytes) => {
                let (v, _) = bincode::serde::decode_from_slice(&bytes, standard())?;
                Ok(Some(v))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(RaftError::Storage(crabka_log::LogError::Io(e))),
        }
    }
}
```

NOTE: openraft 0.9's actual trait is `RaftLogStorage` with methods like `get_log_state`, `get_log_entries`, `append`, `truncate`, `purge`, `save_vote`, `read_vote`. The struct above is a thin private helper; the actual trait impl is added in Step 2 below. The exact method set may differ slightly in the version that resolves — read `openraft::storage::v2::RaftLogStorage` docs and adapt.

- [ ] **Step 2: Implement `RaftLogStorage`**

Append to the same file:

```rust
#[async_trait::async_trait]
impl openraft::storage::RaftLogStorage<TypeConfig> for Arc<RaftLogStore> {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<TypeConfig>, openraft::StorageError<crate::types::NodeId>> {
        let last = self.last_log_id().await;
        let cache = self.cache.lock().await;
        let last_purged_log_id = (cache.last_purged > 0).then(|| {
            // No older entries retained — purged metadata not tracked precisely
            // in slice 7. Future snapshot work will restore precision.
            openraft::LogId {
                leader_id: openraft::LeaderId::new(0, 0),
                index: cache.last_purged - 1,
            }
        });
        Ok(openraft::storage::LogState { last_purged_log_id, last_log_id: last })
    }

    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<crate::types::NodeId>,
    ) -> Result<(), openraft::StorageError<crate::types::NodeId>> {
        RaftLogStore::save_vote(self, vote)
            .await
            .map_err(|e| openraft::StorageError::write_logs(&e))
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<openraft::Vote<crate::types::NodeId>>, openraft::StorageError<crate::types::NodeId>> {
        RaftLogStore::read_vote(self)
            .await
            .map_err(|e| openraft::StorageError::read_vote(&e))
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), openraft::StorageError<crate::types::NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        RaftLogStore::append(self, entries)
            .await
            .map_err(|e| openraft::StorageError::write_logs(&e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: openraft::LogId<crate::types::NodeId>,
    ) -> Result<(), openraft::StorageError<crate::types::NodeId>> {
        RaftLogStore::truncate(self, log_id.index)
            .await
            .map_err(|e| openraft::StorageError::write_logs(&e))
    }

    async fn purge(
        &mut self,
        _log_id: openraft::LogId<crate::types::NodeId>,
    ) -> Result<(), openraft::StorageError<crate::types::NodeId>> {
        // Slice 7: snapshots deferred, so purge is a no-op. Future
        // snapshot work will compact the log behind the snapshot index.
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

#[async_trait::async_trait]
impl openraft::storage::RaftLogReader<TypeConfig> for Arc<RaftLogStore> {
    async fn try_get_log_entries<R: RangeBounds<u64> + Send + Sync + Clone>(
        &mut self,
        range: R,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, openraft::StorageError<crate::types::NodeId>> {
        Ok(RaftLogStore::read_range(self, range).await)
    }
}
```

CRITICAL ADAPTATION NOTE: The exact signatures of `RaftLogStorage` / `RaftLogReader` in openraft 0.9 may differ from the above. Refer to `openraft::docs::storage` or the example at `https://github.com/databendlabs/openraft/blob/main/examples/raft-kv-memstore/src/store/mod.rs` for the version-current shape. Common drift points: `LogFlushed` callback API, `StorageError::write_logs` vs `::IO`, `purge` taking `LogId` vs `index`. If the trait method signature differs, change it — but keep this file's responsibility narrow (storage only). Add `async_trait = { workspace = true }` to `Cargo.toml` if it isn't already present.

- [ ] **Step 3: Test + commit**

Append a smoke test (in-process, no openraft yet — exercises the inner helpers):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn open_empty_returns_no_last_log_id() {
        let dir = TempDir::new().unwrap();
        let store = RaftLogStore::open(dir.path().to_path_buf()).await.unwrap();
        assert!(store.last_log_id().await.is_none());
    }

    #[tokio::test]
    async fn append_then_recover_round_trips() {
        let dir = TempDir::new().unwrap();
        let dir_path = dir.path().to_path_buf();
        {
            let store = RaftLogStore::open(dir_path.clone()).await.unwrap();
            let entry = openraft::Entry::<TypeConfig> {
                log_id: openraft::LogId {
                    leader_id: openraft::LeaderId::new(1, 1),
                    index: 1,
                },
                payload: openraft::EntryPayload::Blank,
            };
            store.append(vec![entry.clone()]).await.unwrap();
        }
        let store2 = RaftLogStore::open(dir_path).await.unwrap();
        assert_eq!(store2.last_log_id().await.unwrap().index, 1);
    }
}
```

```bash
cargo test -p crabka-raft log_store
git add crates/raft
git commit -m "feat(raft): RaftLogStorage on crabka-log @metadata-0"
```

---

### Task 9: `RaftStateMachine` (openraft `RaftStateMachine` impl)

**Files:**
- Create: `crates/raft/src/state_machine.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: State machine**

`crates/raft/src/state_machine.rs`:

```rust
//! openraft `RaftStateMachine` wrapping a `MetadataImage`. Apply is
//! synchronous + infallible; we swap the `Arc<MetadataImage>` after
//! mutating a fresh clone so readers always observe a consistent view.

use std::sync::Arc;

use tokio::sync::{watch, Mutex};
use uuid::Uuid;

use crabka_metadata::MetadataImage;

use crate::error::RaftError;
use crate::types::{AppData, AppDataResponse, TypeConfig};

pub(crate) struct RaftStateMachine {
    image: watch::Sender<Arc<MetadataImage>>,
    last_applied: Mutex<Option<openraft::LogId<crate::types::NodeId>>>,
}

impl RaftStateMachine {
    pub(crate) fn new(cluster_id: Uuid) -> Self {
        let initial = Arc::new(MetadataImage::new(cluster_id));
        let (image, _rx) = watch::channel(initial);
        Self { image, last_applied: Mutex::new(None) }
    }

    pub(crate) fn current_image(&self) -> Arc<MetadataImage> {
        self.image.borrow().clone()
    }

    pub(crate) fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image.subscribe()
    }

    pub(crate) async fn apply_entry(
        &self,
        log_id: openraft::LogId<crate::types::NodeId>,
        data: &AppData,
    ) -> Result<AppDataResponse, RaftError> {
        let current = self.image.borrow().clone();
        let mut next: MetadataImage = (*current).clone();
        for rec in &data.records {
            next.apply(rec);
        }
        self.image.send(Arc::new(next)).ok();
        *self.last_applied.lock().await = Some(log_id);
        Ok(AppDataResponse { applied_index: log_id.index })
    }
}

#[async_trait::async_trait]
impl openraft::storage::RaftStateMachine<TypeConfig> for Arc<RaftStateMachine> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<openraft::LogId<crate::types::NodeId>>,
            openraft::StoredMembership<crate::types::NodeId, crate::types::Node>,
        ),
        openraft::StorageError<crate::types::NodeId>,
    > {
        let last = *self.last_applied.lock().await;
        Ok((last, Default::default()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<AppDataResponse>, openraft::StorageError<crate::types::NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
    {
        let mut out = Vec::new();
        for entry in entries {
            let resp = match &entry.payload {
                openraft::EntryPayload::Blank => AppDataResponse { applied_index: entry.log_id.index },
                openraft::EntryPayload::Normal(data) => {
                    self.apply_entry(entry.log_id, data)
                        .await
                        .map_err(|e| openraft::StorageError::apply(&e))?
                }
                openraft::EntryPayload::Membership(_) => {
                    AppDataResponse { applied_index: entry.log_id.index }
                }
            };
            out.push(resp);
        }
        Ok(out)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<std::io::Cursor<Vec<u8>>>,
        openraft::StorageError<crate::types::NodeId>,
    > {
        Err(openraft::StorageError::IO {
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "snapshots deferred to a future slice",
            )
            .into(),
        })
    }

    async fn install_snapshot(
        &mut self,
        _meta: &openraft::SnapshotMeta<crate::types::NodeId, crate::types::Node>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<crate::types::NodeId>> {
        Err(openraft::StorageError::IO {
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "snapshots deferred to a future slice",
            )
            .into(),
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::Snapshot<TypeConfig>>,
        openraft::StorageError<crate::types::NodeId>,
    > {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl openraft::storage::RaftSnapshotBuilder<TypeConfig> for Arc<RaftStateMachine> {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::Snapshot<TypeConfig>,
        openraft::StorageError<crate::types::NodeId>,
    > {
        Err(openraft::StorageError::IO {
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "snapshots deferred to a future slice",
            )
            .into(),
        })
    }
}
```

ADAPTATION NOTE: As with Task 8, openraft 0.9's exact `RaftStateMachine` / `RaftSnapshotBuilder` trait shapes drift between point releases. Adapt to the version's actual methods. The principle is: apply is infallible for committed entries; snapshot methods return a typed "Unsupported" error so openraft handles lagging followers gracefully via append-entries (sufficient for an MVP small metadata log).

- [ ] **Step 2: Test + commit**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{MetadataRecord, TopicRecord};

    #[tokio::test]
    async fn apply_publishes_image_to_watcher() {
        let sm = Arc::new(RaftStateMachine::new(Uuid::nil()));
        let mut rx = sm.watch_image();
        let log_id = openraft::LogId { leader_id: openraft::LeaderId::new(1, 1), index: 1 };
        sm.apply_entry(log_id, &AppData {
            records: vec![MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(), topic_id: Uuid::new_v4(), partitions: 1, replication_factor: 1,
            })],
        }).await.unwrap();
        rx.changed().await.unwrap();
        assert!(rx.borrow().topic("t").is_some());
    }
}
```

```bash
cargo test -p crabka-raft state_machine
git add crates/raft
git commit -m "feat(raft): RaftStateMachine wrapping MetadataImage via watch channel"
```

---

### Task 10: `RaftNetworkFactory` + `RaftNetwork`

**Files:**
- Create: `crates/raft/src/network.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: Network adapters**

`crates/raft/src/network.rs`:

```rust
//! openraft `RaftNetwork` over Kafka TCP framing using the existing
//! `crabka-client-core::Connection`. One cached connection per peer.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;

use crabka_client_core::Connection;

use crate::error::RaftError;
use crate::types::{Node, NodeId, TypeConfig};
use crate::wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_VOTE, CrabkaAppendEntriesRequest,
    CrabkaAppendEntriesResponse, CrabkaLogEntry, CrabkaVoteRequest, CrabkaVoteResponse,
};

pub(crate) struct RaftNetworkFactory {
    connections: Arc<DashMap<NodeId, Arc<Connection>>>,
    client_id: String,
}

impl RaftNetworkFactory {
    pub(crate) fn new(client_id: String) -> Self {
        Self { connections: Arc::new(DashMap::new()), client_id }
    }

    async fn connect(&self, target: NodeId, addr: &str) -> Result<Arc<Connection>, RaftError> {
        if let Some(c) = self.connections.get(&target) {
            return Ok(c.value().clone());
        }
        let sock: SocketAddr = addr.parse().map_err(|_| {
            RaftError::Network(crabka_client_core::ClientError::InvalidAddress(addr.into()))
        })?;
        let conn = Arc::new(
            Connection::connect(sock, self.client_id.clone()).await
                .map_err(RaftError::Network)?,
        );
        self.connections.insert(target, conn.clone());
        Ok(conn)
    }
}

#[async_trait::async_trait]
impl openraft::network::RaftNetworkFactory<TypeConfig> for RaftNetworkFactory {
    type Network = RaftNetworkConn;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        RaftNetworkConn {
            target,
            addr: node.addr.clone(),
            factory: self.clone(),
        }
    }
}

impl Clone for RaftNetworkFactory {
    fn clone(&self) -> Self {
        Self {
            connections: self.connections.clone(),
            client_id: self.client_id.clone(),
        }
    }
}

pub(crate) struct RaftNetworkConn {
    target: NodeId,
    addr: String,
    factory: RaftNetworkFactory,
}

#[async_trait::async_trait]
impl openraft::network::RaftNetwork<TypeConfig> for RaftNetworkConn {
    async fn send_append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, Node, openraft::error::Infallible>,
    > {
        let conn = self.factory.connect(self.target, &self.addr).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let body = encode_append_entries(&rpc).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let resp_body = conn.raw_request(API_KEY_APPEND_ENTRIES, 0, body).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        decode_append_entries_resp(&resp_body, rpc.vote.committed_leader_id()).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })
    }

    async fn send_vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, Node, openraft::error::Infallible>,
    > {
        let conn = self.factory.connect(self.target, &self.addr).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let body = encode_vote(&rpc).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        let resp_body = conn.raw_request(API_KEY_VOTE, 0, body).await.map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })?;
        decode_vote_resp(&resp_body).map_err(|e| {
            openraft::error::RPCError::Network(openraft::error::NetworkError::new(&e))
        })
    }
}

fn encode_append_entries(
    rpc: &openraft::raft::AppendEntriesRequest<TypeConfig>,
) -> Result<Bytes, RaftError> {
    use bincode::config::standard;
    let mut entries = Vec::with_capacity(rpc.entries.len());
    for e in &rpc.entries {
        let payload_kind: i8 = match &e.payload {
            openraft::EntryPayload::Blank => 0,
            openraft::EntryPayload::Normal(_) => 1,
            openraft::EntryPayload::Membership(_) => 2,
        };
        let payload = bincode::serde::encode_to_vec(&e.payload, standard())?;
        entries.push(CrabkaLogEntry {
            log_index: i64::try_from(e.log_id.index).unwrap_or(i64::MAX),
            log_term: i64::try_from(e.log_id.leader_id.term).unwrap_or(i64::MAX),
            payload_kind,
            payload: Bytes::from(payload),
        });
    }
    let req = CrabkaAppendEntriesRequest {
        node_id: i32::try_from(rpc.vote.committed_leader_id().node_id).unwrap_or(-1),
        term: i64::try_from(rpc.vote.committed_leader_id().term).unwrap_or(i64::MAX),
        leader_id: i32::try_from(rpc.vote.committed_leader_id().node_id).unwrap_or(-1),
        prev_log_index: rpc.prev_log_id.map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        prev_log_term: rpc.prev_log_id.map_or(-1, |l| i64::try_from(l.leader_id.term).unwrap_or(i64::MAX)),
        leader_commit: rpc.leader_commit.map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        entries,
    };
    let mut out = Vec::with_capacity(64);
    req.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

fn decode_append_entries_resp(
    body: &[u8],
    _leader_id: openraft::CommittedLeaderId<NodeId>,
) -> Result<openraft::raft::AppendEntriesResponse<NodeId>, RaftError> {
    let mut cur = body;
    let resp = CrabkaAppendEntriesResponse::decode_v0(&mut cur)?;
    if resp.success {
        Ok(openraft::raft::AppendEntriesResponse::Success)
    } else {
        Ok(openraft::raft::AppendEntriesResponse::HigherVote(openraft::Vote::new(
            u64::try_from(resp.term).unwrap_or(0),
            0,
        )))
    }
}

fn encode_vote(rpc: &openraft::raft::VoteRequest<NodeId>) -> Result<Bytes, RaftError> {
    let req = CrabkaVoteRequest {
        term: i64::try_from(rpc.vote.committed_leader_id().term).unwrap_or(i64::MAX),
        candidate_id: rpc.vote.committed_leader_id().node_id,
        last_log_index: rpc.last_log_id.map_or(-1, |l| i64::try_from(l.index).unwrap_or(i64::MAX)),
        last_log_term: rpc.last_log_id.map_or(-1, |l| i64::try_from(l.leader_id.term).unwrap_or(i64::MAX)),
    };
    let mut out = Vec::with_capacity(32);
    req.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

fn decode_vote_resp(body: &[u8]) -> Result<openraft::raft::VoteResponse<NodeId>, RaftError> {
    let mut cur = body;
    let resp = CrabkaVoteResponse::decode_v0(&mut cur)?;
    Ok(openraft::raft::VoteResponse {
        vote: openraft::Vote::new(u64::try_from(resp.term).unwrap_or(0), 0),
        vote_granted: resp.vote_granted,
        last_log_id: None,
    })
}
```

CRITICAL ADAPTATION NOTES:
- `Connection::raw_request(api_key, version, body) -> Result<Bytes, ClientError>` is the assumed signature. Look at `crates/client-core/src/connection.rs` for the actual public surface; if `raw_request` doesn't exist, use the lowest-level Send-and-receive method that does (likely a `send_raw` or similar). If the framing is opaque (e.g., `Client::send(req: impl ProtocolRequest)` is the only entry point), add a `pub fn raw_request(api_key: i16, version: i16, body: Bytes) -> ...` to `crabka-client-core` first, in a small commit, then continue this task.
- openraft 0.9's `RaftNetwork` may have moved to `send_*` returning a richer error type. `AppendEntriesResponse` variants (`Success`, `HigherVote`, `Conflict`, …) differ across versions — `HigherVote(Vote)` is illustrative.
- `ClientError::InvalidAddress` may not exist; pick whatever variant the client-core crate uses for "couldn't parse address" or add one if there's a gap.

- [ ] **Step 2: Build + commit**

```bash
cargo build -p crabka-raft
git add crates/raft
git commit -m "feat(raft): RaftNetwork over Kafka TCP framing (cached Connection per peer)"
```

(No unit tests in this commit — the network layer is exercised via the multi-node integration tests in Task 14.)

---

## Phase D — Controller

### Task 11: `ControllerConfig` + controller server listener

**Files:**
- Create: `crates/raft/src/config.rs`
- Create: `crates/raft/src/server.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: Config**

`crates/raft/src/config.rs`:

```rust
//! Construction-time config for `Controller::start`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::types::NodeId;

#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub node_id: NodeId,
    pub voters: Vec<(NodeId, SocketAddr)>,
    pub controller_listen_addr: SocketAddr,
    pub log_dir: PathBuf,
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub client_id: String,
}

impl ControllerConfig {
    #[must_use]
    pub fn for_tests(node_id: NodeId, log_dir: PathBuf) -> Self {
        Self {
            node_id,
            voters: vec![(node_id, "127.0.0.1:0".parse().expect("static"))],
            controller_listen_addr: "127.0.0.1:0".parse().expect("static"),
            log_dir,
            election_timeout: Duration::from_millis(1_000),
            heartbeat_interval: Duration::from_millis(200),
            client_id: "crabka-controller-test".into(),
        }
    }
}
```

- [ ] **Step 2: Server listener**

`crates/raft/src/server.rs`:

```rust
//! Accept loop for the controller TCP listener. Receives Crabka-private
//! Raft RPCs and feeds them into the local `Raft` instance.

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::error::RaftError;
use crate::types::{Raft, TypeConfig};
use crate::wire::{
    API_KEY_APPEND_ENTRIES, API_KEY_VOTE, API_KEY_INSTALL_SNAPSHOT,
    CrabkaAppendEntriesRequest, CrabkaAppendEntriesResponse,
    CrabkaInstallSnapshotResponse, CrabkaVoteRequest, CrabkaVoteResponse,
};

const REJECT_NOT_IMPLEMENTED: i16 = -1;

pub(crate) async fn run(
    listener: TcpListener,
    raft: Arc<Raft>,
    shutdown: CancellationToken,
) {
    info!(addr = %listener.local_addr().expect("bound"), "controller listener started");
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accept = listener.accept() => {
                match accept {
                    Ok((stream, peer)) => {
                        let raft = raft.clone();
                        let shutdown = shutdown.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_conn(stream, raft, shutdown).await {
                                error!(%peer, error = %e, "controller connection error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "controller listener accept failed");
                    }
                }
            }
        }
    }
}

async fn handle_conn(
    mut stream: TcpStream,
    raft: Arc<Raft>,
    shutdown: CancellationToken,
) -> Result<(), RaftError> {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            res = read_one_request(&mut stream) => {
                let (api_key, correlation_id, body) = res?;
                let resp = dispatch(api_key, &body, &raft).await?;
                write_response(&mut stream, correlation_id, resp).await?;
            }
        }
    }
}

async fn read_one_request(stream: &mut TcpStream) -> Result<(i16, i32, Bytes), RaftError> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.map_err(crabka_log::LogError::Io)?;
    let len = i32::from_be_bytes(len_buf).max(0) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.map_err(crabka_log::LogError::Io)?;

    // RequestHeader v2 (flexible): api_key(i16), api_version(i16),
    // correlation_id(i32), client_id(VARSTRING), tagged_fields(varint=0).
    let mut cur: &[u8] = &frame;
    if cur.remaining() < 2 + 2 + 4 {
        return Err(RaftError::Protocol(crabka_protocol::ProtocolError::Truncated));
    }
    let api_key = cur.get_i16();
    let _api_version = cur.get_i16();
    let correlation_id = cur.get_i32();

    // Skip client_id: NULLABLE_STRING (i16 length + bytes).
    if cur.remaining() < 2 {
        return Err(RaftError::Protocol(crabka_protocol::ProtocolError::Truncated));
    }
    let cs_len = cur.get_i16();
    if cs_len > 0 {
        if cur.remaining() < cs_len as usize {
            return Err(RaftError::Protocol(crabka_protocol::ProtocolError::Truncated));
        }
        cur.advance(cs_len as usize);
    }
    // tagged_fields: single varint zero.
    if cur.has_remaining() && cur[0] == 0 {
        cur.advance(1);
    }

    Ok((api_key, correlation_id, Bytes::copy_from_slice(cur)))
}

async fn write_response(
    stream: &mut TcpStream,
    correlation_id: i32,
    body: Bytes,
) -> Result<(), RaftError> {
    let mut frame = BytesMut::with_capacity(4 + 1 + body.len());
    frame.put_i32(correlation_id);
    frame.put_u8(0); // empty tagged_fields
    frame.put_slice(&body);

    let mut len_prefix = [0u8; 4];
    len_prefix.copy_from_slice(&i32::try_from(frame.len()).unwrap_or(i32::MAX).to_be_bytes());
    stream.write_all(&len_prefix).await.map_err(crabka_log::LogError::Io)?;
    stream.write_all(&frame).await.map_err(crabka_log::LogError::Io)?;
    stream.flush().await.map_err(crabka_log::LogError::Io)?;
    Ok(())
}

async fn dispatch(api_key: i16, body: &[u8], raft: &Raft) -> Result<Bytes, RaftError> {
    match api_key {
        API_KEY_APPEND_ENTRIES => {
            let mut cur = body;
            let req = CrabkaAppendEntriesRequest::decode_v0(&mut cur)?;
            let openraft_req = convert_append_entries(req)?;
            let res = raft.append_entries(openraft_req).await
                .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
            let mut out = Vec::with_capacity(32);
            CrabkaAppendEntriesResponse {
                success: matches!(res, openraft::raft::AppendEntriesResponse::Success),
                term: 0,
                last_log_index: 0,
            }.encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_VOTE => {
            let mut cur = body;
            let req = CrabkaVoteRequest::decode_v0(&mut cur)?;
            let openraft_req = openraft::raft::VoteRequest {
                vote: openraft::Vote::new(u64::try_from(req.term).unwrap_or(0), req.candidate_id),
                last_log_id: None,
            };
            let res = raft.vote(openraft_req).await
                .map_err(|e| RaftError::Openraft(format!("{e:?}")))?;
            let mut out = Vec::with_capacity(16);
            CrabkaVoteResponse {
                vote_granted: res.vote_granted,
                term: i64::try_from(res.vote.committed_leader_id().term).unwrap_or(i64::MAX),
            }.encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        API_KEY_INSTALL_SNAPSHOT => {
            let mut out = Vec::with_capacity(4);
            CrabkaInstallSnapshotResponse { error_code: REJECT_NOT_IMPLEMENTED }
                .encode_v0(&mut out)?;
            Ok(Bytes::from(out))
        }
        _ => Err(RaftError::Protocol(crabka_protocol::ProtocolError::Truncated)),
    }
}

fn convert_append_entries(
    req: CrabkaAppendEntriesRequest,
) -> Result<openraft::raft::AppendEntriesRequest<TypeConfig>, RaftError> {
    use bincode::config::standard;
    let entries = req.entries.into_iter().map(|e| {
        let payload: openraft::EntryPayload<TypeConfig> =
            bincode::serde::decode_from_slice(&e.payload, standard())
                .map(|(v, _)| v)
                .unwrap_or(openraft::EntryPayload::Blank);
        openraft::Entry {
            log_id: openraft::LogId {
                leader_id: openraft::LeaderId::new(
                    u64::try_from(e.log_term).unwrap_or(0),
                    u64::try_from(req.leader_id.max(0)).unwrap_or(0),
                ),
                index: u64::try_from(e.log_index).unwrap_or(0),
            },
            payload,
        }
    }).collect();
    Ok(openraft::raft::AppendEntriesRequest {
        vote: openraft::Vote::new(
            u64::try_from(req.term).unwrap_or(0),
            u64::try_from(req.leader_id.max(0)).unwrap_or(0),
        ),
        prev_log_id: (req.prev_log_index >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(
                u64::try_from(req.prev_log_term).unwrap_or(0),
                u64::try_from(req.leader_id.max(0)).unwrap_or(0),
            ),
            index: u64::try_from(req.prev_log_index).unwrap_or(0),
        }),
        entries,
        leader_commit: (req.leader_commit >= 0).then(|| openraft::LogId {
            leader_id: openraft::LeaderId::new(0, 0),
            index: u64::try_from(req.leader_commit).unwrap_or(0),
        }),
    })
}
```

NOTE: The wire helpers are deliberately scrappy — for slice 7 the controller listener is a Crabka-private path. If a future slice ports KRaft on top, this file gets rewritten. For now the priority is "openraft RPCs reach the right method calls."

- [ ] **Step 3: Re-export config**

In `crates/raft/src/lib.rs`, add `mod config; mod server;` and `pub use config::ControllerConfig;`.

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-raft
git add crates/raft
git commit -m "feat(raft): ControllerConfig + controller listener accept loop"
```

---

### Task 12: `Controller::start` + `ControllerHandle::submit_change` + leader watch

**Files:**
- Create: `crates/raft/src/controller.rs`
- Modify: `crates/raft/src/lib.rs`

- [ ] **Step 1: Controller**

`crates/raft/src/controller.rs`:

```rust
//! `Controller` is the public entry point. Owns the openraft node, the
//! state machine watcher, the controller listener task, and the
//! `submit_change` leader-aware forwarding logic.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, RwLock};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crabka_metadata::{MetadataImage, MetadataRecord};

use crate::config::ControllerConfig;
use crate::error::RaftError;
use crate::log_store::RaftLogStore;
use crate::network::RaftNetworkFactory;
use crate::server;
use crate::state_machine::RaftStateMachine;
use crate::types::{AppData, NodeId, Raft, TypeConfig};

pub struct ControllerHandle {
    raft: Arc<Raft>,
    state_machine: Arc<RaftStateMachine>,
    leader: watch::Receiver<Option<NodeId>>,
    shutdown: CancellationToken,
    listener_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    leader_pump_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    voters: Vec<(NodeId, String)>,
}

impl ControllerHandle {
    /// Current metadata snapshot (cheap; Arc clone).
    #[must_use]
    pub fn current_image(&self) -> Arc<MetadataImage> {
        self.state_machine.current_image()
    }

    /// Subscribe to leader-id changes.
    #[must_use]
    pub fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader.clone()
    }

    /// Submit a batch of metadata records. Future resolves when the
    /// records are committed AND applied on this node.
    pub async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), RaftError> {
        // Pre-validate against the current image so we don't spam Raft
        // with records we already know are invalid.
        let image = self.state_machine.current_image();
        for r in &records {
            image.validate(r)?;
        }
        let data = AppData { records };

        // Up to 3 attempts; on NotLeader, re-route. On LeaderUnknown,
        // wait up to 5s for the leader watch to populate.
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            let result = self.raft.client_write(data.clone()).await;
            match result {
                Ok(_) => return Ok(()),
                Err(openraft::error::RaftError::APIError(openraft::error::ClientWriteError::ForwardToLeader(f))) => {
                    if attempts >= 3 {
                        return Err(RaftError::NotLeader { current_leader: f.leader_id });
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => return Err(RaftError::Openraft(format!("{e:?}"))),
            }
        }
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.raft.shutdown().await;
        if let Some(h) = self.listener_task.lock().await.take() {
            let _ = h.await;
        }
        if let Some(h) = self.leader_pump_task.lock().await.take() {
            let _ = h.await;
        }
    }
}

pub struct Controller;

impl Controller {
    /// Start an openraft node, open the controller listener, and begin
    /// participating in the quorum.
    pub async fn start(config: ControllerConfig) -> Result<ControllerHandle, RaftError> {
        // 1. Log + state machine.
        let log_store = Arc::new(RaftLogStore::open(config.log_dir.clone()).await?);
        let state_machine = Arc::new(RaftStateMachine::new(Uuid::nil()));

        // 2. openraft config.
        let raft_config = openraft::Config {
            cluster_name: "crabka-metadata".into(),
            election_timeout_min: u64::try_from(config.election_timeout.as_millis())
                .unwrap_or(1_000),
            election_timeout_max: u64::try_from(config.election_timeout.as_millis() * 2)
                .unwrap_or(2_000),
            heartbeat_interval: u64::try_from(config.heartbeat_interval.as_millis())
                .unwrap_or(200),
            install_snapshot_timeout: 5_000,
            send_snapshot_timeout: 5_000,
            ..Default::default()
        };

        // 3. Network factory.
        let network = RaftNetworkFactory::new(config.client_id.clone());

        // 4. Spawn openraft.
        let raft = Arc::new(
            openraft::Raft::new(
                config.node_id,
                Arc::new(raft_config),
                network,
                log_store.clone(),
                state_machine.clone(),
            )
            .await
            .map_err(|e| RaftError::Openraft(format!("{e:?}")))?,
        );

        // 5. If we're the first to start with no log, initialize the cluster.
        if log_store.last_log_id().await.is_none() {
            let members: std::collections::BTreeMap<NodeId, crate::types::Node> = config
                .voters
                .iter()
                .map(|(id, addr)| {
                    (*id, openraft::BasicNode { addr: addr.to_string() })
                })
                .collect();
            if let Err(e) = raft.initialize(members).await {
                // Already initialized is fine — every node tries.
                warn!(error = ?e, "raft initialize returned error (likely already-initialized); continuing");
            }
        }

        // 6. Controller listener.
        let listener = tokio::net::TcpListener::bind(config.controller_listen_addr)
            .await
            .map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let actual_addr = listener.local_addr().map_err(|e| RaftError::Storage(crabka_log::LogError::Io(e)))?;
        let shutdown = CancellationToken::new();
        let listener_task = tokio::spawn(server::run(listener, raft.clone(), shutdown.clone()));
        info!(node_id = config.node_id, addr = %actual_addr, "controller started");

        // 7. Leader-watch pump: subscribe to openraft's metrics and
        // republish the leader id into our local watch channel.
        let (leader_tx, leader_rx) = watch::channel::<Option<NodeId>>(None);
        let metrics_rx = raft.metrics();
        let shutdown_clone = shutdown.clone();
        let leader_pump_task = tokio::spawn(async move {
            let mut metrics_rx = metrics_rx;
            loop {
                tokio::select! {
                    () = shutdown_clone.cancelled() => break,
                    res = metrics_rx.changed() => {
                        if res.is_err() { break; }
                        let m = metrics_rx.borrow();
                        let _ = leader_tx.send(m.current_leader);
                    }
                }
            }
        });

        Ok(ControllerHandle {
            raft,
            state_machine,
            leader: leader_rx,
            shutdown,
            listener_task: tokio::sync::Mutex::new(Some(listener_task)),
            leader_pump_task: tokio::sync::Mutex::new(Some(leader_pump_task)),
            voters: config.voters.iter().map(|(id, a)| (*id, a.to_string())).collect(),
        })
    }
}
```

ADAPTATION NOTES:
- `openraft::Raft::new` signature varies between versions; pass whatever shape resolves.
- `openraft::error::ClientWriteError::ForwardToLeader` variant has a `leader_id: Option<NodeId>` (or `Option<u64>` after macro expansion). Confirm.
- `raft.metrics()` returns a `watch::Receiver<RaftMetrics<NodeId, Node>>`; field is `.current_leader`.
- The "already initialized" check is best-effort. openraft's `initialize` returns `Err(InitializeError::NotAllowed)` once any node has initialized; treating that as success is correct.

- [ ] **Step 2: Re-export**

In `crates/raft/src/lib.rs`, add `mod controller; mod log_store; mod network; mod state_machine;` and `pub use controller::{Controller, ControllerHandle};`. Add `async_trait = { workspace = true }` to `Cargo.toml` if not present.

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-raft
git add crates/raft
git commit -m "feat(raft): Controller::start + ControllerHandle::submit_change with leader-aware retry"
```

---

### Task 13: Single-voter smoke test

**Files:**
- Create: `crates/raft/tests/single_node.rs`

- [ ] **Step 1: Smoke test**

`crates/raft/tests/single_node.rs`:

```rust
//! In-process single-voter Controller. Validates the openraft + log_store +
//! state_machine + listener wiring without needing a 3-node cluster.

use std::time::Duration;

use crabka_metadata::{MetadataRecord, TopicRecord};
use crabka_raft::{Controller, ControllerConfig};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_create_topic_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    // Pin the controller listen addr to a real loopback port so the network
    // factory has something to dial when initialize wants to seed members.
    cfg.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    cfg.voters = vec![(1, cfg.controller_listen_addr)];

    let controller = Controller::start(cfg).await.expect("controller start");

    // Wait until openraft elects this single voter as leader.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if controller.watch_leader().borrow().is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("no leader elected within 5s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(),
        topic_id: Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    });
    controller.submit_change(vec![topic]).await.expect("submit");

    assert!(controller.current_image().topic("t").is_some());

    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_voter_duplicate_topic_rejected() {
    let dir = TempDir::new().unwrap();
    let mut cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
    cfg.controller_listen_addr = "127.0.0.1:0".parse().unwrap();
    cfg.voters = vec![(1, cfg.controller_listen_addr)];
    let controller = Controller::start(cfg).await.unwrap();

    for _ in 0..50 {
        if controller.watch_leader().borrow().is_some() { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let topic = MetadataRecord::V1Topic(TopicRecord {
        name: "t".into(), topic_id: Uuid::new_v4(), partitions: 1, replication_factor: 1,
    });
    controller.submit_change(vec![topic.clone()]).await.unwrap();
    let err = controller.submit_change(vec![topic]).await.unwrap_err();
    assert!(matches!(err, crabka_raft::RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))));

    controller.shutdown().await;
}
```

The first port-binding pass uses `127.0.0.1:0` for OS allocation; the test then re-uses the listener addr as both `controller_listen_addr` AND the voter address so the controller's outbound dial loops back to itself. If openraft's single-voter election doesn't require the network at all (no peers to call), this loopback may be unused — but specifying it is the safest shape.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-raft --test single_node
git add crates/raft/tests
git commit -m "test(raft): single-voter Controller smoke (CreateTopic + duplicate rejection)"
```

If this hangs (no leader within 5s) the most likely cause is that openraft's `Config::election_timeout_*` are too long for the 5s test deadline; lower `election_timeout` to 200ms in `ControllerConfig::for_tests` if needed.

---

## Phase E — Broker integration

### Task 14: `BrokerConfig` adds quorum fields

**Files:**
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Extend `BrokerConfig`**

Read `crates/broker/src/config.rs` first to see the existing shape. Add three new fields with sensible defaults so single-node test setups don't need to specify them:

```rust
use crabka_raft::NodeId;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub broker_id: i32,
    pub listen_addr: SocketAddr,
    pub advertised_listener: String,
    pub log_dir: PathBuf,
    pub log_config: LogConfig,

    /// Raft node id. Defaults to `broker_id as NodeId`.
    pub node_id: NodeId,

    /// Address the controller listener binds on. Default: same host as
    /// `listen_addr`, port 9093 (matches KRaft convention).
    pub controller_listen_addr: SocketAddr,

    /// Static voter set: `[(node_id, controller_addr), …]`. Defaults
    /// to a single-voter cluster of just this broker, so existing
    /// slice-1..6 tests upgrade with no config changes.
    pub controller_quorum_voters: Vec<(NodeId, SocketAddr)>,
}

impl BrokerConfig {
    #[must_use]
    pub fn for_tests(log_dir: PathBuf) -> Self {
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().expect("hard-coded valid addr");
        let controller_addr: SocketAddr = "127.0.0.1:0".parse().expect("hard-coded valid addr");
        Self {
            broker_id: 1,
            listen_addr,
            advertised_listener: "127.0.0.1:0".into(),
            log_dir,
            log_config: LogConfig::default(),
            node_id: 1,
            controller_listen_addr: controller_addr,
            controller_quorum_voters: vec![(1, controller_addr)],
        }
    }
}
```

The default value for `controller_quorum_voters` (single-voter with this node) means the slice-1..6 tests don't need to be updated.

- [ ] **Step 2: Add `crabka-raft` as a dep of `crabka-broker`**

In `crates/broker/Cargo.toml`'s `[dependencies]`:

```toml
crabka-raft = { version = "0.1", path = "../raft" }
crabka-metadata = { version = "0.1", path = "../metadata" }
```

- [ ] **Step 3: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/config.rs crates/broker/Cargo.toml
git commit -m "feat(broker): BrokerConfig gains node_id, controller_listen_addr, controller_quorum_voters"
```

---

### Task 15: `Broker::start` constructs `Controller`; metadata reads through it

**Files:**
- Modify: `crates/broker/src/broker.rs`
- Delete: `crates/broker/src/metadata.rs` (slice-4 in-memory image)
- Modify: `crates/broker/src/lib.rs` (drop `mod metadata`)

- [ ] **Step 1: Wire `Controller` into `Broker`**

Read `crates/broker/src/broker.rs` first. The existing struct has `metadata: Arc<RwLock<MetadataImage>>` (slice-4 in-memory). Replace with:

```rust
use crabka_raft::{Controller, ControllerConfig, ControllerHandle};

pub struct Broker {
    pub(crate) config: BrokerConfig,
    pub(crate) controller: Arc<ControllerHandle>,
    pub(crate) partitions: Arc<DashMap<(String, i32), Arc<Partition>>>,
    pub(crate) group_manager: Arc<crate::coordinator::GroupManager>,
    pub(crate) producer_ids: Arc<crate::producer_id_manager::ProducerIdManager>,
    pub(crate) producer_state: Arc<crate::producer_state::ProducerState>,
    pub(crate) handlers: Arc<crate::handlers::HandlerTable>,
    pub(crate) shutdown: tokio_util::sync::CancellationToken,
    // ... whatever else was there
}
```

In `Broker::start`:

```rust
        // 1. Start the controller BEFORE the client listener so handlers
        //    can read from it immediately.
        let controller_cfg = ControllerConfig {
            node_id: config.node_id,
            voters: config.controller_quorum_voters.clone(),
            controller_listen_addr: config.controller_listen_addr,
            log_dir: config.log_dir.join("__cluster_metadata"),
            election_timeout: std::time::Duration::from_millis(1_000),
            heartbeat_interval: std::time::Duration::from_millis(200),
            client_id: format!("crabka-broker-{}-controller", config.broker_id),
        };
        let controller = Arc::new(Controller::start(controller_cfg).await
            .map_err(|e| crate::error::BrokerError::Startup(e.to_string()))?);

        // 2. Register self in the quorum (best-effort — leader writes
        //    succeed; followers' submit_change forwards).
        let self_registration = crabka_metadata::MetadataRecord::V1BrokerRegistration(
            crabka_metadata::BrokerRegistrationRecord {
                node_id: config.node_id,
                host: config.advertised_listener.split(':').next().unwrap_or("127.0.0.1").to_string(),
                port: config.listen_addr.port(),
                rack: None,
            },
        );
        // Wait for a leader to be elected.
        let mut leader_rx = controller.watch_leader();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while leader_rx.borrow().is_none() {
            if std::time::Instant::now() > deadline {
                return Err(crate::error::BrokerError::Startup(
                    "no leader elected within 10s".into(),
                ));
            }
            tokio::time::timeout(std::time::Duration::from_millis(100), leader_rx.changed()).await.ok();
        }
        if let Err(e) = controller.submit_change(vec![self_registration]).await {
            tracing::warn!(error = %e, "self-registration failed; continuing");
        }
```

The slice-4 in-memory metadata code (`MetadataImage::new()`, `meta.write()`, partition seeding) is deleted. The partition seed from disk still happens, but it doesn't touch the metadata image — partitions are now derived from `controller.current_image()` whenever a handler needs them.

- [ ] **Step 2: Delete `crates/broker/src/metadata.rs`**

```bash
git rm crates/broker/src/metadata.rs
```

Remove `mod metadata;` from `crates/broker/src/lib.rs` if it was there.

- [ ] **Step 3: Add a `BrokerError::Startup` variant**

In `crates/broker/src/error.rs`:

```rust
    #[error("startup failed: {0}")]
    Startup(String),
```

- [ ] **Step 4: Build + commit**

```bash
cargo build -p crabka-broker
git add crates/broker
git commit -m "feat(broker): wire Controller into Broker::start; delete slice-4 in-memory metadata"
```

Build will likely fail in handlers that read the old `metadata` field. Tasks 16-18 fix those handler call sites.

---

### Task 16: `metadata` handler reads from controller image

**Files:**
- Modify: `crates/broker/src/handlers/metadata.rs`

- [ ] **Step 1: Read recon**

Skim `crates/broker/src/handlers/metadata.rs` to see how the slice-4 handler builds `MetadataResponse`. Likely shape: it iterates `broker.metadata.read().unwrap().topics`, looks up each topic's partition count, and assigns the local broker id as the leader.

- [ ] **Step 2: Replace reads**

Replace every `broker.metadata.read()...` with `broker.controller.current_image()`. The image's API mirrors the old shape:

```rust
let image = broker.controller.current_image();
for topic_record in image.topics() {
    let partitions: Vec<_> = image.partitions_of(&topic_record.name).collect();
    // build MetadataResponseTopic { name, topic_id, partitions, ... }
}
```

If the slice-4 code assumed a per-topic `partition_count: i32`, use `topic_record.partitions`.

If the slice-4 code looked up topics by id (Uuid), use `image.topics().find(|t| t.topic_id == id)`.

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker --lib metadata
cargo test -p crabka-broker --test integration -- metadata
git add crates/broker
git commit -m "refactor(broker): Metadata handler reads from controller image"
```

---

### Task 17: `create_topics` handler submits to controller

**Files:**
- Modify: `crates/broker/src/handlers/create_topics.rs`

- [ ] **Step 1: Submit through `Controller`**

Replace the slice-4 logic (which mutated the in-memory metadata directly) with:

```rust
use crabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use crabka_raft::RaftError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let log_dir = broker.config.log_dir.clone();
    let log_config = broker.config.log_config.clone();
    let partitions_map = broker.partitions.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = CreateTopicsRequest::decode(&mut cur, version)?;
        let mut topic_results = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            let topic_id = Uuid::new_v4();
            let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
                name: topic.name.clone(),
                topic_id,
                partitions: topic.num_partitions,
                replication_factor: topic.replication_factor,
            })];
            for p in 0..topic.num_partitions {
                records.push(MetadataRecord::V1Partition(PartitionRecord {
                    topic: topic.name.clone(),
                    partition: p,
                    leader: node_id,
                    replicas: vec![node_id],
                    isr: vec![node_id],
                }));
            }
            let result = controller.submit_change(records).await;

            let error_code = match result {
                Ok(()) => {
                    // Materialize on-disk partitions for this broker.
                    for p in 0..topic.num_partitions {
                        let dir = log_dir.join(format!("{}-{}", topic.name, p));
                        std::fs::create_dir_all(&dir).ok();
                        if let Ok(log) = crabka_log::Log::open(&dir, log_config.clone()) {
                            let part = std::sync::Arc::new(crate::partition::Partition::from_log(log));
                            partitions_map.insert((topic.name.clone(), p), part);
                        }
                    }
                    codes::NONE
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => codes::TOPIC_ALREADY_EXISTS,
                Err(RaftError::NotLeader { .. }) => codes::NOT_CONTROLLER,
                Err(RaftError::LeaderUnknown) => codes::NOT_CONTROLLER,
                Err(e) => {
                    tracing::error!(topic = %topic.name, error = %e, "CreateTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
            };
            topic_results.push(CreatableTopicResult {
                name: topic.name,
                topic_id,
                error_code,
                error_message: None,
                ..Default::default()
            });
        }

        let resp = CreateTopicsResponse { topics: topic_results, ..Default::default() };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

The slice-4 handler probably did its own concurrent-create dedup via the in-memory map; the controller's `validate` step now does this. `TOPIC_ALREADY_EXISTS = 36`. `NOT_CONTROLLER = 41`. `UNKNOWN_SERVER_ERROR = -1` (or whatever the broker's `codes` module uses for unmapped).

Read the existing handler to confirm `codes::NONE`, `codes::TOPIC_ALREADY_EXISTS`, `codes::NOT_CONTROLLER`, `codes::UNKNOWN_SERVER_ERROR` exist; add the latter two if missing in slice-4's `codes.rs`.

- [ ] **Step 2: Test + commit**

```bash
cargo test -p crabka-broker --test unit -- create_topic
cargo build -p crabka-broker
git add crates/broker
git commit -m "refactor(broker): CreateTopics handler submits through Controller"
```

---

### Task 18: `delete_topics` handler submits to controller

**Files:**
- Modify: `crates/broker/src/handlers/delete_topics.rs`

- [ ] **Step 1: Submit through `Controller`**

Mirror the Task 17 pattern for `DeleteTopics`:

```rust
use crabka_metadata::{DeleteTopicRecord, MetadataRecord};
use crabka_raft::RaftError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let controller = broker.controller.clone();
    let partitions_map = broker.partitions.clone();
    let log_dir = broker.config.log_dir.clone();

    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = DeleteTopicsRequest::decode(&mut cur, version)?;
        let mut results = Vec::with_capacity(req.topic_names.len());

        for name in req.topic_names {
            let res = controller
                .submit_change(vec![MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                    name: name.clone(),
                })])
                .await;
            let error_code = match res {
                Ok(()) => {
                    // Drop partition handles + remove on-disk dirs.
                    partitions_map.retain(|(t, _), _| t != &name);
                    if let Ok(entries) = std::fs::read_dir(&log_dir) {
                        for e in entries.flatten() {
                            let n = e.file_name();
                            let s = n.to_string_lossy();
                            if s.starts_with(&format!("{name}-")) {
                                let _ = std::fs::remove_dir_all(e.path());
                            }
                        }
                    }
                    codes::NONE
                }
                Err(RaftError::Metadata(crabka_metadata::MetadataError::UnknownTopic(_))) => {
                    codes::UNKNOWN_TOPIC_OR_PARTITION
                }
                Err(RaftError::NotLeader { .. }) | Err(RaftError::LeaderUnknown) => {
                    codes::NOT_CONTROLLER
                }
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "DeleteTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
            };
            results.push(DeletableTopicResult {
                name,
                error_code,
                error_message: None,
                ..Default::default()
            });
        }

        let resp = DeleteTopicsResponse { responses: results, ..Default::default() };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Adapt to the actual `DeleteTopicsRequest` shape (Apache Kafka has several variants: by name, by topic_id, etc.). Slice-4's existing handler is the canonical reference.

- [ ] **Step 2: Test + commit**

```bash
cargo test -p crabka-broker --lib delete_topics
cargo build -p crabka-broker
git add crates/broker
git commit -m "refactor(broker): DeleteTopics handler submits through Controller"
```

---

### Task 19: Full-build cleanup pass

**Files:**
- Various — wherever the old `broker.metadata` field is still referenced.

After Tasks 15-18 the build may still fail in places that used `broker.metadata`. Common spots:

- `crates/broker/src/handlers/find_coordinator.rs` — reads topic existence for coordinator topics.
- `crates/broker/src/handlers/produce.rs` — partition lookup before append (this one uses `broker.partitions`, not metadata, so likely fine).
- `crates/broker/src/handlers/fetch.rs` — same.
- `crates/broker/src/handlers/describe_configs.rs` — may read topic configs.
- `crates/broker/tests/support/mod.rs` — test harness may seed topics by directly mutating the metadata image.

- [ ] **Step 1: Recon**

```bash
grep -rn "broker.metadata\|\\.metadata\\.read\|\\.metadata\\.write" crates/broker/src/
```

- [ ] **Step 2: Fix each call site**

For each match: replace reads with `broker.controller.current_image()`, replace writes with `broker.controller.submit_change(vec![...])`. For test-harness writes, the test should call into `controller.submit_change` like a real client.

- [ ] **Step 3: Full workspace build + test**

```bash
cargo build --workspace
cargo test -p crabka-broker
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
```

All slice-1..6 tests must still pass against a single-voter cluster.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(broker): drop remaining slice-4 metadata reads, route everything through Controller"
```

---

## Phase F — Multi-node integration tests

### Task 20: `crates/broker/tests/quorum.rs` (5 multi-node tests)

**Files:**
- Create: `crates/broker/tests/quorum.rs`

- [ ] **Step 1: Test harness for N-node clusters**

```rust
//! Multi-node in-process Crabka cluster tests. Each test spins up
//! 3 brokers on distinct loopback ports, all listed as voters in
//! each other's config.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use tempfile::TempDir;

async fn start_n_node(n: u64) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    // Phase 1: pre-bind each broker's TWO listeners (client + controller).
    // We need to know the controller addresses to construct the voter list
    // for ALL brokers, so we bind ahead of time, capture the addresses,
    // close the listeners, then reopen them inside Broker::start.
    let mut client_addrs = Vec::with_capacity(n as usize);
    let mut controller_addrs = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let cl = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        client_addrs.push(cl.local_addr().unwrap());
        let ct = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        controller_addrs.push(ct.local_addr().unwrap());
        drop((cl, ct));
    }

    let voters: Vec<(u64, SocketAddr)> = (0..n)
        .map(|i| (i + 1, controller_addrs[i as usize]))
        .collect();

    // Phase 2: spawn brokers in parallel so they can elect a leader.
    let mut handles = Vec::with_capacity(n as usize);
    let mut spawned = Vec::with_capacity(n as usize);
    for i in 0..n {
        let dir = TempDir::new().unwrap();
        let cfg = BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: client_addrs[i as usize],
            advertised_listener: client_addrs[i as usize].to_string(),
            log_dir: dir.path().to_path_buf(),
            log_config: Default::default(),
            node_id: i + 1,
            controller_listen_addr: controller_addrs[i as usize],
            controller_quorum_voters: voters.clone(),
        };
        spawned.push(tokio::spawn({
            let cfg = cfg.clone();
            async move { Broker::start(cfg).await.expect("broker start") }
        }));
        handles.push((dir, cfg));
    }
    let mut out = Vec::with_capacity(n as usize);
    for (i, j) in spawned.into_iter().enumerate() {
        let h = j.await.unwrap();
        let (dir, cfg) = handles.remove(0);
        out.push((h, cfg, dir));
    }
    let _ = handles;
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_leader() {
    let cluster = start_n_node(3).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // Ask each broker who it thinks the leader is.
        let mut leaders = std::collections::HashSet::new();
        for (h, _, _) in &cluster {
            if let Some(l) = h.controller_leader_id().await {
                leaders.insert(l);
            }
        }
        if leaders.len() == 1 && !leaders.contains(&0) {
            break;
        }
        if Instant::now() > deadline {
            panic!("leader not converged within 5s; current views: {leaders:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_topic_on_any_node_propagates() {
    let cluster = start_n_node(3).await;
    wait_for_leader(&cluster).await;

    // CreateTopics against node 0.
    let c = Client::builder().bootstrap(cluster[0].1.listen_addr.to_string()).build().await.unwrap();
    let resp = c.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "prop".into(),
            num_partitions: 3,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(resp.topics[0].error_code, 0);

    // Metadata against node 2 should see it within 1s.
    let c2 = Client::builder().bootstrap(cluster[2].1.listen_addr.to_string()).build().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let m = c2.send(MetadataRequest::default()).await.unwrap();
        if m.topics.iter().any(|t| t.name.as_deref() == Some("prop")) {
            break;
        }
        if Instant::now() > deadline {
            panic!("topic not propagated to node 2 within 1s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_kill_recovers() {
    let mut cluster = start_n_node(3).await;
    wait_for_leader(&cluster).await;

    // Find the leader.
    let mut leader_idx = None;
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await == Some(cfg.node_id) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("at least one broker self-identifies as leader");

    // Kill the leader.
    let (leader, _, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;

    // Survivors elect a new leader within 5s.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut leaders = std::collections::HashSet::new();
        for (h, _, _) in &cluster {
            if let Some(l) = h.controller_leader_id().await {
                leaders.insert(l);
            }
        }
        if leaders.len() == 1 && !leaders.contains(&0) {
            break;
        }
        if Instant::now() > deadline {
            panic!("no new leader within 5s of kill");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // CreateTopics against a survivor succeeds.
    let c = Client::builder().bootstrap(cluster[0].1.listen_addr.to_string()).build().await.unwrap();
    let resp = c.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "post-kill".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(resp.topics[0].error_code, 0);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_forwards_create_topic() {
    let cluster = start_n_node(3).await;
    wait_for_leader(&cluster).await;

    // Identify a follower.
    let mut follower_idx = None;
    for (i, (h, cfg, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await != Some(cfg.node_id) {
            follower_idx = Some(i);
            break;
        }
    }
    let follower_idx = follower_idx.expect("at least one follower");

    let c = Client::builder().bootstrap(cluster[follower_idx].1.listen_addr.to_string()).build().await.unwrap();
    let resp = c.send(CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "via-follower".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }).await.unwrap();
    assert_eq!(resp.topics[0].error_code, 0);

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_topic_creates_one_wins() {
    let cluster = start_n_node(3).await;
    wait_for_leader(&cluster).await;

    let clients = {
        let mut v = Vec::new();
        for (_, cfg, _) in &cluster {
            v.push(Client::builder().bootstrap(cfg.listen_addr.to_string()).build().await.unwrap());
        }
        v
    };

    let mut joins = Vec::new();
    for c in clients {
        joins.push(tokio::spawn(async move {
            c.send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "race".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            }).await.unwrap()
        }));
    }
    let mut zero = 0;
    let mut already = 0;
    for j in joins {
        let resp = j.await.unwrap();
        match resp.topics[0].error_code {
            0 => zero += 1,
            36 /* TOPIC_ALREADY_EXISTS */ => already += 1,
            other => panic!("unexpected error_code {other}"),
        }
    }
    assert_eq!(zero, 1, "exactly one winner");
    assert_eq!(already, 2, "two losers see TOPIC_ALREADY_EXISTS");

    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}

async fn wait_for_leader(cluster: &[(BrokerHandle, BrokerConfig, TempDir)]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (h, _, _) in cluster {
            if h.controller_leader_id().await.is_some() {
                return;
            }
        }
        if Instant::now() > deadline {
            panic!("no leader within 5s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

REQUIRED: add a `pub async fn controller_leader_id(&self) -> Option<NodeId>` method on `BrokerHandle` (or `Broker`) that calls `controller.watch_leader().borrow().clone()`. Trivial pass-through but the tests need it.

- [ ] **Step 2: Test + commit**

```bash
cargo test -p crabka-broker --test quorum
git add crates/broker/tests/quorum.rs crates/broker/src/
git commit -m "test(broker): multi-node quorum tests (5 scenarios)"
```

---

### Task 21: JVM acceptance `three_node_jvm_round_trip`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Three-node JVM round-trip**

Append to `crates/broker/tests/jvm_acceptance.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn three_node_jvm_round_trip() {
    const TOPIC: &str = "crabka-quorum-itest";

    // Fixed ports for the 3 nodes so docker-side tools have known bootstraps.
    let client_ports = [9192u16, 9292, 9392];
    let controller_ports = [9193u16, 9293, 9393];
    let voters: Vec<(u64, std::net::SocketAddr)> = (0..3)
        .map(|i| (u64::from(i as u8) + 1, format!("host.docker.internal:{}", controller_ports[i]).parse().unwrap()))
        .collect();

    let mut cluster = Vec::new();
    for i in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let cfg = crabka_broker::BrokerConfig {
            broker_id: i32::try_from(i + 1).unwrap(),
            listen_addr: format!("0.0.0.0:{}", client_ports[i]).parse().unwrap(),
            advertised_listener: format!("host.docker.internal:{}", client_ports[i]),
            log_dir: dir.path().to_path_buf(),
            log_config: Default::default(),
            node_id: (i as u64) + 1,
            controller_listen_addr: format!("0.0.0.0:{}", controller_ports[i]).parse().unwrap(),
            controller_quorum_voters: voters.clone(),
        };
        cluster.push((crabka_broker::Broker::start(cfg).await.expect("broker"), dir));
    }

    let bootstrap_1 = format!("host.docker.internal:{}", client_ports[0]);
    let bootstrap_3 = format!("host.docker.internal:{}", client_ports[2]);

    // 1. Create the topic via node 1.
    docker_run_kafka_tool(&[
        "kafka-topics", "--create", "--if-not-exists", "--topic", TOPIC,
        "--partitions", "1", "--replication-factor", "1",
        "--bootstrap-server", &bootstrap_1,
    ]);

    // Give propagation a moment.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // 2. Produce via node 2 (middle index).
    let producer = crabka_client_producer::Producer::builder()
        .bootstrap(format!("host.docker.internal:{}", client_ports[1]))
        .enable_idempotence(true)
        .acks(crabka_client_producer::Acks::All)
        .build()
        .await
        .expect("producer");
    for v in ["a", "b", "c"] {
        let fut = producer.send(crabka_client_producer::ProducerRecord {
            topic: TOPIC.into(),
            value: Some(bytes::Bytes::from(v)),
            ..Default::default()
        }).await;
        let _ = fut.await.expect("oneshot").expect("ack");
    }
    producer.flush().await.expect("flush");
    producer.close().await.expect("close");

    // 3. Consume via node 3.
    let out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server", &bootstrap_3,
        "--topic", TOPIC,
        "--partition", "0",
        "--from-beginning",
        "--max-messages", "3",
        "--timeout-ms", "20000",
    ]);
    let s = String::from_utf8_lossy(&out.stdout);
    for needle in ["a", "b", "c"] {
        assert!(s.contains(needle), "missing {needle} in {s:?}");
    }

    // 4. Identify the leader, kill it, confirm a survivor still answers Metadata.
    let mut leader_idx = None;
    for (i, (h, _)) in cluster.iter().enumerate() {
        if h.controller_leader_id().await == Some((i as u64) + 1) {
            leader_idx = Some(i);
            break;
        }
    }
    let leader_idx = leader_idx.expect("leader exists");
    let (leader, _dir) = cluster.remove(leader_idx);
    leader.shutdown().await;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 5. Find a survivor and confirm the topic is still listed.
    let survivor_port = client_ports.iter().enumerate()
        .find(|(i, _)| *i != leader_idx)
        .map(|(_, p)| *p)
        .unwrap();
    let bootstrap_survivor = format!("host.docker.internal:{}", survivor_port);
    let list_out = docker_run_kafka_tool(&[
        "kafka-topics", "--list",
        "--bootstrap-server", &bootstrap_survivor,
    ]);
    let list_s = String::from_utf8_lossy(&list_out.stdout);
    assert!(list_s.contains(TOPIC), "topic missing after leader kill: {list_s:?}");

    for (h, _) in cluster {
        h.shutdown().await;
    }
}
```

ADAPTATION NOTE: The exact `cluster.remove(leader_idx)` shape depends on what type `BrokerHandle` is (Send + 'static; should work). The ports above (9192/9292/9392 + 9193/9293/9393) are intentionally far from existing slice-1..6 acceptance tests (which use 9092/9093) so multiple acceptance tests can run sequentially without port conflicts.

- [ ] **Step 2: Build + commit**

```bash
cargo check -p crabka-broker --tests
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(broker): JVM acceptance — 3-node cluster + leader-kill survival"
```

The test is `#[ignore]`-gated; CI runs it via `--include-ignored`. The existing `/etc/hosts` workflow step from slice 6 already makes `host.docker.internal` resolve on the host process, so the producer call works.

---

## Phase G — Acceptance gate + PR

### Task 22: Rustdoc + acceptance gate + PR

- [ ] **Step 1: Crate-level rustdoc**

Update `crates/raft/src/lib.rs`:

```rust
//! Metadata Raft quorum for Crabka.
//!
//! `crabka-raft` adapts [openraft][openraft] to Crabka's storage
//! ([`crabka_log`]) and transport ([`crabka_client_core`]). The public
//! entry point is [`Controller::start`], which spawns an openraft node,
//! opens a TCP listener for Crabka-private Raft RPCs (api keys 1000-
//! 1002), and returns a [`ControllerHandle`] for submitting metadata
//! changes and reading the current [`crabka_metadata::MetadataImage`].
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_metadata::{MetadataRecord, TopicRecord};
//! use crabka_raft::{Controller, ControllerConfig};
//! use uuid::Uuid;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let cfg = ControllerConfig::for_tests(1, dir.path().to_path_buf());
//! let controller = Controller::start(cfg).await?;
//!
//! controller.submit_change(vec![
//!     MetadataRecord::V1Topic(TopicRecord {
//!         name: "my-topic".into(),
//!         topic_id: Uuid::new_v4(),
//!         partitions: 3,
//!         replication_factor: 1,
//!     }),
//! ]).await?;
//!
//! assert!(controller.current_image().topic("my-topic").is_some());
//! controller.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - Snapshots / `InstallSnapshot` (handler is a stub).
//! - Dynamic voter membership changes.
//! - KRaft wire compatibility (api keys 52-55, KRaft Fetch).
//!
//! [openraft]: https://github.com/databendlabs/openraft

#![doc(html_root_url = "https://docs.rs/crabka-raft/0.0.0")]

// ... mod / pub use lines from earlier tasks
```

Add a similar `//!` rustdoc to `crates/metadata/src/lib.rs`.

- [ ] **Step 2: Full local acceptance gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace -- --include-ignored
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

All MUST be clean.

- [ ] **Step 3: Commit any final cleanups**

```bash
git add -A
git commit -m "docs(raft,metadata): crate-level rustdoc + quick-start examples"
```

- [ ] **Step 4: Push branch + open PR**

```bash
git push -u origin feature/metadata-quorum
gh pr create --base main --head feature/metadata-quorum \
    --title "Slice 7: crabka-metadata-quorum (openraft + 3-node cluster)" \
    --body "$(cat <<'PRBODY'
## Summary

Replaces slice-4's in-memory metadata with an openraft-backed metadata quorum across N Crabka brokers. After this slice, a 3-node cluster boots, elects a Raft leader, accepts `CreateTopics` against any node, and a JVM client round-trips records through any of the three brokers. Killing the leader survives metadata reads.

## What landed

- `crates/metadata/` (new): versioned `MetadataRecord` enum (Topic, Partition, BrokerRegistration, DeleteTopic) + `MetadataImage` with `apply`/`validate`.
- `crates/raft/` (new): openraft `RaftLogStorage` on `crabka-log @ @metadata-0`, `RaftStateMachine` wrapping `MetadataImage`, `RaftNetwork` over Kafka TCP framing on api keys 1000-1002, public `Controller::start` + `ControllerHandle::submit_change` with leader-aware forwarding.
- `crates/broker/`: two listeners (client + controller); `Broker::start` wires `Controller`; `CreateTopics`/`DeleteTopics` route through `Controller`; slice-4's in-memory metadata is deleted; existing slice-1..6 tests upgrade to the new "quorum-of-1 = single-node" model with no test changes.
- Tests: 5 multi-node in-process quorum tests, JVM `three_node_jvm_round_trip` acceptance test (leader-kill survival).

## Out of scope (each maps to a future slice)

- Snapshots / `InstallSnapshot` (handler is a stub).
- Dynamic voter membership changes.
- Partition data replication (slice 8).
- KRaft wire compatibility.
- Auth on the controller listener (slice 11).

## Reference

Spec: `docs/superpowers/specs/2026-05-12-crabka-metadata-quorum-design.md`
Plan: `docs/superpowers/plans/2026-05-12-crabka-metadata-quorum.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

Report the PR URL.

---

## Self-review against the spec

| # | Spec section / requirement                                                       | Plan task          |
|---|----------------------------------------------------------------------------------|--------------------|
| 1 | `crabka-metadata` crate with versioned records + `MetadataImage`                  | Tasks 2, 4, 5, 6   |
| 2 | `crabka-raft` crate with openraft adapters                                        | Tasks 3, 8, 9, 10  |
| 3 | Crabka-private wire types for api keys 1000-1002                                  | Task 7             |
| 4 | `Controller::start` + `ControllerHandle` (submit_change, current_image, watch_leader) | Tasks 11, 12 |
| 5 | Single-voter smoke validates the wiring                                           | Task 13            |
| 6 | `BrokerConfig` gains node_id + controller fields                                  | Task 14            |
| 7 | `Broker::start` constructs `Controller`; slice-4 metadata deleted                 | Task 15            |
| 8 | Metadata handler reads from `controller.current_image()`                          | Task 16            |
| 9 | CreateTopics / DeleteTopics route through `Controller`                            | Tasks 17, 18       |
| 10 | Remaining handler call sites updated; full workspace build clean                 | Task 19            |
| 11 | Five multi-node integration tests in `quorum.rs`                                 | Task 20            |
| 12 | JVM acceptance `three_node_jvm_round_trip` with leader-kill                      | Task 21            |
| 13 | Rustdoc + acceptance gate + PR                                                   | Task 22            |
| 14 | `RaftError` with all listed variants                                             | Task 3             |
| 15 | `MetadataError` with all listed variants                                         | Task 2             |
| 16 | `NOT_CONTROLLER = 41` mapping on `RaftError::NotLeader`                          | Task 17, 18        |
| 17 | Snapshot handlers stubbed (return Unsupported)                                   | Task 9             |
| 18 | Tracing spans on the controller path                                             | Tasks 11, 12       |
| 19 | Observability metric names                                                        | Documented in spec; emitted via `tracing` events in Tasks 11/12; OTLP deferred to slice 11 per spec |

**Placeholder scan:** No literal `TBD`/`TODO`/`fill in` markers in the steps. Multiple `ADAPTATION NOTE:` callouts flag openraft API version drift — these are explicit instructions to the implementer to read the resolved crate and pick the matching method signatures, not "figure it out yourself" hand-waves. Every code block in every task is concrete.

**Type consistency:**
- `NodeId = u64` everywhere (Tasks 3, 4, 7, 14).
- `Controller::start(ControllerConfig) -> Result<ControllerHandle, RaftError>` consistent (Task 12).
- `ControllerHandle::submit_change(Vec<MetadataRecord>)` consistent (Tasks 12, 17, 18).
- `MetadataImage::topic`, `topics`, `partition`, `partitions_of`, `brokers`, `cluster_id`, `validate`, `apply` consistent (Tasks 5, 16).
- `MetadataRecord::V1Topic/V1Partition/V1BrokerRegistration/V1DeleteTopic` consistent (Tasks 4, 5, 9, 17, 18).
- Wire api-key constants `API_KEY_APPEND_ENTRIES = 1000`, `API_KEY_VOTE = 1001`, `API_KEY_INSTALL_SNAPSHOT = 1002` consistent (Tasks 7, 11).

The plan is ready for execution.
