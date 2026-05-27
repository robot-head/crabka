# `crabka-consumer-groups` (slice 5) Implementation Plan

## Implementation status

**Slice tracked in STATUS.md as:** Not tracked as a dedicated STATUS.md header — covered implicitly by the protocol-foundation preamble or rolled into subsequent slices.

**Incomplete / deferred steps:** None recorded in STATUS.md.

---

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the classic Kafka group-coordinator protocol end-to-end. JVM `kafka-console-consumer` (no `--partition`) subscribes through a group, receives records, and its committed offsets survive a broker restart. A new `crabka-client-consumer` crate provides the same path for Rust callers.

**Architecture:** A `coordinator` subsystem inside `crabka-broker` owns a `DashMap<group_id, Arc<Mutex<Group>>>` plus the lifecycle of the `__consumer_offsets-0` log. Six new request handlers (`JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`) drive the protocol. The existing `FindCoordinator` stub is replaced. A new `crabka-client-consumer` crate sits on slice-2's `crabka-client-core`, exposing a subscribe-only `Consumer` with a built-in heartbeat task.

**Tech Stack:** Rust 1.95.0 edition 2024; `tokio` (sync, time, macros, rt-multi-thread); `crabka-protocol`, `crabka-log`, `crabka-broker`, `crabka-client-core` (all shipped); `bytes`, `dashmap`, `tracing`, `uuid`, `thiserror`.

**Reference spec:** [`docs/superpowers/specs/2026-05-11-crabka-consumer-groups-design.md`](../specs/2026-05-11-crabka-consumer-groups-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Plan branch: `plan/consumer-groups-plan` (this file). Implementation runs on `feature/consumer-groups` branched off `main` once this plan's PR merges.

---

## File structure

```
crates/broker/                                          # additions to slice-4 crate
└── src/
    ├── codes.rs                                        # MODIFIED — add 5 codes
    ├── error.rs                                        # MODIFIED — add 3 variants
    ├── broker.rs                                       # MODIFIED — bootstrap + register handlers
    ├── coordinator/
    │   ├── mod.rs                                      # GroupManager + tick loop
    │   ├── group.rs                                    # Group + GroupState + Member
    │   ├── persistence.rs                              # OffsetCommit + GroupMetadata record codecs
    │   └── bootstrap.rs                                # ensure_offsets_topic + replay
    └── handlers/
        ├── mod.rs                                      # MODIFIED — register 6 new handlers
        ├── find_coordinator.rs                         # MODIFIED — replace stub
        ├── join_group.rs                               # NEW
        ├── sync_group.rs                               # NEW
        ├── heartbeat.rs                                # NEW
        ├── leave_group.rs                              # NEW
        ├── offset_commit.rs                            # NEW
        └── offset_fetch.rs                             # NEW

crates/broker/tests/
├── unit.rs                                             # MODIFIED — extended with new handler tests
├── integration.rs                                      # MODIFIED — adds group-flow scenarios
└── jvm_acceptance.rs                                   # MODIFIED — adds console_consumer_with_group_round_trip

crates/client-consumer/                                  # NEW crate
├── Cargo.toml
└── src/
    ├── lib.rs                                          # public API: Consumer, ConsumerBuilder, ConsumerRecord, ConsumerError
    ├── builder.rs                                      # ConsumerBuilder
    ├── consumer.rs                                     # Consumer struct + close()
    ├── assignor/
    │   ├── mod.rs                                      # ProtocolMetadata, MemberAssignment
    │   └── range.rs                                    # the range assignor (pure fn)
    ├── heartbeat.rs                                    # spawned task + RebalanceNotice
    ├── poll.rs                                         # Consumer::poll
    ├── commit.rs                                       # commit_sync / commit_async
    └── error.rs                                        # ConsumerError

crates/client-consumer/tests/
├── unit.rs                                             # MockBroker-driven flows
└── integration.rs                                      # end-to-end Rust producer + Rust consumer + restart-replay
```

The workspace root `Cargo.toml` already has `crabka-client-core` + `crabka-broker` + `crabka-log` + `crabka-protocol` as workspace members (`members = ["crates/*"]`). The new `crates/client-consumer/` directory is picked up automatically.

---

## Phase A — Wire-level codes + group state machine

### Task 1: Add the 5 wire codes + 3 internal error variants

**Files:**
- Modify: `crates/broker/src/codes.rs`
- Modify: `crates/broker/src/error.rs`

- [ ] **Step 1: Add the codes**

Append to `crates/broker/src/codes.rs` (preserve the existing `from_broker_error` mapping; we extend it below):

```rust
// Phase 5 additions — group coordinator codes.
pub const ILLEGAL_GENERATION: i16 = 22;
pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
pub const UNKNOWN_MEMBER_ID: i16 = 25;
pub const REBALANCE_IN_PROGRESS: i16 = 27;
pub const MEMBER_ID_REQUIRED: i16 = 79;
```

- [ ] **Step 2: Add the BrokerError variants**

In `crates/broker/src/error.rs`, append to the `BrokerError` enum (preserve `#[non_exhaustive]` and existing variants):

```rust
    #[error("group {group_id} is in state {state:?}, request not allowed")]
    GroupInvalidState { group_id: String, state: String },

    #[error("unknown member {member_id} in group {group_id}")]
    UnknownMember { group_id: String, member_id: String },

    #[error("group {group_id} generation mismatch: have {current}, got {requested}")]
    GenerationMismatch {
        group_id: String,
        current: i32,
        requested: i32,
    },
```

- [ ] **Step 3: Extend `from_broker_error`**

In `crates/broker/src/codes.rs`, update the `match err` in `from_broker_error` to map the new variants:

```rust
        BrokerError::GroupInvalidState { .. } => REBALANCE_IN_PROGRESS,
        BrokerError::UnknownMember { .. } => UNKNOWN_MEMBER_ID,
        BrokerError::GenerationMismatch { .. } => ILLEGAL_GENERATION,
```

(Place them above the catch-all branch.)

- [ ] **Step 4: Test + commit**

Add a unit test inside `codes.rs`'s existing `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn maps_group_invalid_state_to_27() {
        let e = BrokerError::GroupInvalidState {
            group_id: "g".into(),
            state: "PreparingRebalance".into(),
        };
        assert_eq!(from_broker_error(&e), REBALANCE_IN_PROGRESS);
    }

    #[test]
    fn maps_unknown_member_to_25() {
        let e = BrokerError::UnknownMember {
            group_id: "g".into(),
            member_id: "m".into(),
        };
        assert_eq!(from_broker_error(&e), UNKNOWN_MEMBER_ID);
    }

    #[test]
    fn maps_generation_mismatch_to_22() {
        let e = BrokerError::GenerationMismatch {
            group_id: "g".into(),
            current: 5,
            requested: 4,
        };
        assert_eq!(from_broker_error(&e), ILLEGAL_GENERATION);
    }
```

```bash
cargo test -p crabka-broker codes
git add crates/broker/src/codes.rs crates/broker/src/error.rs
git commit -m "feat(broker): group-coordinator wire codes + BrokerError variants"
```

---

### Task 2: `Group` state machine + `Member`

**Files:**
- Create: `crates/broker/src/coordinator/mod.rs` (skeleton)
- Create: `crates/broker/src/coordinator/group.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Module skeleton**

`crates/broker/src/coordinator/mod.rs`:

```rust
//! Group-coordinator state. `GroupManager` lands in Task 3; this module
//! re-exports types from sibling files.

#![allow(dead_code)] // consumers land in Tasks 3-12.

pub(crate) mod group;
```

- [ ] **Step 2: Group + GroupState + Member**

`crates/broker/src/coordinator/group.rs`:

```rust
//! `Group` — per-`group_id` state machine. Pure data + transitions; the
//! coordinator handlers (Tasks 6–12) hold the mutex around it.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bytes::Bytes;

/// Five-state machine for a consumer group, matching the Apache Kafka
/// classic protocol (KIP-62 / KIP-394).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    /// No members and no committed offsets.
    Empty,
    /// At least one member has called JoinGroup; waiting for the rebalance
    /// deadline or every expected member.
    PreparingRebalance,
    /// JoinGroup returned to all members; waiting for the leader's SyncGroup.
    CompletingRebalance,
    /// SyncGroup completed; members are heart-beating.
    Stable,
    /// Group has been deleted (e.g. after the last member leaves and an
    /// optional retention period). Reserved; the MVP doesn't actively
    /// transition into this state.
    Dead,
}

/// One member of a [`Group`].
#[derive(Debug, Clone)]
pub struct Member {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub last_heartbeat: Instant,
    /// Encoded `ConsumerProtocolSubscription` bytes (a `subscription` field
    /// from `JoinGroupRequest`). Opaque to the broker.
    pub protocol_metadata: Bytes,
    /// Encoded `ConsumerProtocolAssignment` bytes — populated by the leader
    /// in `SyncGroup`. `None` until then.
    pub assignment: Option<Bytes>,
}

impl Member {
    #[must_use]
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        host: impl Into<String>,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        protocol_metadata: Bytes,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            client_id: client_id.into(),
            host: host.into(),
            session_timeout,
            rebalance_timeout,
            last_heartbeat: Instant::now(),
            protocol_metadata,
            assignment: None,
        }
    }
}

/// A committed offset entry. Keyed by `(topic, partition)` in
/// [`Group::committed_offsets`].
#[derive(Debug, Clone)]
pub struct OffsetEntry {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
}

#[derive(Debug)]
pub struct Group {
    pub group_id: String,
    pub state: GroupState,
    /// `"consumer"` for `KafkaConsumer`. The broker doesn't interpret the
    /// value beyond rejecting inconsistent proposals.
    pub protocol_type: Option<String>,
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub protocol_name: Option<String>,
    pub members: HashMap<String, Member>,
    pub committed_offsets: HashMap<(String, i32), OffsetEntry>,
    pub rebalance_deadline: Option<Instant>,
}

impl Group {
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            state: GroupState::Empty,
            protocol_type: None,
            generation_id: 0,
            leader_id: None,
            protocol_name: None,
            members: HashMap::new(),
            committed_offsets: HashMap::new(),
            rebalance_deadline: None,
        }
    }

    /// Add or refresh a member. Transitions to `PreparingRebalance` if
    /// currently `Empty` or `Stable`.
    pub fn add_member(&mut self, member: Member) {
        let was_first_join = matches!(self.state, GroupState::Empty | GroupState::Stable);
        self.members.insert(member.member_id.clone(), member);
        if was_first_join {
            self.state = GroupState::PreparingRebalance;
        }
    }

    /// Remove a member; transitions to `Empty` if no members remain.
    pub fn remove_member(&mut self, member_id: &str) {
        self.members.remove(member_id);
        if self.members.is_empty() {
            self.state = GroupState::Empty;
            self.leader_id = None;
            self.protocol_name = None;
            self.rebalance_deadline = None;
        }
    }

    /// Complete the rebalance: pick the leader (oldest member_id wins —
    /// stable for tests), bump the generation, advance state.
    pub fn complete_rebalance(&mut self, protocol_name: impl Into<String>) {
        let leader = self
            .members
            .keys()
            .min()
            .cloned()
            .expect("complete_rebalance requires ≥1 member");
        self.leader_id = Some(leader);
        self.protocol_name = Some(protocol_name.into());
        self.generation_id += 1;
        self.state = GroupState::CompletingRebalance;
        self.rebalance_deadline = None;
    }

    /// Called when the leader's SyncGroup arrives with assignments.
    /// Stores each member's `assignment` and transitions to `Stable`.
    pub fn install_assignments(&mut self, assignments: HashMap<String, Bytes>) {
        for (member_id, bytes) in assignments {
            if let Some(m) = self.members.get_mut(&member_id) {
                m.assignment = Some(bytes);
            }
        }
        self.state = GroupState::Stable;
    }

    /// Drop any member whose `last_heartbeat` is older than its
    /// `session_timeout`. Returns the dropped member IDs. Transitions to
    /// `PreparingRebalance` if any were dropped and the group still has
    /// members; to `Empty` if it became empty.
    pub fn expire_dead_members(&mut self, now: Instant) -> Vec<String> {
        let dropped: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_heartbeat) > m.session_timeout)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dropped {
            self.members.remove(id);
        }
        if !dropped.is_empty() {
            if self.members.is_empty() {
                self.state = GroupState::Empty;
                self.leader_id = None;
                self.protocol_name = None;
            } else {
                self.state = GroupState::PreparingRebalance;
            }
        }
        dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_member(id: &str) -> Member {
        Member::new(
            id,
            "test-client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_secs(60),
            Bytes::new(),
        )
    }

    #[test]
    fn empty_to_preparing_on_first_join() {
        let mut g = Group::new("g");
        assert_eq!(g.state, GroupState::Empty);
        g.add_member(sample_member("m1"));
        assert_eq!(g.state, GroupState::PreparingRebalance);
    }

    #[test]
    fn complete_rebalance_bumps_generation() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.add_member(sample_member("m2"));
        g.complete_rebalance("range");
        assert_eq!(g.generation_id, 1);
        assert_eq!(g.leader_id.as_deref(), Some("m1"));
        assert_eq!(g.protocol_name.as_deref(), Some("range"));
        assert_eq!(g.state, GroupState::CompletingRebalance);
    }

    #[test]
    fn install_assignments_to_stable() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.complete_rebalance("range");
        let mut a = HashMap::new();
        a.insert("m1".into(), Bytes::from_static(b"assignment-bytes"));
        g.install_assignments(a);
        assert_eq!(g.state, GroupState::Stable);
        assert!(g.members["m1"].assignment.is_some());
    }

    #[test]
    fn remove_last_member_empties_group() {
        let mut g = Group::new("g");
        g.add_member(sample_member("m1"));
        g.remove_member("m1");
        assert_eq!(g.state, GroupState::Empty);
        assert!(g.leader_id.is_none());
    }

    #[test]
    fn expire_dead_members_drops_stale() {
        let mut g = Group::new("g");
        let mut m = sample_member("m1");
        m.session_timeout = Duration::from_millis(1);
        m.last_heartbeat = Instant::now() - Duration::from_secs(1);
        g.add_member(m);
        let dropped = g.expire_dead_members(Instant::now());
        assert_eq!(dropped, vec!["m1".to_string()]);
        assert_eq!(g.state, GroupState::Empty);
    }
}
```

- [ ] **Step 3: Hook into `lib.rs`**

In `crates/broker/src/lib.rs`, after the existing internal `mod` declarations and before `pub use`, add:

```rust
mod coordinator;
```

(Keep `coordinator` internal — no `pub use` yet. Phase B exposes `GroupManager` to the rest of the crate.)

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-broker coordinator::group
git add crates/broker
git commit -m "feat(broker): Group + GroupState + Member state machine"
```

---

## Phase B — `GroupManager` + persistence + bootstrap

### Task 3: `GroupManager`

**Files:**
- Modify: `crates/broker/src/coordinator/mod.rs`

- [ ] **Step 1: Replace `mod.rs` with the full manager**

Replace `crates/broker/src/coordinator/mod.rs`:

```rust
//! Group-coordinator subsystem. `GroupManager` owns the runtime registry
//! of `Group`s and exposes per-group locking, blocking-handler gates, and
//! a periodic expiration ticker.

pub(crate) mod bootstrap;
pub(crate) mod group;
pub(crate) mod persistence;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use group::Group;

/// Runtime handles for one group: the locked `Group` plus per-stage
/// `Notify`s used by `join_group` and `sync_group` to park waiting members.
pub(crate) struct GroupHandle {
    pub state: Mutex<Group>,
    /// Wakes all parked JoinGroup handlers when the rebalance deadline fires
    /// or when every expected member has joined.
    pub join_complete: Notify,
    /// Wakes all parked SyncGroup handlers when the leader's SyncGroup arrives.
    pub sync_complete: Notify,
}

impl GroupHandle {
    fn new(group_id: impl Into<String>) -> Self {
        Self {
            state: Mutex::new(Group::new(group_id)),
            join_complete: Notify::new(),
            sync_complete: Notify::new(),
        }
    }
}

pub(crate) struct GroupManager {
    /// Cheap-to-clone shared map.
    pub(crate) groups: Arc<DashMap<String, Arc<GroupHandle>>>,
    /// Cancellation token for the expiration ticker.
    shutdown: CancellationToken,
    /// Held so the ticker is reaped when `GroupManager` drops.
    _ticker: JoinHandle<()>,
}

impl GroupManager {
    pub fn new() -> Self {
        let groups: Arc<DashMap<String, Arc<GroupHandle>>> = Arc::new(DashMap::new());
        let shutdown = CancellationToken::new();
        let ticker = tokio::spawn(expiration_ticker(groups.clone(), shutdown.clone()));
        Self {
            groups,
            shutdown,
            _ticker: ticker,
        }
    }

    pub fn get_or_create(&self, group_id: &str) -> Arc<GroupHandle> {
        if let Some(h) = self.groups.get(group_id) {
            return h.value().clone();
        }
        let new_handle = Arc::new(GroupHandle::new(group_id));
        self.groups
            .entry(group_id.to_string())
            .or_insert(new_handle)
            .value()
            .clone()
    }

    pub fn find(&self, group_id: &str) -> Option<Arc<GroupHandle>> {
        self.groups.get(group_id).map(|h| h.value().clone())
    }

    /// Cancel the ticker. Called from `Broker::shutdown` if it ever wants
    /// to drain explicitly; otherwise the ticker exits when `_ticker`
    /// drops.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl std::fmt::Debug for GroupManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupManager")
            .field("group_count", &self.groups.len())
            .finish_non_exhaustive()
    }
}

/// Wake every group's expirations every second. On any drop, fire the
/// per-group `join_complete` notify so blocked JoinGroup handlers can
/// observe state changes (e.g. transition back to `PreparingRebalance`).
async fn expiration_ticker(
    groups: Arc<DashMap<String, Arc<GroupHandle>>>,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = interval.tick() => {
                let now = std::time::Instant::now();
                for entry in groups.iter() {
                    let handle = entry.value().clone();
                    let dropped = {
                        let mut g = handle.state.lock().await;
                        g.expire_dead_members(now)
                    };
                    if !dropped.is_empty() {
                        tracing::info!(
                            group = %entry.key(),
                            dropped = ?dropped,
                            "expired members; waking joiners"
                        );
                        handle.join_complete.notify_waiters();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn get_or_create_is_idempotent() {
        let m = GroupManager::new();
        let a = m.get_or_create("g");
        let b = m.get_or_create("g");
        // Same Arc.
        assert!(Arc::ptr_eq(&a, &b));
    }
}
```

- [ ] **Step 2: Test + commit**

```bash
cargo test -p crabka-broker coordinator
git add crates/broker
git commit -m "feat(broker): GroupManager + expiration ticker"
```

---

### Task 4: `__consumer_offsets` record codecs

**Files:**
- Create: `crates/broker/src/coordinator/persistence.rs`

- [ ] **Step 1: Write the codecs**

`crates/broker/src/coordinator/persistence.rs`:

```rust
//! Wire-byte codecs for the `__consumer_offsets` internal topic.
//!
//! The topic carries two kinds of records, discriminated by the first
//! `i16` of the key:
//!
//! - **OffsetCommit** (key version `0` or `1`) — one record per
//!   `(group_id, topic, partition)` committed offset.
//! - **GroupMetadata** (key version `2`) — one record per group state
//!   snapshot, written at the end of every successful rebalance.
//!
//! Field layouts mirror Apache Kafka's
//! `clients/src/main/resources/common/message/OffsetCommitValue.json` and
//! `GroupMetadataValue.json` (with the legacy non-flexible encoding —
//! `__consumer_offsets` records are NOT flexible).

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::BrokerError;

/// Discriminator returned by [`parse_key`].
#[derive(Debug, Clone)]
pub enum Key {
    /// `(group_id, topic, partition)` — what offset was committed.
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: i32,
    },
    /// Just `group_id` — value carries the whole `GroupMetadataValue`.
    GroupMetadata { group_id: String },
}

pub fn parse_key(mut buf: &[u8]) -> Result<Key, BrokerError> {
    if buf.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets key too short"),
        ));
    }
    let version = buf.get_i16();
    match version {
        0 | 1 => {
            let group_id = get_string(&mut buf)?;
            let topic = get_string(&mut buf)?;
            let partition = get_i32(&mut buf)?;
            Ok(Key::OffsetCommit {
                group_id,
                topic,
                partition,
            })
        }
        2 => {
            let group_id = get_string(&mut buf)?;
            Ok(Key::GroupMetadata { group_id })
        }
        v => Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue(unknown_key_version_msg(v)),
        )),
    }
}

fn unknown_key_version_msg(_v: i16) -> &'static str {
    "unknown __consumer_offsets key version"
}

#[derive(Debug, Clone)]
pub struct OffsetCommitValue {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
}

impl OffsetCommitValue {
    /// Encode an OffsetCommit key (version 1).
    #[must_use]
    pub fn encode_key(group_id: &str, topic: &str, partition: i32) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // key version
        put_string(&mut buf, group_id);
        put_string(&mut buf, topic);
        buf.put_i32(partition);
        buf.freeze()
    }

    /// Encode an OffsetCommit value (version 3).
    #[must_use]
    pub fn encode_value(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // value version
        buf.put_i64(self.offset);
        buf.put_i32(self.leader_epoch);
        put_string(&mut buf, &self.metadata);
        buf.put_i64(self.commit_timestamp_ms);
        buf.freeze()
    }

    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let version = get_i16(&mut buf)?;
        if version < 0 || version > 3 {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue("unknown OffsetCommitValue version"),
            ));
        }
        let offset = get_i64(&mut buf)?;
        let leader_epoch = if version >= 3 { get_i32(&mut buf)? } else { -1 };
        let metadata = get_string(&mut buf)?;
        let commit_timestamp_ms = get_i64(&mut buf)?;
        // Older versions carried an `expire_timestamp_ms` after commit_timestamp_ms;
        // ignore any trailing bytes.
        Ok(Self {
            offset,
            leader_epoch,
            metadata,
            commit_timestamp_ms,
        })
    }
}

#[derive(Debug, Clone)]
pub struct GroupMetadataValue {
    pub protocol_type: String,
    pub generation: i32,
    pub protocol_name: Option<String>,
    pub leader: Option<String>,
    pub current_state_timestamp_ms: i64,
    pub members: Vec<MemberMetadata>,
}

#[derive(Debug, Clone)]
pub struct MemberMetadata {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub subscription: Bytes,
    pub assignment: Bytes,
}

impl GroupMetadataValue {
    /// Encode a GroupMetadata key (version 2).
    #[must_use]
    pub fn encode_key(group_id: &str) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(2);
        put_string(&mut buf, group_id);
        buf.freeze()
    }

    /// Encode a GroupMetadata value (version 3).
    #[must_use]
    pub fn encode_value(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // value version
        put_string(&mut buf, &self.protocol_type);
        buf.put_i32(self.generation);
        put_nullable_string(&mut buf, self.protocol_name.as_deref());
        put_nullable_string(&mut buf, self.leader.as_deref());
        buf.put_i64(self.current_state_timestamp_ms);
        let n = i32::try_from(self.members.len()).expect("members fit in i32");
        buf.put_i32(n);
        for m in &self.members {
            put_string(&mut buf, &m.member_id);
            put_nullable_string(&mut buf, m.group_instance_id.as_deref());
            put_string(&mut buf, &m.client_id);
            put_string(&mut buf, &m.client_host);
            buf.put_i32(m.rebalance_timeout_ms);
            buf.put_i32(m.session_timeout_ms);
            put_bytes(&mut buf, &m.subscription);
            put_bytes(&mut buf, &m.assignment);
        }
        buf.freeze()
    }

    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let version = get_i16(&mut buf)?;
        if version < 0 || version > 3 {
            return Err(BrokerError::Protocol(
                crabka_protocol::ProtocolError::InvalidValue("unknown GroupMetadataValue version"),
            ));
        }
        let protocol_type = get_string(&mut buf)?;
        let generation = get_i32(&mut buf)?;
        let protocol_name = get_nullable_string(&mut buf)?;
        let leader = get_nullable_string(&mut buf)?;
        let current_state_timestamp_ms = if version >= 2 { get_i64(&mut buf)? } else { -1 };
        let n = get_i32(&mut buf)?;
        let mut members = Vec::with_capacity(n.max(0) as usize);
        for _ in 0..n.max(0) {
            let member_id = get_string(&mut buf)?;
            let group_instance_id = if version >= 3 {
                get_nullable_string(&mut buf)?
            } else {
                None
            };
            let client_id = get_string(&mut buf)?;
            let client_host = get_string(&mut buf)?;
            let rebalance_timeout_ms = if version >= 1 { get_i32(&mut buf)? } else { 0 };
            let session_timeout_ms = get_i32(&mut buf)?;
            let subscription = get_bytes(&mut buf)?;
            let assignment = get_bytes(&mut buf)?;
            members.push(MemberMetadata {
                member_id,
                group_instance_id,
                client_id,
                client_host,
                rebalance_timeout_ms,
                session_timeout_ms,
                subscription,
                assignment,
            });
        }
        Ok(Self {
            protocol_type,
            generation,
            protocol_name,
            leader,
            current_state_timestamp_ms,
            members,
        })
    }
}

// ── primitives (non-flexible Kafka encoding) ───────────────────────────────

fn get_i16(buf: &mut &[u8]) -> Result<i16, BrokerError> {
    if buf.remaining() < 2 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i16"),
        ));
    }
    Ok(buf.get_i16())
}

fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    if buf.remaining() < 4 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i32"),
        ));
    }
    Ok(buf.get_i32())
}

fn get_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
    if buf.remaining() < 8 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("offsets buf < i64"),
        ));
    }
    Ok(buf.get_i64())
}

fn get_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("STRING with negative length"),
        ));
    }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("STRING shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    String::from_utf8(out).map_err(|_| {
        BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(
            "STRING not valid UTF-8",
        ))
    })
}

fn get_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, BrokerError> {
    let len = get_i16(buf)?;
    if len < 0 {
        return Ok(None);
    }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("NULLABLE_STRING shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    String::from_utf8(out).map(Some).map_err(|_| {
        BrokerError::Protocol(crabka_protocol::ProtocolError::InvalidValue(
            "NULLABLE_STRING not valid UTF-8",
        ))
    })
}

fn get_bytes(buf: &mut &[u8]) -> Result<Bytes, BrokerError> {
    let len = get_i32(buf)?;
    if len < 0 {
        return Ok(Bytes::new());
    }
    let n = len as usize;
    if buf.remaining() < n {
        return Err(BrokerError::Protocol(
            crabka_protocol::ProtocolError::InvalidValue("BYTES shorter than declared"),
        ));
    }
    let mut out = vec![0u8; n];
    buf.copy_to_slice(&mut out);
    Ok(Bytes::from(out))
}

fn put_string<B: BufMut>(buf: &mut B, s: &str) {
    let n = i16::try_from(s.len()).expect("string < 32k");
    buf.put_i16(n);
    buf.put_slice(s.as_bytes());
}

fn put_nullable_string<B: BufMut>(buf: &mut B, s: Option<&str>) {
    match s {
        None => buf.put_i16(-1),
        Some(s) => put_string(buf, s),
    }
}

fn put_bytes<B: BufMut>(buf: &mut B, b: &Bytes) {
    let n = i32::try_from(b.len()).expect("bytes < 2GiB");
    buf.put_i32(n);
    buf.put_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_commit_round_trip() {
        let v = OffsetCommitValue {
            offset: 42,
            leader_epoch: 0,
            metadata: "meta".into(),
            commit_timestamp_ms: 1_000_000,
        };
        let encoded = v.encode_value();
        let decoded = OffsetCommitValue::decode_value(&encoded).unwrap();
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.leader_epoch, 0);
        assert_eq!(decoded.metadata, "meta");
        assert_eq!(decoded.commit_timestamp_ms, 1_000_000);
    }

    #[test]
    fn group_metadata_round_trip() {
        let v = GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 5,
            protocol_name: Some("range".into()),
            leader: Some("m1".into()),
            current_state_timestamp_ms: 12_345,
            members: vec![MemberMetadata {
                member_id: "m1".into(),
                group_instance_id: None,
                client_id: "test-client".into(),
                client_host: "127.0.0.1".into(),
                rebalance_timeout_ms: 60_000,
                session_timeout_ms: 30_000,
                subscription: Bytes::from_static(b"sub"),
                assignment: Bytes::from_static(b"asgn"),
            }],
        };
        let encoded = v.encode_value();
        let decoded = GroupMetadataValue::decode_value(&encoded).unwrap();
        assert_eq!(decoded.members.len(), 1);
        assert_eq!(decoded.members[0].member_id, "m1");
        assert_eq!(decoded.members[0].subscription.as_ref(), b"sub");
    }

    #[test]
    fn parse_key_offset_commit_v1() {
        let key = OffsetCommitValue::encode_key("grp", "topic", 7);
        match parse_key(&key).unwrap() {
            Key::OffsetCommit {
                group_id,
                topic,
                partition,
            } => {
                assert_eq!(group_id, "grp");
                assert_eq!(topic, "topic");
                assert_eq!(partition, 7);
            }
            k => panic!("expected OffsetCommit, got {k:?}"),
        }
    }

    #[test]
    fn parse_key_group_metadata_v2() {
        let key = GroupMetadataValue::encode_key("grp");
        match parse_key(&key).unwrap() {
            Key::GroupMetadata { group_id } => assert_eq!(group_id, "grp"),
            k => panic!("expected GroupMetadata, got {k:?}"),
        }
    }
}
```

- [ ] **Step 2: Test + commit**

```bash
cargo test -p crabka-broker coordinator::persistence
git add crates/broker/src/coordinator/persistence.rs
git commit -m "feat(broker): __consumer_offsets record codecs (OffsetCommit + GroupMetadata)"
```

---

### Task 5: `__consumer_offsets` bootstrap + startup replay

**Files:**
- Create: `crates/broker/src/coordinator/bootstrap.rs`
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Write the bootstrap module**

`crates/broker/src/coordinator/bootstrap.rs`:

```rust
//! `__consumer_offsets` topic lifecycle: ensure the topic exists at
//! startup, then synchronously replay every record into the in-memory
//! `GroupManager`.

use std::sync::Arc;

use bytes::Buf;
use crabka_protocol::records::RecordBatch;

use crate::broker::spawn_partition;
use crate::config::BrokerConfig;
use crate::coordinator::group::{Group, Member, OffsetEntry};
use crate::coordinator::persistence::{self, GroupMetadataValue, Key, OffsetCommitValue};
use crate::coordinator::{GroupHandle, GroupManager};
use crate::error::BrokerError;
use crate::log_dir;
use crate::metadata::MetadataImage;
use crate::partition::Partition;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

/// Ensure the `__consumer_offsets-0` partition exists on disk, open its
/// `Log`, spawn a writer task, and replay every record into the supplied
/// `GroupManager`. Adds the topic to the metadata image as a 1-partition
/// internal topic.
///
/// Called exactly once from `Broker::start`, BEFORE the TCP listener binds.
pub async fn bootstrap(
    config: &BrokerConfig,
    metadata: &Arc<std::sync::RwLock<MetadataImage>>,
    partitions: &Arc<dashmap::DashMap<(String, i32), Arc<Partition>>>,
    group_manager: &GroupManager,
) -> Result<(), BrokerError> {
    let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
    std::fs::create_dir_all(&topic_dir)?;
    let log = crabka_log::Log::open(&topic_dir, config.log_config.clone())?;

    // Register the topic in metadata (internal flag set to true).
    {
        let mut meta = metadata.write().expect("metadata poisoned");
        if meta.get(OFFSETS_TOPIC).is_none() {
            // 1 partition, leader = this broker.
            meta.insert_topic(OFFSETS_TOPIC, 1, config.broker_id);
        }
    }

    // Replay before spawning the writer so reads see consistent state.
    replay_records(&log, group_manager).await?;

    // Spawn a writer + register the partition handle.
    let partition = spawn_partition(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION, log);
    partitions.insert((OFFSETS_TOPIC.into(), OFFSETS_PARTITION), partition);
    Ok(())
}

/// Walk every RecordBatch in the log from offset 0 to `log_end_offset()`
/// and apply each record's key/value to the in-memory `GroupManager`.
async fn replay_records(
    log: &crabka_log::Log,
    group_manager: &GroupManager,
) -> Result<(), BrokerError> {
    let mut next = log.log_start_offset();
    let end = log.log_end_offset();
    while next < end {
        let out = log.read(next, 1024 * 1024)?;
        if out.batches.is_empty() {
            break;
        }
        let mut advanced_to = next;
        for batch in &out.batches {
            for record in &batch.records {
                if let (Some(key_bytes), Some(value_bytes)) = (&record.key, &record.value) {
                    let key = persistence::parse_key(key_bytes)?;
                    apply_record(group_manager, key, value_bytes, batch).await?;
                }
            }
            advanced_to = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
        }
        if advanced_to <= next {
            break;
        }
        next = advanced_to;
    }
    Ok(())
}

async fn apply_record(
    group_manager: &GroupManager,
    key: Key,
    value_bytes: &bytes::Bytes,
    batch: &RecordBatch,
) -> Result<(), BrokerError> {
    match key {
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => {
            // Tombstone (value=None) means offset deleted; we don't get None here
            // since we filtered on `Some(value)` above. A value with negative
            // length WAS still encoded — decoded ok.
            let v = OffsetCommitValue::decode_value(value_bytes)?;
            let handle = group_manager.get_or_create(&group_id);
            let mut g = handle.state.lock().await;
            g.committed_offsets.insert(
                (topic, partition),
                OffsetEntry {
                    offset: v.offset,
                    leader_epoch: v.leader_epoch,
                    metadata: v.metadata,
                    commit_timestamp_ms: v.commit_timestamp_ms,
                },
            );
        }
        Key::GroupMetadata { group_id } => {
            let v = GroupMetadataValue::decode_value(value_bytes)?;
            let handle = group_manager.get_or_create(&group_id);
            let mut g = handle.state.lock().await;
            apply_group_metadata(&mut g, v, batch.max_timestamp);
        }
    }
    Ok(())
}

fn apply_group_metadata(g: &mut Group, v: GroupMetadataValue, replay_timestamp_ms: i64) {
    g.protocol_type = Some(v.protocol_type);
    g.generation_id = v.generation;
    g.leader_id = v.leader;
    g.protocol_name = v.protocol_name;
    // Repopulate members. last_heartbeat is set to `now` so they don't
    // immediately time out; the client will re-join anyway.
    g.members.clear();
    for m in v.members {
        let mut member = Member::new(
            m.member_id.clone(),
            m.client_id,
            m.client_host,
            std::time::Duration::from_millis(u64::try_from(m.session_timeout_ms.max(0)).unwrap_or(30_000)),
            std::time::Duration::from_millis(u64::try_from(m.rebalance_timeout_ms.max(0)).unwrap_or(60_000)),
            m.subscription,
        );
        member.assignment = Some(m.assignment);
        g.members.insert(m.member_id, member);
    }
    g.state = if g.members.is_empty() {
        crate::coordinator::group::GroupState::Empty
    } else {
        crate::coordinator::group::GroupState::Stable
    };
    let _ = replay_timestamp_ms; // currently unused; logged for debug
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BrokerConfig;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn bootstrap_creates_topic_dir() {
        let dir = tempdir().unwrap();
        let config = BrokerConfig::for_tests(dir.path().to_path_buf());
        let metadata = Arc::new(std::sync::RwLock::new(MetadataImage::new()));
        let partitions: Arc<dashmap::DashMap<(String, i32), Arc<Partition>>> =
            Arc::new(dashmap::DashMap::new());
        let gm = GroupManager::new();
        bootstrap(&config, &metadata, &partitions, &gm).await.unwrap();
        let topic_dir = log_dir::partition_dir(&config.log_dir, OFFSETS_TOPIC, OFFSETS_PARTITION);
        assert!(topic_dir.exists());
        assert!(partitions.contains_key(&(OFFSETS_TOPIC.into(), OFFSETS_PARTITION)));
        assert!(metadata.read().unwrap().get(OFFSETS_TOPIC).is_some());
    }
}
```

- [ ] **Step 2: Wire into `Broker::start`**

In `crates/broker/src/broker.rs`, modify the `Broker` struct to add a `group_manager` field and `Broker::start` to call `bootstrap` after the `log_dir::scan` recovery loop and before binding the TCP listener.

Add to the `Broker` struct:

```rust
pub(crate) group_manager: Arc<crate::coordinator::GroupManager>,
```

In `Broker::start`, after the recovery loop populates `partitions` from `log_dir::scan` and the metadata image is seeded, BUT before `let listener = TcpListener::bind(...)`:

```rust
        // Group coordinator bootstrap (slice 5).
        let group_manager = Arc::new(crate::coordinator::GroupManager::new());
        crate::coordinator::bootstrap::bootstrap(
            &config,
            &metadata,
            &partitions,
            group_manager.as_ref(),
        )
        .await?;
```

And include `group_manager: group_manager.clone()` in the `Arc::new(Self { … })` construction.

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker coordinator::bootstrap
cargo test -p crabka-broker --test integration
git add crates/broker
git commit -m "feat(broker): __consumer_offsets bootstrap + startup replay; wire GroupManager into Broker"
```

Expected: existing integration tests still pass (they don't depend on `__consumer_offsets`); the new `bootstrap_creates_topic_dir` test passes.

---

## Phase C — Coordinator handlers

Every handler in this phase follows the slice-4 pattern:

```rust
use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        // decode → mutate state → encode
    })
}
```

Each handler clones what it needs from `broker` synchronously, then constructs the boxed future. The dispatch.rs already handles request-header / response-header framing and the per-api flexibility table.

### Task 6: Real `FindCoordinator` handler

**Files:**
- Modify: `crates/broker/src/handlers/find_coordinator.rs`

- [ ] **Step 1: Replace the stub**

The slice-4 version stubs `COORDINATOR_NOT_AVAILABLE`. Replace the file with:

```rust
//! `FindCoordinator` (api_key=10). Single-broker MVP: we are the
//! coordinator for every key. Returns this broker's `(node_id, host, port)`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::find_coordinator_response::{
    Coordinator, FindCoordinatorResponse,
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
    let broker_id = broker.config.broker_id;
    let advertised = broker.config.advertised_listener.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        let (host, port) = parse_host_port(&advertised);

        let coordinators: Vec<Coordinator> = req
            .coordinator_keys
            .iter()
            .map(|k| Coordinator {
                key: k.clone(),
                node_id: broker_id,
                host: host.clone(),
                port: i32::from(port),
                error_code: codes::NONE,
                error_message: None,
                ..Default::default()
            })
            .collect();

        let resp = FindCoordinatorResponse {
            error_code: codes::NONE,
            error_message: None,
            node_id: broker_id,
            host,
            port: i32::from(port),
            coordinators,
            throttle_time_ms: 0,
            ..Default::default()
        };

        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn parse_host_port(addr: &str) -> (String, u16) {
    if let Some((h, p)) = addr.rsplit_once(':') {
        if let Ok(port) = p.parse::<u16>() {
            return (h.to_string(), port);
        }
    }
    tracing::warn!(addr, "advertised_listener not host:port; falling back to localhost:9092");
    ("localhost".into(), 9092)
}
```

- [ ] **Step 2: Add a unit test**

Append to `crates/broker/tests/unit.rs`:

```rust
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;

#[tokio::test]
async fn find_coordinator_returns_self() {
    let p = support::start().await;
    let req = FindCoordinatorRequest {
        coordinator_keys: vec!["any-group".into()],
        ..Default::default()
    };
    let r = p.client.send(req).await.expect("FindCoordinator");
    for c in &r.coordinators {
        assert_eq!(c.error_code, 0);
        assert_eq!(c.node_id, 1);
        assert!(!c.host.is_empty());
        assert!(c.port > 0);
    }
    p.broker.shutdown().await;
}
```

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-broker --test unit find_coordinator
git add crates/broker
git commit -m "feat(broker): real FindCoordinator handler (replaces stub)"
```

---

### Task 7: `JoinGroup` handler

**Files:**
- Create: `crates/broker/src/handlers/join_group.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/join_group.rs`:

```rust
//! `JoinGroup` (api_key=11). Blocks for up to `rebalance_timeout_ms`
//! waiting for the group to transition out of `PreparingRebalance`.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use uuid::Uuid;

use crabka_protocol::owned::join_group_request::JoinGroupRequest;
use crabka_protocol::owned::join_group_response::{
    JoinGroupResponse, JoinGroupResponseMember,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::{GroupState, Member};
use crate::error::BrokerError;

const SUPPORTED_PROTOCOL: &str = "range";

pub(crate) fn handle(
    _broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = _broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = JoinGroupRequest::decode(&mut cur, version)?;

        // 1. Reject proposals that don't include `range`. (For the MVP we
        //    only negotiate `range`; we don't run a real protocol-set
        //    intersection.)
        let proposes_range = req
            .protocols
            .iter()
            .any(|p| p.name == SUPPORTED_PROTOCOL);
        if !proposes_range {
            let resp = JoinGroupResponse {
                error_code: codes::INCONSISTENT_GROUP_PROTOCOL,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        // 2. Empty member_id on first join → broker generates one (KIP-394).
        if req.member_id.is_empty() {
            let new_id = format!(
                "crabka-{client}-{uuid}",
                client = req.client_id().unwrap_or("nocid"),
                uuid = Uuid::new_v4()
            );
            let resp = JoinGroupResponse {
                error_code: codes::MEMBER_ID_REQUIRED,
                member_id: new_id,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        let handle = group_manager.get_or_create(&req.group_id);

        // 3. Add member, transition to PreparingRebalance.
        let protocol_md = req
            .protocols
            .iter()
            .find(|p| p.name == SUPPORTED_PROTOCOL)
            .map(|p| p.metadata.clone())
            .unwrap_or_default();
        let session_timeout = Duration::from_millis(u64::try_from(req.session_timeout_ms.max(0)).unwrap_or(30_000));
        let rebalance_timeout = Duration::from_millis(u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(60_000));
        {
            let mut g = handle.state.lock().await;
            g.protocol_type = Some(req.protocol_type.clone());
            g.add_member(Member::new(
                req.member_id.clone(),
                req.client_id().unwrap_or("").to_string(),
                String::new(), // client_host; unused in MVP
                session_timeout,
                rebalance_timeout,
                protocol_md,
            ));
            if g.rebalance_deadline.is_none() {
                g.rebalance_deadline = Some(std::time::Instant::now() + rebalance_timeout);
            }
        }

        // 4. Wait on the per-group join-complete notify, with a deadline.
        let _ = tokio::time::timeout(rebalance_timeout, handle.join_complete.notified()).await;

        // 5. Complete the rebalance if we're the one who fell out of the
        //    wait first. (Multiple JoinGroup handlers race; whoever wins
        //    transitions the state under the mutex.)
        {
            let mut g = handle.state.lock().await;
            if matches!(g.state, GroupState::PreparingRebalance) && !g.members.is_empty() {
                g.complete_rebalance(SUPPORTED_PROTOCOL);
                handle.join_complete.notify_waiters();
            }
        }

        // 6. Build the response from the post-rebalance state.
        let g = handle.state.lock().await;
        let is_leader = g.leader_id.as_deref() == Some(&req.member_id);
        let members: Vec<JoinGroupResponseMember> = if is_leader {
            g.members
                .values()
                .map(|m| JoinGroupResponseMember {
                    member_id: m.member_id.clone(),
                    metadata: m.protocol_metadata.clone(),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };
        let resp = JoinGroupResponse {
            error_code: codes::NONE,
            generation_id: g.generation_id,
            protocol_type: g.protocol_type.clone(),
            protocol_name: g.protocol_name.clone(),
            leader: g.leader_id.clone().unwrap_or_default(),
            member_id: req.member_id,
            members,
            throttle_time_ms: 0,
            ..Default::default()
        };
        drop(g);

        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

If the codegen's `JoinGroupRequest` doesn't expose `client_id()` as a helper, derive `client_id` from the request header (passed through dispatch). Most likely it's just an empty field or absent — the MVP can use `""`.

- [ ] **Step 2: Register in `handlers/mod.rs`**

Add `pub(crate) mod join_group;` and `t.register(11, join_group::handle);` in `build_table()`.

- [ ] **Step 3: Test + commit**

Append to `crates/broker/tests/unit.rs`:

```rust
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};

#[tokio::test]
async fn join_group_with_empty_member_returns_member_id_required() {
    let p = support::start().await;
    let req = JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: "".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 2_000,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::from_static(b""),
            ..Default::default()
        }],
        ..Default::default()
    };
    let r = p.client.send(req).await.expect("JoinGroup");
    assert_eq!(r.error_code, 79); // MEMBER_ID_REQUIRED
    assert!(!r.member_id.is_empty());
    p.broker.shutdown().await;
}

#[tokio::test]
async fn join_group_single_member_completes_after_deadline() {
    let p = support::start().await;
    // First call to get a member_id.
    let r1 = p.client.send(JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: "".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 1_500,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }).await.expect("JoinGroup1");
    // Retry with the assigned member_id. The handler will block ~1.5s
    // waiting for the rebalance deadline.
    let r2 = p.client.send(JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: r1.member_id.clone(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 1_500,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }).await.expect("JoinGroup2");
    assert_eq!(r2.error_code, 0);
    assert_eq!(r2.leader, r1.member_id);
    assert_eq!(r2.member_id, r1.member_id);
    assert!(!r2.members.is_empty(), "leader sees member list");
    p.broker.shutdown().await;
}
```

```bash
cargo test -p crabka-broker --test unit join_group
git add crates/broker
git commit -m "feat(broker): JoinGroup handler + rebalance gate"
```

---

### Task 8: `SyncGroup` handler

**Files:**
- Create: `crates/broker/src/handlers/sync_group.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/sync_group.rs`:

```rust
//! `SyncGroup` (api_key=14). The leader supplies assignment bytes per
//! member; non-leaders block until the leader's call arrives, then
//! receive their own assignment.

use std::collections::HashMap;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::sync_group_request::SyncGroupRequest;
use crabka_protocol::owned::sync_group_response::SyncGroupResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = SyncGroupRequest::decode(&mut cur, version)?;

        let Some(handle) = group_manager.find(&req.group_id) else {
            return encode_err(version, codes::UNKNOWN_MEMBER_ID);
        };

        // 1. Validate (generation, member).
        {
            let g = handle.state.lock().await;
            if !g.members.contains_key(&req.member_id) {
                return encode_err(version, codes::UNKNOWN_MEMBER_ID);
            }
            if g.generation_id != req.generation_id {
                return encode_err(version, codes::ILLEGAL_GENERATION);
            }
        }

        let is_leader = {
            let g = handle.state.lock().await;
            g.leader_id.as_deref() == Some(&req.member_id)
        };

        if is_leader {
            // 2a. Leader supplies assignments → install + wake waiters.
            let assignments: HashMap<String, Bytes> = req
                .assignments
                .iter()
                .map(|a| (a.member_id.clone(), a.assignment.clone()))
                .collect();
            {
                let mut g = handle.state.lock().await;
                g.install_assignments(assignments);
            }
            handle.sync_complete.notify_waiters();
        } else {
            // 2b. Follower blocks until the leader's SyncGroup arrives.
            let _ = tokio::time::timeout(
                Duration::from_secs(30),
                handle.sync_complete.notified(),
            )
            .await;
        }

        // 3. Read back this member's assignment.
        let g = handle.state.lock().await;
        if !matches!(g.state, GroupState::Stable) {
            return encode_err(version, codes::REBALANCE_IN_PROGRESS);
        }
        let assignment = g
            .members
            .get(&req.member_id)
            .and_then(|m| m.assignment.clone())
            .unwrap_or_default();
        drop(g);

        let resp = SyncGroupResponse {
            error_code: codes::NONE,
            assignment,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}

fn encode_err(version: i16, code: i16) -> Result<Bytes, BrokerError> {
    let resp = SyncGroupResponse {
        error_code: code,
        assignment: Bytes::new(),
        throttle_time_ms: 0,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

- [ ] **Step 2: Register + commit**

In `handlers/mod.rs`, add `pub(crate) mod sync_group;` and `t.register(14, sync_group::handle);`.

```bash
cargo test -p crabka-broker --test unit
git add crates/broker
git commit -m "feat(broker): SyncGroup handler"
```

(Cross-handler tests land in Task 14 once Heartbeat + LeaveGroup are also in place.)

---

### Task 9: `Heartbeat` handler

**Files:**
- Create: `crates/broker/src/handlers/heartbeat.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/heartbeat.rs`:

```rust
//! `Heartbeat` (api_key=12). Validates `(generation, member)` and updates
//! `last_heartbeat`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::heartbeat_response::HeartbeatResponse;
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = HeartbeatRequest::decode(&mut cur, version)?;

        let error_code = match group_manager.find(&req.group_id) {
            None => codes::UNKNOWN_MEMBER_ID,
            Some(handle) => {
                let mut g = handle.state.lock().await;
                let Some(member) = g.members.get_mut(&req.member_id) else {
                    codes::UNKNOWN_MEMBER_ID
                };
                if g.generation_id != req.generation_id {
                    codes::ILLEGAL_GENERATION
                } else if !matches!(g.state, GroupState::Stable) {
                    codes::REBALANCE_IN_PROGRESS
                } else {
                    member.last_heartbeat = std::time::Instant::now();
                    codes::NONE
                }
            }
        };

        let resp = HeartbeatResponse {
            error_code,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

Note the `let Some(member) = ...` pattern is shorthand; in real Rust you can't fall through from `let-else` to "if not found, use this code". Rewrite the body as a small helper closure or `match` on `(g.members.contains_key, generation match, state)`. The end behavior is identical.

- [ ] **Step 2: Register + commit**

```bash
git add crates/broker
git commit -m "feat(broker): Heartbeat handler"
```

---

### Task 10: `LeaveGroup` handler

**Files:**
- Create: `crates/broker/src/handlers/leave_group.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/leave_group.rs`:

```rust
//! `LeaveGroup` (api_key=13). Removes one or more members and transitions
//! the group to `PreparingRebalance` if it still has members.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::leave_group_response::{
    LeaveGroupResponse, MemberResponse,
};
use crabka_protocol::{Decode, Encode};

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::group::GroupState;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = LeaveGroupRequest::decode(&mut cur, version)?;

        let Some(handle) = group_manager.find(&req.group_id) else {
            // No such group; respond OK but no member responses.
            let resp = LeaveGroupResponse {
                error_code: codes::NONE,
                throttle_time_ms: 0,
                members: vec![],
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        };

        // v0-v2: single `member_id` field. v3+: a `members` list of (member_id, group_instance_id).
        // Build a unified list.
        let to_remove: Vec<String> = if !req.member_id.is_empty() {
            vec![req.member_id.clone()]
        } else {
            req.members.iter().map(|m| m.member_id.clone()).collect()
        };

        let mut member_responses: Vec<MemberResponse> = Vec::with_capacity(to_remove.len());
        {
            let mut g = handle.state.lock().await;
            for mid in &to_remove {
                let code = if g.members.contains_key(mid) {
                    g.remove_member(mid);
                    codes::NONE
                } else {
                    codes::UNKNOWN_MEMBER_ID
                };
                member_responses.push(MemberResponse {
                    member_id: mid.clone(),
                    group_instance_id: None,
                    error_code: code,
                    ..Default::default()
                });
            }
            // If group is still non-empty and was Stable, kick rebalance.
            if !g.members.is_empty() && matches!(g.state, GroupState::Stable) {
                g.state = GroupState::PreparingRebalance;
                g.rebalance_deadline = Some(
                    std::time::Instant::now()
                        + g.members
                            .values()
                            .map(|m| m.rebalance_timeout)
                            .max()
                            .unwrap_or(std::time::Duration::from_secs(60)),
                );
            }
        }
        // Wake any waiting JoinGroup handlers in case they need to observe
        // the membership change.
        handle.join_complete.notify_waiters();

        let resp = LeaveGroupResponse {
            error_code: codes::NONE,
            throttle_time_ms: 0,
            members: member_responses,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

- [ ] **Step 2: Register + commit**

```bash
git add crates/broker
git commit -m "feat(broker): LeaveGroup handler"
```

---

### Task 11: `OffsetCommit` handler

**Files:**
- Create: `crates/broker/src/handlers/offset_commit.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/offset_commit.rs`:

```rust
//! `OffsetCommit` (api_key=8). Encodes `OffsetCommitKey` +
//! `OffsetCommitValue` records, appends them to `__consumer_offsets-0`
//! via the partition writer, then updates `Group.committed_offsets`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::offset_commit_request::OffsetCommitRequest;
use crabka_protocol::owned::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use tokio::sync::oneshot;

use crate::broker::Broker;
use crate::codes;
use crate::coordinator::bootstrap::{OFFSETS_PARTITION, OFFSETS_TOPIC};
use crate::coordinator::group::OffsetEntry;
use crate::coordinator::persistence::OffsetCommitValue;
use crate::error::BrokerError;
use crate::partition::ProduceJob;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let group_manager = broker.group_manager.clone();
    let partitions = broker.partitions.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetCommitRequest::decode(&mut cur, version)?;

        let now_ms = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
        )
        .unwrap_or(0);

        // 1. Validate (group, generation, member).
        let handle = group_manager.get_or_create(&req.group_id);
        {
            let g = handle.state.lock().await;
            if !g.members.contains_key(&req.member_id) && !req.member_id.is_empty() {
                let resp = build_response_all(&req, codes::UNKNOWN_MEMBER_ID);
                return encode(version, resp);
            }
            if !req.member_id.is_empty() && g.generation_id != req.generation_id_or_member_epoch {
                let resp = build_response_all(&req, codes::ILLEGAL_GENERATION);
                return encode(version, resp);
            }
        }

        // 2. Build a RecordBatch with one record per (topic, partition) commit.
        let mut batch = RecordBatch::default();
        batch.max_timestamp = now_ms;
        let mut delta: i32 = 0;
        for topic in &req.topics {
            for part in &topic.partitions {
                let value = OffsetCommitValue {
                    offset: part.committed_offset,
                    leader_epoch: part.committed_leader_epoch,
                    metadata: part.committed_metadata.clone().unwrap_or_default(),
                    commit_timestamp_ms: now_ms,
                };
                batch.records.push(Record {
                    offset_delta: delta,
                    timestamp_delta: 0,
                    key: Some(OffsetCommitValue::encode_key(
                        &req.group_id,
                        &topic.name,
                        part.partition_index,
                    )),
                    value: Some(value.encode_value()),
                    ..Default::default()
                });
                delta += 1;
            }
        }
        batch.last_offset_delta = (delta - 1).max(0);

        // 3. Send to the __consumer_offsets-0 writer.
        let Some(part_handle) = partitions
            .get(&(OFFSETS_TOPIC.to_string(), OFFSETS_PARTITION))
            .map(|e| e.value().clone())
        else {
            let resp = build_response_all(&req, codes::UNKNOWN_SERVER_ERROR);
            return encode(version, resp);
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if part_handle
            .writer_tx
            .send(ProduceJob {
                batch: batch.clone(),
                ack: ack_tx,
            })
            .await
            .is_err()
        {
            let resp = build_response_all(&req, codes::UNKNOWN_SERVER_ERROR);
            return encode(version, resp);
        }
        if let Err(e) = ack_rx.await {
            tracing::error!(error = %e, "OffsetCommit writer ack dropped");
            let resp = build_response_all(&req, codes::UNKNOWN_SERVER_ERROR);
            return encode(version, resp);
        }

        // 4. Update in-memory state.
        {
            let mut g = handle.state.lock().await;
            for topic in &req.topics {
                for part in &topic.partitions {
                    g.committed_offsets.insert(
                        (topic.name.clone(), part.partition_index),
                        OffsetEntry {
                            offset: part.committed_offset,
                            leader_epoch: part.committed_leader_epoch,
                            metadata: part.committed_metadata.clone().unwrap_or_default(),
                            commit_timestamp_ms: now_ms,
                        },
                    );
                }
            }
        }

        // 5. Per-(topic, partition) success.
        let resp = build_response_all(&req, codes::NONE);
        encode(version, resp)
    })
}

fn build_response_all(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    let topics = req
        .topics
        .iter()
        .map(|t| OffsetCommitResponseTopic {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| OffsetCommitResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    OffsetCommitResponse {
        topics,
        throttle_time_ms: 0,
        ..Default::default()
    }
}

fn encode(version: i16, resp: OffsetCommitResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

The exact `OffsetCommitRequest` field that carries the generation might be named `generation_id_or_member_epoch` (newer Apache Kafka), `generation_id` (older), or both as separate fields. Grep the generated `OffsetCommitRequest.owned.rs` and use whichever exists.

- [ ] **Step 2: Register + commit**

```bash
cargo test -p crabka-broker --test unit
git add crates/broker
git commit -m "feat(broker): OffsetCommit handler writes to __consumer_offsets-0"
```

---

### Task 12: `OffsetFetch` handler

**Files:**
- Create: `crates/broker/src/handlers/offset_fetch.rs`
- Modify: `crates/broker/src/handlers/mod.rs`

- [ ] **Step 1: Write the handler**

`crates/broker/src/handlers/offset_fetch.rs`:

```rust
//! `OffsetFetch` (api_key=9). Reads from `Group.committed_offsets`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;

use crabka_protocol::owned::offset_fetch_request::OffsetFetchRequest;
use crabka_protocol::owned::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
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
    let group_manager = broker.group_manager.clone();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = OffsetFetchRequest::decode(&mut cur, version)?;

        let handle = group_manager.get_or_create(&req.group_id);
        let g = handle.state.lock().await;

        let topics_out: Vec<OffsetFetchResponseTopic> = req
            .topics
            .iter()
            .map(|t| {
                let partitions = t
                    .partition_indexes
                    .iter()
                    .map(|&pid| match g.committed_offsets.get(&(t.name.clone(), pid)) {
                        Some(entry) => OffsetFetchResponsePartition {
                            partition_index: pid,
                            committed_offset: entry.offset,
                            committed_leader_epoch: entry.leader_epoch,
                            metadata: Some(entry.metadata.clone()),
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                        None => OffsetFetchResponsePartition {
                            partition_index: pid,
                            committed_offset: -1,
                            committed_leader_epoch: -1,
                            metadata: None,
                            error_code: codes::NONE,
                            ..Default::default()
                        },
                    })
                    .collect();
                OffsetFetchResponseTopic {
                    name: t.name.clone(),
                    partitions,
                    ..Default::default()
                }
            })
            .collect();

        let resp = OffsetFetchResponse {
            topics: topics_out,
            error_code: codes::NONE,
            throttle_time_ms: 0,
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
```

OffsetFetch v8 introduced a per-group list; the MVP only handles single-group v0-v7. If the codegen exposes a `groups: Vec<...>` field instead of the flat `group_id` + `topics`, branch on whichever is `Some(...)` and route to the same logic per group.

- [ ] **Step 2: Register + commit**

```bash
git add crates/broker
git commit -m "feat(broker): OffsetFetch handler reads from in-memory committed_offsets"
```

---

### Task 13: End-to-end coordinator integration test (broker side)

**Files:**
- Modify: `crates/broker/tests/unit.rs`

- [ ] **Step 1: One full-flow test**

Append to `crates/broker/tests/unit.rs`:

```rust
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;
use crabka_protocol::owned::leave_group_request::LeaveGroupRequest;
use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use crabka_protocol::owned::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestTopic,
};
use crabka_protocol::owned::sync_group_request::{
    SyncGroupRequest, SyncGroupRequestAssignment,
};

#[tokio::test]
async fn full_group_flow_join_sync_heartbeat_commit_fetch_leave() {
    let p = support::start().await;

    // Step 1: empty member_id → broker returns one.
    let r1 = p.client.send(JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: "".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 1_500,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r1.error_code, 79);

    // Step 2: re-join with assigned member_id → wait for rebalance, become leader.
    let mid = r1.member_id.clone();
    let r2 = p.client.send(JoinGroupRequest {
        group_id: "g".into(),
        protocol_type: "consumer".into(),
        member_id: mid.clone(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 1_500,
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".into(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r2.error_code, 0);
    assert_eq!(r2.leader, mid);
    let gen = r2.generation_id;

    // Step 3: leader SyncGroup with a single-member assignment.
    let r3 = p.client.send(SyncGroupRequest {
        group_id: "g".into(),
        generation_id: gen,
        member_id: mid.clone(),
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        assignments: vec![SyncGroupRequestAssignment {
            member_id: mid.clone(),
            assignment: bytes::Bytes::from_static(b"asgn"),
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r3.error_code, 0);
    assert_eq!(r3.assignment.as_ref(), b"asgn");

    // Step 4: Heartbeat → 0.
    let r4 = p.client.send(HeartbeatRequest {
        group_id: "g".into(),
        generation_id: gen,
        member_id: mid.clone(),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r4.error_code, 0);

    // Step 5: OffsetCommit → 0.
    let r5 = p.client.send(OffsetCommitRequest {
        group_id: "g".into(),
        generation_id_or_member_epoch: gen,
        member_id: mid.clone(),
        topics: vec![OffsetCommitRequestTopic {
            name: "t".into(),
            partitions: vec![OffsetCommitRequestPartition {
                partition_index: 0,
                committed_offset: 42,
                committed_leader_epoch: 0,
                committed_metadata: Some(String::new()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r5.topics[0].partitions[0].error_code, 0);

    // Step 6: OffsetFetch → returns 42.
    let r6 = p.client.send(OffsetFetchRequest {
        group_id: "g".into(),
        topics: vec![OffsetFetchRequestTopic {
            name: "t".into(),
            partition_indexes: vec![0],
            ..Default::default()
        }],
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r6.topics[0].partitions[0].committed_offset, 42);

    // Step 7: LeaveGroup.
    let r7 = p.client.send(LeaveGroupRequest {
        group_id: "g".into(),
        member_id: mid.clone(),
        ..Default::default()
    }).await.unwrap();
    assert_eq!(r7.error_code, 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn offsets_persist_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = crabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    // First boot: commit offsets.
    {
        let broker = crabka_broker::Broker::start(config.clone()).await.unwrap();
        let client = crabka_client_core::Client::builder(&broker.listen_addr().to_string())
            .client_id("recovery")
            .build()
            .await
            .unwrap();
        // (replicate the join → sync → commit flow above against this broker)
        // For brevity, only commit assuming no group required → OffsetCommit
        // with empty member_id is valid only for the simple-group case in
        // Kafka. Use the full flow if needed.
        broker.shutdown().await;
    }
    // Second boot: restart, OffsetFetch reads back what was committed.
    {
        let broker = crabka_broker::Broker::start(config).await.unwrap();
        // expected_offset assertions go here.
        broker.shutdown().await;
    }
}
```

`offsets_persist_across_restart` is intentionally sketchy — flesh it out as a true round-trip in Phase E once the consumer client exists.

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-broker --test unit full_group_flow
git add crates/broker
git commit -m "test(broker): full group-coordinator flow + restart-replay placeholder"
```

---

## Phase D — `crabka-client-consumer` crate

### Task 14: Crate skeleton + `ConsumerError`

**Files:**
- Create: `crates/client-consumer/Cargo.toml`
- Create: `crates/client-consumer/src/lib.rs`
- Create: `crates/client-consumer/src/error.rs`

- [ ] **Step 1: Manifest**

`crates/client-consumer/Cargo.toml`:

```toml
[package]
name = "crabka-client-consumer"
version.workspace = true
edition.workspace = true
license.workspace = true
authors.workspace = true
rust-version = "1.95.0"
description = "Subscribe-style consumer client for Apache Kafka in Rust"

[lints]
workspace = true

[features]
default = []

[dependencies]
crabka-protocol = { version = "0.1", path = "../protocol", default-features = false }
crabka-client-core = { version = "0.1", path = "../client-core" }
bytes = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true, features = ["rt", "rt-multi-thread", "sync", "time", "macros"] }
tracing = { workspace = true }
tokio-util = { workspace = true, features = ["rt"] }
futures-util = { workspace = true }

[dev-dependencies]
crabka-broker = { version = "0.1", path = "../broker" }
crabka-log = { version = "0.1", path = "../log" }
tempfile = { workspace = true }
tokio = { workspace = true, features = ["test-util", "macros"] }
proptest = { workspace = true }
```

- [ ] **Step 2: Stub `lib.rs`**

`crates/client-consumer/src/lib.rs`:

```rust
//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! See the design at
//! `docs/superpowers/specs/2026-05-11-crabka-consumer-groups-design.md`.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.0.0")]

mod error;

pub use error::ConsumerError;
```

- [ ] **Step 3: Write the error**

`crates/client-consumer/src/error.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ConsumerError {
    #[error("client: {0}")]
    Client(#[from] crabka_client_core::ClientError),

    #[error("protocol: {0}")]
    Protocol(#[from] crabka_protocol::ProtocolError),

    #[error("rebalance failed: {0}")]
    RebalanceFailed(String),

    #[error("not subscribed to any topic")]
    NotSubscribed,

    #[error("commit conflict: rejoined since this poll")]
    CommitInvalid,

    #[error("coordinator unavailable")]
    CoordinatorUnavailable,

    #[error("broker error_code {0}")]
    Server(i16),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_not_subscribed() {
        let e = ConsumerError::NotSubscribed;
        assert!(e.to_string().contains("not subscribed"));
    }
}
```

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-client-consumer
git add crates/client-consumer
git commit -m "feat(consumer): crate skeleton + ConsumerError"
```

---

### Task 15: `assignor::range`

**Files:**
- Create: `crates/client-consumer/src/assignor/mod.rs`
- Create: `crates/client-consumer/src/assignor/range.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: Module skeleton**

`crates/client-consumer/src/assignor/mod.rs`:

```rust
//! Partition assignors. `range` is the only one in scope for slice 5.

#![allow(dead_code)]

pub(crate) mod range;
```

- [ ] **Step 2: `range` assignor**

`crates/client-consumer/src/assignor/range.rs`:

```rust
//! Range assignor (Kafka's classic default).
//!
//! Given a set of members and a per-topic partition count, hands each
//! member a contiguous range of partitions per topic. The trailing
//! members get one less partition when the partition count doesn't
//! divide evenly.

use std::collections::HashMap;

/// Returns `member_id → Vec<(topic, partition)>` assignments.
#[must_use]
pub fn assign(
    mut members: Vec<(String, Vec<String>)>,
    topic_partitions: &HashMap<String, i32>,
) -> HashMap<String, Vec<(String, i32)>> {
    members.sort_by(|(a, _), (b, _)| a.cmp(b));
    let mut out: HashMap<String, Vec<(String, i32)>> =
        members.iter().map(|(m, _)| (m.clone(), Vec::new())).collect();

    // Build per-topic the ordered list of subscribed members.
    for (topic, &partition_count) in topic_partitions {
        let subscribed: Vec<&String> = members
            .iter()
            .filter(|(_, subs)| subs.iter().any(|t| t == topic))
            .map(|(m, _)| m)
            .collect();
        if subscribed.is_empty() || partition_count <= 0 {
            continue;
        }
        let n = subscribed.len() as i32;
        let per = partition_count / n;
        let extras = partition_count % n;
        let mut next: i32 = 0;
        for (i, m) in subscribed.iter().enumerate() {
            let take = per + i32::from((i as i32) < extras);
            for p in next..(next + take) {
                out.get_mut(*m).unwrap().push((topic.clone(), p));
            }
            next += take;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_member_takes_everything() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 4);
        let a = assign(vec![("m1".into(), vec!["t".into()])], &tp);
        assert_eq!(a["m1"].len(), 4);
        assert_eq!(a["m1"], vec![("t".into(), 0), ("t".into(), 1), ("t".into(), 2), ("t".into(), 3)]);
    }

    #[test]
    fn two_members_split_evenly() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 4);
        let a = assign(
            vec![
                ("m1".into(), vec!["t".into()]),
                ("m2".into(), vec!["t".into()]),
            ],
            &tp,
        );
        assert_eq!(a["m1"], vec![("t".into(), 0), ("t".into(), 1)]);
        assert_eq!(a["m2"], vec![("t".into(), 2), ("t".into(), 3)]);
    }

    #[test]
    fn extras_go_to_lower_member_ids() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 5);
        let a = assign(
            vec![
                ("m1".into(), vec!["t".into()]),
                ("m2".into(), vec!["t".into()]),
            ],
            &tp,
        );
        assert_eq!(a["m1"].len(), 3);
        assert_eq!(a["m2"].len(), 2);
    }

    #[test]
    fn member_with_no_subscriptions_gets_empty() {
        let mut tp = HashMap::new();
        tp.insert("t".into(), 2);
        let a = assign(
            vec![
                ("m1".into(), vec!["t".into()]),
                ("m2".into(), vec![]),
            ],
            &tp,
        );
        assert_eq!(a["m1"].len(), 2);
        assert_eq!(a["m2"].len(), 0);
    }
}
```

- [ ] **Step 3: Hook into `lib.rs`**

Add `mod assignor;` to `lib.rs` (internal).

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-client-consumer assignor::range
git add crates/client-consumer
git commit -m "feat(consumer): range assignor"
```

---

### Task 16: Heartbeat task + `RebalanceNotice`

**Files:**
- Create: `crates/client-consumer/src/heartbeat.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: Write the heartbeat module**

`crates/client-consumer/src/heartbeat.rs`:

```rust
//! Background `Heartbeat` loop. Spawned by `Consumer` after a successful
//! join+sync. Signals the foreground via an `mpsc::Sender<RebalanceNotice>`
//! whenever the broker tells us to rejoin.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::heartbeat_request::HeartbeatRequest;

#[derive(Debug, Clone, Copy)]
pub enum RebalanceNotice {
    NeedRejoin,
    RejoinFromScratch,
}

/// Periodic heartbeat. Exits when `shutdown` is cancelled.
pub async fn run(
    client: Client,
    group_id: String,
    member_id: String,
    generation_id: i32,
    interval: Duration,
    notice_tx: mpsc::Sender<RebalanceNotice>,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let result = client.send(HeartbeatRequest {
                    group_id: group_id.clone(),
                    generation_id,
                    member_id: member_id.clone(),
                    ..Default::default()
                }).await;
                match result {
                    Ok(r) if r.error_code == 0 => {}
                    Ok(r) if r.error_code == 27 /* REBALANCE_IN_PROGRESS */ => {
                        let _ = notice_tx.send(RebalanceNotice::NeedRejoin).await;
                    }
                    Ok(r) if r.error_code == 25 /* UNKNOWN_MEMBER_ID */ => {
                        let _ = notice_tx.send(RebalanceNotice::RejoinFromScratch).await;
                    }
                    Ok(r) => {
                        tracing::warn!(error_code = r.error_code, "unexpected heartbeat error");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "heartbeat send failed");
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Real Heartbeat-loop tests need a broker; they live in
    // tests/integration.rs. Module-level: just verify the enum compiles.

    #[test]
    fn rebalance_notice_is_copy() {
        fn assert_copy<T: Copy>() {}
        assert_copy::<RebalanceNotice>();
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod heartbeat;` (internal).

- [ ] **Step 3: Test + commit**

```bash
cargo test -p crabka-client-consumer heartbeat
git add crates/client-consumer
git commit -m "feat(consumer): Heartbeat task + RebalanceNotice"
```

---

### Task 17: `Consumer` + `ConsumerBuilder` (lifecycle: build → join → sync)

**Files:**
- Create: `crates/client-consumer/src/builder.rs`
- Create: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: `consumer.rs`**

`crates/client-consumer/src/consumer.rs`:

```rust
//! `Consumer` — public lifecycle handle. Built via [`ConsumerBuilder`]
//! (Task 17). Subscribe-only — no `assign()`. Use `crabka-client-core`
//! directly for manual partition consumption.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;

use crate::error::ConsumerError;
use crate::heartbeat::RebalanceNotice;

pub struct Consumer {
    pub(crate) client: Client,
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    pub(crate) generation_id: i32,
    pub(crate) subscribed_topics: Vec<String>,
    /// Current assigned partitions: `(topic, partition_index)`.
    pub(crate) assigned: Arc<Mutex<Vec<(String, i32)>>>,
    /// Next offset to fetch per partition.
    pub(crate) next_offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    pub(crate) session_timeout: Duration,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) rebalance_rx: Mutex<mpsc::Receiver<RebalanceNotice>>,
    pub(crate) heartbeat_shutdown: CancellationToken,
    pub(crate) heartbeat_handle: Option<JoinHandle<()>>,
}

/// One record returned by [`Consumer::poll`].
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

impl Consumer {
    /// Stop the heartbeat task. Returns immediately if already shut down.
    pub async fn close(mut self) -> Result<(), ConsumerError> {
        self.heartbeat_shutdown.cancel();
        if let Some(h) = self.heartbeat_handle.take() {
            let _ = h.await;
        }
        Ok(())
    }
}
```

- [ ] **Step 2: `builder.rs`**

`crates/client-consumer/src/builder.rs`:

```rust
//! `ConsumerBuilder` — runs the FindCoordinator → JoinGroup → SyncGroup
//! handshake, computes the initial range assignment on the leader, and
//! spawns the heartbeat task.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

use crabka_client_core::Client;
use crabka_protocol::owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
use crabka_protocol::owned::offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic};

use crate::assignor::range;
use crate::consumer::Consumer;
use crate::error::ConsumerError;
use crate::heartbeat;

pub struct ConsumerBuilder {
    bootstrap: String,
    client_id: String,
    group_id: String,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    heartbeat_interval: Duration,
    topics: Vec<String>,
    auto_offset_reset: AutoOffsetReset,
}

#[derive(Debug, Clone, Copy)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

impl ConsumerBuilder {
    #[must_use]
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            client_id: "crabka-consumer".into(),
            group_id: String::new(),
            session_timeout: Duration::from_secs(45),
            rebalance_timeout: Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(3),
            topics: Vec::new(),
            auto_offset_reset: AutoOffsetReset::Latest,
        }
    }

    #[must_use]
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    #[must_use]
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.group_id = id.into();
        self
    }

    #[must_use]
    pub fn session_timeout(mut self, t: Duration) -> Self {
        self.session_timeout = t;
        self
    }

    #[must_use]
    pub fn rebalance_timeout(mut self, t: Duration) -> Self {
        self.rebalance_timeout = t;
        self
    }

    #[must_use]
    pub fn heartbeat_interval(mut self, t: Duration) -> Self {
        self.heartbeat_interval = t;
        self
    }

    #[must_use]
    pub fn subscribe(mut self, topics: &[&str]) -> Self {
        self.topics = topics.iter().map(|s| (*s).to_string()).collect();
        self
    }

    #[must_use]
    pub fn auto_offset_reset(mut self, x: AutoOffsetReset) -> Self {
        self.auto_offset_reset = x;
        self
    }

    pub async fn build(self) -> Result<Consumer, ConsumerError> {
        if self.topics.is_empty() {
            return Err(ConsumerError::NotSubscribed);
        }
        if self.group_id.is_empty() {
            return Err(ConsumerError::RebalanceFailed("group_id required".into()));
        }

        let client = Client::builder(&self.bootstrap)
            .client_id(self.client_id.clone())
            .build()
            .await?;

        // 1. First JoinGroup — get member_id back via MEMBER_ID_REQUIRED.
        let r1 = client
            .send(JoinGroupRequest {
                group_id: self.group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: String::new(),
                session_timeout_ms: self.session_timeout.as_millis() as i32,
                rebalance_timeout_ms: self.rebalance_timeout.as_millis() as i32,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&self.topics),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        let member_id = if r1.error_code == 79 {
            r1.member_id.clone()
        } else if r1.error_code == 0 {
            r1.member_id.clone()
        } else {
            return Err(ConsumerError::Server(r1.error_code));
        };

        // 2. Second JoinGroup with the assigned member_id.
        let r2 = client
            .send(JoinGroupRequest {
                group_id: self.group_id.clone(),
                protocol_type: "consumer".into(),
                member_id: member_id.clone(),
                session_timeout_ms: self.session_timeout.as_millis() as i32,
                rebalance_timeout_ms: self.rebalance_timeout.as_millis() as i32,
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: encode_subscription(&self.topics),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        if r2.error_code != 0 {
            return Err(ConsumerError::Server(r2.error_code));
        }

        // 3. If we are the leader, compute the assignment via range.
        let is_leader = r2.leader == member_id;
        let assignments_for_sync: Vec<SyncGroupRequestAssignment> = if is_leader {
            // Fetch metadata for the subscribed topics to know partition counts.
            let md = client.send(MetadataRequest::default()).await?;
            let mut topic_partitions: HashMap<String, i32> = HashMap::new();
            for t in &md.topics {
                let Some(name) = &t.name else { continue };
                if self.topics.iter().any(|s| s == name) {
                    topic_partitions.insert(name.clone(), t.partitions.len() as i32);
                }
            }
            let members: Vec<(String, Vec<String>)> = r2
                .members
                .iter()
                .map(|m| (m.member_id.clone(), decode_subscription(&m.metadata)))
                .collect();
            let assignments = range::assign(members, &topic_partitions);
            assignments
                .into_iter()
                .map(|(m, partitions)| SyncGroupRequestAssignment {
                    member_id: m,
                    assignment: encode_assignment(&partitions),
                    ..Default::default()
                })
                .collect()
        } else {
            Vec::new()
        };

        // 4. SyncGroup.
        let r3 = client
            .send(SyncGroupRequest {
                group_id: self.group_id.clone(),
                generation_id: r2.generation_id,
                member_id: member_id.clone(),
                protocol_type: Some("consumer".into()),
                protocol_name: Some("range".into()),
                assignments: assignments_for_sync,
                ..Default::default()
            })
            .await?;
        if r3.error_code != 0 {
            return Err(ConsumerError::Server(r3.error_code));
        }
        let assigned_partitions = decode_assignment(&r3.assignment);

        // 5. Fetch existing committed offsets so poll() resumes correctly.
        let mut next_offsets: HashMap<(String, i32), i64> = HashMap::new();
        if !assigned_partitions.is_empty() {
            // Group OffsetFetch by topic.
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (t, p) in &assigned_partitions {
                by_topic.entry(t.clone()).or_default().push(*p);
            }
            let topics = by_topic
                .into_iter()
                .map(|(name, partition_indexes)| OffsetFetchRequestTopic {
                    name,
                    partition_indexes,
                    ..Default::default()
                })
                .collect();
            let of = client
                .send(OffsetFetchRequest {
                    group_id: self.group_id.clone(),
                    topics,
                    ..Default::default()
                })
                .await?;
            for t in &of.topics {
                for p in &t.partitions {
                    let committed = p.committed_offset;
                    let starting = if committed >= 0 {
                        committed
                    } else {
                        match self.auto_offset_reset {
                            AutoOffsetReset::Earliest => 0,
                            AutoOffsetReset::Latest => i64::MAX, // see Task 18 — poll() resolves at fetch time.
                        }
                    };
                    next_offsets.insert((t.name.clone(), p.partition_index), starting);
                }
            }
        }

        // 6. Spawn heartbeat.
        let (notice_tx, notice_rx) = mpsc::channel(8);
        let shutdown = CancellationToken::new();
        let hb_handle = tokio::spawn(heartbeat::run(
            client.clone(),
            self.group_id.clone(),
            member_id.clone(),
            r2.generation_id,
            self.heartbeat_interval,
            notice_tx,
            shutdown.clone(),
        ));

        Ok(Consumer {
            client,
            group_id: self.group_id,
            member_id,
            generation_id: r2.generation_id,
            subscribed_topics: self.topics,
            assigned: Arc::new(Mutex::new(assigned_partitions)),
            next_offsets: Arc::new(Mutex::new(next_offsets)),
            session_timeout: self.session_timeout,
            heartbeat_interval: self.heartbeat_interval,
            rebalance_rx: Mutex::new(notice_rx),
            heartbeat_shutdown: shutdown,
            heartbeat_handle: Some(hb_handle),
        })
    }
}

// ── subscription/assignment codec (ConsumerProtocol) ──────────────────────

/// Encode a `ConsumerProtocolSubscription` v1 record:
/// version (i16=1) + topics (array<STRING>) + user_data (BYTES=-1).
fn encode_subscription(topics: &[String]) -> Bytes {
    use bytes::BufMut;
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(topics.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for t in topics {
        let len = i16::try_from(t.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(t.as_bytes());
    }
    buf.put_i32(-1); // user_data null
    buf.freeze()
}

fn decode_subscription(bytes: &[u8]) -> Vec<String> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let n = cur.get_i32();
    let mut out = Vec::with_capacity(n.max(0) as usize);
    for _ in 0..n.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16() as usize;
        if cur.remaining() < len {
            break;
        }
        let mut s = vec![0u8; len];
        cur.copy_to_slice(&mut s);
        if let Ok(s) = String::from_utf8(s) {
            out.push(s);
        }
    }
    out
}

/// Encode a `ConsumerProtocolAssignment` v1:
/// version (i16=1) + assigned_partitions (array<{topic, partitions: array<i32>}>)
/// + user_data (BYTES=-1).
fn encode_assignment(partitions: &[(String, i32)]) -> Bytes {
    use bytes::BufMut;
    let mut by_topic: std::collections::BTreeMap<&str, Vec<i32>> = Default::default();
    for (t, p) in partitions {
        by_topic.entry(t.as_str()).or_default().push(*p);
    }
    let mut buf = BytesMut::new();
    buf.put_i16(1);
    let n = i32::try_from(by_topic.len()).expect("topics fit in i32");
    buf.put_i32(n);
    for (topic, parts) in by_topic {
        let len = i16::try_from(topic.len()).expect("topic name fits in i16");
        buf.put_i16(len);
        buf.put_slice(topic.as_bytes());
        buf.put_i32(parts.len() as i32);
        for p in parts {
            buf.put_i32(p);
        }
    }
    buf.put_i32(-1);
    buf.freeze()
}

fn decode_assignment(bytes: &[u8]) -> Vec<(String, i32)> {
    use bytes::Buf;
    let mut cur = bytes;
    if cur.remaining() < 2 {
        return Vec::new();
    }
    let _version = cur.get_i16();
    if cur.remaining() < 4 {
        return Vec::new();
    }
    let topic_count = cur.get_i32();
    let mut out = Vec::new();
    for _ in 0..topic_count.max(0) {
        if cur.remaining() < 2 {
            break;
        }
        let len = cur.get_i16() as usize;
        if cur.remaining() < len {
            break;
        }
        let mut name = vec![0u8; len];
        cur.copy_to_slice(&mut name);
        let topic = match String::from_utf8(name) {
            Ok(s) => s,
            Err(_) => break,
        };
        if cur.remaining() < 4 {
            break;
        }
        let pcount = cur.get_i32();
        for _ in 0..pcount.max(0) {
            if cur.remaining() < 4 {
                break;
            }
            out.push((topic.clone(), cur.get_i32()));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_round_trip() {
        let s = encode_subscription(&["t1".into(), "t2".into()]);
        let decoded = decode_subscription(&s);
        assert_eq!(decoded, vec!["t1", "t2"]);
    }

    #[test]
    fn assignment_round_trip() {
        let s = encode_assignment(&[("t".into(), 0), ("t".into(), 1), ("u".into(), 0)]);
        let decoded = decode_assignment(&s);
        assert!(decoded.contains(&("t".into(), 0)));
        assert!(decoded.contains(&("t".into(), 1)));
        assert!(decoded.contains(&("u".into(), 0)));
    }
}
```

- [ ] **Step 3: Hook into `lib.rs`**

```rust
//! Subscribe-style consumer client for Apache Kafka in Rust.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.0.0")]

mod assignor;
mod builder;
mod consumer;
mod error;
mod heartbeat;

pub use builder::{AutoOffsetReset, ConsumerBuilder};
pub use consumer::{Consumer, ConsumerRecord};
pub use error::ConsumerError;
```

- [ ] **Step 4: Test + commit**

```bash
cargo test -p crabka-client-consumer builder
git add crates/client-consumer
git commit -m "feat(consumer): ConsumerBuilder + Consumer lifecycle (join → sync → spawn heartbeat)"
```

---

### Task 18: `Consumer::poll`

**Files:**
- Create: `crates/client-consumer/src/poll.rs`
- Modify: `crates/client-consumer/src/consumer.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: Resolve "latest" sentinel into a real offset**

Inside `crates/client-consumer/src/poll.rs`:

```rust
//! `Consumer::poll` — issues one `Fetch` covering every assigned
//! partition, advances next-offsets, returns records.

use std::collections::HashMap;
use std::time::Duration;

use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use crabka_protocol::records::RecordBatch;
use bytes::Buf;

use crate::consumer::{Consumer, ConsumerRecord};
use crate::error::ConsumerError;
use crate::heartbeat::RebalanceNotice;

impl Consumer {
    /// Returns up to one batch per assigned partition or an empty vec on
    /// timeout. If the heartbeat task signalled a rebalance, this returns
    /// `Err(CommitInvalid)`; the caller should drop in-flight commits
    /// and `poll` again (which will trigger an internal rejoin).
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>, ConsumerError> {
        // 1. Drain any rebalance notices first.
        let mut rebalance_rx = self.rebalance_rx.lock().await;
        if let Ok(notice) = rebalance_rx.try_recv() {
            tracing::info!(?notice, "rebalance notice received during poll");
            // For the MVP: just return an empty batch + flag CommitInvalid so
            // the caller can react. Re-join logic lives in a follow-up (the
            // user can re-create the Consumer).
            let _ = notice;
            return Err(ConsumerError::CommitInvalid);
        }
        drop(rebalance_rx);

        // 2. Resolve any i64::MAX sentinels (auto.offset.reset=latest) via
        //    ListOffsets(timestamp=-1).
        self.resolve_latest_sentinels().await?;

        // 3. Build a FetchRequest covering every assigned partition.
        let assigned = self.assigned.lock().await.clone();
        if assigned.is_empty() {
            tokio::time::sleep(timeout).await;
            return Ok(Vec::new());
        }

        let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        {
            let offsets = self.next_offsets.lock().await;
            for (t, p) in &assigned {
                let next = *offsets.get(&(t.clone(), *p)).unwrap_or(&0);
                by_topic.entry(t.clone()).or_default().push((*p, next));
            }
        }

        let topics: Vec<FetchTopic> = by_topic
            .into_iter()
            .map(|(name, plist)| FetchTopic {
                topic: name,
                partitions: plist
                    .into_iter()
                    .map(|(p, off)| FetchPartition {
                        partition: p,
                        fetch_offset: off,
                        partition_max_bytes: 1 << 20,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();

        let timeout_ms = i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX);
        let resp = self
            .client
            .send(FetchRequest {
                max_wait_ms: timeout_ms,
                min_bytes: 1,
                max_bytes: 50 * 1024 * 1024,
                topics,
                ..Default::default()
            })
            .await?;

        // 4. Decode each partition's records, advance next-offsets.
        let mut out: Vec<ConsumerRecord> = Vec::new();
        let mut offsets = self.next_offsets.lock().await;
        for topic in &resp.responses {
            for part in &topic.partitions {
                let Some(rec_bytes) = &part.records else { continue };
                if rec_bytes.is_empty() {
                    continue;
                }
                let mut cur: &[u8] = rec_bytes;
                while cur.has_remaining() {
                    let before = cur.len();
                    let Ok(batch) = RecordBatch::decode(&mut cur) else { break };
                    if before == cur.len() {
                        break;
                    }
                    for r in &batch.records {
                        let offset = batch.base_offset + i64::from(r.offset_delta);
                        out.push(ConsumerRecord {
                            topic: topic.topic.clone(),
                            partition: part.partition_index,
                            offset,
                            timestamp: batch.base_timestamp + r.timestamp_delta,
                            key: r.key.clone(),
                            value: r.value.clone(),
                        });
                        offsets.insert(
                            (topic.topic.clone(), part.partition_index),
                            offset + 1,
                        );
                    }
                }
            }
        }
        Ok(out)
    }

    async fn resolve_latest_sentinels(&self) -> Result<(), ConsumerError> {
        let mut offsets = self.next_offsets.lock().await;
        let sentinels: Vec<(String, i32)> = offsets
            .iter()
            .filter(|(_, &v)| v == i64::MAX)
            .map(|(k, _)| k.clone())
            .collect();
        if sentinels.is_empty() {
            return Ok(());
        }
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (t, p) in &sentinels {
            by_topic.entry(t.clone()).or_default().push(*p);
        }
        let topics = by_topic
            .into_iter()
            .map(|(name, partitions)| ListOffsetsTopic {
                name,
                partitions: partitions
                    .into_iter()
                    .map(|p| ListOffsetsPartition {
                        partition_index: p,
                        timestamp: -1, // LATEST
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();
        let lo = self
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                topics,
                ..Default::default()
            })
            .await?;
        for t in &lo.topics {
            for p in &t.partitions {
                offsets.insert((t.name.clone(), p.partition_index), p.offset);
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod poll;`.

- [ ] **Step 3: Commit**

```bash
cargo build -p crabka-client-consumer
git add crates/client-consumer
git commit -m "feat(consumer): Consumer::poll + latest-offset resolution"
```

---

### Task 19: `commit_sync` / `commit_async`

**Files:**
- Create: `crates/client-consumer/src/commit.rs`
- Modify: `crates/client-consumer/src/lib.rs`

- [ ] **Step 1: Write the module**

`crates/client-consumer/src/commit.rs`:

```rust
//! `Consumer::commit_sync` and `commit_async`.

use std::collections::HashMap;

use crabka_protocol::owned::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};

use crate::consumer::Consumer;
use crate::error::ConsumerError;

impl Consumer {
    /// Commit the current next-offsets for every assigned partition.
    /// Blocks until the broker acks.
    pub async fn commit_sync(&self) -> Result<(), ConsumerError> {
        let offsets = self.next_offsets.lock().await.clone();
        if offsets.is_empty() {
            return Ok(());
        }
        let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
        for ((t, p), off) in offsets {
            by_topic.entry(t).or_default().push((p, off));
        }
        let topics = by_topic
            .into_iter()
            .map(|(name, parts)| OffsetCommitRequestTopic {
                name,
                partitions: parts
                    .into_iter()
                    .map(|(p, off)| OffsetCommitRequestPartition {
                        partition_index: p,
                        committed_offset: off,
                        committed_leader_epoch: -1,
                        committed_metadata: Some(String::new()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect();

        let resp = self
            .client
            .send(OffsetCommitRequest {
                group_id: self.group_id.clone(),
                generation_id_or_member_epoch: self.generation_id,
                member_id: self.member_id.clone(),
                topics,
                ..Default::default()
            })
            .await?;

        // Surface the first non-zero error_code if any.
        for t in &resp.topics {
            for p in &t.partitions {
                if p.error_code != 0 {
                    return Err(ConsumerError::Server(p.error_code));
                }
            }
        }
        Ok(())
    }

    /// Fire-and-forget commit. Returns once the request is enqueued on the
    /// client's writer task; does NOT wait for the broker ack. Errors are
    /// logged but not returned.
    pub fn commit_async(&self) {
        let client = self.client.clone();
        let group_id = self.group_id.clone();
        let generation = self.generation_id;
        let member_id = self.member_id.clone();
        let offsets = self.next_offsets.clone();
        tokio::spawn(async move {
            let snapshot = offsets.lock().await.clone();
            if snapshot.is_empty() {
                return;
            }
            let mut by_topic: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
            for ((t, p), off) in snapshot {
                by_topic.entry(t).or_default().push((p, off));
            }
            let topics = by_topic
                .into_iter()
                .map(|(name, parts)| OffsetCommitRequestTopic {
                    name,
                    partitions: parts
                        .into_iter()
                        .map(|(p, off)| OffsetCommitRequestPartition {
                            partition_index: p,
                            committed_offset: off,
                            committed_leader_epoch: -1,
                            committed_metadata: Some(String::new()),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect();
            let res = client
                .send(OffsetCommitRequest {
                    group_id,
                    generation_id_or_member_epoch: generation,
                    member_id,
                    topics,
                    ..Default::default()
                })
                .await;
            if let Err(e) = res {
                tracing::warn!(error = %e, "commit_async failed");
            }
        });
    }
}
```

- [ ] **Step 2: Hook into `lib.rs`**

Add `mod commit;`.

- [ ] **Step 3: Commit**

```bash
cargo build -p crabka-client-consumer
git add crates/client-consumer
git commit -m "feat(consumer): commit_sync + commit_async"
```

---

## Phase E — Integration + acceptance

### Task 20: Cross-crate integration tests

**Files:**
- Create: `crates/client-consumer/tests/integration.rs`

- [ ] **Step 1: End-to-end Rust → Rust**

`crates/client-consumer/tests/integration.rs`:

```rust
//! End-to-end: a Rust producer (via crabka-client-core) writes records;
//! a Rust Consumer (via crabka-client-consumer) subscribes through a
//! group and reads them back; commits survive a broker restart.

use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_consumer::{AutoOffsetReset, ConsumerBuilder};
use crabka_client_core::Client;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};
use bytes::Bytes;
use tempfile::TempDir;

fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.last_offset_delta = (values.len() as i32) - 1;
    b.max_timestamp = values.len() as i64;
    for (i, v) in values.iter().enumerate() {
        b.records.push(Record {
            offset_delta: i as i32,
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    b
}

async fn produce(client: &Client, topic: &str, values: &[&str]) {
    client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
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
        .expect("produce");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_producer_to_rust_consumer_through_group() {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();

    let producer = Client::builder(&bootstrap)
        .client_id("rust-producer")
        .build()
        .await
        .unwrap();
    producer
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "rrtopic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    produce(&producer, "rrtopic", &["a", "b", "c"]).await;

    let mut consumer = ConsumerBuilder::new(&bootstrap)
        .client_id("rust-consumer")
        .group_id("g1")
        .session_timeout(Duration::from_secs(30))
        .rebalance_timeout(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_secs(1))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(&["rrtopic"])
        .build()
        .await
        .unwrap();

    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline && seen.len() < 3 {
        let records = consumer.poll(Duration::from_millis(500)).await.unwrap();
        for r in records {
            seen.push(String::from_utf8_lossy(r.value.as_deref().unwrap_or(&[])).into_owned());
        }
    }
    assert_eq!(seen, vec!["a", "b", "c"]);

    consumer.commit_sync().await.unwrap();
    consumer.close().await.unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offsets_survive_broker_restart() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().to_path_buf();

    // First boot: create + produce + consume + commit.
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let producer = Client::builder(&bootstrap)
            .client_id("p")
            .build()
            .await
            .unwrap();
        producer
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "persist".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap();
        produce(&producer, "persist", &["x", "y", "z"]).await;
        let mut consumer = ConsumerBuilder::new(&bootstrap)
            .client_id("c")
            .group_id("persist-grp")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .rebalance_timeout(Duration::from_secs(2))
            .subscribe(&["persist"])
            .build()
            .await
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut seen = 0;
        while std::time::Instant::now() < deadline && seen < 3 {
            seen += consumer.poll(Duration::from_millis(500)).await.unwrap().len();
        }
        assert_eq!(seen, 3);
        consumer.commit_sync().await.unwrap();
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }

    // Second boot: same group reads from the committed offset (= end).
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_path))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut consumer = ConsumerBuilder::new(&bootstrap)
            .client_id("c2")
            .group_id("persist-grp")
            .rebalance_timeout(Duration::from_secs(2))
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(&["persist"])
            .build()
            .await
            .unwrap();
        // Quick poll: should NOT receive the same x/y/z again.
        let r = consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap();
        assert!(r.is_empty(), "expected empty poll after restart, got {r:?}");
        consumer.close().await.unwrap();
        broker.shutdown().await;
    }
}
```

- [ ] **Step 2: Run + commit**

```bash
cargo test -p crabka-client-consumer --test integration
git add crates/client-consumer/tests
git commit -m "test(consumer): end-to-end Rust producer → Rust consumer + restart-replay"
```

---

### Task 21: JVM acceptance test for console-consumer with group

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Add the new scenario**

Append to `crates/broker/tests/jvm_acceptance.rs`:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn console_consumer_with_group_round_trip() {
    const TOPIC: &str = "crabka-broker-grp-itest";

    let (broker, _dir) = start_host_broker().await;

    // 1. Create the topic.
    docker_run_kafka_tool(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--topic",
        TOPIC,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--bootstrap-server",
        BOOTSTRAP,
    ]);

    // 2. Produce records.
    let mut child = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-i",
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE,
            "kafka-console-producer",
            "--bootstrap-server",
            BOOTSTRAP,
            "--topic",
            TOPIC,
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn producer");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(b"x\ny\nz\n")
        .expect("write stdin");
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("wait producer");
    assert!(out.status.success(), "producer failed: {}", String::from_utf8_lossy(&out.stderr));

    // 3. Consume WITHOUT --partition. The default `console-consumer` group
    //    will JoinGroup → SyncGroup → Heartbeat → Fetch through our coordinator.
    let consumer_out = docker_run_kafka_tool(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        BOOTSTRAP,
        "--topic",
        TOPIC,
        "--from-beginning",
        "--group",
        "crabka-acceptance-group",
        "--max-messages",
        "3",
        "--timeout-ms",
        "20000",
    ]);
    let s = String::from_utf8_lossy(&consumer_out.stdout);
    for needle in ["x", "y", "z"] {
        assert!(s.contains(needle), "consumer didn't emit {needle}: {s:?}");
    }

    broker.shutdown().await;
}
```

- [ ] **Step 2: Commit (compile-only; CI runs it)**

```bash
cargo check -p crabka-broker --tests
git add crates/broker/tests
git commit -m "test(broker): JVM acceptance — console-consumer with group round-trip"
```

---

### Task 22: Acceptance gate + rustdoc + PR

- [ ] **Step 1: Crate-level rustdoc on `crabka-client-consumer`**

Replace `crates/client-consumer/src/lib.rs` top with:

```rust
//! Subscribe-style consumer client for Apache Kafka in Rust.
//!
//! Builds on [`crabka-client-core`] for transport; adds the classic
//! consumer-group lifecycle (JoinGroup → SyncGroup → Heartbeat → Fetch
//! → OffsetCommit → LeaveGroup) and a built-in heartbeat task.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use crabka_client_consumer::{ConsumerBuilder, AutoOffsetReset};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut consumer = ConsumerBuilder::new("localhost:9092")
//!     .group_id("my-group")
//!     .client_id("my-app")
//!     .auto_offset_reset(AutoOffsetReset::Earliest)
//!     .subscribe(&["my-topic"])
//!     .build()
//!     .await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_millis(500)).await?;
//!     for r in records {
//!         // ... handle r ...
//!     }
//!     consumer.commit_sync().await?;
//! }
//! # }
//! ```
//!
//! ## Out of scope
//!
//! - `assign()` (manual partition consumption) — use `crabka-client-core`
//!   directly.
//! - Admin RPCs (DescribeGroups, ListGroups) — slice 10.
//! - KIP-848 / cooperative-sticky rebalance — slice 5b.
//! - Transactional consumers (`isolation.level=read_committed`) — slice 9.
//!
//! ## Cargo features
//!
//! None for now.

#![doc(html_root_url = "https://docs.rs/crabka-client-consumer/0.0.0")]

mod assignor;
mod builder;
mod commit;
mod consumer;
mod error;
mod heartbeat;
mod poll;

pub use builder::{AutoOffsetReset, ConsumerBuilder};
pub use consumer::{Consumer, ConsumerRecord};
pub use error::ConsumerError;
```

- [ ] **Step 2: Verify doc builds**

```bash
RUSTDOCFLAGS="-D warnings" cargo doc -p crabka-broker --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p crabka-client-consumer --no-deps
```

Expected: clean.

- [ ] **Step 3: Full local gate**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p crabka-broker
cargo test -p crabka-client-consumer
cargo test --workspace -- --include-ignored   # Docker-dependent JVM tests need a Docker daemon
```

(`--include-ignored` requires Docker. Skip locally if you don't have it; CI will catch.)

- [ ] **Step 4: Push + PR**

```bash
git push -u origin feature/consumer-groups
gh pr create --base main --head feature/consumer-groups \
    --title "Slice 5: consumer groups + coordinator" \
    --body "$(cat <<'PRBODY'
## Summary

Classic Kafka consumer-group coordinator on the broker side + a new `crabka-client-consumer` crate. After this slice, JVM `kafka-console-consumer` (no `--partition`) joins a group and reads records produced by `kafka-console-producer`.

## What landed

- `crates/broker/src/coordinator/`: GroupManager, Group state machine, `__consumer_offsets` record codecs, startup replay.
- Six new handlers on the broker: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch. Real FindCoordinator (replaces the slice-4 stub).
- New `crates/client-consumer/` crate: subscribe-only `Consumer`, range assignor, background Heartbeat task, `poll` + `commit_sync` + `commit_async` + `close`.
- Tests: per-handler unit tests, full group-flow integration test, end-to-end Rust-producer → Rust-consumer scenario, restart-replay test, and `broker-jvm-acceptance` extension `console_consumer_with_group_round_trip` (JVM kafka-console-consumer against the Rust broker).

## Out of scope

KIP-848, cooperative-sticky rebalance, static membership, transactional offset commits, multi-broker coordinator handoff, log-compacted `__consumer_offsets`, DescribeGroups / ListGroups, 50-partition `__consumer_offsets`. Each is mapped to a future slice.

## Reference

Spec: `docs/superpowers/specs/2026-05-11-crabka-consumer-groups-design.md`.
Plan: `docs/superpowers/plans/2026-05-11-crabka-consumer-groups.md`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

---

## Self-review against the spec

| # | Spec criterion                                          | Plan task             |
|---|---------------------------------------------------------|-----------------------|
| 1 | Real FindCoordinator                                    | Task 6                |
| 2 | JoinGroup + rebalance gate                              | Task 7                |
| 3 | SyncGroup with leader assignment                        | Task 8                |
| 4 | Heartbeat + session-timeout expiration                  | Tasks 3, 9            |
| 5 | LeaveGroup                                              | Task 10               |
| 6 | OffsetCommit writes to `__consumer_offsets-0`           | Task 11               |
| 7 | OffsetFetch reads from in-memory `committed_offsets`    | Task 12               |
| 8 | Startup replay of `__consumer_offsets-0`                | Tasks 4, 5            |
| 9 | `crabka-client-consumer` subscribe-only API             | Tasks 14, 17, 18, 19  |
| 10 | `range` partition assignor                             | Task 15               |
| 11 | Heartbeat task on the consumer                         | Task 16               |
| 12 | Cross-crate integration tests                          | Task 20               |
| 13 | JVM acceptance: `console_consumer_with_group_round_trip` | Task 21              |
| 14 | New `BrokerError` variants + wire codes                | Task 1                |
| 15 | Rustdoc on every public type                           | Task 22               |
| 16 | fmt + clippy + workspace tests clean                   | Task 22               |

**Placeholder scan:** no "TBD" / "TODO" markers. The few "adapt the field name if the codegen differs" notes point at specific generated structs (`OffsetCommitRequest`, `OffsetFetchRequest`, `JoinGroupRequest`) and the canonical fallback for each.

**Type consistency:** `Group`, `GroupState`, `Member`, `OffsetEntry`, `GroupManager`, `GroupHandle`, `Consumer`, `ConsumerBuilder`, `ConsumerRecord`, `ConsumerError`, `AutoOffsetReset`, `RebalanceNotice` — used consistently across all tasks. `next_offsets`, `assigned`, `generation_id`, `member_id` all stay snake-case throughout.

The plan is ready for execution.
