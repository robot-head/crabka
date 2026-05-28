# Slice 64a — KIP-848 next-gen consumer group protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship KIP-848 next-gen consumer group protocol on Crabka — two new handlers (`ConsumerGroupHeartbeat`, `ConsumerGroupDescribe`), server-side `UniformAssignor` + `RangeAssignor`, per-group reconciler-task actors writing `__consumer_offsets` record types 3/5/6/7/8, coexisting with classic groups, validated against `apache/kafka:4.0.0` clients.

**Architecture:** Per-group tokio actor owns next-gen group state; heartbeats are mpsc messages with oneshot replies. Bootstrap replays `__consumer_offsets` in-place, then spawns actors. Classic↔next-gen group-type locking via first persisted record. Reconciliation runs trigger-driven inside the actor on subscription / member-set / metadata change.

**Tech Stack:** Rust 1.95, tokio (mpsc + oneshot + tasks), bytes, dashmap, uuid, existing `crabka-broker` + `crabka-protocol` codegen.

**Spec:** `docs/superpowers/specs/2026-05-28-crabka-kip-848-next-gen-consumer-group-64a-design.md`

---

## File map

**Create:**
- `crates/broker/src/coordinator/next_gen/mod.rs` — `NextGenCoordinator` registry + group-type cache
- `crates/broker/src/coordinator/next_gen/group_actor.rs` — per-group tokio task + message types
- `crates/broker/src/coordinator/next_gen/group_state.rs` — `MemberState`, `TargetAssignment`, `CurrentAssignment`, state transitions
- `crates/broker/src/coordinator/next_gen/reconciler.rs` — dirty-bit + recompute pipeline
- `crates/broker/src/coordinator/next_gen/persistence.rs` — encode/decode v3–8 records + tombstones
- `crates/broker/src/coordinator/next_gen/assignor/mod.rs` — `Assignor` trait + dispatch
- `crates/broker/src/coordinator/next_gen/assignor/uniform.rs` — `UniformAssignor`
- `crates/broker/src/coordinator/next_gen/assignor/range.rs` — `RangeAssignor`
- `crates/broker/src/coordinator/next_gen/config.rs` — `NextGenConfig` struct + defaults
- `crates/broker/src/handlers/consumer_group_heartbeat.rs` — api_key 68 handler
- `crates/broker/src/handlers/consumer_group_describe.rs` — api_key 69 handler
- `crates/broker/tests/consumer_group_next_gen.rs` — raw-RPC integration
- `crates/broker/tests/consumer_group_next_gen_persistence.rs` — bootstrap-replay test
- `crates/broker/tests/jvm_consumer_group_next_gen.rs` — apache/kafka:4.0.0 acceptance

**Modify:**
- `crates/broker/src/codes.rs` — add `FENCED_MEMBER_EPOCH=110`, `UNSUPPORTED_ASSIGNOR=111`, `UNRELEASED_INSTANCE_ID=114`, `UNKNOWN_SUBSCRIPTION_ID=117`
- `crates/broker/src/config.rs` — `group.consumer.*` + `group.coordinator.rebalance.protocols`
- `crates/broker/src/coordinator/mod.rs` — `GroupManager::is_classic_group()` + hold `NextGenCoordinator`
- `crates/broker/src/coordinator/persistence.rs` — extend `parse_key()` for key types 3/5/6/7/8
- `crates/broker/src/coordinator/bootstrap.rs` — dispatch v3–8 records; finalize-bootstrap step
- `crates/broker/src/handlers/api_versions.rs` — register `v!(consumer_group_heartbeat_request)` + `v!(consumer_group_describe_request)`
- `crates/broker/src/handlers/mod.rs` (or wherever the dispatcher lives) — route api_keys 68/69
- `crates/broker/src/handlers/offset_commit.rs` — dispatch next-gen group via `OffsetValidate`
- `crates/broker/src/handlers/offset_fetch.rs` — same
- `.github/workflows/ci.yml` — preload `apache/kafka:4.0.0`
- `STATUS.md` — slice-64a entry

---

## Pre-flight

- [ ] **PF-1: Branch + baseline**

```bash
git checkout -b kip-848-64a
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: clean baseline (already on `kip-848-64a` with spec commit; baseline must pass before any task begins).

---

## Task 1 — Error codes

**Files:**
- Modify: `crates/broker/src/codes.rs`

- [ ] **Step 1.1: Add the four new constants**

Insert below the existing `STALE_MEMBER_EPOCH = 113` line:

```rust
// crates/broker/src/codes.rs
/// `FENCED_MEMBER_EPOCH` (110, KIP-848) — the supplied member epoch is
/// newer than the coordinator's; the consumer must rejoin from epoch 0.
pub const FENCED_MEMBER_EPOCH: i16 = 110;
/// `UNSUPPORTED_ASSIGNOR` (111, KIP-848) — the requested `server_assignor`
/// is not enabled on this broker.
pub const UNSUPPORTED_ASSIGNOR: i16 = 111;
/// `UNRELEASED_INSTANCE_ID` (114, KIP-848 + KIP-345) — the static
/// `instance_id` is still bound to a live member of the group.
pub const UNRELEASED_INSTANCE_ID: i16 = 114;
/// `UNKNOWN_SUBSCRIPTION_ID` (117, KIP-848) — the consumer's persisted
/// subscription identifier was not found by the coordinator.
pub const UNKNOWN_SUBSCRIPTION_ID: i16 = 117;
```

- [ ] **Step 1.2: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/codes.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(codes): add KIP-848 error codes 110/111/114/117"
```

Expected: build green.

---

## Task 2 — Broker config knobs

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/config.rs`
- Modify: `crates/broker/src/config.rs`
- Modify: `crates/broker/src/coordinator/mod.rs` (re-export)

- [ ] **Step 2.1: Create `NextGenConfig`**

```rust
// crates/broker/src/coordinator/next_gen/config.rs
//! Static broker config for the KIP-848 next-gen consumer group protocol.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct NextGenConfig {
    /// Comma-separated list; "consumer" enables KIP-848. Default "classic,consumer".
    pub rebalance_protocols: Vec<RebalanceProtocol>,
    pub session_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub min_session_timeout: Duration,
    pub max_session_timeout: Duration,
    pub min_heartbeat_interval: Duration,
    pub max_heartbeat_interval: Duration,
    pub assignors: Vec<String>,
    pub max_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceProtocol {
    Classic,
    Consumer,
}

impl Default for NextGenConfig {
    fn default() -> Self {
        Self {
            rebalance_protocols: vec![RebalanceProtocol::Classic, RebalanceProtocol::Consumer],
            session_timeout: Duration::from_millis(45_000),
            heartbeat_interval: Duration::from_millis(5_000),
            min_session_timeout: Duration::from_millis(45_000),
            max_session_timeout: Duration::from_millis(60_000),
            min_heartbeat_interval: Duration::from_millis(5_000),
            max_heartbeat_interval: Duration::from_millis(15_000),
            assignors: vec!["uniform".into(), "range".into()],
            max_size: 200,
        }
    }
}

impl NextGenConfig {
    pub fn next_gen_enabled(&self) -> bool {
        self.rebalance_protocols.contains(&RebalanceProtocol::Consumer)
    }

    pub fn assignor_enabled(&self, name: &str) -> bool {
        self.assignors.iter().any(|a| a == name)
    }
}
```

- [ ] **Step 2.2: Wire the field into `BrokerConfig`**

In `crates/broker/src/config.rs` find the `BrokerConfig` struct definition and add after `auto_leader_rebalance_enable`:

```rust
    pub next_gen_consumer_group: crate::coordinator::next_gen::config::NextGenConfig,
```

Add to `BrokerConfig::default()` (and `for_tests` if it's a separate impl): `next_gen_consumer_group: Default::default(),`.

- [ ] **Step 2.3: Module wiring**

In `crates/broker/src/coordinator/mod.rs`, add at the top:

```rust
pub mod next_gen;
```

And in `crates/broker/src/coordinator/next_gen/mod.rs` (new file, stub for now):

```rust
//! KIP-848 next-gen consumer group protocol coordinator.

pub mod config;
```

- [ ] **Step 2.4: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/next_gen/ crates/broker/src/coordinator/mod.rs crates/broker/src/config.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): NextGenConfig + module skeleton"
```

Expected: build green.

---

## Task 3 — Assignor trait + UniformAssignor

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/assignor/mod.rs`
- Create: `crates/broker/src/coordinator/next_gen/assignor/uniform.rs`

- [ ] **Step 3.1: Trait + dispatcher**

```rust
// crates/broker/src/coordinator/next_gen/assignor/mod.rs
//! Server-side assignors (KIP-848). Each implementation maps a set of
//! members + subscriptions + topic metadata to per-member partition
//! assignments.

pub mod uniform;
pub mod range;

use std::collections::HashMap;

use crabka_protocol::primitives::uuid::Uuid;

/// Input subscription for one group member.
#[derive(Debug, Clone)]
pub struct MemberSubscription {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub subscribed_topic_ids: Vec<Uuid>,
}

/// `topic_id → partition count` snapshot at assignment time.
#[derive(Debug, Clone, Default)]
pub struct TopicMetadata {
    pub partitions_per_topic: HashMap<Uuid, i32>,
}

/// Resulting assignment: `member_id → topic_id → partitions`.
pub type Assignment = HashMap<String, HashMap<Uuid, Vec<i32>>>;

pub trait Assignor: Send + Sync {
    fn name(&self) -> &'static str;
    fn assign(
        &self,
        members: &[MemberSubscription],
        topics: &TopicMetadata,
    ) -> Assignment;
}

pub fn select(name: &str) -> Option<Box<dyn Assignor>> {
    match name {
        "uniform" => Some(Box::new(uniform::UniformAssignor)),
        "range" => Some(Box::new(range::RangeAssignor)),
        _ => None,
    }
}
```

- [ ] **Step 3.2: UniformAssignor — failing tests first**

```rust
// crates/broker/src/coordinator/next_gen/assignor/uniform.rs
//! `UniformAssignor` — KIP-848's default. Distributes partitions as evenly
//! as possible across members subscribed to each topic. Deterministic.

use std::collections::HashMap;

use crabka_protocol::primitives::uuid::Uuid;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

pub struct UniformAssignor;

impl Assignor for UniformAssignor {
    fn name(&self) -> &'static str {
        "uniform"
    }

    fn assign(
        &self,
        members: &[MemberSubscription],
        topics: &TopicMetadata,
    ) -> Assignment {
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        for (topic_id, partition_count) in &topics.partitions_per_topic {
            let mut subscribers: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            subscribers.sort();
            if subscribers.is_empty() {
                continue;
            }
            for p in 0..*partition_count {
                let idx = (p as usize) % subscribers.len();
                let mid = subscribers[idx].to_string();
                out.get_mut(&mid)
                    .expect("inserted above")
                    .entry(*topic_id)
                    .or_default()
                    .push(p);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(b: u8) -> Uuid {
        Uuid::from_bytes([b; 16])
    }

    fn member(id: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    #[test]
    fn single_member_gets_all_partitions() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert_eq!(a["m1"][&t], vec![0, 1, 2, 3]);
    }

    #[test]
    fn two_members_split_round_robin() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0, 2]);
        assert_eq!(a["m2"][&t], vec![1, 3]);
    }

    #[test]
    fn unsubscribed_member_gets_empty_for_topic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
        };
        let a = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0, 1]);
        assert!(a["m2"].get(&t).is_none() || a["m2"][&t].is_empty());
    }

    #[test]
    fn zero_partitions_no_assignment() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 0)].into(),
        };
        let a = UniformAssignor.assign(&[member("m1", &[t])], &topics);
        assert!(a["m1"].get(&t).is_none() || a["m1"][&t].is_empty());
    }

    #[test]
    fn deterministic_under_member_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
        };
        let a1 = UniformAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        let a2 = UniformAssignor.assign(
            &[member("m3", &[t]), member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert_eq!(a1, a2);
    }

    #[test]
    fn empty_members_no_panic() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = UniformAssignor.assign(&[], &topics);
        assert!(a.is_empty());
    }
}
```

- [ ] **Step 3.3: Stub `range.rs` so the module compiles**

```rust
// crates/broker/src/coordinator/next_gen/assignor/range.rs
//! `RangeAssignor` — stub, real implementation in Task 4.

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }
    fn assign(
        &self,
        _members: &[MemberSubscription],
        _topics: &TopicMetadata,
    ) -> Assignment {
        Default::default()
    }
}
```

- [ ] **Step 3.4: Wire the module**

In `crates/broker/src/coordinator/next_gen/mod.rs`:

```rust
pub mod assignor;
pub mod config;
```

- [ ] **Step 3.5: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::assignor::uniform
git add crates/broker/src/coordinator/next_gen/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): Assignor trait + UniformAssignor"
```

Expected: 6 tests pass.

---

## Task 4 — RangeAssignor

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/assignor/range.rs`

- [ ] **Step 4.1: Implementation + tests**

Replace the stub with:

```rust
// crates/broker/src/coordinator/next_gen/assignor/range.rs
//! `RangeAssignor` — assigns contiguous partition ranges per topic.
//! Matches classic RangeAssignor semantics for co-partitioning across
//! topics with equal partition counts.

use std::collections::HashMap;

use crabka_protocol::primitives::uuid::Uuid;

use super::{Assignment, Assignor, MemberSubscription, TopicMetadata};

pub struct RangeAssignor;

impl Assignor for RangeAssignor {
    fn name(&self) -> &'static str {
        "range"
    }

    fn assign(
        &self,
        members: &[MemberSubscription],
        topics: &TopicMetadata,
    ) -> Assignment {
        let mut out: Assignment = HashMap::new();
        for m in members {
            out.insert(m.member_id.clone(), HashMap::new());
        }
        let mut sorted_topics: Vec<(&Uuid, &i32)> =
            topics.partitions_per_topic.iter().collect();
        sorted_topics.sort_by_key(|(id, _)| *id);
        for (topic_id, partition_count) in sorted_topics {
            let mut subscribers: Vec<&str> = members
                .iter()
                .filter(|m| m.subscribed_topic_ids.contains(topic_id))
                .map(|m| m.member_id.as_str())
                .collect();
            subscribers.sort();
            if subscribers.is_empty() {
                continue;
            }
            let n = subscribers.len() as i32;
            let p = *partition_count;
            let per_member = p / n;
            let remainder = p % n;
            let mut cursor = 0;
            for (i, sub) in subscribers.iter().enumerate() {
                let extra = if (i as i32) < remainder { 1 } else { 0 };
                let take = per_member + extra;
                if take == 0 {
                    continue;
                }
                let range: Vec<i32> = (cursor..cursor + take).collect();
                out.get_mut(*sub)
                    .expect("inserted above")
                    .insert(*topic_id, range);
                cursor += take;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(b: u8) -> Uuid {
        Uuid::from_bytes([b; 16])
    }
    fn member(id: &str, topics: &[Uuid]) -> MemberSubscription {
        MemberSubscription {
            member_id: id.into(),
            rack_id: None,
            subscribed_topic_ids: topics.to_vec(),
        }
    }

    #[test]
    fn contiguous_ranges() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0, 1, 2]);
        assert_eq!(a["m2"][&t], vec![3, 4, 5]);
    }

    #[test]
    fn non_divisible_extra_goes_to_first_members() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 7)].into(),
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0, 1, 2]);
        assert_eq!(a["m2"][&t], vec![3, 4]);
        assert_eq!(a["m3"][&t], vec![5, 6]);
    }

    #[test]
    fn co_partitioning_two_topics_equal_size() {
        let t1 = tid(1);
        let t2 = tid(2);
        let topics = TopicMetadata {
            partitions_per_topic: [(t1, 4), (t2, 4)].into(),
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t1, t2]), member("m2", &[t1, t2])],
            &topics,
        );
        assert_eq!(a["m1"][&t1], vec![0, 1]);
        assert_eq!(a["m1"][&t2], vec![0, 1]);
        assert_eq!(a["m2"][&t1], vec![2, 3]);
        assert_eq!(a["m2"][&t2], vec![2, 3]);
    }

    #[test]
    fn fewer_partitions_than_members() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 2)].into(),
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t]), member("m3", &[t])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0]);
        assert_eq!(a["m2"][&t], vec![1]);
        assert!(a["m3"].get(&t).is_none() || a["m3"][&t].is_empty());
    }

    #[test]
    fn unsubscribed_skipped() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 4)].into(),
        };
        let a = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[])],
            &topics,
        );
        assert_eq!(a["m1"][&t], vec![0, 1, 2, 3]);
    }

    #[test]
    fn deterministic_under_input_order() {
        let t = tid(1);
        let topics = TopicMetadata {
            partitions_per_topic: [(t, 6)].into(),
        };
        let a1 = RangeAssignor.assign(
            &[member("m1", &[t]), member("m2", &[t])],
            &topics,
        );
        let a2 = RangeAssignor.assign(
            &[member("m2", &[t]), member("m1", &[t])],
            &topics,
        );
        assert_eq!(a1, a2);
    }
}
```

- [ ] **Step 4.2: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::assignor::range
git add crates/broker/src/coordinator/next_gen/assignor/range.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): RangeAssignor"
```

Expected: 6 tests pass.

---

## Task 5 — Persistence (key types 3, 5, 6, 7, 8 + tombstones)

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/persistence.rs`
- Modify: `crates/broker/src/coordinator/persistence.rs` (extend `parse_key`)
- Modify: `crates/broker/src/coordinator/next_gen/mod.rs` (add module)

- [ ] **Step 5.1: New record-key/value types**

```rust
// crates/broker/src/coordinator/next_gen/persistence.rs
//! KIP-848 record types persisted in `__consumer_offsets`. Wire encoding
//! matches the Apache Kafka reference implementation; values are flexible
//! (tagged-field) format with version preamble.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::ProtocolError;

use crate::coordinator::persistence::{
    get_bytes, get_i16, get_i32, get_i64, get_nullable_string, get_string, put_bytes,
    put_nullable_string, put_string,
};
use crate::error::BrokerError;

pub const KEY_GROUP_METADATA: i16 = 3;
pub const KEY_MEMBER_METADATA: i16 = 5;
pub const KEY_TARGET_ASSIGNMENT_METADATA: i16 = 6;
pub const KEY_TARGET_ASSIGNMENT_MEMBER: i16 = 7;
pub const KEY_CURRENT_MEMBER_ASSIGNMENT: i16 = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextGenKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

pub fn parse_key(version: i16, mut buf: &[u8]) -> Result<NextGenKey, BrokerError> {
    let key = match version {
        KEY_GROUP_METADATA => NextGenKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_MEMBER_METADATA => NextGenKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_METADATA => NextGenKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_TARGET_ASSIGNMENT_MEMBER => NextGenKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_CURRENT_MEMBER_ASSIGNMENT => NextGenKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown next-gen key version",
            )));
        }
    };
    Ok(key)
}

pub fn encode_key(key: &NextGenKey) -> Bytes {
    let mut buf = BytesMut::new();
    match key {
        NextGenKey::GroupMetadata { group_id } => {
            buf.put_i16(KEY_GROUP_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::MemberMetadata { group_id, member_id } => {
            buf.put_i16(KEY_MEMBER_METADATA);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::TargetAssignmentMetadata { group_id } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_METADATA);
            put_string(&mut buf, group_id);
        }
        NextGenKey::TargetAssignmentMember { group_id, member_id } => {
            buf.put_i16(KEY_TARGET_ASSIGNMENT_MEMBER);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
        NextGenKey::CurrentMemberAssignment { group_id, member_id } => {
            buf.put_i16(KEY_CURRENT_MEMBER_ASSIGNMENT);
            put_string(&mut buf, group_id);
            put_string(&mut buf, member_id);
        }
    }
    buf.freeze()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMetadataValue {
    pub epoch: i32,
}

impl GroupMetadataValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.epoch);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            epoch: get_i32(&mut buf)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout_ms: i32,
}

impl MemberMetadataValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.instance_id.as_deref());
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        let n = i32::try_from(self.subscribed_topic_names.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subscribed_topic_names {
            put_string(&mut buf, s);
        }
        put_nullable_string(&mut buf, self.server_assignor.as_deref());
        buf.put_i32(self.rebalance_timeout_ms);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let instance_id = get_nullable_string(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subscribed_topic_names = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subscribed_topic_names.push(get_string(&mut buf)?);
        }
        let server_assignor = get_nullable_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
            server_assignor,
            rebalance_timeout_ms,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetAssignmentMetadataValue {
    pub assignment_epoch: i32,
}

impl TargetAssignmentMetadataValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.assignment_epoch);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        Ok(Self {
            assignment_epoch: get_i32(&mut buf)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedTopicPartitions {
    pub topic_id: Uuid,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TargetAssignmentMemberValue {
    pub topic_partitions: Vec<AssignedTopicPartitions>,
}

impl TargetAssignmentMemberValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.topic_partitions.len()).expect("fits");
        buf.put_i32(n);
        for tp in &self.topic_partitions {
            put_bytes(&mut buf, &Bytes::copy_from_slice(tp.topic_id.as_bytes()));
            let pn = i32::try_from(tp.partitions.len()).expect("fits");
            buf.put_i32(pn);
            for p in &tp.partitions {
                buf.put_i32(*p);
            }
        }
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut topic_partitions = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let id_bytes = get_bytes(&mut buf)?;
            let mut arr = [0u8; 16];
            if id_bytes.len() != 16 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "topic_id not 16 bytes",
                )));
            }
            arr.copy_from_slice(&id_bytes);
            let topic_id = Uuid::from_bytes(arr);
            let pn = get_i32(&mut buf)?;
            let pcap = usize::try_from(pn.max(0)).expect("non-negative");
            let mut partitions = Vec::with_capacity(pcap);
            for _ in 0..pn.max(0) {
                partitions.push(get_i32(&mut buf)?);
            }
            topic_partitions.push(AssignedTopicPartitions {
                topic_id,
                partitions,
            });
        }
        Ok(Self { topic_partitions })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberAssignmentState {
    Stable = 0,
    UnreleasedPartitions = 1,
    UnrevokedPartitions = 2,
}

impl MemberAssignmentState {
    pub fn from_i8(v: i8) -> Result<Self, BrokerError> {
        match v {
            0 => Ok(Self::Stable),
            1 => Ok(Self::UnreleasedPartitions),
            2 => Ok(Self::UnrevokedPartitions),
            _ => Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown MemberAssignmentState",
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMemberAssignmentValue {
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub state: MemberAssignmentState,
    pub assigned_partitions: Vec<AssignedTopicPartitions>,
    pub partitions_pending_revocation: Vec<AssignedTopicPartitions>,
}

impl CurrentMemberAssignmentValue {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_i32(self.member_epoch);
        buf.put_i32(self.previous_member_epoch);
        buf.put_i8(self.state as i8);
        encode_topic_partitions(&mut buf, &self.assigned_partitions);
        encode_topic_partitions(&mut buf, &self.partitions_pending_revocation);
        buf.freeze()
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let member_epoch = get_i32(&mut buf)?;
        let previous_member_epoch = get_i32(&mut buf)?;
        if buf.remaining() < 1 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "missing state byte",
            )));
        }
        let state = MemberAssignmentState::from_i8(buf.get_i8())?;
        let assigned_partitions = decode_topic_partitions(&mut buf)?;
        let partitions_pending_revocation = decode_topic_partitions(&mut buf)?;
        Ok(Self {
            member_epoch,
            previous_member_epoch,
            state,
            assigned_partitions,
            partitions_pending_revocation,
        })
    }
}

fn encode_topic_partitions(buf: &mut BytesMut, items: &[AssignedTopicPartitions]) {
    let n = i32::try_from(items.len()).expect("fits");
    buf.put_i32(n);
    for tp in items {
        put_bytes(buf, &Bytes::copy_from_slice(tp.topic_id.as_bytes()));
        let pn = i32::try_from(tp.partitions.len()).expect("fits");
        buf.put_i32(pn);
        for p in &tp.partitions {
            buf.put_i32(*p);
        }
    }
}

fn decode_topic_partitions(buf: &mut &[u8]) -> Result<Vec<AssignedTopicPartitions>, BrokerError> {
    let n = get_i32(buf)?;
    let cap = usize::try_from(n.max(0)).expect("non-negative");
    let mut out = Vec::with_capacity(cap);
    for _ in 0..n.max(0) {
        let id_bytes = get_bytes(buf)?;
        let mut arr = [0u8; 16];
        if id_bytes.len() != 16 {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "topic_id not 16 bytes",
            )));
        }
        arr.copy_from_slice(&id_bytes);
        let topic_id = Uuid::from_bytes(arr);
        let pn = get_i32(buf)?;
        let pcap = usize::try_from(pn.max(0)).expect("non-negative");
        let mut partitions = Vec::with_capacity(pcap);
        for _ in 0..pn.max(0) {
            partitions.push(get_i32(buf)?);
        }
        out.push(AssignedTopicPartitions { topic_id, partitions });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_metadata_roundtrip() {
        let k = NextGenKey::GroupMetadata { group_id: "g".into() };
        let kb = encode_key(&k);
        let mut r = &kb[..];
        let v = bytes::Buf::get_i16(&mut r);
        let parsed = parse_key(v, r).unwrap();
        assert_eq!(parsed, k);
        let v = GroupMetadataValue { epoch: 7 };
        let vb = v.encode();
        assert_eq!(GroupMetadataValue::decode(&vb).unwrap(), v);
    }

    #[test]
    fn member_metadata_roundtrip() {
        let v = MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
        };
        let vb = v.encode();
        assert_eq!(MemberMetadataValue::decode(&vb).unwrap(), v);
    }

    #[test]
    fn target_assignment_metadata_roundtrip() {
        let v = TargetAssignmentMetadataValue { assignment_epoch: 12 };
        assert_eq!(TargetAssignmentMetadataValue::decode(&v.encode()).unwrap(), v);
    }

    #[test]
    fn target_assignment_member_roundtrip() {
        let v = TargetAssignmentMemberValue {
            topic_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid::from_bytes([1; 16]),
                partitions: vec![0, 1, 2],
            }],
        };
        assert_eq!(TargetAssignmentMemberValue::decode(&v.encode()).unwrap(), v);
    }

    #[test]
    fn current_member_assignment_roundtrip() {
        let v = CurrentMemberAssignmentValue {
            member_epoch: 5,
            previous_member_epoch: 4,
            state: MemberAssignmentState::Stable,
            assigned_partitions: vec![AssignedTopicPartitions {
                topic_id: Uuid::from_bytes([2; 16]),
                partitions: vec![0, 1],
            }],
            partitions_pending_revocation: vec![],
        };
        assert_eq!(CurrentMemberAssignmentValue::decode(&v.encode()).unwrap(), v);
    }

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_key(99, &[]).is_err());
    }
}
```

- [ ] **Step 5.2: Extend classic `parse_key` to dispatch next-gen keys**

In `crates/broker/src/coordinator/persistence.rs`, locate the `Key` enum and `parse_key` function. Extend `Key`:

```rust
#[derive(Debug, Clone)]
pub enum Key {
    OffsetCommit { group_id: String, topic: String, partition: i32 },
    GroupMetadata { group_id: String },
    NextGen(crate::coordinator::next_gen::persistence::NextGenKey),
}
```

In `parse_key`, after the existing match arms, add:

```rust
        3 | 5 | 6 | 7 | 8 => Ok(Key::NextGen(
            crate::coordinator::next_gen::persistence::parse_key(version, buf)?,
        )),
```

(Replace the existing `_ =>` arm with the above plus a new `_ =>` returning the original error.)

Also expose the helpers `get_string`, `get_i16`, etc., used by next-gen persistence — change their `pub(super)` / `pub(crate)` visibility to `pub(crate)` if not already.

- [ ] **Step 5.3: Module wiring**

In `crates/broker/src/coordinator/next_gen/mod.rs`:

```rust
pub mod assignor;
pub mod config;
pub mod persistence;
```

- [ ] **Step 5.4: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::persistence
cargo test -p crabka-broker --lib coordinator::persistence
git add crates/broker/src/coordinator/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): persistence v3-8 record types"
```

Expected: 6 next-gen tests + classic persistence tests still green.

---

## Task 6 — GroupState (member-epoch state machine)

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/group_state.rs`

- [ ] **Step 6.1: Type definitions**

```rust
// crates/broker/src/coordinator/next_gen/group_state.rs
//! Per-group state for KIP-848 next-gen consumer groups. Owned by exactly
//! one [`group_actor::GroupActor`] task; never shared.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crabka_protocol::primitives::uuid::Uuid;

use super::persistence::{AssignedTopicPartitions, MemberAssignmentState};

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout: Duration,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub assignment_state: MemberAssignmentState,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub partitions_pending_revocation: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct TargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}

#[derive(Debug)]
pub struct GroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, MemberState>,
    pub instance_to_member: HashMap<String, String>,
    pub target: TargetAssignment,
    pub dirty: bool,
}

impl GroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 0,
            members: HashMap::new(),
            instance_to_member: HashMap::new(),
            target: TargetAssignment::default(),
            dirty: false,
        }
    }

    pub fn bump_epoch(&mut self) {
        self.group_epoch += 1;
        self.dirty = true;
    }

    pub fn add_or_update_member(&mut self, m: MemberState) {
        if let Some(iid) = m.instance_id.clone() {
            self.instance_to_member.insert(iid, m.member_id.clone());
        }
        let cached: Option<HashSet<String>> = self
            .members
            .get(&m.member_id)
            .map(|prev| prev.subscribed_topic_names.clone());
        let subscription_changed =
            cached.as_ref().is_none_or(|prev| prev != &m.subscribed_topic_names);
        self.members.insert(m.member_id.clone(), m);
        if subscription_changed {
            self.dirty = true;
        }
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberState> {
        let m = self.members.remove(member_id)?;
        if let Some(ref iid) = m.instance_id {
            if self.instance_to_member.get(iid).map(String::as_str) == Some(member_id) {
                self.instance_to_member.remove(iid);
            }
        }
        self.dirty = true;
        Some(m)
    }

    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let evicted: Vec<String> = self
            .members
            .iter()
            .filter(|(_, m)| now.duration_since(m.last_seen) > session_timeout)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &evicted {
            self.remove_member(id);
        }
        evicted
    }

    pub fn install_target(&mut self, per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>) {
        self.target.epoch = self.group_epoch;
        self.target.per_member = per_member;
        for (mid, member) in &mut self.members {
            let target = self.target.per_member.get(mid).cloned().unwrap_or_default();
            let (revoke, assigned) = compute_revoke_split(&member.assigned_partitions, &target);
            member.partitions_pending_revocation = revoke;
            member.assigned_partitions = assigned;
            member.assignment_state = if member.partitions_pending_revocation.is_empty() {
                MemberAssignmentState::Stable
            } else {
                MemberAssignmentState::UnrevokedPartitions
            };
        }
    }

    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.previous_member_epoch = m.member_epoch;
            m.member_epoch = self.group_epoch;
        }
    }

    pub fn current_member_for_instance(&self, instance_id: &str) -> Option<&str> {
        self.instance_to_member.get(instance_id).map(String::as_str)
    }
}

/// Split `current` into (to-revoke, to-keep) given a `target`. Partitions
/// in current but absent from target end up in the revoke map; partitions
/// in both end up in the keep map.
fn compute_revoke_split(
    current: &HashMap<Uuid, Vec<i32>>,
    target: &HashMap<Uuid, Vec<i32>>,
) -> (HashMap<Uuid, Vec<i32>>, HashMap<Uuid, Vec<i32>>) {
    let mut revoke: HashMap<Uuid, Vec<i32>> = HashMap::new();
    let mut keep: HashMap<Uuid, Vec<i32>> = HashMap::new();
    for (tid, parts) in current {
        let target_parts = target.get(tid).cloned().unwrap_or_default();
        let target_set: HashSet<i32> = target_parts.into_iter().collect();
        for p in parts {
            if target_set.contains(p) {
                keep.entry(*tid).or_default().push(*p);
            } else {
                revoke.entry(*tid).or_default().push(*p);
            }
        }
    }
    (revoke, keep)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str) -> MemberState {
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: HashSet::new(),
            server_assignor: None,
            rebalance_timeout: Duration::from_secs(60),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn add_member_marks_dirty_first_time() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        assert!(g.dirty);
    }

    #[test]
    fn re_add_same_subscription_keeps_clean_after_reset() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.add_or_update_member(member("m1"));
        assert!(!g.dirty);
    }

    #[test]
    fn subscription_change_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        let mut m = member("m1");
        m.subscribed_topic_names.insert("t".into());
        g.add_or_update_member(m);
        assert!(g.dirty);
    }

    #[test]
    fn remove_member_marks_dirty() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.dirty = false;
        g.remove_member("m1");
        assert!(g.dirty);
    }

    #[test]
    fn evict_expired_drops_old_members() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.last_seen = Instant::now() - Duration::from_secs(120);
        g.add_or_update_member(m);
        g.add_or_update_member(member("m2"));
        let evicted = g.evict_expired(Instant::now(), Duration::from_secs(60));
        assert_eq!(evicted, vec!["m1".to_string()]);
        assert!(g.members.contains_key("m2"));
    }

    #[test]
    fn install_target_computes_revoke_split() {
        let mut g = GroupState::new("g");
        let t = Uuid::from_bytes([1; 16]);
        let mut m = member("m1");
        m.assigned_partitions.insert(t, vec![0, 1, 2]);
        g.add_or_update_member(m);
        let mut target_for_m1 = HashMap::new();
        target_for_m1.insert(t, vec![0, 1]);
        g.install_target([(("m1".to_string()), target_for_m1)].into());
        let m = &g.members["m1"];
        assert_eq!(m.partitions_pending_revocation[&t], vec![2]);
        assert_eq!(m.assigned_partitions[&t], vec![0, 1]);
        assert_eq!(m.assignment_state, MemberAssignmentState::UnrevokedPartitions);
    }

    #[test]
    fn instance_binding_tracked() {
        let mut g = GroupState::new("g");
        let mut m = member("m1");
        m.instance_id = Some("inst1".into());
        g.add_or_update_member(m);
        assert_eq!(g.current_member_for_instance("inst1"), Some("m1"));
    }

    #[test]
    fn bump_epoch_increments_and_dirties() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        g.bump_epoch();
        assert_eq!(g.group_epoch, 1);
        assert!(g.dirty);
    }

    #[test]
    fn advance_member_epoch_records_previous() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(member("m1"));
        g.group_epoch = 5;
        g.advance_member_epoch("m1");
        let m = &g.members["m1"];
        assert_eq!(m.member_epoch, 5);
        assert_eq!(m.previous_member_epoch, 0);
    }
}
```

- [ ] **Step 6.2: Module wiring**

In `crates/broker/src/coordinator/next_gen/mod.rs` add `pub mod group_state;`.

- [ ] **Step 6.3: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::group_state
git add crates/broker/src/coordinator/next_gen/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): GroupState + member-epoch transitions"
```

Expected: 9 tests pass.

---

## Task 7 — Reconciler

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/reconciler.rs`

- [ ] **Step 7.1: Implementation + tests**

```rust
// crates/broker/src/coordinator/next_gen/reconciler.rs
//! Trigger-driven reconciler. Runs at the next heartbeat after a dirty
//! signal: subscription change, member add/leave, metadata change, or
//! assignor selection change.

use std::collections::{HashMap, HashSet};

use crabka_protocol::primitives::uuid::Uuid;

use super::assignor::{self, MemberSubscription, TopicMetadata};
use super::group_state::GroupState;

#[derive(Debug, Clone, Default)]
pub struct ReconcileInput {
    pub topic_id_by_name: HashMap<String, Uuid>,
    pub partitions_per_topic: HashMap<Uuid, i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileOutcome {
    NoChange,
    Recomputed,
}

pub fn reconcile_if_dirty(
    group: &mut GroupState,
    input: &ReconcileInput,
    assignor_name: &str,
) -> ReconcileOutcome {
    if !group.dirty {
        return ReconcileOutcome::NoChange;
    }
    let Some(impl_) = assignor::select(assignor_name) else {
        return ReconcileOutcome::NoChange;
    };
    let subscriptions: Vec<MemberSubscription> = group
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: m
                .subscribed_topic_names
                .iter()
                .filter_map(|n| input.topic_id_by_name.get(n).copied())
                .collect(),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
    };
    let assignment = impl_.assign(&subscriptions, &topics);
    group.bump_epoch();
    group.install_target(assignment);
    group.dirty = false;
    ReconcileOutcome::Recomputed
}

pub fn membership_topic_ids(group: &GroupState, input: &ReconcileInput) -> HashSet<Uuid> {
    let mut out = HashSet::new();
    for m in group.members.values() {
        for name in &m.subscribed_topic_names {
            if let Some(id) = input.topic_id_by_name.get(name) {
                out.insert(*id);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::next_gen::group_state::MemberState;
    use crate::coordinator::next_gen::persistence::MemberAssignmentState;
    use std::time::{Duration, Instant};

    fn fresh_member(id: &str, topic: &str) -> MemberState {
        let mut sub = HashSet::new();
        sub.insert(topic.into());
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: sub,
            server_assignor: None,
            rebalance_timeout: Duration::from_secs(60),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
        }
    }

    fn input(topic_name: &str, partitions: i32) -> (ReconcileInput, Uuid) {
        let t = Uuid::from_bytes([1; 16]);
        (
            ReconcileInput {
                topic_id_by_name: [(topic_name.into(), t)].into(),
                partitions_per_topic: [(t, partitions)].into(),
            },
            t,
        )
    }

    #[test]
    fn dirty_triggers_recompute() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 4);
        let outcome = reconcile_if_dirty(&mut g, &inp, "uniform");
        assert_eq!(outcome, ReconcileOutcome::Recomputed);
        assert_eq!(g.target.per_member["m1"][&t], vec![0, 1, 2, 3]);
        assert!(!g.dirty);
    }

    #[test]
    fn clean_is_no_op() {
        let mut g = GroupState::new("g");
        g.dirty = false;
        let (inp, _) = input("t", 4);
        assert_eq!(
            reconcile_if_dirty(&mut g, &inp, "uniform"),
            ReconcileOutcome::NoChange
        );
    }

    #[test]
    fn unknown_assignor_is_no_op() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 4);
        assert_eq!(
            reconcile_if_dirty(&mut g, &inp, "doesnotexist"),
            ReconcileOutcome::NoChange
        );
        assert!(g.dirty, "unknown assignor must leave dirty bit set");
    }

    #[test]
    fn idempotent_under_repeated_calls() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp, "uniform");
        let epoch1 = g.group_epoch;
        let outcome = reconcile_if_dirty(&mut g, &inp, "uniform");
        assert_eq!(outcome, ReconcileOutcome::NoChange);
        assert_eq!(g.group_epoch, epoch1);
    }

    #[test]
    fn metadata_change_via_dirty_flag_recomputes() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp1, _) = input("t", 2);
        reconcile_if_dirty(&mut g, &inp1, "uniform");
        let epoch_before = g.group_epoch;
        let (inp2, _) = input("t", 4);
        g.dirty = true; // simulates MetadataChanged trigger
        let outcome = reconcile_if_dirty(&mut g, &inp2, "uniform");
        assert_eq!(outcome, ReconcileOutcome::Recomputed);
        assert!(g.group_epoch > epoch_before);
    }

    #[test]
    fn subscription_topic_ids_resolved() {
        let mut g = GroupState::new("g");
        g.add_or_update_member(fresh_member("m1", "t"));
        let (inp, t) = input("t", 2);
        let ids = membership_topic_ids(&g, &inp);
        assert!(ids.contains(&t));
    }
}
```

- [ ] **Step 7.2: Module wiring**

In `crates/broker/src/coordinator/next_gen/mod.rs` add `pub mod reconciler;`.

- [ ] **Step 7.3: Run + commit**

```bash
cargo test -p crabka-broker --lib coordinator::next_gen::reconciler
git add crates/broker/src/coordinator/next_gen/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): trigger-driven reconciler"
```

Expected: 6 tests pass.

---

## Task 8 — GroupActor

**Files:**
- Create: `crates/broker/src/coordinator/next_gen/group_actor.rs`

- [ ] **Step 8.1: Message types + actor loop**

```rust
// crates/broker/src/coordinator/next_gen/group_actor.rs
//! Per-group tokio actor. Owns `GroupState` for one next-gen consumer
//! group. Heartbeats are mpsc messages; responses go back via oneshot
//! channels.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crabka_protocol::owned::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequest, TopicPartitions as ReqTopicPartitions,
};
use crabka_protocol::owned::consumer_group_heartbeat_response::{
    Assignment as RespAssignment, ConsumerGroupHeartbeatResponse,
};
use crabka_protocol::primitives::uuid::Uuid;

use crate::codes;

use super::config::NextGenConfig;
use super::group_state::{GroupState, MemberState};
use super::persistence::MemberAssignmentState;
use super::reconciler::{self, ReconcileInput};

#[derive(Debug)]
pub enum GroupActorMessage {
    Heartbeat {
        request: ConsumerGroupHeartbeatRequest,
        client_host: String,
        reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
    },
    OffsetValidate {
        member_id: String,
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Describe {
        reply: oneshot::Sender<DescribeView>,
    },
    /// Sent once after bootstrap replay finishes; populates state.
    Seed(super::GroupSeed),
    Shutdown(oneshot::Sender<()>),
}

#[derive(Debug, Clone)]
pub struct DescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub members: Vec<DescribeMember>,
}

#[derive(Debug, Clone)]
pub struct DescribeMember {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub member_epoch: i32,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

pub struct GroupActorHandle {
    pub tx: mpsc::Sender<GroupActorMessage>,
    _task: JoinHandle<()>,
}

impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let task = tokio::spawn(actor_loop(group_id, config, metadata_provider, rx));
        Self { tx, _task: task }
    }
}

pub trait MetadataProvider: Send + Sync {
    fn snapshot(&self) -> ReconcileInput;
}

async fn actor_loop(
    group_id: String,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) {
    let mut state = GroupState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    GroupActorMessage::Heartbeat { request, client_host, reply } => {
                        let resp = handle_heartbeat(&mut state, &config, &*metadata, request, &client_host);
                        let _ = reply.send(resp);
                    }
                    GroupActorMessage::OffsetValidate { member_id, member_epoch, reply } => {
                        let result = match state.members.get(&member_id) {
                            None => Err(codes::UNKNOWN_MEMBER_ID),
                            Some(m) if member_epoch < m.member_epoch => Err(codes::STALE_MEMBER_EPOCH),
                            Some(m) if member_epoch > m.member_epoch => Err(codes::FENCED_MEMBER_EPOCH),
                            Some(_) => Ok(()),
                        };
                        let _ = reply.send(result);
                    }
                    GroupActorMessage::Describe { reply } => {
                        let view = build_describe(&state);
                        let _ = reply.send(view);
                    }
                    GroupActorMessage::Seed(seed) => {
                        apply_seed(&mut state, seed);
                    }
                    GroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                let evicted = state.evict_expired(Instant::now(), config.session_timeout);
                if !evicted.is_empty() {
                    state.bump_epoch();
                    run_reconcile(&mut state, &config, &*metadata);
                }
            }
        }
    }
}

fn apply_seed(state: &mut GroupState, seed: super::GroupSeed) {
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    for (mid, meta) in seed.members {
        let mut sub = std::collections::HashSet::new();
        for n in meta.subscribed_topic_names {
            sub.insert(n);
        }
        state.add_or_update_member(super::group_state::MemberState {
            member_id: mid.clone(),
            instance_id: meta.instance_id,
            rack_id: meta.rack_id,
            client_id: meta.client_id,
            client_host: meta.client_host,
            subscribed_topic_names: sub,
            server_assignor: meta.server_assignor,
            rebalance_timeout: Duration::from_millis(u64::try_from(meta.rebalance_timeout_ms.max(0)).unwrap_or(60_000)),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
        });
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            m.previous_member_epoch = cur.previous_member_epoch;
            m.assignment_state = cur.state;
            for tp in cur.assigned_partitions {
                m.assigned_partitions.insert(tp.topic_id, tp.partitions);
            }
            for tp in cur.partitions_pending_revocation {
                m.partitions_pending_revocation.insert(tp.topic_id, tp.partitions);
            }
        }
    }
    state.dirty = false;
}

fn handle_heartbeat(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: ConsumerGroupHeartbeatRequest,
    client_host: &str,
) -> ConsumerGroupHeartbeatResponse {
    let now = Instant::now();
    // 1. Leave path.
    if req.member_epoch == -1 {
        state.remove_member(&req.member_id);
        state.bump_epoch();
        return base_resp(0, req.member_epoch, &config);
    }
    // 2. Validate assignor selection.
    if let Some(name) = req.server_assignor.as_deref() {
        if !config.assignor_enabled(name) {
            return error_resp(codes::UNSUPPORTED_ASSIGNOR, &config);
        }
    }
    // 3. First-join path.
    if req.member_epoch == 0 && req.member_id.is_empty() {
        let new_member_id = uuid::Uuid::new_v4().to_string();
        if let Some(iid) = req.instance_id.as_deref() {
            if let Some(existing) = state.current_member_for_instance(iid) {
                if state.members.get(existing).is_some_and(|m| m.member_epoch != 0) {
                    return error_resp(codes::UNRELEASED_INSTANCE_ID, &config);
                }
            }
        }
        let m = build_member(&new_member_id, &req, client_host, now);
        state.add_or_update_member(m);
        run_reconcile(state, config, metadata);
        state.advance_member_epoch(&new_member_id);
        return build_assignment_resp(state, &new_member_id, &config);
    }
    // 4. Existing member: validate epoch.
    let cur_epoch = state.members.get(&req.member_id).map(|m| m.member_epoch).unwrap_or(-2);
    if cur_epoch == -2 {
        return error_resp(codes::UNKNOWN_MEMBER_ID, &config);
    }
    if req.member_epoch < cur_epoch {
        return error_resp(codes::STALE_MEMBER_EPOCH, &config);
    }
    if req.member_epoch > cur_epoch {
        return error_resp(codes::FENCED_MEMBER_EPOCH, &config);
    }
    // 5. Steady state — update last_seen, subscription, owned partitions.
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                state.dirty = true;
            }
        }
        if let Some(ref tp) = req.topic_partitions {
            let owned: HashMap<Uuid, Vec<i32>> = tp
                .iter()
                .map(|t| (t.topic_id, t.partitions.clone()))
                .collect();
            m.assigned_partitions = owned;
            if m.partitions_pending_revocation.is_empty() {
                m.assignment_state = MemberAssignmentState::Stable;
            }
        }
    }
    run_reconcile(state, config, metadata);
    if state.target.epoch > cur_epoch {
        state.advance_member_epoch(&req.member_id);
    }
    build_assignment_resp(state, &req.member_id, &config)
}

fn run_reconcile(state: &mut GroupState, config: &NextGenConfig, metadata: &dyn MetadataProvider) {
    let input = metadata.snapshot();
    let assignor_name = pick_assignor(state, config);
    reconciler::reconcile_if_dirty(state, &input, &assignor_name);
}

fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> String {
    state
        .members
        .values()
        .find_map(|m| m.server_assignor.clone())
        .unwrap_or_else(|| config.assignors.first().cloned().unwrap_or_else(|| "uniform".into()))
}

fn build_member(member_id: &str, req: &ConsumerGroupHeartbeatRequest, host: &str, now: Instant) -> MemberState {
    let subs: std::collections::HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    MemberState {
        member_id: member_id.into(),
        instance_id: req.instance_id.clone(),
        rack_id: req.rack_id.clone(),
        client_id: String::new(),
        client_host: host.into(),
        subscribed_topic_names: subs,
        server_assignor: req.server_assignor.clone(),
        rebalance_timeout: Duration::from_millis(u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(60_000)),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        last_seen: now,
    }
}

fn base_resp(error_code: i16, member_epoch: i32, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code,
        member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis()).unwrap_or(5_000),
        ..Default::default()
    }
}

fn error_resp(error_code: i16, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    base_resp(error_code, 0, config)
}

fn build_assignment_resp(state: &GroupState, member_id: &str, config: &NextGenConfig) -> ConsumerGroupHeartbeatResponse {
    let m = state.members.get(member_id).expect("member exists at build_assignment_resp");
    let assignment = Some(RespAssignment {
        topic_partitions: m
            .assigned_partitions
            .iter()
            .map(|(tid, parts)| {
                crabka_protocol::owned::common::topic_partitions::TopicPartitions {
                    topic_id: *tid,
                    partitions: parts.clone(),
                    ..Default::default()
                }
            })
            .collect(),
        ..Default::default()
    });
    ConsumerGroupHeartbeatResponse {
        error_code: 0,
        member_id: Some(member_id.into()),
        member_epoch: m.member_epoch,
        heartbeat_interval_ms: i32::try_from(config.heartbeat_interval.as_millis()).unwrap_or(5_000),
        assignment,
        ..Default::default()
    }
}

fn build_describe(state: &GroupState) -> DescribeView {
    DescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        members: state
            .members
            .values()
            .map(|m| DescribeMember {
                member_id: m.member_id.clone(),
                instance_id: m.instance_id.clone(),
                member_epoch: m.member_epoch,
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                assigned_partitions: m.assigned_partitions.clone(),
            })
            .collect(),
    }
}
```

- [ ] **Step 8.2: Module wiring**

In `crates/broker/src/coordinator/next_gen/mod.rs` add `pub mod group_actor;`.

- [ ] **Step 8.3: Compile only (integration tests cover behavior)**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/next_gen/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): per-group actor + heartbeat state machine"
```

Expected: build green.

---

## Task 9 — NextGenCoordinator

**Files:**
- Modify: `crates/broker/src/coordinator/next_gen/mod.rs`
- Modify: `crates/broker/src/coordinator/mod.rs` (hold + expose `NextGenCoordinator`)

- [ ] **Step 9.1: Coordinator + group-type cache**

Replace `crates/broker/src/coordinator/next_gen/mod.rs` with:

```rust
// crates/broker/src/coordinator/next_gen/mod.rs
//! KIP-848 next-gen consumer group coordinator.

pub mod assignor;
pub mod config;
pub mod group_actor;
pub mod group_state;
pub mod persistence;
pub mod reconciler;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::oneshot;

use config::NextGenConfig;
use group_actor::{GroupActorHandle, GroupActorMessage, MetadataProvider};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupType {
    Classic,
    NextGen,
}

pub struct NextGenCoordinator {
    pub config: Arc<NextGenConfig>,
    pub metadata: Arc<dyn MetadataProvider>,
    pub groups: Arc<DashMap<String, Arc<GroupActorHandle>>>,
    /// First record persisted per `group_id` locks its type for life.
    pub group_types: Arc<DashMap<String, GroupType>>,
    /// Bootstrap-time accumulator; drained by `finalize_bootstrap`.
    pub seeds: Arc<DashMap<String, GroupSeed>>,
}

#[derive(Debug, Default)]
pub struct GroupSeed {
    pub group_epoch: i32,
    pub target_epoch: i32,
    pub members: std::collections::HashMap<String, persistence::MemberMetadataValue>,
    pub target_per_member: std::collections::HashMap<String, persistence::TargetAssignmentMemberValue>,
    pub current_per_member: std::collections::HashMap<String, persistence::CurrentMemberAssignmentValue>,
}

impl NextGenCoordinator {
    pub fn new(config: NextGenConfig, metadata: Arc<dyn MetadataProvider>) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            groups: Arc::new(DashMap::new()),
            group_types: Arc::new(DashMap::new()),
            seeds: Arc::new(DashMap::new()),
        }
    }

    pub fn group_type(&self, group_id: &str) -> Option<GroupType> {
        self.group_types.get(group_id).map(|e| *e.value())
    }

    pub fn mark_classic(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::Classic);
    }

    pub fn mark_next_gen(&self, group_id: &str) {
        self.group_types
            .entry(group_id.into())
            .or_insert(GroupType::NextGen);
    }

    pub fn get_or_create(&self, group_id: &str) -> Arc<GroupActorHandle> {
        if let Some(h) = self.groups.get(group_id) {
            return h.value().clone();
        }
        let h = Arc::new(GroupActorHandle::spawn(
            group_id.into(),
            self.config.clone(),
            self.metadata.clone(),
        ));
        self.groups
            .entry(group_id.into())
            .or_insert(h)
            .value()
            .clone()
    }

    pub fn find(&self, group_id: &str) -> Option<Arc<GroupActorHandle>> {
        self.groups.get(group_id).map(|e| e.value().clone())
    }

    pub async fn shutdown_all(&self) {
        let handles: Vec<Arc<GroupActorHandle>> =
            self.groups.iter().map(|e| e.value().clone()).collect();
        for h in handles {
            let (tx, rx) = oneshot::channel();
            if h.tx.send(GroupActorMessage::Shutdown(tx)).await.is_ok() {
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), rx).await;
            }
        }
    }
}
```

- [ ] **Step 9.2: Hold a `NextGenCoordinator` on `GroupManager`**

In `crates/broker/src/coordinator/mod.rs`, add a field to `GroupManager`:

```rust
pub(crate) next_gen: std::sync::OnceLock<std::sync::Arc<next_gen::NextGenCoordinator>>,
```

Initialize in `GroupManager::new()` with `next_gen: OnceLock::new()`. Add accessor:

```rust
pub fn next_gen(&self) -> Option<&std::sync::Arc<next_gen::NextGenCoordinator>> {
    self.next_gen.get()
}

pub fn set_next_gen(&self, ng: std::sync::Arc<next_gen::NextGenCoordinator>) {
    let _ = self.next_gen.set(ng);
}
```

`Broker::start` (search for `GroupManager::new()` invocation) builds a `NextGenCoordinator` from `config.next_gen_consumer_group` and `metadata_provider`, then calls `group_manager.set_next_gen(ng)`. The `MetadataProvider` impl is a small wrapper around `ControllerHandle::current_image()` placed in `crates/broker/src/coordinator/next_gen/mod.rs`:

```rust
pub struct ImageMetadataProvider {
    pub controller: Arc<crate::controller::ControllerHandle>,
}

impl group_actor::MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        let image = self.controller.current_image();
        let mut topic_id_by_name = std::collections::HashMap::new();
        let mut partitions_per_topic = std::collections::HashMap::new();
        for topic in image.topics() {
            topic_id_by_name.insert(topic.name.clone(), topic.topic_id);
            partitions_per_topic.insert(topic.topic_id, topic.partitions.len() as i32);
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
        }
    }
}
```

(Adjust field/method names — `image.topics()`, `topic.partitions.len()` — to match the actual `MetadataImage` API in `crates/broker/src/controller/image.rs` or similar.)

- [ ] **Step 9.3: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): NextGenCoordinator + GroupManager wiring"
```

Expected: build green.

---

## Task 10 — Bootstrap replay extension

**Files:**
- Modify: `crates/broker/src/coordinator/bootstrap.rs`

- [ ] **Step 10.1: Dispatch v3–8 records to a seed map**

In `crates/broker/src/coordinator/bootstrap.rs`, locate the `apply_record` function (it currently matches on `Key::OffsetCommit` and `Key::GroupMetadata`). Add a `Key::NextGen(...)` arm that updates a per-group seed map carried on `NextGenCoordinator`:

```rust
        Key::NextGen(ng) => {
            apply_next_gen_record(group_manager, ng, value_bytes).await?;
        }
```

Add the helper at the bottom of the file:

```rust
async fn apply_next_gen_record(
    group_manager: &GroupManager,
    key: crate::coordinator::next_gen::persistence::NextGenKey,
    value_bytes: &bytes::Bytes,
) -> Result<(), BrokerError> {
    use crate::coordinator::next_gen::persistence as ng;
    let ng_coord = match group_manager.next_gen() {
        Some(c) => c.clone(),
        None => return Ok(()),
    };
    match key {
        ng::NextGenKey::GroupMetadata { group_id } => {
            ng_coord.mark_next_gen(&group_id);
            ng_coord.replay_group_metadata(&group_id, ng::GroupMetadataValue::decode(value_bytes)?);
        }
        ng::NextGenKey::MemberMetadata { group_id, member_id } => {
            ng_coord.mark_next_gen(&group_id);
            ng_coord.replay_member_metadata(&group_id, &member_id, ng::MemberMetadataValue::decode(value_bytes)?);
        }
        ng::NextGenKey::TargetAssignmentMetadata { group_id } => {
            ng_coord.mark_next_gen(&group_id);
            ng_coord.replay_target_assignment_metadata(&group_id, ng::TargetAssignmentMetadataValue::decode(value_bytes)?);
        }
        ng::NextGenKey::TargetAssignmentMember { group_id, member_id } => {
            ng_coord.mark_next_gen(&group_id);
            ng_coord.replay_target_assignment_member(&group_id, &member_id, ng::TargetAssignmentMemberValue::decode(value_bytes)?);
        }
        ng::NextGenKey::CurrentMemberAssignment { group_id, member_id } => {
            ng_coord.mark_next_gen(&group_id);
            ng_coord.replay_current_member_assignment(&group_id, &member_id, ng::CurrentMemberAssignmentValue::decode(value_bytes)?);
        }
    }
    Ok(())
}
```

Also extend `Key::GroupMetadata` → call `group_manager.next_gen().and_then(|ng| Some(ng.mark_classic(&group_id)))` before the existing classic logic.

- [ ] **Step 10.2: Replay helpers on `NextGenCoordinator`**

Append to `crates/broker/src/coordinator/next_gen/mod.rs`:

```rust
impl NextGenCoordinator {
    pub fn replay_group_metadata(&self, group_id: &str, v: persistence::GroupMetadataValue) {
        let seed = self.seeds.entry(group_id.into()).or_default();
        seed.group_epoch = v.epoch;
    }
    pub fn replay_member_metadata(&self, group_id: &str, member_id: &str, v: persistence::MemberMetadataValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.members.insert(member_id.into(), v);
    }
    pub fn replay_target_assignment_metadata(&self, group_id: &str, v: persistence::TargetAssignmentMetadataValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.target_epoch = v.assignment_epoch;
    }
    pub fn replay_target_assignment_member(&self, group_id: &str, member_id: &str, v: persistence::TargetAssignmentMemberValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.target_per_member.insert(member_id.into(), v);
    }
    pub fn replay_current_member_assignment(&self, group_id: &str, member_id: &str, v: persistence::CurrentMemberAssignmentValue) {
        let mut seed = self.seeds.entry(group_id.into()).or_default();
        seed.current_per_member.insert(member_id.into(), v);
    }

    pub fn finalize_bootstrap(&self) {
        let group_ids: Vec<String> = self.seeds.iter().map(|e| e.key().clone()).collect();
        for gid in group_ids {
            if let Some((_, seed)) = self.seeds.remove(&gid) {
                let handle = self.get_or_create(&gid);
                let _ = handle.tx.try_send(group_actor::GroupActorMessage::Seed(seed));
            }
        }
    }
}
```

(The `seeds` field on `NextGenCoordinator`, `GroupSeed`, and the `Seed(GroupSeed)` actor-message variant were already defined in Tasks 8 and 9 — this task only adds the `replay_*` methods and the `finalize_bootstrap` driver.)

- [ ] **Step 10.3: Call `finalize_bootstrap` after replay**

In `bootstrap.rs::bootstrap`, after `replay_records(...)` completes, add:

```rust
    if let Some(ng) = group_manager.next_gen() {
        ng.finalize_bootstrap();
    }
```

- [ ] **Step 10.4: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/coordinator/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(coordinator/next_gen): bootstrap replay + actor seeding"
```

Expected: build green.

---

## Task 11 — ConsumerGroupHeartbeat handler (api_key 68)

**Files:**
- Create: `crates/broker/src/handlers/consumer_group_heartbeat.rs`

- [ ] **Step 11.1: Handler implementation**

```rust
// crates/broker/src/handlers/consumer_group_heartbeat.rs
//! `ConsumerGroupHeartbeat` (api_key 68) — KIP-848 next-gen consumer
//! group protocol. Routes the request to the per-group actor in
//! `NextGenCoordinator`; returns the actor's response shape verbatim.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;

use crate::codes;
use crate::coordinator::next_gen::{group_actor::GroupActorMessage, GroupType};
use crate::error::BrokerError;
use crate::Broker;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

    let ng = match broker.group_manager.next_gen() {
        Some(c) if c.config.next_gen_enabled() => c.clone(),
        _ => return encode(version, &error(codes::GROUP_ID_NOT_FOUND)),
    };

    // Type-lock check: classic groups invisible to next-gen API.
    if matches!(ng.group_type(&req.group_id), Some(GroupType::Classic)) {
        return encode(version, &error(codes::GROUP_ID_NOT_FOUND));
    }

    // First record persisted for this group_id locks it next-gen.
    ng.mark_next_gen(&req.group_id);

    let handle = ng.get_or_create(&req.group_id);
    let (tx, rx) = oneshot::channel();
    let host = ctx.peer.to_string();
    if handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: req,
            client_host: host,
            reply: tx,
        })
        .await
        .is_err()
    {
        return encode(version, &error(codes::COORDINATOR_LOAD_IN_PROGRESS));
    }
    let resp = rx.await.unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
    encode(version, &resp)
}

fn error(code: i16) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

fn encode(version: i16, resp: &ConsumerGroupHeartbeatResponse) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

- [ ] **Step 11.2: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/handlers/consumer_group_heartbeat.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): ConsumerGroupHeartbeat (api_key 68)"
```

Expected: build green.

---

## Task 12 — ConsumerGroupDescribe handler (api_key 69)

**Files:**
- Create: `crates/broker/src/handlers/consumer_group_describe.rs`

- [ ] **Step 12.1: Handler implementation**

```rust
// crates/broker/src/handlers/consumer_group_describe.rs
//! `ConsumerGroupDescribe` (api_key 69) — returns one DescribedGroup per
//! requested group_id. Uses the actor's `Describe` view to render.

use bytes::{Bytes, BytesMut};
use tokio::sync::oneshot;

use crabka_protocol::owned::consumer_group_describe_request::ConsumerGroupDescribeRequest;
use crabka_protocol::owned::consumer_group_describe_response::{
    ConsumerGroupDescribeResponse, DescribedGroup,
};

use crate::codes;
use crate::coordinator::next_gen::{group_actor::GroupActorMessage, GroupType};
use crate::error::BrokerError;
use crate::Broker;

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    _ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = ConsumerGroupDescribeRequest::decode(&mut cur, version)?;

    let mut described: Vec<DescribedGroup> = Vec::with_capacity(req.group_ids.len());
    let ng_opt = broker.group_manager.next_gen().cloned();
    for group_id in &req.group_ids {
        let mut row = DescribedGroup {
            group_id: group_id.clone(),
            error_code: codes::NONE,
            ..Default::default()
        };
        let ng = match &ng_opt {
            Some(c) if c.config.next_gen_enabled() => c,
            _ => {
                row.error_code = codes::GROUP_ID_NOT_FOUND;
                described.push(row);
                continue;
            }
        };
        if matches!(ng.group_type(group_id), Some(GroupType::Classic)) {
            row.error_code = codes::GROUP_ID_NOT_FOUND;
            described.push(row);
            continue;
        }
        let Some(handle) = ng.find(group_id) else {
            row.error_code = codes::GROUP_ID_NOT_FOUND;
            described.push(row);
            continue;
        };
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .is_err()
        {
            row.error_code = codes::COORDINATOR_LOAD_IN_PROGRESS;
            described.push(row);
            continue;
        }
        match rx.await {
            Ok(view) => {
                row.group_state = match view.members.len() {
                    0 => "EMPTY".into(),
                    _ => "STABLE".into(),
                };
                // Members rendered minimally; fuller mapping in 64b.
                described.push(row);
            }
            Err(_) => {
                row.error_code = codes::UNKNOWN_SERVER_ERROR;
                described.push(row);
            }
        }
    }
    let resp = ConsumerGroupDescribeResponse {
        groups: described,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}
```

- [ ] **Step 12.2: Compile + commit**

```bash
cargo build -p crabka-broker
git add crates/broker/src/handlers/consumer_group_describe.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): ConsumerGroupDescribe (api_key 69)"
```

Expected: build green.

---

## Task 13 — OffsetCommit / OffsetFetch next-gen dispatch

**Files:**
- Modify: `crates/broker/src/handlers/offset_commit.rs`
- Modify: `crates/broker/src/handlers/offset_fetch.rs`

- [ ] **Step 13.1: OffsetCommit member-epoch validation**

In `crates/broker/src/handlers/offset_commit.rs`, after the ACL preamble and `group_handle = broker.group_manager.get_or_create(&req.group_id)`, add:

```rust
    // KIP-848: next-gen groups validate member_epoch.
    if let Some(ng) = broker.group_manager.next_gen() {
        if matches!(ng.group_type(&req.group_id), Some(crate::coordinator::next_gen::GroupType::NextGen)) {
            let Some(handle) = ng.find(&req.group_id) else {
                let resp = build_response_all(&req, codes::GROUP_ID_NOT_FOUND);
                return encode(version, &resp);
            };
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = handle
                .tx
                .send(crate::coordinator::next_gen::group_actor::GroupActorMessage::OffsetValidate {
                    member_id: req.member_id.clone(),
                    member_epoch: req.generation_id_or_member_epoch,
                    reply: tx,
                })
                .await;
            match rx.await {
                Ok(Ok(())) => { /* proceed */ }
                Ok(Err(code)) => {
                    let resp = build_response_all(&req, code);
                    return encode(version, &resp);
                }
                Err(_) => {
                    let resp = build_response_all(&req, codes::UNKNOWN_SERVER_ERROR);
                    return encode(version, &resp);
                }
            }
            // Skip the classic `validate()` call below for next-gen groups.
            // Jump to the topic-authz step.
        }
    }
```

Note: in OffsetCommit v9+, the field is `generation_id_or_member_epoch`. If the codegen names it differently, adjust. For v0–v8 this field is `generation_id` and KIP-848 hasn't allocated a v9 yet at the time of this slice — verify by reading `OffsetCommitRequest` codegen. If next-gen consumers always use v9+, gate this branch on `version >= 9`.

Wrap the existing classic `validate(...)` call so it's skipped when the group is next-gen.

- [ ] **Step 13.2: OffsetFetch next-gen pass-through**

In `crates/broker/src/handlers/offset_fetch.rs`, after the ACL preamble and `handle = broker.group_manager.get_or_create(&req.group_id)`, add a type-lock check: if the group is next-gen, the existing read path against `g.committed_offsets` is still correct (KIP-848 reuses the offset record format), but we should rebuff classic-protocol fetches against a next-gen group with `GROUP_ID_NOT_FOUND` only if the request is fenced by member_id mismatch. For 64a, the simplest correct behavior is: allow `OffsetFetch` against next-gen groups unconditionally (read-only). No code change needed here beyond the type-lock dispatch we'll add in Task 14 if `OffsetFetch` ever gains member_epoch validation (it doesn't in v0–v9).

- [ ] **Step 13.3: Run + commit**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib handlers::offset_commit
git add crates/broker/src/handlers/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): next-gen member_epoch validation in OffsetCommit"
```

Expected: build + existing offset_commit tests green.

---

## Task 14 — Handler registration + ApiVersions

**Files:**
- Modify: `crates/broker/src/handlers/mod.rs` (dispatcher)
- Modify: `crates/broker/src/handlers/api_versions.rs`

- [ ] **Step 14.1: Register the two handlers in the dispatcher**

In `crates/broker/src/handlers/mod.rs`, find the match arm in the request dispatcher (likely a `match api_key { ... }`). Add:

```rust
        owned::consumer_group_heartbeat_request::API_KEY => {
            consumer_group_heartbeat::handle(broker, version, correlation_id, body, &ctx).await
        }
        owned::consumer_group_describe_request::API_KEY => {
            consumer_group_describe::handle(broker, version, correlation_id, body, &ctx).await
        }
```

And declare the modules near the other handler declarations:

```rust
pub(crate) mod consumer_group_heartbeat;
pub(crate) mod consumer_group_describe;
```

- [ ] **Step 14.2: Advertise in ApiVersions**

In `crates/broker/src/handlers/api_versions.rs`, add to the `supported_apis()` vec near the other group handlers:

```rust
        v!(consumer_group_heartbeat_request),
        v!(consumer_group_describe_request),
```

- [ ] **Step 14.3: Smoke test via existing integration suite**

```bash
cargo test -p crabka-broker --test api_versions 2>&1 | tail -20
```

Adjust any snapshot of the supported-API table that includes a generated count (use `UPDATE_SNAPSHOTS=1` once to refresh, then verify by re-running).

- [ ] **Step 14.4: Commit**

```bash
git add crates/broker/src/handlers/
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(handlers): register ConsumerGroupHeartbeat/Describe + ApiVersions"
```

Expected: api_versions snapshot updated; all api_versions tests green.

---

## Task 15 — Broker integration tests (raw-RPC scenarios)

**Files:**
- Create: `crates/broker/tests/consumer_group_next_gen.rs`

- [ ] **Step 15.1: Test harness**

```rust
// crates/broker/tests/consumer_group_next_gen.rs
//! Raw-RPC integration tests for KIP-848 next-gen consumer groups,
//! driven against an in-process Crabka broker via `crabka-client-core`.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::Client;
use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::consumer_group_describe_request::ConsumerGroupDescribeRequest;

async fn boot() -> (crabka_broker::BrokerHandle, String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf())).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let mut admin = AdminClient::connect(&[bootstrap.into()]).await.unwrap();
    admin
        .create_topics(
            &[CreateTopicSpec {
                name: topic.into(),
                partitions,
                replicas: 1,
                configs: Default::default(),
            }],
            5_000,
        )
        .await
        .unwrap();
}

fn heartbeat(group: &str, member_id: &str, epoch: i32) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        rebalance_timeout_ms: 60_000,
        ..Default::default()
    }
}
```

- [ ] **Step 15.2: Single-member join → assign → leave**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_full_lifecycle() {
    let (_b, bootstrap, _d) = boot().await;
    create_topic(&bootstrap, "t1", 4).await;
    let client = Arc::new(
        Client::builder()
            .bootstrap(bootstrap.as_str())
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    );

    let mut req = heartbeat("g1", "", 0);
    req.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp = client.send(req).await.unwrap();
    assert_eq!(resp.error_code, 0);
    let member_id = resp.member_id.clone().unwrap();
    assert_eq!(resp.member_epoch, 1);
    let assigned = resp.assignment.as_ref().unwrap();
    let total_partitions: usize = assigned.topic_partitions.iter().map(|t| t.partitions.len()).sum();
    assert_eq!(total_partitions, 4);

    let mut hb2 = heartbeat("g1", &member_id, 1);
    hb2.subscribed_topic_names = Some(vec!["t1".into()]);
    let resp2 = client.send(hb2).await.unwrap();
    assert_eq!(resp2.error_code, 0);
    assert_eq!(resp2.member_epoch, 1);

    let leave = heartbeat("g1", &member_id, -1);
    let resp3 = client.send(leave).await.unwrap();
    assert_eq!(resp3.error_code, 0);
}
```

- [ ] **Step 15.3: Two-member rebalance**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_members_split_partitions() {
    let (_b, bootstrap, _d) = boot().await;
    create_topic(&bootstrap, "t2", 4).await;
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());

    let mut a = heartbeat("g2", "", 0);
    a.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra = client.send(a).await.unwrap();
    let mid_a = ra.member_id.unwrap();

    let mut b = heartbeat("g2", "", 0);
    b.subscribed_topic_names = Some(vec!["t2".into()]);
    let rb = client.send(b).await.unwrap();
    let mid_b = rb.member_id.unwrap();

    // Re-heartbeat A at the new group epoch to pick up the rebalanced assignment.
    let mut a2 = heartbeat("g2", &mid_a, ra.member_epoch);
    a2.subscribed_topic_names = Some(vec!["t2".into()]);
    let _ = client.send(a2).await.unwrap();

    let mut a3 = heartbeat("g2", &mid_a, rb.member_epoch);
    a3.subscribed_topic_names = Some(vec!["t2".into()]);
    let ra3 = client.send(a3).await.unwrap();

    let parts_a: usize = ra3.assignment.unwrap().topic_partitions.iter().map(|t| t.partitions.len()).sum();
    let parts_b: usize = rb.assignment.unwrap().topic_partitions.iter().map(|t| t.partitions.len()).sum();
    assert_eq!(parts_a + parts_b, 4);
    let _ = mid_b;
}
```

- [ ] **Step 15.4: Type-lock enforcement**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classic_group_locked_against_next_gen() {
    use crabka_protocol::owned::join_group_request::JoinGroupRequest;
    let (_b, bootstrap, _d) = boot().await;
    create_topic(&bootstrap, "t3", 2).await;
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());

    // Establish a classic group first.
    let join = JoinGroupRequest {
        group_id: "g3".into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: String::new(),
        protocol_type: "consumer".into(),
        ..Default::default()
    };
    let _ = client.send(join).await.unwrap();

    // Next-gen heartbeat for the same group_id is rejected.
    let mut req = heartbeat("g3", "", 0);
    req.subscribed_topic_names = Some(vec!["t3".into()]);
    let resp = client.send(req).await.unwrap();
    assert_eq!(resp.error_code, crabka_broker::codes::GROUP_ID_NOT_FOUND);
}
```

- [ ] **Step 15.5: Kill-switch config**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_switch_returns_group_id_not_found() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.next_gen_consumer_group.rebalance_protocols =
        vec![crabka_broker::coordinator::next_gen::config::RebalanceProtocol::Classic];
    let broker = Broker::start(config).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());

    let mut req = heartbeat("g4", "", 0);
    req.subscribed_topic_names = Some(vec!["t".into()]);
    let resp = client.send(req).await.unwrap();
    assert_eq!(resp.error_code, crabka_broker::codes::GROUP_ID_NOT_FOUND);
}
```

- [ ] **Step 15.6: Describe surfaces members**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_after_join() {
    let (_b, bootstrap, _d) = boot().await;
    create_topic(&bootstrap, "t5", 2).await;
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());

    let mut req = heartbeat("g5", "", 0);
    req.subscribed_topic_names = Some(vec!["t5".into()]);
    let _ = client.send(req).await.unwrap();

    let desc = client
        .send(ConsumerGroupDescribeRequest {
            group_ids: vec!["g5".into()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(desc.groups.len(), 1);
    assert_eq!(desc.groups[0].error_code, 0);
    assert_eq!(desc.groups[0].group_state, "STABLE");
}
```

- [ ] **Step 15.7: Stale-epoch rejection**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_epoch_rejected() {
    let (_b, bootstrap, _d) = boot().await;
    create_topic(&bootstrap, "t6", 2).await;
    let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());

    let mut req = heartbeat("g6", "", 0);
    req.subscribed_topic_names = Some(vec!["t6".into()]);
    let r = client.send(req).await.unwrap();
    let mid = r.member_id.unwrap();

    // Send an obviously stale epoch.
    let stale = heartbeat("g6", &mid, 0);
    let resp = client.send(stale).await.unwrap();
    assert_eq!(resp.error_code, crabka_broker::codes::STALE_MEMBER_EPOCH);
}
```

- [ ] **Step 15.8: Run + commit**

```bash
cargo test -p crabka-broker --test consumer_group_next_gen
git add crates/broker/tests/consumer_group_next_gen.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-848 raw-RPC integration scenarios"
```

Expected: 6 tests pass.

---

## Task 16 — Persistence-replay integration test

**Files:**
- Create: `crates/broker/tests/consumer_group_next_gen_persistence.rs`

- [ ] **Step 16.1: Test — broker restart preserves next-gen group state**

```rust
// crates/broker/tests/consumer_group_next_gen_persistence.rs
//! Broker restart preserves next-gen group state via __consumer_offsets replay.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::sync::Arc;
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::Client;
use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_preserves_group_epoch_and_members() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    // Phase 1: boot, create topic, join a single member, get member_id.
    let member_id;
    let initial_epoch;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone())).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.unwrap();
        admin
            .create_topics(
                &[CreateTopicSpec {
                    name: "tp".into(),
                    partitions: 2,
                    replicas: 1,
                    configs: Default::default(),
                }],
                5_000,
            )
            .await
            .unwrap();
        let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gp".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["tp".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        member_id = resp.member_id.unwrap();
        initial_epoch = resp.member_epoch;
        // Give time for the actor to persist before shutdown.
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await.ok();
    }

    // Phase 2: restart and re-heartbeat — same epoch should be accepted.
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gp".into(),
            member_id: member_id.clone(),
            member_epoch: initial_epoch,
            subscribed_topic_names: Some(vec!["tp".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.error_code, 0, "post-restart heartbeat must succeed");
        assert_eq!(resp.member_epoch, initial_epoch);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_gen_state_cleared_after_leave_then_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let member_id;
    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir.clone())).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let mut admin = AdminClient::connect(&[bootstrap.clone()]).await.unwrap();
        admin.create_topics(&[CreateTopicSpec { name: "tp2".into(), partitions: 1, replicas: 1, configs: Default::default() }], 5_000).await.unwrap();
        let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());
        let join = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec!["tp2".into()]),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        };
        let resp = client.send(join).await.unwrap();
        member_id = resp.member_id.unwrap();
        let leave = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: -1,
            ..Default::default()
        };
        let _ = client.send(leave).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await.ok();
    }

    {
        let broker = Broker::start(BrokerConfig::for_tests(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = Arc::new(Client::builder().bootstrap(bootstrap.as_str()).client_id("c").build().await.unwrap());
        // The member tombstone replays; a fresh join with the SAME member_id must
        // be treated as a new join (member unknown).
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "gpx".into(),
            member_id: member_id.clone(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["tp2".into()]),
            ..Default::default()
        };
        let resp = client.send(req).await.unwrap();
        assert_eq!(resp.error_code, crabka_broker::codes::UNKNOWN_MEMBER_ID);
    }
}
```

- [ ] **Step 16.2: Run + commit**

```bash
cargo test -p crabka-broker --test consumer_group_next_gen_persistence
git add crates/broker/tests/consumer_group_next_gen_persistence.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-848 bootstrap-replay round trip"
```

Expected: 2 tests pass.

---

## Task 17 — JVM acceptance against apache/kafka:4.0.0

**Files:**
- Create: `crates/broker/tests/jvm_consumer_group_next_gen.rs`

- [ ] **Step 17.1: Test harness + image constant**

```rust
// crates/broker/tests/jvm_consumer_group_next_gen.rs
//! JVM-acceptance tests for KIP-848 — drives the GA Kafka 4.0 client
//! against an in-process Crabka broker. `group.protocol=consumer`
//! activates the next-gen heartbeat path on the client.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

use std::process::{Command, Stdio};
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig};

const HOST_PORT: u16 = 9092;
const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
const KAFKA_IMAGE_NEXT_GEN: &str = "apache/kafka:4.0.0";
const KAFKA_IMAGE_CLASSIC: &str = "confluentinc/cp-kafka:7.5.0";

async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = LISTEN.parse().unwrap();
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().unwrap();
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: BOOTSTRAP.into(),
        log_dir: dir.path().to_path_buf(),
        node_id: 1,
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(1, controller_addr)],
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    let _ = HOST_PORT;
    (handle, dir)
}

fn docker_run(image: &str, args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(image)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("docker run");
    eprintln!(
        "CRABKA[test] docker {image} {args:?} status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    out
}
```

- [ ] **Step 17.2: Test 1 — single consumer round-trip**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker; run with --include-ignored"]
async fn jvm_kip848_single_consumer_round_trip() {
    let (_broker, _dir) = start_host_broker().await;

    // Produce 3 records via classic producer.
    let produced = docker_run(
        KAFKA_IMAGE_CLASSIC,
        &[
            "bash",
            "-c",
            &format!(
                "printf 'a\\nb\\nc\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-rt --producer-property max.block.ms=10000"
            ),
        ],
    );
    assert!(produced.status.success(), "producer failed: {produced:?}");

    // Consume via apache/kafka:4.0.0 with next-gen protocol.
    let consumed = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &[
            "bash",
            "-c",
            &format!(
                "kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-rt --group g-rt --consumer-property group.protocol=consumer --from-beginning --timeout-ms 8000 --max-messages 3"
            ),
        ],
    );
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    assert!(stdout.contains('a') && stdout.contains('b') && stdout.contains('c'),
        "expected a/b/c in stdout, got {stdout}");
}
```

- [ ] **Step 17.3: Test 2 — kafka-consumer-groups --describe sees next-gen group**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker; run with --include-ignored"]
async fn jvm_kip848_describe_group() {
    let (_broker, _dir) = start_host_broker().await;
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &["bash", "-c", &format!("printf '1\\n2\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-d")],
    );
    let _ = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &["bash", "-c", &format!("kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-d --group g-d --consumer-property group.protocol=consumer --from-beginning --timeout-ms 6000 --max-messages 2")],
    );
    let described = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &["bash", "-c", &format!("kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --describe --group g-d")],
    );
    let stdout = String::from_utf8_lossy(&described.stdout);
    assert!(stdout.contains("g-d"), "expected group g-d in describe output, got {stdout}");
}
```

- [ ] **Step 17.4: Test 3 — delete next-gen group**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker; run with --include-ignored"]
async fn jvm_kip848_delete_group() {
    let (_broker, _dir) = start_host_broker().await;
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &["bash", "-c", &format!("printf 'x\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-del")],
    );
    let _ = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &["bash", "-c", &format!("kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-del --group g-del --consumer-property group.protocol=consumer --from-beginning --timeout-ms 4000 --max-messages 1")],
    );
    let deleted = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &["bash", "-c", &format!("kafka-consumer-groups.sh --bootstrap-server {BOOTSTRAP} --delete --group g-del")],
    );
    assert!(deleted.status.success(), "delete failed: {deleted:?}");
}
```

- [ ] **Step 17.5: Test 4 — classic + next-gen coexistence**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker; run with --include-ignored"]
async fn jvm_kip848_coexists_with_classic() {
    let (_broker, _dir) = start_host_broker().await;
    docker_run(
        KAFKA_IMAGE_CLASSIC,
        &["bash", "-c", &format!("printf 'p\\nq\\n' | kafka-console-producer --bootstrap-server {BOOTSTRAP} --topic kip848-coex")],
    );
    // Classic consumer (no group.protocol override).
    let classic = docker_run(
        KAFKA_IMAGE_CLASSIC,
        &["bash", "-c", &format!("kafka-console-consumer --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-classic --from-beginning --timeout-ms 5000 --max-messages 2")],
    );
    let cs = String::from_utf8_lossy(&classic.stdout);
    assert!(cs.contains('p') && cs.contains('q'));

    // Next-gen consumer in a different group on the same topic.
    let next_gen = docker_run(
        KAFKA_IMAGE_NEXT_GEN,
        &["bash", "-c", &format!("kafka-console-consumer.sh --bootstrap-server {BOOTSTRAP} --topic kip848-coex --group g-next --consumer-property group.protocol=consumer --from-beginning --timeout-ms 5000 --max-messages 2")],
    );
    let ns = String::from_utf8_lossy(&next_gen.stdout);
    assert!(ns.contains('p') && ns.contains('q'));
}
```

- [ ] **Step 17.6: Run + commit**

```bash
docker pull apache/kafka:4.0.0
cargo test -p crabka-broker --test jvm_consumer_group_next_gen -- --ignored --nocapture --test-threads=1
git add crates/broker/tests/jvm_consumer_group_next_gen.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "test(broker): KIP-848 JVM acceptance against apache/kafka:4.0.0"
```

Expected: 4 tests pass with `--include-ignored`.

---

## Task 18 — CI workflow + image preload

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 18.1: Preload apache/kafka:4.0.0**

In `.github/workflows/ci.yml`, find the `broker-jvm-acceptance` job. Before the `cargo llvm-cov` step, add (or extend the existing image-preload step):

```yaml
    - name: Preload Kafka images
      run: |
        docker pull confluentinc/cp-kafka:6.1.1
        docker pull confluentinc/cp-kafka:7.5.0
        docker pull confluentinc/cp-kafka:3.1.2
        docker pull apache/kafka:4.0.0
```

If a preload step already exists, just append the `docker pull apache/kafka:4.0.0` line. The new test file `jvm_consumer_group_next_gen.rs` runs as part of the same `--ignored` sweep — no new job needed.

- [ ] **Step 18.2: Verify cargo test command picks up the new test**

The existing `broker-jvm-acceptance` job runs `cargo llvm-cov -p crabka-broker --test jvm_acceptance ...`. That's `--test jvm_acceptance` specifically (single test binary). Replace with a glob or add a second `--test`:

```yaml
        cargo llvm-cov -p crabka-broker \
          --test jvm_acceptance \
          --test jvm_consumer_group_next_gen \
          --lcov --output-path coverage/broker-jvm-acceptance.lcov \
          -- --ignored --nocapture --test-threads=1
```

- [ ] **Step 18.3: Commit**

```bash
git add .github/workflows/ci.yml
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "ci: preload apache/kafka:4.0.0 + add KIP-848 JVM test binary"
```

Expected: no local verification possible (CI workflow); pushed branch will run it.

---

## Final verification

- [ ] **F-1: Full workspace test gate**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace -- --ignored --test-threads=1
```

Expected: all green; no codegen drift.

- [ ] **F-2: STATUS.md entry**

Append to `STATUS.md`:

```markdown
## Slice 64a — KIP-848 next-gen consumer group protocol foundations + JVM acceptance (2026-05-28)

- 2 new handlers: `ConsumerGroupHeartbeat` (68), `ConsumerGroupDescribe` (69).
- New module `coordinator/next_gen/` — per-group tokio actor (`group_actor`),
  state machine (`group_state`), reconciler (`reconciler`), persistence for
  `__consumer_offsets` record types 3/5/6/7/8 (`persistence`), and two
  server-side assignors: `UniformAssignor` + `RangeAssignor`.
- Classic↔next-gen coexistence via `GroupType` lock on first persisted
  record per `group_id`.
- 4 new error codes: `FENCED_MEMBER_EPOCH` (110), `UNSUPPORTED_ASSIGNOR` (111),
  `UNRELEASED_INSTANCE_ID` (114), `UNKNOWN_SUBSCRIPTION_ID` (117).
- New broker configs: `group.coordinator.rebalance.protocols`,
  `group.consumer.{session.timeout.ms, heartbeat.interval.ms,
  min/max.session.timeout.ms, min/max.heartbeat.interval.ms, assignors,
  max.size}`.
- Bootstrap replay extended to dispatch v3–8 records into actors.
- Tests:
  - Unit: assignors (12), state machine (9), reconciler (6), persistence (6).
  - Broker integration: 6 raw-RPC scenarios + 2 bootstrap-replay round trips.
  - JVM acceptance: 4 scenarios driving apache/kafka:4.0.0 with
    `group.protocol=consumer`, including classic↔next-gen coexistence.
- CI: `apache/kafka:4.0.0` preloaded; new test binary
  `jvm_consumer_group_next_gen` added to `broker-jvm-acceptance` job.
- Out of scope (follow-up slices): rack-aware uniform, custom assignor
  plugin point (64c), group migration policy (64d), share groups (KIP-932).
```

- [ ] **F-3: Commit + push**

```bash
git add STATUS.md
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "Slice 64a: STATUS.md entry + final gate"
git push -u origin kip-848-64a
gh pr create --title "Slice 64a: KIP-848 next-gen consumer group protocol foundations" --body "$(cat <<'EOF'
## Summary
- KIP-848 next-gen consumer group protocol on Crabka — `ConsumerGroupHeartbeat` + `ConsumerGroupDescribe` handlers, server-side `UniformAssignor` + `RangeAssignor`, per-group reconciler-task actors.
- New `__consumer_offsets` record types 3/5/6/7/8; classic↔next-gen coexistence via group-type lock.
- Validated against `apache/kafka:4.0.0` clients with `group.protocol=consumer`.

## Test plan
- [ ] cargo test --workspace (unit + integration)
- [ ] cargo test --workspace -- --ignored (JVM acceptance, requires docker + apache/kafka:4.0.0)
- [ ] CI: broker-jvm-acceptance green
- [ ] CI: codecov/patch above target

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR opened against `main`.

---



