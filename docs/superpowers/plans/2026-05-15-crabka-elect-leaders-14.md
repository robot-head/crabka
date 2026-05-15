# Slice 14: ElectLeaders + auto-rebalance — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Kafka's operator-facing leader-election controls — manual `ElectLeaders` RPC (api_key 43, PREFERRED + UNCLEAN types per KIP-460) and a background auto-preferred-replica rebalance ticker — on top of slice 10b's automatic-on-broker-death election.

**Architecture:** A pure-logic `select_new_leader_for_partition` function in `crates/broker/src/leader_election.rs` produces a new `PartitionRecord` for one partition under one election type, returning a small `ElectError` enum for the various refusal cases. The new `elect_leaders.rs` wire handler decodes requests, runs Cluster Alter authorize, and submits per-partition results via `controller.submit_change`. A separate `leader_rebalance.rs` background task wakes every `leader_imbalance_check_interval_secs`, scans the image for imbalanced partitions, and submits preferred-elections in batches when above the threshold.

**Tech Stack:** Rust 1.95.0; reuses slice 10b's `ControllerLivenessState`, slice 12's `ConnectionAuth`/`Principal` plumbing, and slice 13's `authorize` + super-user bypass. Wire types already generated at `crates/protocol/generated/ElectLeaders{Request,Response}.owned.rs` (api_key 43, v0–v2, flexible from v2).

**Reference spec:** [`docs/superpowers/specs/2026-05-15-crabka-elect-leaders-14-design.md`](../specs/2026-05-15-crabka-elect-leaders-14-design.md).

**Working directory:** `C:\Users\Matt Stone\git\crabka`. Branch `feature/elect-leaders-14` already created off main with spec committed.

---

## File structure

```
crates/broker/src/
├── leader_election.rs      # MODIFIED — ElectionType + ElectError + select_new_leader_for_partition + 8 unit tests
├── leader_rebalance.rs     # NEW       — background ticker; ControllerLike trait; rebalance_tick + 2 unit tests
├── handlers/
│   ├── elect_leaders.rs    # NEW       — api_key 43 handler
│   ├── mod.rs              # MODIFIED  — registration left to inline-intercept (slice 13 pattern)
│   └── api_versions.rs     # MODIFIED  — supported_apis += 43
├── network/dispatch.rs     # MODIFIED  — flex table entry + handle_elect_leaders_frame intercept arm
├── codes.rs                # MODIFIED  — PREFERRED_LEADER_NOT_AVAILABLE/ELIGIBLE_LEADERS_NOT_AVAILABLE/ELECTION_NOT_NEEDED constants
├── error.rs                # MODIFIED  — InvalidLeaderRebalanceInterval + InvalidLeaderRebalanceThreshold variants
├── config.rs               # MODIFIED  — 3 new BrokerConfig fields + validate() + 4 tests
├── broker.rs               # MODIFIED  — spawn leader_rebalance task from Broker::start
└── lib.rs                  # MODIFIED  — pub mod leader_rebalance

crates/broker/tests/
├── elect_leaders.rs        # NEW       — 4 broker-side integration tests (no Docker)
└── jvm_acceptance.rs       # MODIFIED  — 1 new JVM test
```

12 tasks across 6 batches.

---

## Batch 1 — Election algorithm

### Task 1: `select_new_leader_for_partition` + matrix tests

**Files:**
- Modify: `crates/broker/src/leader_election.rs`

The existing file holds slice 10b's `on_broker_dead`. Extend with the operator-triggered election entry point and the matrix-of-errors enum.

- [ ] **Step 1: Append types to `leader_election.rs`**

After the existing `on_broker_dead` function, add:

```rust
/// Operator-triggered election type per KIP-460.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectionType {
    /// Move leadership back to the first replica in `replicas[]` if it's
    /// alive and in the ISR. Safe — no data loss possible.
    Preferred,
    /// Allow election outside the ISR when every ISR member is dead.
    /// Operator has accepted the possible-data-loss risk.
    Unclean,
}

/// Reasons `select_new_leader_for_partition` may refuse to elect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElectError {
    UnknownTopicOrPartition,
    PreferredAlreadyLeader,
    PreferredNotInIsr,
    PreferredNotAlive,
    NoEligibleReplica,
    NotControllerLeader,
}
```

- [ ] **Step 2: Add the algorithm**

```rust
/// Operator-triggered single-partition election. Returns the new
/// `PartitionRecord` ready to submit, or an `ElectError`.
///
/// Pure: no I/O, no panics. Caller is responsible for submitting the
/// returned record via the controller.
pub(crate) async fn select_new_leader_for_partition(
    image: &MetadataImage,
    liveness: &ControllerLivenessState,
    topic: &str,
    partition: i32,
    election: ElectionType,
) -> Result<PartitionRecord, ElectError> {
    let pr = image
        .partition(topic, partition)
        .ok_or(ElectError::UnknownTopicOrPartition)?;
    match election {
        ElectionType::Preferred => {
            let preferred = *pr
                .replicas
                .first()
                .ok_or(ElectError::UnknownTopicOrPartition)?;
            if pr.leader == preferred {
                return Err(ElectError::PreferredAlreadyLeader);
            }
            if !pr.isr.contains(&preferred) {
                return Err(ElectError::PreferredNotInIsr);
            }
            if !liveness.is_alive(preferred).await {
                return Err(ElectError::PreferredNotAlive);
            }
            Ok(PartitionRecord {
                topic: pr.topic.clone(),
                partition: pr.partition,
                leader: preferred,
                replicas: pr.replicas.clone(),
                isr: pr.isr.clone(),
                leader_epoch: pr.leader_epoch + 1,
            })
        }
        ElectionType::Unclean => {
            // Bail if any ISR member is alive — UNCLEAN is meant for
            // catastrophic ISR loss, not routine rebalances.
            for &n in &pr.isr {
                if liveness.is_alive(n).await {
                    return Err(ElectError::PreferredAlreadyLeader);
                }
            }
            // Find the first alive replica, in or out of ISR.
            for &n in &pr.replicas {
                if liveness.is_alive(n).await {
                    return Ok(PartitionRecord {
                        topic: pr.topic.clone(),
                        partition: pr.partition,
                        leader: n,
                        replicas: pr.replicas.clone(),
                        isr: vec![n],
                        leader_epoch: pr.leader_epoch + 1,
                    });
                }
            }
            Err(ElectError::NoEligibleReplica)
        }
    }
}
```

Note: `is_alive` is `async` on the existing `ControllerLivenessState` (locks a `tokio::sync::Mutex` internally). The function is async because of that, not because of any I/O.

- [ ] **Step 3: Write 8 unit tests**

Append to `crates/broker/src/leader_election.rs`. The file already has a `#[cfg(test)] mod tests` from slice 10b — add to it. Helpers needed:

```rust
    use crabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
    use uuid::Uuid;

    async fn img_with_partition(
        topic: &str,
        partition: i32,
        leader: NodeId,
        replicas: &[NodeId],
        isr: &[NodeId],
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition,
            leader,
            replicas: replicas.to_vec(),
            isr: isr.to_vec(),
            leader_epoch: 5,
        }));
        img
    }

    async fn liveness_with_alive(alive: &[NodeId]) -> std::sync::Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(std::time::Duration::from_secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        std::sync::Arc::new(l)
    }
```

Then the 8 tests:

```rust
    #[tokio::test]
    async fn preferred_happy_path() {
        let img = img_with_partition("foo", 0, /*leader*/ 2, &[1, 2, 3], &[1, 2, 3]).await;
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .expect("should elect");
        assert_eq!(new_pr.leader, 1);
        assert_eq!(new_pr.isr, vec![1, 2, 3]);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn preferred_already_leader() {
        let img = img_with_partition("foo", 0, /*leader*/ 1, &[1, 2, 3], &[1, 2, 3]).await;
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredAlreadyLeader);
    }

    #[tokio::test]
    async fn preferred_not_in_isr() {
        let img = img_with_partition("foo", 0, 2, &[1, 2, 3], &[2, 3]).await;
        let l = liveness_with_alive(&[1, 2, 3]).await;
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredNotInIsr);
    }

    #[tokio::test]
    async fn preferred_not_alive() {
        let img = img_with_partition("foo", 0, 2, &[1, 2, 3], &[1, 2, 3]).await;
        let l = liveness_with_alive(&[2, 3]).await; // 1 dead
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredNotAlive);
    }

    #[tokio::test]
    async fn unclean_happy_path() {
        // ISR is just {1}, broker 1 is dead, brokers 2/3 are alive.
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]).await;
        let l = liveness_with_alive(&[2, 3]).await;
        let new_pr = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .expect("unclean should elect");
        assert_eq!(new_pr.leader, 2);
        assert_eq!(new_pr.isr, vec![2]);
        assert_eq!(new_pr.leader_epoch, 6);
    }

    #[tokio::test]
    async fn unclean_no_alive_replicas() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1]).await;
        let l = liveness_with_alive(&[]).await; // everyone dead
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::NoEligibleReplica);
    }

    #[tokio::test]
    async fn unclean_isr_member_alive_returns_election_not_needed() {
        let img = img_with_partition("foo", 0, 1, &[1, 2, 3], &[1, 2]).await;
        let l = liveness_with_alive(&[1, 2]).await; // ISR has live member
        let err = select_new_leader_for_partition(&img, &l, "foo", 0, ElectionType::Unclean)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::PreferredAlreadyLeader);
    }

    #[tokio::test]
    async fn unknown_topic_returns_error() {
        let img = MetadataImage::new(Uuid::nil());
        let l = liveness_with_alive(&[]).await;
        let err = select_new_leader_for_partition(&img, &l, "ghost", 0, ElectionType::Preferred)
            .await
            .unwrap_err();
        assert_eq!(err, ElectError::UnknownTopicOrPartition);
    }
```

- [ ] **Step 4: Run tests + lints**

```bash
cargo test -p crabka-broker --lib leader_election
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: 8 new tests PASS. Any clippy warnings on the new code (e.g. `must_use_candidate`, `doc_markdown`) — fix per workspace conventions.

- [ ] **Step 5: Commit**

```bash
git add crates/broker/src/leader_election.rs
git commit -m "feat(broker): select_new_leader_for_partition + ElectError matrix

Pure-logic algorithm for KIP-460 PREFERRED + UNCLEAN leader election.
Shared between the operator-triggered ElectLeaders handler (T4) and
the auto-rebalance background task (T7).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Wire error code constants + `BrokerError` variants

**Files:**
- Modify: `crates/broker/src/codes.rs`
- Modify: `crates/broker/src/error.rs`

- [ ] **Step 1: Append error code constants in `crates/broker/src/codes.rs`**

```rust
pub const PREFERRED_LEADER_NOT_AVAILABLE: i16 = 80;
pub const ELIGIBLE_LEADERS_NOT_AVAILABLE: i16 = 81;
pub const ELECTION_NOT_NEEDED: i16 = 84;
```

(`UNKNOWN_TOPIC_OR_PARTITION = 3`, `COORDINATOR_NOT_AVAILABLE = 15`, `CLUSTER_AUTHORIZATION_FAILED = 31`, `INVALID_REQUEST = 42` already exist from earlier slices.)

- [ ] **Step 2: Append `BrokerError` variants in `crates/broker/src/error.rs`**

Add two arms to the `BrokerError` enum (location: look for the existing `#[error("...")]` arms):

```rust
    #[error("invalid leader_imbalance_check_interval_secs = {value}: must be >= 1")]
    InvalidLeaderRebalanceInterval { value: u64 },

    #[error("invalid leader_imbalance_per_broker_percentage = {value}: must be <= 100")]
    InvalidLeaderRebalanceThreshold { value: u32 },
```

If a sibling `from_broker_error` in `codes.rs` does exhaustive matching, add map arms returning `UNKNOWN_SERVER_ERROR` for both.

- [ ] **Step 3: Verify build**

```bash
cargo build -p crabka-broker
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/codes.rs crates/broker/src/error.rs
git commit -m "feat(broker): wire codes + BrokerError variants for ElectLeaders

PREFERRED_LEADER_NOT_AVAILABLE (80), ELIGIBLE_LEADERS_NOT_AVAILABLE
(81), ELECTION_NOT_NEEDED (84) constants. Two new BrokerError variants
for rebalance config validation.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 2 — Wire handler

### Task 3: `BrokerConfig` rebalance fields + validation

**Files:**
- Modify: `crates/broker/src/config.rs`

- [ ] **Step 1: Add the three fields to `BrokerConfig`**

Locate the existing field block (after slice-13's `super_users` field — find via `rg "pub super_users" crates/broker/src/config.rs`). Append:

```rust
    /// KIP-460 auto preferred-replica election. When true, a background
    /// task on the controller leader periodically scans partitions and
    /// re-elects the preferred replica as leader when it's alive + in
    /// ISR. Matches Kafka's `auto.leader.rebalance.enable`.
    pub auto_leader_rebalance_enable: bool,

    /// How often the auto-rebalance ticker fires, in seconds. Default
    /// 300 (5 minutes). Matches Kafka's
    /// `leader.imbalance.check.interval.seconds`.
    pub leader_imbalance_check_interval_secs: u64,

    /// Minimum percentage of imbalanced partitions before the
    /// auto-rebalance ticker submits any changes. Default 10. Matches
    /// Kafka's `leader.imbalance.per.broker.percentage`.
    pub leader_imbalance_per_broker_percentage: u32,
```

- [ ] **Step 2: Update the `Default` impl**

Find the existing `impl Default for BrokerConfig` (or `Default`-style constructor) and append:

```rust
            auto_leader_rebalance_enable: true,
            leader_imbalance_check_interval_secs: 300,
            leader_imbalance_per_broker_percentage: 10,
```

- [ ] **Step 3: Update `for_tests`**

Find `pub fn for_tests(...)` and append:

```rust
            auto_leader_rebalance_enable: false,  // tests opt in explicitly
            leader_imbalance_check_interval_secs: 300,
            leader_imbalance_per_broker_percentage: 10,
```

- [ ] **Step 4: Extend `validate()`**

Find the existing `pub fn validate(&self) -> Result<(), BrokerError>` and append before the `Ok(())`:

```rust
        if self.leader_imbalance_check_interval_secs == 0 {
            return Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 });
        }
        if self.leader_imbalance_per_broker_percentage > 100 {
            return Err(BrokerError::InvalidLeaderRebalanceThreshold {
                value: self.leader_imbalance_per_broker_percentage,
            });
        }
```

- [ ] **Step 5: Add 4 unit tests**

Append to the existing `#[cfg(test)] mod tests` block in `config.rs`:

```rust
    #[test]
    fn auto_leader_rebalance_defaults_to_true_in_default() {
        let c = BrokerConfig::default();
        assert!(c.auto_leader_rebalance_enable);
        assert_eq!(c.leader_imbalance_check_interval_secs, 300);
        assert_eq!(c.leader_imbalance_per_broker_percentage, 10);
    }

    #[test]
    fn auto_leader_rebalance_defaults_to_false_in_for_tests() {
        let dir = std::path::PathBuf::from("/tmp/crabka-test");
        let c = BrokerConfig::for_tests(dir);
        assert!(!c.auto_leader_rebalance_enable);
    }

    #[test]
    fn rebalance_zero_interval_rejected_by_validate() {
        let mut c = BrokerConfig::default();
        c.leader_imbalance_check_interval_secs = 0;
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceInterval { value: 0 })
        ));
    }

    #[test]
    fn rebalance_threshold_over_100_rejected_by_validate() {
        let mut c = BrokerConfig::default();
        c.leader_imbalance_per_broker_percentage = 101;
        assert!(matches!(
            c.validate(),
            Err(BrokerError::InvalidLeaderRebalanceThreshold { value: 101 })
        ));
    }
```

(`BrokerConfig::for_tests` signature varies — check the existing slice-12 / slice-13 calls. If it takes `(node_id, log_dir)` or similar, adjust the test invocation accordingly.)

- [ ] **Step 6: Run tests + lints**

```bash
cargo test -p crabka-broker --lib config
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: 4 new tests pass; all existing config tests still pass; legacy `BrokerConfig::default()` validates clean.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/config.rs
git commit -m "feat(broker): BrokerConfig rebalance fields + validation

auto_leader_rebalance_enable, leader_imbalance_check_interval_secs,
leader_imbalance_per_broker_percentage. Production Default matches
Kafka (enable=true, interval=300s, threshold=10%); for_tests opts
out (enable=false) to keep slice 10b multi-broker tests stable.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 4: `elect_leaders.rs` wire handler

**Files:**
- Create: `crates/broker/src/handlers/elect_leaders.rs`

- [ ] **Step 1: Write the handler**

```rust
//! `ElectLeaders` (api_key 43, KIP-460).
//!
//! Operator-triggered leader election. PREFERRED type moves leadership
//! back to `replicas[0]` after operator intervention; UNCLEAN type
//! elects outside the ISR when every ISR member is dead.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny the
//! whole request returns `CLUSTER_AUTHORIZATION_FAILED (31)` on every
//! per-partition row.

#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use crabka_metadata::{MetadataRecord, PartitionRecord, ResourceType};
use crabka_protocol::Encode;
use crabka_protocol::owned::elect_leaders_request::{ElectLeadersRequest, TopicPartitions};
use crabka_protocol::owned::elect_leaders_response::{
    ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
};
use crabka_security::Principal;

use crate::authorizer::{authorize, AuthorizationRequest, AuthorizationResult};
use crate::broker::Broker;
use crate::codes;
use crate::leader_election::{select_new_leader_for_partition, ElectError, ElectionType};

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

pub(crate) async fn handle(
    broker: &Broker,
    req: ElectLeadersRequest,
    principal: &Principal,
    peer: &SocketAddr,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Authorize Cluster Alter — whole-request gate.
    let image = broker.controller.current_image();
    let allow = authorize(
        &image,
        &broker.config.super_users,
        &AuthorizationRequest {
            principal,
            host: peer,
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster",
            operation: crabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "elect-leaders denied",
            api_version,
        );
    }

    // Decode election_type discriminant.
    let election = match req.election_type {
        WIRE_ELECTION_PREFERRED => ElectionType::Preferred,
        WIRE_ELECTION_UNCLEAN => ElectionType::Unclean,
        _ => {
            return encode_whole_request_error(
                &req,
                codes::INVALID_REQUEST,
                "unknown election_type",
                api_version,
            );
        }
    };

    // Resolve target partition set:
    //   topic_partitions = None      → every partition in the image
    //   Some([{topic, []}])          → every partition of that topic
    //   Some([{topic, [p, q, ...]}]) → exact set
    let targets: Vec<(String, Vec<i32>)> = match &req.topic_partitions {
        None => image
            .topics()
            .map(|t| {
                (
                    t.name.clone(),
                    image
                        .partitions_of(&t.name)
                        .map(|p| p.partition)
                        .collect::<Vec<_>>(),
                )
            })
            .collect(),
        Some(list) => list
            .iter()
            .map(|tp| {
                let partitions = if tp.partitions.is_empty() {
                    image
                        .partitions_of(&tp.topic)
                        .map(|p| p.partition)
                        .collect()
                } else {
                    tp.partitions.clone()
                };
                (tp.topic.clone(), partitions)
            })
            .collect(),
    };

    // Run the algorithm per target; accumulate new records to submit
    // and per-partition results to ship back.
    let liveness = broker.liveness.clone();
    let mut by_topic: HashMap<String, Vec<PartitionResult>> = HashMap::new();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    for (topic, partitions) in &targets {
        let mut rows = Vec::with_capacity(partitions.len());
        for &p in partitions {
            let result =
                select_new_leader_for_partition(&image, &liveness, topic, p, election).await;
            match result {
                Ok(new_pr) => {
                    to_submit.push(MetadataRecord::V1Partition(new_pr));
                    rows.push(PartitionResult {
                        partition_id: p,
                        error_code: 0,
                        error_message: None,
                        ..Default::default()
                    });
                }
                Err(err) => {
                    let (code, msg) = elect_error_to_wire(err);
                    rows.push(PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    });
                }
            }
        }
        by_topic.insert(topic.clone(), rows);
    }

    // Submit accumulated records. On failure, mark every queued OK row
    // with COORDINATOR_NOT_AVAILABLE.
    if !to_submit.is_empty() {
        if let Err(e) = broker.controller.submit_change(to_submit).await {
            tracing::warn!(error = %e, "elect-leaders submit failed");
            for rows in by_topic.values_mut() {
                for r in rows.iter_mut() {
                    if r.error_code == 0 {
                        r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
                        r.error_message = Some(format!("submit failed: {e}"));
                    }
                }
            }
        }
    }

    // Build response.
    let replica_election_results: Vec<ReplicaElectionResult> = by_topic
        .into_iter()
        .map(|(topic, partition_result)| ReplicaElectionResult {
            topic,
            partition_result,
            ..Default::default()
        })
        .collect();

    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn elect_error_to_wire(err: ElectError) -> (i16, &'static str) {
    match err {
        ElectError::UnknownTopicOrPartition => {
            (codes::UNKNOWN_TOPIC_OR_PARTITION, "unknown topic or partition")
        }
        ElectError::PreferredAlreadyLeader => {
            (codes::ELECTION_NOT_NEEDED, "election not needed")
        }
        ElectError::PreferredNotInIsr => {
            (codes::PREFERRED_LEADER_NOT_AVAILABLE, "preferred replica not in ISR")
        }
        ElectError::PreferredNotAlive => {
            (codes::PREFERRED_LEADER_NOT_AVAILABLE, "preferred replica not alive")
        }
        ElectError::NoEligibleReplica => {
            (codes::ELIGIBLE_LEADERS_NOT_AVAILABLE, "no alive replica")
        }
        ElectError::NotControllerLeader => {
            (codes::COORDINATOR_NOT_AVAILABLE, "not controller leader")
        }
    }
}

fn encode_whole_request_error(
    req: &ElectLeadersRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Build a response where every requested (topic, partition) row
    // carries the whole-request error code. Top-level error_code = 0
    // since the per-row codes carry the failure (matches Kafka).
    let results: Vec<ReplicaElectionResult> = match &req.topic_partitions {
        None => vec![],
        Some(list) => list
            .iter()
            .map(|tp| ReplicaElectionResult {
                topic: tp.topic.clone(),
                partition_result: tp
                    .partitions
                    .iter()
                    .map(|&p| PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
    };
    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results: results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let mut body = Vec::new();
    resp.encode(&mut body, api_version)
        .map_err(|e| crate::error::BrokerError::Replication(format!("encode ElectLeaders: {e}")))?;
    Ok(Bytes::from(body))
}
```

- [ ] **Step 2: Verify the handler compiles**

The handler references types not yet wired into dispatch. Check via:

```bash
cargo build -p crabka-broker
```

May fail because `broker.liveness` field doesn't exist or is named differently — search via `rg "ControllerLivenessState" crates/broker/src/broker.rs` and adjust the access path. Also: confirm `broker.controller.submit_change` exists with the signature `async fn (records: Vec<MetadataRecord>) -> Result<...>` (slice 12b).

- [ ] **Step 3: Lints**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Fix any pedantic lints inline (likely `#[must_use]`, doc backticks, format args).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/handlers/elect_leaders.rs
git commit -m "feat(broker): ElectLeaders handler (api_key 43)

Cluster Alter gate; per-partition election via
select_new_leader_for_partition; batched submit_change for queued
PartitionRecords. Per-partition error codes per ElectError mapping
(80, 81, 84) plus 3/15/31/42 reused.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 5: Wire dispatch + api_versions registration

**Files:**
- Modify: `crates/broker/src/handlers/mod.rs`
- Modify: `crates/broker/src/handlers/api_versions.rs`
- Modify: `crates/broker/src/network/dispatch.rs`

- [ ] **Step 1: Register the module**

In `crates/broker/src/handlers/mod.rs`, add:

```rust
mod elect_leaders;
```

(Do NOT register it in any `HandlerTable` — like slice-13's ACL handlers, this one needs `&Principal` + `&SocketAddr` which the static table can't carry.)

- [ ] **Step 2: Add to `supported_apis`**

In `crates/broker/src/handlers/api_versions.rs`, find the `supported_apis` function or list, and append (following the existing `v!()` macro convention):

```rust
v!(elect_leaders_request),
```

(The `v!` macro reads `MIN_VERSION` and `MAX_VERSION` from the generated module.)

- [ ] **Step 3: Add to flexible-body table**

In `crates/broker/src/network/dispatch.rs::handler_body_flexible`, add:

```rust
        43 => version >= crabka_protocol::owned::elect_leaders_request::FLEXIBLE_MIN,
```

- [ ] **Step 4: Inline-intercept dispatch arm**

In `crates/broker/src/network/dispatch.rs`, find the per-connection request loop that already handles slice-13 api_keys 29/30/31 (search for `peek_api_key`). Add a sibling block:

```rust
        if peek_api_key(&frame) == Some(43) {
            handle_elect_leaders_frame(
                broker,
                frame,
                api_version,
                correlation_id,
                client_id,
                auth,
                peer,
            )
            .await?;
            continue;
        }
```

And the helper function (anywhere in the file, alongside the other `handle_*_frame` helpers):

```rust
async fn handle_elect_leaders_frame<S>(
    broker: &Arc<crate::broker::Broker>,
    frame: Bytes,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    auth: &crate::network::auth::ConnectionAuth,
    peer: &std::net::SocketAddr,
) -> Result<(), crate::error::BrokerError>
where
    S: AsyncWrite + Unpin,
{
    use crabka_protocol::Decode;
    use crabka_protocol::owned::elect_leaders_request::ElectLeadersRequest;
    let req = ElectLeadersRequest::decode(&mut frame.as_ref(), api_version)
        .map_err(|e| crate::error::BrokerError::Codec(e.to_string()))?;
    let principal = auth.principal();
    let response_bytes =
        crate::handlers::elect_leaders::handle(broker, req, principal, peer, api_version).await?;
    write_response(/* ... use existing pattern ... */).await?;
    Ok(())
}
```

(The exact signature + write_response invocation pattern matches existing slice-13 helpers. Find slice 13's `handle_create_acls_frame` and follow its shape exactly — including the `auth.principal()` accessor and the response framing.)

- [ ] **Step 5: Build + verify existing tests pass**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Existing tests should pass — no behavior change for non-ElectLeaders requests, and ElectLeaders calls from anything other than the slice-14 integration tests don't exist yet.

- [ ] **Step 6: Commit**

```bash
git add crates/broker/src/handlers/mod.rs crates/broker/src/handlers/api_versions.rs crates/broker/src/network/dispatch.rs
git commit -m "feat(broker): wire ElectLeaders dispatch + api_versions

api_key 43 registered in supported_apis + flexible-body table.
Inline-intercept dispatch arm matches slice-13 ACL handler pattern.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 3 — Auto-rebalance

### Task 6: `leader_rebalance.rs` background task + unit tests

**Files:**
- Create: `crates/broker/src/leader_rebalance.rs`
- Modify: `crates/broker/src/lib.rs`

- [ ] **Step 1: Write the module**

```rust
//! KIP-460 auto preferred-replica rebalance. A background task on the
//! controller leader periodically scans every partition; for each
//! where `select_new_leader_for_partition(Preferred)` succeeds, queues
//! a `V1Partition` update. Submits in one batch per tick when the
//! cluster-wide imbalance ratio crosses the configured threshold.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use crabka_metadata::{MetadataImage, MetadataRecord};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::heartbeat::controller_state::ControllerLivenessState;
use crate::leader_election::{select_new_leader_for_partition, ElectError, ElectionType};

/// Minimal trait for the controller surface we use. Lets tests inject
/// a mock without spinning up real raft.
#[async_trait]
pub(crate) trait ControllerLike: Send + Sync {
    fn is_leader(&self) -> bool;
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

#[derive(Debug, Clone)]
pub(crate) struct AutoRebalanceConfig {
    pub check_interval: Duration,
    pub imbalance_threshold_pct: u32,
}

/// Spawned task entry point.
pub(crate) async fn run(
    controller: Arc<dyn ControllerLike>,
    liveness: Arc<ControllerLivenessState>,
    cfg: AutoRebalanceConfig,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(cfg.check_interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {},
            _ = shutdown.cancelled() => {
                info!("auto-rebalance task shutting down");
                return;
            }
        }
        if !controller.is_leader() {
            debug!("auto-rebalance tick skipped: not controller leader");
            continue;
        }
        rebalance_tick(&*controller, &liveness, &cfg).await;
    }
}

pub(crate) async fn rebalance_tick(
    controller: &dyn ControllerLike,
    liveness: &ControllerLivenessState,
    cfg: &AutoRebalanceConfig,
) {
    let image = controller.current_image();
    let mut to_submit: Vec<MetadataRecord> = Vec::new();
    let mut total: u64 = 0;
    for topic in image.topics() {
        for p in image.partitions_of(&topic.name) {
            total += 1;
            match select_new_leader_for_partition(
                &image,
                liveness,
                &topic.name,
                p.partition,
                ElectionType::Preferred,
            )
            .await
            {
                Ok(new_pr) => to_submit.push(MetadataRecord::V1Partition(new_pr)),
                Err(ElectError::PreferredAlreadyLeader) => {}
                Err(_) => {} // already at the only-imbalanced-partition handling
            }
        }
    }
    let imbalanced = to_submit.len() as u64;
    if total == 0 {
        return;
    }
    let pct = (imbalanced * 100) / total;
    if pct < u64::from(cfg.imbalance_threshold_pct) {
        debug!(imbalanced, total, pct, "auto-rebalance: below threshold");
        return;
    }
    info!(count = imbalanced, "auto-rebalance: submitting elections");
    if let Err(e) = controller.submit_change(to_submit).await {
        warn!(error = %e, "auto-rebalance submit failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabka_metadata::{PartitionRecord, TopicRecord};
    use std::sync::Mutex;
    use uuid::Uuid;

    struct MockController {
        image: Arc<MetadataImage>,
        is_leader: bool,
        submitted: Mutex<Vec<MetadataRecord>>,
    }

    #[async_trait]
    impl ControllerLike for MockController {
        fn is_leader(&self) -> bool {
            self.is_leader
        }
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            self.submitted.lock().unwrap().extend(records);
            Ok(())
        }
    }

    fn img_with_n_partitions(imbalanced: usize, balanced: usize) -> Arc<MetadataImage> {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: (imbalanced + balanced) as i32,
            replication_factor: 3,
        }));
        let mut p = 0i32;
        // Imbalanced: leader = 2 (not preferred). ISR has all three.
        for _ in 0..imbalanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: 2,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
            }));
            p += 1;
        }
        // Balanced: leader = 1 (preferred).
        for _ in 0..balanced {
            img.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "foo".into(),
                partition: p,
                leader: 1,
                replicas: vec![1, 2, 3],
                isr: vec![1, 2, 3],
                leader_epoch: 5,
            }));
            p += 1;
        }
        Arc::new(img)
    }

    async fn liveness_all_alive() -> ControllerLivenessState {
        let l = ControllerLivenessState::new(Duration::from_secs(10));
        for n in [1, 2, 3] {
            l.record_heartbeat(n).await;
        }
        l
    }

    #[tokio::test]
    async fn below_threshold_skips_submit() {
        // 5 imbalanced out of 100 → 5%; threshold 10% → no submit.
        let mock = MockController {
            image: img_with_n_partitions(5, 95),
            is_leader: true,
            submitted: Mutex::new(Vec::new()),
        };
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_secs(300),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        assert!(mock.submitted.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn above_threshold_submits_imbalanced_set() {
        // 20 imbalanced out of 100 → 20%; threshold 10% → submit 20.
        let mock = MockController {
            image: img_with_n_partitions(20, 80),
            is_leader: true,
            submitted: Mutex::new(Vec::new()),
        };
        let liveness = liveness_all_alive().await;
        let cfg = AutoRebalanceConfig {
            check_interval: Duration::from_secs(300),
            imbalance_threshold_pct: 10,
        };
        rebalance_tick(&mock, &liveness, &cfg).await;
        let submitted = mock.submitted.lock().unwrap();
        assert_eq!(submitted.len(), 20);
        // Every submitted record must promote preferred (replicas[0] = 1).
        for record in submitted.iter() {
            match record {
                MetadataRecord::V1Partition(p) => assert_eq!(p.leader, 1),
                _ => panic!("unexpected record type"),
            }
        }
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`, add:

```rust
pub mod leader_rebalance;
```

- [ ] **Step 3: Build + run tests**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib leader_rebalance
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: 2 unit tests PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/leader_rebalance.rs crates/broker/src/lib.rs
git commit -m "feat(broker): leader_rebalance background ticker

ControllerLike trait + rebalance_tick + 2 unit tests covering
below-threshold no-op and above-threshold batched submit. Spawning
the task from Broker::start is task 7.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 7: Spawn auto-rebalance task from `Broker::start`

**Files:**
- Modify: `crates/broker/src/broker.rs`

- [ ] **Step 1: Add a `Controller`-side wrapper that implements `ControllerLike`**

Anywhere in `broker.rs`, add a small adapter:

```rust
struct ControllerAdapter {
    handle: Arc<crabka_raft::ControllerHandle>,
    node_id: crabka_raft::NodeId,
}

#[async_trait::async_trait]
impl crate::leader_rebalance::ControllerLike for ControllerAdapter {
    fn is_leader(&self) -> bool {
        self.handle.is_leader_for(self.node_id)
    }
    fn current_image(&self) -> std::sync::Arc<crabka_metadata::MetadataImage> {
        self.handle.current_image()
    }
    async fn submit_change(
        &self,
        records: Vec<crabka_metadata::MetadataRecord>,
    ) -> Result<(), String> {
        self.handle
            .submit_change(records)
            .await
            .map_err(|e| e.to_string())
    }
}
```

(`ControllerHandle::is_leader_for(node_id)` may not exist — search slice 12b's controller for a similar helper. If only `watch_leader()` exists, use `*controller.watch_leader().borrow() == Some(self.node_id)`.)

- [ ] **Step 2: Spawn the rebalance task**

In `Broker::start`, find the spot just after the controller is created (search for `let controller_cell.set(controller.clone())` from slice 12b T6). Add:

```rust
        // Spawn auto preferred-replica rebalance task. The task itself
        // checks `controller.is_leader()` per tick — safe to run on
        // every broker.
        let rebalance_handle = if config.auto_leader_rebalance_enable {
            let cfg = crate::leader_rebalance::AutoRebalanceConfig {
                check_interval: std::time::Duration::from_secs(
                    config.leader_imbalance_check_interval_secs,
                ),
                imbalance_threshold_pct: config.leader_imbalance_per_broker_percentage,
            };
            let adapter: Arc<dyn crate::leader_rebalance::ControllerLike> =
                Arc::new(ControllerAdapter {
                    handle: controller.clone(),
                    node_id: config.node_id,
                });
            let liveness_clone = liveness.clone();
            let shutdown_clone = shutdown.clone();
            Some(tokio::spawn(crate::leader_rebalance::run(
                adapter,
                liveness_clone,
                cfg,
                shutdown_clone,
            )))
        } else {
            None
        };
        let _ = rebalance_handle; // held by Broker if you want; or just let drop
```

(Whether to hold the JoinHandle on `Broker` for clean shutdown depends on existing patterns — slice 12b's heartbeat task uses `let _heartbeat_handle = tokio::spawn(...)` and relies on the shutdown CancellationToken. Match that pattern.)

- [ ] **Step 3: Build + verify**

```bash
cargo build -p crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
```

Expected: all existing tests pass. `BrokerConfig::for_tests` defaults to `auto_leader_rebalance_enable = false` (task 3), so no test sees the new background task firing.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/broker.rs
git commit -m "feat(broker): spawn auto-rebalance task in Broker::start

ControllerAdapter wraps ControllerHandle to satisfy the ControllerLike
trait. Task spawned only when auto_leader_rebalance_enable=true;
the task itself checks is_leader() per tick.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 4 — Broker integration tests

### Task 8: `preferred_election_via_wire_returns_success` + `unclean_election_via_wire_picks_alive_replica`

**Files:**
- Create: `crates/broker/tests/elect_leaders.rs`

The first two integration tests need 2-broker scaffolding. Reuse the pattern from `tests/acl_handlers.rs::multi_super_user_both_can_provision` (slice 13b T3) and slice 10b's leader-election tests.

- [ ] **Step 1: Write the test file scaffold**

```rust
//! Slice 14. Broker-side integration tests for the operator-triggered
//! `ElectLeaders` RPC. Drives the wire path end-to-end with a Rust
//! SASL/PLAIN client; verifies the resulting partition state via
//! `BrokerHandle` test accessors.

#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)] // clippy ICE on this file (slice 11 precedent)

use std::collections::HashSet;
use std::time::Duration;

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_security::SaslMechanism;

// Reuse SASL client helpers from the slice 13 acl_handlers integration
// test by copy. Rust integration tests can't share `mod common` across
// sibling test files; the helpers are stable and small.
include!("../tests/common/sasl_plain_client.rs");
```

Actually, slice 13's `tests/acl_handlers.rs` doesn't use a shared `common` module — it inlines its helpers. Follow the same pattern: paste the SASL/PLAIN driver helpers inline. The plan body sketches the test bodies; the implementer copies the `sasl_plain_authenticate` + `round_trip` helpers from `acl_handlers.rs` verbatim.

- [ ] **Step 2: Helpers (copied from `acl_handlers.rs`)**

Copy these helpers from `crates/broker/tests/acl_handlers.rs` (they live near the top of the file):

- `sasl_plain_authenticate(stream, username, password)` — drives SaslHandshake + SaslAuthenticate (PLAIN).
- `round_trip(stream, api_key, api_version, body, flexible)` — single-request length-prefixed exchange.
- `create_topic_as_admin(addr, topic_name, partitions)` — uses CreateTopics v7 to create a topic.

Plus new helpers specific to ElectLeaders:

```rust
async fn drive_elect_leaders_as_plain(
    addr: std::net::SocketAddr,
    user: &str,
    pass: &str,
    election_type: i8,
    topic_partitions: Option<Vec<(&str, Vec<i32>)>>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    use crabka_protocol::owned::elect_leaders_request::{ElectLeadersRequest, TopicPartitions};
    use crabka_protocol::owned::elect_leaders_response::ElectLeadersResponse;
    use crabka_protocol::{Decode, Encode};

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    sasl_plain_authenticate(&mut stream, user, pass).await;
    let req = ElectLeadersRequest {
        election_type,
        topic_partitions: topic_partitions.map(|list| {
            list.into_iter()
                .map(|(t, p)| TopicPartitions {
                    topic: t.to_string(),
                    partitions: p,
                    ..Default::default()
                })
                .collect()
        }),
        timeout_ms: 30_000,
        ..Default::default()
    };
    let mut body = Vec::new();
    req.encode(&mut body, 2).expect("encode");
    let response_bytes = round_trip(&mut stream, 43, 2, &body, true).await;
    let resp = ElectLeadersResponse::decode(&mut response_bytes.as_ref(), 2).expect("decode");
    resp.replica_election_results
        .into_iter()
        .map(|r| {
            (
                r.topic,
                r.partition_result
                    .into_iter()
                    .map(|p| (p.partition_id, p.error_code))
                    .collect(),
            )
        })
        .collect()
}
```

- [ ] **Step 3: Write the two tests**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn preferred_election_via_wire_returns_success() {
    // 2-broker SASL_PLAINTEXT cluster, super-user admin, rf=2 topic.
    // Kill broker 1 → broker 2 takes over. Revive broker 1. Send
    // ElectLeaders Preferred via wire. Assert error_code=0 and
    // partition.leader == 1 on both brokers.

    let (handle1, handle2, _d1, _d2, addr1) = start_two_broker_sasl_plaintext_cluster(
        /* super_user = */ "admin",
        /* admin_pass = */ "admin-secret",
    )
    .await;
    create_topic_as_admin(addr1, "foo", 1, /*rf=*/ 2).await;
    wait_partition_exists(&handle1, "foo", 0).await;
    wait_partition_exists(&handle2, "foo", 0).await;
    // Initial leader is whichever broker raft picks; we'll force it.
    // Kill broker 1 → broker 2 takes over via slice-10b's on_broker_dead.
    handle1.shutdown().await;
    wait_partition_leader(&handle2, "foo", 0, /*leader=*/ 2).await;
    // Revive broker 1 (uses Rejoin mode so it reads its existing raft log).
    // ... revive logic — see comments below ...
    let handle1 = restart_broker_for_test(addr1, "broker-1", &handle1_dir).await;
    wait_isr_contains(&handle2, "foo", 0, /*node=*/ 1).await;
    // Now request ElectLeaders Preferred.
    let resp = drive_elect_leaders_as_plain(
        addr1,
        "admin",
        "admin-secret",
        /*election_type=*/ 0, // PREFERRED
        Some(vec![("foo", vec![0])]),
    )
    .await;
    let foo = resp.iter().find(|(t, _)| t == "foo").expect("foo in resp");
    assert_eq!(foo.1, vec![(0, 0)], "expected error_code=0 for partition 0");
    wait_partition_leader(&handle2, "foo", 0, /*leader=*/ 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unclean_election_via_wire_picks_alive_replica() {
    // 2-broker cluster, rf=2. Kill broker 1, wait for ISR to shrink to
    // {2}. Kill broker 2, revive broker 1 (still out of ISR after revive).
    // Send ElectLeaders Unclean. Assert error_code=0, leader=1, isr=[1].

    let (h1, h2, _d1, _d2, addr) =
        start_two_broker_sasl_plaintext_cluster("admin", "admin-secret").await;
    create_topic_as_admin(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;
    h1.shutdown().await;
    wait_partition_isr_only(&h2, "foo", 0, &[2]).await;
    h2.shutdown().await;
    // Revive broker 1; it comes up in Rejoin mode from its existing raft log.
    let h1 = restart_broker_for_test(addr, "broker-1", &_d1).await;
    // ISR is still {2} on the image; broker 2 is dead. UNCLEAN should
    // pick broker 1 (the only alive replica, even though out of ISR).
    let resp = drive_elect_leaders_as_plain(
        addr,
        "admin",
        "admin-secret",
        /*election_type=*/ 1, // UNCLEAN
        Some(vec![("foo", vec![0])]),
    )
    .await;
    let foo = resp.iter().find(|(t, _)| t == "foo").expect("foo in resp");
    assert_eq!(foo.1, vec![(0, 0)], "expected error_code=0 for UNCLEAN election");
    wait_partition_leader(&h1, "foo", 0, /*leader=*/ 1).await;
    wait_partition_isr_only(&h1, "foo", 0, &[1]).await;
}
```

Helpers `start_two_broker_sasl_plaintext_cluster`, `wait_partition_exists`, `wait_partition_leader`, `wait_partition_isr_only`, `restart_broker_for_test` are integration-test glue. The first 4 mirror existing slice 10b patterns; check `crates/broker/tests/leader_election.rs` and `tests/auth_handlers.rs` for similar scaffolding to reuse or copy.

If slice 10b doesn't have a `restart_broker_for_test`, write one inline: re-runs `Broker::start` against the same `log_dir` and the same `node_id` with `bootstrap_mode = Rejoin`.

- [ ] **Step 4: Run tests via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test elect_leaders -- preferred_election_via_wire unclean_election_via_wire --nocapture --test-threads=1"
```

Expected: 2 PASS.

Common failure modes:
- Raft commit-then-apply gap: the wire response returns `0` before the new leader propagates to all brokers. Use polling (`wait_partition_leader`) with a 10s timeout.
- ISR shrink timing: slice 10b's `replica_lag_time_max_ms` defaults to 30s in production but tests use shorter values. Verify the test's broker config sets a short `replica_lag_time_max_ms` (~3s).

- [ ] **Step 5: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/elect_leaders.rs
git commit -m "test(broker): elect_leaders preferred + unclean wire tests

Two 2-broker tests exercising the ElectLeaders RPC end-to-end through
the Rust SASL/PLAIN client. PREFERRED moves leadership back to
replicas[0] after recovery; UNCLEAN picks an alive replica when every
ISR member is dead.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 9: `non_super_user_without_acl_denied` + `auto_rebalance_restores_preferred_leader`

**Files:**
- Modify: `crates/broker/tests/elect_leaders.rs`

- [ ] **Step 1: Append tests**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_without_acl_denied() {
    // Single-broker SASL_PLAINTEXT, super-user admin, alice has PLAIN
    // creds but no ACLs. One unrelated ACL exists to disable the
    // slice-13 compat shim. Auth as alice, send ElectLeaders; expect
    // per-partition error_code = CLUSTER_AUTHORIZATION_FAILED (31).

    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        /*super_user=*/ "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;
    // Seed one unrelated ACL via direct controller submit to disable
    // the compat shim.
    let unrelated = crabka_metadata::MetadataRecord::V1AccessControlEntry(
        crabka_metadata::AclEntry {
            resource_type: crabka_metadata::ResourceType::Topic,
            resource_name: "__compat_shim_disable__".into(),
            pattern_type: crabka_metadata::PatternType::Literal,
            principal: "User:admin".into(),
            host: "*".into(),
            operation: crabka_metadata::AclOperation::Read,
            permission_type: crabka_metadata::PermissionType::Allow,
        },
    );
    handle
        .submit_metadata_record_for_test(unrelated)
        .await
        .expect("seed ACL");

    // Need a topic to elect on (so the response isn't trivially empty).
    create_topic_as_admin(addr, "foo", 1, 1).await;

    let resp = drive_elect_leaders_as_plain(
        addr,
        "alice",
        "alice-secret",
        0, // PREFERRED
        Some(vec![("foo", vec![0])]),
    )
    .await;
    let foo = resp.iter().find(|(t, _)| t == "foo").expect("foo");
    assert_eq!(foo.1, vec![(0, 31)], "expected error_code=31 for unauth alice");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn auto_rebalance_restores_preferred_leader() {
    // 2-broker cluster with auto_leader_rebalance_enable=true,
    // check_interval=1s, threshold=0% (always trigger).
    // Kill broker 1 → broker 2 leads. Revive broker 1. Within ~3s of
    // its return, the background task elects broker 1 back as leader.

    let (h1, h2, d1, _d2, addr) =
        start_two_broker_cluster_with_auto_rebalance(/*interval_secs=*/ 1, /*threshold_pct=*/ 0)
            .await;
    create_topic_as_admin(addr, "foo", 1, 2).await;
    wait_partition_exists(&h1, "foo", 0).await;
    h1.shutdown().await;
    wait_partition_leader(&h2, "foo", 0, /*leader=*/ 2).await;
    let h1 = restart_broker_for_test(addr, "broker-1", &d1).await;
    wait_isr_contains(&h2, "foo", 0, /*node=*/ 1).await;

    // Auto-rebalance ticks every 1s. Within 10s, the preferred
    // replica should be leader again.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if h2.partition_leader_for_test("foo", 0).await == Some(1) {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "auto-rebalance didn't restore preferred leader within 10s; current leader = {:?}",
                h2.partition_leader_for_test("foo", 0).await
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
```

`start_two_broker_cluster_with_auto_rebalance` is a small variant of the existing two-broker helper that sets:

```rust
config.auto_leader_rebalance_enable = true;
config.leader_imbalance_check_interval_secs = interval_secs;
config.leader_imbalance_per_broker_percentage = threshold_pct;
```

`start_single_broker_sasl_plaintext_with_users` is from slice 13 (search via `rg "start_single_broker_sasl_plaintext_with_users\|start_sasl_plaintext_broker_with_super_user"`).

`partition_leader_for_test` is a `BrokerHandle` test accessor. If it doesn't exist, add one:

```rust
#[cfg(any(test, feature = "test-helpers"))]
impl BrokerHandle {
    pub async fn partition_leader_for_test(&self, topic: &str, partition: i32) -> Option<NodeId> {
        self.broker
            .controller
            .current_image()
            .partition(topic, partition)
            .map(|p| p.leader)
    }
}
```

(Search for `submit_metadata_record_for_test` to find the right `#[cfg]` gate to use for sibling helpers.)

- [ ] **Step 2: Run tests via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test elect_leaders -- non_super_user_without_acl_denied auto_rebalance_restores_preferred_leader --nocapture --test-threads=1"
```

Expected: 2 PASS. Full elect_leaders file: 4 tests PASS.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/elect_leaders.rs crates/broker/src/broker.rs
git commit -m "test(broker): elect_leaders auth gate + auto-rebalance tests

non-super-user without Cluster Alter ACL gets per-partition 31.
auto-rebalance ticker restores preferred leader after broker rejoin
within 10s on a 1-second check interval.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 5 — JVM acceptance

### Task 10: `jvm_kafka_leader_election_preferred`

**Files:**
- Modify: `crates/broker/tests/jvm_acceptance.rs`

- [ ] **Step 1: Append the test**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_kafka_leader_election_preferred() {
    // 2-broker SASL_PLAINTEXT cluster (cp-kafka:7.5 image for kafka-
    // leader-election support), super-user admin, rf=2 topic.
    // Kill broker 1 → broker 2 leads. Revive broker 1. Run
    // `kafka-leader-election --election-type preferred` via Docker.
    // Assert exit 0 and broker 1 is leader again.

    const ADMIN: &str = "admin";
    const ADMIN_PASS: &str = "admin-secret";
    const TOPIC: &str = "crabka-elect-preferred-itest";

    let (h1, h2, _d1, _d2, addr) =
        start_two_broker_sasl_plaintext_jvm_cluster(ADMIN, ADMIN_PASS).await;
    nc_check_connectivity();

    let admin_props = write_client_props(&format!(
        "security.protocol=SASL_PLAINTEXT\n\
         sasl.mechanism=PLAIN\n\
         sasl.jaas.config={}\n",
        plain_jaas(ADMIN, ADMIN_PASS),
    ));
    let admin_mount = admin_props.mount_str();

    // Create rf=2 topic as super-user.
    docker_run_kafka_tool_with_image_and_mount(
        KAFKA_IMAGE_TXN,
        &admin_mount,
        &[
            "kafka-topics",
            "--create",
            "--if-not-exists",
            "--topic",
            TOPIC,
            "--partitions",
            "1",
            "--replication-factor",
            "2",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ],
    );
    wait_partition_leader(&h1, TOPIC, 0, 1).await;

    // Kill broker 1 → broker 2 takes over.
    h1.shutdown().await;
    wait_partition_leader(&h2, TOPIC, 0, 2).await;

    // Revive broker 1; wait for ISR to include it again.
    let h1 = restart_broker_for_test(addr, "broker-1", &_d1).await;
    wait_isr_contains(&h2, TOPIC, 0, 1).await;

    // Trigger PREFERRED election via JVM tool.
    let out = std::process::Command::new("docker")
        .args([
            "run",
            "--rm",
            "-v",
            &admin_mount,
            "--add-host=host.docker.internal:host-gateway",
            KAFKA_IMAGE_TXN,
            "kafka-leader-election",
            "--election-type",
            "preferred",
            "--topic",
            TOPIC,
            "--partition",
            "0",
            "--bootstrap-server",
            BOOTSTRAP,
            "--command-config",
            "/client.properties",
        ])
        .output()
        .expect("spawn kafka-leader-election");
    assert!(
        out.status.success(),
        "kafka-leader-election failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    wait_partition_leader(&h2, TOPIC, 0, 1).await;
    h1.shutdown().await;
    h2.shutdown().await;
}
```

The helper `start_two_broker_sasl_plaintext_jvm_cluster` is parallel to slice 12b's `start_two_sasl_brokers` — search for it and replicate the shape.

- [ ] **Step 2: Run via WSL**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance jvm_kafka_leader_election_preferred -- --ignored --nocapture --test-threads=1"
```

Expected: PASS. May hit the documented WSL `host.docker.internal` /etc/hosts issue on some systems — if so, CI will validate.

- [ ] **Step 3: Lints + commit**

```bash
cargo fmt --check -p crabka-broker
cargo clippy -p crabka-broker --all-targets -- -D warnings
git add crates/broker/tests/jvm_acceptance.rs
git commit -m "test(jvm): kafka-leader-election --election-type preferred

Two-broker SASL_PLAINTEXT cluster; trigger PREFERRED election via
the JVM admin CLI after broker rejoin. Verifies the wire protocol
matches Kafka's expectations end-to-end.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Batch 6 — Final acceptance sweep

### Task 11: Sweep + docs + PR

**Files:**
- Modify: `README.md`
- Modify: `STATUS.md`

- [ ] **Step 1: Full local test matrix**

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace --exclude crabka-client-core --exclude crabka-log --exclude crabka-broker
cargo test -p crabka-broker --lib
cargo test -p crabka-broker --tests
```

All clean.

- [ ] **Step 2: WSL JVM acceptance (full)**

```bash
wsl bash -c "cd /mnt/c/Users/Matt\\ Stone/git/crabka && RUSTC_WRAPPER= cargo test -p crabka-broker --test jvm_acceptance -- --ignored --nocapture --test-threads=1"
```

All green. If `jvm_inter_broker_sasl_ssl_raft_replication` (slice 12b) hits the documented WSL networking issue, document and rely on CI.

- [ ] **Step 3: Update `README.md`**

Append under "Slices delivered":

```markdown
- **Slice 14** — leader-election controls: operator-triggered
  `ElectLeaders` RPC (api_key 43, KIP-460) with PREFERRED + UNCLEAN
  types. Auto preferred-replica rebalance background task driven by
  Kafka's `auto.leader.rebalance.enable` / `leader.imbalance.*`
  config knobs. JVM `kafka-leader-election.sh` works end-to-end.
  Slice 10b's automatic-on-broker-death election is unchanged; this
  slice adds the manual and scheduled trigger paths.
```

- [ ] **Step 4: Append `STATUS.md` section**

```markdown
## Slice 14 — ElectLeaders + auto-rebalance (2026-05-15)

- Pure-logic `select_new_leader_for_partition` in
  `crates/broker/src/leader_election.rs` computes the new
  `PartitionRecord` for one partition under PREFERRED or UNCLEAN.
  Returns a small `ElectError` enum mapped to wire codes 3/15/80/81/84.
- New `crates/broker/src/handlers/elect_leaders.rs` (api_key 43, KIP-460).
  Cluster Alter authorize gate; per-partition results in the response.
  Inline-intercept dispatch matches the slice-13 ACL handler pattern.
- New `crates/broker/src/leader_rebalance.rs`. Background ticker on
  the controller leader scans for imbalanced partitions every
  `leader_imbalance_check_interval_secs` (default 300s, matches Kafka);
  submits batched preferred-elections when imbalance crosses
  `leader_imbalance_per_broker_percentage` (default 10%).
- `BrokerConfig` gains `auto_leader_rebalance_enable` (default `true`
  in `Default`, `false` in `for_tests` so slice-10b multi-broker tests
  don't see surprise re-elections from the ticker),
  `leader_imbalance_check_interval_secs`, and
  `leader_imbalance_per_broker_percentage`. Two new `BrokerError`
  variants validate non-zero interval and ≤100% threshold at startup.
- 8 new authorizer-pure unit tests (PREFERRED + UNCLEAN matrix), 2
  rebalance-tick unit tests with mock controller, 4 broker integration
  tests (preferred + unclean wire paths, non-super-user denied,
  auto-rebalance restores preferred leader). 1 new JVM acceptance test
  drives `kafka-leader-election --election-type preferred` through
  cp-kafka:7.5.
- Out of scope: manual partition reassignment, quotas, log compaction,
  KIP-841 force-elect, operator preferred-replica override.
```

- [ ] **Step 5: Commit docs**

```bash
git add README.md STATUS.md
git commit -m "docs(slice-14): README + STATUS entry

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

- [ ] **Step 6: Push + open PR**

```bash
git push -u origin feature/elect-leaders-14
gh pr create --base main --head feature/elect-leaders-14 \
  --title "Slice 14: ElectLeaders + auto-rebalance" \
  --body "$(cat <<'EOF'
## Summary

Two operator-facing leader-election controls on top of slice 10b's automatic-on-broker-death election:

1. **Manual `ElectLeaders` RPC** (api_key 43, KIP-460). PREFERRED type moves leadership back to `replicas[0]` after operator intervention; UNCLEAN type elects outside the ISR when every ISR member is dead. JVM `kafka-leader-election.sh --election-type preferred|unclean` works end-to-end.

2. **Auto preferred-replica rebalance**. A controller-side background task scans every partition each `leader_imbalance_check_interval_secs` (default 300s), submits batched preferred-elections when the cluster-wide imbalance crosses `leader_imbalance_per_broker_percentage` (default 10%). Matches Kafka's `auto.leader.rebalance.enable` defaults.

## Verified

- 8 new authorizer-pure unit tests covering the PREFERRED + UNCLEAN matrix.
- 2 rebalance-tick unit tests via a `ControllerLike` trait mock (below-threshold no-op + above-threshold batched submit).
- 4 broker integration tests (`tests/elect_leaders.rs`): PREFERRED wire path, UNCLEAN wire path, non-super-user denied, auto-rebalance restores preferred leader within 10s on a 1-second tick.
- 1 new JVM acceptance test drives `kafka-leader-election --election-type preferred` against a 2-broker cluster.
- Workspace `cargo fmt --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` all green.
- Slice 10b automatic on-broker-dead election tests pass unchanged (`BrokerConfig::for_tests` defaults `auto_leader_rebalance_enable=false`).

## Out of scope

Manual partition reassignment, quotas, log compaction, KIP-841 force-elect, operator preferred-replica override.

## Plan / spec

- Spec: `docs/superpowers/specs/2026-05-15-crabka-elect-leaders-14-design.md`
- Plan: `docs/superpowers/plans/2026-05-15-crabka-elect-leaders-14.md` (11 tasks across 6 batches)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 7: Confirm CI passes**

Watch for clippy lints unique to Linux/macOS (slice 12b-style `doc_markdown`, `too_many_lines`, `duration_suboptimal_units`). Re-run `cargo clippy --workspace --all-targets -- -D warnings` locally before push if uncertain.

---

## Notes for the executing agent

1. **Branch:** all work on `feature/elect-leaders-14`. Do NOT push to main.
2. **`for_tests` defaults `auto_leader_rebalance_enable = false`** — this is load-bearing for slice-10b leader-election regression safety. The background ticker firing during a `kill_leader → wait → revive_leader` test would race against the test's explicit assertions. Tests that DO want the ticker (T9's `auto_rebalance_restores_preferred_leader`) set the flag explicitly via `start_two_broker_cluster_with_auto_rebalance`.
3. **`ControllerLike` trait is internal-only.** Used to make `rebalance_tick` testable without spinning up a real raft. The production `ControllerAdapter` impl wraps `Arc<ControllerHandle>`. Don't expose the trait outside `leader_rebalance`.
4. **UNCLEAN election shrinks ISR to `[new_leader]`.** This matches Kafka — old ISR members can't be trusted for the new leader's data after an unclean election. They rejoin via the normal replicator catch-up flow once back online.
5. **WSL `host.docker.internal` /etc/hosts** for JVM tests — same caveat as every slice 12+ JVM test. CI's workflow adds the entry.
6. **`unimplemented!()` placeholders** are not allowed in committed code. T8/T9 sketch the test bodies with comments — the implementer copies SASL/PLAIN helpers from `tests/acl_handlers.rs` verbatim.
7. **Slice 13 `Cluster Alter` ACL semantics**: super-user bypass keeps the ElectLeaders handler accessible to slice-12/13 super-user-configured test brokers without explicit ACL grants. Match the slice-13 `AlterUserScramCredentials` pattern in test setup.
