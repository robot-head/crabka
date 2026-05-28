# KIP-966 Offset-Aware Unclean Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace crabka's naive "first alive replica" unclean leader election with offset-aware recovery: the controller polls surviving replicas for their log-end-offset + last-written leader epoch via a new `GetReplicaLogInfo` RPC (api_key 70) and elects the most complete log, governed by an `unclean.recovery.strategy` topic config that supersedes the existing `unclean.leader.election.enable` flag.

**Architecture:** A controller-side Unclean Recovery Manager (URM) task runs on the raft-leader node. Both trigger paths — automatic failover (`on_broker_dead` when the ISR empties) and the operator `ElectLeaders` UNCLEAN admin path — enqueue recovery jobs to the URM. The URM dials each alive replica's broker over the existing `InterBrokerClient`, applies the strategy's wait rules, picks the winner with a pure selection function, and submits a new `PartitionRecord`. Recovery state is in-memory and re-derived on controller-leadership change (no schema change, no persisted RECOVERING marker).

**Tech Stack:** Rust, tokio, openraft (`ControllerHandle`), crabka protocol-codegen (Kafka JSON schemas → generated types), `prometheus_client` counters.

**Spec:** `docs/superpowers/specs/2026-05-28-crabka-unclean-recovery-kip966-design.md`

**Greenfield note (from CLAUDE.md):** crabka is undeployed. No back-compat shims, no `#[serde(default)]` for old logs, no migration code. Preserve Kafka wire-protocol byte-exactness for `GetReplicaLogInfo` v0.

---

## Implementation deviations from the spec (read first)

Two spec details change here because the codebase lacks the prerequisite state — these are intentional and noted inline in the relevant tasks:

1. **Broker-epoch fencing is dropped.** The spec proposed discarding a `GetReplicaLogInfo` response whose `BrokerEpoch` no longer matches the broker's registration epoch. crabka's `BrokerRegistrationRecord` (`crates/metadata/src/records.rs`) carries **no broker epoch**, and heartbeats send `broker_epoch: 0`. So fencing is `CurrentLeaderEpoch`-only: abort a recovery as stale if any responding replica reports a `CurrentLeaderEpoch` higher than the controller's known `leader_epoch` for the partition (a newer leader already exists). The response still carries `BrokerEpoch` for wire-exactness; we just don't compare it.

2. **Balanced waits for "all alive replicas," not ELR.** crabka has no Eligible-Leader-Replica tracking, so Balanced substitutes "all currently-alive members of the replica set" for KIP-966's "all `LastKnownELR` members."

---

## File structure

**New files:**
- `crates/protocol/schemas/GetReplicaLogInfoRequest.json` — Kafka v0 request schema.
- `crates/protocol/schemas/GetReplicaLogInfoResponse.json` — Kafka v0 response schema.
- `crates/protocol/src/owned/get_replica_log_info_request.rs` + `..._response.rs` — generated-include wrappers.
- `crates/protocol/src/borrowed/get_replica_log_info_request.rs` + `..._response.rs` — generated-include wrappers.
- `crates/broker/src/handlers/get_replica_log_info.rs` — broker-side handler (api_key 70).
- `crates/broker/src/unclean_recovery.rs` — pure selection helpers + the URM task and its handle.
- `crates/broker/tests/unclean_recovery.rs` — end-to-end integration test.

**Modified files:**
- `crates/broker/src/config_keys.rs` — `unclean.recovery.strategy` key, `RecoveryStrategy` enum + parse, `resolve_recovery_strategy`, validation, `is_recognized`.
- `crates/protocol/src/owned/mod.rs`, `crates/protocol/src/borrowed/mod.rs` — module declarations.
- `crates/broker/src/handlers/mod.rs` — register handler 70.
- `crates/broker/src/network/dispatch.rs` — flexible-body map entry for api_key 70.
- `crates/broker/src/handlers/api_versions.rs` — advertise api_key 70 for inter-broker negotiation.
- `crates/broker/src/leader_election.rs` — `compute_failover_changes` returns recovery requests; `on_broker_dead` enqueues them.
- `crates/broker/src/handlers/elect_leaders.rs` — route UNCLEAN through the URM when strategy ≠ None.
- `crates/broker/src/broker.rs` — construct + spawn the URM; store its handle on `Broker`.

---

## Batch plan (per CLAUDE.md parallel-batch execution)

- **Batch A (parallel — disjoint files):** Task 1 (schemas/codegen), Task 2 (config), Task 3 (pure selection).
- **Batch B (parallel — disjoint files, depend on A):** Task 4 (broker handler), Task 5 (URM task core).
- **Batch C (sequential — shared files, depends on B):** Task 6 (trigger wiring + spawn).
- **Batch D (depends on C):** Task 7 (integration test).

---

## Task 1: `GetReplicaLogInfo` wire types (api_key 70, v0)

**Files:**
- Create: `crates/protocol/schemas/GetReplicaLogInfoRequest.json`
- Create: `crates/protocol/schemas/GetReplicaLogInfoResponse.json`
- Create: `crates/protocol/src/owned/get_replica_log_info_request.rs`
- Create: `crates/protocol/src/owned/get_replica_log_info_response.rs`
- Create: `crates/protocol/src/borrowed/get_replica_log_info_request.rs`
- Create: `crates/protocol/src/borrowed/get_replica_log_info_response.rs`
- Modify: `crates/protocol/src/owned/mod.rs`, `crates/protocol/src/borrowed/mod.rs`
- Test: `crates/protocol/tests/` (round-trip) — see Step 6

- [ ] **Step 1: Write the request schema**

Create `crates/protocol/schemas/GetReplicaLogInfoRequest.json` (mirrors Apache Kafka's KIP-966 schema; `listeners` is metadata only — crabka routes via the handler table):

```json
{
  "apiKey": 70,
  "type": "request",
  "listeners": ["broker"],
  "name": "GetReplicaLogInfoRequest",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    { "name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
      "about": "The ID of the broker sending the request." },
    { "name": "TopicPartitions", "type": "[]TopicPartitions", "versions": "0+",
      "about": "The topic partitions to query.", "fields": [
      { "name": "TopicId", "type": "uuid", "versions": "0+", "about": "The unique topic ID." },
      { "name": "Partitions", "type": "[]int32", "versions": "0+",
        "about": "The partitions of this topic whose leader should be elected." }
    ]}
  ]
}
```

- [ ] **Step 2: Write the response schema**

Create `crates/protocol/schemas/GetReplicaLogInfoResponse.json`:

```json
{
  "apiKey": 70,
  "type": "response",
  "name": "GetReplicaLogInfoResponse",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    { "name": "BrokerEpoch", "type": "int64", "versions": "0+", "about": "The epoch of the broker." },
    { "name": "TopicPartitionLogInfoList", "type": "[]TopicPartitionLogInfo", "versions": "0+",
      "about": "The list of topic partition log info.", "fields": [
      { "name": "TopicId", "type": "uuid", "versions": "0+", "about": "The unique topic ID." },
      { "name": "PartitionLogInfo", "type": "[]PartitionLogInfo", "versions": "0+",
        "about": "The log info of a partition.", "fields": [
        { "name": "Partition", "type": "int32", "versions": "0+", "about": "The id of the partition." },
        { "name": "LastWrittenLeaderEpoch", "type": "int32", "versions": "0+",
          "about": "The last written leader epoch in the log." },
        { "name": "CurrentLeaderEpoch", "type": "int32", "versions": "0+",
          "about": "The current leader epoch for the partition from the broker point of view." },
        { "name": "LogEndOffset", "type": "int64", "versions": "0+",
          "about": "The log end offset for the partition." },
        { "name": "ErrorCode", "type": "int16", "versions": "0+", "about": "The result error, or zero if there was no error." },
        { "name": "ErrorMessage", "type": "string", "versions": "0+", "nullableVersions": "0+",
          "default": "null", "about": "The result message, or null if there was no error." }
      ]}
    ]}
  ]
}
```

> Note: Kafka's real response nests `PartitionLogInfo` under a per-topic `TopicId`. The spec described a flat `TopicPartitionLogInfoList`; this nested shape is the byte-exact Kafka schema and is what we generate. The handler/URM code in later tasks uses this nested shape.

- [ ] **Step 3: Run codegen**

Run: `bash tools/regenerate.sh`
Expected: creates `crates/protocol/generated/GetReplicaLogInfoRequest.owned.rs`, `.borrowed.rs`, and the matching `Response` files; updates the schema SHA that `crates/protocol/build.rs` validates. No errors.

- [ ] **Step 4: Add the include-wrapper source files**

Follow the existing convention (see `crates/protocol/src/owned/elect_leaders_request.rs:32-35`). Create `crates/protocol/src/owned/get_replica_log_info_request.rs`:

```rust
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/GetReplicaLogInfoRequest.owned.rs"
));
```

Create `crates/protocol/src/owned/get_replica_log_info_response.rs`:

```rust
include!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/generated/GetReplicaLogInfoResponse.owned.rs"
));
```

Create the two `crates/protocol/src/borrowed/get_replica_log_info_{request,response}.rs` files identically but pointing at `.borrowed.rs` includes.

- [ ] **Step 5: Declare the modules**

In `crates/protocol/src/owned/mod.rs` add (alphabetical with the other `pub mod` lines):

```rust
pub mod get_replica_log_info_request;
pub mod get_replica_log_info_response;
```

Add the same two lines to `crates/protocol/src/borrowed/mod.rs`.

- [ ] **Step 6: Write a round-trip test**

Add to the protocol crate's round-trip test module (follow the pattern used for other RPCs — search `crates/protocol/tests` or `crates/protocol/src` for an existing `encode`/`decode` round-trip test such as one referencing `ElectLeadersRequest`, and add a sibling):

```rust
#[test]
fn get_replica_log_info_round_trip_v0() {
    use crabka_protocol::owned::get_replica_log_info_request::{
        GetReplicaLogInfoRequest, TopicPartitions,
    };
    use crabka_protocol::{Decode, Encode};
    use uuid::Uuid;

    let req = GetReplicaLogInfoRequest {
        broker_id: 7,
        topic_partitions: vec![TopicPartitions {
            topic_id: Uuid::from_u128(0x1234),
            partitions: vec![0, 3, 5],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = Vec::new();
    req.encode(&mut buf, 0).expect("encode");
    let mut cur: &[u8] = &buf;
    let decoded = GetReplicaLogInfoRequest::decode(&mut cur, 0).expect("decode");
    assert_eq!(decoded.broker_id, 7);
    assert_eq!(decoded.topic_partitions.len(), 1);
    assert_eq!(decoded.topic_partitions[0].partitions, vec![0, 3, 5]);
}
```

> If the generated field names differ (e.g. `topicPartitions` vs `topic_partitions`), open `crates/protocol/generated/GetReplicaLogInfoRequest.owned.rs` and use the exact snake_case names the codegen emitted.

- [ ] **Step 7: Build + test**

Run: `cargo test -p crabka-protocol get_replica_log_info`
Expected: PASS. Also run `cargo build -p crabka-protocol` to confirm the generated code compiles.

- [ ] **Step 8: Commit**

```bash
git add crates/protocol/schemas/GetReplicaLogInfo*.json \
        crates/protocol/generated/GetReplicaLogInfo*.rs \
        crates/protocol/src/owned/get_replica_log_info_*.rs \
        crates/protocol/src/borrowed/get_replica_log_info_*.rs \
        crates/protocol/src/owned/mod.rs crates/protocol/src/borrowed/mod.rs \
        crates/protocol/tests
git commit -m "feat(protocol): GetReplicaLogInfo RPC types (api_key 70, KIP-966)"
```

---

## Task 2: `unclean.recovery.strategy` config

**Files:**
- Modify: `crates/broker/src/config_keys.rs`
- Test: `crates/broker/src/config_keys.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write failing tests**

Add to the `#[cfg(test)] mod tests` block in `config_keys.rs`:

```rust
#[test]
fn recovery_strategy_accepts_valid_values() {
    for v in ["None", "Balanced", "Aggressive"] {
        assert!(validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, v).is_ok(), "{v}");
    }
}

#[test]
fn recovery_strategy_rejects_garbage() {
    assert!(validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, "fast").is_err());
}

#[test]
fn recovery_strategy_recognized() {
    assert!(is_recognized(UNCLEAN_RECOVERY_STRATEGY));
}

#[test]
fn parse_recovery_strategy_maps_values() {
    assert_eq!(RecoveryStrategy::parse("None"), Some(RecoveryStrategy::None));
    assert_eq!(RecoveryStrategy::parse("Balanced"), Some(RecoveryStrategy::Balanced));
    assert_eq!(RecoveryStrategy::parse("Aggressive"), Some(RecoveryStrategy::Aggressive));
    assert_eq!(RecoveryStrategy::parse("bogus"), None);
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test -p crabka-broker config_keys::tests::recovery_strategy`
Expected: FAIL — `UNCLEAN_RECOVERY_STRATEGY` and `RecoveryStrategy` are undefined.

- [ ] **Step 3: Add the constant + enum + parse**

Near the other `pub(crate) const` keys in `config_keys.rs`:

```rust
/// KIP-966: per-topic unclean-recovery strategy. Supersedes
/// `unclean.leader.election.enable`: when set to `Balanced` or
/// `Aggressive` the controller runs offset-aware recovery (polls
/// surviving replicas for their log offsets and elects the most complete
/// log). `None` (the default) falls back to the legacy enable-flag
/// behavior. Consumed by `crate::unclean_recovery` and the failover /
/// ElectLeaders paths.
pub(crate) const UNCLEAN_RECOVERY_STRATEGY: &str = "unclean.recovery.strategy";

/// Resolved value of `unclean.recovery.strategy` for a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStrategy {
    /// No offset-aware recovery. Defer to `unclean.leader.election.enable`.
    None,
    /// Wait for all currently-alive replicas (ELR is not tracked in
    /// crabka), then elect the most complete log.
    Balanced,
    /// Elect the most complete log among the replicas that respond within
    /// a short deadline; optimize availability.
    Aggressive,
}

impl RecoveryStrategy {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "None" => Some(Self::None),
            "Balanced" => Some(Self::Balanced),
            "Aggressive" => Some(Self::Aggressive),
            _ => None,
        }
    }
}
```

- [ ] **Step 4: Add validation + recognition**

In `validate_topic_config`, add a match arm before the `unknown =>` arm:

```rust
        UNCLEAN_RECOVERY_STRATEGY => RecoveryStrategy::parse(value).map(|_| ()).ok_or_else(|| {
            format!("unclean.recovery.strategy={value} not supported; expected `None`, `Balanced`, or `Aggressive`")
        }),
```

In `is_recognized`, add `UNCLEAN_RECOVERY_STRATEGY` to the `matches!` list.

- [ ] **Step 5: Add the resolver**

Add to `config_keys.rs`:

```rust
/// Resolve `unclean.recovery.strategy` for `topic`, defaulting to
/// `RecoveryStrategy::None` when unset or unparseable. Per-topic only
/// for now (mirrors `unclean.leader.election.enable`); a cluster default
/// can layer in later via the same `topic_config` lookup precedence.
pub(crate) fn resolve_recovery_strategy(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
) -> RecoveryStrategy {
    image
        .topic_config(topic)
        .and_then(|m| m.get(UNCLEAN_RECOVERY_STRATEGY))
        .and_then(|v| RecoveryStrategy::parse(v))
        .unwrap_or(RecoveryStrategy::None)
}
```

Add a test for it:

```rust
#[test]
fn resolve_recovery_strategy_defaults_none_and_reads_override() {
    use crabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
    use std::collections::BTreeMap;
    use uuid::Uuid;
    let mut img = MetadataImage::new(Uuid::nil());
    assert_eq!(resolve_recovery_strategy(&img, "t"), RecoveryStrategy::None);
    let mut overrides = BTreeMap::new();
    overrides.insert(UNCLEAN_RECOVERY_STRATEGY.into(), "Balanced".into());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "t".into(),
        overrides,
    }));
    assert_eq!(resolve_recovery_strategy(&img, "t"), RecoveryStrategy::Balanced);
}
```

- [ ] **Step 6: Run tests to confirm pass**

Run: `cargo test -p crabka-broker config_keys::tests`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/config_keys.rs
git commit -m "feat(broker): unclean.recovery.strategy topic config (KIP-966)"
```

---

## Task 3: Pure replica-selection helpers

**Files:**
- Create: `crates/broker/src/unclean_recovery.rs`
- Modify: `crates/broker/src/lib.rs` (add `mod unclean_recovery;`)
- Test: `crates/broker/src/unclean_recovery.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Create the module with the input type + failing tests**

Create `crates/broker/src/unclean_recovery.rs`:

```rust
//! KIP-966 offset-aware unclean recovery: pure selection helpers + the
//! controller-side Unclean Recovery Manager (URM) task. The URM polls
//! surviving replicas for their log-end-offset and last-written leader
//! epoch (`GetReplicaLogInfo`, api_key 70) and elects the most complete
//! log. See docs/superpowers/specs/2026-05-28-crabka-unclean-recovery-kip966-design.md.

#![allow(dead_code)]

use crabka_raft::NodeId;

/// One replica's reported log state, gathered from a `GetReplicaLogInfo`
/// response. Decoupled from the generated wire type so the selection
/// logic is unit-testable without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Pick the replica with the most complete log: highest
/// `last_written_leader_epoch`, then highest `log_end_offset`, then
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    responses
        .iter()
        .max_by(|a, b| {
            a.last_written_leader_epoch
                .cmp(&b.last_written_leader_epoch)
                .then(a.log_end_offset.cmp(&b.log_end_offset))
                .then(b.broker_id.cmp(&a.broker_id)) // lower broker_id wins ties
        })
        .map(|r| r.broker_id)
}

/// True if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition,
/// meaning a newer leader already exists and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ri(broker_id: NodeId, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id,
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert_eq!(select_best_replica(&r), Some(3));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert_eq!(select_best_replica(&r), Some(1));
    }

    #[test]
    fn empty_input_returns_none() {
        assert_eq!(select_best_replica(&[]), None);
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo { broker_id: 2, last_written_leader_epoch: 5, log_end_offset: 10, current_leader_epoch: 7 }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}
```

- [ ] **Step 2: Register the module**

In `crates/broker/src/lib.rs`, add `mod unclean_recovery;` alongside the other `mod` declarations (e.g. near `mod leader_election;`).

- [ ] **Step 3: Run tests to confirm pass**

Run: `cargo test -p crabka-broker unclean_recovery::tests`
Expected: PASS (all 5 selection tests).

- [ ] **Step 4: Commit**

```bash
git add crates/broker/src/unclean_recovery.rs crates/broker/src/lib.rs
git commit -m "feat(broker): pure replica-selection helpers for unclean recovery"
```

---

## Task 4: `GetReplicaLogInfo` broker-side handler

**Files:**
- Create: `crates/broker/src/handlers/get_replica_log_info.rs`
- Modify: `crates/broker/src/handlers/mod.rs` (declare + register)
- Modify: `crates/broker/src/network/dispatch.rs` (flexible-body map)
- Modify: `crates/broker/src/handlers/api_versions.rs` (advertise key 70)
- Test: inline `#[cfg(test)]` in the handler file

**Depends on:** Task 1.

- [ ] **Step 1: Write the handler**

Create `crates/broker/src/handlers/get_replica_log_info.rs`. It answers, per requested partition the broker hosts locally, with the local LEO + leader epoch; partitions it does not host get `REPLICA_NOT_AVAILABLE`. The `BrokerId` in the request is the caller's id and is not needed to answer.

```rust
//! `GetReplicaLogInfo` (api_key 70, KIP-966). Inter-broker RPC: the
//! controller asks this broker for the log-end-offset and last-written
//! leader epoch of partitions it hosts, to drive offset-aware unclean
//! recovery. Served on the inter-broker listener via the handler table.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use crabka_protocol::owned::get_replica_log_info_request::GetReplicaLogInfoRequest;
use crabka_protocol::owned::get_replica_log_info_response::{
    GetReplicaLogInfoResponse, PartitionLogInfo, TopicPartitionLogInfo,
};
use crabka_protocol::{Decode, Encode};
use futures_util::future::BoxFuture;

use crate::broker::Broker;
use crate::codes;
use crate::error::BrokerError;

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    // Decode synchronously, then build the (cheap, local) response.
    let mut cur = req_bytes;
    let decoded = GetReplicaLogInfoRequest::decode(&mut cur, version);
    let image = broker.controller.current_image();
    // Snapshot the data we need before crossing the async boundary.
    let mut topic_results: Vec<TopicPartitionLogInfo> = Vec::new();
    if let Ok(req) = decoded {
        for tp in &req.topic_partitions {
            // Resolve TopicId -> name (crabka keys partitions by name).
            let topic_name = image
                .topics()
                .find(|t| t.topic_id == tp.topic_id)
                .map(|t| t.name.clone());
            let mut partition_log_info = Vec::with_capacity(tp.partitions.len());
            for &p in &tp.partitions {
                partition_log_info.push(match topic_name
                    .as_deref()
                    .and_then(|name| broker.partitions.get(&(name.to_string(), p)))
                {
                    Some(part) => PartitionLogInfo {
                        partition: p,
                        last_written_leader_epoch: part
                            .current_leader_epoch
                            .load(Ordering::Acquire),
                        current_leader_epoch: part.current_leader_epoch.load(Ordering::Acquire),
                        log_end_offset: part.log_end_offset(),
                        error_code: 0,
                        error_message: None,
                        ..Default::default()
                    },
                    None => PartitionLogInfo {
                        partition: p,
                        last_written_leader_epoch: -1,
                        current_leader_epoch: -1,
                        log_end_offset: -1,
                        error_code: codes::REPLICA_NOT_AVAILABLE,
                        error_message: Some("partition not hosted locally".into()),
                        ..Default::default()
                    },
                });
            }
            topic_results.push(TopicPartitionLogInfo {
                topic_id: tp.topic_id,
                partition_log_info,
                ..Default::default()
            });
        }
    }
    let resp = GetReplicaLogInfoResponse {
        broker_epoch: 0,
        topic_partition_log_info_list: topic_results,
        ..Default::default()
    };
    Box::pin(async move {
        let mut body = Vec::new();
        resp.encode(&mut body, version).map_err(|e| {
            BrokerError::Replication(format!("encode GetReplicaLogInfo: {e}"))
        })?;
        Ok(Bytes::from(body))
    })
}
```

> Confirm the generated field names (`topic_partitions`, `topic_partition_log_info_list`, `partition_log_info`, `last_written_leader_epoch`, etc.) against `crates/protocol/generated/GetReplicaLogInfo*.owned.rs` and `broker.partitions` access (`crates/broker/src/broker.rs:34`, a `DashMap<(String, i32), Arc<Partition>>`). `codes::REPLICA_NOT_AVAILABLE` — if absent in `crate::codes`, use the numeric Kafka code 9 and add the constant.

- [ ] **Step 2: Declare + register the handler**

In `crates/broker/src/handlers/mod.rs`:
- Add `pub(crate) mod get_replica_log_info;` with the other handler modules.
- In the function that builds the `HandlerTable` (where `t.register(63, broker_heartbeat::handle);` lives, ~line 224), add:

```rust
    t.register(70, get_replica_log_info::handle);
```

- [ ] **Step 3: Add the flexible-body map entry**

In `crates/broker/src/network/dispatch.rs`, in `handler_body_flexible()` (the match around lines 3652-3732), add alongside the api_key 63 arm:

```rust
        70 => version >= owned::get_replica_log_info_request::FLEXIBLE_MIN,
```

- [ ] **Step 4: Advertise api_key 70**

Open `crates/broker/src/handlers/api_versions.rs`. Find where supported api keys are advertised (a static list or a range derived from the handler table). Add an entry for api_key 70 with `min_version: 0, max_version: 0` so `InterBrokerClient` version negotiation succeeds. Match the exact struct/idiom already used for other keys in that file.

- [ ] **Step 5: Write a handler unit test**

Add to `get_replica_log_info.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // A non-hosted partition must come back REPLICA_NOT_AVAILABLE with
    // sentinel offsets, never a panic.
    #[tokio::test]
    async fn unknown_topic_partition_returns_not_available() {
        let broker = crate::broker::Broker::for_test().await; // see other handler tests for the constructor in use
        let req = GetReplicaLogInfoRequest {
            broker_id: 1,
            topic_partitions: vec![
                crabka_protocol::owned::get_replica_log_info_request::TopicPartitions {
                    topic_id: uuid::Uuid::from_u128(0xdead),
                    partitions: vec![0],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = Vec::new();
        req.encode(&mut buf, 0).unwrap();
        let bytes = handle(&broker, 0, 1, &buf).await.unwrap();
        let mut cur: &[u8] = &bytes;
        let resp = GetReplicaLogInfoResponse::decode(&mut cur, 0).unwrap();
        let pli = &resp.topic_partition_log_info_list[0].partition_log_info[0];
        assert_eq!(pli.error_code, codes::REPLICA_NOT_AVAILABLE);
        assert_eq!(pli.log_end_offset, -1);
    }
}
```

> Replace `Broker::for_test()` with whatever test constructor the existing handler unit tests use (grep `crates/broker/src/handlers/*.rs` for `async fn` test setup). If no in-process `Broker` test constructor exists, drop this unit test and rely on the Task 7 integration test instead — note that decision in the commit message.

- [ ] **Step 6: Build + test**

Run: `cargo test -p crabka-broker get_replica_log_info`
Expected: PASS. Run `cargo build -p crabka-broker`.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/handlers/get_replica_log_info.rs \
        crates/broker/src/handlers/mod.rs \
        crates/broker/src/network/dispatch.rs \
        crates/broker/src/handlers/api_versions.rs
git commit -m "feat(broker): GetReplicaLogInfo handler (api_key 70, KIP-966)"
```

---

## Task 5: Unclean Recovery Manager (URM) task

**Files:**
- Modify: `crates/broker/src/unclean_recovery.rs` (add job types, handle, task)
- Test: inline `#[cfg(test)]` in the same file

**Depends on:** Tasks 1, 2, 3.

- [ ] **Step 1: Add the job + handle + outcome types**

Append to `crates/broker/src/unclean_recovery.rs`:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crabka_metadata::{MetadataRecord, PartitionRecord};
use crabka_raft::ControllerHandle;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::warn;

use crate::config_keys::RecoveryStrategy;
use crate::heartbeat::controller_state::ControllerLivenessState;
use crate::network::client::InterBrokerClient;

/// Deadline for an Aggressive poll: pick the best responder seen within
/// this window (or the first response thereafter).
const AGGRESSIVE_DEADLINE: Duration = Duration::from_secs(2);
/// Hard cap for a Balanced poll: wait for all alive replicas, but never
/// longer than this.
const BALANCED_DEADLINE: Duration = Duration::from_secs(30);

/// One partition recovery request.
pub(crate) struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// `Some` for the operator (ElectLeaders) path that awaits the result;
    /// `None` for automatic failover (fire-and-forget).
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    Elected(NodeId),
    /// No alive replica responded; partition stays offline.
    NoEligibleReplica,
    /// Partition already had a live leader (or regained one) — nothing to do.
    NotNeeded,
    /// A newer leader epoch was observed; recovery abandoned as stale.
    Stale,
    /// Another recovery for this partition is already running.
    InProgress,
}

/// Cloneable handle used by the failover path and the ElectLeaders
/// handler to enqueue recovery jobs.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }
}
```

- [ ] **Step 2: Add the manager spawn + dispatch loop**

Append:

```rust
/// Owns the work channel and the in-flight dedup set. `spawn` returns a
/// cloneable handle and drives jobs on a background task. The task only
/// acts when this node is the raft leader.
pub(crate) struct UncleanRecoveryManager {
    controller: Arc<ControllerHandle>,
    liveness: Arc<ControllerLivenessState>,
    node_id: NodeId,
    inter_broker_client: InterBrokerClient,
    listener_protocol: crabka_security::ListenerProtocol,
    metrics: crate::metrics::BrokerMetrics,
    in_flight: Arc<Mutex<HashSet<(String, i32)>>>,
}

impl UncleanRecoveryManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        controller: Arc<ControllerHandle>,
        liveness: Arc<ControllerLivenessState>,
        node_id: NodeId,
        inter_broker_client: InterBrokerClient,
        listener_protocol: crabka_security::ListenerProtocol,
        metrics: crate::metrics::BrokerMetrics,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> UncleanRecoveryHandle {
        let (tx, mut rx) = mpsc::channel::<RecoveryJob>(256);
        let mgr = Arc::new(Self {
            controller,
            liveness,
            node_id,
            inter_broker_client,
            listener_protocol,
            metrics,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        });
        tokio::spawn(async move {
            loop {
                let job = tokio::select! {
                    () = shutdown.cancelled() => return,
                    j = rx.recv() => match j { Some(j) => j, None => return },
                };
                let mgr = mgr.clone();
                tokio::spawn(async move { mgr.recover_one(job).await; });
            }
        });
        UncleanRecoveryHandle { tx }
    }

    async fn recover_one(self: Arc<Self>, job: RecoveryJob) {
        let key = (job.topic.clone(), job.partition);
        // Dedup: only one recovery per partition at a time.
        {
            let mut set = self.in_flight.lock().await;
            if !set.insert(key.clone()) {
                if let Some(r) = job.reply { let _ = r.send(RecoveryOutcome::InProgress); }
                return;
            }
        }
        let outcome = self.run_recovery(&job).await;
        self.in_flight.lock().await.remove(&key);
        if let Some(r) = job.reply { let _ = r.send(outcome); }
    }
}
```

- [ ] **Step 3: Add the core recovery routine**

Append the `run_recovery` method (still in `impl UncleanRecoveryManager`):

```rust
impl UncleanRecoveryManager {
    async fn run_recovery(&self, job: &RecoveryJob) -> RecoveryOutcome {
        // Only the raft leader may submit changes.
        let is_leader = self
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == self.node_id);
        if !is_leader {
            return RecoveryOutcome::NotNeeded;
        }

        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        // If a live leader exists, nothing to recover.
        if self.liveness.is_alive(pr.leader).await {
            return RecoveryOutcome::NotNeeded;
        }
        let known_epoch = pr.leader_epoch;
        let topic_id = image.topic(&job.topic).map(|t| t.topic_id).unwrap_or_default();

        // Alive replicas to poll.
        let mut alive: Vec<NodeId> = Vec::new();
        for &r in &pr.replicas {
            if self.liveness.is_alive(r).await {
                alive.push(r);
            }
        }
        if alive.is_empty() {
            return RecoveryOutcome::NoEligibleReplica;
        }

        // Poll each alive replica concurrently for this single partition.
        let mut futs = Vec::with_capacity(alive.len());
        for r in alive {
            let Some(reg) = image.broker(r) else { continue };
            let (host, port) = (reg.host.clone(), reg.port);
            let client = self.inter_broker_client.clone();
            let proto = self.listener_protocol;
            let topic = job.topic.clone();
            let partition = job.partition;
            let my_id = i32::try_from(self.node_id).unwrap_or(-1);
            futs.push(async move {
                query_replica(&client, proto, &host, port, my_id, topic_id, &topic, partition, r)
                    .await
            });
        }

        let deadline = match job.strategy {
            RecoveryStrategy::Aggressive => AGGRESSIVE_DEADLINE,
            RecoveryStrategy::Balanced => BALANCED_DEADLINE,
            // None never reaches the URM (callers gate on strategy).
            RecoveryStrategy::None => AGGRESSIVE_DEADLINE,
        };

        // Gather responses up to the strategy deadline. Aggressive: take
        // whatever arrived by `deadline`, but if nothing arrived, wait for
        // the first. Balanced: wait for all, capped at `deadline`.
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, job.strategy, deadline).await;

        if has_newer_leader(&collected, known_epoch) {
            return RecoveryOutcome::Stale;
        }
        let Some(winner) = select_best_replica(&collected) else {
            return RecoveryOutcome::NoEligibleReplica;
        };

        // Re-check the image hasn't changed leadership while we polled.
        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        if self.liveness.is_alive(pr.leader).await {
            return RecoveryOutcome::NotNeeded;
        }

        let new_pr = PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: winner,
            replicas: pr.replicas.clone(),
            isr: vec![winner],
            leader_epoch: pr.leader_epoch + 1,
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
        };
        warn!(topic = %job.topic, partition = job.partition, leader = winner,
            "unclean recovery: elected most-complete-log replica (possible data loss)");
        if let Err(e) = self
            .controller
            .submit_change(vec![MetadataRecord::V1Partition(new_pr)])
            .await
        {
            warn!(error = %e, "unclean recovery submit_change failed");
            return RecoveryOutcome::NoEligibleReplica;
        }
        self.metrics.record_unclean_leader_election();
        RecoveryOutcome::Elected(winner)
    }
}
```

- [ ] **Step 4: Add the per-replica query + gather helpers**

Append free functions:

```rust
/// Dial one replica's broker and return its log info for `partition`, or
/// `None` on any transport/decoding error or per-partition error code.
#[allow(clippy::too_many_arguments)]
async fn query_replica(
    client: &InterBrokerClient,
    proto: crabka_security::ListenerProtocol,
    host: &str,
    port: u16,
    my_broker_id: i32,
    topic_id: uuid::Uuid,
    _topic: &str,
    partition: i32,
    replica: NodeId,
) -> Option<ReplicaLogInfo> {
    use crabka_protocol::owned::get_replica_log_info_request::{
        GetReplicaLogInfoRequest, TopicPartitions,
    };
    let opts = crabka_client_core::ConnectionOptions {
        client_id: "crabka-unclean-recovery".to_string(),
        ..crabka_client_core::ConnectionOptions::default()
    };
    let conn = client
        .connect_as_connection(host, port, proto, "localhost", opts)
        .await
        .ok()?;
    let req = GetReplicaLogInfoRequest {
        broker_id: my_broker_id,
        topic_partitions: vec![TopicPartitions {
            topic_id,
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = conn.send(req).await.ok()?;
    for t in &resp.topic_partition_log_info_list {
        for pli in &t.partition_log_info {
            if pli.partition == partition && pli.error_code == 0 {
                return Some(ReplicaLogInfo {
                    broker_id: replica,
                    last_written_leader_epoch: pli.last_written_leader_epoch,
                    log_end_offset: pli.log_end_offset,
                    current_leader_epoch: pli.current_leader_epoch,
                });
            }
        }
    }
    None
}

/// Collect query futures per the strategy's wait rules.
async fn gather_responses<F>(
    futs: Vec<F>,
    strategy: RecoveryStrategy,
    deadline: Duration,
) -> Vec<ReplicaLogInfo>
where
    F: std::future::Future<Output = Option<ReplicaLogInfo>> + Send + 'static,
{
    use futures_util::stream::{FuturesUnordered, StreamExt};
    let total = futs.len();
    let mut stream: FuturesUnordered<_> = futs.into_iter().collect();
    let mut out: Vec<ReplicaLogInfo> = Vec::with_capacity(total);

    let collect_all = async {
        while let Some(item) = stream.next().await {
            if let Some(info) = item {
                out.push(info);
            }
            if out.len() == total {
                break;
            }
        }
        out
    };

    match strategy {
        // Wait for all, capped at the deadline; take whatever we have.
        RecoveryStrategy::Balanced => {
            match tokio::time::timeout(deadline, collect_all).await {
                Ok(v) => v,
                Err(_) => std::mem::take(&mut Vec::new()), // unreachable: see note
            }
        }
        // Take whatever arrived by the deadline; if nothing, the first.
        RecoveryStrategy::Aggressive | RecoveryStrategy::None => {
            tokio::time::timeout(deadline, collect_all)
                .await
                .unwrap_or_default()
        }
    }
}
```

> The `Balanced` timeout branch above is wrong on purpose-of-illustration — fix it during implementation so the partial results survive a timeout. Use a shared `Vec` captured by the future (move the accumulation out of the timed future), e.g. collect into an `Arc<Mutex<Vec<_>>>` or restructure with `tokio::select!` on a `tokio::time::sleep(deadline)` while draining `stream`. The required behavior: on timeout, return the responses gathered so far (not empty). Implement with a TDD step (Step 5) that asserts partial-on-timeout.

- [ ] **Step 5: Write tests for the gather/wait semantics**

Add tests that don't require a network (drive `gather_responses` with ready/delayed futures):

```rust
#[cfg(test)]
mod urm_tests {
    use super::*;
    use std::time::Duration;

    fn info(id: NodeId, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo { broker_id: id, last_written_leader_epoch: 1, log_end_offset: leo, current_leader_epoch: 1 }
    }

    #[tokio::test]
    async fn balanced_waits_for_all_then_picks_best() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], RecoveryStrategy::Balanced,
            Duration::from_secs(5)).await;
        assert_eq!(got.len(), 2);
        assert_eq!(select_best_replica(&got), Some(2));
    }

    #[tokio::test]
    async fn balanced_returns_partial_on_timeout() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], RecoveryStrategy::Balanced,
            Duration::from_millis(50)).await;
        assert_eq!(got.len(), 1, "must return what arrived before the cap");
        assert_eq!(got[0].broker_id, 1);
    }

    #[tokio::test]
    async fn aggressive_takes_early_responders() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], RecoveryStrategy::Aggressive,
            Duration::from_millis(50)).await;
        assert_eq!(got, vec![info(1, 50)]);
    }
}
```

Add `use futures_util::FutureExt as _;` (for `.boxed()`) inside the test module.

- [ ] **Step 6: Run tests; fix the Balanced-partial-on-timeout impl until green**

Run: `cargo test -p crabka-broker unclean_recovery`
Expected: all selection + URM tests PASS. Iterate on `gather_responses` until `balanced_returns_partial_on_timeout` passes.

- [ ] **Step 7: Commit**

```bash
git add crates/broker/src/unclean_recovery.rs
git commit -m "feat(broker): unclean recovery manager task (KIP-966)"
```

---

## Task 6: Wire both triggers + spawn the URM

**Files:**
- Modify: `crates/broker/src/leader_election.rs` (failover return type + enqueue)
- Modify: `crates/broker/src/handlers/elect_leaders.rs` (operator path)
- Modify: `crates/broker/src/broker.rs` (spawn URM, store handle)

**Depends on:** Task 5. Sequential (shared files / signatures).

- [ ] **Step 1: Change `compute_failover_changes` to surface recovery requests**

In `crates/broker/src/leader_election.rs`, add a return struct and update the function. The unclean branch now splits on strategy: `None` keeps the legacy naive pick (gated by `unclean_election_enabled`); `Balanced`/`Aggressive` emit a recovery request instead of a change.

Add near the top:

```rust
use crate::config_keys::{resolve_recovery_strategy, RecoveryStrategy};

/// Output of a failover scan: immediate metadata changes plus partitions
/// that need asynchronous offset-aware recovery via the URM.
pub(crate) struct FailoverPlan {
    pub changes: Vec<MetadataRecord>,
    pub recoveries: Vec<(String, i32, RecoveryStrategy)>,
}
```

Change `compute_failover_changes` to return `FailoverPlan`. In the `needs_election` + `alive_isr.first() == None` branch, replace the body with:

```rust
                } else {
                    match resolve_recovery_strategy(image, &pr.topic) {
                        RecoveryStrategy::Balanced | RecoveryStrategy::Aggressive => {
                            // Offset-aware recovery runs asynchronously.
                            recoveries.push((
                                pr.topic.clone(),
                                pr.partition,
                                resolve_recovery_strategy(image, &pr.topic),
                            ));
                        }
                        RecoveryStrategy::None if unclean_election_enabled(image, &pr.topic) => {
                            // Legacy KIP-841 naive pick (unchanged).
                            let mut elected: Option<NodeId> = None;
                            for &n in &pr.replicas {
                                if n != dead && liveness.is_alive(n).await {
                                    elected = Some(n);
                                    break;
                                }
                            }
                            if let Some(new_leader) = elected {
                                warn!(topic = %pr.topic, partition = pr.partition, leader = new_leader,
                                    "unclean leader election: ISR empty, electing out-of-ISR replica (possible data loss)");
                                metrics.record_unclean_leader_election();
                                changes.push(MetadataRecord::V1Partition(PartitionRecord {
                                    topic: pr.topic.clone(),
                                    partition: pr.partition,
                                    leader: new_leader,
                                    replicas: pr.replicas.clone(),
                                    isr: vec![new_leader],
                                    leader_epoch: pr.leader_epoch + 1,
                                    adding_replicas: pr.adding_replicas.clone(),
                                    removing_replicas: pr.removing_replicas.clone(),
                                }));
                            } else {
                                warn!(topic = %pr.topic, partition = pr.partition,
                                    "unclean enabled but no alive replica; partition unavailable");
                            }
                        }
                        RecoveryStrategy::None => {
                            warn!(topic = %pr.topic, partition = pr.partition,
                                "no live ISR replica; partition unavailable (strategy None, unclean.leader.election.enable=false)");
                        }
                    }
                }
```

Declare `let mut recoveries: Vec<(String, i32, RecoveryStrategy)> = Vec::new();` next to `changes`, and return `FailoverPlan { changes, recoveries }`.

- [ ] **Step 2: Update `compute_failover_changes` unit tests for the new return type**

The existing tests call `one_partition_change(&changes)` on the returned `Vec`. Update them to `compute_failover_changes(...).await.changes` and add two tests:

```rust
#[tokio::test]
async fn failover_balanced_strategy_requests_recovery_not_immediate_change() {
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", crate::config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced");
    let l = ControllerLivenessState::new(Duration::from_secs(10));
    for n in [2u64, 3] { l.record_heartbeat(n).await; }
    let plan = compute_failover_changes(&img, /*dead=*/ 1, &l, &crate::metrics::BrokerMetrics::new()).await;
    assert!(plan.changes.is_empty(), "offset-aware recovery defers the change");
    assert_eq!(plan.recoveries, vec![("t".to_string(), 0, RecoveryStrategy::Balanced)]);
}

#[tokio::test]
async fn failover_strategy_none_still_uses_legacy_enable_flag() {
    // Strategy None + enable=true => immediate naive pick (legacy KIP-841).
    let mut img = img_with_partition("t", 0, /*leader*/ 1, &[1, 2, 3], &[1]);
    set_topic_config(&mut img, "t", UNCLEAN_LEADER_ELECTION_ENABLE, "true");
    let l = ControllerLivenessState::new(Duration::from_secs(10));
    for n in [2u64, 3] { l.record_heartbeat(n).await; }
    let metrics = crate::metrics::BrokerMetrics::new();
    let plan = compute_failover_changes(&img, /*dead=*/ 1, &l, &metrics).await;
    let pr = one_partition_change(&plan.changes);
    assert_eq!(pr.leader, 2);
    assert!(plan.recoveries.is_empty());
}
```

Update `one_partition_change` callers in the other tests to pass `&plan.changes`. The clean-failover and ISR-shrink tests (`failover_picks_alive_isr_member_when_available`, `failover_shrinks_isr_for_partitions_where_dead_is_non_leader`, etc.) assert on `.changes` and `.recoveries.is_empty()`.

- [ ] **Step 3: Thread the URM handle into `on_broker_dead`**

Change the signature:

```rust
pub(crate) async fn on_broker_dead(
    controller: &Arc<ControllerHandle>,
    node_id: NodeId,
    dead: NodeId,
    liveness: &Arc<ControllerLivenessState>,
    metrics: &crate::metrics::BrokerMetrics,
    recovery: &crate::unclean_recovery::UncleanRecoveryHandle,
) -> Result<(), BrokerError> {
    let is_controller_leader = controller.watch_leader().borrow().is_some_and(|n| n == node_id);
    if !is_controller_leader {
        return Ok(());
    }
    let image = controller.current_image();
    let plan = compute_failover_changes(&image, dead, liveness, metrics).await;
    if !plan.changes.is_empty() {
        controller
            .submit_change(plan.changes)
            .await
            .map_err(|e| BrokerError::Replication(format!("submit_change: {e}")))?;
    }
    for (topic, partition, strategy) in plan.recoveries {
        recovery
            .enqueue(crate::unclean_recovery::RecoveryJob { topic, partition, strategy, reply: None })
            .await;
    }
    Ok(())
}
```

- [ ] **Step 4: Update the ticker spawn site to pass the URM handle**

In `crates/broker/src/broker.rs` (~lines 1151-1201), the ticker closure calls `on_broker_dead(...)`. Clone the URM handle (created in Step 6) into the ticker closure and pass it as the new argument. (Order the URM construction before the ticker spawn.)

- [ ] **Step 5: Route the operator ElectLeaders UNCLEAN path through the URM**

In `crates/broker/src/handlers/elect_leaders.rs`, when `election == ElectionType::Unclean`, resolve the strategy per topic and, for `Balanced`/`Aggressive`, enqueue a job and await the outcome under a deadline instead of calling `select_new_leader_for_partition`. `Preferred` and `None`-strategy UNCLEAN keep the existing synchronous path.

Replace the per-partition loop body for the UNCLEAN+strategy case:

```rust
use crate::config_keys::{resolve_recovery_strategy, RecoveryStrategy};
use crate::unclean_recovery::{RecoveryJob, RecoveryOutcome};
use tokio::sync::oneshot;

const OPERATOR_RECOVERY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(25);

// inside the `for &p in partitions` loop:
let use_offset_aware = matches!(election, ElectionType::Unclean)
    && !matches!(resolve_recovery_strategy(&image, topic), RecoveryStrategy::None);

if use_offset_aware {
    let strategy = resolve_recovery_strategy(&image, topic);
    let (tx, rx) = oneshot::channel();
    broker
        .unclean_recovery
        .enqueue(RecoveryJob { topic: topic.clone(), partition: p, strategy, reply: Some(tx) })
        .await;
    let row = match tokio::time::timeout(OPERATOR_RECOVERY_DEADLINE, rx).await {
        Ok(Ok(RecoveryOutcome::Elected(_))) => PartitionResult {
            partition_id: p, error_code: 0, error_message: None, ..Default::default()
        },
        Ok(Ok(RecoveryOutcome::NoEligibleReplica)) => PartitionResult {
            partition_id: p, error_code: codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
            error_message: Some("no eligible replica responded".into()), ..Default::default()
        },
        Ok(Ok(RecoveryOutcome::NotNeeded)) => PartitionResult {
            partition_id: p, error_code: codes::ELECTION_NOT_NEEDED,
            error_message: Some("partition already has a leader".into()), ..Default::default()
        },
        Ok(Ok(RecoveryOutcome::Stale | RecoveryOutcome::InProgress)) | Ok(Err(_)) | Err(_) => {
            PartitionResult {
                partition_id: p, error_code: codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                error_message: Some("unclean recovery in progress".into()), ..Default::default()
            }
        }
    };
    rows.push(row);
    continue;
}
// else: existing synchronous select_new_leader_for_partition path (unchanged)
```

Note: the URM owns the `submit_change` for recovered partitions, so do **not** push these into `to_submit`. The existing `to_submit` batch only carries `Preferred`/`None`-strategy results.

- [ ] **Step 6: Construct + spawn the URM in `broker.rs` and store the handle**

- Add a field to the `Broker` struct (`crates/broker/src/broker.rs`): `pub(crate) unclean_recovery: crate::unclean_recovery::UncleanRecoveryHandle,`.
- Before the ticker spawn, build the handle. Reuse the same `InterBrokerClient` and inter-broker listener protocol the heartbeat client uses (see `crates/broker/src/heartbeat/client.rs` config and how `inter_broker_client` / `inter_broker_listener_protocol` are obtained at this point in `broker.rs`):

```rust
let unclean_recovery = crate::unclean_recovery::UncleanRecoveryManager::spawn(
    controller.clone(),
    liveness.clone(),
    config.node_id,
    inter_broker_client.clone(),
    inter_broker_listener_protocol,
    metrics.clone(),
    supervisor_shutdown.child_token(),
);
```

- Pass `unclean_recovery.clone()` into the ticker closure (Step 4) and store `unclean_recovery` in the `Broker { ... }` constructor.

> Grep `broker.rs` for the exact local variable names for the inter-broker client and listener protocol in scope at this spawn point; the heartbeat-client setup nearby uses them. If the client isn't already in scope here, construct one the same way the heartbeat path does.

- [ ] **Step 7: Build + run the affected unit tests**

Run: `cargo test -p crabka-broker leader_election`
Run: `cargo build -p crabka-broker`
Expected: PASS / clean build. Fix any callers of `on_broker_dead` / `compute_failover_changes` the signature changes touched.

- [ ] **Step 8: Commit**

```bash
git add crates/broker/src/leader_election.rs \
        crates/broker/src/handlers/elect_leaders.rs \
        crates/broker/src/broker.rs
git commit -m "feat(broker): route failover + ElectLeaders UNCLEAN through unclean recovery manager"
```

---

## Task 7: End-to-end integration test

**Files:**
- Create: `crates/broker/tests/unclean_recovery.rs`

**Depends on:** Task 6. Model the harness on the existing `crates/broker/tests/elect_leaders.rs` (3-broker PLAINTEXT cluster, minimal TCP wire helpers, `BrokerHandle` test accessors).

- [ ] **Step 1: Write the integration test**

The scenario: 3-broker cluster; a topic with one partition, RF=3, `unclean.recovery.strategy=Aggressive`. Produce records so replicas diverge in LEO. Kill the leader and the rest of the ISR so the partition goes offline. Drive an `ElectLeaders` UNCLEAN request (or wait for automatic failover). Assert the controller elects the surviving replica with the **highest** LEO, not just the first alive one.

```rust
//! KIP-966 offset-aware unclean recovery, end to end. Mirrors the harness
//! in elect_leaders.rs.

mod common; // if elect_leaders.rs uses a shared helper module; otherwise inline the wire helpers

#[tokio::test(flavor = "multi_thread")]
async fn unclean_recovery_elects_longest_log_replica() {
    // 1. Start a 3-broker cluster (copy setup from tests/elect_leaders.rs).
    // 2. Create topic "t" partition 0, replicas [1,2,3], with
    //    unclean.recovery.strategy=Aggressive (via CreateTopics configs or
    //    IncrementalAlterConfigs).
    // 3. Produce enough records that broker 3's log is longer than broker 2's
    //    (e.g. briefly partition/stop broker 2's fetcher, produce more, so 3
    //    has a higher LEO). Record each survivor's LEO.
    // 4. Take down the leader (broker 1) and broker 2 so only broker 3 (the
    //    longest log) and possibly a shorter replica remain.
    // 5. Send ElectLeaders(UNCLEAN) for (t,0) OR wait past the liveness
    //    timeout for automatic failover.
    // 6. Fetch metadata; assert the new leader is the longest-log survivor.

    // Concrete wire/cluster calls: reuse helpers from tests/elect_leaders.rs.
    todo!("flesh out using the elect_leaders.rs harness; assert leader == longest-log survivor");
}
```

- [ ] **Step 2: Replace the scaffold with real harness calls**

Open `crates/broker/tests/elect_leaders.rs`, copy its cluster bootstrap, topic-creation, produce, and `ElectLeaders` send/parse helpers, and implement the steps above concretely. The single load-bearing assertion: the elected leader equals the broker with the highest produced LEO among survivors. Remove the `todo!`.

- [ ] **Step 3: Run the test**

Run: `cargo test -p crabka-broker --test unclean_recovery -- --nocapture`
Expected: PASS — elected leader is the longest-log survivor.

- [ ] **Step 4: Commit**

```bash
git add crates/broker/tests/unclean_recovery.rs
git commit -m "test(broker): end-to-end unclean recovery elects longest-log replica"
```

---

## Final verification (before declaring done — superpowers:verification-before-completion)

- [ ] `cargo fmt --all` (CI gates on `cargo fmt --check`; clippy passing is not enough — see memory `feedback-rustfmt-before-push`).
- [ ] `cargo clippy --all-targets -- -D warnings` clean.
- [ ] `cargo test -p crabka-protocol && cargo test -p crabka-broker` green.
- [ ] Manually confirm `git log` shows the per-task commits and the diff matches the spec.
- [ ] Commit any formatting changes.

---

## Self-review notes (author)

- **Spec coverage:** §1 config → Task 2; §2 wire RPC → Tasks 1 + 4; §3 URM → Tasks 5 + 6; §4 selection + wait rules → Tasks 3 + 5; §5 error handling → Tasks 5 + 6; §6 testing → unit tests across Tasks 2/3/4/5, integration in Task 7. Covered.
- **Two deviations** (broker-epoch fencing dropped; Balanced = all-alive not ELR) are called out at the top and re-noted at the relevant tasks; they match crabka's actual state model and the spec's stated ELR non-goal.
- **Known soft spots flagged inline for the implementer:** generated field-name verification (Tasks 1/4), the `Broker` test constructor for the handler unit test (Task 4), the `gather_responses` Balanced-partial-on-timeout implementation (Task 5, with a TDD test that forces it correct), `codes::REPLICA_NOT_AVAILABLE` existence (Task 4), and the exact in-scope inter-broker-client variable in `broker.rs` (Task 6). Each names a concrete file/symbol to check rather than leaving a blank.
