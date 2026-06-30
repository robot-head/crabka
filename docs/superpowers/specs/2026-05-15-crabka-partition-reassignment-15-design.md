# Slice 15: Partition reassignment — Design

**Status:** Approved 2026-05-15.

**Goal:** Implement KIP-455 `AlterPartitionReassignments` (api_key 45) and `ListPartitionReassignments` (api_key 46) with a two-phase URP-aware state machine, cancellation, and leader handoff. JVM `kafka-reassign-partitions.sh --execute|--verify` works end-to-end. Builds on slice 14's metadata image, ControllerHandle, and authorization plumbing.

**Out of scope (deferred to slice 15b):**
- KIP-73 throttled replication (`leader.replication.throttled.replicas`, `follower.replication.throttled.replicas` configs + byte-rate enforcement on inter-broker Fetch)
- Log-dir reassignment (KIP-113 `--replica-alteration`)

---

## 1. Scope

### In

- `AlterPartitionReassignments` (api_key 45, v0–1) — including:
  - Start new reassignment (`replicas: Some(target)`)
  - Cancel in-flight reassignment (`replicas: None`)
  - `allow_replication_factor_change` v1 flag honored
- `ListPartitionReassignments` (api_key 46, v0):
  - Filter by `topics: Option<Vec<{name, partition_indexes: Option<Vec<i32>>}>>`
  - Returns per-partition `replicas`, `adding_replicas`, `removing_replicas`
- Two-phase URP-aware state machine: `PartitionRecord` gains `adding_replicas` + `removing_replicas` fields
- Background reassignment-completion task on the controller leader
- Leader handoff: if current leader is in `removing_replicas`, elect a new leader from the target set before completion
- Authorization: Cluster Alter for `AlterPartitionReassignments`; Cluster Describe for `ListPartitionReassignments`
- JVM acceptance: `kafka-reassign-partitions --execute` + `--verify`

### Not in (slice 15b)

- KIP-73 throttled replication
- KIP-113 log-dir reassignment
- Auto-balancer integration (slice 14's preferred-leader ticker stays independent)

### Wire types — confirmed shapes

`AlterPartitionReassignmentsRequest` (v0–1, flex from v0):
- `timeout_ms: i32`
- `allow_replication_factor_change: bool` (v1+)
- `topics: Vec<ReassignableTopic { name: String, partitions: Vec<ReassignablePartition { partition_index: i32, replicas: Option<Vec<i32>> }> }>`

`AlterPartitionReassignmentsResponse`:
- Top-level `throttle_time_ms`, `error_code`, `error_message`
- `responses: Vec<ReassignableTopicResponse { name, partitions: Vec<ReassignablePartitionResponse { partition_index, error_code, error_message }> }>`

`ListPartitionReassignmentsRequest` / `Response` — per generated owned types under `crates/protocol/generated/`.

---

## 2. State-machine semantics

### `PartitionRecord` field additions

```rust
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,           // existing
    pub isr: Vec<NodeId>,                // existing
    pub leader_epoch: i32,               // existing
    pub adding_replicas: Vec<NodeId>,    // NEW — empty when no reassignment in flight
    pub removing_replicas: Vec<NodeId>,  // NEW — empty when no reassignment in flight
}
```

Add these as plain new fields. No `#[serde(default)]`, no backwards-compat shim — the project is greenfield and undeployed (per `CLAUDE.md`); developers wipe local data dirs as needed.

### Invariants

- `adding_replicas ⊆ replicas` and `removing_replicas ⊆ replicas`
- `adding_replicas ∩ removing_replicas = ∅`
- `target_replicas := replicas \ removing_replicas` (the post-reassignment state)
- Reassignment in flight ⇔ `adding_replicas ≠ [] ∨ removing_replicas ≠ []`
- Old (pre-slice-14) callers see the new fields as empty Vecs — no semantic change

### AlterPartitionReassignments flow

For each `(topic, partition, target_opt)`:

**Case A — Start new reassignment (`target_opt = Some(target)`):**
1. Validate target: non-empty, no duplicates, every node id is a known broker; RF-change permitted if v1 flag false → must equal `len(current_target)`.
2. Compute `current_target = replicas \ removing_replicas`, `old = current_target \ target`, `new = target \ current_target`.
3. If both empty → return `error_code = 0` (no-op).
4. If reassignment is already in flight, the new alter replaces it; old `adding`/`removing` get recomputed against the new target.
5. Submit `PartitionRecord` with `replicas = current_target ∪ target`, `adding_replicas = new`, `removing_replicas = old`; `leader, isr, leader_epoch` unchanged.

**Case B — Cancellation (`target_opt = None`):**
1. If no reassignment in flight → return `NO_REASSIGNMENT_IN_PROGRESS (85)`.
2. Submit `PartitionRecord` with:
   - `replicas = replicas \ adding_replicas` (revert to pre-reassignment set)
   - `adding_replicas = []`, `removing_replicas = []`
   - `isr = isr \ adding_replicas`
   - If current leader is in old `adding_replicas` → elect new leader from reverted `replicas ∩ isr`; if none alive → `ELIGIBLE_LEADERS_NOT_AVAILABLE (81)`. Bump `leader_epoch += 1` only when leader changes; otherwise epoch stays. (Matches Kafka: leader_epoch is per-leader, not per-config.)

### ListPartitionReassignments flow

- `topics = None` → every partition where `adding ≠ [] ∨ removing ≠ []`.
- `topics = Some([{name, partition_indexes: Vec<i32>}])` → filter; **empty `partition_indexes`** = all of that topic; non-empty = just those. (Per generated owned type: `partition_indexes` is a `Vec<i32>`, not an `Option`. Empty is the "all" sentinel.) Skip partitions where adding+removing are both empty.
- Per row: `{partition_index, replicas, adding_replicas, removing_replicas}`.

### Background completion task — algorithm

For each partition where `adding ≠ [] ∨ removing ≠ []`:

1. `target = replicas \ removing_replicas`
2. `adding_caught_up = adding_replicas ⊆ isr`
3. If `!adding_caught_up` → wait this tick.
4. If `leader ∈ removing_replicas`:
   - Pick `new_leader ∈ target ∩ isr` that is alive. If none → wait.
   - Submit `PartitionRecord` with `leader = new_leader`, `leader_epoch += 1`; replicas/adding/removing unchanged.
   - On next image apply, step 5 fires.
5. Otherwise (leader stays — it's already in target):
   - Submit completion record: `replicas = target`, `adding = []`, `removing = []`, `isr = isr ∩ target`, `leader_epoch` unchanged.

### Error code mapping

| Condition | Wire code |
|---|---|
| Unknown topic/partition | `UNKNOWN_TOPIC_OR_PARTITION (3)` |
| Cancellation but no reassignment in progress | `NO_REASSIGNMENT_IN_PROGRESS (85)` |
| Duplicate replica id in target | `INVALID_REPLICA_ASSIGNMENT (39)` |
| Unknown broker id in target | `INVALID_REPLICA_ASSIGNMENT (39)` |
| Empty target | `INVALID_REPLICA_ASSIGNMENT (39)` |
| RF change when `allow_replication_factor_change=false` | `INVALID_REPLICA_ASSIGNMENT (39)` |
| Cancel where leader was adding and no eligible new leader | `ELIGIBLE_LEADERS_NOT_AVAILABLE (81)` |
| Submit failed (raft timeout / not leader) | `COORDINATOR_NOT_AVAILABLE (15)` |
| Non-super user, no ACL | `CLUSTER_AUTHORIZATION_FAILED (31)` (whole-request) |

---

## 3. Controller wiring & replicator interaction

### Reassignment completion task

`crates/broker/src/reassignment.rs`. Mirrors slice 14's `leader_rebalance.rs` shape:

- Trait `ReassignmentController`: `is_leader()`, `current_image()`, `watch_image() -> watch::Receiver<Arc<MetadataImage>>`, `submit_change(Vec<MetadataRecord>) -> Result<...>`.
- Driver loop:
  ```rust
  loop {
      tokio::select! {
          _ = image_watcher.changed() => {},
          _ = shutdown.cancelled() => return,
      }
      if !controller.is_leader() { continue; }
      let image = controller.current_image();
      let updates = compute_reassignment_progress(&image, &liveness).await;
      if !updates.is_empty() {
          controller.submit_change(updates).await?;
      }
  }
  ```
- **Image-driven, not timer-driven.** Reassignment progress is gated on ISR catching up — which only changes on image apply.
- Spawned unconditionally from `Broker::start`; per-tick `is_leader()` check no-ops on followers.

### `compute_reassignment_progress` (pure logic, unit-testable)

Iterates every partition; produces `Vec<MetadataRecord>` for the partitions whose state should advance. Returns updates following the algorithm in §2. Mockable via the trait above for unit tests.

### Replicator interaction — zero changes needed

`replicator_supervisor.rs` already iterates `p.replicas` to determine which partitions to follow. Because slice 15's invariant is `replicas = union(old, new)` during reassignment, **the existing replicator transparently starts fetching on adding-replicas the instant the image is applied.** Slice 10b's ISR-maintenance task grows ISR as new replicas catch up, triggering the completion task.

When reassignment completes and old replicas drop out of `replicas`, the replicator's existing diff-on-image-change loop tears down those followers naturally.

### `MetadataImage` accessor additions

- `partition(topic, partition_index) -> Option<&PartitionRecord>` — existing.
- `reassignments_in_flight() -> impl Iterator<Item = &PartitionRecord>` — new; returns partitions with non-empty adding or removing. Used by `ListPartitionReassignments`.

### `MetadataRecord` serialization

Same `V1Partition(PartitionRecord)` discriminant. Two new fields appended. No `#[serde(default)]`, no migration shim — wipe local raft logs / data dirs when developing across the slice boundary.

---

## 4. Handler implementation

### Files (new)

```
crates/broker/src/handlers/
├── alter_partition_reassignments.rs   # NEW — api_key 45
└── list_partition_reassignments.rs    # NEW — api_key 46
```

### `AlterPartitionReassignments` handler

- Cluster Alter authorize gate (same pattern as ElectLeaders).
- For each `(topic, partition)`, call `process_one_partition(&image, topic, partition, target_opt, allow_rf_change)` — pure logic, returns `Result<Option<PartitionRecord>, (i16, String)>`.
- Accumulate successful records into `to_submit`, accumulate per-partition rows into the response.
- Submit `to_submit` via `controller.submit_change(...)`. On error, mark every queued OK row with `COORDINATOR_NOT_AVAILABLE`.
- Encode `AlterPartitionReassignmentsResponse`.

### `process_one_partition` — pure logic

Pseudocode:

```rust
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
) -> Result<Option<PartitionRecord>, (i16, String)> {
    let pr = image.partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr),
        Some(t) => {
            validate_target(t, image, allow_rf_change, pr)?;
            start_path(pr, t)
        }
    }
}
```

The `cancel_path` and `start_path` branches follow §2's case-A/case-B algorithms exactly.

### `ListPartitionReassignments` handler

- Cluster Describe authorize.
- Walk `req.topics`; for each requested partition (or every in-flight partition when `topics = None`), emit a row.
- Group by topic into the response shape.

### Error code constants

Append to `crates/broker/src/codes.rs`:

```rust
pub const NO_REASSIGNMENT_IN_PROGRESS: i16 = 85;
// Confirm INVALID_REPLICA_ASSIGNMENT (39) already exists — slice 11 may have added it.
```

### Dispatch wiring

Same inline-intercept pattern as slice 13 ACLs + slice 14 ElectLeaders. Both handlers need `&Principal` + `&SocketAddr`, so they can't ride the static `HandlerTable`.

`handlers/api_versions.rs::supported_apis` appends:
```rust
v!(alter_partition_reassignments_request),
v!(list_partition_reassignments_request),
```

`network/dispatch.rs::handler_body_flexible` appends:
```rust
45 => version >= crabka_protocol::owned::alter_partition_reassignments_request::FLEXIBLE_MIN,
46 => version >= crabka_protocol::owned::list_partition_reassignments_request::FLEXIBLE_MIN,
```

Plus per-connection intercept arms with `handle_alter_partition_reassignments_frame` and `handle_list_partition_reassignments_frame` helpers (mirroring slice 14's `handle_elect_leaders_frame`).

---

## 5. Testing strategy

### Unit tests (~14 tests)

**`reassignment.rs` — completion-task pure logic (~8 tests):**
- `start_new_reassignment_writes_union_replicas`
- `cancel_clears_adding_and_removing`
- `cancel_when_no_reassignment_returns_error`
- `complete_when_adding_in_isr_writes_target`
- `wait_when_adding_not_in_isr`
- `leader_handoff_when_leader_in_removing`
- `validate_rejects_duplicate_replicas`
- `validate_rejects_unknown_brokers`

**`alter_partition_reassignments.rs` — `process_one_partition` (~6 tests):**
- `noop_when_already_at_target`
- `replaces_existing_in_flight_reassignment`
- `rf_change_rejected_when_disabled`
- `rf_change_allowed_when_enabled`
- `cancel_with_leader_in_adding_reverts_leader`
- `empty_target_rejected`

### Broker integration tests (`crates/broker/tests/partition_reassignment.rs`, 4 tests)

Reuse 3-broker PLAINTEXT scaffolding from slice 14's `elect_leaders.rs`:

1. **`alter_then_complete_via_isr_catchup`** — create rf=2 topic on `[1,2]`, alter to `[1,3]`, assert image shows `adding=[3], removing=[2]`; inject ISR=[1,2,3] via `submit_metadata_record_for_test`; assert completion writes `replicas=[1,3], adding=[], removing=[]`.
2. **`list_in_flight_returns_pending_rows`** — alter, immediately call `ListPartitionReassignments(None)`, assert non-empty.
3. **`cancel_via_null_replicas_reverts`** — alter, then call alter with `replicas=None`, assert image reverts to pre-reassignment state.
4. **`non_super_user_denied`** — single-broker SASL/PLAIN, alice has no ACLs, seed one unrelated ACL to disable the slice-13 compat shim, expect per-partition `CLUSTER_AUTHORIZATION_FAILED (31)`.

(The fully-organic "spin up actual replicator + wait for fetch catch-up" path is too flaky for an integration test given timing variance. The metadata-injection approach used by slice 14 is the deterministic substitute; the replicator's actual catchup behavior is already exercised by slice 10b's existing tests.)

### JVM acceptance (`crates/broker/tests/jvm_acceptance.rs`, 1 test)

`jvm_kafka_reassign_partitions_end_to_end` — `#[ignore]`-tagged, runs via WSL:

1. Spin up 3-broker SASL/PLAINTEXT cluster (reuse slice 14's `start_three_broker_sasl_plaintext_jvm_cluster`).
2. `kafka-topics --create --topic foo --partitions 1 --replication-factor 2`.
3. Write a reassignment JSON file: `{"version":1, "partitions":[{"topic":"foo","partition":0,"replicas":[2,3]}]}` (move off broker 1).
4. `docker run mirror.gcr.io/confluentinc/cp-kafka:7.5 kafka-reassign-partitions --execute --reassignment-json-file ...` — assert exit 0.
5. `kafka-reassign-partitions --verify` — assert "completed successfully" or poll until.
6. Assert image shows `replicas=[2,3]` on the partition.

Use metadata-injection workaround if WSL2 networking blocks organic replicator catchup (same trick as slice 14 T10).

### Compat-shim seeding for the deny test

Slice 13's "no ACLs ⇒ allow everything" compat shim applies. The auth-deny test needs one seeded unrelated ACL (via `submit_metadata_record_for_test`) to disable the shim. Same recipe as slice 14 T9.
