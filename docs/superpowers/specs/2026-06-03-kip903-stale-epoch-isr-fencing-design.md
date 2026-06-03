# KIP-903 — Stale-epoch ISR fencing (design)

Date: 2026-06-03
Status: approved

## Goal

Take KIP-903 ("Fence replicas with stale broker epoch from the ISR") from ⚠️
(partial) to ✅. The Fetch-side wire change (moving `replica_id` into the tagged
`replica_state` field) is already done. What remains is the actual **fencing**:
the controller must reject replicas with a stale — or absent — broker epoch from
joining the ISR on `AlterPartition`, returning `INELIGIBLE_REPLICA` (error 92).

This requires introducing a real broker-epoch model, which Crabka currently
lacks end-to-end:

- `BrokerRegistrationRecord` carries no epoch.
- The heartbeat client sends `broker_epoch: 0`.
- `isr_maintenance` stamps each ISR member's epoch as `-1`.
- The `AlterPartition` handler reads `new_isr_with_epochs` but ignores the epoch.

## Broker-epoch model (KRaft-faithful)

The broker epoch is the **raft log offset at which the broker's registration
record committed** — exactly Kafka's semantic (`ClusterControlManager` sets
`brokerEpoch = writeOffset`). The offset is assigned **once, at append, on the
leader**, and baked into the record bytes. Every other code path (live commit
apply, restart replay from log, snapshot install) reads the epoch out of the
record bytes — there is no position-derived logic at apply time.

Why this is correct for fencing: the fencing only needs the epoch to *change on
every (re-)registration*, with leader and controller both reading it from the
same replicated record. When a broker restarts it re-registers at a new offset →
new epoch. A partition leader whose metadata image predates the re-registration
stamps the *old* epoch on its `AlterPartition`; the controller (newer image)
compares against the *new* epoch, they differ, and the replica is fenced. Once
the leader's image catches up, it stamps the new epoch and the expand succeeds.
No false positives when both sides see the same image.

Single-writer invariant the assignment relies on: in the controller actor,
`self.log.log_end_offset()` immediately before `self.log.append(&mut batch)`
equals the `base` offset that `append` returns, so the i-th record in the batch
lands at `base + i`.

## Changes

### 1. Metadata record — `crates/metadata/src/records.rs`

Add `pub broker_epoch: i64` to `BrokerRegistrationRecord`. Snapshot-safe with no
extra work: `MetadataImage::to_records()` already re-emits the full
`V1BrokerRegistration` record (image.rs ~619), so the field round-trips through
snapshots as long as the KRaft translate layer carries it (change 2).

### 2. KRaft wire round-trip — `crates/metadata/src/kraft_translate.rs`

`RegisterBrokerRecord` (generated wire struct) already has `broker_epoch: i64`;
translate currently defaults it on encode and drops it on decode.

- `register_broker_to_kraft`: set `broker_epoch: b.broker_epoch`.
- `register_broker_from_kraft`: read `broker_epoch` into the metadata record.
- Update the module doc comment (lines ~45–46) that lists `broker_epoch` among
  the dropped extras.

### 3. Offset = epoch assignment — `crates/raft/src/kraft/controller.rs`

In `on_submit_change`, before the encode/validate loop:

```rust
let assign_base = self.log.log_end_offset();
```

Inside the loop, for each `V1BrokerRegistration` record, stamp the epoch into a
cloned record *before* `validate` / `to_kraft_values` / `scratch.apply`, using
the number of value blobs already allocated as its offset delta:

```rust
let delta = i64::try_from(value_blobs.len()).unwrap_or(i64::MAX);
let stamped;
let r: &MetadataRecord = match r {
    MetadataRecord::V1BrokerRegistration(b) => {
        let mut b = b.clone();
        b.broker_epoch = assign_base + delta;
        stamped = MetadataRecord::V1BrokerRegistration(b);
        &stamped
    }
    other => other,
};
```

`V1BrokerRegistration` maps 1:1 to a single blob, so `value_blobs.len()` at the
point this record is processed equals its offset delta within the batch. The
test-only `test_append_and_commit` path is left unchanged (not the registration
path under test).

### 4. Image accessor — `crates/metadata/src/image.rs`

```rust
pub fn broker_epoch(&self, node_id: NodeId) -> Option<i64> {
    self.brokers.get(&node_id).map(|b| b.broker_epoch)
}
```

### 5. Leader stamps real epochs — `crates/broker/src/isr_maintenance.rs`

`send_alter_partition` currently sends `broker_epoch: -1` for every `BrokerState`
and `-1` at the request top level. Pass the metadata image in (or look it up) and:

- top-level `broker_epoch` = `image.broker_epoch(self_node).unwrap_or(-1)`.
- each `BrokerState.broker_epoch` = `image.broker_epoch(node).unwrap_or(-1)`.

The image is already fetched by `compute_proposal`'s caller via
`cfg.controller`; `send_alter_partition` already calls `controller.current_image()`
for topic-id lookup, so the epochs come from that same image.

### 6. Controller fencing — `crates/broker/src/handlers/alter_partition.rs`

In `handle_partition`, after the leader-epoch fence and before (or together with)
the subset validation: walk the v3 `new_isr_with_epochs`. For each `BrokerState`:

- broker not registered in the image (`image.broker(node).is_none()`) →
  partition is ineligible.
- stamped `broker_epoch != -1` and `image.broker_epoch(node) != Some(stamped)` →
  partition is ineligible.

If any replica is ineligible, return `INELIGIBLE_REPLICA` for the whole partition
(matches Kafka: the partition fails as a unit; no partial ISR application). v2
requests (`new_isr` populated, `new_isr_with_epochs` empty) carry no epochs and
skip epoch fencing entirely — the existing subset validation still applies.

`handle_partition` needs the `image` (already passed) to call `broker` /
`broker_epoch`.

### 7. Error code — `crates/broker/src/codes.rs`

```rust
/// KIP-903: the new ISR contains at least one ineligible replica
/// (unregistered, or carrying a stale broker epoch).
pub const INELIGIBLE_REPLICA: i16 = 92;
```

No `BrokerError` variant is needed — `handle_partition` returns codes directly
via `error_part` (same pattern as `FENCED_LEADER_EPOCH`).

## Tests

- **kraft_translate** (unit): `broker_epoch` round-trips through the
  `RegisterBroker` KRaft value bytes (extend the existing
  `register_broker_*_round_trips` tests to set a non-zero epoch).
- **raft controller** (integration, single-voter, model on
  `submit_change_commits_on_single_voter_leader`): submitting a
  `V1BrokerRegistration` assigns `image.broker_epoch(id) == committed base
  offset`; a second registration of the same broker bumps the epoch to the new
  offset.
- **alter_partition handler** (unit on `handle_partition`):
  - epoch matches image → success, change appended.
  - stale epoch (request epoch ≠ image epoch) → `INELIGIBLE_REPLICA`, no change.
  - replica not registered in image → `INELIGIBLE_REPLICA`.
  - v2 path (`new_isr` set, no epochs) → unaffected by epoch fencing.
- Existing `alter_partition` / ISR / kraft tests stay green.

## Out of scope

- **Heartbeat broker-epoch validation** — that is KIP-500 fencing, not KIP-903.
  The heartbeat continues to send its current value; nothing validates it.
- **JVM-broker-as-follower epoch feedback** — Crabka brokers self-register via
  `submit_change`, so they never learn their assigned epoch from a response. A
  JVM broker registering through a Crabka controller would need
  `BrokerRegistrationResponse` to echo the assigned epoch; that is a separate
  registration-RPC work item.
- **README KIP-matrix flip** (⚠️→✅) happens at the very end, after the
  implementation and tests land.

## Construction-site fallout

Adding `broker_epoch` to `BrokerRegistrationRecord` breaks every struct literal
that builds one. These are compiler-found and set to `0` (or `broker_epoch: 0`)
where the epoch is irrelevant:

- `crates/broker/src/broker.rs` self-registration (submits 0; the leader
  overwrites it with the assigned offset at append).
- Test/bench literals: `crates/metadata` (image.rs, records.rs, kraft_translate.rs
  tests; benches/image.rs; tests/evolution.rs), `crates/raft` (snapshot.rs),
  `crates/protocol` (kraft_metadata_roundtrip.rs) — wherever a
  `BrokerRegistrationRecord { .. }` literal appears.
