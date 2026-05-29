# Crabka — KIP-966 offset-aware unclean recovery

**Date:** 2026-05-28
**Status:** Design approved, pending implementation plan

## Summary

Replace crabka's naive "elect the first alive replica" unclean leader election
with KIP-966-style offset-aware recovery: when a partition is offline (no live
leader, all ISR dead), the controller queries the surviving replicas for their
log-end-offset and last-written leader epoch, then elects the replica with the
most complete log. A new per-topic `unclean.recovery.strategy` config governs
the behavior and supersedes the existing `unclean.leader.election.enable` flag.

This covers **both** trigger paths: automatic failover (`on_broker_dead` when the
ISR empties) and the operator-triggered `ElectLeaders` UNCLEAN admin path.

## Background — what already exists

- `ElectLeaders` API (api_key 43) with PREFERRED + UNCLEAN election types
  (`crates/broker/src/handlers/elect_leaders.rs`).
- Pure election functions in `crates/broker/src/leader_election.rs`
  (`select_new_leader_for_partition`, `compute_failover_changes`,
  `on_broker_dead`). The current UNCLEAN path picks the first alive replica.
- `unclean.leader.election.enable` per-topic config (`config_keys.rs`), read
  on-demand by `leader_election::unclean_election_enabled()`. Drives automatic
  failover opt-in (commit `61dd1a24`).
- `PartitionRecord` metadata (`crates/metadata/src/records.rs`): leader,
  replicas, isr, leader_epoch, etc.
- `InterBrokerClient` (`crates/broker/src/network/client.rs`) for outbound
  inter-broker calls (used by raft and the replica fetcher).
- `ControllerLivenessState` (`crates/broker/src/heartbeat/controller_state.rs`)
  — per-broker alive/dead tracking via heartbeats.

### Gaps this design fills

- No controller→broker RPC exists; communication is broker→controller only.
- Replica LEOs live only on the partition leader broker and are lost when it
  dies, so the controller has no offset information to make a smart choice.

## Non-goals

- **Eligible Leader Replicas (ELR)** tracking. KIP-966's `Balanced` strategy
  waits for "all `LastKnownELR` members"; crabka has no ELR. We approximate
  Balanced with the alive members of the replica set (see §4). True ELR is a
  separate, later increment.
- **Persisted `LeaderRecoveryState` / RECOVERING marker** (KIP-704). Recovery
  state is kept in-memory and re-derived on controller-leadership change.
- Backwards-compat shims. Crabka is greenfield/undeployed; when the config or
  wire shape changes, it just changes.

## 1. Config model & strategy resolution

Add `unclean.recovery.strategy` to `crates/broker/src/config_keys.rs`:

- Constant `UNCLEAN_RECOVERY_STRATEGY = "unclean.recovery.strategy"`.
- Values: `None` | `Balanced` | `Aggressive`. Default `None`.
- Recognized in `is_recognized()`; validated in `validate_topic_config()`
  (reject any value outside the three; `INVALID_CONFIG` otherwise).
- Per-topic with cluster-default fallback, mirroring the precedence of the
  existing `unclean.leader.election.enable` flag.

New helper `resolve_recovery_strategy(image, topic) -> Strategy` (parallel to
the existing `unclean_election_enabled`) implements the **"strategy supersedes
flag"** layering:

| Resolved strategy | Behavior |
|---|---|
| `Balanced` / `Aggressive` | Offset-aware recovery (this design). |
| `None` | Fall back to existing `unclean.leader.election.enable`: `true` = legacy naive "first alive replica" pick (current code, untouched); `false` = stay offline. |

The legacy naive code path is retained unchanged for the `None` + `enable=true`
case.

## 2. Wire protocol — `GetReplicaLogInfo` (api_key 70, v0, flexible)

Add Kafka's `GetReplicaLogInfoRequest.json` / `GetReplicaLogInfoResponse.json`
to the protocol-codegen input set so owned/borrowed types are generated
byte-exact.

- **Request:** `BrokerId` (i32) + `TopicPartitions[{ TopicId (uuid),
  Partitions (i32[]) }]`.
- **Response:** `BrokerEpoch` (i64) + `TopicPartitionLogInfoList[{ Partition
  (i32), LastWrittenLeaderEpoch (i32), CurrentLeaderEpoch (i32), LogEndOffset
  (i64), ErrorCode (i16), ErrorMessage (nullable string) }]`.
- Version 0, flexible/tagged-fields encoding.

New broker-side handler `crates/broker/src/handlers/get_replica_log_info.rs`,
routed in `crates/broker/src/network/dispatch.rs`:

- Served on the **inter-broker listener** (controller→broker call,
  authenticated via inter-broker credentials / ClusterAction — not a public
  client RPC).
- Resolves `TopicId` → topic name via the metadata image.
- For each partition the broker hosts locally: returns local `LogEndOffset`,
  `LastWrittenLeaderEpoch` (leader epoch of the last batch in the local log),
  the broker's metadata-cache view of `CurrentLeaderEpoch`, and the broker's
  current `BrokerEpoch`.
- Partitions the broker doesn't host or whose log dir is offline get a
  per-partition error code (`REPLICA_NOT_AVAILABLE` / `KAFKA_STORAGE_ERROR`).

> Implementation note: confirm how to read the last-written leader epoch from
> the local log (leader-epoch cache vs. last batch header) during planning.

## 3. Controller-side Unclean Recovery Manager (URM)

New module `crates/broker/src/unclean_recovery.rs`.

A task spawned on the raft-leader node, owning an mpsc work queue. Work item:

```
{ topic, partition, strategy, reply: Option<oneshot::Sender<RecoveryOutcome>> }
```

The URM is the single owner of the poll→pick→elect flow. Both triggers funnel
through it:

- **Automatic failover:** `on_broker_dead` / `compute_failover_changes` in
  `leader_election.rs` — when `alive_isr` is empty and the resolved strategy is
  `Balanced`/`Aggressive`, enqueue with `reply: None` instead of doing the naive
  election. `None` strategy keeps the existing enable-flag behavior.
- **Operator:** the `ElectLeaders` UNCLEAN handler enqueues with a `reply`
  oneshot and awaits it under a request-bound deadline. Aggressive returns fast;
  a Balanced wait that exceeds the deadline returns a timeout / in-progress
  error code to the operator while the URM continues.

**Dedup:** an in-flight `HashSet<(topic, partition)>`. A second enqueue for an
already-in-flight partition returns an in-progress/`ELECTION_NOT_NEEDED` code to
the operator rather than starting a duplicate job.

**Controller-leadership change:** the in-memory queue is lost; the new raft
leader re-derives pending work by scanning leaderless partitions whose resolved
strategy ≠ `None`. No persisted RECOVERING marker. An operator oneshot pinned to
the old leader fails — the operator may retry.

**Idempotency:** before submitting, the URM re-reads the current metadata image
and aborts (`ELECTION_NOT_NEEDED`) if the partition already regained a live
leader via another path.

## 4. Replica selection & strategy wait rules

The URM resolves each alive replica's broker endpoint from registration
metadata, dials it via `InterBrokerClient` with a `GetReplicaLogInfo` request,
and collects responses.

**Winner:** maximum by `(LastWrittenLeaderEpoch, LogEndOffset)`; tie-broken by
lowest broker id for determinism.

**Fencing:**
- Discard a response whose `BrokerEpoch` no longer matches the broker's current
  registration epoch (the broker rebooted mid-poll).
- Abort the recovery as stale if any replica reports a `CurrentLeaderEpoch`
  higher than the controller's known partition leader epoch (a newer leader
  already exists).

**Wait semantics:**
- **Aggressive:** poll all alive replicas; pick the best among responders within
  a short timeout, or the first response received after the timeout. Optimizes
  availability.
- **Balanced:** poll all alive replicas; wait until *all currently-alive*
  replicas in the replica set respond (or a longer hard cap elapses), then pick
  the best. **ELR approximation** — substitutes "all alive members of the
  replica set" for KIP-966's "all `LastKnownELR` members" since crabka has no
  ELR. Zero responses → no election.

**On success:** `submit_change` a `PartitionRecord` with `leader = winner`,
`isr = [winner]`, bumped `leader_epoch`, replicas unchanged. Call the existing
`metrics.record_unclean_leader_election()`.

## 5. Error handling & edges

- **No alive replicas / zero responses:** no election; partition stays offline;
  operator receives an eligible-leaders-not-available code. Extend the
  `ElectErr` → wire-code mapping (`elect_error_to_wire`) in
  `handlers/elect_leaders.rs`.
- **Stale / rebooted responses:** fenced per §4.
- **Partition regains a leader mid-recovery:** URM aborts cleanly
  (`ELECTION_NOT_NEEDED`).
- **Controller leadership change mid-recovery:** new leader re-derives and
  re-enqueues; operator may retry.

## 6. Testing

- **Unit:**
  - Selection algorithm: epoch-then-offset ordering, broker-id tie-break,
    stale-`BrokerEpoch` fencing, higher-`CurrentLeaderEpoch` abort.
  - Strategy wait semantics against a mocked responder set (Aggressive early
    pick, Balanced full wait, hard-cap expiry).
  - `resolve_recovery_strategy` layering and fallback-to-flag when `None`.
- **Wire:**
  - Codegen round-trip (encode/decode) for `GetReplicaLogInfo` v0.
  - Broker handler returns correct LEO/epoch for hosted partitions and the
    right error codes for non-hosted / offline-log-dir partitions.
- **Integration:** multi-broker cluster; kill the leader and all ISR; assert the
  URM polls survivors and elects the longest-log replica. Cover Aggressive vs.
  Balanced timing, the operator `ElectLeaders` UNCLEAN path returning the
  elected leader, and the `None` + enable-flag fallback paths.

## Files touched

- `crates/protocol/...` — add `GetReplicaLogInfo` request/response JSON schemas
  to the codegen input; generated types.
- `crates/broker/src/config_keys.rs` — new strategy config key, validation,
  recognition.
- `crates/broker/src/leader_election.rs` — `resolve_recovery_strategy` helper;
  route Balanced/Aggressive from failover to the URM; retain `None` legacy path;
  extend `ElectErr` if needed.
- `crates/broker/src/unclean_recovery.rs` *(new)* — URM task, work item,
  selection algorithm, strategy wait logic.
- `crates/broker/src/handlers/get_replica_log_info.rs` *(new)* — broker-side
  handler for api_key 70.
- `crates/broker/src/network/dispatch.rs` — route api_key 70 on the inter-broker
  listener.
- `crates/broker/src/handlers/elect_leaders.rs` — route UNCLEAN with
  strategy ≠ None through the URM (enqueue + await oneshot under deadline);
  extend error mapping.
- Controller wiring — spawn the URM on raft leadership; supply broker-endpoint
  resolution + `InterBrokerClient`.
- `crates/broker/src/metrics.rs` — reuse `record_unclean_leader_election`;
  optional new counter for offset-aware recoveries.

## References

- [KIP-966: Eligible Leader Replicas](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966:+Eligible+Leader+Replicas)
- Prior crabka specs: `2026-05-15-crabka-elect-leaders-14-design.md`,
  `2026-05-13-crabka-bulletproof-eos-10b-design.md`.
