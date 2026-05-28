# KIP-392 — Fetch From Follower / Rack-Aware Reads

**Date:** 2026-05-28
**Status:** Design approved, pending implementation plan

## Summary

Implement Kafka KIP-392 end-to-end in Crabka: a consumer that advertises a
`client.rack` can be redirected by the partition leader to a *follower* replica
in the same rack and read committed records from it, reducing cross-rack
(cross-AZ) network traffic.

Most of the wire surface already exists in the codebase:

- `FetchRequest.rack_id` (v11+) — already decoded.
- `FetchResponse.preferred_read_replica` (v11+, default `-1`) — already in the
  generated type and encoded only at v11+.
- `BrokerRegistrationRecord.rack: Option<String>` — already stored and already
  emitted in Metadata responses.
- The fetch handler already reads from whatever local partition exists (it does
  **not** gate on leadership), so a follower can structurally serve a read.

The feature therefore reduces to three additions plus one correctness fix:

1. Two broker config fields (`broker.rack`, `replica.selector`) and wiring the
   rack into broker self-registration.
2. A pure `ReplicaSelector` that the leader runs to populate
   `preferred_read_replica`.
3. **Follower HW propagation:** the follower must learn the leader-reported high
   watermark so consumer reads against it are correctly bounded. Today the
   replicator throws this value away.
4. A long-poll correctness fix so a consumer parked at a follower's HW wakes
   when that HW advances.

## Goals / Success Criteria

- A multi-broker cluster with brokers in distinct racks, a topic replicated
  across them, a consumer advertising `client.rack` matching a follower's rack:
  - First fetch to the leader returns `preferred_read_replica` = the follower's
    node id.
  - The consumer's next fetch, sent to that follower, returns the committed
    records, bounded by the follower's high watermark.
- Wire byte-exactness preserved: `preferred_read_replica` encodes only at Fetch
  v11+ and is `-1` (no preference) otherwise.
- No regression to follower (inter-broker) replication fetches.

## Non-Goals

- JVM `replica.selector.class` class-loading. Crabka uses a native enum
  (`replica.selector = leader | rack-aware`). Crabka is greenfield with no
  JVM-style `server.properties` users to satisfy.
- Custom user-supplied selector plugins. Only the two built-in selectors ship.
- Backwards-compatibility shims of any kind (Crabka is greenfield/undeployed).

## Background: relevant existing code

- Fetch handler: `crates/broker/src/handlers/fetch.rs`.
  - Consumer vs. follower fetch decided by `is_follower_fetch`
    (`effective_replica_id >= 0`), ~line 96-101.
  - `do_read` (~line 850) clamps consumer reads to `part.high_watermark()`;
    follower reads see up to LEO.
  - Consumer long-poll (`long_poll_then_reread`, ~line 1027) currently waits
    only on each partition's `append_notify`.
  - KIP-320 epoch fence already implemented at ~line 271.
- Partition: `crates/broker/src/partition.rs`.
  - `high_watermark()` (~line 400) returns `replica_state.hw`.
  - `hw_advance_notify` exists and is fired when HW advances.
  - `current_leader: AtomicU64`, `current_leader_epoch` already tracked.
- Replica progress: `crates/broker/src/replica_state.rs`. `ReplicaState.hw`
  is computed as `min(LEO over ISR)` — **leader-side only** today.
- Replicator: `crates/broker/src/replicator.rs`. `handle_response`
  (~line 291) appends batches but **ignores** `part_resp.high_watermark`.
- Metadata records: `crates/metadata/src/records.rs`.
  `PartitionRecord { leader, replicas, isr, leader_epoch, ... }` and
  `BrokerRegistrationRecord { node_id, host, port, rack, endpoints }`.
- Broker config: `crates/broker/src/config.rs` (`BrokerConfig`).
- Broker self-registration: `crates/broker/src/broker.rs` (today builds
  `BrokerRegistrationRecord` with `rack: None`).

## Design

### Chosen approach: reuse `ReplicaState.hw` for follower HW (Approach A)

A follower stores the leader-reported HW into the same `replica_state.hw` field
the leader uses. Because `do_read` already reads `part.high_watermark()`, the
consumer-read clamp on a follower works with **no read-path changes**. The
selector runs only on the leader. On a follower, `ReplicaState`'s
`isr`/`per_follower` fields are simply unused; on a follower→leader transition
the existing `install_isr` / `recompute_hw_for_leader_append` paths re-establish
`hw` coherently.

Rejected alternative (Approach B): a separate `follower_hw: AtomicI64` on
`Partition` with role-dependent `high_watermark()`. Cleaner role separation but
duplicates HW storage and the notify path for identical behavior.

### 1. Config & broker registration

Add to `BrokerConfig`:

- `rack: Option<String>` — parsed from `broker.rack`. Default `None`.
- `replica_selector: ReplicaSelectorKind` — parsed from `replica.selector`.
  Enum `{ Leader, RackAware }`, default `Leader`. Unknown values are a
  startup error.

Wire `config.rack` into broker self-registration in `broker.rs` so the emitted
`BrokerRegistrationRecord.rack` carries it (replacing the hardcoded `None`).
Metadata responses already project `rack`, so no handler change there.

`ReplicaSelectorKind` is reachable from the fetch handler via the `Broker`
(alongside the rest of `broker.config`).

### 2. `ReplicaSelector` module — `crates/broker/src/replica_selector.rs`

Pure, no I/O, fully unit-testable.

```rust
pub(crate) struct ReplicaView {
    pub node_id: i32,
    pub rack: Option<String>,
    pub in_isr: bool,
}

pub(crate) enum ReplicaSelectorKind { Leader, RackAware }

impl ReplicaSelectorKind {
    /// Returns the node id of the preferred read replica, or -1 to mean
    /// "no preference — read from the leader".
    pub(crate) fn select(
        &self,
        client_rack: Option<&str>,
        leader_id: i32,
        replicas: &[ReplicaView],
    ) -> i32;
}
```

Behavior:

- `Leader`: always returns `-1`.
- `RackAware`:
  - If `client_rack` is `None` → `-1`.
  - Otherwise consider only replicas that are **in the ISR** and whose `rack`
    equals `client_rack`. Restricting to ISR ensures we only redirect a consumer
    to a replica known to be caught up.
  - Among those, pick the **lowest `node_id`** (deterministic tie-break; KIP-392
    does not mandate one).
  - If the winner is the leader, or there is no same-rack in-sync replica →
    `-1` (the consumer is already best served by the leader).

`-1` is the existing default of `preferred_read_replica` and the universal
"stay on the leader" signal.

### 3. Follower HW propagation

Add to `Partition`:

```rust
/// Record the high watermark the leader reported in a follower Fetch
/// response. Clamps to the local log end so we never expose records this
/// follower has not yet replicated. Fires `hw_advance_notify` on advance.
pub async fn set_follower_hw(&self, reported_hw: i64) {
    let log_end = self.log_end_offset();
    let new_hw = reported_hw.min(log_end);
    let advanced = {
        let mut st = self.replica_state.lock().await;
        if new_hw > st.hw { st.hw = new_hw; true } else { false }
    };
    if advanced { self.hw_advance_notify.notify_waiters(); }
}
```

HW is monotonic from the leader and `log_end` only grows during normal
replication; after a truncation (`OFFSET_OUT_OF_RANGE` / epoch fence) the
clamp to `log_end` keeps `hw` from exceeding retained data. We only advance
`hw` (never regress it here), matching HW monotonicity semantics.

In the replicator's `handle_response`, in the `codes::NONE` branch, call
`set_follower_hw(part_resp.high_watermark)` on **every** successful response,
including those with no new batch — a caught-up follower must still track the
leader's advancing HW so consumers reading from it see newly committed offsets.
Place the call after the (optional) `replicate_batch` so `log_end` reflects any
just-appended records before the clamp.

### 4. Leader-side selection in the fetch handler

For **consumer** fetches only (`!is_follower_fetch`) where `req.rack_id` is
non-empty: after `responses` is built and before the KIP-227 session-shaping
block, iterate each partition row whose `error_code == NONE` and set
`preferred_read_replica`:

1. From the metadata `image` already loaded in the handler, look up the
   partition's `PartitionRecord` to get `replicas`, `isr`, and `leader`.
2. Join each replica `node_id` to its broker `rack` via the image's broker
   registrations, building `Vec<ReplicaView>`.
3. `let pref = broker.config.replica_selector.select(Some(&req.rack_id),
   leader_id, &views);`
4. `out.preferred_read_replica = pref;`

The existing fetch-session cache already tracks `last_preferred_read_replica`
and diffs on it, so incremental responses are handled with no extra work. The
field encodes only at v11+, so older clients are unaffected (and v<11 clients
cannot send `rack_id` anyway).

### 5. Long-poll correctness fix

In `long_poll_then_reread`, for **consumer** fetches, add each readable
partition's `hw_advance_notify` to the wait set (in addition to
`append_notify`). A consumer parked at a follower's HW must wake when the
leader-reported HW advances — that signal now arrives via `set_follower_hw`
→ `hw_advance_notify`, not via raw append. Follower (inter-broker) fetches keep
the current append-only wait behavior.

### 6. Edge cases / behavior

- **Follower serves reads:** no leadership gate is added; the handler already
  reads the local partition. With a populated follower HW, consumer reads clamp
  correctly via the unchanged `do_read`.
- **Offset beyond follower HW:** `do_read` returns empty; the consumer
  long-polls and wakes on the HW-advance fix above.
- **Stale epoch:** the existing KIP-320 epoch fence returns
  `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH`; the consumer refreshes
  metadata. Verify the follower partition's `current_leader_epoch` is kept
  current by the metadata-apply path.
- **Reassignment / partition no longer hosted:** local partition is absent →
  `UNKNOWN_TOPIC_OR_PARTITION`; the consumer refreshes metadata and returns to
  the leader.
- **Follower falls out of ISR:** the leader stops naming it in
  `preferred_read_replica` (it is no longer `in_isr`); the consumer reverts to
  the leader after `metadata.max.age.ms`. Standard KIP-392 client semantics; no
  broker-side special-casing.

## Testing

### Unit — `replica_selector`
- `Leader` kind always returns `-1`.
- `RackAware` picks the same-rack ISR member (lowest node id on tie).
- `RackAware` returns `-1` when: `client_rack` is `None`; no same-rack replica;
  the only same-rack in-sync replica is the leader.
- `RackAware` ignores a same-rack replica that is **not** in the ISR.

### Unit — `Partition::set_follower_hw`
- Clamps to local LEO when `reported_hw > log_end`.
- Advances `hw` and fires the notify when `reported_hw` (clamped) `> hw`.
- No-op (no notify) when not advancing.

### Integration — end-to-end (success criterion)
- Leader + follower on distinct racks; topic replicated across both.
- Produce records to the leader; wait for the follower to replicate and for HW
  to advance.
- Consumer with `client.rack` = follower's rack issues a Fetch to the leader →
  response carries `preferred_read_replica` = follower node id.
- Consumer issues a Fetch to the follower → receives the committed records,
  bounded by the follower's HW.
- Model on the existing multi-broker loopback integration test (slice 48f
  topic-backed RLMM loopback test).

### Wire exactness
- Assert `preferred_read_replica` is encoded at Fetch v11+ and absent / `-1` at
  lower versions.

## Files touched

| File | Change |
|------|--------|
| `crates/broker/src/config.rs` | Add `rack`, `replica_selector` fields + parsing |
| `crates/broker/src/broker.rs` | Populate `BrokerRegistrationRecord.rack` from config |
| `crates/broker/src/replica_selector.rs` | **New** — `ReplicaView`, `ReplicaSelectorKind`, `select` + unit tests |
| `crates/broker/src/partition.rs` | Add `set_follower_hw` + unit tests |
| `crates/broker/src/replicator.rs` | Call `set_follower_hw` in `handle_response` NONE branch |
| `crates/broker/src/handlers/fetch.rs` | Populate `preferred_read_replica` for rack-aware consumer fetches; add `hw_advance_notify` to consumer long-poll wait set |
| `crates/broker/src/lib.rs` (or module root) | Register `replica_selector` module |
| integration tests | New end-to-end fetch-from-follower test |
