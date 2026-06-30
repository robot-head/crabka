# Slice 64a — KIP-848 next-gen consumer group protocol (foundations + JVM acceptance)

**Status:** design
**Date:** 2026-05-28
**Roadmap:** operator-roadmap slice 64 (Phase 12 — Parity tail). First sub-slice of the KIP-848 work; the original roadmap entry flagged "likely splits into sub-slices when planned."

## Goal

Ship the KIP-848 next-gen consumer group protocol on Crabka: two new handlers
(`ConsumerGroupHeartbeat` api_key 68, `ConsumerGroupDescribe` api_key 69),
server-side `UniformAssignor` + `RangeAssignor`, member-epoch state machine,
and full `__consumer_offsets` persistence for the new record types. The
next-gen coordinator runs alongside the existing classic coordinator on the
same broker; `group_id`s lock to either classic or next-gen on first record
persisted (no migration in this slice). Validated end-to-end against
`mirror.gcr.io/apache/kafka:4.0.0` clients using `group.protocol=consumer`.

## Non-goals

- Group migration (classic → next-gen, or back). A `group_id` is classic or
  next-gen for its lifetime; matches Kafka 4.0 production behavior.
- Custom server-side assignor plugin point — `Assignor` trait exists
  internally but is not exposed for external implementations. Defer to a
  follow-up slice (64c).
- Share groups (KIP-932). The codegen already emits `ShareGroupHeartbeat`;
  no handler.
- Rack-aware `UniformAssignor`. Trait carries rack info, but the assignment
  algorithm itself does not yet bias toward racks. Defer to 64b.
- Per-partition reconciliation queue or background tick. Reconciliation is
  trigger-driven; runs on the next heartbeat after a dirty signal.
- KIP-584 feature-level records. Gating uses a static broker config
  (`group.coordinator.rebalance.protocols`) rather than a metadata-quorum
  feature flag.
- No backwards-compatibility code (per `CLAUDE.md`). Crabka is greenfield;
  classic and next-gen are both fresh-ship.

## Architecture

### Top-level layout

```
crates/broker/src/
├── coordinator/
│   ├── mod.rs                    [existing GroupManager — dispatch by group type]
│   ├── group.rs                  [existing classic 5-state machine — unchanged]
│   ├── persistence.rs            [extended: parse key versions 3–8]
│   ├── bootstrap.rs              [extended: feed v3–8 records to NextGenCoordinator]
│   └── next_gen/
│       ├── mod.rs                [NextGenCoordinator — holds DashMap<group_id, NextGenGroupActor>]
│       ├── group_actor.rs        [per-group tokio task; mpsc messages, oneshot replies]
│       ├── group_state.rs        [MemberState, TargetAssignment, CurrentAssignment]
│       ├── reconciler.rs         [trigger-driven recompute on dirty]
│       ├── persistence.rs        [encode/decode v3–8 + tombstones]
│       ├── assignor/
│       │   ├── mod.rs            [Assignor trait + dispatch by name]
│       │   ├── uniform.rs        [UniformAssignor: equal-share]
│       │   └── range.rs          [RangeAssignor: per-topic ranges]
│       └── config.rs             [group.consumer.* broker configs]
├── handlers/
│   ├── consumer_group_heartbeat.rs   [NEW — api_key 68]
│   ├── consumer_group_describe.rs    [NEW — api_key 69]
│   ├── offset_commit.rs              [EXTEND — validate member_epoch for next-gen]
│   └── offset_fetch.rs               [EXTEND — validate member_epoch for next-gen]
└── codes.rs                          [EXTEND — new error codes]
```

### Architectural choice

**Approach 2 — split coordinator with per-group reconciler task.** Each
next-gen group is owned by its own tokio task; heartbeats are queued
mpsc messages to the task. Matches Kafka 4.0's "asynchronous
reconciliation" architecture and isolates assignor cost from the
heartbeat request-path latency. Costs paid: actor lifecycle plumbing
(shutdown ordering, supervisor on panic), but the isolation is worth
it for a protocol whose whole point is bounded heartbeat latency.

### Actor message protocol

```rust
enum GroupActorMessage {
    Heartbeat {
        request: HeartbeatRequest,                // member_id, epoch, subscription, owned_partitions
        reply: oneshot::Sender<HeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<GroupDescribeView>,
    },
    OffsetValidate {
        member_id: String,
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), GroupError>>,
    },
    MetadataChanged(Arc<MetadataImage>),         // broadcast from metadata-image subscriber
    SessionTick(Instant),                        // periodic eviction sweep
    Shutdown(oneshot::Sender<()>),
}
```

The actor owns: `members: HashMap<String, MemberState>`,
`target_assignment: TargetAssignment`, `current_assignment: HashMap<String,
MemberAssignment>`, `epoch: i32`, `subscribed_topics: HashSet<String>`,
`last_seen: HashMap<String, Instant>`.

### Heartbeat flow

1. Handler decodes `ConsumerGroupHeartbeatRequest`.
2. Looks up actor in `NextGenCoordinator::groups`; creates one if `group_id`
   is new (and the group is not already a classic group — type lock).
3. Sends `Heartbeat { request, reply }` over mpsc.
4. Actor processes:
   - Validate `member_epoch` (stale/fenced rules below).
   - Mutate member state; mark dirty if subscription / owned-partitions
     changed.
   - If dirty: run reconciler → recompute target assignment → persist new
     records to `__consumer_offsets`.
   - Compute per-member delta to send in response.
   - Persist `CurrentMemberAssignment` (key 8) on every state transition.
   - Reply via oneshot.
5. Handler encodes response.

### Member-epoch state transitions (per KIP-848)

1. **Join** (`epoch=0`, no `member_id`): actor assigns `member_id` (UUIDv7),
   creates `MemberState`, bumps group epoch, marks dirty, replies with
   `member_epoch=new_group_epoch` and empty assignment.
2. **Steady-state** (`epoch=group_epoch`): actor checks `owned_partitions`
   matches `current_assignment[member]`; replies with empty delta + interval
   if matched, otherwise sends remaining target-delta.
3. **Revocation phase**: when target ≠ current, actor sends current ∩ target
   (partitions to *keep*); member ack'd revocations advance its epoch.
4. **Assignment phase**: after revocations ack, actor sends new partitions
   to assignees.
5. **Leave** (`epoch=-1`): remove member, tombstone keys 5+7+8, bump group
   epoch, mark dirty.
6. **Stale epoch**: `request.epoch < member.epoch` → `STALE_MEMBER_EPOCH`.
7. **Fenced**: `request.epoch > member.epoch` → `FENCED_MEMBER_EPOCH`
   (client must rejoin from 0).
8. **Session timeout**: `SessionTick` evicts members whose
   `last_seen + session_timeout < now`. Tombstones records, bumps group epoch.

### Reconciliation triggers (mark-dirty signals)

- Member join, leave, or session-timeout eviction.
- Subscription change (`subscribed_topic_names` differs from cached).
- `MetadataChanged` — only if any subscribed topic's partition count changed.
- Per-group `server_assignor` selection change.

When dirty, reconciler runs at the next heartbeat: (1) re-runs assignor →
new `TargetAssignment`; (2) persists key types 6 + 7 to `__consumer_offsets`;
(3) per-member delta computed lazily during heartbeat response encoding.

### Persistence record types (in `__consumer_offsets`, partition 0)

| Key | Value | Lifecycle |
|----:|-------|-----------|
| 3 — `ConsumerGroupMetadataKey { group_id }` | `{ epoch }` | Written on every group-epoch bump. Tombstoned on group delete. |
| 5 — `ConsumerGroupMemberMetadataKey { group_id, member_id }` | `{ instance_id?, rack_id?, client_id, client_host, subscribed_topic_names[], subscribed_topic_regex?, server_assignor?, rebalance_timeout_ms, classic_member_metadata? }` | Written on member subscription change. Tombstoned on member leave/evict. |
| 6 — `ConsumerGroupTargetAssignmentMetadataKey { group_id }` | `{ assignment_epoch }` | Written when target assignment recomputed. Tombstoned on group delete. |
| 7 — `ConsumerGroupTargetAssignmentMemberKey { group_id, member_id }` | `{ topic_partitions: [{topic_id, partitions}] }` | Written per-member when target changes. Tombstoned on member leave. |
| 8 — `ConsumerGroupCurrentMemberAssignmentKey { group_id, member_id }` | `{ member_epoch, previous_member_epoch, state, assigned_partitions, partitions_pending_revocation }` | Written on every member state transition. Tombstoned on member leave. |

Key type 4 (`ConsumerGroupPartitionMetadataKey`) is **skipped** — Kafka 4.0
dropped it in implementation.

All writes go via the existing log-write path the classic coordinator uses
today. Compaction settings unchanged.

### Bootstrap

`coordinator/bootstrap.rs` already replays `__consumer_offsets` from start.
Extend `parse_key()` to dispatch v3–8 records to a per-group seed map held
by `NextGenCoordinator` during replay. After replay completes,
`NextGenCoordinator::finalize_bootstrap()` spawns one actor per non-empty
group, seeding each actor with its replayed state. No actors process
messages until replay is finished — the existing `/readyz` gate covers
this.

### Gating

Unconditional advertisement of api_keys 68/69 in
`crates/broker/src/handlers/api_versions.rs` — matches Kafka 4.0 production
behavior. Broker config `group.coordinator.rebalance.protocols` (default
`"classic,consumer"`) acts as a kill switch; when `consumer` is absent the
handlers return `GROUP_ID_NOT_FOUND` to force clients to classic.

### Group-type locking

The *first* record persisted for a `group_id` decides its lifetime type:

- Classic `JoinGroup` against a next-gen `group_id` → `UNKNOWN_MEMBER_ID`.
- `ConsumerGroupHeartbeat` against a classic `group_id` → `GROUP_ID_NOT_FOUND`.

Matches Kafka 4.0 — no migration in this slice.

### New broker configs (`crates/broker/src/config.rs`)

```
group.coordinator.rebalance.protocols = "classic,consumer"     # kill switch
group.consumer.session.timeout.ms = 45000
group.consumer.heartbeat.interval.ms = 5000
group.consumer.min.session.timeout.ms = 45000
group.consumer.max.session.timeout.ms = 60000
group.consumer.min.heartbeat.interval.ms = 5000
group.consumer.max.heartbeat.interval.ms = 15000
group.consumer.assignors = "uniform,range"
group.consumer.max.size = 200
```

All exposed via `IncrementalAlterConfigs` per the existing broker-config
flow.

### Error codes (`crates/broker/src/codes.rs`)

- `FENCED_MEMBER_EPOCH = 110` (new)
- `UNSUPPORTED_ASSIGNOR = 111` (new)
- `STALE_MEMBER_EPOCH = 113` (already present)
- `UNRELEASED_INSTANCE_ID = 114` (new)
- `UNKNOWN_SUBSCRIPTION_ID = 117` (new)
- `GROUP_ID_NOT_FOUND = 69` (already present; KIP-848 reuses)

### OffsetCommit / OffsetFetch extension

Both handlers gain a "lookup group type" step:

- Classic group → existing path unchanged.
- Next-gen group → forward `OffsetValidate { member_id, member_epoch }` to
  the group's actor; on success, persist offset record to
  `__consumer_offsets` (offset keys/values unchanged — KIP-848 reuses the
  classic offset record format).

## Error handling & edge cases

| Condition | Error code | Notes |
|-----------|-----------:|-------|
| `group.coordinator.rebalance.protocols` excludes `consumer` | `GROUP_ID_NOT_FOUND` | Forces client to classic. |
| `group_id` exists as classic group | `GROUP_ID_NOT_FOUND` | Type lock. |
| `member_id` unknown after join | `UNKNOWN_MEMBER_ID` | Client restarts at `epoch=0`. |
| `request.member_epoch < member.epoch` | `STALE_MEMBER_EPOCH` | Client retries with current. |
| `request.member_epoch > member.epoch` | `FENCED_MEMBER_EPOCH` | Client must rejoin from 0. |
| `server_assignor` not in configured assignors | `UNSUPPORTED_ASSIGNOR` | Per-group rejection. |
| `instance_id` already bound to a different live `member_id` | `UNRELEASED_INSTANCE_ID` | KIP-345 static-membership conflict. |
| Group size exceeds `group.consumer.max.size` | `GROUP_MAX_SIZE_REACHED` | Existing constant. |
| Subscription includes unknown topic | not an error | Member gets empty assignment for that topic. |

### Actor lifecycle

- **Reply-channel send failure (handler timed out):** actor logs, discards
  reply, continues. State already mutated — fine; client will retry with
  current epoch.
- **`__consumer_offsets` write failure:** actor rolls back in-memory state
  and returns `COORDINATOR_LOAD_IN_PROGRESS` to caller. Actor stays alive;
  next heartbeat retries.
- **Actor panic:** parent supervisor logs, removes actor from
  `NextGenCoordinator::groups`. The group's last-known state lives in a
  `NextGenCoordinator`-owned cache that survives the panic (updated by the
  actor on every state transition, snapshot-style). Next heartbeat
  re-creates the actor, seeded from the cache — no `__consumer_offsets`
  re-scan.
- **Shutdown:** `BrokerHandle::shutdown` → `NextGenCoordinator::shutdown_all`
  sends `Shutdown` to every actor with 5-second per-actor timeout; abort
  on timeout.

### Race / ordering edges

- **Heartbeat during bootstrap replay:** handler returns
  `COORDINATOR_LOAD_IN_PROGRESS`; `/readyz` gates traffic accordingly.
- **MetadataImage update during reconciliation:** reconciler snapshots the
  image once at start of each pass — no torn reads.
- **Concurrent `OffsetCommit` and member fenced:** `OffsetValidate`
  happens-before persistence, so a fenced member's offsets are never
  written.
- **Group becomes Empty:** actor stays alive (state may resurrect on
  rejoin). Explicit `DeleteGroups` only triggers tombstones. Matches
  Kafka 4.0.

### Compaction safety

Reconciler writes target/current assignment records **before** the heartbeat
response acknowledges the transition. Crash between write and client receipt
→ client retries at prior epoch → server returns the (already-persisted)
latest state. No torn transitions visible to clients.

## Testing

### Unit tests

| Module | Coverage |
|--------|----------|
| `next_gen/assignor/uniform.rs` | Equal-share distribution; deterministic ordering; stability under churn; single-member; zero-partition. ~12 tests. |
| `next_gen/assignor/range.rs` | Per-topic ranges; non-divisible partition counts; co-partitioning. ~6 tests. |
| `next_gen/group_state.rs` | Member-epoch transitions; revocation lifecycle; session timeout eviction; fenced/stale detection. ~15 tests. |
| `next_gen/reconciler.rs` | Dirty triggers; reconciliation no-op when target unchanged; idempotency. ~6 tests. |
| `next_gen/persistence.rs` | Round-trip encode/decode of key types 3, 5, 6, 7, 8 + tombstones; version negotiation; unknown-key tolerance. ~10 tests. |
| `coordinator/persistence.rs` extension | Mixed-version replay (v0–2 classic + v3–8 next-gen interleaved). ~4 tests. |
| `handlers/consumer_group_heartbeat.rs` | All error paths; response shape for join, steady, revoke, leave. ~10 tests. |
| `handlers/consumer_group_describe.rs` | Empty, single-member, mid-rebalance, fenced visibility. ~5 tests. |
| `handlers/offset_commit.rs` extension | Next-gen dispatch: stale rejected, fenced rejected, classic unchanged. ~4 tests. |

### Broker integration tests (`crates/broker/tests/`)

- `consumer_group_next_gen.rs` — raw-RPC scenarios: single-member join +
  heartbeat + assignment + commit + leave; two-member rebalance with
  revocation; three-member with one death + eviction; type-lock enforcement
  (cross-API attempts); kill-switch config; broker restart preserves state
  via `__consumer_offsets` replay. ~12 tests.
- `consumer_group_next_gen_persistence.rs` — bootstrap replay correctness
  for mixed classic+next-gen records; tombstone application. ~4 tests.

### JVM acceptance (`crates/broker/tests/jvm_consumer_group_next_gen.rs`)

New constant `KAFKA_IMAGE_NEXT_GEN = "mirror.gcr.io/apache/kafka:4.0.0"`. All consumer
invocations use `--consumer-property group.protocol=consumer`:

1. **Single-consumer round-trip.** Produce via `cp-kafka:7.5.0`; consume via
   `mirror.gcr.io/apache/kafka:4.0.0` with next-gen protocol; verify commit landed via
   `kafka-consumer-groups --describe`.
2. **Two-consumer rebalance.** Two consumers in same group; verify partitions
   split; kill one; verify survivor takes both.
3. **Server-assignor selection.** Config-driven opt-in to range; verify
   partitions assigned in contiguous ranges.
4. **`kafka-consumer-groups --describe`.** Confirms `ConsumerGroupDescribe`
   decoded by JVM admin tooling. State STABLE; member epochs; partitions.
5. **`kafka-consumer-groups --delete`.** Tombstones land; group disappears.
6. **Coexistence.** Same broker; two groups; one classic, one next-gen;
   both work concurrently; cross-API queries see only their own type.

### CI infrastructure

- Add `mirror.gcr.io/apache/kafka:4.0.0` to the JVM image preload list in
  `.github/workflows/ci.yml`.
- New test path `jvm_consumer_group_next_gen` runs in `broker-jvm-acceptance`
  via existing `--include-ignored` mechanism.

## Acceptance gates

1. All unit + integration + JVM acceptance tests pass under
   `cargo test --workspace --include-ignored`.
2. `kafka-consumer-groups --describe --group <next-gen>` against the JVM
   admin tool returns sensible output (JVM test 4).
3. Coexistence test (JVM test 6) proves no regression in classic group
   behavior.
4. `cargo clippy --workspace --all-targets` clean; `cargo fmt --check`
   clean; no codegen drift.

## Follow-up slices (not in 64a)

- **64b** — Rack-aware `UniformAssignor`; more JVM acceptance coverage
  (subscription regex, instance_id static membership round-trip).
- **64c** — Server-side assignor plugin point exposed for external
  implementations.
- **64d** — Group migration (classic → next-gen) via
  `group.consumer.migration.policy`.
- **64e** — Operator-roadmap follow-up: `KafkaTopic`/`KafkaUser` field for
  group-type pinning, if user demand emerges.
