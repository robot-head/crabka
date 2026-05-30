# KIP-932 Queues for Kafka (Share Groups) — Design

**Date:** 2026-05-30
**Status:** Approved (roadmap + Slice A spec). Slices B–F are roadmap-level only.
**KIP:** [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
**Target compatibility:** Apache Kafka 4.3.0 (share groups: early access 4.0, preview 4.1, GA 4.2).

## Background

Share groups give Kafka queue semantics: many consumers in a *share group*
cooperatively consume a topic's partitions, with per-record acknowledgement and
redelivery instead of per-partition offset commits. Records are handed out under
a time-limited *acquisition lock*; a consumer Accepts, Releases, or Rejects each
record; unacknowledged records are redelivered up to a delivery-attempt limit.

KIP-932 introduces three new server-side subsystems:

- **Share group coordinator** — membership and assignment. This *is* the
  existing (KIP-848) group coordinator, extended to handle a `share` group type.
  Group metadata persists to `__consumer_offsets`.
- **Share coordinator (persister)** — durably persists per-share-partition
  delivery state (the Share-Partition Start Offset and the in-flight state map)
  to a new internal topic `__share_group_state`.
- **Share-partition leader** — co-located with the topic-partition log leader;
  an in-memory engine that materialises records from the log, hands them out
  under acquisition locks, tracks delivery counts, applies acknowledgements, and
  flushes durable deltas to the share coordinator.

### What already exists in Crabka (free foundation)

- **All 24 share-group wire codecs are generated** into
  `crates/protocol/generated/` as a byproduct of the blanket protocol-coverage
  codegen. The messages decode/encode today; nothing handles them.
- The **KIP-848 next-gen coordinator** actor framework
  (`crates/broker/src/coordinator/next_gen/`): `NextGenCoordinator` with a
  per-group tokio actor (`group_actor.rs`), `GroupState`, a `reconciler`, an
  `Assignor` trait (`UniformAssignor`, `RangeAssignor`), an `offsets_log`
  abstraction, and `__consumer_offsets` bootstrap + replay.
- The `ConsumerGroupHeartbeat` handler
  (`crates/broker/src/handlers/consumer_group_heartbeat.rs`) as a template.
- The handler dispatch table (`crates/broker/src/handlers/mod.rs`).
- The incremental-fetch-session machinery and long-poll fetch path (reused in
  Slice C, not Slice A).

### What is missing (all functional behavior)

Share error codes, ApiVersions advertisement of the share RPCs, the
`share.version` feature gate, the share group coordinator (membership), the
share coordinator/persister, and the share-partition leader. "Completely
unimplemented" is accurate at the behavior level: the bytes decode, but the
broker does nothing with them.

### Confirmed wire facts

Client-facing RPCs and ApiKeys (from `crates/protocol/schemas/*.json`):

| RPC | ApiKey | validVersions | Slice |
|-----|:------:|:-------------:|:-----:|
| ShareGroupHeartbeat | 76 | 1 | **A** |
| ShareGroupDescribe | 77 | 1 | **A** |
| ShareFetch | 78 | 1–2 | C |
| ShareAcknowledge | 79 | 1–2 | C |
| InitializeShareGroupState | 83 | 0 | B |
| ReadShareGroupState | 84 | 0 | B |
| WriteShareGroupState | 85 | 0–1 | B |
| DeleteShareGroupState | 86 | 0 | B |
| ReadShareGroupStateSummary | 87 | 0–1 | B |
| DescribeShareGroupOffsets | 90 | 0–1 | D |
| AlterShareGroupOffsets | 91 | 0 | D |
| DeleteShareGroupOffsets | 92 | 0 | D |

New error codes (from Kafka `Errors.java`): `INVALID_RECORD_STATE(121)`,
`SHARE_SESSION_NOT_FOUND(122)`, `INVALID_SHARE_SESSION_EPOCH(123)`,
`FENCED_STATE_EPOCH(124)`, `SHARE_SESSION_LIMIT_REACHED(133)`.

## Roadmap

The feature is multi-month and spans three new subsystems. It is decomposed into
six slices (mirroring the KIP-848 roadmap pattern in this repo). This document
specifies **Slice A** in detail; B–F are roadmap-level scope only and get their
own spec when reached.

| Slice | Name | Delivers | Key new pieces |
|------|------|----------|----------------|
| **A** | Membership foundation | A client can join a share group and receive a partition assignment; observable via `ShareGroupDescribe`. No record delivery. | `share.version` gate, ApiVersions advertisement, share error codes, `ShareGroupHeartbeat`, `ShareGroupDescribe`, ShareGroup\* records in `__consumer_offsets`, a share assignor |
| **B** | Share coordinator (persister) | Durable per-share-partition state, independently testable. | `__share_group_state` topic, `ShareSnapshot`/`ShareUpdate` records, persister RPCs 83–87, snapshot/prune loop, `FindCoordinator SHARE(2)` |
| **C** | Share-partition leader + ShareFetch/Acknowledge | End-to-end consume+ack on a single broker. | acquisition state machine (Available/Acquired/Acknowledged/Archived, locks, delivery counts, SPSO/SPEO), share sessions, `ShareFetch(78)`/`ShareAcknowledge(79)`, leader↔persister wiring |
| **D** | Admin offsets surface | Operators inspect/reset queue head. | `DescribeShareGroupOffsets(90)`, `AlterShareGroupOffsets(91)`, `DeleteShareGroupOffsets(92)`, Initialize/Delete lifecycle |
| **E** | Native share consumer client | `crabka-client-consumer` drives a share group. | client heartbeat loop, share-fetch+ack, poll API, implicit/explicit ack modes |
| **F** | GA parity extras | 4.3 fidelity. | `RENEW` ack type (KIP-1222), read_committed isolation, lag persistence/metrics, full config bounds |

---

# Slice A — Membership foundation

## Goal

A client can join a share group via `ShareGroupHeartbeat(76)`, converge on a
group epoch, and receive a partition `Assignment`; a second member triggers
reconciliation; `ShareGroupDescribe(77)` reflects current membership; members
leave via `MemberEpoch = -1` or session-timeout eviction; group state survives a
broker restart via `__consumer_offsets` replay. **No record delivery** (that is
Slices B/C).

## Non-goals (Slice A)

- No `ShareFetch`/`ShareAcknowledge`, no acquisition state, no SPSO/SPEO.
- No share coordinator / `__share_group_state` topic / persister RPCs.
- No `FindCoordinator SHARE(2)` — see "Coordinator discovery" below.
- No native Rust share consumer client (Slice E).
- No JVM differential test gating this slice (see "Testing").

## Coordinator discovery

Share-group *membership* is served by the ordinary group coordinator, located by
clients via the existing `FindCoordinator GROUP(0)` path keyed by `group.id`,
which already returns this broker. The new `FindCoordinator SHARE(2)` coordinator
type locates the **persister** (keyed by `groupId:topicId:partition`) and is
therefore deferred to Slice B. **Slice A makes no change to
`handlers/find_coordinator.rs`.** (To be re-confirmed against the
`KafkaShareConsumer` discovery path during planning; if membership turns out to
require a distinct lookup, it is a small additive change.)

## Architecture

Share groups become a new **group variant** inside the existing `next_gen`
coordinator, reusing its actor-per-group model, bootstrap/replay, offsets-log,
and reconciler:

```
ShareGroupHeartbeat(76) ─┐
ShareGroupDescribe(77)  ─┼─► handlers/ ──► NextGenCoordinator.get_or_create(group_id)
                         │                    └─► GroupActor { variant: Share(ShareGroupState) }
                         │                          ├─ membership: members, group epoch, member epochs
                         │                          ├─ reconciler ──► ShareGroupAssignor (overlapping)
                         │                          └─ persists ShareGroup* records ──► __consumer_offsets
```

A group's variant (Consumer vs Share) is fixed on first heartbeat. An RPC of the
wrong variant against an existing group returns `GROUP_ID_NOT_FOUND` (Kafka's
behavior — a consumer-protocol RPC against a share group, or vice versa, is
rejected).

## Components

### 1. `coordinator/next_gen/share/` module

- `ShareGroupState` — members map, group epoch, per-member epoch, subscribed
  topic set, target assignment, current assignments. Mirrors the consumer-group
  next-gen state machine **minus all offset machinery** (no committed offsets, no
  offset fetch/commit).
- Reconciliation: on membership/subscription/topic-metadata change, bump group
  epoch, run the assignor, persist the new target assignment, and converge each
  member's epoch toward the group epoch over subsequent heartbeats (same
  epoch-bump/target-assignment model as consumer groups).
- Session expiry: a per-member deadline (`group.share.session.timeout.ms`);
  on expiry the actor evicts the member, bumps the group epoch, and reconciles
  survivors. Reuses the next-gen actor's existing timer/expiry mechanism.

The `GroupActor` gains a `variant` field:
`enum GroupVariant { Consumer(ConsumerGroupState), Share(ShareGroupState) }`.
`ShareGroupHeartbeat` messages route to the `Share` variant; existing consumer
messages to `Consumer`. A new `GroupActorMessage::ShareHeartbeat { request, reply }`
(and `ShareDescribe`) is added.

### 2. `ShareGroupAssignor`

Implements KIP-932 `SimpleAssignor` semantics: distribute the subscribed
topic-partitions across members **without exclusive ownership**. A partition may
be assigned to multiple members; when members > partitions, sharing is the
expected outcome (every member still gets at least one partition where possible).
Kept as a **dedicated type**, not an impl of the exclusive consumer `Assignor`
trait, because the share model permits overlap that the consumer trait's
contract forbids. Exact assignment distribution is **not protocol-critical**
(clients consume whatever partitions they are handed; there is no client-side
assignor), so we match SimpleAssignor's documented round-robin/hash behavior
without pursuing byte-parity with the JVM assignor.

### 3. Handlers

- `handlers/share_group_heartbeat.rs` (apiKey 76): decode
  `ShareGroupHeartbeatRequest` v1, ACL-check (Read on the group resource),
  route to the actor via a oneshot reply, encode `ShareGroupHeartbeatResponse`.
  Modeled on `consumer_group_heartbeat.rs`.
- `handlers/share_group_describe.rs` (apiKey 77): decode, ACL-check (Describe on
  the group), query the coordinator for members/assignment, encode
  `ShareGroupDescribeResponse`.
- Register both in `handlers/mod.rs` `build_table()`.

ACL enforcement follows the principal/authorize pattern in `offset_commit.rs`.

### 4. Persistence (records in `__consumer_offsets`)

Hand-written key/value codecs in `coordinator/next_gen/share/persistence.rs`,
following Kafka's coordinator-record layouts, written to and replayed from
`__consumer_offsets` (no new topic in Slice A). Record types:

- `ShareGroupMetadata` — group epoch.
- `ShareGroupMemberMetadata` — member id, rack id, client id/host, subscribed
  topics.
- `ShareGroupPartitionMetadata` — subscribed-topic partition counts /
  initialized partitions (the `__consumer_offsets` flavor; the
  share-coordinator-side `ShareGroupStatePartitionMetadata` with `DeletingTopics`
  arrives with Slice B's lifecycle wiring).
- `ShareGroupTargetAssignmentMetadata` — assignment epoch.
- `ShareGroupTargetAssignmentMember` — per-member target assignment.
- `ShareGroupCurrentMemberAssignment` — per-member current assignment + epoch.

Because Crabka is greenfield and the only broker, `__consumer_offsets` record
formats need only be self-consistent for Crabka's own replay; we follow Kafka's
schemas for fidelity but do not require byte-parity with the JVM on-disk format.
Records are appended through the existing `offsets_log` abstraction and replayed
by extending the bootstrap key-dispatch to recognise the share record keys.

### 5. Feature gate

- A `share.version` cluster feature (0 = off, 1 = on). Greenfield default: on
  (matches 4.2+ GA). No migration shims.
- `group.share.enable` broker/group config (master switch under `share.version`).
- Membership configs: `group.share.heartbeat.interval.ms`,
  `group.share.session.timeout.ms` (+ broker min/max bounds
  `group.share.min/max.session.timeout.ms`), `group.share.max.groups`,
  `group.share.max.size` (max members per group).
- When the feature is disabled, share RPCs return `UNSUPPORTED_VERSION`.

### 6. ApiVersions

Advertise apiKeys **76 and 77** (v1) when the share feature is enabled.
ShareFetch/Acknowledge and the persister/admin RPCs stay unadvertised until their
slices land. (Advertising a subset is correct for an incremental build; a real
share consumer cannot make progress past join until Slice C, which is expected.)

### 7. Error codes

Wire all five share error codes into `crates/protocol/src/error.rs` now (cheap,
non-conflicting leaf change): `INVALID_RECORD_STATE(121)`,
`SHARE_SESSION_NOT_FOUND(122)`, `INVALID_SHARE_SESSION_EPOCH(123)`,
`FENCED_STATE_EPOCH(124)`, `SHARE_SESSION_LIMIT_REACHED(133)`. Slice A actively
exercises the reused epoch-fencing codes `FENCED_MEMBER_EPOCH` /
`STALE_MEMBER_EPOCH` for heartbeat epoch validation; the five new codes are
defined now and consumed by later slices.

## Data flow (heartbeat lifecycle)

1. Member sends `ShareGroupHeartbeat` with empty `MemberId`, `MemberEpoch = 0`,
   and `SubscribedTopicNames`.
2. Coordinator assigns a UUID `MemberId`, adds the member, bumps the group epoch,
   persists `ShareGroupMemberMetadata` + `ShareGroupMetadata`.
3. Reconciler runs `ShareGroupAssignor`, persists
   `ShareGroupTargetAssignment*`.
4. Response carries `MemberId` (on first reply), `MemberEpoch`,
   `HeartbeatIntervalMs`, and `Assignment { TopicId, Partitions[] }` — **topic-
   partitions only, no offsets**.
5. Subsequent heartbeats converge the member epoch to the group epoch; the
   member acks its assignment; `ShareGroupCurrentMemberAssignment` is persisted.
6. `MemberEpoch = -1` → member leaves; coordinator removes it, bumps group epoch,
   reconciles survivors.
7. Missed session timeout → actor evicts the member identically.

Epoch validation: a heartbeat with a stale/fenced member epoch returns
`FENCED_MEMBER_EPOCH` / `STALE_MEMBER_EPOCH`.

## Error handling

- Share feature disabled → `UNSUPPORTED_VERSION`.
- Wrong group variant (consumer RPC vs share group) → `GROUP_ID_NOT_FOUND`.
- Unknown/fenced member epoch → `FENCED_MEMBER_EPOCH` / `STALE_MEMBER_EPOCH`.
- Group at `group.share.max.groups` or `group.share.max.size` → appropriate
  Kafka error (`GROUP_MAX_SIZE_REACHED` for member cap).
- Coordinator still loading `__consumer_offsets` → `COORDINATOR_LOAD_IN_PROGRESS`.
- Not the coordinator for the group → `NOT_COORDINATOR`.

## Concurrency note

Per the repo's "broker is serial per-connection" constraint: `ShareGroupHeartbeat`
is a short request/response (not a long-poll), so it does not head-of-line-block
the connection. The parkable stream concern applies to `ShareFetch` in Slice C,
not Slice A.

## Testing

- **Unit:** assignor distribution including members > partitions (overlap);
  epoch bump/reconcile; session-timeout eviction; ShareGroup\* record
  encode/decode round-trips.
- **Integration** (`crates/broker/tests/share_groups.rs`, using the raw-RPC
  low-level client as `consumer_group_next_gen.rs` does):
  - single member joins → receives a non-empty assignment covering the topic;
  - second member joins → both reconciled, group epoch advanced;
  - `ShareGroupDescribe` reflects current members + assignments;
  - `MemberEpoch = -1` leave removes the member;
  - session-timeout eviction removes a silent member;
  - restart → membership/assignment recovered from `__consumer_offsets` replay.
- **JVM differential:** deferred to Slice C. A real `KafkaShareConsumer` cannot
  progress past join without `ShareFetch`, and `kafka-share-groups.sh --describe`
  leans on state produced in B/C, so an A-only JVM test would be brittle.

## Acceptance gate (Slice A)

1. `cargo fmt --check` clean.
2. `cargo clippy --workspace --all-targets` clean.
3. `cargo test --workspace` green (new unit + integration tests included).
4. No codegen drift (`regenerate.sh` + `git diff` clean).
5. A two-member share group joins, reconciles, is described, members leave, and
   state survives restart — all exercised by `tests/share_groups.rs`.
6. Share RPCs (76, 77) advertised in ApiVersions only when `share.version` is on;
   return `UNSUPPORTED_VERSION` when off.

## File-set sketch (for parallel-batch implementation)

Non-overlapping task groups (per CLAUDE.md execution guidance):

- **Wire/error (leaf):** `crates/protocol/src/error.rs` (+ codes), ApiVersions
  advertisement table.
- **Coordinator core:** `crates/broker/src/coordinator/next_gen/share/` (new
  module: state, reconciler hook, assignor), `group_actor.rs` (variant enum +
  message arms), `next_gen/mod.rs` (variant routing).
- **Persistence:** `crates/broker/src/coordinator/next_gen/share/persistence.rs`
  (+ bootstrap replay key-dispatch).
- **Handlers:** `crates/broker/src/handlers/share_group_heartbeat.rs`,
  `share_group_describe.rs`, `handlers/mod.rs` registration.
- **Config/feature:** broker config additions + `share.version` feature
  definition.
- **Tests:** `crates/broker/tests/share_groups.rs`.

The coordinator-core, group_actor, and next_gen/mod edits touch shared files and
must be sequenced or assigned to one implementer; wire/error, config, and the
handler files are largely independent and can run in parallel.
