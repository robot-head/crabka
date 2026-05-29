# Roadmap — classic ↔ next-gen consumer-group migration (KIP-848 slice 64d)

**Status:** roadmap (architecture decision + decomposition; individual slices
get their own spec → plan → implement cycles)
**Date:** 2026-05-29
**Roadmap:** the "Group migration policy classic → next-gen (64d)" item
deferred by slice 64a and its follow-up. Builds on slice 64e
(JVM-client engagement) which must land first.

## Goal

Support **bidirectional online migration** between the classic consumer-group
protocol (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) and the KIP-848
next-gen protocol (`ConsumerGroupHeartbeat`), governed by a Kafka-compatible
`group.consumer.migration.policy` (`disabled` / `upgrade` / `downgrade` /
`bidirectional`). A single group may hold **both** classic and consumer-protocol
members simultaneously during a rolling client upgrade or downgrade, sharing one
epoch and one coherent partition assignment, with no partition gaps or overlaps
and no consumption stall.

This matches Apache Kafka 4.0's behavior, which the JVM admin tools
(`kafka-consumer-groups --describe`, `--list`) and the GA clients rely on. Per
CLAUDE.md, **Kafka compatibility is the constraint that matters**; where the
classic and next-gen models differ, the unified design follows Kafka's.

## Architecture decision: unified `GroupCoordinator` rewrite

Crabka today runs **two independent coordinators**:

- **Classic** (`coordinator/group.rs`, `coordinator/mod.rs`): a `GroupManager`
  owning `Arc<GroupHandle>` per group — a `Mutex<Group>` plus `join_complete` /
  `sync_complete` `Notify`s used to park `JoinGroup`/`SyncGroup` handlers.
  Generation-based, assignment carried as opaque `SyncGroup` blobs, the
  client-side assignor decides partitions. Persisted as `__consumer_offsets`
  key v2 `GroupMetadata`.
- **Next-gen** (`coordinator/next_gen/`): a `NextGenCoordinator` owning one
  actor (tokio task + mpsc) per group. Epoch-based, server-side assignor,
  topic-ID assignment, incremental reconciliation. Persisted as key
  v3/v5/v6/v7/v8 records.

A permanent first-record-wins type lock (`group_types: DashMap<String,
GroupType>`, set via `mark_classic` in `join_group.rs:66` and `mark_next_gen`
in the heartbeat handler) keeps every group on exactly one coordinator for life.
That lock is precisely what forbids mixed membership.

**Decision (chosen 2026-05-29): collapse both coordinators into a single
`GroupCoordinator` that natively speaks both protocols**, mirroring Apache
Kafka's `GroupMetadataManager` / `GroupCoordinatorShard`. One `Group` model
represents members of either protocol; one assignment engine produces a target
that is expressed to consumer members as `ConsumerGroupHeartbeat.assignment`
(topic-ID) and to classic members as a translated `ConsumerProtocolAssignment`
blob returned via `SyncGroup`.

### Why the rewrite over a bridge or actor-hosted port

Crabka is **greenfield and undeployed** — there is no running cluster whose
classic coordinator must be preserved bit-for-bit, and CLAUDE.md explicitly
favors changing interfaces over carrying compatibility shims. The two
alternatives considered:

- **Two-coordinator bridge** (keep both, synchronize a live classic `Group` and
  a live next-gen actor for one group_id): rejected — synchronizing two
  independent state machines across a mutex and an mpsc actor, sharing one
  epoch, is the highest concurrency-bug surface of the three and leaves two
  persistence code paths to keep consistent.
- **Next-gen actor hosts classic members** (route classic RPCs into the
  next-gen actor as an incremental step): a viable middle path, but it grows
  the actor into a second protocol's state machine while the classic
  `GroupManager` still exists, leaving the codebase with two group models
  indefinitely. The unified rewrite reaches the same end state with one model.

The unified coordinator is the larger up-front investment but the only option
that ends with a **single** group model and a **single** persistence path —
which is also what makes bidirectional conversion tractable (conversion becomes
a type-field change on one in-memory `Group`, not a hand-off between subsystems).

### Risk & mitigation

The rewrite touches the already-working classic path (the `jvm_acceptance`
classic-consumer tests, KIP-345 static membership, cooperative-sticky rebalance,
KIP-394 bootstrap). Mitigation: the existing classic + next-gen unit,
integration, and JVM suites are the regression gate. Slice B (below) is a
**behavior-preserving port** — no migration, no semantic change — validated by
those suites passing unmodified before any migration logic is added.

## Persistence & wire compatibility constraints

- `__consumer_offsets` key versions are globally assigned by Kafka and must not
  drift: `0/1` `OffsetCommit`, `2` classic `GroupMetadata`, `3`
  `ConsumerGroupMetadata`, `5` `ConsumerGroupMemberMetadata`, `6`
  `ConsumerGroupTargetAssignmentMetadata`, `7`
  `…TargetAssignmentMember`, `8` `…CurrentMemberAssignment`. Crabka's
  `next_gen::persistence::NextGenKey` already aligns. A migrated (upgraded)
  group is persisted as a consumer group (k3 + k5…), and conversion writes a
  **tombstone** for the prior type's records (e.g. tombstone the k2
  `GroupMetadata` on upgrade) so bootstrap replay reconstructs the correct
  current type.
- A classic member living inside an upgraded consumer group must be persisted
  with enough classic-protocol state (supported protocols, session timeout,
  last assignment) to **downgrade** losslessly. Kafka stores this in
  `ConsumerGroupMemberMetadataValue`'s classic-member sub-fields; the unified
  model must carry the same.
- Bidirectional conversion must preserve **committed offsets** (k0/k1 records
  are group-scoped and protocol-agnostic — they survive a type flip untouched).

## Migration triggers & policy (Kafka-faithful)

- **Upgrade** (`policy ∈ {upgrade, bidirectional}`): a classic group receives a
  `ConsumerGroupHeartbeat`. Convert iff the group is **convertible** — every
  current classic member advertises the `consumer` embedded protocol (i.e. its
  `protocols` include the consumer-protocol name with parseable subscription
  metadata) so its subscription survives translation. Otherwise the heartbeat
  is rejected and the joining consumer stays classic (or fails per Kafka's
  behavior). `policy = disabled` or `downgrade` → reject the heartbeat.
- **Downgrade** (`policy ∈ {downgrade, bidirectional}`): a consumer group
  receives a classic `JoinGroup`. Convert back: consumer members are re-expressed
  as classic members; the server-computed target becomes the seed assignment.
  `policy = disabled` or `upgrade` → reject the `JoinGroup`.
- The mutable group type replaces the permanent `mark_classic`/`mark_next_gen`
  lock and is the source of truth persisted via the record schema above.

## Decomposition

Each slice is its own spec → plan → implement cycle. A–F are ordered; later
slices depend on earlier ones.

| Slice | Scope | Depends on |
|-------|-------|------------|
| **A — JVM-engagement fix (64e)** | First-join member-ID semantics; un-ignore the 4 JVM tests; restore CI. Small, diagnosed, verified. Specced separately (`…-kip-848-jvm-engagement-64e-design.md`). | — |
| **B — Unified coordinator skeleton** | Introduce the single `GroupCoordinator` and unified `Group` model capable of representing classic *and* consumer members; port the classic `JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup` handlers onto it; route `ConsumerGroupHeartbeat` into the same coordinator. **Behavior-preserving** — groups remain single-type; existing classic + next-gen + JVM suites pass unmodified. The big architectural lift. | A |
| **C — Migration policy + dynamic type** | Add `group.consumer.migration.policy` config (disabled/upgrade/downgrade/bidirectional, default matching Kafka). Replace the permanent type lock with a mutable, policy-governed type persisted via record schema (k2 vs k3) with tombstone-on-convert. Convertibility predicate. No live conversion yet. | B |
| **D — Upgrade path (classic → consumer)** | In-place conversion of a classic group on incoming heartbeat: classic members become classic members *inside* the consumer group; one epoch; server target translated to `ConsumerProtocolAssignment` blobs delivered via `SyncGroup`; persist conversion. Live mixed membership. | C |
| **E — Downgrade path (consumer → classic)** | Reverse conversion on incoming classic `JoinGroup`; re-express consumer members as classic; seed assignment from the server target; persist. | D |
| **F — Rolling-migration JVM acceptance** | cp-kafka classic consumer + apache/kafka 4.0 consumer-protocol consumer in the **same group**, rolled both directions under `policy=bidirectional`. Assert no partition gaps/overlaps and continuous consumption across the flip. | E |

## Open questions (resolve during the per-slice brainstorms)

- **Coordinator concurrency model for the unified `Group`.** Actor-per-group
  (extends the next-gen model; aligns with the project memory that the broker is
  serial per-connection and parkable streams want their own path) vs.
  `Mutex<Group>` + `Notify` (extends the classic model). The classic protocol's
  parking semantics (`JoinGroup`/`SyncGroup` block until a rebalance boundary)
  must be expressible either way; the actor model needs an explicit
  park/wake message protocol. Decide in Slice B.
- **Default `group.consumer.migration.policy`.** Match the exact Apache Kafka
  4.0 default (verify empirically against the cp-kafka/apache image rather than
  the wiki). Decide in Slice C.
- **Convertibility when classic members use a non-consumer assignor** (e.g. a
  custom `PartitionAssignor` with no consumer-protocol embedding). Kafka refuses
  the upgrade; confirm the exact rejection error and client-visible behavior.
  Decide in Slice D.
- **Static membership (KIP-345) across a flip.** `group.instance.id` identity
  must survive upgrade/downgrade so a static member keeps its assignment. Decide
  in Slice D/E.

## Acceptance (program-level, realized across D–F)

- A group created by classic consumers, then joined incrementally by
  `group.protocol=consumer` consumers under `policy=bidirectional`, migrates
  with no partition gap/overlap and no consumption stall — verified with real
  JVM clients in Slice F.
- The reverse (consumer group joined by classic consumers) likewise.
- `kafka-consumer-groups --describe`/`--list` report the migrating group
  coherently throughout.
- `policy=disabled` reproduces today's hard separation (the 64e
  `coexists_with_classic` two-separate-groups behavior).
