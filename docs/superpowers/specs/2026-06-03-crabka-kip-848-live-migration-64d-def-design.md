# KIP-848 live classic ↔ next-gen migration — full program (slices 64d-D/E/F)

**Status:** design
**Date:** 2026-06-03
**Roadmap:** `2026-05-29-crabka-classic-nextgen-migration-roadmap-design.md`
(slices D = upgrade, E = downgrade, F = JVM acceptance).
**Supersedes:** `2026-05-30-crabka-kip-848-upgrade-64d-d-design.md` (the
slice-D-only design). This spec covers D, E, and F as one cycle and reconciles
two points with the earlier D-only doc — see *Reconciliation* below.

Builds on slice B (unified `GroupCoordinator`, per-group actor) and slice C
(`group.consumer.migration.policy` + the convertibility predicate and conversion
machinery in `coordinator/unified/migration.rs`, currently `#![allow(dead_code)]`
because nothing calls it).

## Goal

Support **bidirectional online migration** between the classic consumer-group
protocol (`JoinGroup`/`SyncGroup`/`Heartbeat`/`LeaveGroup`) and the KIP-848
next-gen protocol (`ConsumerGroupHeartbeat`), governed by
`group.consumer.migration.policy` (`disabled` / `upgrade` / `downgrade` /
`bidirectional`, default `bidirectional` — already wired in slice C, matching
Apache Kafka 4.0). A single group may hold **both** classic and consumer-protocol
members at once during a rolling client upgrade or downgrade, sharing one epoch
and one coherent partition assignment, with no partition gap/overlap and no
consumption stall. The flip is durable across a coordinator failover, and the
JVM admin tools (`kafka-consumer-groups --describe`/`--list`) report the group
coherently throughout.

Per CLAUDE.md, **Kafka compatibility is the constraint that matters**: where the
classic and next-gen models differ, this follows Apache Kafka 4.0, verified
empirically against `mirror.gcr.io/apache/kafka:4.0.0` rather than the wiki.

## What already exists (slices B + C)

- A unified `Group { kind: GroupKind::Classic(ClassicState) | Consumer(ConsumerState), committed_offsets }`
  container, single-kind for its lifetime (`coordinator/unified/group.rs`).
- A per-group actor (`GroupActorHandle`, tag `GroupKindTag::{Classic,Consumer}`)
  whose mailbox (`GroupActorMessage`) **already carries both** RPC families —
  classic `ClassicJoin/Sync/Heartbeat/Leave` *and* next-gen `Heartbeat`. The
  actor currently rejects the "wrong" family with an error code rather than
  serving it (`actor.rs:288–439`).
- A registry-level type lock: `get_or_create(kind)` returns `None` on a kind
  mismatch (`mod.rs:255–298`), which is what handlers turn into a wrong-protocol
  rejection today.
- Slice-C machinery, trigger-less: `ConsumerGroupMigrationPolicy` (`config.rs`),
  `classic_is_convertible`, `consumer_is_convertible`, `convert_classic_to_consumer`
  (produces a `ConsumerState` whose ex-classic members carry a
  `ClassicMemberFacade`), and `target_to_consumer_assignment`
  (`migration.rs`). `MemberState.classic: Option<ClassicMemberFacade>` and the
  facade struct already exist (`consumer_state.rs`).

Live migration is therefore **not** a rewrite: stop rejecting the foreign family,
convert-then-serve instead, and persist + replay the flip.

## Trigger semantics (Kafka-faithful)

The group **type is re-evaluated after every membership change**, not on a single
RPC. The precise rules (to be confirmed empirically against `mirror.gcr.io/apache/kafka:4.0.0`):

- **A consumer-type group can host classic members.** A classic `JoinGroup`
  arriving at a consumer group does **not** downgrade it — the member joins as a
  *hosted classic member* via the facade. Mixed membership lives under the
  **consumer** type.
- **Upgrade** (classic → consumer type): fires when a `ConsumerGroupHeartbeat`
  arrives for a classic group **and** `policy.allows_upgrade()` **and** the
  classic group is stable + convertible (every current member's selected protocol
  decodes as a `ConsumerProtocolSubscription` — the slice-C predicate). Otherwise
  the heartbeat is rejected and the joining consumer stays classic / fails per
  Kafka.
- **Downgrade** (consumer → classic type): fires when the **last consumer-protocol
  member leaves** a consumer group that still has classic members **and**
  `policy.allows_downgrade()`. It is *not* triggered by a classic join.

Under `policy=disabled` neither flip ever fires, reproducing today's hard
separation (the 64e `coexists_with_classic` two-separate-groups behavior).

## Architecture

### 1. Routing & in-actor kind flip

- **Drop the registry-level type lock.** Replace `get_or_create_classic` /
  `get_or_create_consumer` (`None`-on-mismatch) with a single
  `get_or_create_group(group_id)` that returns the one actor regardless of its
  current kind. `GroupActorHandle.kind` stops being a router/lock; it is at most
  an initial hint. Handlers (`join_group.rs:59`, `consumer_group_heartbeat.rs:49`,
  `sync_group.rs`, `leave_group.rs`, `offset_fetch.rs`, `offset_commit.rs`,
  `txn/handlers/txn_offset_commit.rs`) stop treating `None` as wrong-protocol.
- **The actor loop branches on live `group.kind`, not the captured `kind`.** The
  tick handler's `match kind { … expect("consumer kind") }` (`actor.rs:441–464`)
  and the `tick_period` selection (`actor.rs:277–280`) must consult
  `group.is_consumer()` / `group.is_classic()` each iteration, or the actor panics
  the instant a group flips. A single unified tick dispatches on the live kind.
  This is the principal mechanical refactor of slice D.
- **Conversion happens inside the handler arm.** In the `Heartbeat` (next-gen)
  arm, when `group.kind == Classic`: check `policy.allows_upgrade()` +
  `classic_is_convertible`; if yes, replace `group.kind` with
  `Consumer(convert_classic_to_consumer(state))`, persist the conversion batch
  (§Persistence), then fall through to the normal next-gen heartbeat path so the
  joining consumer member is added and the reconciler runs. If not
  convertible/allowed, return the Kafka rejection code (likely
  `GROUP_ID_NOT_FOUND`; pin empirically).

**Alternative considered & rejected:** respawn a fresh actor of the target kind
seeded with the converted state. Rejected — it drops in-flight parked
`JoinGroup`/`SyncGroup` waiters and reintroduces the spawn race the unified model
removed. In-place flip keeps the mailbox and parked waiters intact, and the actor
is single-threaded over its mailbox so the flip is automatically serialized
against every other RPC for that group (no lock dance).

### 2. Serving classic members inside a consumer group (the heart of D)

A hosted classic member keeps speaking `JoinGroup`/`SyncGroup`/`Heartbeat`; the
coordinator maps it onto the epoch-based reconciler:

- **Assignment source — unchanged reconciler.** `reconcile_if_dirty` already runs
  the assignor over *all* `GroupState.members`, facade members included (their
  `subscribed_topic_names` are populated by `convert_classic_to_consumer`). A
  facade member gets a server-computed target like any consumer member; no
  reconciler change.
- **`SyncGroup`** returns the member's current target translated to a
  `ConsumerProtocolAssignment` blob via `target_to_consumer_assignment`, caches it
  in `facade.last_synced_assignment`, and clears `facade.awaiting_sync`.
- **`Heartbeat`** returns `REBALANCE_IN_PROGRESS` when the member owes a rejoin
  (target epoch advanced / `awaiting_sync`), else `NONE`. This is what makes a
  classic client re-`JoinGroup`+`SyncGroup` to pick up a new assignment. It also
  refreshes `last_seen`; a hosted classic member expires on its own
  `facade.session_timeout`.
- **`JoinGroup`** (rejoin, or a *new* classic member joining an already-upgraded
  group) does **not** run the classic leader-election/vote — the group is
  server-assigned. It adds/refreshes the member as a facade member, returns a
  trivial single-member view (the member as its own leader, `consumer` protocol)
  with `generation_id` mapped from the group epoch; the real assignment arrives on
  the subsequent `SyncGroup`. Mirrors how Kafka serves a classic member embedded
  in a `ConsumerGroup`.
- **`generation_id ↔ group_epoch`.** `facade.generation_id` advances with the
  group epoch, so classic clients observe monotonic generations across rebalances
  and across the flip itself.

### 3. Downgrade (slice E) — the mirror

When the last consumer-protocol member departs a consumer group that still has
classic members and `policy.allows_downgrade()`, convert in place to a classic
group:

- Build a `ClassicState` from the surviving facade members: each becomes a classic
  `Member` restored from its facade (`supported_protocols`, `session_timeout`,
  `group_instance_id`, last assignment). `generation_id` seeds from the group
  epoch so classic generations stay monotonic.
- The server-computed target becomes the seed assignment the classic members hold
  (no spurious revoke on the flip).
- Replace `group.kind` with `Classic(state)`; persist the downgrade batch
  (§Persistence). Committed offsets (k0/k1) ride on the `Group` container,
  untouched.

Downgrade re-uses `consumer_is_convertible` (always `true`) for symmetry; the real
work is the re-expression and persistence.

## Persistence & bootstrap replay

`OffsetsLog::append` takes one `RecordBatch` that may carry many records, and a
tombstone is a record with `value: None` (`offsets_log.rs`, `actor.rs` `into_batch`
+ `PendingRecords`). So each conversion is one **atomic** batch:

- **Upgrade (classic → consumer):** tombstone k2 `GroupMetadata`; write k3
  `ConsumerGroupMetadata`, and k5 member-metadata + k8 current-assignment per
  member, + k6/k7 target assignment. One batch.
- **Downgrade (consumer → classic):** tombstone k3 + k5/k6/k7/k8 for every member;
  write k2 `GroupMetadata` with the re-expressed classic members and seed
  assignment. One batch.

### Schema change — k5 carries the classic facade

The k5 `MemberMetadataValue` (`persistence_next_gen.rs:122–135`) today holds only
next-gen fields. For a **lossless downgrade** after a coordinator failover, a
hosted classic member's classic state must be durable — exactly as Kafka stores
`ClassicMemberMetadata` inside `ConsumerGroupMemberMetadataValue`.

**Decision (per CLAUDE.md greenfield — change the schema, no compat shim):** add
an optional `classic_member_metadata` sub-struct to `MemberMetadataValue`
carrying `supported_protocols: Vec<(String, Bytes)>`, `session_timeout_ms: i32`,
and `last_synced_assignment: Bytes`. `None` = native consumer member; `Some` =
hosted classic member. Bootstrap reconstructs the `ClassicMemberFacade` from it.
This corrects the earlier D-only doc's "no schema change" claim, which held only
because it deferred downgrade.

### Replay correctness — the downgrade trap

`replay_next_gen_tombstone` for the k3 `GroupMetadata` key currently only **zeroes
`group_epoch`** (`mod.rs:587–621`); it does **not** drop the group from the
`seeds` map that `finalize_bootstrap` uses to decide "this group is next-gen"
(`bootstrap.rs:456–500`). So a downgraded group (k3 tombstoned, k2 later written)
would naively replay back as an *empty next-gen* group — wrong.

**Fix:** the k3 `GroupMetadata` tombstone must **remove the seed entry entirely**.
Because records replay in log order, the later-written k2 record then
reconstructs the group as classic. Mandatory test: an upgrade-then-downgrade
record stream replays back to a **classic** group with the correct members and
committed offsets intact.

### `describe` / `list` coherence

`list_groups` / `describe_group` (`mod.rs:434–469`) surface **only** classic
groups today (literal `TODO(64d-C+): surface consumer groups in admin APIs`).
Because migration makes a group's type mutable, `kafka-consumer-groups
--describe`/`--list` must report the group coherently throughout a flip. This
TODO is resolved as part of slice D/E.

## Edge cases & invariants

- **Static membership (KIP-345) across a flip.** `group.instance.id` continuity:
  `convert_classic_to_consumer` maps `group_instance_id → MemberState.instance_id`;
  downgrade restores it. A static member keeps its identity (and its assignment)
  across upgrade *and* downgrade. Explicit test both directions.
- **Non-convertible upgrade.** A classic group with a member whose selected
  protocol doesn't decode (custom assignor, no consumer embedding) → upgrade
  refused, group stays classic, the joining consumer gets Kafka's exact rejection
  code (pin empirically). The predicate already returns `false`; wire the
  rejection.
- **Empty-group flips.** An empty classic group receiving a consumer heartbeat
  upgrades trivially (predicate is `true` for empty). A consumer group emptied of
  consumer members but holding classic members downgrades.
- **Concurrent-flip safety.** The actor is single-threaded over its mailbox, so a
  flip is serialized against all other RPCs for that group; parked
  `JoinGroup`/`SyncGroup` waiters survive (in-place, not respawn).
- **Committed offsets.** k0/k1 records are group-scoped and protocol-agnostic;
  they survive both flips untouched (on the `Group` container).

## Validation

### In-process integration (slices D, E — the correctness gate, no Docker)

Drive a unified coordinator with a mix of classic (`ClassicJoin/Sync/Heartbeat`)
and next-gen (`Heartbeat`) messages against one `group_id`. Assert:

- upgrade flips the kind on the first consumer-protocol heartbeat; the hosted
  classic member's next `SyncGroup` returns a correct translated assignment; the
  consumer member gets a topic-ID assignment; the union of assignments covers the
  topic with no gap/overlap;
- a new classic member joining an already-upgraded group is hosted (no downgrade);
- downgrade fires when the last consumer member leaves; classic members are
  restored losslessly with their seed assignment;
- the §Replay trap test (upgrade → downgrade → replay = classic);
- static-member identity survives both flips;
- `policy=disabled` → no flip (reject), reproducing the two-separate-groups
  behavior.

### JVM acceptance (slice F — the README ⚠️→✅ gate)

New test in `crates/broker/tests/jvm_consumer_group_next_gen.rs`, single-broker
control-plane (runs locally on the Mac per project memory — consumer coordination
is control-plane, no inter-broker replication), `#[ignore = "requires Docker"]`.

A real `cp-kafka:7.4.0` **classic** consumer and a real `mirror.gcr.io/apache/kafka:4.0.0`
**`group.protocol=consumer`** consumer in the **same** group under
`policy=bidirectional`, on a 2+-partition topic. Assert both hold disjoint
partitions covering the whole topic; then roll one direction and back, asserting
continuous consumption and no gap/overlap, and that `kafka-consumer-groups
--describe` reports the group coherently.

**Riskiest unknown:** no existing harness runs *two concurrent* JVM consumers in
one group — today's tests run a single consumer to a timeout
(`jvm_consumer_group_next_gen.rs:104`, `jvm_acceptance.rs:370`). Add a helper that
launches each consumer via `spawn_blocking` with a generous `--timeout-ms` /
`--max-messages` budget so they overlap, asserting on the union/disjointness of
consumed partitions parsed from stdout. **If concurrent-container orchestration
proves flaky on the Mac, the in-process suite is the real correctness gate and the
JVM test is the interop proof** — flag that explicitly rather than let a flaky
test gate the slice (no silent cap on coverage).

## Reconciliation with the earlier D-only doc

`2026-05-30-crabka-kip-848-upgrade-64d-d-design.md` proposed (a) a
`protocols_accepted` set on the actor handle plus a `MaybeUpgrade` message routed
from the handler, and (b) "no schema change." This spec replaces both: (a) a
single `get_or_create_group` + in-actor conversion in the heartbeat arm (simpler,
keeps all routing in one place), and (b) a k5 schema extension, which is required
once downgrade (E) is in scope. The facade model, serving semantics, and atomic
k2-tombstone-on-upgrade are carried forward unchanged.

## Open questions — resolve empirically during implementation (per CLAUDE.md)

- Exact rejection error code when an upgrade is refused (non-convertible /
  `policy ∈ {disabled, downgrade}`): confirm against `mirror.gcr.io/apache/kafka:4.0.0`.
- Precise downgrade trigger boundary (last-consumer-member-leaves vs. any
  membership re-evaluation) and whether Kafka downgrades a group that *never* held
  a classic member: confirm empirically.
- Whether Kafka bumps the classic `generation_id` exactly once per epoch advance
  or tracks it independently: confirm via `--describe` output across a roll.

## Decomposition (parallel batches per CLAUDE.md; non-overlapping file sets)

| Batch | Task | Primary files |
|-------|------|---------------|
| **1** | k5 schema + classic sub-struct; replay reconstruction of the facade; **fix the k3-`GroupMetadata` tombstone to drop the seed entry** | `persistence_next_gen.rs`, `bootstrap.rs`, `mod.rs` (replay) |
| **1** | Routing: `get_or_create_group`; handlers stop treating `None` as wrong-protocol | `mod.rs`, `handlers/{join_group,consumer_group_heartbeat,sync_group,leave_group,offset_fetch,offset_commit}.rs`, `txn/handlers/txn_offset_commit.rs` |
| **2** | Actor loop: branch on live `group.kind`; unified tick; upgrade trigger + serve hosted classic member (Join/Sync/Heartbeat bridge) | `actor.rs`, `migration.rs` |
| **2** | Conversion persistence batches (upgrade + downgrade `PendingRecords`) | `actor.rs`, `migration.rs`, `persistence_next_gen.rs` |
| **3** | Downgrade trigger (last-consumer-member-leaves) + classic re-expression | `actor.rs`, `migration.rs` |
| **3** | `describe` / `list` coherence for mutable-type groups | `mod.rs` |
| **4** | In-process integration tests; JVM acceptance test (slice F) | `coordinator/unified/*` tests, `tests/jvm_consumer_group_next_gen.rs` |

Batch 1 fans out (disjoint files). Batches 2 and 3 both edit `actor.rs` /
`migration.rs`, so they serialize. Batch 4 follows.

## Acceptance

- A classic group joined incrementally by `group.protocol=consumer` consumers
  under `policy=bidirectional` upgrades with no partition gap/overlap and no
  consumption stall; the reverse downgrades likewise — proven in-process and with
  real JVM clients (slice F).
- `kafka-consumer-groups --describe`/`--list` report the migrating group
  coherently throughout.
- A converted group survives a coordinator restart (bootstrap replay) at the
  correct post-conversion type, including upgrade-then-downgrade.
- `policy=disabled` reproduces today's hard separation.
- The README KIP matrix entry for KIP-848 flips ⚠️ → ✅.
