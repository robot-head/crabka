# Slice 64d-B — Unified `GroupCoordinator` skeleton (KIP-848 migration foundation)

**Status:** design
**Date:** 2026-05-30
**Roadmap:** `2026-05-29-crabka-classic-nextgen-migration-roadmap-design.md`, Slice B.
Builds on Slice A (64e, JVM-client engagement) which has landed. Slices C–F
(migration policy, upgrade path, downgrade path, rolling-migration JVM
acceptance) depend on this slice.

## Goal

Collapse Crabka's **two independent consumer-group coordinators** — the classic
`GroupManager` (`coordinator/mod.rs` + `group.rs`, `Mutex<Group>` + `Notify`
parking) and the next-gen `NextGenCoordinator` (`coordinator/next_gen/`,
per-group tokio actor) — into a **single `GroupCoordinator`** that natively
owns both protocols behind one registry, one persistence path, and one actor
model. Mirrors Apache Kafka 4.0's `GroupCoordinatorShard` / `GroupMetadataManager`.

This slice is a **behavior-preserving port**. After it lands:

- Groups are still **single-type** (a `group_id` is classic *or* consumer for its
  lifetime). No live migration, no mixed membership — that is Slices C–F.
- Every existing classic, next-gen, and JVM test passes **unmodified**. The
  suites are the regression gate (see Acceptance).
- The codebase has **one** group model and **one** persistence path, which is
  what makes the Slice C–E conversion tractable (conversion becomes a
  discriminant flip on one in-memory `Group`, not a hand-off between subsystems).

## Non-goals

- **Live migration / mixed membership.** The group-type discriminant stays
  effectively permanent in this slice (set once, never flipped). Policy config,
  convertibility predicate, and conversion logic are Slices C–E.
- **`group.consumer.migration.policy` config.** Introduced in Slice C.
- **Any wire-protocol or `__consumer_offsets` record-schema change.** Key
  versions `0/1` (OffsetCommit), `2` (classic `GroupMetadata`), `3/5/6/7/8`
  (next-gen) stay byte-identical. The unified coordinator reads and writes the
  exact same records the two coordinators do today.
- **Semantic changes to either protocol.** Same epochs, generations, timeouts,
  error codes, parking deadlines. A diff of the JVM-observable behavior is empty.
- **Backwards-compatibility shims** (per `CLAUDE.md`). The old `GroupManager` and
  `NextGenCoordinator` types are **deleted**, not deprecated-and-kept.

## Architecture decision: actor-per-group, single registry

Crabka runs two concurrency models today:

- **Classic:** `GroupManager.groups: DashMap<group_id, Arc<GroupHandle>>`, each
  `GroupHandle = { state: Mutex<Group>, join_complete: Notify, sync_complete:
  Notify }`. `JoinGroup`/`SyncGroup` **park the connection task** on a `Notify`
  until a rebalance boundary; the first waiter to wake drives rebalance
  completion. Deadlines live in the *handler* (`INITIAL_REBALANCE_DELAY = 3s`,
  per-member `rebalance_timeout`, `FOLLOWER_WAIT = 30s`).
- **Next-gen:** `NextGenCoordinator.groups: DashMap<group_id,
  Arc<GroupActorHandle>>`, each a tokio task consuming
  `mpsc<GroupActorMessage>` with `oneshot` replies. `ConsumerGroupHeartbeat`
  never parks; reconciliation runs inside the actor on a dirty bit; the actor
  owns a session-timeout `tick`.

**Decision (2026-05-30): unify onto the actor-per-group model.** The unified
`GroupCoordinator` owns one `DashMap<group_id, Arc<GroupActorHandle>>`; each
actor owns one `Group`. This resolves the roadmap's open question ("Coordinator
concurrency model for the unified `Group` — decide in Slice B").

### Why the actor model over `Mutex<Group>` + `Notify`

- **It reaches the single-model end state.** Kafka's real coordinator is an
  event-loop-per-shard; the actor mirrors it. The next-gen reconcile +
  persistence pipeline (`group_actor.rs`'s `flush_pending` / `PendingRecords`,
  ~1300 LOC) is built around the actor and works today — porting *into* the
  actor leaves that delicate path **untouched**, whereas re-expressing it under
  a shared mutex would mean holding the group lock across the
  `offsets_log.append().await` and re-deriving every message handler as a direct
  method call.
- **Parking is expressible as a message protocol.** Classic `JoinGroup`/
  `SyncGroup` parking — the one piece that does *not* map trivially to an actor —
  becomes: the handler sends `ClassicJoin { reply: oneshot }`, the actor
  registers the `oneshot::Sender` in a per-group **parked-joiner set**, and
  resolves it when the rebalance boundary fires. The actor already owns a
  periodic `tick`; the rebalance deadline rides the same timer. This is the
  "explicit park/wake message protocol" the roadmap flagged. It is the
  deliberate cost of this slice, isolated to one task (B3) and gated by the
  unmodified classic suites.
- **Conversion (Slices C–E) becomes local.** With both protocols' state living
  in one actor's `Group`, an upgrade/downgrade is a transition *inside* one
  actor — no cross-subsystem handoff, no two persistence paths to keep
  consistent. This is the property the roadmap's rewrite rationale (lines 71–75)
  is buying.

### The unified `Group` container

Slice B does **not** merge the two state machines field-by-field — that would be
gratuitous risk for a behavior-preserving port. Instead the unified `Group` is a
discriminated container that **reuses the existing, tested state machines**:

```rust
// coordinator/unified/group.rs
pub(crate) struct Group {
    pub group_id: String,
    pub kind: GroupKind,
}

pub(crate) enum GroupKind {
    /// Today's classic 5-state machine, moved verbatim from coordinator/group.rs.
    Classic(ClassicState),   // == today's `Group` (group.rs:166)
    /// Today's next-gen epoch machine, moved verbatim from group_state.rs.
    Consumer(ConsumerState), // == today's `GroupState` (group_state.rs:98)
}
```

`ClassicState` is the current `coordinator::group::Group` (renamed, fields and
methods intact: `add_member`, `complete_rebalance`, `install_assignments`,
`expire_dead_members`, `current_member_id_for_instance`, …). `ConsumerState` is
the current `next_gen::group_state::GroupState` (intact: `add_or_update_member`,
`install_target`, `advance_member_epoch`, `evict_expired`, …). Committed offsets
(k0/k1) are **group-scoped and protocol-agnostic**; they live on `Group`
directly so a future type flip (Slice C/D) leaves them untouched — matching
roadmap line 103–104.

> Slices C–E later replace `GroupKind` with a single member-list that holds
> classic *and* consumer members simultaneously (mixed membership). Slice B's
> enum container is the seam that makes that change a local edit to one file.

### Top-level layout after the slice

```
crates/broker/src/coordinator/
├── mod.rs                 [SHRINK — re-exports; GroupCoordinator construction;
│                           shared GroupSnapshot / DeleteGroupError / list/describe]
├── unified/
│   ├── mod.rs             [NEW — GroupCoordinator: DashMap<group_id, Arc<GroupActorHandle>>,
│   │                       get_or_create, find, list/describe/delete, shutdown,
│   │                       bootstrap-seed accumulator + finalize]
│   ├── group.rs           [NEW — Group + GroupKind; ClassicState (moved from group.rs);
│   │                       group-scoped committed_offsets]
│   ├── actor.rs           [NEW — GroupActorHandle + UnifiedMessage enum + actor loop;
│   │                       parked-joiner/parked-follower registries + rebalance-deadline
│   │                       timer; hosts BOTH protocols' handling]
│   ├── classic_ops.rs     [NEW — classic Join/Sync/Heartbeat/Leave logic against
│   │                       ClassicState, invoked by the actor]
│   ├── consumer_ops.rs    [NEW — next-gen heartbeat/offset-validate/describe logic,
│   │                       moved from group_actor.rs, against ConsumerState]
│   ├── reconciler.rs      [MOVED from next_gen/ — unchanged]
│   ├── assignor/          [MOVED from next_gen/ — unchanged]
│   ├── config.rs          [MOVED from next_gen/ — unchanged (NextGenConfig)]
│   ├── offsets_log.rs     [MOVED from next_gen/ — unchanged]
│   └── persistence.rs     [MERGED — classic k0/1/2 (was coordinator/persistence.rs)
│                           + next-gen k3/5/6/7/8 (was next_gen/persistence.rs)
│                           behind one parse_key]
└── bootstrap.rs           [REWORK — one replay path feeding GroupCoordinator;
                            classic GroupMetadata seeds a Classic actor, next-gen
                            records seed a Consumer actor]
```

`coordinator/group.rs` and the entire `coordinator/next_gen/` tree are
**deleted** after their contents move; `GroupManager` and `NextGenCoordinator`
are **deleted** types.

### Actor message protocol (unified)

```rust
// coordinator/unified/actor.rs
pub(crate) enum UnifiedMessage {
    // --- classic protocol (parking) ---
    ClassicJoin   { req: JoinGroupRequest,  client_host: String,
                    reply: oneshot::Sender<JoinOutcome> },
    ClassicSync   { req: SyncGroupRequest,
                    reply: oneshot::Sender<SyncOutcome> },
    ClassicHeartbeat { req: HeartbeatRequest,
                    reply: oneshot::Sender<i16> },              // error code
    ClassicLeave  { req: LeaveGroupRequest,
                    reply: oneshot::Sender<LeaveOutcome> },

    // --- next-gen protocol (non-parking) — moved verbatim from group_actor.rs ---
    Heartbeat { request: ConsumerGroupHeartbeatRequest, client_host: String,
                reply: oneshot::Sender<ConsumerGroupHeartbeatResponse> },
    OffsetValidate { member_id: String, member_epoch: i32,
                reply: oneshot::Sender<Result<(), i16>> },
    Describe  { reply: oneshot::Sender<DescribeView> },

    // --- lifecycle ---
    Seed(GroupSeed),
    Shutdown(oneshot::Sender<()>),
}
```

`JoinOutcome`/`SyncOutcome` carry either an **immediate** response (fast paths:
static-rejoin-to-Stable, validation errors) or a **parked** marker. For a parked
join/sync the actor stores the `oneshot::Sender` in its parked registry and
returns nothing yet; the handler `await`s the `oneshot::Receiver` with the same
deadline it uses today (`tokio::time::timeout(wait, rx)`), so the *timeout*
semantics stay handler-side and byte-identical. The actor resolves parked
senders when:

1. **every expected member has joined this round** (today: `all_members_joined_
   this_round()` → `join_complete.notify_waiters()`), or
2. the **rebalance deadline** fires on the actor `tick` (today: the
   handler-side `INITIAL_REBALANCE_DELAY`/`rebalance_timeout` timeout), or
3. the leader's `SyncGroup` installs assignments (today:
   `sync_complete.notify_waiters()`).

This is a faithful re-housing of the `Notify` wake-points: `notify_waiters()`
→ "drain the parked-sender set and `send(())` each".

### Concurrency & ordering invariants to preserve

- **Per-group serialization.** The actor processes one message at a time →
  same as today's `Mutex<Group>` per group. Cross-group concurrency is
  preserved (one actor per group).
- **Leader election determinism.** Classic leader = oldest `member_id`
  (`complete_rebalance` picks it). Preserve exactly — `jvm_acceptance` asserts
  on assignment shape.
- **Persistence-after-state.** Next-gen flushes `PendingRecords` *after* a state
  change and updates the seed cache; preserve the exact flush ordering. Classic
  `__consumer_offsets` writes (GroupMetadata on rebalance completion, offset
  commits) keep their current call sites.
- **Static membership (KIP-345).** `current_member_id_for_instance` /
  `instance_to_member` indices move with their state machines; the static-rejoin
  fast path (join_group.rs:226) must still short-circuit before parking.

## Persistence & wire constraints (unchanged, restated as guardrails)

- `__consumer_offsets` key versions are globally assigned by Kafka and must not
  drift: `0/1` OffsetCommit, `2` classic `GroupMetadata`, `3` ConsumerGroupMetadata,
  `5` ConsumerGroupMemberMetadata, `6` TargetAssignmentMetadata, `7`
  TargetAssignmentMember, `8` CurrentMemberAssignment. The merged
  `unified/persistence.rs::parse_key` dispatches all of them; the encoders are
  the existing ones moved verbatim.
- A group persisted with a `k2` GroupMetadata replays as a `Classic` actor; a
  group whose first replayed record is `k3/k5…` replays as a `Consumer` actor.
  This **is** the type-lock, now expressed as the `GroupKind` chosen at
  actor-spawn time rather than a separate `group_types: DashMap`. The
  `mark_classic`/`mark_next_gen` race-lock is **removed**; first-writer-wins is
  enforced by which message creates the actor (a `ClassicJoin` creates a
  `Classic` actor; a `Heartbeat` creates a `Consumer` actor; the second protocol
  hitting an existing group is rejected with `GROUP_ID_NOT_FOUND`, exactly as
  today).
- Committed offsets (k0/k1) survive on `Group` regardless of kind.

## Decomposition (executed task-by-task in the plan)

| Step | Scope |
|------|-------|
| **B0** | Green baseline: classic + next-gen + JVM suites pass before any edit. |
| **B1** | `unified/` scaffolding: move `config.rs`, `offsets_log.rs`, `reconciler.rs`, `assignor/` under `unified/` unchanged; merge the two `persistence.rs` behind one `parse_key`. Pure moves + a unit test that both key families still round-trip. |
| **B2** | Unified `Group`/`GroupKind`; rehouse classic `Group`→`ClassicState`, next-gen `GroupState`→`ConsumerState`; group-scoped `committed_offsets`. State-machine unit tests move with their code and pass unchanged. |
| **B3** | `actor.rs` + `consumer_ops.rs`: stand up the actor with the **next-gen** message arms ported verbatim from `group_actor.rs` (Heartbeat/OffsetValidate/Describe/Seed/Shutdown + session tick + flush pipeline). `consumer_group_*`/offset handlers route here. Next-gen + next-gen-persistence + KIP-848 JVM suites green. |
| **B4** | `classic_ops.rs` + the parked-joiner/parked-follower registries + rebalance-deadline timer on the actor `tick`. Port classic Join/Sync/Heartbeat/Leave with park/wake. Classic handlers route here. `group_protocol_negotiation`, `static_membership`, classic `jvm_acceptance` green. |
| **B5** | One `GroupCoordinator` + reworked `bootstrap.rs` (single replay path); rewire all 11 handlers; delete `GroupManager`, `NextGenCoordinator`, `group_types`, `mark_classic`/`mark_next_gen`, `coordinator/group.rs`, `coordinator/next_gen/`. Full workspace green. |
| **B6** | Verification gate (fmt, clippy `-D warnings`, `cargo test --workspace`, `--include-ignored` JVM), `STATUS.md` + README note, drop the 64d-B bullet from the roadmap's pending list. |

Each step keeps the tree compiling and the relevant suite green; B3 lands the
next-gen path on the new actor first (lower risk — it is already an actor), then
B4 lands the harder classic-parking port, so a regression is bisectable to one
protocol.

## Risk & mitigation

The slice touches the working classic path (`jvm_acceptance` classic consumers,
KIP-345 static membership, cooperative-sticky rebalance, KIP-394 bootstrap). The
single highest-risk piece is the classic parking port (B4). Mitigations:

- **The existing suites are the gate, run unmodified at every step.** No test is
  edited to accommodate the refactor; a change that needs a test edit is a
  behavior change and is out of scope.
- **Protocols land separately** (B3 next-gen, B4 classic) so a regression points
  at one protocol.
- **State machines move verbatim**, not rewritten — their unit tests move with
  them and must pass byte-for-byte.
- **Deadline/timeout semantics stay handler-side** (the handler still owns the
  `tokio::time::timeout`), so the port cannot silently change parking durations.

## Acceptance

Program-level gate — all must pass with **no test modified for the refactor**:

1. `cargo fmt --all --check` clean.
2. `cargo clippy --workspace --all-targets -- -D warnings` clean.
3. `cargo test --workspace` green — includes `group_protocol_negotiation`,
   `static_membership`, `consumer_group_next_gen`,
   `consumer_group_next_gen_persistence`, `offset_delete`, and the coordinator
   unit suites.
4. `cargo test --workspace -- --include-ignored` green — includes
   `jvm_acceptance` (classic produce/consume, cooperative-sticky, static
   membership) and `jvm_consumer_group_next_gen` (mirror.gcr.io/apache/kafka:4.0.0
   `group.protocol=consumer`).
5. `rg "GroupManager|NextGenCoordinator|mark_classic|mark_next_gen|group_types"
   crates/broker/src` returns **no hits** — the two coordinators are gone, not
   wrapped.
6. `coordinator/group.rs` and `coordinator/next_gen/` no longer exist; their
   contents live under `coordinator/unified/`.
7. No diff to any `__consumer_offsets` encoder or any wire response shape
   (the persistence round-trip and protocol snapshot tests are unchanged and
   green).

## Open questions deferred to later slices

- **`group.consumer.migration.policy` default** — Slice C (verify empirically
  against the cp-kafka/apache image).
- **Convertibility predicate** (classic member advertises the `consumer`
  embedded protocol) — Slice D.
- **Static membership identity across a flip** — Slice D/E.

These do not block B; the unified container is the prerequisite they all build on.
