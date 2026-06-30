# KIP-1071 — Streams ↔ Classic group migration, slice 2 (cold downgrade + admin type-awareness)

> **Status:** design approved 2026-06-12. Second slice of broker-side
> classic↔streams group migration (KIP-1071). Builds on slice 1
> (`2026-06-12-kip-1071-streams-classic-upgrade-design.md`, merged in `#496`),
> which delivered the **classic → streams** cold-upgrade direction. This slice
> adds the reverse **streams → classic** cold-downgrade direction AND closes the
> admin-observability seams slice 1 deferred to "slice 2" in its §7a.

## 1. Goal

Two things, sharing one mechanism (streams-record tombstoning):

1. **streams → classic cold downgrade.** When a Kafka Streams application that
   previously ran on the **KIP-1071 streams** protocol is restarted on the
   **classic** rebalance protocol, its broker-side group must convert from a
   streams group back to a classic group **without losing committed offsets**.
   Concretely: a classic `JoinGroup` that arrives for a `group_id` currently held
   as a drained (zero-member) streams group auto-converts that group to a classic
   group, preserving the `__consumer_offsets` commit records (k0/k1) and
   tombstoning the streams records (k15–21).

2. **Admin handlers respect the type lock.** Slice 1 keeps a converted group's
   drained **classic-kind** actor in the `groups` registry as the protocol-
   agnostic offset home (slice-1 §4.2.1). Because no admin handler consults the
   `group_type` lock — each infers a group's type from *which registry* it was
   found in — a converted (now `Streams`-locked) group is mislabeled and, worse,
   deletable through the classic path. This slice makes `ListGroups`,
   `DescribeGroups`, and `DeleteGroups` consult the lock.

This is the streams analog of the merged KIP-848 classic↔consumer migration
(`#376`), but **cold, not online** — see §2.

## 2. Kafka fidelity: streams migration is COLD and AUTOMATIC

Apache Kafka 4.2's Streams Rebalance Protocol documentation
(`kafka.apache.org/42/streams/developer-guide/streams-rebalance-protocol/`)
states:

> Online migration while the application is running is **not** available between
> the classic and new streams protocol. After shutting down all members and
> waiting for their `session.timeout.ms` to expire, a classic group can be
> converted to a streams group and a streams group can be converted to a classic
> group.

The same two consequences that shaped slice 1 apply to the reverse direction:

- **No online translation.** Unlike the KIP-848 consumer downgrade (where the
  unified coordinator re-expresses live native consumer members as classic
  members via `convert_consumer_to_classic` + facade translation), streams
  downgrade does **not** support live mixed membership. There is therefore **no
  member re-expression** here — conversion only happens once the streams group
  has fully drained. This makes the streams downgrade *simpler* than the consumer
  downgrade, not harder.
- **No migration-policy config.** Cold conversion is safe by construction (no
  live members), so Kafka gates it on the `streams.version` feature, not a
  dedicated policy. This slice introduces **no** new config (same as slice 1).

Per CLAUDE.md, where the wiki/KIP and the released image differ, the released
image wins; the 4.2 docs above are the ground truth for "cold + automatic".

## 3. Scope

**In scope (slice 2):**

- **Downgrade:** detect, in the classic `JoinGroup` path, that `group_id` is
  currently a **drained streams group** (typed `Streams`, zero live members) and
  auto-convert it to a classic group; tombstone the streams records (k15–21);
  force the type lock `Streams → Classic`; drop the streams actor. Reject the
  `JoinGroup` when the streams group still has **live members** (online migration
  is unsupported).
- **Persistence:** preserve committed offsets (k0/k1) across the flip (no offset
  rewrite); the offset-home `groups` entry (slice 1's mechanism) survives.
- **Admin type-awareness** (slice-1 §7a):
  - `ListGroups`: a `Streams`-locked group is reported once as `streams`, never
    as `classic`.
  - `DeleteGroups`: type-aware. A `Streams`-locked group is deleted through the
    streams path (empty-check the streams actor; if empty, tombstone k15–21 + drop
    the streams actor + remove the offset-home `groups` entry + clear the
    lock/seeds), never by silently removing the offset home through the classic
    path. Non-empty → rejected.
  - `DescribeGroups`: a `Streams`-locked group is reported per its streams
    identity, not the stale classic-actor projection.
- **Steady-state coverage:** a restart-after-conversion replay test plus
  post-conversion admin assertions (the slice-1 §7a test gap).

**Out of scope (explicit non-goals — later slices or Kafka-divergent):**

- **Online / live** streams migration with member translation (Kafka does not do
  this for streams — never build it).
- A `group.streams.migration.policy` config (pending empirical confirmation it
  does not exist; same as slice 1).
- Offset-record (k0/k1) lifecycle changes beyond what the existing classic
  `delete_group` already does (this slice does not newly introduce offset
  tombstoning on group delete; it matches the classic path's current behavior).
- A real cp-kafka JVM-differential downgrade acceptance test — deferred to an
  F-level slice, mirroring slice 1 (which covered behavior in-process).
- `ConsumerGroupDescribe` (api 69) / `StreamsGroupDescribe` (api 89) changes —
  those dedicated paths already handle streams groups correctly; this slice only
  fixes the generic classic-era admin handlers (api 15 / 16 / 42).

## 4. Design

### 4.1 Slice-1 recap (the state this slice extends)

After a classic→streams upgrade, slice 1's `try_convert_classic_to_streams`
(`coordinator/unified/mod.rs:427`):

- tombstones the classic k2 `GroupMetadata` (defensive),
- force-flips the lock to `Streams` via `mark_streams_after_upgrade`,
- **keeps** the drained **classic-kind** actor in `self.groups` as the offset
  home (committed offsets are protocol-agnostic and route via `find()` →
  `groups` for every protocol).

So a converted group simultaneously has: a `Streams` type lock, a streams actor
in `streams_groups`, and a drained classic-kind actor in `groups`. That last fact
is the source of both the downgrade entry condition and the admin seams.

### 4.2 Downgrade: where conversion hooks in

The classic `JoinGroup` handler (`crates/broker/src/handlers/join_group.rs`)
today calls `mark_classic` (first-mark-wins, so it will NOT override a prior
`Streams` lock) and serves the join against the classic coordinator path. Add a
pre-step mirroring the `streams_group_heartbeat.rs:64–78` pre-step, **before**
`mark_classic`:

- **Not `Streams`-locked:** existing classic path, unchanged (fresh classic
  group, existing classic group, or a consumer group — none are touched here).
- **`Streams`-locked, zero live members:** **convert** (§4.3), then serve the
  `JoinGroup` against the now-classic group.
- **`Streams`-locked, ≥1 live member:** **reject** (§4.5).

The "zero live members" check reads the **streams** actor's member set (mirroring
how `StreamsGroupDescribe` inspects a streams group), not the classic actor — the
inverse of slice 1, which inspected the classic actor.

### 4.3 The conversion (`streams → classic`)

A new function in the existing streams-migration module
(`coordinator/unified/streams/migration.rs`) plus a coordinator method in
`mod.rs`, mirroring slice 1:

1. Committed offsets (k0/k1) are group-scoped and protocol-agnostic — **not
   rewritten**.
2. Produce a record batch that **tombstones the streams records** for `group_id`:
   the group-level k15 `GroupMetadata`, k17 `Topology`, k18 `PartitionMetadata`,
   and k19 `TargetAssignmentMetadata`, plus per-member k16 `MemberMetadata`, k20
   `TargetAssignmentMember`, and k21 `CurrentMemberAssignment` for any persisted
   member. Built **directly** from the `streams/persistence.rs` key encoders
   (`encode_group_metadata_key`, etc.) emitting `Record { value: None }` per key —
   not via `PendingStreamsRecords`, whose group-level fields are `Option<Value>`
   (present-or-absent) and so cannot express a group-level null-value tombstone
   (only its per-member `Vec` fields can). **k15 is load-bearing**: a surviving
   k15 would reconstruct a streams group on bootstrap replay (the streams analog
   of the consumer downgrade's load-bearing k6 tombstone, `migration.rs:185-211`).
3. Force the type lock `Streams → Classic` via a new
   `mark_classic_after_streams_downgrade(group_id)` that **overrides** the prior
   `Streams` lock AND drops the `streams_seeds` / `streams_seeds_cache` for the
   group (so a respawn does not re-hydrate it as streams). This is distinct from
   the existing `mark_classic_after_downgrade` (`mod.rs:207`), which drops the
   *consumer* `seeds` / `seeds_cache` — the wrong seed maps for a streams group.
4. Drop the streams actor from `streams_groups`. The offset-home `groups` entry
   (the drained classic-kind actor — present whenever the group has committed
   offsets) survives and now serves the classic `JoinGroup`. If no `groups` entry
   exists yet (a streams group that never committed offsets), the classic
   `JoinGroup` path's existing `get_or_create_classic` creates one — no bespoke
   member translation, because a cold-drained group has no members to carry over.
5. Append the tombstone batch through the same `offsets_log.append` slice 1 uses,
   so the flip is durable.

### 4.4 Persistence summary (downgrade)

| Record | On downgrade |
|--------|--------------|
| k0/k1 `OffsetCommit` | **untouched** (offset continuity) |
| k15 `StreamsGroupMetadata` | **tombstoned** (load-bearing for replay) |
| k17/k18/k19 group-level streams | **tombstoned** |
| k16/k20/k21 per-member streams | **tombstoned** for each persisted member |
| k2 classic `GroupMetadata` | written by the existing classic `JoinGroup` path post-conversion, as members join |

Bootstrap replay after the downgrade reconstructs a classic group (k15 tombstoned,
k2 written by the classic path), with the prior offsets intact.

### 4.5 Live-members rejection (downgrade)

If the streams group has ≥1 live member when the classic `JoinGroup` arrives,
reject it (online streams migration is unsupported). Default to
`GROUP_ID_NOT_FOUND` (69), reusing slice-1 §7.1's resolved precedent: Crabka's
consumer migration returns `GROUP_ID_NOT_FOUND` when a classic-protocol group
cannot serve a new-protocol heartbeat, a path JVM-validated in `#376`, and slice
1 mirrors it for the streams-heartbeat-rejects-classic-group case. The reverse
(classic-`JoinGroup`-rejects-streams-group) is the same semantics. Confirm the
exact code empirically (§7.3) but `GROUP_ID_NOT_FOUND` is the authoritative
default.

### 4.6 Admin handlers consult the type lock

Root cause (all three seams): the admin handlers infer a group's wire type from
which registry/method surfaced it, never from `group_type()`. Slice 1's kept
classic-kind offset-home actor therefore leaks a `Streams`-locked group into the
classic projections. Introduce a small shared notion — "a group is *effectively*
streams iff `group_type(group_id) == Some(GroupType::Streams)`" — and apply it:

- **`ListGroups`** (`handlers/list_groups.rs` + coordinator `list_groups()`,
  `mod.rs:505`): the classic snapshot pass must **skip** `Streams`-locked ids, so
  the existing streams pass (which already labels them `streams`) is the sole
  emitter. The classic-vs-consumer discrimination already works (a consumer-kind
  actor drops `ClassicInspect`); only the streams case — whose offset-home actor
  *is* classic-kind and answers `ClassicInspect` — needs the explicit lock check.
  Fixes converted **and** born-streams groups uniformly.

- **`DeleteGroups`** (`handlers/delete_groups.rs` + coordinator `delete_group()`,
  `mod.rs:543`): make `delete_group` **type-aware**. For a `Streams`-locked
  `group_id`, route to a streams delete: inspect the streams actor's member set
  (the same inspection used by the downgrade drained-check); if non-empty →
  `NonEmpty`; if empty → tombstone k15–21 (reusing §4.3's helper), drop the
  streams actor, remove the offset-home `groups` entry if present, and drop the
  `streams_seeds`/`streams_seeds_cache`. Whether to also clear the `group_types`
  lock is a plan detail: the classic `delete_group` currently leaves `group_types`
  in place, and the §4.2 pre-steps degrade gracefully against a stale `Streams`
  lock with no live actor, so clearing is a cleanliness choice, not a correctness
  requirement. For non-`Streams` groups, the existing classic
  delete path is unchanged. Keeping the branch inside `delete_group` leaves the
  generic api-15 handler unchanged (Kafka's `DeleteGroups` is type-generic). This
  closes the slice-1 §7a data-loss bug (classic delete removing a live streams
  group's offset home) **and** the born-streams deletion gap.

- **`DescribeGroups`** (`handlers/describe_groups.rs` + coordinator
  `describe_group()`, `mod.rs:530`): consult the lock; for a `Streams`-locked
  group, report its streams identity rather than the classic-actor `InspectAny`
  projection. The exact api-15 response shape (`protocol_type`, `group_state`,
  whether members are listed) is matched empirically against `mirror.gcr.io/apache/kafka:4.2`
  (§7.4); the firm requirement is that a `Streams`-locked group is **no longer
  reported as `classic`**.

After a downgrade completes, the group is `Classic`-locked with no streams actor,
so all three handlers see a normal classic group — the downgrade direction
introduces no new seams.

## 5. Components / files

- **Modify:** `crates/broker/src/coordinator/unified/streams/migration.rs` —
  `streams_records_tombstone_batch(...)` (group-level + per-member streams
  tombstones) and the `try_convert_streams_to_classic(...)` outcome (reuse the
  existing `ConvertOutcome` enum, which already has `NotClassic`/`Converted`/
  `RejectLiveMembers` shapes — generalize the doc comments or add a parallel enum
  if the variant names read wrong for this direction; decide in the plan).
- **Modify:** `crates/broker/src/coordinator/unified/mod.rs` —
  `mark_classic_after_streams_downgrade(group_id)` (forced `→ Classic` + drop
  `streams_seeds`/`streams_seeds_cache`); `try_convert_streams_to_classic(...)`
  coordinator method; type-aware branch in `delete_group(...)`; `Streams`-lock
  skip in `list_groups(...)`; `Streams`-lock branch in `describe_group(...)`.
- **Modify:** `crates/broker/src/handlers/join_group.rs` — the downgrade
  pre-step (convert / reject / passthrough), before `mark_classic`.
- **Modify:** `crates/broker/src/handlers/{list_groups,describe_groups,delete_groups}.rs`
  — only as needed if the lock-consultation lives in the handler rather than the
  coordinator method (prefer the coordinator method for a single source of truth).
- **Create:** `crates/broker/tests/streams_classic_downgrade.rs` — in-process
  integration tests (mirror `tests/streams_classic_upgrade.rs`).
- **Modify:** `.github/workflows/ci.yml` — add `streams_classic_downgrade` to the
  broker crate's llvm-cov `--test` list (per-crate-integration convention).

Keep the new conversion logic in `streams/migration.rs` alongside slice 1's, so
both directions live together and the streams actor stays focused.

## 6. Testing

1. **Unit — downgrade tombstone batch:** `streams_records_tombstone_batch` emits
   tombstones (null value) for k15/k17/k18/k19 and for each supplied member's
   k16/k20/k21, and only those; k15 is present (load-bearing). Mirror slice 1's
   `tombstone_batch_has_one_null_value_k2_record`.
2. **Unit — forced type flip:** `mark_classic_after_streams_downgrade` overrides a
   `Streams` lock to `Classic` and drops the streams seeds (mirror slice 1's
   `mark_streams_after_upgrade_forces_streams_over_classic`).
3. **In-process — downgrade preserves offsets:** seed a drained streams group with
   committed offsets; send a classic `JoinGroup`; assert (a) the group is now
   `Classic`, (b) committed offsets are readable unchanged (`OffsetFetch`), (c) the
   streams k15 is tombstoned, (d) a classic generation/assignment is produced.
4. **In-process — live-members rejection:** a streams group with a live member
   receives a classic `JoinGroup` → rejected (`GROUP_ID_NOT_FOUND` per §4.5), no
   flip.
5. **In-process — admin type-awareness (the §7a gap):** for a converted
   (classic→streams, slice-1) group: `ListGroups` reports it once as `streams`
   (never `classic`); `DescribeGroups` does not report it as `classic`;
   `DeleteGroups` on it while the streams group has a live member → `NonEmpty` (no
   offset-home removal); `DeleteGroups` on it while drained → deletes (streams
   records tombstoned, actor dropped) and the offset home is gone.
6. **In-process — restart-after-conversion replay:** convert classic→streams
   (slice 1), restart the broker (replay `__consumer_offsets`), assert the group
   replays as `streams` with offsets intact; then for the downgrade, convert
   streams→classic, restart, assert it replays as `classic` with offsets intact.
7. **Regression:** existing classic-group, consumer-migration, streams-group, and
   slice-1 upgrade suites pass unmodified (the `JoinGroup` pre-step is inert for
   non-`Streams` group_ids; the admin lock-checks are inert for non-`Streams`
   groups).

## 7. Open items to resolve during implementation (empirical, per CLAUDE.md)

1. **Streams actor member inspection.** Confirm the exact streams-actor message
   that returns the live member set (used by both the downgrade drained-check and
   the `DeleteGroups` streams empty-check); reuse `StreamsGroupDescribe`'s path
   rather than adding a new one.
2. **`ConvertOutcome` reuse vs. a downgrade-specific enum.** The slice-1 enum
   variant names (`NotClassic`) read awkwardly for the reverse direction; decide
   in the plan whether to generalize the names or add a sibling enum.
3. **Downgrade trigger boundary & rejection code.** Confirm against
   `mirror.gcr.io/apache/kafka:4.2` that a streams group converts on the first classic
   `JoinGroup` for a drained `group_id` (vs. only after the streams
   `GroupMetadata` is compaction-removed — the same question as slice-1 §7.3), and
   confirm the live-members rejection code (default `GROUP_ID_NOT_FOUND`).
4. **api-15 `DescribeGroups` / `DeleteGroups` on a streams group.** Capture the
   real response shape (`protocol_type`, state, error semantics) for a streams
   group queried/deleted through the generic classic-era admin APIs, and match it.
   The firm requirement (no `classic` mislabel; no offset-home data loss) holds
   regardless; the empirical capture fixes the exact wire bytes.

## 8. Coverage map

- §1.1 downgrade goal (offset continuity) → tests §6.3.
- §4.3 conversion + §4.4 persistence → tests §6.1, §6.3, §6.6.
- §4.2 routing pre-step → tests §6.3 (convert), §6.4 (reject), §6.7 (passthrough).
- §4.5 rejection → test §6.4 + open item §7.3.
- §4.6 admin type-awareness → tests §6.5 + open item §7.4.
- §1.2 / §7a steady-state → test §6.6 (replay) + §6.5 (post-conversion admin).
- §3 non-goals → no code.
