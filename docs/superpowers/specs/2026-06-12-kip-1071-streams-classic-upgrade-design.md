# KIP-1071 — Classic → Streams group cold upgrade (slice 1)

> **Status:** design approved 2026-06-12. First slice of broker-side
> classic↔streams group migration (KIP-1071). Scope: the **classic → streams**
> cold-conversion direction only.

## 1. Goal

When a Kafka Streams application that previously ran on the **classic** rebalance
protocol is restarted on the **KIP-1071 streams** protocol (`group.protocol=streams`),
its broker-side group must convert from a classic group to a streams group
**without losing committed offsets**. Concretely: a `StreamsGroupHeartbeat` that
arrives for a `group_id` currently held as a drained (`Empty`) classic group
auto-converts that group to a streams group, preserving the `__consumer_offsets`
commit records and tombstoning the classic `GroupMetadata`.

This is the streams analog of the merged KIP-848 classic→consumer upgrade
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

Two consequences that shape this slice and **differ from the consumer
migration**:

- **No online translation.** Unlike KIP-848 (where the unified coordinator hosts
  live classic members and translates their `JoinGroup`/`SyncGroup`/`Heartbeat`
  into the next-gen protocol), streams migration does **not** support live mixed
  membership. We therefore build **no** hosted-classic-member facade in the
  streams actor — doing so would diverge from Kafka. Conversion only happens once
  the classic group has fully drained.
- **No migration-policy config.** The online consumer migration is gated by
  `group.consumer.migration.policy` (a rollout-safety knob). Cold streams
  conversion is safe by construction (no live members), so Kafka gates it only by
  the `streams.version` feature, not a dedicated policy. This slice introduces
  **no** new config. (Implementation note: empirically confirm `mirror.gcr.io/apache/kafka:4.2`
  exposes no `group.streams.migration.policy` before finalizing; if one exists,
  add it in a follow-up.)

Per CLAUDE.md, where the wiki/KIP and the released image differ, the released
image wins; the 4.2 docs above are the ground truth for "cold + automatic".

## 3. Scope

**In scope (slice 1):**

- Detect, in the `StreamsGroupHeartbeat` path, that `group_id` is currently a
  **drained classic group** (typed `Classic`, zero live members) and auto-convert
  it to a streams group.
- Preserve committed offsets across the flip (no offset rewrite).
- Tombstone the classic `GroupMetadata` (k2) record; the group then proceeds as a
  normal streams group (k15–21 written by the existing streams paths as members
  join and assignment proceeds).
- A forced type-lock transition `Classic → Streams` (override the prior lock).
- Reject the `StreamsGroupHeartbeat` when the classic group still has **live
  members** (online migration is unsupported) with an empirically-confirmed error
  code.

**Out of scope (explicit non-goals — later slices or Kafka-divergent):**

- **streams → classic** cold downgrade (slice 2).
- **Online / live** migration with classic-member translation (Kafka does not do
  this for streams — never build it).
- A `group.streams.migration.policy` config (pending empirical confirmation it
  doesn't exist).
- Internal repartition/changelog topic creation changes — the existing streams
  topology-ingest path already auto-creates them; conversion reuses it unchanged.
- Standby/warmup assignment changes — unchanged; the converted group is a normal
  streams group.

## 4. Design

### 4.1 Where conversion hooks in

The `StreamsGroupHeartbeat` handler (`crates/broker/src/handlers/streams_group_heartbeat.rs`)
today routes straight to the streams actor registry (`streams_groups`). Add a
pre-step: before serving the heartbeat, consult the coordinator's type lock /
classic registry for `group_id`:

- **No group / unknown type:** fresh streams group — existing path, unchanged.
- **`Streams`:** existing streams group — existing path, unchanged.
- **`Classic`, zero live members:** **convert** (§4.2), then serve the heartbeat
  against the new streams group.
- **`Classic`, ≥1 live member:** **reject** (§4.4).

The "zero live members" check reads the classic group's member set from the
unified coordinator (a drained classic group is in `Empty` state but still holds
its committed offsets and a k2 `GroupMetadata` until offsets expire).

### 4.2 The conversion (`classic → streams`)

A new function (a streams-migration module under
`crates/broker/src/coordinator/unified/streams/`, e.g. `migration.rs`, mirroring
the consumer `unified/migration.rs` location) performs the flip atomically:

1. Read the classic group's committed offsets — they are k0/k1 records, group-
   scoped and protocol-agnostic; **not rewritten**.
2. Produce a record batch that **tombstones the classic k2 `GroupMetadata`** for
   `group_id`. (Offsets k0/k1 are left intact.)
3. Force the type lock `Classic → Streams` via a new
   `mark_streams_after_upgrade(group_id)` that **overrides** any prior `Classic`
   lock (mirrors the existing `mark_classic_after_downgrade`, which forces
   `Classic` and drops the consumer seed). Remove any classic seed/seed-cache for
   the group so a respawn does not re-hydrate it as classic.
4. Instantiate the streams group in the `streams_groups` registry. Topology comes
   from the `StreamsGroupHeartbeat` (the existing ingest path); members, epochs,
   and k15–21 records are produced by the normal streams paths as the heartbeat
   (and subsequent ones) are served. No bespoke member translation.
5. Append the conversion batch (the k2 tombstone) through the same offsets-log
   append the streams actor uses, so the flip and the first streams records are
   durable.

**The classic group actor is KEPT alive in the `groups` registry** (it is the
offset home — see below), not dropped. Detection requires the classic group to
carry a `Classic` type lock; `JoinGroup` now calls `mark_classic` (first-mark-
wins, so a consumer group's prior lock is never overridden) so a drained classic
group is `group_type == Some(Classic)` when the streams heartbeat arrives.

### 4.2.1 Why the classic actor stays (offset home)

Committed offsets are NOT stored in the streams actor. Both `OffsetCommit`
(`handlers/offset_commit.rs`) and `OffsetFetch` (`handlers/offset_fetch.rs`) route
via `coordinator.find(group_id)` to the **unified `groups` registry** `Group`
container — for groups of any protocol, including streams. A streams group
therefore legitimately has BOTH a streams actor (membership/assignment) and a
`groups` entry (offsets). On conversion we keep the existing classic actor as that
offsets-holding `groups` entry; dropping it would lose the in-memory committed
offsets (until a replay reconstructed them). Its membership is empty (drained), so
it no longer serves rebalances — the type lock is `Streams` and the streams actor
owns the protocol.

### 4.3 Persistence summary

| Record | On conversion |
|--------|---------------|
| k0/k1 `OffsetCommit` | **untouched** (offset continuity) |
| k2 classic `GroupMetadata` | **tombstoned** |
| k15–21 streams records | written by existing streams paths post-conversion |

Bootstrap replay after the conversion reconstructs a streams group (k15 present,
k2 tombstoned), with the prior offsets intact — the recovery boundary is correct.

### 4.4 Live-members rejection

If the classic group has ≥1 live member when the `StreamsGroupHeartbeat` arrives,
reject it (online streams migration is unsupported). The exact wire error code is
**to be confirmed empirically** against `mirror.gcr.io/apache/kafka:4.2` (drive a classic
Streams app + a streams-protocol instance at the same `group_id` without draining
and capture the `StreamsGroupHeartbeat` response error). Spec placeholder:
`GROUP_ID_NOT_FOUND` or a coordinator-load/unsupported error — **do not hard-code
until confirmed**. The implementation plan's first step is this empirical probe.

## 5. Components / files

- **New:** `crates/broker/src/coordinator/unified/streams/migration.rs` — the
  `classic_streams_convertible(...)` predicate, `convert_classic_to_streams(...)`,
  and the k2-tombstone record builder.
- **Modify:** `crates/broker/src/handlers/streams_group_heartbeat.rs` — the
  pre-step routing (convert / reject / passthrough).
- **Modify:** `crates/broker/src/coordinator/unified/mod.rs` — add
  `mark_streams_after_upgrade(group_id)` (forced override + seed cleanup),
  alongside the existing `mark_classic_after_downgrade`.
- **Modify:** `crates/broker/src/handlers/join_group.rs` — call
  `mark_classic(group_id)` (first-mark-wins) so a classic group carries a
  `Classic` type lock the conversion can detect. (`group_types` was previously
  unset for classic groups — the per-group kind lived only in the actor's
  `GroupKindTag`; `mark_next_gen` has no callers, so this is additive and does not
  affect the classic/consumer migration path.)
- **Modify:** streams coordinator entry (`streams/mod.rs` / `actor.rs`) only as
  needed to instantiate a streams group seeded from an existing `group_id` with
  pre-existing committed offsets.

Keep the migration logic in its own `migration.rs` so the conversion is testable
in isolation and the streams actor stays focused.

## 6. Testing

1. **Unit — convertibility predicate:** a drained classic group (zero members,
   has offsets) is convertible; a classic group with a live member is not.
2. **Unit — conversion record batch:** `convert_classic_to_streams` emits exactly
   one k2 tombstone for `group_id` and leaves k0/k1 untouched; the type lock
   flips `Classic → Streams` (forced) and the classic seed is dropped.
3. **In-process broker integration:** seed a classic group with committed offsets
   + an `Empty` classic `GroupMetadata`; send a `StreamsGroupHeartbeat`; assert
   (a) the group is now `Streams`, (b) the committed offsets are readable
   unchanged (`OffsetFetch`), (c) the classic k2 is tombstoned, (d) a streams
   assignment is produced. Use the existing streams + offsets test harness.
4. **In-process broker — live-members rejection:** a classic group with a live
   member receives a `StreamsGroupHeartbeat` → assert it is rejected (assert the
   error code once §4.4's empirical probe fixes it; until then, assert rejection
   shape).
5. **Regression:** existing classic-group, consumer-migration, and streams-group
   suites pass unmodified (the pre-step is inert for non-classic `group_id`s).

A JVM-differential acceptance test (real cp-kafka classic Streams app → apache
streams-protocol restart, offset continuity) is a **slice-2/F-level** follow-up,
not part of this slice; §6.3 covers the behavior in-process.

## 7. Open items to resolve during implementation (empirical, per CLAUDE.md)

1. **Exact rejection error code** for the live-members case (§4.4) — **RESOLVED:
   `GROUP_ID_NOT_FOUND` (69).** Crabka's consumer migration already returns
   `GROUP_ID_NOT_FOUND` when a classic group cannot serve a next-gen
   `ConsumerGroupHeartbeat` (`handlers/consumer_group_heartbeat.rs:57,63`), a path
   JVM-validated against real clients in `#376`. The streams case is the same
   semantics ("this classic group can't serve the new-protocol heartbeat → report
   not-found, client falls back"), so the streams path mirrors it. A full
   `mirror.gcr.io/apache/kafka:4.2` Streams-app coexistence capture remains a nice-to-have
   confirmation but the JVM-validated consumer precedent is authoritative here.
2. **Confirm no `group.streams.migration.policy`** config exists in
   `mirror.gcr.io/apache/kafka:4.2`; if it does, add it (small follow-up) rather than diverge.
3. **Confirm the conversion trigger boundary** — does Kafka convert on the first
   `StreamsGroupHeartbeat` for a drained classic `group_id`, or only after the
   classic group's `GroupMetadata` has been compaction-removed? The 4.2 docs say
   "after … session.timeout.ms expire", implying the drained-but-still-present
   case converts; confirm with a capture.

## 7a. Deferred to slice 2 (admin-observability seams; surfaced in final review)

Because the converted group keeps its classic actor in the `groups` registry as
the offset home (§4.2.1), some admin paths still report it through the classic
projection. None corrupt offsets or panic; all are cold-migration-acceptable for
slice 1 and should be addressed in the downgrade/admin-polish slice:

- **Unfiltered `ListGroups` / `DescribeGroups`** report the converted group as
  `classic` (state `Empty`), because the classic actor is enumerated and the
  classic snapshot pass wins the dedup. The JVM `kafka-streams-groups.sh --list`
  (`types_filter=["streams"]`) reports it correctly as `streams`; only the broad
  unfiltered view is wrong.
- **`DeleteGroups`** targets the classic actor and only checks classic emptiness,
  so it could delete the converted group's offset home out from under the live
  streams group. Guard this in slice 2.
- **Steady-state test gap:** add a restart-after-conversion replay test and a
  post-conversion `DescribeGroups`/`ListGroups` assertion.

## 8. Coverage map

- §1 goal (offset continuity) → tests §6.3.
- §4.2 conversion + §4.3 persistence → tests §6.2, §6.3.
- §4.1 routing pre-step → tests §6.3 (convert), §6.4 (reject), §6.5 (passthrough).
- §4.4 rejection → test §6.4 + open item §7.1.
- §3 non-goals → no code.
