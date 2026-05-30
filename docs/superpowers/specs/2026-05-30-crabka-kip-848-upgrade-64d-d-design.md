# Slice 64d-D — Upgrade path (classic → consumer) (KIP-848 migration)

**Status:** design
**Date:** 2026-05-30
**Roadmap:** `2026-05-29-crabka-classic-nextgen-migration-roadmap-design.md`, Slice D.
Builds on B (unified `GroupCoordinator`) and C (`group.consumer.migration.policy`
+ convertibility predicate). E (downgrade) is the mirror; F is JVM acceptance.

## Goal

When a classic consumer group receives a `ConsumerGroupHeartbeat` and the policy
allows upgrade (`upgrade` / `bidirectional`) and the group is **convertible**
(Slice C predicate), convert it **in place** to a next-gen consumer group that
**continues to host its classic members**. The result is one group, one
`group_epoch`, one server-computed target assignment, expressed:

- to **consumer** members as `ConsumerGroupHeartbeat.assignment` (topic-ID), and
- to **classic** members as a translated `ConsumerProtocolAssignment` blob
  returned via their next `SyncGroup`.

No partition gap/overlap, no consumption stall. Matches Apache Kafka 4.0's
`ConsumerGroup` hosting classic members.

## Design decision: a Consumer group hosts classic members (a "classic facade")

Slice B kept `GroupKind::Classic | Consumer` with separate state machines. Rather
than fuse the two field-by-field, an **upgraded** group is a `Consumer`-kind
`Group` whose `ConsumerState` members may each carry an optional classic
sub-state. This reuses the entire, already-tested next-gen machinery — the
reconciler, target computation, epoch advancement, persistence (k3/k5/k6/k7/k8) —
unchanged, and adds a thin facade that serves the classic members' RPCs by
mapping them onto that machinery. (This is also how Kafka models it: a
`ConsumerGroupMember` has `classicMemberMetadata`.)

### `MemberState` gains a classic sub-state

`coordinator/unified/consumer_state.rs`:

```rust
pub struct MemberState {
    // ... existing next-gen fields ...
    /// Set when this member speaks the *classic* protocol inside an upgraded
    /// group. `None` for native consumer-protocol members.
    pub classic: Option<ClassicMemberFacade>,
}

pub struct ClassicMemberFacade {
    pub generation_id: i32,            // classic generation echoed to the member
    pub supported_protocols: Vec<(String, Bytes)>, // for a future downgrade (E)
    pub session_timeout: Duration,     // classic session.timeout.ms
    pub last_synced_assignment: Bytes, // last ConsumerProtocolAssignment we returned
}
```

A member is classic iff `classic.is_some()`. `evict_expired` already removes
members by `last_seen`; classic members use their own `session_timeout`.

### Conversion (the upgrade trigger)

In the actor, a `ConsumerGroupHeartbeat` for a **classic-kind** group is the
trigger. Today (B/C) the coordinator rejects it (`get_or_create_consumer`
returns `None` because the actor is classic-kind). In D:

1. `consumer_group_heartbeat` handler: if `find(group_id)` is a **classic** actor
   and `policy.allows_upgrade()`, send a new `MaybeUpgrade` message to that
   actor instead of rejecting. The actor checks `migration::classic_is_convertible`;
   if not convertible, reply `GROUP_ID_NOT_FOUND` (the joining consumer stays
   classic / fails, per Kafka). If convertible, **convert in place**:
   - Build a `ConsumerState` from the `ClassicState`: each classic member →
     `MemberState { classic: Some(facade), subscribed_topic_names: <decoded from
     ConsumerProtocolSubscription>, .. }`, carrying its `member_id` unchanged.
   - `group_epoch` seeds from `classic.generation_id` max; bump once.
   - Replace `group.kind` with `Consumer(state)`; **the handle keeps serving
     both** message families (see routing).
   - Persist: append k3 (`ConsumerGroupMetadata`) + k5/k6/k7/k8 for every member
     **and a tombstone for the k2 `GroupMetadata`** (so bootstrap replays the
     group as consumer). This is the first production use of the k2 *tombstone*
     and the k3+ write path for a converted group.
2. The joining consumer member is then added normally and the reconciler runs.

### Routing change: a group may accept both protocols after conversion

The Slice B per-group type lock (handle `kind`) becomes: an actor has a
`protocols_accepted` set. A freshly-created classic actor accepts classic; after
upgrade it accepts **both** classic (its hosted legacy members) and consumer.
`get_or_create_classic`/`get_or_create_consumer` and the heartbeat/join rejection
consult this instead of a single `kind`. Under `policy=disabled` the set never
grows, reproducing Slice B's hard separation (and the 64e
`coexists_with_classic` two-separate-groups behavior).

### Serving classic members in a converted group

- **Heartbeat** (classic): validate `member_id`/`generation`; refresh
  `last_seen`; reply `REBALANCE_IN_PROGRESS` while the member still owes a
  rejoin (its target changed and it must re-`SyncGroup`), else `NONE`.
- **SyncGroup** (classic): return the member's current target translated to a
  `ConsumerProtocolAssignment` blob (topic-ID → topic-name via the metadata
  image), cached in `last_synced_assignment`; advance the member past the
  rebalance.
- **JoinGroup** (classic, e.g. a new classic member joining an already-upgraded
  group): add it as a classic-facade member, bump the epoch, reconcile.
- **target → `ConsumerProtocolAssignment`** translation: resolve each assigned
  `topic_id` back to its name via the `MetadataProvider` image; encode a
  `ConsumerProtocolAssignment` with the leading version prefix (mirror of the
  decode in `migration::decode_consumer_subscription`).

## Persistence & wire constraints

- No schema change. Upgrade writes the existing k3/k5/k6/k7/k8 records and a k2
  tombstone; downgrade (E) does the reverse. Committed offsets (k0/k1) are
  untouched by conversion (they already live on the kind-agnostic `Group`).
- `ConsumerProtocolAssignment`/`ConsumerProtocolSubscription` codecs are the
  existing `crabka_protocol::owned` types.

## Tests

- **Unit:** conversion builds a `ConsumerState` preserving member ids,
  subscriptions, and committed offsets; epoch seeded/bumped; k2-tombstone +
  k3+ pending records emitted. Target → `ConsumerProtocolAssignment` round-trips
  through the metadata image. `policy=disabled` → no conversion (reject).
- **Integration (raw RPC):** a classic `JoinGroup`/`SyncGroup` group, then a
  `ConsumerGroupHeartbeat` for the same group under `bidirectional` → group
  upgrades; the classic member's next `SyncGroup` returns the translated
  assignment; the consumer member gets a topic-ID assignment; no partition
  gap/overlap.
- **Persistence:** bootstrap replay of a converted group (k3+ records with the
  k2 tombstone) reconstructs a consumer group.
- JVM rolling acceptance is Slice F.

## Out of scope (later slices)

- Downgrade (consumer → classic): Slice E (mirror; consumes
  `consumer_is_convertible` + writes k2 + tombstones k3+).
- Static-membership identity across a flip and the non-consumer-assignor
  rejection nuance: resolved in D/E per the roadmap open questions; covered by
  the convertibility predicate (a non-consumer subscription is not convertible).
- Real JVM rolling migration: Slice F.
