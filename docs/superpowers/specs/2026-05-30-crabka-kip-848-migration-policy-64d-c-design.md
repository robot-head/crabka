# Slice 64d-C — Migration policy + dynamic type (KIP-848 migration)

**Status:** design
**Date:** 2026-05-30
**Roadmap:** `2026-05-29-crabka-classic-nextgen-migration-roadmap-design.md`, Slice C.
Builds on Slice B (unified `GroupCoordinator`, PR #351). Slices D/E (live
upgrade/downgrade) consume the policy + convertibility predicate this slice adds.

## Goal

Introduce the **machinery** that governs classic ↔ next-gen conversion, without
performing any live conversion yet (that is Slices D/E):

1. A Kafka-compatible `group.consumer.migration.policy` config
   (`disabled` / `upgrade` / `downgrade` / `bidirectional`).
2. A **convertibility predicate** — given a classic group, can it become a
   consumer group? (and the trivial reverse).
3. Helper accessors (`allows_upgrade()`, `allows_downgrade()`) the conversion
   triggers in D/E will gate on.

This slice is **behavior-preserving**: with no conversion wired, the
cross-protocol rejection (a `ConsumerGroupHeartbeat` on a classic group, or a
`JoinGroup` on a consumer group) still rejects exactly as in Slice B. The policy
and predicate are added with unit coverage; D/E flip the rejection into a
conversion when the policy + predicate allow.

## Empirical default (per CLAUDE.md "check the image")

Verified against `mirror.gcr.io/apache/kafka:4.0.0` (`kafka-configs --describe --entity-type
brokers --entity-name 1 --all`):

```
group.consumer.migration.policy=bidirectional  synonyms={DEFAULT_CONFIG:...=bidirectional}
```

So Crabka defaults to **`bidirectional`**, matching Kafka 4.0.

## Design

### `ConsumerGroupMigrationPolicy` (config)

`coordinator/unified/config.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsumerGroupMigrationPolicy {
    /// No conversion in either direction (Slice B behavior).
    Disabled,
    /// Classic → consumer only.
    Upgrade,
    /// Consumer → classic only.
    Downgrade,
    /// Both directions.
    #[default]
    Bidirectional,
}
impl ConsumerGroupMigrationPolicy {
    pub fn allows_upgrade(self) -> bool { matches!(self, Upgrade | Bidirectional) }
    pub fn allows_downgrade(self) -> bool { matches!(self, Downgrade | Bidirectional) }
}
impl FromStr for ConsumerGroupMigrationPolicy { /* case-insensitive, Kafka names */ }
```

`NextGenConfig` gains `pub migration_policy: ConsumerGroupMigrationPolicy`
(default `Bidirectional`). `NextGenConfig` is default-constructed today (no
broker-property parser wires the next-gen knobs yet), so the field defaults to
`bidirectional`; the `FromStr` is ready for when a parser lands.

### Convertibility predicate (`coordinator/unified/migration.rs`, new)

Mirrors Kafka's `ConsumerGroup.fromClassicGroup` admission rule: a classic group
is convertible to a consumer group iff its `protocol_type` is `"consumer"` and
**every** member's selected `protocol_metadata` decodes as a valid
`ConsumerProtocolSubscription` (so each subscription survives translation to the
server-side model). An empty group is trivially convertible.

```rust
pub(crate) fn classic_is_convertible(state: &ClassicState) -> bool;
```

The reverse (consumer → classic) is unconditional in Kafka (a consumer group can
always be expressed as a classic group); a `consumer_is_convertible` returning
`true` is provided for symmetry/readability in E.

Subscription decoding reuses the same leading-`i16`-version + `ConsumerProtocol
Subscription::decode` shape already used by `offset_delete::decode_subscribed_topics`,
factored into `migration::decode_consumer_subscription`.

### What this slice does NOT change

- No conversion trigger is wired. `consumer_group_heartbeat` on a classic actor
  and `JoinGroup` on a consumer actor still return `GROUP_ID_NOT_FOUND` /
  `INCONSISTENT_GROUP_PROTOCOL` as in Slice B.
- No new persistence. Tombstone-on-convert (write k3 + tombstone k2 on upgrade,
  and vice-versa) is implemented in D/E where the conversion actually happens;
  the k2/k3 codecs already exist (`unified::persistence`).
- The `GroupKind` container is unchanged. D replaces it with a mixed member list.

## Acceptance

- `cargo fmt`/`clippy -D warnings` clean; `cargo test --workspace` green.
- Unit tests: policy `FromStr` round-trips all four names (case-insensitive) +
  rejects junk; `allows_upgrade`/`allows_downgrade` truth table; convertibility
  predicate (empty group convertible; `protocol_type != "consumer"` not
  convertible; a member with un-decodable metadata not convertible; a group of
  valid `consumer`-subscription members convertible).
- No behavior change to any existing suite (the rejection paths are untouched).
