# Generalized feature-versioning framework + `group.version` + `transaction.version`

**Date:** 2026-05-30
**Status:** Design approved; ready for implementation planning.

## Problem

KIP-584 feature versioning in Crabka is fully implemented but **hardwired to a
single feature, `metadata.version`**:

- `crates/broker/src/features.rs::supported_features()` is a one-row table.
- The level/string table (`crates/metadata/src/metadata_version.rs`), the
  downgrade floor (`MetadataImage::min_required_metadata_version`), and the
  `UpdateFeatures` validation all special-case `metadata.version` by name.
- `crabka format` finalizes only `metadata.version` at bootstrap.
- The startup + post-commit range guards in `crates/raft/src/state_machine.rs`
  check only `metadata.version`.

The *storage* layer is already generic — `MetadataImage.feature_levels:
BTreeMap<String, i16>`, the `V1FeatureLevel` record, `apply`, the
`features_epoch`, and the `ApiVersions` surfacing all operate over arbitrary
feature names. Everything *above* storage is single-feature.

Kafka 4.0 ships several feature flags beyond `metadata.version`. This slice
generalizes the machinery and lands two of them with full behavior:
`group.version` (KIP-848) and `transaction.version` (KIP-890). Two further
features — `kraft.version` (KIP-853) and `eligible.leader.replicas.version`
(KIP-966 ELR) — are explicitly deferred to their own slices.

## Goal

1. **A generalized feature framework** — N features, each owning its supported
   range, per-release default level, downgrade floor, dependencies, and
   optional level→string naming, with one registry as the single source of
   truth.
2. **`group.version` (KIP-848) with full faithful gating** — finalized at
   bootstrap per release; next-gen consumer-group RPCs gated on the finalized
   level with classic fallback below it.
3. **`transaction.version` (KIP-890) with full faithful downlevel behavior** —
   finalized at bootstrap per release; the transaction coordinator's record
   format, epoch-bump, and verification behavior become *conditional* on the
   finalized level, so every level behaves exactly like Kafka.

## Scope decisions (resolved during brainstorming)

- **Depth:** Framework generalization **plus** the new downstream behavior each
  level unlocks (not plumbing-only).
- **Bootstrap defaults:** **Kafka-faithful per-release map** — `--release-version
  X.Y` derives each feature's default level from that release.
- **Gate strictness:** **Full faithful downlevel behavior** — behavior is
  genuinely conditional on the finalized level at every level, including a
  distinct pre-KIP-890 transaction path below `transaction.version=2`.
- **Feature representation:** **Trait-based registry (Approach A)** — see below.
- **Out of scope (separate slices):** `kraft.version` unification (Slice C),
  `eligible.leader.replicas.version` / ELR (Slice D).

## Hard Kafka-compat constraint

Per CLAUDE.md, every exact integer level, per-release default, and dependency
level in this document is **provisional and must be pinned empirically against
the cp-kafka 4.0 `Feature` / `MetadataVersion` / `GroupVersion` /
`TransactionVersion` enums at implementation time** — not trusted from memory.
JVM clients call `Feature.fromFeatureLevel(N)` / `MetadataVersion
.fromFeatureLevel(N)` and throw on a level their enum doesn't enumerate, so the
advertised ranges and the bootstrap defaults must match upstream exactly.
Raising any advertised `*_MAX` **requires** re-running the Docker
`jvm_acceptance` suite (this also closes the still-open `metadata.version` JVM
re-baseline gap noted in STATUS.md).

---

## 1. The `Feature` trait + registry

A `Feature` describes the *versioning facts* of a feature. Behavioral *gating*
(rejecting RPCs, choosing a record format) stays in the handlers / coordinator;
the trait only answers "what levels exist, what's the default, what's the
floor, what does it depend on."

```rust
/// One versioned cluster feature (KIP-584). The trait owns versioning facts
/// only; behavioral gating lives in the handlers that read the finalized level.
pub trait Feature: Sync {
    /// KIP-584 feature name, e.g. "metadata.version".
    fn name(&self) -> &'static str;

    /// Inclusive [min, max] supported level range advertised in ApiVersions
    /// and accepted by UpdateFeatures.
    fn supported_range(&self) -> (i16, i16);

    /// Level finalized at `crabka format` given the bootstrap metadata.version
    /// level (the resolved `--release-version`). Kafka derives every feature's
    /// default from the release this way.
    fn default_level(&self, bootstrap_mv: i16) -> i16;

    /// Lowest level the live image state permits finalizing/downgrading to
    /// (the "unsafe downgrade" floor). Defaults to the supported min.
    fn min_required_floor(&self, _image: &MetadataImage) -> i16 {
        self.supported_range().0
    }

    /// KIP-1022 dependencies: other features that must already be finalized at
    /// >= the given level before THIS feature may be finalized at `level`.
    fn dependencies(&self, _level: i16) -> &'static [(&'static str, i16)] {
        &[]
    }

    /// Optional human/Kafka level name (e.g. metadata.version's "3.7-IV4").
    /// `None` for plain integer features.
    fn level_name(&self, _level: i16) -> Option<&'static str> {
        None
    }
}
```

- **Location:** the trait + the registry live in `crates/metadata` (the crate
  that already owns `metadata_version.rs` and is depended on by both `broker`
  and `cli`). `crates/broker/src/features.rs` re-exports the registry and the
  broker-side helpers, keeping today's short `crate::features::*` paths.
- **Registry:** `feature_registry() -> &'static [&'static dyn Feature]`. Single
  source of truth consumed by ApiVersions advertisement, UpdateFeatures
  validation, bootstrap, and the range guards. Lookup by name:
  `feature(name) -> Option<&'static dyn Feature>`.
- **`metadata.version` refactor:** becomes `MetadataVersionFeature` implementing
  the trait. Its string table stays in `metadata_version.rs` behind
  `level_name`; its existing state-derived floor (SCRAM ≥ 11, delegation tokens
  ≥ 14) moves into `min_required_floor`. No behavior change for
  `metadata.version`.

### Generic helpers (replace the metadata.version-specific ones)

- `min_required_level(image, feature) -> i16` — dispatches to
  `feature.min_required_floor(image)`.
- `require_feature(image, name, level) -> Result<(), i16>` — returns
  `Err(UNSUPPORTED_VERSION)` when the finalized level for `name` is below
  `level`. **Permissive when the feature is unfinalized** (no level to gate
  against), matching today's `metadata_version_blocks(None, _) == false` and the
  range guard's treatment of a missing level. The existing
  `metadata_version_blocks` callers (SCRAM, delegation tokens) are rewritten in
  terms of `require_feature(image, "metadata.version", N)`.

---

## 2. Bootstrap — Kafka-faithful per-release defaults

In `crates/cli/src/format.rs`, replace the single `metadata.version`
`V1FeatureLevel` emission with a loop over `feature_registry()`:

1. Resolve `--release-version X.Y[-IVn]` → `bootstrap_mv` level (existing
   `resolve_release_level`; default `METADATA_VERSION_MAX` when absent).
2. For each registered feature, emit `V1FeatureLevel { name,
   level: feature.default_level(bootstrap_mv) }`.

A 4.0 format thus seeds `metadata.version=25, group.version=1,
transaction.version=2` (exact values pinned empirically). The operator handoff
(operator → `crabka format --release-version <resolved MV string>`) is
unchanged — it already passes the release version; it now implicitly seeds every
feature's release default.

`kraft.version` continues to be seeded via its existing `V1KRaftVersion` record
(unchanged; it is not in this registry — Slice C).

---

## 3. `group.version` (KIP-848) — full faithful gating

`GroupVersionFeature`:

- **Range:** `0..1` (provisional; pin against `GroupVersion` enum). `0` = classic
  only; `1` = KIP-848 GA next-gen groups.
- **`default_level`:** `1` when `bootstrap_mv` ≥ the release that GA'd KIP-848,
  else `0` (exact MV threshold pinned empirically).
- **`min_required_floor`:** `1` when the image holds next-gen consumer-group
  state (a `ConsumerGroup`/`GroupType::Consumer` group exists) — you cannot
  downgrade `group.version` out from under live next-gen groups; otherwise the
  supported min.
- **`dependencies`:** `group.version=1` depends on `metadata.version ≥` its
  introduction level (pinned empirically).

**Behavioral gating (teeth):** the next-gen admission path
(`ConsumerGroupHeartbeat`, api_key 68; `ConsumerGroupDescribe`, api_key 69)
calls `require_feature(image, "group.version", 1)` up front. Below the level →
respond `UNSUPPORTED_VERSION` (35); a kafka-clients 4.0 consumer then falls back
to the classic protocol (real Kafka behavior). Classic group RPCs
(`JoinGroup`/`SyncGroup`/`Heartbeat`/etc.) are **never** gated. Because
bootstrap finalizes `group.version=1` at 4.0, a freshly-formatted cluster
engages KIP-848 with no manual step — closing the gap STATUS.md flagged (the
feature was claimed advertised but never actually in the table).

This makes the gate the single switch for next-gen, replacing today's purely
client-selected entry (client sends a heartbeat → group locks to next-gen).

---

## 4. `transaction.version` (KIP-890) — full faithful downlevel behavior

`TransactionVersionFeature`:

- **Range:** `0..2` (provisional; pin against `TransactionVersion` enum).
  - **TV_0 (0):** classic transactions (KIP-98), **non-flexible** `__transaction
    _state` records.
  - **TV_1 (1):** **flexible** (tagged-field) `__transaction_state` records;
    explicit client-driven `AddPartitionsToTxn`; no automatic epoch bump.
  - **TV_2 (2):** epoch bump on every transaction completion + server-side
    "verify-only" `AddPartitionsToTxn` / implicit partition add on Produce — the
    behavior Crabka currently runs *unconditionally*.
- **`default_level`:** `2` at ≥4.0 (pin the MV threshold empirically).
- **`min_required_floor`:** ≥ the level required by persisted/in-flight txn
  state — e.g. an ongoing transaction or TV_2-format records on the
  `__transaction_state` log pin the floor at the current level so a downgrade
  can't strand unreadable state.
- **`dependencies`:** `transaction.version=2` (and `1`) depend on
  `metadata.version ≥` their introduction levels (pinned empirically).

**Behavioral gating (teeth) — the coordinator becomes level-conditional:**

- **Record format:** the `__transaction_state` record codec selects flexible vs
  non-flexible based on finalized TV (≥1 → flexible). The coordinator reads the
  finalized level from the current image at the point it writes coordinator
  state.
- **Epoch bump:** the producer-epoch bump on `EndTxn`/completion (today in
  `init_producer_id.rs` / the EndTxn path) is applied only at **TV=2**; below
  that the coordinator runs the pre-KIP-890 no-bump fencing path.
- **AddPartitionsToTxn:** at **TV=2** the verify-only / implicit-add path; below
  that the explicit client-add path.

Because the bootstrap default is TV_2 at 4.0, the common path is unchanged from
today; the new work is the genuinely-distinct TV_0/TV_1 paths and routing the
behavior through `require_feature` / the finalized level rather than running
v2 unconditionally.

> Implementer note: inventory exactly which v2 behaviors are currently
> unconditional (epoch bump, verification, record flexibility) and confirm each
> has a faithful downlevel counterpart before writing the conditional split.
> Existing transaction tests assume v2 behavior and a 4.0-default cluster, so
> they must stay green; new tests cover the downlevel paths.

---

## 5. Feature dependencies (KIP-1022)

`UpdateFeatures` and bootstrap honor `Feature::dependencies(level)`: a finalize
of feature F to level L is rejected (`INVALID_UPDATE_VERSION`, 95, with a
message naming the unmet dependency) unless every `(dep_name, dep_level)` in
`F.dependencies(L)` is already finalized at ≥ `dep_level` in the target image.
Dependency levels (e.g. `transaction.version=2` → `metadata.version ≥ N`) are
pinned empirically.

---

## 6. `UpdateFeatures` + range-guard generalization

- **`crates/broker/src/handlers/update_features.rs`:** drop the `if name ==
  METADATA_VERSION` special-case. Per row: `feature(name)` lookup → supported
  range check (existing) → `feature.min_required_floor(image)` downgrade-floor
  check → `feature.dependencies(level)` check. All generic; no per-feature
  branches in the handler.
- **`crates/raft/src/state_machine.rs`:** the startup-load and post-commit range
  guards iterate **every** finalized feature against `feature.supported_range()`
  (any *present-but-out-of-range* level → fatal log + abort, preserving the
  infallible-apply contract). A missing/unfinalized feature is never a
  violation. Replaces the metadata.version-only `guard_metadata_version`.

---

## 7. `ApiVersions`

Already iterates `supported_features()` and the image's `finalized_features()`
generically. It simply advertises more rows once the registry grows. The only
change is sourcing the supported list from `feature_registry()` instead of the
one-row table. No wire-shape change.

---

## 8. Testing & jvm_acceptance

- **Unit:**
  - Trait/registry: every registered feature round-trips its range; `feature
    (name)` lookup; unknown name → `None`.
  - `default_level(bootstrap_mv)` for each feature across representative release
    levels (incl. below the introduction threshold → baseline).
  - `min_required_floor` for each feature across live-state combinations
    (metadata.version: SCRAM/token; group.version: next-gen group present;
    transaction.version: ongoing/persisted txn state).
  - `dependencies` rejection logic.
  - Generalized range-guard predicate over multiple features.
- **Integration (broker):**
  - Fresh format at 4.0 → image finalizes all three features at their release
    defaults, epoch ≥ 0; `ApiVersions` surfaces all three.
  - KIP-848: next-gen heartbeat **accepted** at `group.version=1`; **rejected**
    (`UNSUPPORTED_VERSION`) when finalized at 0; classic path unaffected.
  - Transactions: coordinator behavior differs across TV levels (record format
    flexible≥1; epoch bump only at 2) on a cluster formatted/finalized at each
    level; existing v2 txn suite stays green at the 4.0 default.
  - `UpdateFeatures`: dependency rejection (e.g. TV_2 with too-low MV); floor
    rejection per feature; downgrade-flag semantics unchanged.
- **jvm_acceptance (Docker):** full sweep re-baselined with `group.version` and
  `transaction.version` now advertised/finalized alongside `metadata.version`.
  Verify `kafka-features describe` lists all three at the expected levels, a
  kafka-clients 4.0 consumer engages KIP-848 against the advertised
  `group.version`, and a transactional producer works end-to-end. This run also
  completes the deferred `metadata.version` MAX=25 re-baseline.

---

## 9. Out of scope (future slices)

- **Slice C — `kraft.version` unification (KIP-853).** Surface the existing
  `image.kraft_version` / per-voter `KRaftVersionRange` through the unified
  feature framework (supported = intersection of voter ranges; finalized from
  the image; route `UpdateFeatures(kraft.version)`). Semantics differ from
  broker-only features (per-voter range intersection), so it is its own slice.
- **Slice D — `eligible.leader.replicas.version` / ELR (KIP-966).** Greenfield:
  ELR + `LastKnownELR` fields on `PartitionRecord`/`PartitionChangeRecord`,
  controller ELR maintenance on ISR shrink + `min.insync.replicas`,
  ELR-preferring leader election, `DescribeTopicPartitions` population, then the
  feature gate. A full KIP in its own right.

## Files touched

- `crates/metadata/src/` — new `feature.rs` (trait + registry + generic
  helpers); `metadata_version.rs` refactored to `MetadataVersionFeature` (string
  table behind `level_name`, floor in `min_required_floor`); new
  `group_version.rs`, `transaction_version.rs`; `lib.rs` re-exports.
- `crates/metadata/src/image.rs` — `min_required_metadata_version` generalized /
  relocated behind the trait floor; accessors unchanged (already generic).
- `crates/broker/src/features.rs` — re-export the registry + broker helpers
  (`require_feature`, `min_required_level`); rewrite `metadata_version_blocks`
  callers in terms of `require_feature`.
- `crates/broker/src/handlers/update_features.rs` — generic per-feature floor +
  dependency validation; drop the metadata.version special-case.
- `crates/broker/src/handlers/api_versions.rs` — source supported list from the
  registry (mechanical).
- `crates/broker/src/handlers/consumer_group_heartbeat.rs`,
  `consumer_group_describe.rs` — `group.version` admission gate.
- transaction coordinator (`crates/broker/src/handlers/init_producer_id.rs`,
  `add_partitions_to_txn.rs`, `end_txn.rs`, and the `__transaction_state`
  record codec) — TV-conditional record format / epoch bump / verification.
- `crates/broker/src/handlers/{alter_user_scram_credentials, *delegation_token*}`
  — rewritten on `require_feature`.
- `crates/raft/src/state_machine.rs` — generalized multi-feature range guard.
- `crates/cli/src/format.rs` — multi-feature bootstrap defaults.
- README / STATUS.md — KIP-584 row → ✅ after the jvm sweep; KIP-848 row update;
  new slice entry.
