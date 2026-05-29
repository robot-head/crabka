# Broker runtime `metadata.version` enforcement

**Date:** 2026-05-29
**Status:** Design approved; ready for implementation planning.

## Problem

The broker has no runtime `metadata.version` enforcement. The `UpdateFeatures`
handler (api_key 57) exists and persists a `V1FeatureLevel` record via Raft, and
`ApiVersions` advertises `supported_features` / `finalized_features` / the epoch
— but **nothing consumes the finalized level**:

- `crate::features` advertises `metadata.version` at a single conservative level
  (`MIN = MAX = 1`, `3.0-IV1`).
- No code path reads `image.finalized_features()["metadata.version"]` to gate
  behavior, reject operations, or block an unsafe downgrade.
- A freshly-formatted cluster never establishes a finalized MV at all — it sits
  at `epoch −1` ("UNKNOWN") until an admin runs `UpdateFeatures`.
- All upgrade safety lives in the operator (`crabka_operator::version`, Slice
  28), which renders a `metadata.version` *string* into the broker's **inert**
  `[server_properties]` (ignored at `file_config.rs:545`).

STATUS.md (Slice 28) explicitly deferred "broker-side `metadata.version`
feature-level enforcement (`UpdateFeatures` handler)" as a Crabka-core slice.
This design closes that gap with real enforcement inside Crabka-core.

## Goal

Real broker enforcement, decomposed into four behaviors (all in scope):

1. **Startup range guard** — refuse to operate if the finalized MV is outside
   this binary's supported range.
2. **Bootstrap a finalized MV** — every formatted image carries a real finalized
   MV (epoch ≥ 0), not `UNKNOWN`.
3. **Downgrade-safety floor** — the controller refuses to finalize below what
   live state requires.
4. **Feature-gating mechanism with teeth** — per-RPC admission gates that reject
   feature RPCs below their introduction level.

## Key decisions

- **Broker owns the string↔level table** (upstream-Kafka shape). The operator
  stays string-based; the broker maps the operator's version string to an
  integer level.
- **Ceiling = Kafka 4.0.** `MAX ≈ 25` (`4.0-IVx`); `MIN = 7` (`3.3-IV3`, the
  minimum real 4.0 supports). Exact level numbers + names pinned against the
  cp-kafka 4.0 `MetadataVersion` enum at implementation time, per CLAUDE.md
  (empirical, not from memory).
- **Fail-fast / abort** on an out-of-range finalized MV — one reaction for both
  startup-load and runtime post-commit.
- **Full upstream gate set** — downgrade floor *plus* per-RPC admission gates.

## Hard Kafka-compat constraint

The table must use Kafka's **actual** integer feature levels and `X.Y-IVn`
string names for the entire advertised `[MIN, MAX]` range. JVM clients call
`MetadataVersion.fromFeatureLevel(N)` and throw on any level their enum doesn't
know. No invented levels.

---

## 1. The `MetadataVersion` table

Replace the two-constant stub in `crates/broker/src/features.rs` with a
Kafka-faithful enum mirroring upstream `MetadataVersion.java` over `[MIN, MAX]`.

- `MIN = 7` (`3.3-IV3`); `MAX ≈ 25` (`4.0-IVx`, exact value verified empirically).
- Every level in range carries its real integer + `X.Y-IVn` name.
- API:
  - `from_feature_level(i16) -> Option<MetadataVersion>`
  - `from_version_string(&str) -> Option<MetadataVersion>` — accepts both short
    (`"3.7"`) and IV (`"3.7-IV4"`) spellings.
  - `feature_level() -> i16`
- The generic `SupportedFeature { name, min, max }` row for `metadata.version`
  now reads `MIN`/`MAX` from this enum.

This module is the single source of truth consumed by: `ApiVersions`
advertisement, `UpdateFeatures` validation, `crabka format` bootstrap, the range
guard, and the per-RPC gates.

## 2. Bootstrap — every image carries a real finalized MV

- `crabka format` (`crates/cli/src/format.rs`) gains `--release-version
  <X.Y[-IVn]>` (the kafka-storage spelling). It maps the string → level via the
  table, validates it's in `[MIN, MAX]`, and emits a `V1FeatureLevel { name:
  "metadata.version", level: N }` into `bootstrap.records.bin` alongside the
  existing seed records (`V1KRaftVersion`, `V1Voters`, optional SCRAM/ACL).
  Default when the flag is absent: `MAX`.
- **Operator handoff (chosen: option A).** The operator passes its resolved
  `metadata.version` string to `crabka format --release-version` — a format-arg,
  not just the inert `[server_properties]` key. Kafka-faithful and explicit; the
  format CLI already owns bootstrap-record generation. (Rejected: B — broker
  reads `[server_properties]` at first boot, splitting bootstrap generation
  across two places; C — broker-default only, discarding the operator's resolved
  version.)

Result: a freshly-formatted cluster comes up with
`finalized_features["metadata.version"] = N` and `epoch ≥ 0`, so `ApiVersions`
reports a real `MetadataVersion` instead of `UNKNOWN`.

## 3. Range guard — fail-fast on out-of-range finalized MV

Two observation points, one reaction (abort):

- **Startup-load:** after the image is materialized at boot (snapshot install
  and/or bootstrap+log replay), before serving anything, read
  `finalized_features["metadata.version"]`. If present and outside `[MIN, MAX]`
  → fatal log naming the finalized level and this binary's range; exit non-zero.
- **Post-commit (runtime):** in `crates/raft/src/state_machine.rs::apply_entry`,
  after the new image is published, if the just-applied state carries a
  `metadata.version` outside `[MIN, MAX]` → fatal log + abort. This respects the
  infallible-apply contract: the committed record is applied successfully,
  *then* the node crashes (matches the existing "the right move is to crash"
  comment).
- A **missing** finalized MV (epoch −1) is *not* a violation — a pre-bootstrap
  or legacy/test image is permitted (the broker advertises `UNKNOWN`). The guard
  fires only on a *present, out-of-range* level.

The operator's existing `binary >= finalized` version-guard is what keeps a
correctly-run cluster from ever tripping this.

## 4. Downgrade-safety floor

New pure function `features::min_required_metadata_version(image) -> i16`,
computing the floor from live image state. Initial requirement map (exact levels
pinned at implementation):

| Live state present | Requires MV ≥ |
|---|---|
| SCRAM credentials (`V1ScramCredential`) | `3.5-IV2` (11) |
| Delegation tokens (`V1DelegationToken`) | `3.6-IV2` (14)* |
| (baseline) | `MIN` (7) |

\*exact level verified against cp-kafka 4.0.

Wired into the `UpdateFeatures` handler: when a finalize/downgrade would set
`metadata.version` below this floor, reject that row with
`INVALID_UPDATE_VERSION` and a message naming the blocking feature — **even when
the downgrade flag is set**. This is the "unsafe downgrade" Kafka refuses,
distinct from today's `allow_downgrade` / `upgrade_type` flag check. The floor is
computed against the current image at admission time.

## 5. Per-RPC admission gates

Helper on the handler path: `require_metadata_version(image, level) -> Result<(),
code>`. When the finalized MV is below a feature's introduction level, the RPC is
rejected up front. Initial gated set:

- `AlterUserScramCredentials` → requires `≥ 11`
- `CreateDelegationToken` / `RenewDelegationToken` / `ExpireDelegationToken` →
  requires `≥ 14`*

Rejection code: Kafka's convention for "feature not enabled at this
metadata.version" — `UNSUPPORTED_VERSION` (35) expected; verified empirically
against cp-kafka 4.0 before locking.

If finalized MV is `UNKNOWN` (epoch −1, unformatted), gates are **permissive** —
there's no finalized level to check against, consistent with the range guard's
treatment of a missing level.

## 6. Operator-side changes

Targeted, since the broker now owns the table:

- `crabka_operator::version::evaluate` gains the broker's `[MIN, MAX]` as bounds.
  A resolved `metadata.version` below `MIN` (7) yields a new reason
  `MetadataVersionTooLow` (mirror of the existing `MetadataVersionTooHigh`),
  keeping the operator from injecting a value the broker would abort on.
- The resolved `metadata.version` string is passed to `crabka format
  --release-version` (the option-A handoff).
- No change to the Strimzi-shaped `spec.metadataVersion` surface.

## 7. Testing & jvm_acceptance

- **Unit:** table round-trips (`from_feature_level` / `from_version_string` /
  `feature_level` for every level in range; unknown level → `None`);
  `min_required_metadata_version` for each live-state combination; range-guard
  predicate; per-RPC gate helper.
- **Integration (broker):** fresh format → image carries finalized MV + epoch ≥
  0; `UpdateFeatures` downgrade below the SCRAM/token floor rejected; out-of-range
  finalized level aborts startup; per-RPC gate rejects below level.
- **Operator:** `evaluate` rejects below-`MIN` resolved versions; format handoff
  passes the resolved string.
- **jvm_acceptance:** raising `METADATA_VERSION_MAX` **requires** re-running the
  Docker `jvm_acceptance` suite (STATUS.md). This work does that — verifying
  `kafka-features describe`, `kafka-storage format --release-version`, and a real
  JVM admin client negotiating the advertised range against cp-kafka 4.0.

## 8. Out of scope

Other 4.0 features that upstream splits into their own feature flags —
`group.version`, `transaction.version`, `eligible.leader.replicas.version`,
`kraft.version` (the last already partially modeled via the image's
`kraft_version`). The generic `SupportedFeature` table makes each a future row;
none are wired here.

## Files touched

- `crates/broker/src/features.rs` — table, `min_required_metadata_version`,
  `require_metadata_version` helper, range-guard predicate.
- `crates/broker/src/handlers/update_features.rs` — downgrade floor.
- `crates/broker/src/handlers/{alter_user_scram_credentials, *delegation_token*}`
  — per-RPC gates.
- `crates/broker/src/handlers/api_versions.rs` — reads `MIN`/`MAX` from the table
  (mechanical).
- `crates/raft/src/state_machine.rs` — post-commit range guard.
- broker startup path — startup-load range guard.
- `crates/cli/src/format.rs` — `--release-version`, bootstrap `V1FeatureLevel`.
- `crates/operator/src/version.rs` — `[MIN,MAX]` bounds, `MetadataVersionTooLow`.
- `crates/operator/src/controller/common.rs` — format-arg handoff.
