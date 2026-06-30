# KIP-1022 — finish "Formatting and updating features"

**Date:** 2026-06-03
**Status:** Design approved; implementing directly with TDD (small, tightly-coupled change).

## Problem

KIP-1022 ("Formatting and Updating Features") has two halves:

- **Updating features** (`kafka-features` tool → `UpdateFeatures` / `ApiVersions`
  wire): the broker-side handler — per-feature supported-range check, downgrade
  floor, KIP-1022 dependency check, v2 fail-fast top-level error promotion — and
  the `ApiVersions` feature advertisement are **already implemented** by the
  KIP-584 feature-framework slice (`handlers/update_features.rs`,
  `handlers/api_versions.rs`). This half just needs end-to-end JVM validation.
- **Formatting features** (`kafka-storage format` → `crabka format`): the KIP
  added a `--feature NAME=VERSION` per-feature override flag. Crabka's `crabka
  format` supports `--release-version` (seeds every feature at its per-release
  default via `bootstrap_feature_records`) but **not** `--feature`.

The README marks KIP-1022 ⚠️. This slice adds `crabka format --feature`,
validates the whole feature surface against the real JVM `kafka-features` tool,
and flips the row to ✅.

## Empirically-pinned Kafka semantics (mirror.gcr.io/apache/kafka:4.0.0)

Pinned by running `kafka-storage format …` then dumping `bootstrap.checkpoint`
with `kafka-dump-log --cluster-metadata-decoder`, and by `kafka-features
version-mapping` / `feature-dependencies`. (Per CLAUDE.md: pinned empirically,
not from memory.)

**`format` feature-resolution algorithm:**

1. Resolve `bootstrap_mv`:
   - `--feature metadata.version=N` present → `N`;
   - else `--release-version V` present → `level(V)`;
   - else → `LATEST_PRODUCTION` (= `METADATA_VERSION_MAX` = 25).
2. For each production feature, `level = explicit --feature override if present,
   else default_level(bootstrap_mv)`.
3. **Emit a `FEATURE_LEVEL_RECORD` only when `level > 0`.** Level-0 features are
   omitted (level 0 = absent = disabled). `kraft.version` is seeded via its own
   `V1KRaftVersion` control record, not a feature-level record (out of the
   registry — matches Crabka today).
4. `--feature` and `--release-version` **combine** — the release sets the base,
   `--feature` overrides individual features — **except** `--release-version` +
   `--feature metadata.version=X`, which is rejected:
   `"Use --release-version instead of --feature metadata.version=X to avoid
   ambiguity."`

**Validation errors observed:**

- Unknown feature → `"Unsupported feature: <name>. Supported features are:
  eligible.leader.replicas.version, group.version, kraft.version,
  transaction.version"` (sorted list).
- Level outside the feature's supported range → reject (`"No feature:<name> with
  feature level N"`; for metadata.version: `"No MetadataVersion with feature
  level N. Valid feature levels are from 7 to 26."`).

**Observed seeding examples** (feature-level records written, in order):

| format args | records written |
|---|---|
| (no args) | `metadata.version=25`, `group.version=1`, `transaction.version=2` |
| `--release-version 4.0-IV0` | `metadata.version=22`, `group.version=1` (txn=0 omitted) |
| `--feature metadata.version=20` | `metadata.version=20` only (group=0, txn=0 omitted) |
| `--feature metadata.version=24 --feature transaction.version=2` | `metadata.version=24`, `group.version=1`, `transaction.version=2` |
| `--release-version 4.0-IV0 --feature transaction.version=2` | `metadata.version=22`, `group.version=1`, `transaction.version=2` |

`version-mapping` / `feature-dependencies`: all of metadata.version /
group.version / transaction.version declare **no dependencies** in 4.0 —
consistent with Crabka's registry (all `dependencies()` return `&[]`).

## Changes

### `crates/metadata/src/feature.rs` — override-aware, level-0-omitting seeding

- Add `bootstrap_feature_records_with_overrides(bootstrap_mv: i16, overrides:
  &BTreeMap<String, i16>) -> Vec<MetadataRecord>`: for each registered feature,
  `level = overrides.get(name).copied().unwrap_or(default_level(bootstrap_mv))`;
  push a `V1FeatureLevel` record **only when `level > 0`**.
- Re-point the existing `bootstrap_feature_records(mv)` at it with an empty
  override map, so the broker's standalone self-bootstrap (`broker.rs:1202`) and
  `crabka format` share one code path. This drops the level-0 tombstone records
  the broker currently writes — the resulting `MetadataImage` is identical
  (applying a level-0 `FeatureLevelRecord` already leaves the feature absent).
- Add `validate_feature_dependencies(resolved: &BTreeMap<String, i16>) ->
  Result<(), String>`: for every `(name, level)` in `resolved`, every
  `(dep, min)` in `feature(name).dependencies(level)` must have `resolved[dep] >=
  min`. A no-op for today's registry (all deps empty) but wires the KIP-1022
  rule at format time, mirroring the `UpdateFeatures` handler. Errors name the
  unmet dependency.

### `crates/cli/src/format.rs` — `--feature` flag + validation

- New arg `--feature NAME=VERSION` (repeatable), parsed by a `value_parser` into
  `(String, i16)`. Keep `--release-version`.
- In `run()`, before seeding:
  1. Reject `--release-version` set together with a `--feature metadata.version`
     entry (ambiguity error).
  2. For each `--feature` entry: reject an unknown feature name (with the sorted
     supported-features list), and a level outside the feature's
     `supported_range()`.
  3. Resolve `bootstrap_mv` per the algorithm (feature metadata.version >
     release-version > `METADATA_VERSION_MAX`).
  4. Build the override `BTreeMap`; run `validate_feature_dependencies` on the
     fully-resolved level map; on error, fail.
  5. Seed via `bootstrap_feature_records_with_overrides`.
- New exit code `EXIT_INVALID_FEATURE` for an invalid/ambiguous `--feature`
  spec (distinct from the existing bootstrap-fail code).

`--release-version`-only and no-flag paths are unchanged in behavior (still seed
the per-release defaults), except that level-0 records are no longer emitted.

## Testing

- **Unit (`feature.rs`):** override resolution; level-0 omission; an unlisted
  feature follows `bootstrap_mv`; `validate_feature_dependencies` ok/err.
- **Unit (`format.rs`):** `--feature` parse happy/err; ambiguity rejection;
  unknown-feature rejection; out-of-range rejection; `bootstrap_mv` precedence.
- **Integration (broker):** boot a broker from a `crabka format --feature
  group.version=0`-formatted dir → `ApiVersions` shows `group.version` not
  finalized (next-gen gate falls back to classic); a `--feature
  transaction.version=2` dir finalizes tv=2. (Reuses the existing
  format-then-boot harness in `cli_smoke.rs` / `bootstrap_consumption.rs`.)
- **JVM acceptance (`crates/broker/tests/jvm_features.rs`, `#[ignore]` / Docker):**
  against an in-process Crabka broker advertised at `host.docker.internal:9092`,
  driving `mirror.gcr.io/apache/kafka:4.0.0`'s `kafka-features`:
  1. `describe` lists `metadata.version=25`, `group.version=1`,
     `transaction.version=2` at the standalone self-bootstrap defaults.
  2. `downgrade --feature group.version=0` succeeds; a follow-up `describe`
     reflects the change (and the next-gen gate is now disabled).
  3. `upgrade --feature group.version=1` round-trips back.

## Docs

- README: KIP-1022 row ⚠️ → ✅.
- STATUS.md: new slice entry summarizing the `--feature` flag, the pinned
  format algorithm, the level-0-omission unification, and the JVM
  `kafka-features` validation.

## Out of scope

- `crabka features` subcommands mirroring the JVM tool's client-side
  `version-mapping` / `feature-dependencies` (computed locally; not a wire-compat
  constraint; Crabka operators use the JVM `kafka-features` tool over the wire).
- `kraft.version` / ELR feature registration (their own KIP slices).
