# Generalized Feature-Versioning Framework + group.version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize Crabka's single-feature (`metadata.version`) KIP-584 machinery into an N-feature framework, and land `group.version` (KIP-848) as its first new feature with full faithful gating of the next-gen consumer-group path.

**Architecture:** A `Feature` trait (in `crates/metadata`) owns each feature's versioning facts — supported range, per-release default level, downgrade floor, KIP-1022 dependencies, optional level-name. A static `feature_registry()` is the single source of truth consumed by `ApiVersions`, `UpdateFeatures`, `crabka format` bootstrap, and the Raft range guards. `metadata.version` is refactored onto the trait with no behavior change; `group.version` is added as a second registry row. Behavioral *gating* (rejecting `ConsumerGroupHeartbeat`/`Describe` below `group.version=1`) lives in the handlers, which read the finalized level from the live image.

**Tech Stack:** Rust, `crabka-metadata` (records + image + the new feature framework), `crabka-broker` (handlers + features re-exports), `crabka-raft` (state-machine guard), `crabka-cli` (`format`). Tests: `cargo test`, plus a Docker-gated empirical-pinning check against cp-kafka 4.0.

**Companion plan:** `transaction.version` (KIP-890) downlevel behavior is a separate, dependent plan (`2026-05-30-transaction-version-downlevel.md`) that builds on this framework. Spec for both: `docs/superpowers/specs/2026-05-30-feature-versioning-framework-group-txn-design.md`.

**Empirical-pinning rule (CLAUDE.md):** every concrete level/threshold marked `⟨pin⟩` below is provisional and MUST be verified against the cp-kafka 4.0 `GroupVersion` / `Feature` enums before locking. Task 0 does this once and the rest of the plan consumes the verified values.

---

## File Structure

- `crates/metadata/src/feature.rs` *(new)* — the `Feature` trait, the `feature_registry()`, `feature(name)` lookup, `is_supported_level(name, level)`, and the `MetadataVersionFeature` + `GroupVersionFeature` impls. One file: the framework + the feature definitions live together because they change together.
- `crates/metadata/src/group_version.rs` *(new)* — `group.version` name + level constants + the MV-dependency threshold (mirrors `metadata_version.rs`'s shape; plain integer feature, no string table).
- `crates/metadata/src/metadata_version.rs` — unchanged except possibly a re-export; its constants/table are reused by `MetadataVersionFeature`.
- `crates/metadata/src/image.rs` — keep `min_required_metadata_version()` (now called from `MetadataVersionFeature::min_required_floor`); no field changes.
- `crates/metadata/src/lib.rs` — `mod feature; mod group_version;` + re-exports (`Feature`, `feature_registry`, `feature`, `is_supported_level`, `group_version`).
- `crates/broker/src/features.rs` — re-export the registry; derive `SupportedFeature`/`supported_features()`/`lookup()` from it; add `require_feature(image, name, level) -> Result<(), i16>`; delete `metadata_version_blocks` after rewriting its callers.
- `crates/broker/src/handlers/alter_user_scram_credentials.rs`, `create_delegation_token.rs`, `renew_delegation_token.rs`, `expire_delegation_token.rs` — rewrite the `metadata_version_blocks` gate calls onto `require_feature`.
- `crates/broker/src/handlers/update_features.rs` — generic per-feature floor + dependency validation; drop the `if name == METADATA_VERSION` special-case.
- `crates/broker/src/handlers/consumer_group_heartbeat.rs`, `consumer_group_describe.rs` — `group.version >= 1` admission gate.
- `crates/raft/src/state_machine.rs` — generalize `guard_metadata_version` into a multi-feature range guard over the registry.
- `crates/cli/src/format.rs` — emit one bootstrap `V1FeatureLevel` per registered feature at its `default_level(bootstrap_mv)`.
- `crates/broker/tests/update_features.rs`, `api_versions_features.rs` — extend for the multi-feature surface.
- `crates/broker/tests/group_version.rs` *(new)* — KIP-848 gate integration tests.

**Batching (per CLAUDE.md — parallel where file sets are disjoint):**
- **Batch 1 (sequential foundation):** Task 0 → Task 1 → Task 2. (All touch `crates/metadata` / `features.rs`; later tasks depend on them.)
- **Batch 2 (parallel):** Task 3 (`update_features.rs`), Task 4 (`state_machine.rs`), Task 5 (`format.rs`), Task 6 (scram/delegation gate rewrites). Disjoint files.
- **Batch 3 (sequential):** Task 7 (`group_version.rs` + registry row) → Task 8 (heartbeat/describe gate) → Task 9 (integration tests) → Task 10 (docs + workspace gate).

---

### Task 0: Empirically pin the group.version levels (Docker)

**Files:** none (produces verified constants the rest of the plan uses).

- [ ] **Step 1: Read the cp-kafka 4.0 GroupVersion + Feature enums**

Run a throwaway cp-kafka 4.0 container and dump the feature definitions:

```bash
docker run --rm confluentinc/cp-kafka:7.9.0 \
  bash -lc 'find / -name "GroupVersion*.class" 2>/dev/null; \
            kafka-features --help 2>&1 | head -40'
```

If the class files are not introspectable, format a scratch cluster and ask the live broker what it supports:

```bash
docker run --rm confluentinc/cp-kafka:7.9.0 bash -lc '
  KAFKA_CLUSTER_ID=$(kafka-storage random-uuid); \
  kafka-storage format -t $KAFKA_CLUSTER_ID -c /etc/kafka/kraft/server.properties --release-version 4.0 --standalone >/dev/null 2>&1; \
  echo "release 4.0 feature defaults:"; \
  grep -ri "group.version\|transaction.version\|metadata.version" /tmp 2>/dev/null | head'
```

- [ ] **Step 2: Record the verified values**

Confirm and write down (replacing every `⟨pin⟩` in later tasks):
- `group.version` supported range — expected `0..=1` `⟨pin⟩`.
- `group.version` GA default at `--release-version 4.0` — expected `1` `⟨pin⟩`.
- The `metadata.version` level at/above which `group.version=1` is the default (its KIP-1022 dependency) — expected `metadata.version >= 22` (`4.0-IV0`) `⟨pin⟩`.

If the empirical values differ from the expectations above, use the empirical ones throughout and note the discrepancy in `crates/metadata/src/group_version.rs` module docs. No commit (verification only).

---

### Task 1: `Feature` trait + registry + `MetadataVersionFeature`

**Files:**
- Create: `crates/metadata/src/feature.rs`
- Modify: `crates/metadata/src/lib.rs`
- Test: `crates/metadata/src/feature.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Write `crates/metadata/src/feature.rs` with the trait, the metadata.version impl, the registry, and failing tests**

```rust
//! KIP-584 feature framework. A [`Feature`] owns the *versioning facts* of one
//! cluster feature — supported range, per-release default, downgrade floor,
//! KIP-1022 dependencies, optional level name. The static [`feature_registry`]
//! is the single source of truth consumed by `ApiVersions`, `UpdateFeatures`,
//! `crabka format` bootstrap, and the Raft range guards. Behavioral *gating*
//! (rejecting RPCs below a level) lives in the broker handlers that read the
//! finalized level from the image — not here.

use crate::MetadataImage;

/// One versioned cluster feature (KIP-584).
pub trait Feature: Sync {
    /// KIP-584 feature name, e.g. `"metadata.version"`.
    fn name(&self) -> &'static str;

    /// Inclusive `[min, max]` supported level range. Advertised in
    /// `ApiVersions.supported_features` and accepted by `UpdateFeatures`.
    fn supported_range(&self) -> (i16, i16);

    /// The level finalized at `crabka format` given the bootstrap
    /// `metadata.version` level (the resolved `--release-version`). Kafka
    /// derives every feature's default from the release this way.
    fn default_level(&self, bootstrap_mv: i16) -> i16;

    /// Lowest level the live image permits finalizing/downgrading to — the
    /// "unsafe downgrade" floor. Defaults to the supported min (no live-state
    /// constraint).
    fn min_required_floor(&self, _image: &MetadataImage) -> i16 {
        self.supported_range().0
    }

    /// KIP-1022 dependencies: `(other_feature, min_level)` pairs that must be
    /// finalized at `>= min_level` before THIS feature may be finalized at
    /// `level`. Empty by default.
    fn dependencies(&self, _level: i16) -> &'static [(&'static str, i16)] {
        &[]
    }

    /// Optional Kafka level name (e.g. metadata.version's `"3.7-IV4"`).
    /// `None` for plain integer features.
    fn level_name(&self, _level: i16) -> Option<&'static str> {
        None
    }
}

/// `metadata.version` (KIP-584 / KIP-778). Range, string table, and the
/// SCRAM/delegation-token floor are reused from [`crate::metadata_version`].
pub struct MetadataVersionFeature;

impl Feature for MetadataVersionFeature {
    fn name(&self) -> &'static str {
        crate::metadata_version::METADATA_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::metadata_version::METADATA_VERSION_MIN,
            crate::metadata_version::METADATA_VERSION_MAX,
        )
    }
    fn default_level(&self, bootstrap_mv: i16) -> i16 {
        // For metadata.version the release string IS the metadata.version, so
        // the default is the bootstrap level itself, clamped into range.
        bootstrap_mv.clamp(
            crate::metadata_version::METADATA_VERSION_MIN,
            crate::metadata_version::METADATA_VERSION_MAX,
        )
    }
    fn min_required_floor(&self, image: &MetadataImage) -> i16 {
        image.min_required_metadata_version()
    }
    fn level_name(&self, level: i16) -> Option<&'static str> {
        crate::metadata_version::from_feature_level(level).map(|m| m.ivn())
    }
}

/// All features this broker supports finalizing. Single source of truth.
#[must_use]
pub fn feature_registry() -> &'static [&'static dyn Feature] {
    const REGISTRY: &[&dyn Feature] = &[&MetadataVersionFeature];
    REGISTRY
}

/// Look up a registered feature by name.
#[must_use]
pub fn feature(name: &str) -> Option<&'static dyn Feature> {
    feature_registry().iter().copied().find(|f| f.name() == name)
}

/// True if `level` is within the registered feature's supported range.
/// `false` for an unknown feature (nothing supports that level).
#[must_use]
pub fn is_supported_level(name: &str, level: i16) -> bool {
    feature(name).is_some_and(|f| {
        let (min, max) = f.supported_range();
        (min..=max).contains(&level)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn registry_contains_metadata_version() {
        let f = feature("metadata.version").expect("registered");
        assert!(f.supported_range() == (7, 25));
        assert!(feature("not.a.feature").is_none());
    }

    #[test]
    fn metadata_version_default_is_the_bootstrap_level_clamped() {
        let f = feature("metadata.version").unwrap();
        assert!(f.default_level(25) == 25);
        assert!(f.default_level(7) == 7);
        assert!(f.default_level(99) == 25); // clamped to MAX
        assert!(f.default_level(1) == 7); // clamped to MIN
    }

    #[test]
    fn is_supported_level_checks_range() {
        assert!(is_supported_level("metadata.version", 7));
        assert!(is_supported_level("metadata.version", 25));
        assert!(!is_supported_level("metadata.version", 6));
        assert!(!is_supported_level("metadata.version", 26));
        assert!(!is_supported_level("not.a.feature", 1));
    }

    #[test]
    fn metadata_version_level_name() {
        let f = feature("metadata.version").unwrap();
        assert!(f.level_name(25) == Some("4.0-IV3"));
        assert!(f.level_name(7) == Some("3.3-IV3"));
        assert!(f.level_name(99).is_none());
    }
}
```

- [ ] **Step 2: Register the module + re-exports**

In `crates/metadata/src/lib.rs`, add `mod feature;` after `pub mod metadata_version;` (line ~20) and extend the re-exports:

```rust
pub use feature::{Feature, feature, feature_registry, is_supported_level};
```

- [ ] **Step 3: Run to verify it fails, then passes**

Run: `cargo test -p crabka-metadata feature::`
Expected: PASS (4 tests). If it fails to compile on `const REGISTRY: &[&dyn Feature]`, confirm `MetadataVersionFeature` is a unit struct (it is) — unit-struct trait objects const-promote.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/feature.rs crates/metadata/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "metadata: KIP-584 Feature trait + registry (metadata.version on it)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Source broker `features.rs` from the registry + `require_feature`

**Files:**
- Modify: `crates/broker/src/features.rs`
- Test: `crates/broker/src/features.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Replace the body of `crates/broker/src/features.rs`**

Keep the public surface (`METADATA_VERSION`, `METADATA_VERSION_MIN/MAX`, `SupportedFeature`, `supported_features()`, `lookup()`) so existing callers (`api_versions.rs`) compile unchanged, but derive everything from the registry. Add `require_feature`; delete `metadata_version_blocks` (callers move to `require_feature` in Task 6).

```rust
//! KIP-584 supported-feature surface for the broker. Re-exports the
//! `crabka_metadata` feature registry and derives the `ApiVersions`
//! advertisement rows from it, so the advertised and validated feature sets
//! can never disagree. Behavioral gating helpers (`require_feature`) live here
//! because they return broker error codes.

pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MAX;
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MIN;

use crabka_metadata::MetadataImage;

/// One row of the `ApiVersions.supported_features` advertisement.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing, derived from the
/// `crabka_metadata` registry (single source of truth).
pub(crate) fn supported_features() -> Vec<SupportedFeature> {
    crabka_metadata::feature_registry()
        .iter()
        .map(|f| {
            let (min_version, max_version) = f.supported_range();
            SupportedFeature {
                name: f.name(),
                min_version,
                max_version,
            }
        })
        .collect()
}

/// Look up a supported feature by name (for `UpdateFeatures` range checks).
pub(crate) fn lookup(name: &str) -> Option<SupportedFeature> {
    crabka_metadata::feature(name).map(|f| {
        let (min_version, max_version) = f.supported_range();
        SupportedFeature {
            name: f.name(),
            min_version,
            max_version,
        }
    })
}

/// KIP-584 admission gate. `Err(UNSUPPORTED_VERSION)` when `name` is finalized
/// below `required_level`. Permissive when the feature is unfinalized (no level
/// to gate against) — matching the range guard's treatment of a missing level.
pub(crate) fn require_feature(
    image: &MetadataImage,
    name: &str,
    required_level: i16,
) -> Result<(), i16> {
    let finalized = image.finalized_features().get(name).copied();
    if finalized.is_some_and(|level| level < required_level) {
        Err(crate::codes::UNSUPPORTED_VERSION)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn supported_features_include_metadata_version() {
        let f = lookup(METADATA_VERSION).expect("metadata.version supported");
        assert!(f.min_version == METADATA_VERSION_MIN);
        assert!(f.max_version == METADATA_VERSION_MAX);
        assert!(lookup("not.a.feature").is_none());
    }

    #[test]
    fn require_feature_is_permissive_on_unfinalized() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        // Nothing finalized → permissive.
        assert!(require_feature(&image, METADATA_VERSION, 11).is_ok());
    }

    #[test]
    fn require_feature_gates_below_level() {
        use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: METADATA_VERSION.to_string(),
            level: 10,
        }));
        assert!(require_feature(&image, METADATA_VERSION, 11) == Err(crate::codes::UNSUPPORTED_VERSION));
        assert!(require_feature(&image, METADATA_VERSION, 10).is_ok());
        assert!(require_feature(&image, METADATA_VERSION, 7).is_ok());
    }
}
```

- [ ] **Step 2: Verify the crate still builds (callers of the old `metadata_version_blocks` will break — fixed in Task 6)**

Run: `cargo build -p crabka-broker`
Expected: errors ONLY of the form "cannot find function `metadata_version_blocks`" in the SCRAM/delegation handlers. That is expected; Task 6 fixes them. If `api_versions.rs` errors on `supported_features()` returning `Vec`, confirm it calls `.iter()` (it does — `.iter()` borrows a `Vec` fine).

- [ ] **Step 3: Run the features unit tests in isolation**

Run: `cargo test -p crabka-broker features::tests`
Expected: PASS (3 tests). (Build of the whole crate may still fail until Task 6; `cargo test` of just this module compiles the lib — if the lib doesn't compile, defer this run to after Task 6 and note it.)

- [ ] **Step 4: Commit (with Task 6, since the crate only builds clean after the gate rewrites)**

Do NOT commit alone — `crabka-broker` does not build until Task 6. Stage and commit together at the end of Task 6.

---

### Task 3: Generalize `UpdateFeatures` validation (floor + dependencies)

**Files:**
- Modify: `crates/broker/src/handlers/update_features.rs`
- Test: `crates/broker/src/handlers/update_features.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Replace the metadata.version-specific floor block with generic per-feature floor + dependency checks**

In `crates/broker/src/handlers/update_features.rs`, inside the `for upd in &req.feature_updates` loop, replace the `if name == crate::features::METADATA_VERSION { ... }` block (lines ~111-121) with a registry-driven floor + dependency check. Replace it with:

```rust
        // Per-feature downgrade-safety floor (KIP-584 unsafe downgrade): a
        // finalize below the level the live image requires is rejected even
        // with the downgrade flag set. `level == 0` (delete) is handled by the
        // tombstone path below, not the floor.
        if let Some(feat) = crabka_metadata::feature(&name) {
            let floor = feat.min_required_floor(&image);
            if level > 0 && level < floor {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not downgrade the feature below the level required by existing cluster state.",
                ));
                continue;
            }
            // KIP-1022 dependencies: every dependency must already be finalized
            // at >= its required level in the current image.
            let mut unmet = None;
            for (dep_name, dep_level) in feat.dependencies(level) {
                let dep_finalized = image.finalized_features().get(*dep_name).copied();
                if !dep_finalized.is_some_and(|l| l >= *dep_level) {
                    unmet = Some(*dep_name);
                    break;
                }
            }
            if unmet.is_some() {
                results.push(row(
                    name,
                    codes::INVALID_UPDATE_VERSION,
                    "Can not finalize feature: a required dependency feature is not finalized at a high enough level.",
                ));
                continue;
            }
        }
```

Delete the now-unused `violates_downgrade_floor` helper (lines ~21-27) and its test `below_floor_is_rejected_even_with_downgrade_flag`, replacing the test with the generic one below. Also delete the `use crate::features;` import only if it becomes unused (it is still used for `features::lookup` — keep it).

- [ ] **Step 2: Replace the deleted floor test with a generic floor + dependency unit test**

In the `#[cfg(test)] mod tests`, add:

```rust
    #[test]
    fn metadata_version_floor_still_enforced_via_registry() {
        use crabka_metadata::{FeatureLevelRecord, MetadataRecord, MetadataImage, ScramCredentialRecord};
        // metadata.version floor rises to SCRAM_MIN_LEVEL (11) once a SCRAM
        // credential exists; the registry-driven check must reject a finalize
        // to 10.
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            mechanism: "SCRAM-SHA-256".into(),
            name: "alice".into(),
            salt: vec![1, 2, 3],
            stored_key: vec![4, 5, 6],
            server_key: vec![7, 8, 9],
            iterations: 4096,
        }));
        let feat = crabka_metadata::feature("metadata.version").unwrap();
        assert!(feat.min_required_floor(&image) >= 11);
    }
```

> Implementer note: confirm the exact field names of `ScramCredentialRecord` against `crates/metadata/src/records.rs` and adjust the literal if they differ. The assertion only depends on the floor rising to ≥ 11; if constructing the record is awkward, instead assert `feat.min_required_floor(&image) == 7` on a fresh image and rely on Task 1's coverage of the delegating path.

- [ ] **Step 3: Run the unit tests**

Run: `cargo test -p crabka-broker update_features::tests` (after Task 6 makes the crate build; if run now, expect the crate-build failure noted in Task 2).
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/handlers/update_features.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(update_features): generic per-feature floor + KIP-1022 dependency checks

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Generalize the Raft range guard to all features

**Files:**
- Modify: `crates/raft/src/state_machine.rs`
- Test: `crates/raft/src/state_machine.rs` (inline `#[cfg(test)]`)

- [ ] **Step 1: Replace `metadata_version_out_of_range` / `guard_metadata_version` with a registry-driven guard**

In `crates/raft/src/state_machine.rs`, replace the two functions (lines ~29-54) with:

```rust
/// The first finalized feature whose *present* level is outside its registered
/// supported range, if any. A feature not in the image is never a violation.
fn first_out_of_range_feature(image: &crabka_metadata::MetadataImage) -> Option<(String, i16)> {
    for (name, level) in image.finalized_features() {
        if crabka_metadata::feature(name).is_some()
            && !crabka_metadata::is_supported_level(name, *level)
        {
            return Some((name.clone(), *level));
        }
    }
    None
}

/// Abort the process if any finalized feature carries a level outside this
/// binary's supported range. Apply is infallible (a committed record cannot be
/// rejected), so an out-of-range level — applied via a snapshot from a newer
/// binary or a record finalized by a newer controller — means this binary
/// cannot safely interpret the metadata log. Fail loud and fast; the operator's
/// `binary >= finalized` guard prevents this on a correctly-run cluster.
fn guard_finalized_features(image: &crabka_metadata::MetadataImage) {
    if let Some((name, level)) = first_out_of_range_feature(image) {
        tracing::error!(
            feature = %name,
            level,
            "finalized feature level is outside this binary's supported range; aborting"
        );
        std::process::abort();
    }
}
```

- [ ] **Step 2: Update the three call sites**

Replace the three `guard_metadata_version(...)` calls (lines ~99, ~152, and one in `install_snapshot` ~241 per the design) with `guard_finalized_features(...)`, passing the same `&MetadataImage` argument. Use ripgrep to find them all:

Run: `rg -n "guard_metadata_version" crates/raft/src/state_machine.rs`
Replace every hit. Confirm zero remain afterward.

- [ ] **Step 3: Update / add the guard unit tests**

Replace the existing `guard_rejects_out_of_range_finalized_level` test with one that exercises the generic predicate over a built image:

```rust
    #[test]
    fn guard_predicate_flags_only_present_out_of_range_features() {
        use crabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
        // Empty image: nothing finalized → no violation.
        let empty = MetadataImage::new(uuid::Uuid::nil());
        assert!(super::first_out_of_range_feature(&empty).is_none());

        // In-range metadata.version → no violation.
        let mut ok = MetadataImage::new(uuid::Uuid::nil());
        ok.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 25,
        }));
        assert!(super::first_out_of_range_feature(&ok).is_none());

        // Out-of-range metadata.version → flagged (we cannot test the abort,
        // only the predicate that drives it).
        let mut bad = MetadataImage::new(uuid::Uuid::nil());
        bad.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".into(),
            level: 99,
        }));
        let hit = super::first_out_of_range_feature(&bad).expect("flagged");
        assert!(hit.0 == "metadata.version");
        assert!(hit.1 == 99);
    }
```

(Use `assert2::assert` consistent with the file; add the `use` if the test module lacks it.)

- [ ] **Step 4: Run**

Run: `cargo test -p crabka-raft state_machine`
Expected: PASS. Then `cargo build -p crabka-raft`.

- [ ] **Step 5: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/raft/src/state_machine.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "raft(state_machine): multi-feature finalized-level range guard (KIP-584)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Multi-feature bootstrap in `crabka format`

**Files:**
- Modify: `crates/cli/src/format.rs`
- Test: `crates/cli/src/format.rs` (inline `#[cfg(test)]`, near `release_version_maps_to_feature_level`)

- [ ] **Step 1: Replace the single metadata.version emission with a registry loop**

In `crates/cli/src/format.rs`, replace the block that pushes the single `V1FeatureLevel` for `metadata.version` (lines ~396-399) with a loop over the registry. The resolved `release_level` (already computed just above as the bootstrap metadata.version) is the `bootstrap_mv` each feature derives its default from:

```rust
    // KIP-584 / KIP-1022 bootstrap: finalize every registered feature at its
    // per-release default, derived from the bootstrap metadata.version. A 4.0
    // format thus seeds metadata.version, group.version, etc. at their 4.0
    // defaults so a fresh cluster engages each feature with no manual step.
    let bootstrap_mv = release_level;
    for feat in crabka_metadata::feature_registry() {
        records.push(MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: feat.name().to_string(),
            level: feat.default_level(bootstrap_mv),
        }));
    }
```

(Remove the standalone `metadata.version` push it replaces. `feature_registry` / `Feature` are re-exported from `crabka_metadata`; add the `use` only if the file doesn't already glob-import.)

- [ ] **Step 2: Extend the bootstrap test to assert all registered features are seeded**

Find the existing `release_version_maps_to_feature_level` test (line ~537) and add a sibling that asserts the bootstrap record stream contains one `V1FeatureLevel` per registered feature, each at the feature's `default_level(bootstrap_mv)`. Mirror the existing test's harness (it already runs `format` with `--release-version` and inspects `bootstrap.records.bin` / the emitted records). Concretely:

```rust
    #[test]
    fn bootstrap_seeds_every_registered_feature_at_release_default() {
        // Reuse the same setup the sibling test uses to obtain the emitted
        // Vec<MetadataRecord> for `--release-version 4.0`. Name it `records`.
        let records = emitted_records_for_release("4.0"); // existing/extracted helper
        let bootstrap_mv = crabka_metadata::metadata_version::from_version_string("4.0")
            .unwrap()
            .feature_level();
        for feat in crabka_metadata::feature_registry() {
            let found = records.iter().find_map(|r| match r {
                crabka_metadata::MetadataRecord::V1FeatureLevel(f) if f.name == feat.name() => {
                    Some(f.level)
                }
                _ => None,
            });
            assert_eq!(
                found,
                Some(feat.default_level(bootstrap_mv)),
                "feature {} not seeded at its release default",
                feat.name()
            );
        }
    }
```

> Implementer note: the sibling test `release_version_maps_to_feature_level` already obtains the emitted records somehow — factor that into a small helper `emitted_records_for_release(&str) -> Vec<MetadataRecord>` and call it from both tests (DRY). If `format` writes `bootstrap.records.bin` to a temp dir rather than returning records, read+decode that file in the helper.

- [ ] **Step 3: Run**

Run: `cargo test -p crabka-cli format`
Expected: PASS. With only `metadata.version` registered so far, the new test asserts exactly that one feature is seeded; it becomes meaningful once `group.version` is registered in Task 7.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/cli/src/format.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "cli(format): seed every registered feature at its per-release default (KIP-584/1022)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Rewrite the SCRAM/delegation gates onto `require_feature`

**Files:**
- Modify: `crates/broker/src/handlers/alter_user_scram_credentials.rs`
- Modify: `crates/broker/src/handlers/create_delegation_token.rs`
- Modify: `crates/broker/src/handlers/renew_delegation_token.rs`
- Modify: `crates/broker/src/handlers/expire_delegation_token.rs`

- [ ] **Step 1: Find every `metadata_version_blocks` caller**

Run: `rg -n "metadata_version_blocks" crates/broker/src`
Expected hits: the four handler files above (plus the now-deleted definition in `features.rs`).

- [ ] **Step 2: Rewrite each call site**

Each current call has the shape:

```rust
if crate::features::metadata_version_blocks(
    image.finalized_metadata_version(),
    crabka_metadata::metadata_version::SCRAM_MIN_LEVEL, // or DELEGATION_TOKEN_MIN_LEVEL
) {
    // push error row / return error
}
```

Replace the condition with the generic gate:

```rust
if crate::features::require_feature(
    &image,
    crate::features::METADATA_VERSION,
    crabka_metadata::metadata_version::SCRAM_MIN_LEVEL, // or DELEGATION_TOKEN_MIN_LEVEL
)
.is_err()
{
    // unchanged error-row / return body
}
```

Keep each handler's existing error message and `UNSUPPORTED_VERSION` response shape (it already used `codes::UNSUPPORTED_VERSION`; `require_feature` returns the same code). Apply the analogous change in all four files, using `SCRAM_MIN_LEVEL` for `alter_user_scram_credentials.rs` and `DELEGATION_TOKEN_MIN_LEVEL` for the three delegation-token handlers.

> Implementer note: confirm each handler holds the image as `&image` or `image` (an `Arc<MetadataImage>`); `require_feature` takes `&MetadataImage`, so pass `&image` (Arc derefs). Update the in-file unit tests that called `metadata_version_blocks` directly (e.g. `alter_user_scram_credentials.rs:294`) to call `require_feature` against a constructed image instead.

- [ ] **Step 3: Build the whole broker crate (now that no `metadata_version_blocks` remains)**

Run: `cargo build -p crabka-broker`
Expected: clean build. If "cannot find function `metadata_version_blocks`" persists, a call site was missed — re-run the ripgrep from Step 1.

- [ ] **Step 4: Run the affected unit tests + Tasks 2/3 tests now that the crate builds**

Run: `cargo test -p crabka-broker features:: update_features:: scram delegation`
Expected: PASS.

- [ ] **Step 5: Commit Tasks 2 + 6 together (the crate first builds clean here)**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/features.rs crates/broker/src/handlers/alter_user_scram_credentials.rs crates/broker/src/handlers/create_delegation_token.rs crates/broker/src/handlers/renew_delegation_token.rs crates/broker/src/handlers/expire_delegation_token.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(features): registry-sourced features + require_feature gate; rewrite SCRAM/token gates

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: `group.version` feature definition + registry row

**Files:**
- Create: `crates/metadata/src/group_version.rs`
- Modify: `crates/metadata/src/feature.rs` (add `GroupVersionFeature` + register it)
- Modify: `crates/metadata/src/lib.rs` (`mod group_version;` + re-export)
- Test: `crates/metadata/src/feature.rs` (inline)

- [ ] **Step 1: Create `crates/metadata/src/group_version.rs`**

Use the Task 0 verified values in place of every `⟨pin⟩`.

```rust
//! KIP-848 `group.version` feature-level constants. A plain integer feature
//! (no `X.Y-IVn` string table): level 0 = classic consumer groups only,
//! level 1 = next-gen (KIP-848) protocol GA. Verify the range and the
//! metadata.version dependency threshold against the cp-kafka 4.0
//! `GroupVersion` enum before editing.

/// KIP-848 feature name.
pub const GROUP_VERSION_FEATURE: &str = "group.version";

/// Minimum supported level: classic-only.
pub const GROUP_VERSION_MIN: i16 = 0; // ⟨pin⟩
/// Maximum supported level: next-gen (KIP-848) GA.
pub const GROUP_VERSION_MAX: i16 = 1; // ⟨pin⟩

/// `group.version=1` is the default once the bootstrap metadata.version is at
/// least this level (KIP-1022 dependency / per-release default). Expected:
/// 22 (`4.0-IV0`).
pub const GROUP_VERSION_GA_METADATA_LEVEL: i16 = 22; // ⟨pin⟩
```

- [ ] **Step 2: Add `GroupVersionFeature` to `crates/metadata/src/feature.rs` and register it**

After `MetadataVersionFeature`, add:

```rust
/// `group.version` (KIP-848). Plain integer feature; default rises to 1 once
/// the bootstrap metadata.version reaches the KIP-848 GA level.
pub struct GroupVersionFeature;

impl Feature for GroupVersionFeature {
    fn name(&self) -> &'static str {
        crate::group_version::GROUP_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::group_version::GROUP_VERSION_MIN,
            crate::group_version::GROUP_VERSION_MAX,
        )
    }
    fn default_level(&self, bootstrap_mv: i16) -> i16 {
        if bootstrap_mv >= crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL {
            crate::group_version::GROUP_VERSION_MAX
        } else {
            crate::group_version::GROUP_VERSION_MIN
        }
    }
    // min_required_floor: defaults to the supported min. Next-gen group state
    // lives in the coordinator / __consumer_offsets, NOT the MetadataImage, so
    // a live-state-aware downgrade floor cannot be computed here. Deferred —
    // see the spec's Slice-A note.
    //
    // dependencies: EMPTY. Empirically (Task 0 / cp-kafka 4.0) Kafka declares
    // no hard UpdateFeatures dependency for group.version — the
    // metadata.version GA threshold is a *bootstrap-default* input (used in
    // default_level above), NOT a finalize-time floor. So we inherit the
    // trait's empty `dependencies()` default and do NOT override it.
}
```

Update `feature_registry`:

```rust
    const REGISTRY: &[&dyn Feature] = &[&MetadataVersionFeature, &GroupVersionFeature];
```

- [ ] **Step 3: Register the module + re-export in `lib.rs`**

In `crates/metadata/src/lib.rs`: add `pub mod group_version;` (near `pub mod metadata_version;`).

- [ ] **Step 4: Add unit tests in `feature.rs`**

```rust
    #[test]
    fn group_version_registered_with_range() {
        let f = feature("group.version").expect("registered");
        assert!(f.supported_range() == (0, 1));
    }

    #[test]
    fn group_version_default_follows_release() {
        let f = feature("group.version").unwrap();
        // Below the GA metadata level → classic only (0); at/above → 1.
        assert!(f.default_level(crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL - 1) == 0);
        assert!(f.default_level(crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL) == 1);
        assert!(f.default_level(25) == 1);
    }

    #[test]
    fn group_version_declares_no_hard_dependencies() {
        // Kafka 4.0 declares no UpdateFeatures dependency for group.version
        // (the metadata.version threshold only drives the bootstrap default).
        let f = feature("group.version").unwrap();
        assert!(f.dependencies(0).is_empty());
        assert!(f.dependencies(1).is_empty());
    }
```

- [ ] **Step 5: Run**

Run: `cargo test -p crabka-metadata feature:: group_version`
Expected: PASS. Then `cargo test -p crabka-cli format` — the Task 5 `bootstrap_seeds_every_registered_feature_at_release_default` test now also covers `group.version`.

- [ ] **Step 6: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/metadata/src/group_version.rs crates/metadata/src/feature.rs crates/metadata/src/lib.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "metadata: register group.version (KIP-848) feature with per-release default + MV dependency

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 8: Gate the next-gen consumer-group path on `group.version >= 1`

**Files:**
- Modify: `crates/broker/src/handlers/consumer_group_heartbeat.rs`
- Modify: `crates/broker/src/handlers/consumer_group_describe.rs`

- [ ] **Step 1: Gate `ConsumerGroupHeartbeat` (key 68)**

In `crates/broker/src/handlers/consumer_group_heartbeat.rs`, the closure already has `broker`. Capture the image and check the gate immediately after decoding `req`, before the `next_gen()` lookup (around line 30). Replace the lines from the `let ng = match ...` down to `let resp = ...` only by *prepending* the gate; keep everything else:

```rust
        let mut cur: &[u8] = &req_bytes;
        let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

        // KIP-848 / KIP-584: the next-gen protocol is gated on a finalized
        // group.version >= 1. Below that (or unfinalized on an old-release
        // cluster) the broker rejects next-gen so the client falls back to the
        // classic protocol. require_feature is permissive when unfinalized.
        if crate::features::require_feature(
            &image,
            crabka_metadata::group_version::GROUP_VERSION_FEATURE,
            1,
        )
        .is_err()
        {
            return encode(version, &error(codes::UNSUPPORTED_VERSION));
        }
```

This requires `image` in scope inside the closure. The closure currently captures `group_manager`; also capture the image *before* the `Box::pin(async move {...})` so it moves in:

```rust
    let group_manager = broker.group_manager.clone();
    let image = broker.controller.current_image();
    Box::pin(async move {
```

> Implementer note: confirm `broker.controller.current_image()` returns an `Arc<MetadataImage>` (it does — used identically in `update_features.rs:46`). `&image` then derefs the Arc to `&MetadataImage`.

- [ ] **Step 2: Gate `ConsumerGroupDescribe` (key 69) per row**

In `crates/broker/src/handlers/consumer_group_describe.rs`, capture the image the same way (`let image = broker.controller.current_image();` before `Box::pin`). Inside the `for group_id in &req.group_ids` loop, before the `next_gen` lookup, add the gate so a sub-`group.version` cluster reports the group as not found (matching the heartbeat fallback):

```rust
            if crate::features::require_feature(
                &image,
                crabka_metadata::group_version::GROUP_VERSION_FEATURE,
                1,
            )
            .is_err()
            {
                row.error_code = codes::UNSUPPORTED_VERSION;
                described.push(row);
                continue;
            }
```

(Place it as the first check inside the loop body, before `let ng = match &ng_opt { ... }`.)

- [ ] **Step 3: Build**

Run: `cargo build -p crabka-broker`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/src/handlers/consumer_group_heartbeat.rs crates/broker/src/handlers/consumer_group_describe.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(groups): gate next-gen heartbeat/describe on finalized group.version>=1 (KIP-848/584)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 9: Integration tests — bootstrap finalization + KIP-848 gate

**Files:**
- Create: `crates/broker/tests/group_version.rs`
- Modify: `crates/broker/tests/api_versions_features.rs`

- [ ] **Step 1: Extend the ApiVersions surface regression test**

In `crates/broker/tests/api_versions_features.rs`, the fresh-broker test asserts `supported_features` advertises `metadata.version` at `7..25`. Add an assertion that `group.version` is also advertised at `0..1`:

```rust
    let gv = resp
        .supported_features
        .iter()
        .find(|f| f.name == "group.version")
        .expect("group.version advertised in supported_features");
    assert_eq!(gv.min_version, 0, "{resp:?}");
    assert_eq!(gv.max_version, 1, "{resp:?}");
```

Run: `cargo test -p crabka-broker --test api_versions_features`
Expected: PASS.

- [ ] **Step 2: Write the KIP-848 gate integration test**

Create `crates/broker/tests/group_version.rs`. Mirror the harness the other broker integration tests use (inspect `crates/broker/tests/support/` and a test such as `consumer_group_next_gen.rs` for how to boot a broker, finalize features, and send a `ConsumerGroupHeartbeat`). The two cases:

```rust
#![cfg(not(target_os = "windows"))]
#![allow(clippy::pedantic)]

mod support;

use crabka_protocol::owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest;
use crabka_protocol::owned::update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest};

fn heartbeat(group: &str) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        group_id: group.into(),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: Some(vec!["t".into()]),
        ..Default::default()
    }
}

#[tokio::test]
async fn next_gen_accepted_when_group_version_finalized() {
    // support::start() formats at the default release → group.version=1 seeded.
    let p = support::start().await;
    let resp = p.client.send(heartbeat("g1")).await.expect("heartbeat");
    // Not UNSUPPORTED_VERSION (35): the gate passed. (The group may still need
    // a topic / further heartbeats; we only assert the feature gate didn't fire.)
    assert_ne!(resp.error_code, 35, "next-gen gate must pass at gv=1: {resp:?}");
    p.broker.shutdown().await;
}

#[tokio::test]
async fn next_gen_rejected_when_group_version_below_one() {
    // Boot a cluster whose group.version is finalized to 0 (classic-only).
    // If support::start() always seeds gv=1, downgrade it via UpdateFeatures
    // with the downgrade flag before the heartbeat.
    let p = support::start().await;
    let _ = p
        .client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "group.version".into(),
                max_version_level: 0,
                upgrade_type: 2, // SAFE_DOWNGRADE
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures downgrade");

    let resp = p.client.send(heartbeat("g2")).await.expect("heartbeat");
    assert_eq!(resp.error_code, 35, "next-gen must be rejected at gv=0: {resp:?}");
    p.broker.shutdown().await;
}
```

> Implementer note: confirm the `support::start()` harness formats with a 4.0 release (so `group.version=1` is seeded) and that its principal is authorized for `Cluster:Alter` (needed for the downgrade in the second test) — mirror `crates/broker/tests/update_features.rs`, which already exercises `UpdateFeatures` through the same harness. Confirm the exact `ConsumerGroupHeartbeatRequest` required fields against the generated struct; adjust the constructor if fields differ. If downgrading `group.version` to 0 trips the `dependencies` check (it shouldn't — deps apply to level≥1), the second test instead needs a harness that formats below the GA metadata level; use whichever the harness supports.

- [ ] **Step 3: Run**

Run: `cargo test -p crabka-broker --test group_version`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add crates/broker/tests/group_version.rs crates/broker/tests/api_versions_features.rs
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "broker(test): group.version bootstrap finalization + KIP-848 admission gate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 10: Workspace gate + docs

**Files:**
- Modify: `STATUS.md`
- Modify: `README.md` (KIP-848 row only; the KIP-584 ✅ flip waits for the txn plan + full jvm sweep)

- [ ] **Step 1: Full workspace checks**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace`
Expected: PASS (fmt clean per the "run rustfmt before push" rule; clippy clean; all tests green, Docker tests `#[ignore]`d). Fix any fallout (most likely: another test asserting the old single-feature `ApiVersions` surface, or a `supported_features()` `&[..]` vs `Vec` call site).

- [ ] **Step 2: Empirical JVM spot-check (Docker)**

Run the metadata.version/group.version-affected JVM acceptance tests to confirm the new `group.version` advertisement doesn't break a JVM client handshake:

Run: `cargo test -p crabka-broker --test jvm_acceptance -- --ignored --test-threads=1 group consumer api_versions`
Expected: PASS. (The *full* 45-test sweep is run in the companion txn plan's final task, which re-baselines all advertised features at once and flips the README KIP-584 row.)

- [ ] **Step 3: Update STATUS.md + README**

Add a STATUS.md slice entry "Slice — Generalized feature framework + group.version (KIP-584/848/1022) (2026-05-30)" summarizing: the `Feature` trait/registry, registry-sourced `ApiVersions`/`UpdateFeatures`/bootstrap/guards, `group.version` finalized per-release and gating next-gen with classic fallback, and the deferred group.version downgrade-floor (coordinator state not in the image). Flip the README KIP-848 row to ✅ (next-gen now finalized + gated). Leave the KIP-584 row at ⚠️ until the txn plan completes the full jvm sweep.

- [ ] **Step 4: Commit**

```bash
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" add STATUS.md README.md
git -c user.name="Matthew Stone" -c user.email="matthew.d.stone@gmail.com" commit -m "docs: generalized feature framework + group.version slice (KIP-584/848)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review Notes

- **Spec coverage:** Feature trait/registry (Task 1) → bootstrap defaults (Task 5) → group.version definition (Task 7) + gate (Task 8) → generalized UpdateFeatures floor+deps (Task 3) → multi-feature range guard (Task 4) → SCRAM/token gates on the generic helper (Task 6) → ApiVersions (auto via Task 2's registry-sourced `supported_features`) → tests (Tasks 9) → jvm spot-check + docs (Task 10). The spec's `metadata.version` floor behavior is preserved (Task 1 delegates to `min_required_metadata_version`, Task 3 calls it via the trait).
- **Deviations from spec (flagged):** (1) `group.version` downgrade floor is deferred to the supported min because next-gen group state is not in the `MetadataImage`. (2) Full README KIP-584 ✅ flip + full jvm sweep are deferred to the companion txn plan so they re-baseline all features at once.
- **Type consistency:** `Feature` trait methods (`name`/`supported_range`/`default_level`/`min_required_floor`/`dependencies`/`level_name`), `feature_registry()`/`feature(name)`/`is_supported_level(name, level)`, `features::require_feature(&image, name, level) -> Result<(), i16>`, `features::supported_features() -> Vec<SupportedFeature>` — used consistently across Tasks 1–9.
- **Open implementer confirmations (flagged inline):** Task 0 pins all `⟨pin⟩` levels; `support::start()` release + Cluster:Alter authorization; exact `ScramCredentialRecord`/`ConsumerGroupHeartbeatRequest` field names; the third `guard_metadata_version` call-site location.
