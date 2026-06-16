//! KIP-584 feature framework. A [`Feature`] owns the *versioning facts* of one
//! cluster feature — supported range, per-release default, downgrade floor,
//! KIP-1022 dependencies, optional level name. The static [`feature_registry`]
//! is the single source of truth consumed by `ApiVersions`, `UpdateFeatures`,
//! `crabka format` bootstrap, and the Raft range guards. Behavioral *gating*
//! (rejecting RPCs below a level) lives in the broker handlers that read the
//! finalized level from the image — not here.

use std::collections::BTreeMap;

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
        crate::metadata_version::from_feature_level(level)
            .map(crate::metadata_version::MetadataVersion::ivn)
    }
}

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
    // min_required_floor: inherits the default (supported min). Next-gen group
    // state lives in the coordinator / __consumer_offsets, NOT the
    // MetadataImage, so a live-state-aware downgrade floor can't be computed
    // here. dependencies: inherits the empty default — Kafka 4.0
    // declares no hard `UpdateFeatures` dependency for group.version.
}

/// `transaction.version` (KIP-890). Default jumps to 2 once the bootstrap
/// metadata.version reaches 4.0-IV2; downgrade floor is the supported min
/// (in-flight txn state lives in the `__transaction_state` log, not the
/// [`MetadataImage`], so an image-derived floor is not computed here).
pub struct TransactionVersionFeature;

impl Feature for TransactionVersionFeature {
    fn name(&self) -> &'static str {
        crate::transaction_version::TRANSACTION_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::transaction_version::TRANSACTION_VERSION_MIN,
            crate::transaction_version::TRANSACTION_VERSION_MAX,
        )
    }
    // The TV_1 tier is retained as an explicit (currently coincident) threshold
    // so a future Kafka release that splits TV_1's bootstrap level below TV_2's
    // is a one-line constant change; `clippy::bool_to_int_with_if` would
    // collapse the tiers and lose that.
    #[allow(clippy::bool_to_int_with_if)]
    fn default_level(&self, bootstrap_mv: i16) -> i16 {
        // Both TV_1 and TV_2 bootstrap at level 24 (4.0-IV2) → default jumps
        // 0 -> 2 at >= 24. Empirically pinned.
        use crate::transaction_version::{TV1_METADATA_LEVEL, TV2_METADATA_LEVEL};
        if bootstrap_mv >= TV2_METADATA_LEVEL {
            2
        } else if bootstrap_mv >= TV1_METADATA_LEVEL {
            1
        } else {
            0
        }
    }
    // dependencies + min_required_floor: inherit the empty/supported-min defaults.
}

/// `share.version` (KIP-932). Plain integer feature gating share-group
/// membership. Default stays at the supported min (0, disabled) until the
/// bootstrap metadata.version reaches the KIP-932 GA level.
pub struct ShareVersionFeature;

impl Feature for ShareVersionFeature {
    fn name(&self) -> &'static str {
        crate::metadata_version::SHARE_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::metadata_version::SHARE_VERSION_MIN,
            crate::metadata_version::SHARE_VERSION_MAX,
        )
    }
    fn default_level(&self, _bootstrap_mv: i16) -> i16 {
        // Share groups are opt-in (KIP-932 early access): no released
        // metadata.version enables share.version by default, so the bootstrap
        // default stays at the supported min (0, disabled).
        crate::metadata_version::SHARE_VERSION_MIN
    }
    // dependencies + min_required_floor: inherit the empty/supported-min defaults.
}

/// `streams.version` (KIP-1071). Plain integer feature gating the broker-side
/// Streams rebalance protocol. Default stays at the supported min (0, disabled)
/// — KIP-1071 is early access, so no released metadata.version enables it by
/// default; an operator opts in via `UpdateFeatures`.
pub struct StreamsVersionFeature;

impl Feature for StreamsVersionFeature {
    fn name(&self) -> &'static str {
        crate::metadata_version::STREAMS_VERSION_FEATURE
    }
    fn supported_range(&self) -> (i16, i16) {
        (
            crate::metadata_version::STREAMS_VERSION_MIN,
            crate::metadata_version::STREAMS_VERSION_MAX,
        )
    }
    fn default_level(&self, _bootstrap_mv: i16) -> i16 {
        crate::metadata_version::STREAMS_VERSION_MIN
    }
    // dependencies + min_required_floor: inherit the empty/supported-min
    // defaults. Streams group state lives in __consumer_offsets, not the
    // MetadataImage, so a live-state-aware downgrade floor can't be computed
    // here, mirroring group.version / share.version.
}

/// All features this broker supports finalizing. Single source of truth.
#[must_use]
pub fn feature_registry() -> &'static [&'static dyn Feature] {
    const REGISTRY: &[&dyn Feature] = &[
        &MetadataVersionFeature,
        &GroupVersionFeature,
        &TransactionVersionFeature,
        &ShareVersionFeature,
        &StreamsVersionFeature,
    ];
    REGISTRY
}

/// Look up a registered feature by name.
#[must_use]
pub fn feature(name: &str) -> Option<&'static dyn Feature> {
    feature_registry()
        .iter()
        .copied()
        .find(|f| f.name() == name)
}

/// KIP-584/1022 bootstrap: one `V1FeatureLevel` record per registered feature
/// at its per-release default, derived from `bootstrap_mv` (the bootstrap
/// metadata.version level). Used by both `crabka format` and the broker's
/// standalone self-bootstrap so a fresh cluster finalizes every feature's
/// release default.
#[must_use]
pub fn bootstrap_feature_records(bootstrap_mv: i16) -> Vec<crate::MetadataRecord> {
    bootstrap_feature_records_with_overrides(bootstrap_mv, &BTreeMap::new())
}

/// KIP-1022 `crabka format` seeding: one `V1FeatureLevel` record per registered
/// feature, each at its explicit `--feature NAME=LEVEL` override if present in
/// `overrides`, else its per-release default for `bootstrap_mv`.
///
/// Mirrors `kafka-storage format`: a feature whose resolved level is `0` is
/// **omitted** (level 0 = absent = disabled — the default state), so the seeded
/// record set matches what Kafka writes to its `bootstrap.checkpoint`.
#[must_use]
pub fn bootstrap_feature_records_with_overrides(
    bootstrap_mv: i16,
    overrides: &BTreeMap<String, i16>,
) -> Vec<crate::MetadataRecord> {
    feature_registry()
        .iter()
        .filter_map(|f| {
            let level = overrides
                .get(f.name())
                .copied()
                .unwrap_or_else(|| f.default_level(bootstrap_mv));
            (level > 0).then(|| {
                crate::MetadataRecord::V1FeatureLevel(crate::FeatureLevelRecord {
                    name: f.name().to_string(),
                    level,
                })
            })
        })
        .collect()
}

/// KIP-1022 dependency validation for a fully-resolved feature→level map (as
/// seeded by `crabka format`). For every finalized feature, each of its
/// `dependencies(level)` must be present in `resolved` at `>=` the required
/// level. Returns `Err` naming the first unmet dependency. A no-op for today's
/// registry (no feature declares dependencies) but enforces the rule at format
/// time, mirroring the `UpdateFeatures` handler.
pub fn validate_feature_dependencies(resolved: &BTreeMap<String, i16>) -> Result<(), String> {
    check_deps(resolved, |name, level| {
        feature(name).map_or(&[][..], |f| f.dependencies(level))
    })
}

/// Core of [`validate_feature_dependencies`], parameterized over the dependency
/// source so the rejection logic is unit-testable without a dependency-bearing
/// feature in the real registry.
fn check_deps(
    resolved: &BTreeMap<String, i16>,
    deps_of: impl Fn(&str, i16) -> &'static [(&'static str, i16)],
) -> Result<(), String> {
    for (name, &level) in resolved {
        for &(dep, min) in deps_of(name, level) {
            let have = resolved.get(dep).copied().unwrap_or(0);
            if have < min {
                return Err(format!(
                    "feature {name}={level} requires {dep}>={min}, but {dep} is finalized at {have}"
                ));
            }
        }
    }
    Ok(())
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
    use std::collections::BTreeMap;

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

    #[test]
    fn group_version_registered_with_range() {
        let f = feature("group.version").expect("registered");
        assert!(f.supported_range() == (0, 1));
    }

    #[test]
    fn group_version_default_follows_release() {
        let f = feature("group.version").unwrap();
        assert!(f.default_level(crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL - 1) == 0);
        assert!(f.default_level(crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL) == 1);
        assert!(f.default_level(25) == 1);
    }

    #[test]
    fn group_version_declares_no_hard_dependencies() {
        let f = feature("group.version").unwrap();
        assert!(f.dependencies(0).is_empty());
        assert!(f.dependencies(1).is_empty());
    }

    #[test]
    fn transaction_version_registered() {
        let f = feature("transaction.version").expect("registered");
        assert!(f.supported_range() == (0, 2));
    }

    #[test]
    fn streams_version_registered_opt_in() {
        let f = feature("streams.version").expect("registered");
        assert!(f.supported_range() == (0, 1));
        // KIP-1071 is early access: never auto-enabled by any release level.
        assert!(f.default_level(25) == 0);
        assert!(f.dependencies(1).is_empty());
    }

    #[test]
    fn transaction_version_default_jumps_to_two_at_4_0_iv2() {
        let f = feature("transaction.version").unwrap();
        assert!(f.default_level(23) == 0); // below 4.0-IV2
        assert!(f.default_level(24) == 2); // at 4.0-IV2 → jumps to 2
        assert!(f.default_level(25) == 2);
    }

    #[test]
    fn transaction_version_declares_no_hard_dependencies() {
        let f = feature("transaction.version").unwrap();
        assert!(f.dependencies(0).is_empty());
        assert!(f.dependencies(1).is_empty());
        assert!(f.dependencies(2).is_empty());
    }

    #[test]
    fn transaction_version_ga_threshold_is_4_0_iv2() {
        // Anchor the bare 24 to the metadata.version table (mirrors the
        // group.version GA-threshold anchor test).
        assert!(
            crate::metadata_version::from_feature_level(
                crate::transaction_version::TV2_METADATA_LEVEL
            )
            .unwrap()
            .ivn()
                == "4.0-IV2"
        );
    }

    fn levels_of(recs: &[crate::MetadataRecord]) -> BTreeMap<String, i16> {
        recs.iter()
            .filter_map(|r| match r {
                crate::MetadataRecord::V1FeatureLevel(f) => Some((f.name.clone(), f.level)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn bootstrap_omits_level_zero_features() {
        // share.version / streams.version default to 0 at every release → no
        // record emitted (level 0 = absent = disabled, like Kafka's format).
        let levels = levels_of(&bootstrap_feature_records(25));
        assert!(levels.get("metadata.version") == Some(&25));
        assert!(levels.get("group.version") == Some(&1));
        assert!(levels.get("transaction.version") == Some(&2));
        assert!(!levels.contains_key("share.version"));
        assert!(!levels.contains_key("streams.version"));
    }

    #[test]
    fn bootstrap_with_overrides_applies_explicit_levels() {
        // group.version overridden down to 0 → omitted; the rest follow mv=25.
        let mut ov = BTreeMap::new();
        ov.insert("group.version".to_string(), 0i16);
        let levels = levels_of(&bootstrap_feature_records_with_overrides(25, &ov));
        assert!(levels.get("metadata.version") == Some(&25));
        assert!(levels.get("transaction.version") == Some(&2));
        assert!(!levels.contains_key("group.version"));
    }

    #[test]
    fn bootstrap_override_can_enable_an_opt_in_feature() {
        // streams.version is 0 by default at any release; an explicit override
        // turns it on and earns a record.
        let mut ov = BTreeMap::new();
        ov.insert("streams.version".to_string(), 1i16);
        let levels = levels_of(&bootstrap_feature_records_with_overrides(25, &ov));
        assert!(levels.get("streams.version") == Some(&1));
    }

    #[test]
    fn bootstrap_unlisted_feature_follows_bootstrap_mv() {
        // mv=23: at/above group GA (22) but below txn GA (24) → group=1,
        // transaction omitted (default 0), metadata.version mirrors mv.
        let levels = levels_of(&bootstrap_feature_records_with_overrides(
            23,
            &BTreeMap::new(),
        ));
        assert!(levels.get("metadata.version") == Some(&23));
        assert!(levels.get("group.version") == Some(&1));
        assert!(!levels.contains_key("transaction.version"));
    }

    #[test]
    fn validate_dependencies_ok_for_real_registry() {
        // The real registry declares no dependencies, so any resolved set passes.
        let mut resolved = BTreeMap::new();
        resolved.insert("metadata.version".to_string(), 25i16);
        resolved.insert("group.version".to_string(), 1i16);
        resolved.insert("transaction.version".to_string(), 2i16);
        assert!(validate_feature_dependencies(&resolved).is_ok());
    }

    #[test]
    fn check_deps_enforces_minimum_dependency_levels() {
        // Synthetic dependency source: "b" at level 1 requires "a" >= 2.
        fn deps_of(name: &str, level: i16) -> &'static [(&'static str, i16)] {
            match (name, level) {
                ("b", 1) => &[("a", 2)],
                _ => &[],
            }
        }
        let mut ok = BTreeMap::new();
        ok.insert("a".to_string(), 2i16);
        ok.insert("b".to_string(), 1i16);
        assert!(check_deps(&ok, deps_of).is_ok());

        let mut too_low = BTreeMap::new();
        too_low.insert("a".to_string(), 1i16);
        too_low.insert("b".to_string(), 1i16);
        assert!(check_deps(&too_low, deps_of).is_err());

        // dependency feature absent entirely (treated as level 0) → rejected.
        let mut missing = BTreeMap::new();
        missing.insert("b".to_string(), 1i16);
        assert!(check_deps(&missing, deps_of).is_err());
    }

    #[test]
    fn group_version_ga_threshold_is_4_0_iv0() {
        // Anchor the bare `22` to the metadata.version table so the
        // bootstrap-default threshold can't silently drift from `4.0-IV0`
        // (mirrors metadata_version's `gate_level_constants`).
        assert!(
            crate::metadata_version::from_feature_level(
                crate::group_version::GROUP_VERSION_GA_METADATA_LEVEL
            )
            .unwrap()
            .ivn()
                == "4.0-IV0"
        );
    }

    // --- mutation-coverage tests --------------------------------------------
    //
    // Exercise the per-feature accessors the suite above never reads directly.
    // Some sub-mutants are equivalent and intentionally left: a feature whose
    // supported min is 0 makes "replace floor with 0" a no-op; group/streams
    // `supported_range` and `default_level` mutated to their real `(0,1)` / `0`;
    // `dependencies` `&[]` vs a leaked empty slice; and `validate_feature_
    // dependencies -> Ok(())` (the live registry declares no dependencies).

    #[test]
    fn default_min_required_floor_is_supported_min() {
        // group.version uses the default min_required_floor -> supported min (0).
        let img = MetadataImage::new(uuid::Uuid::nil());
        assert!(feature("group.version").unwrap().min_required_floor(&img) == 0);
    }

    #[test]
    fn metadata_version_min_required_floor_tracks_image() {
        // The override returns the image's min_required_metadata_version.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let mv = feature("metadata.version").unwrap();
        assert!(mv.min_required_floor(&img) == img.min_required_metadata_version());
        // Empty image floor is METADATA_VERSION_MIN (7), distinct from 0/1/-1.
        assert!(mv.min_required_floor(&img) == 7);
    }

    #[test]
    fn plain_features_have_no_level_name() {
        // Integer features use the default level_name -> None.
        assert!(feature("group.version").unwrap().level_name(1).is_none());
        assert!(
            feature("transaction.version")
                .unwrap()
                .level_name(2)
                .is_none()
        );
    }

    #[test]
    fn share_version_accessors() {
        let f = feature("share.version").expect("registered");
        assert!(f.name() == "share.version");
        assert!(f.supported_range() == (0, 1));
        // Opt-in (KIP-932 early access): never auto-enabled by any release.
        assert!(f.default_level(25) == 0);
    }
}
