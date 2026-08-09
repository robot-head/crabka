//! KIP-584 supported-feature surface for the broker. This module re-exports
//! the `crabka_metadata` feature registry and derives the `ApiVersions`
//! advertisement rows from it, so the advertised and the validated feature
//! sets can never disagree. The behavioral gating helper `require_feature`
//! lives here because it returns broker error codes.

use crabka_metadata::MetadataImage;
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
// Re-exported for `ApiVersions` tests / range-bound assertions; consumed only
// from `#[cfg(test)]` modules, so the non-test lib target sees them as unused.
#[allow(unused_imports)]
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MAX;
#[allow(unused_imports)]
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MIN;
/// The `share.version` feature name (KIP-932). Only the `#[cfg(test)]`
/// module that asserts share.version is advertised uses it.
#[allow(unused_imports)]
pub(crate) use crabka_metadata::metadata_version::SHARE_VERSION_FEATURE as SHARE_VERSION;
/// The `streams.version` feature name (KIP-1071). It gates
/// `StreamsGroupHeartbeat` and `StreamsGroupDescribe`. Those handlers read it
/// with `feature_enabled`.
pub(crate) use crabka_metadata::metadata_version::STREAMS_VERSION_FEATURE as STREAMS_VERSION;

/// One row of the `ApiVersions.supported_features` advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing. They come from the
/// `crabka_metadata` registry, the single source of truth.
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

/// Look up a supported feature by name. It pairs with `supported_features` as
/// the module's feature-surface API. The `UpdateFeatures` handler resolves the
/// registry feature directly, so the non-test lib target sees this as unused.
#[allow(dead_code)]
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

/// KIP-584 admission gate. Returns `Err(UNSUPPORTED_VERSION)` when `name` is
/// finalized below `required_level`. It is permissive when the feature is
/// unfinalized, because there is no level to gate against. This matches how
/// the range guard treats a missing level.
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

/// True when `name` is finalized at >= `level`. It treats an UNFINALIZED
/// feature as level 0, which is disabled. Use it for features where absence
/// means "off", for example `group.version` → next-gen disabled. This differs
/// from `require_feature`, which is permissive on absence and serves
/// metadata.version-gated RPCs on legacy images.
pub(crate) fn feature_enabled(
    image: &crabka_metadata::MetadataImage,
    name: &str,
    level: i16,
) -> bool {
    image.finalized_features().get(name).copied().unwrap_or(0) >= level
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn feature_enabled_treats_absence_as_disabled() {
        use crabka_metadata::{FeatureLevelRecord, MetadataRecord};
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        assert!(!feature_enabled(&image, "group.version", 1)); // absent → disabled
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "group.version".into(),
            level: 1,
        }));
        assert!(feature_enabled(&image, "group.version", 1)); // present at 1 → enabled
    }

    #[test]
    fn supported_features_include_metadata_version() {
        let expected = SupportedFeature {
            name: METADATA_VERSION,
            min_version: METADATA_VERSION_MIN,
            max_version: METADATA_VERSION_MAX,
        };
        assert!(lookup(METADATA_VERSION) == Some(expected));
        assert!(lookup("not.a.feature").is_none());
    }

    #[test]
    fn share_version_is_supported() {
        let expected = SupportedFeature {
            name: SHARE_VERSION,
            min_version: 0,
            max_version: 1,
        };
        assert!(lookup(SHARE_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features()
                .iter()
                .any(|f| f.name == SHARE_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    #[test]
    fn streams_version_is_supported() {
        let expected = SupportedFeature {
            name: STREAMS_VERSION,
            min_version: 0,
            max_version: 1,
        };
        assert!(lookup(STREAMS_VERSION) == Some(expected));
        // Advertised via the registry-derived supported-feature table.
        assert!(
            supported_features()
                .iter()
                .any(|f| f.name == STREAMS_VERSION && f.min_version == 0 && f.max_version == 1)
        );
    }

    #[test]
    fn require_feature_is_permissive_on_unfinalized() {
        let image = MetadataImage::new(uuid::Uuid::nil());
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
        for (required_level, want) in [
            (11, Err(crate::codes::UNSUPPORTED_VERSION)),
            (10, Ok(())),
            (7, Ok(())),
        ] {
            assert!(
                require_feature(&image, METADATA_VERSION, required_level) == want,
                "level {required_level}"
            );
        }
    }
}
