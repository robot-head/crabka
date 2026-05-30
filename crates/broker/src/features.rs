//! KIP-584 supported-feature surface for the broker. Re-exports the
//! `crabka_metadata` feature registry and derives the `ApiVersions`
//! advertisement rows from it, so the advertised and validated feature sets
//! can never disagree. Behavioral gating helpers (`require_feature`) live here
//! because they return broker error codes.

pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
// Re-exported for `ApiVersions` tests / range-bound assertions; consumed only
// from `#[cfg(test)]` modules, so the non-test lib target sees them as unused.
#[allow(unused_imports)]
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MAX;
#[allow(unused_imports)]
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
        assert!(
            require_feature(&image, METADATA_VERSION, 11) == Err(crate::codes::UNSUPPORTED_VERSION)
        );
        assert!(require_feature(&image, METADATA_VERSION, 10).is_ok());
        assert!(require_feature(&image, METADATA_VERSION, 7).is_ok());
    }
}
