//! KIP-584 supported-feature table. Drives both the `ApiVersions`
//! `supported_features` advertisement and the `UpdateFeatures` validation
//! path so the two can never disagree about what this broker supports.
//!
//! The canonical `metadata.version` string/level table lives in
//! [`crabka_metadata::metadata_version`]; this module re-exports the three
//! constants so local code keeps its short `crate::features::*` paths.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_FEATURE as METADATA_VERSION;
/// Maximum supported `metadata.version` level.
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MAX;
/// Minimum supported `metadata.version` level.
pub(crate) use crabka_metadata::metadata_version::METADATA_VERSION_MIN;

/// One row of the supported-feature table.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SupportedFeature {
    pub name: &'static str,
    pub min_version: i16,
    pub max_version: i16,
}

/// The features this broker supports finalizing.
pub(crate) fn supported_features() -> &'static [SupportedFeature] {
    const TABLE: &[SupportedFeature] = &[SupportedFeature {
        name: METADATA_VERSION,
        min_version: METADATA_VERSION_MIN,
        max_version: METADATA_VERSION_MAX,
    }];
    TABLE
}

/// Look up a supported feature by name.
pub(crate) fn lookup(name: &str) -> Option<SupportedFeature> {
    supported_features()
        .iter()
        .copied()
        .find(|f| f.name == name)
}

/// True when a feature requiring `required_level` must be blocked given
/// the `finalized` metadata.version. A missing finalized level (`None`,
/// `MetadataVersion.UNKNOWN`) is permissive — there is no level to gate
/// against — matching the runtime range guard's treatment.
pub(crate) fn metadata_version_blocks(finalized: Option<i16>, required_level: i16) -> bool {
    finalized.is_some_and(|level| level < required_level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn metadata_version_is_supported() {
        let f = lookup(METADATA_VERSION).expect("metadata.version supported");
        assert!(f.min_version == crabka_metadata::metadata_version::METADATA_VERSION_MIN);
        assert!(f.max_version == crabka_metadata::metadata_version::METADATA_VERSION_MAX);
        assert!(f.min_version == 7);
        assert!(f.max_version == 25);
        assert!(lookup("not.a.feature").is_none());
    }

    #[test]
    fn metadata_version_blocks_is_permissive_on_unknown() {
        assert!(!metadata_version_blocks(None, 11));
        assert!(metadata_version_blocks(Some(10), 11));
        assert!(!metadata_version_blocks(Some(11), 11));
    }
}
