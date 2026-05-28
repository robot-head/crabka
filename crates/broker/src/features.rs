//! KIP-584 supported-feature table. Drives both the `ApiVersions`
//! `supported_features` advertisement and the `UpdateFeatures` validation
//! path so the two can never disagree about what this broker supports.
//!
//! `metadata.version` is advertised at a single conservative level (1 =
//! `3.0-IV1`). JVM clients validate finalized + supported `metadata.version`
//! levels via `MetadataVersion.fromFeatureLevel(N)` and throw on a level
//! their enum doesn't know; level 1 is known to every KRaft-aware client
//! (Kafka >= 3.0). Raising `METADATA_VERSION_MAX` REQUIRES re-running the
//! Docker `jvm_acceptance` suite — see the slice plan's compatibility note.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub(crate) const METADATA_VERSION: &str = "metadata.version";
/// Minimum supported `metadata.version` level.
pub(crate) const METADATA_VERSION_MIN: i16 = 1;
/// Maximum supported `metadata.version` level. Conservative on purpose —
/// see the module note before raising it.
pub(crate) const METADATA_VERSION_MAX: i16 = 1;

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
    supported_features().iter().copied().find(|f| f.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_version_is_supported() {
        let f = lookup(METADATA_VERSION).expect("metadata.version supported");
        assert_eq!(f.min_version, 1);
        assert_eq!(f.max_version, METADATA_VERSION_MAX);
        assert!(lookup("not.a.feature").is_none());
    }
}
