//! KIP-848 `group.version` feature-level constants. It is a plain integer
//! feature with no `X.Y-IVn` string table: level 0 means classic consumer
//! groups only, and level 1 means next-gen (KIP-848) protocol GA. The range and
//! the metadata.version bootstrap-default threshold are pinned against the
//! cp-kafka 4.0 `GroupVersion` enum, verified empirically 2026-05-30.

/// KIP-848 feature name.
pub const GROUP_VERSION_FEATURE: &str = "group.version";

/// Minimum supported level: classic-only.
pub const GROUP_VERSION_MIN: i16 = 0;
/// Maximum supported level: next-gen (KIP-848) GA.
pub const GROUP_VERSION_MAX: i16 = 1;

/// `group.version=1` is the bootstrap default once the formatted
/// metadata.version is at least this level (4.0-IV0 = 22). This is a
/// bootstrap-default input only. Kafka declares no hard `UpdateFeatures`
/// dependency for group.version.
pub const GROUP_VERSION_GA_METADATA_LEVEL: i16 = 22;
