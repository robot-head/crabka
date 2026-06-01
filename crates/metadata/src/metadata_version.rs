//! KIP-778 `metadata.version` feature-level model. The canonical
//! string<->integer-level table, mirrored byte-for-byte from upstream
//! Kafka's `MetadataVersion` enum over the range Crabka advertises
//! (`[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`). JVM clients call
//! `MetadataVersion.fromFeatureLevel(N)` and throw on any level their
//! enum doesn't know, so the levels and `X.Y-IVn` names here MUST match
//! upstream exactly. Verify against the cp-kafka 4.0 enum before editing.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub const METADATA_VERSION_FEATURE: &str = "metadata.version";

/// The `share.version` feature name (KIP-932). Gates share-group membership.
pub const SHARE_VERSION_FEATURE: &str = "share.version";
/// Minimum supported `share.version` level: `0` (feature disabled).
pub const SHARE_VERSION_MIN: i16 = 0;
/// Maximum supported `share.version` level: `1` (KIP-932 GA).
pub const SHARE_VERSION_MAX: i16 = 1;

/// Minimum supported level: `3.3-IV3` (`KRaft` GA) — the floor real Kafka
/// 4.0 supports.
pub const METADATA_VERSION_MIN: i16 = 7;
/// Maximum supported level: `4.0-IV3`.
pub const METADATA_VERSION_MAX: i16 = 25;

/// Level at which `KRaft` gained SCRAM credentials (`3.5-IV2`).
pub const SCRAM_MIN_LEVEL: i16 = 11;
/// Level at which `KRaft` gained delegation tokens (`3.6-IV2`).
pub const DELEGATION_TOKEN_MIN_LEVEL: i16 = 14;

/// One `metadata.version` level: its integer feature level, canonical
/// `X.Y-IVn` name, and short `X.Y` form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataVersion {
    level: i16,
    ivn: &'static str,
    short: &'static str,
}

impl MetadataVersion {
    #[must_use]
    pub fn feature_level(self) -> i16 {
        self.level
    }
    #[must_use]
    pub fn ivn(self) -> &'static str {
        self.ivn
    }
    #[must_use]
    pub fn short(self) -> &'static str {
        self.short
    }
}

const TABLE: &[MetadataVersion] = &[
    MetadataVersion {
        level: 7,
        ivn: "3.3-IV3",
        short: "3.3",
    },
    MetadataVersion {
        level: 8,
        ivn: "3.4-IV0",
        short: "3.4",
    },
    MetadataVersion {
        level: 9,
        ivn: "3.5-IV0",
        short: "3.5",
    },
    MetadataVersion {
        level: 10,
        ivn: "3.5-IV1",
        short: "3.5",
    },
    MetadataVersion {
        level: 11,
        ivn: "3.5-IV2",
        short: "3.5",
    },
    MetadataVersion {
        level: 12,
        ivn: "3.6-IV0",
        short: "3.6",
    },
    MetadataVersion {
        level: 13,
        ivn: "3.6-IV1",
        short: "3.6",
    },
    MetadataVersion {
        level: 14,
        ivn: "3.6-IV2",
        short: "3.6",
    },
    MetadataVersion {
        level: 15,
        ivn: "3.7-IV0",
        short: "3.7",
    },
    MetadataVersion {
        level: 16,
        ivn: "3.7-IV1",
        short: "3.7",
    },
    MetadataVersion {
        level: 17,
        ivn: "3.7-IV2",
        short: "3.7",
    },
    MetadataVersion {
        level: 18,
        ivn: "3.7-IV3",
        short: "3.7",
    },
    MetadataVersion {
        level: 19,
        ivn: "3.7-IV4",
        short: "3.7",
    },
    MetadataVersion {
        level: 20,
        ivn: "3.8-IV0",
        short: "3.8",
    },
    MetadataVersion {
        level: 21,
        ivn: "3.9-IV0",
        short: "3.9",
    },
    MetadataVersion {
        level: 22,
        ivn: "4.0-IV0",
        short: "4.0",
    },
    MetadataVersion {
        level: 23,
        ivn: "4.0-IV1",
        short: "4.0",
    },
    MetadataVersion {
        level: 24,
        ivn: "4.0-IV2",
        short: "4.0",
    },
    MetadataVersion {
        level: 25,
        ivn: "4.0-IV3",
        short: "4.0",
    },
];

/// Look up a level by integer feature level. `None` if outside the
/// supported table.
#[must_use]
pub fn from_feature_level(level: i16) -> Option<MetadataVersion> {
    TABLE.iter().copied().find(|m| m.level == level)
}

/// Resolve a version string to a level. Accepts both the exact `X.Y-IVn`
/// form and the short `X.Y` form; the short form resolves to the highest
/// level within that minor (matching `MetadataVersion.fromVersionString`).
#[must_use]
pub fn from_version_string(s: &str) -> Option<MetadataVersion> {
    let s = s.trim();
    if s.contains('-') {
        return TABLE.iter().copied().find(|m| m.ivn == s);
    }
    TABLE
        .iter()
        .copied()
        .filter(|m| m.short == s)
        .max_by_key(|m| m.level)
}

/// True if `level` is within `[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`.
#[must_use]
pub fn is_supported_level(level: i16) -> bool {
    (METADATA_VERSION_MIN..=METADATA_VERSION_MAX).contains(&level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn min_max_levels() {
        assert!(METADATA_VERSION_MIN == 7);
        assert!(METADATA_VERSION_MAX == 25);
        assert!(TABLE.first().unwrap().level == METADATA_VERSION_MIN);
        assert!(TABLE.last().unwrap().level == METADATA_VERSION_MAX);
    }

    #[test]
    fn share_version_feature_levels() {
        assert!(SHARE_VERSION_FEATURE == "share.version");
        assert!(SHARE_VERSION_MIN == 0);
        assert!(SHARE_VERSION_MAX == 1);
    }

    #[test]
    fn from_feature_level_known_and_unknown() {
        assert!(from_feature_level(7).unwrap().short() == "3.3");
        assert!(from_feature_level(25).unwrap().ivn() == "4.0-IV3");
        assert!(from_feature_level(6).is_none());
        assert!(from_feature_level(26).is_none());
    }

    #[test]
    fn from_version_string_exact_ivn() {
        assert!(from_version_string("3.5-IV2").unwrap().feature_level() == 11);
        assert!(from_version_string("4.0-IV3").unwrap().feature_level() == 25);
        assert!(from_version_string("3.5-IV9").is_none());
    }

    #[test]
    fn from_version_string_short_picks_highest_in_minor() {
        assert!(from_version_string("3.7").unwrap().feature_level() == 19);
        assert!(from_version_string("4.0").unwrap().feature_level() == 25);
        assert!(from_version_string("2.8").is_none());
    }

    #[test]
    fn in_supported_range_predicate() {
        assert!(is_supported_level(7));
        assert!(is_supported_level(25));
        assert!(!is_supported_level(6));
        assert!(!is_supported_level(26));
    }

    #[test]
    fn gate_level_constants() {
        assert!(SCRAM_MIN_LEVEL == 11);
        assert!(DELEGATION_TOKEN_MIN_LEVEL == 14);
        assert!(from_feature_level(SCRAM_MIN_LEVEL).unwrap().ivn() == "3.5-IV2");
        assert!(
            from_feature_level(DELEGATION_TOKEN_MIN_LEVEL)
                .unwrap()
                .ivn()
                == "3.6-IV2"
        );
    }
}
