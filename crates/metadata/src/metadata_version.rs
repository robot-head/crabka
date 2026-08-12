//! KIP-778 `metadata.version` feature-level model. The canonical
//! string<->integer-level table, mirrored byte-for-byte from upstream
//! Kafka's `MetadataVersion` enum over the range Crabka advertises
//! (`[METADATA_VERSION_MIN, METADATA_VERSION_MAX]`). JVM clients call
//! `MetadataVersion.fromFeatureLevel(N)` and throw on any level their
//! enum does not know, so the levels and `X.Y-IVn` names here MUST match
//! upstream exactly. Verify against the cp-kafka 4.0 enum before editing.

/// The `metadata.version` feature name (KIP-584 / KIP-778).
pub const METADATA_VERSION_FEATURE: &str = "metadata.version";

/// Crabka registration-only marker for KIP-1155 downgrade support. The KIP is
/// still under discussion and has not assigned its promised capability
/// `metadata.version` level, so this must not extend the canonical 7..=25
/// metadata-version range or appear in `ApiVersions`. It is carried only in
/// broker/controller registration feature maps; pre-KIP JVM nodes omit it.
pub const METADATA_DOWNGRADE_CAPABILITY_FEATURE: &str = "crabka.metadata.downgrade";
/// The only supported level of [`METADATA_DOWNGRADE_CAPABILITY_FEATURE`].
pub const METADATA_DOWNGRADE_CAPABILITY_LEVEL: i16 = 1;

/// The `share.version` feature name (KIP-932). Gates share-group membership.
pub const SHARE_VERSION_FEATURE: &str = "share.version";
/// KIP-853 Raft protocol and dynamic-membership feature.
pub const KRAFT_VERSION_FEATURE: &str = "kraft.version";
/// Minimum supported `share.version` level: `0` (feature disabled).
pub const SHARE_VERSION_MIN: i16 = 0;
/// Maximum supported `share.version` level: `1` (KIP-932 GA).
pub const SHARE_VERSION_MAX: i16 = 1;

/// The `streams.version` feature name (KIP-1071). Gates the broker-side
/// Streams rebalance protocol (`StreamsGroupHeartbeat` / `StreamsGroupDescribe`).
pub const STREAMS_VERSION_FEATURE: &str = "streams.version";
/// Minimum supported `streams.version` level: `0` (feature disabled).
pub const STREAMS_VERSION_MIN: i16 = 0;
/// Maximum supported `streams.version` level: `1` (KIP-1071 early access).
pub const STREAMS_VERSION_MAX: i16 = 1;

/// Minimum supported level: `3.3-IV3` (`KRaft` GA), the floor that real Kafka
/// 4.0 supports.
pub const METADATA_VERSION_MIN: i16 = 7;
/// Maximum supported level: `4.0-IV3`.
pub const METADATA_VERSION_MAX: i16 = 25;

/// Level at which `KRaft` gained SCRAM credentials (`3.5-IV2`).
pub const SCRAM_MIN_LEVEL: i16 = 11;
/// Level at which `KRaft` gained delegation tokens (`3.6-IV2`).
pub const DELEGATION_TOKEN_MIN_LEVEL: i16 = 14;
/// Lowest level that supports controller registrations (`3.7-IV0`). Online
/// downgrades cannot cross this boundary because the active controller needs
/// those registrations to verify every quorum member supports the target.
pub const ONLINE_DOWNGRADE_MIN_LEVEL: i16 = 15;
/// Level at which partition directory assignments became part of the `KRaft`
/// metadata records (`3.7-IV2`, KIP-858).
pub const DIRECTORY_ASSIGNMENT_MIN_LEVEL: i16 = 17;
/// Level at which partition records gained KIP-966 eligible-leader fields
/// (`4.0-IV1`). Crabka does not model ELR state, but it must still select the
/// record version Kafka readers expect at this metadata version.
pub const ELR_MIN_LEVEL: i16 = 23;

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

/// Resolve a version string to a level. The function accepts both the exact
/// `X.Y-IVn` form and the short `X.Y` form. The short form resolves to the
/// highest level within that minor, which matches
/// `MetadataVersion.fromVersionString`.
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
    use assert2::check;

    use super::*;

    #[test]
    fn min_max_levels() {
        check!(
            (
                METADATA_VERSION_MIN,
                METADATA_VERSION_MAX,
                TABLE.first().unwrap().level,
                TABLE.last().unwrap().level,
            ) == (7, 25, METADATA_VERSION_MIN, METADATA_VERSION_MAX)
        );
    }

    #[test]
    fn share_version_feature_levels() {
        check!(
            (SHARE_VERSION_FEATURE, SHARE_VERSION_MIN, SHARE_VERSION_MAX)
                == ("share.version", 0, 1)
        );
    }

    #[test]
    fn streams_version_feature_levels() {
        check!(
            (
                STREAMS_VERSION_FEATURE,
                STREAMS_VERSION_MIN,
                STREAMS_VERSION_MAX
            ) == ("streams.version", 0, 1)
        );
    }

    #[test]
    fn from_feature_level_known_and_unknown() {
        for (level, want) in [
            (
                7,
                Some(MetadataVersion {
                    level: 7,
                    ivn: "3.3-IV3",
                    short: "3.3",
                }),
            ),
            (
                25,
                Some(MetadataVersion {
                    level: 25,
                    ivn: "4.0-IV3",
                    short: "4.0",
                }),
            ),
            (6, None),
            (26, None),
        ] {
            assert2::assert!(from_feature_level(level) == want);
        }
    }

    #[test]
    fn from_version_string_exact_ivn() {
        for (_case, s, want) in [
            ("known 3.5 IV", "3.5-IV2", Some(11)),
            ("known 4.0 IV", "4.0-IV3", Some(25)),
            ("unknown IV", "3.5-IV9", None),
        ] {
            assert2::assert!(
                from_version_string(s).map(super::MetadataVersion::feature_level) == want
            );
        }
    }

    #[test]
    fn from_version_string_short_picks_highest_in_minor() {
        for (_case, s, want) in [
            ("known 3.7 minor", "3.7", Some(19)),
            ("known 4.0 minor", "4.0", Some(25)),
            ("unsupported minor", "2.8", None),
        ] {
            assert2::assert!(
                from_version_string(s).map(super::MetadataVersion::feature_level) == want
            );
        }
    }

    #[test]
    fn in_supported_range_predicate() {
        for (_case, level, want) in [
            ("minimum", 7, true),
            ("maximum", 25, true),
            ("below minimum", 6, false),
            ("above maximum", 26, false),
        ] {
            assert2::assert!(is_supported_level(level) == want);
        }
    }

    #[test]
    fn gate_level_constants() {
        for (case, level, expected_ivn) in [
            ("SCRAM gate", SCRAM_MIN_LEVEL, "3.5-IV2"),
            (
                "delegation-token gate",
                DELEGATION_TOKEN_MIN_LEVEL,
                "3.6-IV2",
            ),
        ] {
            check!(
                from_feature_level(level).unwrap().ivn() == expected_ivn,
                "case {case}"
            );
        }
    }
}
