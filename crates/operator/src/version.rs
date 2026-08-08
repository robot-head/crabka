//! Kafka version and metadata-version model for upgrade orchestration.
//!
//! Crabka supports only `KRaft`, so this module models one feature-level
//! setting: `metadata.version`. It is the runtime equivalent of the ZK-era
//! `inter.broker.protocol.version`. There is no
//! `inter.broker.protocol.version` or `log.message.format.version`
//! lineage.
//!
//! The broker enforces metadata.version at runtime, in the
//! `UpdateFeatures` handler and in a fail-fast range guard. It reads the
//! value that `crabka format --release-version` seeded. The operator owns
//! the safety of the upgrade window. The binary must always be
//! `>= resolved metadata >= finalized metadata`.

/// A parsed Kafka version.
///
/// The order is by `(major, minor, patch)`. Metadata-version comparisons
/// use only `(major, minor)`, because Kafka keys the feature levels by the
/// release minor and not by the patch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct KafkaVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

/// A version string could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid version string: {0:?}")]
pub struct VersionError(pub String);

impl KafkaVersion {
    /// Parses `X`, `X.Y`, or `X.Y.Z`. A trailing IBP or feature suffix
    /// such as `3.7-IV2` is acceptable. This method drops the suffix,
    /// because the model has no feature levels inside a release minor.
    /// # Errors
    /// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
    pub fn parse(s: &str) -> Result<Self, VersionError> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(VersionError(s.to_string()));
        }
        // Drop an IBP-style suffix: "3.7-IV2" -> "3.7".
        let core = trimmed.split('-').next().unwrap_or(trimmed);
        let mut parts = core.split('.');
        let major = parse_component(parts.next(), s)?;
        let minor = match parts.next() {
            Some(p) => parse_component(Some(p), s)?,
            None => 0,
        };
        let patch = match parts.next() {
            Some(p) => parse_component(Some(p), s)?,
            None => 0,
        };
        if parts.next().is_some() {
            // More than three dot-separated components is not a version.
            return Err(VersionError(s.to_string()));
        }
        Ok(Self {
            major,
            minor,
            patch,
        })
    }

    /// The `(major, minor)` key for the metadata-version comparisons.
    #[must_use]
    pub fn metadata_key(&self) -> (u32, u32) {
        (self.major, self.minor)
    }

    /// Canonical `major.minor` rendering. This is the on-wire form of the
    /// metadata version.
    #[must_use]
    pub fn short(&self) -> String {
        format!("{}.{}", self.major, self.minor)
    }
}

fn parse_component(c: Option<&str>, original: &str) -> Result<u32, VersionError> {
    c.and_then(|p| p.parse::<u32>().ok())
        .ok_or_else(|| VersionError(original.to_string()))
}

/// Machine reason for a `KafkaVersionValid=False` condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
pub enum VersionReason {
    /// `kafkaVersion` or `metadataVersion` did not parse.
    InvalidVersion,
    /// The resolved metadata version is newer than the binary.
    MetadataVersionTooHigh,
    /// The resolved metadata version is below the broker's supported floor.
    MetadataVersionTooLow,
    /// The resolved metadata version is older than the finalized one.
    MetadataVersionDowngrade,
}

impl VersionReason {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Outcome of the evaluation of the declared versions of a `Kafka`
/// against the finalized metadata version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionOutcome {
    /// The versions are compatible. `resolved_metadata` is the canonical
    /// `major.minor` that the operator renders into the broker config and
    /// finalizes in the status.
    Valid { resolved_metadata: String },
    /// The versions are incompatible. The reason and the message go into
    /// a `KafkaVersionValid=False` condition and block the roll.
    Invalid {
        reason: VersionReason,
        message: String,
    },
}

/// Validates the declared `kafka_version` and the optional pinned
/// `spec_metadata_version` against the metadata version that the operator
/// finalized in `status.metadataVersion`.
///
/// On success this invariant holds:
/// `binary >= resolved metadata >= finalized metadata`. The two
/// inequalities define the downgrade window. A binary can never drop below
/// the finalized metadata version, and the metadata version never goes
/// backward.
#[must_use]
pub fn evaluate(
    kafka_version: &str,
    spec_metadata_version: Option<&str>,
    finalized_metadata_version: Option<&str>,
) -> VersionOutcome {
    let Ok(binary) = KafkaVersion::parse(kafka_version) else {
        return VersionOutcome::Invalid {
            reason: VersionReason::InvalidVersion,
            message: format!("spec.kafkaVersion {kafka_version:?} is not a valid version"),
        };
    };

    let resolved = match spec_metadata_version {
        Some(raw) => {
            let Ok(v) = KafkaVersion::parse(raw) else {
                return VersionOutcome::Invalid {
                    reason: VersionReason::InvalidVersion,
                    message: format!("spec.metadataVersion {raw:?} is not a valid version"),
                };
            };
            v
        }
        None => binary,
    };

    if resolved.metadata_key() > binary.metadata_key() {
        return VersionOutcome::Invalid {
            reason: VersionReason::MetadataVersionTooHigh,
            message: format!(
                "metadata.version {} is newer than kafkaVersion {}; upgrade the binary first",
                resolved.short(),
                binary.short()
            ),
        };
    }

    // The broker aborts on a finalized metadata.version below its
    // supported floor (3.3-IV3). Refuse to inject one.
    if let Some(mv) = crabka_metadata::metadata_version::from_version_string(&resolved.short()) {
        if mv.feature_level() < crabka_metadata::metadata_version::METADATA_VERSION_MIN {
            return VersionOutcome::Invalid {
                reason: VersionReason::MetadataVersionTooLow,
                message: format!(
                    "metadata.version {} is below the broker's supported floor (3.3-IV3)",
                    resolved.short()
                ),
            };
        }
    } else {
        return VersionOutcome::Invalid {
            reason: VersionReason::MetadataVersionTooLow,
            message: format!(
                "metadata.version {} is not a supported level",
                resolved.short()
            ),
        };
    }

    if let Some(finalized_raw) = finalized_metadata_version
        && let Ok(finalized) = KafkaVersion::parse(finalized_raw)
        && resolved.metadata_key() < finalized.metadata_key()
    {
        return VersionOutcome::Invalid {
            reason: VersionReason::MetadataVersionDowngrade,
            message: format!(
                "metadata.version {} is older than the finalized {}; metadata.version cannot be downgraded",
                resolved.short(),
                finalized.short()
            ),
        };
    }

    VersionOutcome::Valid {
        resolved_metadata: resolved.short(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn parse_major_minor_patch() {
        assert!(
            KafkaVersion::parse("3.7.1").unwrap()
                == KafkaVersion {
                    major: 3,
                    minor: 7,
                    patch: 1
                }
        );
    }

    #[test]
    fn parse_major_minor() {
        let v = KafkaVersion::parse("3.7").unwrap();
        assert!(v.metadata_key() == (3, 7));
        assert!(v.short() == "3.7");
    }

    #[test]
    fn parse_bare_major() {
        assert!(KafkaVersion::parse("4").unwrap().metadata_key() == (4, 0));
    }

    #[test]
    fn parse_strips_ibp_suffix() {
        assert!(KafkaVersion::parse("3.7-IV2").unwrap().short() == "3.7");
    }

    #[test]
    fn parse_rejects_junk() {
        for input in ["banana", "", "3.x", "1.2.3.4"] {
            assert!(KafkaVersion::parse(input).is_err(), "case {input:?}");
        }
    }

    #[test]
    fn evaluate_default_tracks_binary() {
        let out = evaluate("3.7.0", None, None);
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.7".into()
            }
        );
    }

    #[test]
    fn evaluate_explicit_pin_below_binary_ok() {
        let out = evaluate("3.7.0", Some("3.6"), None);
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.6".into()
            }
        );
    }

    #[test]
    fn evaluate_pin_equal_binary_ok() {
        let out = evaluate("3.7", Some("3.7-IV4"), None);
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.7".into()
            }
        );
    }

    #[test]
    fn evaluate_pin_above_binary_rejected() {
        let out = evaluate("3.6.0", Some("3.7"), None);
        match out {
            VersionOutcome::Invalid { reason, .. } => {
                assert!(reason == VersionReason::MetadataVersionTooHigh);
            }
            other @ VersionOutcome::Valid { .. } => {
                panic!("expected MetadataVersionTooHigh, got {other:?}")
            }
        }
    }

    #[test]
    fn evaluate_metadata_downgrade_rejected() {
        // Finalized at 3.7; pinning back to 3.6 is forbidden.
        let out = evaluate("3.7.0", Some("3.6"), Some("3.7"));
        match out {
            VersionOutcome::Invalid { reason, .. } => {
                assert!(reason == VersionReason::MetadataVersionDowngrade);
            }
            other @ VersionOutcome::Valid { .. } => {
                panic!("expected MetadataVersionDowngrade, got {other:?}")
            }
        }
    }

    #[test]
    fn evaluate_binary_downgrade_below_finalized_rejected() {
        // Finalized at 3.7, default-tracking; downgrading the binary to 3.6
        // drags the resolved metadata to 3.6 < finalized 3.7.
        let out = evaluate("3.6.0", None, Some("3.7"));
        match out {
            VersionOutcome::Invalid { reason, .. } => {
                assert!(reason == VersionReason::MetadataVersionDowngrade);
            }
            other @ VersionOutcome::Valid { .. } => {
                panic!("expected MetadataVersionDowngrade, got {other:?}")
            }
        }
    }

    #[test]
    fn evaluate_same_finalized_is_ok() {
        let out = evaluate("3.7.0", None, Some("3.7"));
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.7".into()
            }
        );
    }

    #[test]
    fn evaluate_upgrade_above_finalized_ok() {
        let out = evaluate("3.8.0", None, Some("3.7"));
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.8".into()
            }
        );
    }

    #[test]
    fn evaluate_invalid_binary() {
        let out = evaluate("nope", None, None);
        match out {
            VersionOutcome::Invalid { reason, .. } => {
                assert!(reason == VersionReason::InvalidVersion);
            }
            other @ VersionOutcome::Valid { .. } => {
                panic!("expected InvalidVersion, got {other:?}")
            }
        }
    }

    #[test]
    fn evaluate_invalid_pin() {
        let out = evaluate("3.7.0", Some("nope"), None);
        match out {
            VersionOutcome::Invalid { reason, .. } => {
                assert!(reason == VersionReason::InvalidVersion);
            }
            other @ VersionOutcome::Valid { .. } => {
                panic!("expected InvalidVersion, got {other:?}")
            }
        }
    }

    #[test]
    fn evaluate_unparseable_finalized_is_ignored() {
        // A malformed finalized value should not block progress.
        let out = evaluate("3.7.0", None, Some("garbage"));
        assert!(
            out == VersionOutcome::Valid {
                resolved_metadata: "3.7".into()
            }
        );
    }

    #[test]
    fn resolved_below_broker_min_is_too_low() {
        // 3.2 maps below the broker's metadata.version floor (3.3-IV3).
        let out = evaluate("3.2.0", None, None);
        assert!(matches!(
            out,
            VersionOutcome::Invalid {
                reason: VersionReason::MetadataVersionTooLow,
                ..
            }
        ));
    }

    #[test]
    fn resolved_at_or_above_min_is_valid() {
        let out = evaluate("3.7.0", None, None);
        assert!(matches!(out, VersionOutcome::Valid { .. }));
    }
}
