//! Kafka version + metadata-version model for upgrade orchestration.
//!
//! Crabka is `KRaft`-only, so the only feature-level knob we model is
//! `metadata.version` — the runtime analog of the ZK-era
//! `inter.broker.protocol.version`. There is no
//! `inter.broker.protocol.version` / `log.message.format.version` lineage.
//!
//! The broker enforces metadata.version at runtime (`UpdateFeatures` handler +
//! fail-fast range guard), consuming the value seeded by `crabka format
//! --release-version`. The operator owns upgrade-window safety: the binary
//! must always be `>= resolved metadata >= finalized metadata`.

/// A parsed Kafka version. Ordering is by `(major, minor, patch)`, but
/// metadata-version comparisons use only `(major, minor)` — Kafka feature
/// levels are keyed by the release minor, not the patch.
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
    /// Parse `X`, `X.Y`, or `X.Y.Z`, tolerating a trailing IBP/feature
    /// suffix (`3.7-IV2`). The suffix is dropped — feature levels within a
    /// release minor are not modeled.
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

    /// The `(major, minor)` key used for metadata-version comparisons.
    #[must_use]
    pub fn metadata_key(&self) -> (u32, u32) {
        (self.major, self.minor)
    }

    /// Canonical `major.minor` rendering — the on-wire form for the
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

/// Outcome of evaluating a `Kafka`'s declared versions against the
/// finalized metadata version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionOutcome {
    /// Versions are compatible. `resolved_metadata` is the canonical
    /// `major.minor` to render into broker config and finalize in status.
    Valid { resolved_metadata: String },
    /// Versions are incompatible; the reason + human message feed a
    /// `KafkaVersionValid=False` condition and block the roll.
    Invalid {
        reason: VersionReason,
        message: String,
    },
}

/// Validate the declared `kafka_version` and (optional) pinned
/// `spec_metadata_version` against the operator-finalized metadata version
/// (`status.metadataVersion`).
///
/// Invariant on success: `binary >= resolved metadata >= finalized
/// metadata`. The two inequalities are the downgrade window — a binary can
/// never drop below the finalized metadata version, and the metadata
/// version never regresses.
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
    fn parse_success_cases() {
        for (name, input, expected) in [
            (
                "major minor patch",
                "3.7.1",
                KafkaVersion {
                    major: 3,
                    minor: 7,
                    patch: 1,
                },
            ),
            (
                "major minor",
                "3.7",
                KafkaVersion {
                    major: 3,
                    minor: 7,
                    patch: 0,
                },
            ),
            (
                "bare major",
                "4",
                KafkaVersion {
                    major: 4,
                    minor: 0,
                    patch: 0,
                },
            ),
            (
                "IBP suffix",
                "3.7-IV2",
                KafkaVersion {
                    major: 3,
                    minor: 7,
                    patch: 0,
                },
            ),
        ] {
            assert_eq!(KafkaVersion::parse(input).unwrap(), expected, "case {name}");
        }
    }

    #[test]
    fn parse_rejects_junk() {
        for input in ["banana", "", "3.x", "1.2.3.4"] {
            assert!(KafkaVersion::parse(input).is_err(), "case {input:?}");
        }
    }

    #[test]
    fn evaluate_cases() {
        for (name, binary, pin, finalized, expected) in [
            ("default tracks binary", "3.7.0", None, None, Ok("3.7")),
            (
                "explicit pin below binary",
                "3.7.0",
                Some("3.6"),
                None,
                Ok("3.6"),
            ),
            ("pin equals binary", "3.7", Some("3.7-IV4"), None, Ok("3.7")),
            (
                "pin above binary",
                "3.6.0",
                Some("3.7"),
                None,
                Err(VersionReason::MetadataVersionTooHigh),
            ),
            (
                "metadata downgrade",
                "3.7.0",
                Some("3.6"),
                Some("3.7"),
                Err(VersionReason::MetadataVersionDowngrade),
            ),
            (
                "binary downgrade",
                "3.6.0",
                None,
                Some("3.7"),
                Err(VersionReason::MetadataVersionDowngrade),
            ),
            ("same finalized", "3.7.0", None, Some("3.7"), Ok("3.7")),
            (
                "upgrade above finalized",
                "3.8.0",
                None,
                Some("3.7"),
                Ok("3.8"),
            ),
            (
                "invalid binary",
                "nope",
                None,
                None,
                Err(VersionReason::InvalidVersion),
            ),
            (
                "invalid pin",
                "3.7.0",
                Some("nope"),
                None,
                Err(VersionReason::InvalidVersion),
            ),
            (
                "unparseable finalized ignored",
                "3.7.0",
                None,
                Some("garbage"),
                Ok("3.7"),
            ),
            (
                "below broker minimum",
                "3.2.0",
                None,
                None,
                Err(VersionReason::MetadataVersionTooLow),
            ),
            ("at broker minimum", "3.7.0", None, None, Ok("3.7")),
        ] {
            let actual = match evaluate(binary, pin, finalized) {
                VersionOutcome::Valid { resolved_metadata } => Ok(resolved_metadata),
                VersionOutcome::Invalid { reason, .. } => Err(reason),
            };
            assert_eq!(actual, expected.map(str::to_string), "case {name}");
        }
    }
}
