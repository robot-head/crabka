//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! Eight keys are recognized. Five propagate live to `Log.config`
//! (`retention.ms`, `retention.bytes`, `segment.bytes`,
//! `cleanup.policy`, `compression.type`). One is accepted but not yet
//! enforced: `min.insync.replicas` (integers >= 1 accepted but not yet
//! enforced — see the design spec for the rationale). Two are KIP-73
//! throttle keys (`leader.replication.throttled.replicas`,
//! `follower.replication.throttled.replicas`) validated via
//! `ThrottledReplicas::parse`.
//!
//! Unknown keys are rejected with `INVALID_CONFIG`.

// Items are `pub(crate)` for downstream handlers (Tasks 7-10); until those
// modules land they appear unused to the compiler.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use crabka_log::LogConfig;

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";

/// Validate a single key/value pair. `Err(reason)` carries an
/// operator-readable explanation that the handler propagates into the
/// `error_message` field of the response.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    match key {
        RETENTION_MS | RETENTION_BYTES => parse_i64_at_least(-1, value).map(|_| ()),
        SEGMENT_BYTES => parse_u64_at_least(1, value).map(|_| ()),
        CLEANUP_POLICY => match value {
            "delete" | "compact" => Ok(()),
            _ => Err(format!(
                "cleanup.policy={value} not supported; expected `delete` or `compact`"
            )),
        },
        COMPRESSION_TYPE => parse_compression_type(value).map(|_| ()),
        MIN_INSYNC_REPLICAS => parse_i64_at_least(1, value).map(|_| ()),
        crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
        | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY => {
            crate::throttle::ThrottledReplicas::parse(value).map(|_| ())
        }
        unknown => Err(format!("unrecognized config key `{unknown}`")),
    }
}

/// Map the wire-side `compression.type` value to the matching
/// [`LogConfig::compression_type`]. Returns `Ok(None)` for the special
/// `producer` value (the Kafka default; no broker-side re-encoding).
/// `Ok(Some(_))` for any concrete codec. `Err` for an unknown name.
pub(crate) fn parse_compression_type(
    value: &str,
) -> Result<Option<crabka_compression::CompressionType>, String> {
    use crabka_compression::CompressionType;
    match value {
        "producer" => Ok(None),
        "uncompressed" | "none" => Ok(Some(CompressionType::None)),
        "gzip" => Ok(Some(CompressionType::Gzip)),
        "snappy" => Ok(Some(CompressionType::Snappy)),
        "lz4" => Ok(Some(CompressionType::Lz4)),
        "zstd" => Ok(Some(CompressionType::Zstd)),
        other => Err(format!(
            "compression.type=`{other}` not recognized; expected one of \
             producer, uncompressed, gzip, snappy, lz4, zstd"
        )),
    }
}

fn parse_i64_at_least(min: i64, value: &str) -> Result<i64, String> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| format!("expected integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

fn parse_u64_at_least(min: u64, value: &str) -> Result<u64, String> {
    let parsed: u64 = value
        .parse()
        .map_err(|_| format!("expected non-negative integer, got `{value}`"))?;
    if parsed < min {
        return Err(format!("value `{value}` must be >= {min}"));
    }
    Ok(parsed)
}

/// Returns `true` if `key` is one of the eight whitelisted topic-config keys.
/// Useful for `IncrementalAlterConfigs` DELETE-op validation without
/// requiring a sentinel probe value.
pub(crate) fn is_recognized(key: &str) -> bool {
    matches!(
        key,
        RETENTION_MS
            | RETENTION_BYTES
            | SEGMENT_BYTES
            | CLEANUP_POLICY
            | COMPRESSION_TYPE
            | MIN_INSYNC_REPLICAS
            | crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
            | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY
    )
}

/// Merge `overrides` over `base` and return a fresh `LogConfig` to push
/// into `Log::set_config`. Unknown keys are silently dropped because
/// `validate_topic_config` should have rejected them at `AlterConfigs` time
/// before the record reached the metadata image; this function is the
/// applier and treats the input as already-validated.
#[must_use]
pub(crate) fn apply_to_log_config(
    overrides: &BTreeMap<String, String>,
    base: &LogConfig,
) -> LogConfig {
    let mut out = base.clone();
    for (k, v) in overrides {
        match k.as_str() {
            RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    out.retention_ms = if ms < 0 {
                        None
                    } else {
                        Some(Duration::from_millis(
                            u64::try_from(ms).expect("validated non-negative above"),
                        ))
                    };
                }
            }
            RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.retention_bytes = if b < 0 {
                        None
                    } else {
                        Some(u64::try_from(b).expect("validated non-negative above"))
                    };
                }
            }
            SEGMENT_BYTES => {
                if let Ok(b) = v.parse::<u64>() {
                    out.segment_bytes = b;
                }
            }
            CLEANUP_POLICY => {
                out.cleanup_policy = if v == "compact" {
                    crabka_log::CleanupPolicy::Compact
                } else {
                    crabka_log::CleanupPolicy::Delete
                };
            }
            COMPRESSION_TYPE => {
                if let Ok(target) = parse_compression_type(v) {
                    out.compression_type = target;
                }
            }
            // The remaining keys are recognized but no broker behavior is
            // wired to them yet (see module docs).
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_retention_ms_accepts_positive_and_minus_one() {
        assert!(validate_topic_config(RETENTION_MS, "60000").is_ok());
        assert!(validate_topic_config(RETENTION_MS, "-1").is_ok());
    }

    #[test]
    fn validate_retention_ms_rejects_below_minus_one() {
        assert!(validate_topic_config(RETENTION_MS, "-5").is_err());
    }

    #[test]
    fn validate_retention_ms_rejects_non_integer() {
        assert!(validate_topic_config(RETENTION_MS, "abc").is_err());
    }

    #[test]
    fn validate_segment_bytes_rejects_zero() {
        assert!(validate_topic_config(SEGMENT_BYTES, "0").is_err());
    }

    #[test]
    fn validate_cleanup_policy_accepts_delete_and_compact() {
        assert!(validate_topic_config(CLEANUP_POLICY, "delete").is_ok());
        assert!(validate_topic_config(CLEANUP_POLICY, "compact").is_ok());
    }

    #[test]
    fn validate_cleanup_policy_rejects_unknown() {
        assert!(validate_topic_config(CLEANUP_POLICY, "compact,delete").is_err());
        assert!(validate_topic_config(CLEANUP_POLICY, "junk").is_err());
    }

    #[test]
    fn validate_compression_all_supported_values_accepted() {
        for v in [
            "producer",
            "uncompressed",
            "none",
            "gzip",
            "snappy",
            "lz4",
            "zstd",
        ] {
            assert!(
                validate_topic_config(COMPRESSION_TYPE, v).is_ok(),
                "compression.type={v} should be accepted",
            );
        }
    }

    #[test]
    fn validate_compression_bogus_rejected() {
        let err = validate_topic_config(COMPRESSION_TYPE, "bzip3").unwrap_err();
        assert!(err.contains("compression.type"), "got: {err}");
    }

    #[test]
    fn parse_compression_type_maps_producer_to_none() {
        assert_eq!(parse_compression_type("producer"), Ok(None));
    }

    #[test]
    fn parse_compression_type_maps_codecs() {
        use crabka_compression::CompressionType;
        assert_eq!(
            parse_compression_type("gzip"),
            Ok(Some(CompressionType::Gzip))
        );
        assert_eq!(
            parse_compression_type("snappy"),
            Ok(Some(CompressionType::Snappy))
        );
        assert_eq!(
            parse_compression_type("lz4"),
            Ok(Some(CompressionType::Lz4))
        );
        assert_eq!(
            parse_compression_type("zstd"),
            Ok(Some(CompressionType::Zstd))
        );
        assert_eq!(
            parse_compression_type("uncompressed"),
            Ok(Some(CompressionType::None))
        );
    }

    #[test]
    fn apply_compression_type_zstd_propagates() {
        use crabka_compression::CompressionType;
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "zstd".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.compression_type, Some(CompressionType::Zstd));
    }

    #[test]
    fn apply_compression_type_producer_resets_to_none() {
        use crabka_compression::CompressionType;
        let base = LogConfig {
            compression_type: Some(CompressionType::Lz4),
            ..LogConfig::default()
        };
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "producer".into());
        let out = apply_to_log_config(&o, &base);
        assert_eq!(out.compression_type, None);
    }

    #[test]
    fn validate_min_isr_positive_accepted() {
        assert!(validate_topic_config(MIN_INSYNC_REPLICAS, "2").is_ok());
    }

    #[test]
    fn validate_unknown_key_rejected() {
        let err = validate_topic_config("flush.ms", "1000").unwrap_err();
        assert!(err.contains("unrecognized"));
    }

    #[test]
    fn is_recognized_returns_true_for_whitelisted_keys() {
        assert!(is_recognized(RETENTION_MS));
        assert!(is_recognized(RETENTION_BYTES));
        assert!(is_recognized(SEGMENT_BYTES));
        assert!(is_recognized(CLEANUP_POLICY));
        assert!(is_recognized(COMPRESSION_TYPE));
        assert!(is_recognized(MIN_INSYNC_REPLICAS));
    }

    #[test]
    fn is_recognized_returns_false_for_unknown_keys() {
        assert!(!is_recognized("flush.ms"));
        assert!(!is_recognized("unclean.leader.election.enable"));
        assert!(!is_recognized(""));
    }

    #[test]
    fn apply_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "60000".into());
        let base = LogConfig::default();
        let out = apply_to_log_config(&o, &base);
        assert_eq!(out.retention_ms, Some(Duration::from_mins(1)));
    }

    #[test]
    fn apply_retention_ms_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.retention_ms, None);
    }

    #[test]
    fn apply_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.retention_bytes, Some(1_048_576));
    }

    #[test]
    fn apply_retention_bytes_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.retention_bytes, None);
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert_eq!(out.segment_bytes, 1_048_576);
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention_ms: Some(Duration::from_millis(12345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert_eq!(out.retention_ms, base.retention_ms);
    }

    #[test]
    fn apply_cleanup_policy_compact_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "compact".to_string());
        let out = apply_to_log_config(&overrides, &crabka_log::LogConfig::default());
        assert_eq!(out.cleanup_policy, crabka_log::CleanupPolicy::Compact);
    }

    #[test]
    fn apply_cleanup_policy_delete_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "delete".to_string());
        let base = crabka_log::LogConfig {
            cleanup_policy: crabka_log::CleanupPolicy::Compact,
            ..crabka_log::LogConfig::default()
        };
        let out = apply_to_log_config(&overrides, &base);
        assert_eq!(out.cleanup_policy, crabka_log::CleanupPolicy::Delete);
    }
}
