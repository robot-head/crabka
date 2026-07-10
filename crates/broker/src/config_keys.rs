//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! Fifteen keys are recognized. Five propagate live to `Log.config`
//! (`retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
//! `compression.type`), plus the tiered-storage local-retention
//! pair (`local.retention.ms`, `local.retention.bytes`) and the KIP-534
//! delete-horizon grace window (`delete.retention.ms`). One is read by
//! the produce hot path's pre-flight gate: `min.insync.replicas`
//! (integers >= 1) — `acks=-1` produces against a partition whose ISR is
//! already smaller fail fast with `NOT_ENOUGH_REPLICAS` (19). Two are
//! KIP-73 throttle keys (`leader.replication.throttled.replicas`,
//! `follower.replication.throttled.replicas`) validated via
//! `ThrottledReplicas::parse`. One is the KIP-841 unclean-recovery toggle
//! (`unclean.leader.election.enable`) read by the controller's automatic
//! failover path on ISR-empty. One is the KIP-966 offset-aware recovery
//! strategy (`unclean.recovery.strategy`) which supersedes it.
//! One is Crabka's `QoS` routing key (`qos.tier`), used by producer quota
//! enforcement to partition runtime buckets by topic tier.
//!
//! Unknown keys are rejected with `INVALID_CONFIG`.

// Items are `pub(crate)` for downstream handlers (Tasks 7-10); until those
// modules land they appear unused to the compiler.
#![allow(dead_code)]

use std::{collections::BTreeMap, time::Duration};

use crabka_log::LogConfig;

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
/// KIP-841: gates whether the controller may auto-elect an out-of-ISR
/// replica as leader on ISR-empty failover. Default `false` matches
/// Apache Kafka — partition stays unavailable until a former ISR member
/// returns. `true` accepts possible data loss in exchange for
/// availability. Consumed at runtime by
/// `crate::leader_election::on_broker_dead` via
/// [`MetadataImage::topic_config`].
pub(crate) const UNCLEAN_LEADER_ELECTION_ENABLE: &str = "unclean.leader.election.enable";
/// KIP-966: per-topic unclean-recovery strategy. Supersedes
/// `unclean.leader.election.enable`: when set to `Balanced` or
/// `Aggressive` the controller runs offset-aware recovery (polls
/// surviving replicas for their log offsets and elects the most complete
/// log). `None` (the default) falls back to the legacy enable-flag
/// behavior. Consumed by `crate::unclean_recovery` and the failover /
/// `ElectLeaders` paths.
pub(crate) const UNCLEAN_RECOVERY_STRATEGY: &str = "unclean.recovery.strategy";

/// Resolved value of `unclean.recovery.strategy` for a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStrategy {
    /// No offset-aware recovery. Defer to `unclean.leader.election.enable`.
    None,
    /// Wait for all currently-alive replicas (ELR is not tracked in
    /// crabka), then elect the most complete log.
    Balanced,
    /// Elect the most complete log among the replicas that respond within
    /// a short deadline; optimize availability.
    Aggressive,
}

impl RecoveryStrategy {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "None" => Some(Self::None),
            "Balanced" => Some(Self::Balanced),
            "Aggressive" => Some(Self::Aggressive),
            _ => None,
        }
    }
}
/// KIP-405: per-topic tiered-storage opt-in.
pub(crate) const REMOTE_STORAGE_ENABLE: &str = "remote.storage.enable";
/// KIP-405: per-topic local-retention time window for tiered partitions.
pub(crate) const LOCAL_RETENTION_MS: &str = "local.retention.ms";
/// KIP-405: per-topic local-retention size budget for tiered partitions.
pub(crate) const LOCAL_RETENTION_BYTES: &str = "local.retention.bytes";
/// KIP-534: how long tombstones and transaction markers are retained after
/// they first become compaction-eligible (the delete-horizon grace window).
pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";
/// Crabka extension: per-topic `QoS` tier used to partition producer quota
/// buckets. Unset topics resolve to [`DEFAULT_QOS_TIER`].
pub(crate) const QOS_TIER: &str = "qos.tier";
pub(crate) const DEFAULT_QOS_TIER: &str = "default";

/// Kafka sentinel for `retention.ms` / `retention.bytes`: `-1` means
/// unlimited retention, and is the lowest legal value.
const RETENTION_UNLIMITED: i64 = -1;

/// KIP-405 sentinel for `local.retention.ms` / `local.retention.bytes`:
/// `-2` means "inherit the corresponding non-local retention setting", and
/// is the lowest legal value (`-1` = unlimited also applies).
const LOCAL_RETENTION_INHERIT: i64 = -2;

/// Validate a single key/value pair. `Err(reason)` carries an
/// operator-readable explanation that the handler propagates into the
/// `error_message` field of the response.
pub(crate) fn validate_topic_config(key: &str, value: &str) -> Result<(), String> {
    match key {
        RETENTION_MS | RETENTION_BYTES => {
            parse_i64_at_least(RETENTION_UNLIMITED, value).map(|_| ())
        }
        LOCAL_RETENTION_MS | LOCAL_RETENTION_BYTES => {
            parse_i64_at_least(LOCAL_RETENTION_INHERIT, value).map(|_| ())
        }
        DELETE_RETENTION_MS => parse_i64_at_least(0, value).map(|_| ()),
        SEGMENT_BYTES => parse_u64_at_least(1, value).map(|_| ()),
        CLEANUP_POLICY => match value {
            "delete" | "compact" => Ok(()),
            _ => Err(format!(
                "cleanup.policy={value} not supported; expected `delete` or `compact`"
            )),
        },
        COMPRESSION_TYPE => parse_compression_type(value).map(|_| ()),
        MIN_INSYNC_REPLICAS => parse_i64_at_least(1, value).map(|_| ()),
        UNCLEAN_LEADER_ELECTION_ENABLE => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "unclean.leader.election.enable={value} not supported; expected `true` or `false`"
            )),
        },
        UNCLEAN_RECOVERY_STRATEGY => RecoveryStrategy::parse(value).map(|_| ()).ok_or_else(|| {
            format!(
                "unclean.recovery.strategy={value} not supported; expected `None`, `Balanced`, or `Aggressive`"
            )
        }),
        REMOTE_STORAGE_ENABLE => match value {
            "true" | "false" => Ok(()),
            _ => Err(format!(
                "remote.storage.enable={value} not supported; expected `true` or `false`"
            )),
        },
        QOS_TIER => validate_qos_tier(value),
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

fn validate_qos_tier(value: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err("qos.tier must not be empty".into());
    }
    if value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(format!(
            "qos.tier={value} not supported; expected non-empty ASCII letters, digits, '.', '_' or '-'"
        ))
    }
}

/// Returns `true` if `key` is one of the recognized topic-config keys.
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
            | UNCLEAN_LEADER_ELECTION_ENABLE
            | UNCLEAN_RECOVERY_STRATEGY
            | REMOTE_STORAGE_ENABLE
            | LOCAL_RETENTION_MS
            | LOCAL_RETENTION_BYTES
            | DELETE_RETENTION_MS
            | QOS_TIER
            | crate::throttle::LEADER_THROTTLED_REPLICAS_KEY
            | crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY
    )
}

/// Resolve a topic's `QoS` tier for producer quota bucket partitioning.
/// Missing or corrupt values fall back to `default`, matching the
/// permissive runtime behavior of other Produce-side topic config reads.
#[must_use]
pub(crate) fn resolve_qos_tier<'a>(
    image: &'a crabka_metadata::MetadataImage,
    topic: &str,
) -> &'a str {
    image
        .topic_config(topic)
        .and_then(|m| m.get(QOS_TIER))
        .filter(|v| validate_qos_tier(v).is_ok())
        .map_or(DEFAULT_QOS_TIER, String::as_str)
}

/// Resolve `unclean.recovery.strategy` for `topic`, defaulting to
/// `RecoveryStrategy::None` when unset or unparseable. Per-topic only
/// for now (mirrors `unclean.leader.election.enable`); a cluster default
/// can layer in later via the same `topic_config` lookup precedence.
pub(crate) fn resolve_recovery_strategy(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
) -> RecoveryStrategy {
    image
        .topic_config(topic)
        .and_then(|m| m.get(UNCLEAN_RECOVERY_STRATEGY))
        .and_then(|v| RecoveryStrategy::parse(v))
        .unwrap_or(RecoveryStrategy::None)
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
            LOCAL_RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    // -2 (inherit) and -1 (unlimited)
                    // both collapse to `None` — the greenfield simplification noted
                    // in the spec. >=0 maps to `Some(Duration::from_millis(n))`.
                    out.local_retention_ms = if ms < 0 {
                        None
                    } else {
                        Some(Duration::from_millis(
                            u64::try_from(ms).expect("validated non-negative above"),
                        ))
                    };
                }
            }
            LOCAL_RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.local_retention_bytes = if b < 0 {
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
            REMOTE_STORAGE_ENABLE => {
                out.remote_storage_enable = v == "true";
            }
            DELETE_RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>()
                    && ms >= 0
                {
                    out.delete_retention_ms = Duration::from_millis(
                        u64::try_from(ms).expect("validated non-negative above"),
                    );
                }
            }
            // The remaining keys are recognized but no broker behavior is
            // wired to them yet (see module docs).
            _ => {}
        }
    }
    out
}

/// One whitelisted topic-config key, for the generated reference page.
#[derive(Debug, Clone, Copy)]
pub struct TopicConfigDoc {
    pub key: &'static str,
    pub value_type: &'static str,
    pub default: Option<&'static str>,
    pub kip: Option<&'static str>,
    pub description: &'static str,
}

/// The full whitelist documented on the topic-configs reference page.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn topic_config_docs() -> Vec<TopicConfigDoc> {
    vec![
        TopicConfigDoc {
            key: RETENTION_MS,
            value_type: "long (ms)",
            default: None,
            kip: None,
            description: "Retention time before log segments become eligible for deletion.",
        },
        TopicConfigDoc {
            key: RETENTION_BYTES,
            value_type: "long (bytes)",
            default: None,
            kip: None,
            description: "Maximum partition size before old segments are deleted.",
        },
        TopicConfigDoc {
            key: SEGMENT_BYTES,
            value_type: "int (bytes)",
            default: None,
            kip: None,
            description: "Target size of a single log segment file.",
        },
        TopicConfigDoc {
            key: CLEANUP_POLICY,
            value_type: "string",
            default: Some("delete"),
            kip: None,
            description: "`delete`, `compact`, or `compact,delete`.",
        },
        TopicConfigDoc {
            key: COMPRESSION_TYPE,
            value_type: "string",
            default: Some("producer"),
            kip: None,
            description: "Broker-side compression codec for the topic.",
        },
        TopicConfigDoc {
            key: MIN_INSYNC_REPLICAS,
            value_type: "int (>=1)",
            default: Some("1"),
            kip: None,
            description: "With acks=all, the minimum in-sync replicas required to accept a write; otherwise NOT_ENOUGH_REPLICAS (19).",
        },
        TopicConfigDoc {
            key: UNCLEAN_LEADER_ELECTION_ENABLE,
            value_type: "boolean",
            default: Some("false"),
            kip: Some("KIP-841"),
            description: "Allow electing an out-of-ISR replica as leader on ISR-empty failover (possible data loss).",
        },
        TopicConfigDoc {
            key: UNCLEAN_RECOVERY_STRATEGY,
            value_type: "string",
            default: Some("None"),
            kip: Some("KIP-966"),
            description: "Offset-aware unclean recovery: `None`, `Balanced`, or `Aggressive`. Supersedes unclean.leader.election.enable.",
        },
        TopicConfigDoc {
            key: REMOTE_STORAGE_ENABLE,
            value_type: "boolean",
            default: Some("false"),
            kip: Some("KIP-405"),
            description: "Opt this topic into tiered (remote) storage.",
        },
        TopicConfigDoc {
            key: LOCAL_RETENTION_MS,
            value_type: "long (ms)",
            default: None,
            kip: Some("KIP-405"),
            description: "Local-tier retention time for tiered partitions.",
        },
        TopicConfigDoc {
            key: LOCAL_RETENTION_BYTES,
            value_type: "long (bytes)",
            default: None,
            kip: Some("KIP-405"),
            description: "Local-tier retention size budget for tiered partitions.",
        },
        TopicConfigDoc {
            key: DELETE_RETENTION_MS,
            value_type: "long (ms)",
            default: Some("86400000"),
            kip: Some("KIP-534"),
            description: "How long tombstones and transaction markers are retained after becoming compaction-eligible.",
        },
        TopicConfigDoc {
            key: QOS_TIER,
            value_type: "string",
            default: Some(DEFAULT_QOS_TIER),
            kip: None,
            description: "Crabka QoS tier used to partition producer quota buckets.",
        },
        TopicConfigDoc {
            key: crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
            value_type: "string",
            default: None,
            kip: Some("KIP-73"),
            description: "Replica list throttled on the leader side during reassignment.",
        },
        TopicConfigDoc {
            key: crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
            value_type: "string",
            default: None,
            kip: Some("KIP-73"),
            description: "Replica list throttled on the follower side during reassignment.",
        },
    ]
}

#[cfg(test)]
mod doc_tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn topic_config_docs_cover_known_keys() {
        use std::collections::HashSet;
        let docs = topic_config_docs();
        let doc_keys: HashSet<&str> = docs.iter().map(|d| d.key).collect();
        // No duplicate keys in the doc table.
        assert!(
            doc_keys.len() == docs.len(),
            "duplicate key in topic_config_docs"
        );
        // Every documented key is recognized by the validator.
        for k in &doc_keys {
            assert!(
                is_recognized(k),
                "documented key `{k}` not recognized by validator"
            );
        }
        // Every recognized key is documented.
        for k in [
            RETENTION_MS,
            RETENTION_BYTES,
            SEGMENT_BYTES,
            CLEANUP_POLICY,
            COMPRESSION_TYPE,
            MIN_INSYNC_REPLICAS,
            UNCLEAN_LEADER_ELECTION_ENABLE,
            UNCLEAN_RECOVERY_STRATEGY,
            REMOTE_STORAGE_ENABLE,
            LOCAL_RETENTION_MS,
            LOCAL_RETENTION_BYTES,
            DELETE_RETENTION_MS,
            QOS_TIER,
            crate::throttle::LEADER_THROTTLED_REPLICAS_KEY,
            crate::throttle::FOLLOWER_THROTTLED_REPLICAS_KEY,
        ] {
            assert!(
                doc_keys.contains(k),
                "recognized key `{k}` missing from topic_config_docs"
            );
        }
        assert!(docs.iter().all(|d| !d.description.is_empty()));
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn validate_retention_ms_boundary_cases() {
        let cases = [
            ("60000", true), // positive accepted
            ("-1", true),    // -1 (unlimited) accepted
            ("-5", false),   // below -1 rejected
            ("abc", false),  // non-integer rejected
        ];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(RETENTION_MS, value).is_ok() == want_ok,
                "retention.ms={value}"
            );
        }
    }

    #[test]
    fn validate_segment_bytes_boundary_cases() {
        for (case, value, expected) in [
            ("zero rejected", "0", Err(())),
            ("minimum accepted", "1", Ok(())),
        ] {
            assert!(
                validate_topic_config(SEGMENT_BYTES, value).map_err(|_| ()) == expected,
                "case {case}"
            );
        }
    }

    #[test]
    fn validate_cleanup_policy_cases() {
        for (case, value, expected) in [
            ("delete accepted", "delete", Ok(())),
            ("compact accepted", "compact", Ok(())),
            ("combined rejected", "compact,delete", Err(())),
            ("unknown rejected", "junk", Err(())),
        ] {
            assert!(
                validate_topic_config(CLEANUP_POLICY, value).map_err(|_| ()) == expected,
                "case {case}"
            );
        }
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
        assert!(parse_compression_type("producer") == Ok(None));
    }

    #[test]
    fn parse_compression_type_maps_codecs() {
        use crabka_compression::CompressionType;
        let cases = [
            ("gzip", CompressionType::Gzip),
            ("snappy", CompressionType::Snappy),
            ("lz4", CompressionType::Lz4),
            ("zstd", CompressionType::Zstd),
            ("uncompressed", CompressionType::None),
        ];
        for (input, want) in cases {
            assert!(
                parse_compression_type(input) == Ok(Some(want)),
                "compression.type={input}"
            );
        }
    }

    #[test]
    fn apply_compression_type_zstd_propagates() {
        use crabka_compression::CompressionType;
        let mut o = BTreeMap::new();
        o.insert(COMPRESSION_TYPE.into(), "zstd".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.compression_type == Some(CompressionType::Zstd));
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
        assert!(out.compression_type == None);
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
    fn validate_qos_tier_accepts_ascii_identifiers() {
        for v in ["default", "gold", "bulk_1", "critical-prod", "tier.2"] {
            assert!(validate_topic_config(QOS_TIER, v).is_ok(), "qos.tier={v}");
        }
    }

    #[test]
    fn validate_qos_tier_rejects_empty_or_unsafe_values() {
        for v in ["", "has space", "../escape", "ümlaut"] {
            assert!(validate_topic_config(QOS_TIER, v).is_err(), "qos.tier={v}");
        }
    }

    #[test]
    fn resolve_qos_tier_defaults_when_unset() {
        let image = crabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        assert!(resolve_qos_tier(&image, "t") == DEFAULT_QOS_TIER);
    }

    #[test]
    fn validate_remote_storage_enable_accepts_bools() {
        assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "true").is_ok());
        assert!(validate_topic_config(REMOTE_STORAGE_ENABLE, "false").is_ok());
    }

    #[test]
    fn validate_remote_storage_enable_rejects_junk() {
        let err = validate_topic_config(REMOTE_STORAGE_ENABLE, "yes").unwrap_err();
        assert!(err.contains("remote.storage.enable"), "got: {err}");
    }

    #[test]
    fn is_recognized_includes_remote_storage_enable() {
        assert!(is_recognized(REMOTE_STORAGE_ENABLE));
    }

    #[test]
    fn apply_remote_storage_enable_propagates() {
        let mut o = BTreeMap::new();
        o.insert(REMOTE_STORAGE_ENABLE.into(), "true".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.remote_storage_enable);

        let mut off = BTreeMap::new();
        off.insert(REMOTE_STORAGE_ENABLE.into(), "false".into());
        let base = LogConfig {
            remote_storage_enable: true,
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&off, &base);
        assert!(!out.remote_storage_enable);
    }

    #[test]
    fn is_recognized_matches_whitelist() {
        let cases = [
            (RETENTION_MS, true),
            (RETENTION_BYTES, true),
            (SEGMENT_BYTES, true),
            (CLEANUP_POLICY, true),
            (COMPRESSION_TYPE, true),
            (MIN_INSYNC_REPLICAS, true),
            ("flush.ms", false),
            ("", false),
        ];
        for (key, want) in cases {
            assert!(is_recognized(key) == want, "key {key:?}");
        }
    }

    #[test]
    fn validate_unclean_leader_election_enable_accepts_bools() {
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "true").is_ok());
        assert!(validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "false").is_ok());
    }

    #[test]
    fn validate_unclean_leader_election_enable_rejects_junk() {
        let err = validate_topic_config(UNCLEAN_LEADER_ELECTION_ENABLE, "yes").unwrap_err();
        assert!(err.contains("unclean.leader.election.enable"), "got: {err}");
    }

    #[test]
    fn is_recognized_includes_unclean_leader_election_enable() {
        assert!(is_recognized(UNCLEAN_LEADER_ELECTION_ENABLE));
    }

    #[test]
    fn apply_retention_ms_cases() {
        for (case, value, expected) in [
            ("positive", "60000", Some(Duration::from_mins(1))),
            ("unlimited", "-1", None),
            ("zero", "0", Some(Duration::from_millis(0))),
        ] {
            let mut overrides = BTreeMap::new();
            overrides.insert(RETENTION_MS.into(), value.into());
            let out = apply_to_log_config(&overrides, &LogConfig::default());
            assert!(out.retention_ms == expected, "case {case}");
        }
    }

    #[test]
    fn apply_retention_bytes_cases() {
        for (case, value, expected) in [
            ("positive", "1048576", Some(1_048_576)),
            ("unlimited", "-1", None),
            ("zero", "0", Some(0)),
        ] {
            let mut overrides = BTreeMap::new();
            overrides.insert(RETENTION_BYTES.into(), value.into());
            let out = apply_to_log_config(&overrides, &LogConfig::default());
            assert!(out.retention_bytes == expected, "case {case}");
        }
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_bytes == 1_048_576);
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention_ms: Some(Duration::from_millis(12345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert!(out.retention_ms == base.retention_ms);
    }

    #[test]
    fn apply_cleanup_policy_cases() {
        for (case, value, base_policy, expected) in [
            (
                "compact",
                "compact",
                crabka_log::CleanupPolicy::Delete,
                crabka_log::CleanupPolicy::Compact,
            ),
            (
                "delete",
                "delete",
                crabka_log::CleanupPolicy::Compact,
                crabka_log::CleanupPolicy::Delete,
            ),
        ] {
            let mut overrides = BTreeMap::new();
            overrides.insert(CLEANUP_POLICY.to_string(), value.to_string());
            let base = LogConfig {
                cleanup_policy: base_policy,
                ..LogConfig::default()
            };
            let out = apply_to_log_config(&overrides, &base);
            assert!(out.cleanup_policy == expected, "case {case}");
        }
    }

    #[test]
    fn validate_local_retention_ms_accepts_minus_one_minus_two_and_positive() {
        for value in ["-2", "-1", "60000"] {
            assert!(
                validate_topic_config(LOCAL_RETENTION_MS, value) == Ok(()),
                "local.retention.ms={value}"
            );
        }
    }

    #[test]
    fn validate_local_retention_ms_rejects_below_minus_two() {
        assert!(validate_topic_config(LOCAL_RETENTION_MS, "-3").is_err());
    }

    #[test]
    fn is_recognized_includes_local_retention_keys() {
        assert!(is_recognized(LOCAL_RETENTION_MS));
        assert!(is_recognized(LOCAL_RETENTION_BYTES));
    }

    #[test]
    fn apply_local_retention_ms_cases() {
        for (case, value, expected) in [
            ("inherit", "-2", None),
            ("unlimited", "-1", None),
            ("zero", "0", Some(Duration::from_millis(0))),
            ("positive", "60000", Some(Duration::from_mins(1))),
        ] {
            let mut overrides = BTreeMap::new();
            overrides.insert(LOCAL_RETENTION_MS.into(), value.into());
            let out = apply_to_log_config(&overrides, &LogConfig::default());
            assert!(out.local_retention_ms == expected, "case {case}");
        }
    }

    #[test]
    fn apply_local_retention_bytes_cases() {
        for (case, value, expected) in [
            ("positive", "1048576", Some(1_048_576)),
            ("inherit", "-2", None),
            ("zero", "0", Some(0)),
        ] {
            let mut overrides = BTreeMap::new();
            overrides.insert(LOCAL_RETENTION_BYTES.into(), value.into());
            let out = apply_to_log_config(&overrides, &LogConfig::default());
            assert!(out.local_retention_bytes == expected, "case {case}");
        }
    }

    #[test]
    fn validate_delete_retention_ms_accepts_nonneg_rejects_negative() {
        let cases = [("0", true), ("86400000", true), ("-1", false)];
        for (value, want_ok) in cases {
            assert!(
                validate_topic_config(DELETE_RETENTION_MS, value).is_ok() == want_ok,
                "delete.retention.ms={value}"
            );
        }
    }

    #[test]
    fn is_recognized_includes_delete_retention_ms() {
        assert!(is_recognized(DELETE_RETENTION_MS));
    }

    #[test]
    fn apply_delete_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(DELETE_RETENTION_MS.into(), "12345".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.delete_retention_ms == std::time::Duration::from_millis(12345));
    }

    #[test]
    fn recovery_strategy_validation_cases() {
        for (case, value, expected) in [
            ("none accepted", "None", Ok(())),
            ("balanced accepted", "Balanced", Ok(())),
            ("aggressive accepted", "Aggressive", Ok(())),
            ("unknown rejected", "fast", Err(())),
        ] {
            assert!(
                validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, value).map_err(|_| ()) == expected,
                "case {case}"
            );
        }
    }

    #[test]
    fn recovery_strategy_recognized() {
        assert!(is_recognized(UNCLEAN_RECOVERY_STRATEGY));
    }

    #[test]
    fn parse_recovery_strategy_maps_values() {
        let cases = [
            ("None", Some(RecoveryStrategy::None)),
            ("Balanced", Some(RecoveryStrategy::Balanced)),
            ("Aggressive", Some(RecoveryStrategy::Aggressive)),
            ("bogus", None),
        ];
        for (input, want) in cases {
            assert!(RecoveryStrategy::parse(input) == want, "input {input:?}");
        }
    }

    #[test]
    fn resolve_recovery_strategy_defaults_none_and_reads_override() {
        use std::collections::BTreeMap;

        use crabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
        use uuid::Uuid;
        let mut img = MetadataImage::new(Uuid::nil());
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
        let mut overrides = BTreeMap::new();
        overrides.insert(UNCLEAN_RECOVERY_STRATEGY.into(), "Balanced".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        }));
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Balanced);
    }
}
