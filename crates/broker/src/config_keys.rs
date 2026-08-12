//! Topic-config whitelist for `AlterConfigs` / `IncrementalAlterConfigs`.
//!
//! The broker recognizes fifteen topic keys. Five propagate live to `Log.config`:
//! `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`, and
//! `compression.type`. The tiered-storage local-retention pair
//! (`local.retention.ms`, `local.retention.bytes`) and the KIP-534
//! delete-horizon grace window (`delete.retention.ms`) propagate live too.
//!
//! The produce hot path's pre-flight gate reads one key,
//! `min.insync.replicas`, which takes integers >= 1. An `acks=-1` produce
//! against a partition whose ISR is already smaller fails fast with
//! `NOT_ENOUGH_REPLICAS` (19).
//!
//! Two keys are KIP-73 throttle keys:
//! `leader.replication.throttled.replicas` and
//! `follower.replication.throttled.replicas`. `ThrottledReplicas::parse`
//! validates both. One key is the KIP-841 unclean-recovery toggle,
//! `unclean.leader.election.enable`. The controller's automatic failover
//! path reads it on ISR-empty. One key is the KIP-966 offset-aware recovery
//! strategy, `unclean.recovery.strategy`, which supersedes that toggle. Both
//! unclean-recovery settings also accept a cluster-wide default broker config;
//! a topic override takes precedence. One
//! key is krabka's `QoS` routing key, `qos.tier`. Producer quota enforcement
//! uses it to partition runtime buckets by topic tier.
//!
//! The broker rejects unknown keys with `INVALID_CONFIG`.

use std::{collections::BTreeMap, time::Duration};

use crabka_log::LogConfig;
use crabka_units::{
    ByteSize, Time,
    convert::{
        ByteSizeExt as _, TimeExt as _,
        wire::{opt_size_from_bytes_i64, opt_time_from_millis_i64},
    },
};

pub(crate) const RETENTION_MS: &str = "retention.ms";
pub(crate) const RETENTION_BYTES: &str = "retention.bytes";
pub(crate) const SEGMENT_BYTES: &str = "segment.bytes";
pub(crate) const CLEANUP_POLICY: &str = "cleanup.policy";
pub(crate) const COMPRESSION_TYPE: &str = "compression.type";
pub(crate) const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
/// KIP-841: gates whether the controller may auto-elect an out-of-ISR
/// replica as leader on ISR-empty failover. Default: `false`, which matches
/// Apache Kafka. The partition then stays unavailable until a former ISR
/// member returns. `true` accepts possible data loss in exchange for
/// availability. `crate::leader_election::on_broker_dead` reads the topic
/// override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_LEADER_ELECTION_ENABLE: &str = "unclean.leader.election.enable";
/// KIP-966: topic-level unclean-recovery strategy. It supersedes
/// `unclean.leader.election.enable`. At `Balanced` or `Aggressive` the
/// controller runs offset-aware recovery: it polls surviving replicas for
/// their log offsets and elects the most complete log. Default: `None`,
/// which falls back to the legacy enable-flag behavior.
/// `crate::unclean_recovery` and the failover / `ElectLeaders` paths read the
/// topic override first and then the cluster-wide default broker config.
pub(crate) const UNCLEAN_RECOVERY_STRATEGY: &str = "unclean.recovery.strategy";

/// Resolved value of `unclean.recovery.strategy` for a topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryStrategy {
    /// No offset-aware recovery. Defer to `unclean.leader.election.enable`.
    None,
    /// Wait for all currently-alive replicas, then elect the most complete
    /// log. krabka does not track ELR.
    Balanced,
    /// Elect the most complete log among the replicas that respond within
    /// a short deadline. This optimizes availability.
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
/// KIP-534: how long the broker keeps tombstones and transaction markers
/// after they first become compaction-eligible. This is the delete-horizon
/// grace window.
pub(crate) const DELETE_RETENTION_MS: &str = "delete.retention.ms";
/// Crabka extension: per-topic `QoS` tier used to partition producer quota
/// buckets. Unset topics resolve to [`DEFAULT_QOS_TIER`].
pub(crate) const QOS_TIER: &str = "qos.tier";
pub(crate) const DEFAULT_QOS_TIER: &str = "default";

/// KIP-1075: server-side deadline for remote `ListOffsets` work when an older
/// request does not carry `timeout_ms`. Kafka exposes this as a dynamic broker
/// config and defaults it to 30 seconds.
pub(crate) const REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS: &str =
    "remote.list.offsets.request.timeout.ms";
pub(crate) const DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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
/// [`LogConfig::compression_type`]. This function returns `Ok(None)` for the
/// special `producer` value, which is the Kafka default and does no
/// broker-side re-encoding. It returns `Ok(Some(_))` for any concrete codec.
/// It returns `Err` for an unknown name.
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
/// This helps `IncrementalAlterConfigs` DELETE-op validation, which then
/// needs no sentinel probe value.
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

/// Resolve a topic's `QoS` tier, which partitions producer quota buckets.
/// Missing or corrupt values fall back to `default`. This matches the
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

fn topic_or_cluster_default<'a>(
    image: &'a crabka_metadata::MetadataImage,
    topic: &str,
    key: &str,
) -> Option<&'a str> {
    image
        .topic_config(topic)
        .and_then(|configs| configs.get(key))
        .or_else(|| image.default_broker_config()?.get(key))
        .map(String::as_str)
}

/// Resolve `unclean.recovery.strategy` for `topic`. A topic override takes
/// precedence over the cluster-wide default broker config. The result is
/// [`RecoveryStrategy::None`] when neither value exists or the selected value
/// is unparseable.
pub(crate) fn resolve_recovery_strategy(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
) -> RecoveryStrategy {
    topic_or_cluster_default(image, topic, UNCLEAN_RECOVERY_STRATEGY)
        .and_then(RecoveryStrategy::parse)
        .unwrap_or(RecoveryStrategy::None)
}

/// Resolve `unclean.leader.election.enable` for `topic`. A topic override
/// takes precedence over the cluster-wide default broker config. Missing or
/// invalid values resolve to `false`.
pub(crate) fn resolve_unclean_leader_election_enabled(
    image: &crabka_metadata::MetadataImage,
    topic: &str,
) -> bool {
    topic_or_cluster_default(image, topic, UNCLEAN_LEADER_ELECTION_ENABLE) == Some("true")
}

/// Parse KIP-1075's dynamic broker timeout.
pub(crate) fn parse_remote_list_offsets_timeout(value: &str) -> Result<Duration, String> {
    let millis = value
        .parse::<i32>()
        .map_err(|_| format!("{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be a positive int"))?;
    if millis <= 0 {
        return Err(format!(
            "{REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS} must be in 1..={}",
            i32::MAX
        ));
    }
    Ok(Duration::from_millis(
        u64::try_from(millis).expect("positive i32 fits u64"),
    ))
}

/// Resolve the per-broker KIP-1075 timeout over the cluster default.
pub(crate) fn resolve_remote_list_offsets_timeout(
    image: &crabka_metadata::MetadataImage,
    node_id: crabka_metadata::NodeId,
) -> Duration {
    image
        .broker_config(node_id)
        .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        .or_else(|| {
            image
                .default_broker_config()
                .and_then(|configs| configs.get(REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS))
        })
        .and_then(|value| parse_remote_list_offsets_timeout(value).ok())
        .unwrap_or(DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT)
}

/// Merge `overrides` over `base` and return a fresh `LogConfig` to push
/// into `Log::set_config`. This function drops unknown keys silently.
/// `validate_topic_config` should have rejected them at `AlterConfigs` time,
/// before the record reached the metadata image. This function is the
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
                    out.retention = opt_time_from_millis_i64(ms);
                }
            }
            RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.retention_size = opt_size_from_bytes_i64(b);
                }
            }
            LOCAL_RETENTION_MS => {
                if let Ok(ms) = v.parse::<i64>() {
                    // -2 (inherit) and -1 (unlimited)
                    // both collapse to `None` — the greenfield simplification noted
                    // in the spec. >=0 maps to `Some(Time)`.
                    out.local_retention = opt_time_from_millis_i64(ms);
                }
            }
            LOCAL_RETENTION_BYTES => {
                if let Ok(b) = v.parse::<i64>() {
                    out.local_retention_size = opt_size_from_bytes_i64(b);
                }
            }
            SEGMENT_BYTES => {
                if let Ok(b) = v.parse::<u64>() {
                    out.segment_size = ByteSize::from_bytes(b);
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
                    out.delete_retention = Time::from_millis(ms);
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

const TOPIC_CONFIG_DOCS: &[TopicConfigDoc] = &[
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
];

/// The full whitelist documented on the topic-configs reference page.
#[must_use]
pub fn topic_config_docs() -> Vec<TopicConfigDoc> {
    TOPIC_CONFIG_DOCS.to_vec()
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
    use crabka_units::{bytes, mebibytes, millis, minutes};

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
    fn validate_segment_bytes_rejects_zero() {
        assert!(validate_topic_config(SEGMENT_BYTES, "0").is_err());
    }

    #[test]
    fn validate_segment_bytes_accepts_minimum_one() {
        assert!(validate_topic_config(SEGMENT_BYTES, "1").is_ok());
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
    fn apply_retention_ms_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "60000".into());
        let base = LogConfig::default();
        let out = apply_to_log_config(&o, &base);
        assert!(out.retention == Some(minutes(1)));
    }

    #[test]
    fn apply_retention_ms_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == None);
    }

    #[test]
    fn apply_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention == Some(millis(0)));
    }

    #[test]
    fn apply_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_retention_bytes_minus_one_means_unlimited() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "-1".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == None);
    }

    #[test]
    fn apply_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.retention_size == Some(bytes(0)));
    }

    #[test]
    fn apply_segment_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(SEGMENT_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.segment_size == mebibytes(1));
    }

    #[test]
    fn apply_empty_overrides_preserves_base() {
        let base = LogConfig {
            retention: Some(millis(12_345)),
            ..LogConfig::default()
        };
        let out = apply_to_log_config(&BTreeMap::new(), &base);
        assert!(out.retention == base.retention);
    }

    #[test]
    fn apply_cleanup_policy_compact_propagates() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert(CLEANUP_POLICY.to_string(), "compact".to_string());
        let out = apply_to_log_config(&overrides, &crabka_log::LogConfig::default());
        assert!(out.cleanup_policy == crabka_log::CleanupPolicy::Compact);
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
        assert!(out.cleanup_policy == crabka_log::CleanupPolicy::Delete);
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
    fn apply_local_retention_ms_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == None);

        let mut unlimited = BTreeMap::new();
        unlimited.insert(LOCAL_RETENTION_MS.into(), "-1".into());
        let out = apply_to_log_config(&unlimited, &LogConfig::default());
        assert!(out.local_retention == None);
    }

    #[test]
    fn apply_local_retention_ms_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(millis(0)));
    }

    #[test]
    fn apply_local_retention_ms_positive_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_MS.into(), "60000".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention == Some(minutes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_propagates() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "1048576".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(mebibytes(1)));
    }

    #[test]
    fn apply_local_retention_bytes_minus_two_means_inherit() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "-2".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == None);
    }

    #[test]
    fn apply_local_retention_bytes_zero_is_retained() {
        let mut o = BTreeMap::new();
        o.insert(LOCAL_RETENTION_BYTES.into(), "0".into());
        let out = apply_to_log_config(&o, &LogConfig::default());
        assert!(out.local_retention_size == Some(bytes(0)));
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
        assert!(out.delete_retention == millis(12_345));
    }

    #[test]
    fn recovery_strategy_accepts_valid_values() {
        for v in ["None", "Balanced", "Aggressive"] {
            assert!(
                validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, v).is_ok(),
                "{v}"
            );
        }
    }

    #[test]
    fn recovery_strategy_rejects_garbage() {
        assert!(validate_topic_config(UNCLEAN_RECOVERY_STRATEGY, "fast").is_err());
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
    fn recovery_settings_resolve_topic_over_cluster_default() {
        use std::collections::BTreeMap;

        use crabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            TopicConfigRecord,
        };
        use uuid::Uuid;
        let mut img = MetadataImage::new(Uuid::nil());
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));

        for (key, value) in [
            (UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
            (UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
        ] {
            img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: key.into(),
                config_value: Some(value.into()),
            }));
        }
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Balanced);
        assert!(resolve_unclean_leader_election_enabled(&img, "t"));

        let mut overrides = BTreeMap::new();
        overrides.insert(UNCLEAN_RECOVERY_STRATEGY.into(), "Aggressive".into());
        overrides.insert(UNCLEAN_LEADER_ELECTION_ENABLE.into(), "false".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        }));
        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::Aggressive);
        assert!(!resolve_unclean_leader_election_enabled(&img, "t"));
    }

    #[test]
    fn invalid_topic_recovery_setting_does_not_expose_cluster_default() {
        use std::collections::BTreeMap;

        use crabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            TopicConfigRecord,
        };
        use uuid::Uuid;

        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: UNCLEAN_RECOVERY_STRATEGY.into(),
            config_value: Some("Balanced".into()),
        }));
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: BTreeMap::from([(UNCLEAN_RECOVERY_STRATEGY.into(), "invalid".into())]),
        }));

        assert!(resolve_recovery_strategy(&img, "t") == RecoveryStrategy::None);
    }
}
