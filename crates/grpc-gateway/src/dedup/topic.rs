//! Idempotent creation of the internal compacted dedup-claim topic.
//! `cleanup.policy=compact,delete` + `retention.ms=window` bounds both the
//! topic size and the dedup horizon.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::{config::GatewayRuntimeConfig, error::GatewayError};

const INVALID_REPLICATION_FACTOR: i16 = 38;
const TOPIC_ALREADY_EXISTS: i16 = 36;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalTopicPolicy {
    pub replication_factor: i16,
    pub allow_replication_fallback: bool,
    pub create_timeout_ms: i32,
    pub segment_ms: i64,
    pub min_cleanable_dirty_ratio: String,
}

#[must_use]
pub fn internal_topic_policy(runtime: &GatewayRuntimeConfig) -> InternalTopicPolicy {
    InternalTopicPolicy {
        replication_factor: runtime.internal_topic_replication_factor,
        allow_replication_fallback: runtime.internal_topic_allow_replication_fallback,
        create_timeout_ms: runtime.internal_topic_create_timeout_ms,
        segment_ms: runtime.internal_topic_segment_ms,
        min_cleanable_dirty_ratio: (f64::from(
            runtime.internal_topic_min_cleanable_dirty_ratio_basis_points,
        ) / 10_000.0)
            .to_string(),
    }
}

impl Default for InternalTopicPolicy {
    fn default() -> Self {
        internal_topic_policy(&GatewayRuntimeConfig::default())
    }
}

/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn ensure_dedup_topic(
    bootstrap: &str,
    name: &str,
    partitions: u32,
    window_ms: i64,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_secured(&addrs, security)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact,delete".to_string());
    configs.insert("retention.ms".to_string(), window_ms.to_string());
    configs.insert(
        "min.cleanable.dirty.ratio".to_string(),
        policy.min_cleanable_dirty_ratio.clone(),
    );
    configs.insert("segment.ms".to_string(), policy.segment_ms.to_string());

    create_with_rf(&mut admin, name, partitions, policy, &configs).await
}

/// Idempotently create the compacted, single-partition membership topic.
/// `cleanup.policy=compact` (no delete) keeps one live record per node until a
/// tombstone supersedes it. Single partition ⇒ all publishes are totally
/// ordered, so the routing table's offset tiebreak is exact.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn ensure_membership_topic(
    bootstrap: &str,
    name: &str,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_secured(&addrs, security)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert(
        "min.cleanable.dirty.ratio".to_string(),
        policy.min_cleanable_dirty_ratio.clone(),
    );
    configs.insert("segment.ms".to_string(), policy.segment_ms.to_string());

    create_with_rf(&mut admin, name, 1, policy, &configs).await
}

async fn create_with_rf(
    admin: &mut AdminClient,
    name: &str,
    partitions: u32,
    policy: &InternalTopicPolicy,
    configs: &BTreeMap<String, String>,
) -> Result<(), GatewayError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: i32::try_from(partitions).unwrap_or(i32::MAX),
        replicas: i32::from(policy.replication_factor),
        configs: configs.clone(),
    };
    let outcomes = admin
        .create_topics(&[spec], policy.create_timeout_ms)
        .await
        .map_err(|e| GatewayError::Other(format!("create_topics: {e}")))?;
    for o in outcomes {
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0 | TOPIC_ALREADY_EXISTS) => {}
            Some(INVALID_REPLICATION_FACTOR)
                if policy.allow_replication_fallback && policy.replication_factor > 1 =>
            {
                let fallback = InternalTopicPolicy {
                    replication_factor: 1,
                    ..policy.clone()
                };
                return Box::pin(create_with_rf(admin, name, partitions, &fallback, configs)).await;
            }
            Some(code) => {
                return Err(GatewayError::Other(format!(
                    "create internal topic failed: code {code}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{InternalTopicPolicy, internal_topic_policy};
    use crate::config::GatewayRuntimeConfig;

    #[test]
    fn internal_topic_policy_uses_runtime_values() {
        let runtime = GatewayRuntimeConfig {
            internal_topic_replication_factor: 2,
            internal_topic_allow_replication_fallback: false,
            internal_topic_create_timeout_ms: 7_000,
            internal_topic_segment_ms: 22_000,
            internal_topic_min_cleanable_dirty_ratio_basis_points: 250,
            ..GatewayRuntimeConfig::default()
        };

        assert!(
            internal_topic_policy(&runtime)
                == InternalTopicPolicy {
                    replication_factor: 2,
                    allow_replication_fallback: false,
                    create_timeout_ms: 7_000,
                    segment_ms: 22_000,
                    min_cleanable_dirty_ratio: "0.025".into(),
                }
        );
    }
}
