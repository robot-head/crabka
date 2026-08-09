//! Idempotent creation of the internal compacted dedup-claim topic.
//!
//! `cleanup.policy=compact,delete` with `retention.ms=window` bounds both the
//! topic size and the dedup horizon.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_units::prelude::*;

use crate::{config::GatewayRuntimeConfig, error::GatewayError};

const INVALID_REPLICATION_FACTOR: i16 = 38;
const TOPIC_ALREADY_EXISTS: i16 = 36;

#[derive(Debug, Clone, PartialEq)]
pub struct InternalTopicPolicy {
    pub replication_factor: i16,
    pub allow_replication_fallback: bool,
    pub create_timeout: Time,
    pub segment: Time,
    pub min_cleanable_dirty_ratio: Ratio,
}

#[must_use]
pub fn internal_topic_policy(runtime: &GatewayRuntimeConfig) -> InternalTopicPolicy {
    InternalTopicPolicy {
        replication_factor: runtime.internal_topic_replication_factor,
        allow_replication_fallback: runtime.internal_topic_allow_replication_fallback,
        create_timeout: runtime.internal_topic_create_timeout,
        segment: runtime.internal_topic_segment,
        min_cleanable_dirty_ratio: runtime.internal_topic_min_cleanable_dirty_ratio,
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
    window: Time,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    ensure_dedup_topic_with_policy(
        bootstrap,
        name,
        partitions,
        window,
        policy,
        security,
        &GatewayRuntimeConfig::default(),
    )
    .await
}

/// Ensure the dedup topic with the deployment's client resource policy.
/// # Errors
/// Returns an error when admin operations fail.
pub async fn ensure_dedup_topic_with_policy(
    bootstrap: &str,
    name: &str,
    partitions: u32,
    window: Time,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
    runtime: &GatewayRuntimeConfig,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_with_options(&addrs, admin_options(security, runtime))
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let configs = dedup_topic_configs(window, policy);
    create_with_rf(&mut admin, name, partitions, policy, &configs).await
}

/// Kafka's topic configs are raw strings in fixed units. `retention.ms` and
/// `segment.ms` are millisecond integers, and `min.cleanable.dirty.ratio` is a
/// bare fraction. This is where the gateway's quantities become those strings.
fn dedup_topic_configs(window: Time, policy: &InternalTopicPolicy) -> BTreeMap<String, String> {
    let mut configs = compaction_configs(policy);
    configs.insert("cleanup.policy".to_string(), "compact,delete".to_string());
    configs.insert("retention.ms".to_string(), window.millis_i64().to_string());
    configs
}

/// The compaction configs both internal topics share.
fn compaction_configs(policy: &InternalTopicPolicy) -> BTreeMap<String, String> {
    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert(
        "min.cleanable.dirty.ratio".to_string(),
        policy.min_cleanable_dirty_ratio.as_f64().to_string(),
    );
    configs.insert(
        "segment.ms".to_string(),
        policy.segment.millis_i64().to_string(),
    );
    configs
}

/// Idempotently create the compacted, single-partition membership topic.
///
/// `cleanup.policy=compact`, with no delete, keeps one live record per node
/// until a tombstone replaces it. A single partition ⇒ all publishes are
/// totally ordered, so the routing table's offset tiebreak is exact.
/// # Errors
/// Returns an error when configuration is invalid, protocol encoding fails, the broker rejects the request, or transport I/O fails.
pub async fn ensure_membership_topic(
    bootstrap: &str,
    name: &str,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    ensure_membership_topic_with_policy(
        bootstrap,
        name,
        policy,
        security,
        &GatewayRuntimeConfig::default(),
    )
    .await
}

/// Ensure the membership topic with the deployment's client resource policy.
/// # Errors
/// Returns an error when admin operations fail.
pub async fn ensure_membership_topic_with_policy(
    bootstrap: &str,
    name: &str,
    policy: &InternalTopicPolicy,
    security: Option<crabka_client_core::security::ClientSecurity>,
    runtime: &GatewayRuntimeConfig,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_with_options(&addrs, admin_options(security, runtime))
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let configs = compaction_configs(policy);
    create_with_rf(&mut admin, name, 1, policy, &configs).await
}

fn admin_options(
    security: Option<crabka_client_core::security::ClientSecurity>,
    runtime: &GatewayRuntimeConfig,
) -> crabka_client_core::ConnectionOptions {
    crabka_client_core::ConnectionOptions {
        dns_timeout: crabka_client_core::ClientDnsTimeout::default(),
        connect_timeout: secs(5),
        request_timeout: secs(30),
        client_id: "crabka-operator".to_owned(),
        dispatch_queue_capacity: runtime.client_dispatch_queue_capacity,
        frame_max: runtime.client_frame_max,
        security: security.map(Box::new),
    }
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
        partitions: topic_partition_count(partitions)?,
        replicas: i32::from(policy.replication_factor),
        configs: configs.clone(),
    };
    let outcomes = admin
        .create_topics(&[spec], policy.create_timeout)
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

fn topic_partition_count(partitions: u32) -> Result<i32, GatewayError> {
    i32::try_from(partitions)
        .map_err(|_| GatewayError::Other("internal topic partition count exceeds i32::MAX".into()))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_units::prelude::*;

    use super::{
        InternalTopicPolicy, compaction_configs, dedup_topic_configs, internal_topic_policy,
        topic_partition_count,
    };
    use crate::config::GatewayRuntimeConfig;

    fn policy() -> InternalTopicPolicy {
        InternalTopicPolicy {
            replication_factor: 2,
            allow_replication_fallback: false,
            create_timeout: secs(7),
            segment: secs(22),
            min_cleanable_dirty_ratio: fraction(0.025),
        }
    }

    #[test]
    fn internal_topic_policy_uses_runtime_values() {
        let runtime = GatewayRuntimeConfig {
            internal_topic_replication_factor: 2,
            internal_topic_allow_replication_fallback: false,
            internal_topic_create_timeout: secs(7),
            internal_topic_segment: secs(22),
            internal_topic_min_cleanable_dirty_ratio: fraction(0.025),
            ..GatewayRuntimeConfig::default()
        };

        assert!(internal_topic_policy(&runtime) == policy());
    }

    /// Each quantity reaches Kafka in the unit that its topic config uses.
    /// `retention.ms` and `segment.ms` are millisecond integers, and the dirty
    /// ratio is a bare fraction.
    #[test]
    fn topic_configs_render_quantities_in_kafka_units() {
        let dedup = dedup_topic_configs(hours(1), &policy());

        check!(dedup.get("cleanup.policy").map(String::as_str) == Some("compact,delete"));
        check!(dedup.get("retention.ms").map(String::as_str) == Some("3600000"));
        check!(dedup.get("segment.ms").map(String::as_str) == Some("22000"));
        check!(dedup.get("min.cleanable.dirty.ratio").map(String::as_str) == Some("0.025"));

        let membership = compaction_configs(&policy());

        check!(membership.get("cleanup.policy").map(String::as_str) == Some("compact"));
        check!(membership.get("retention.ms") == None);
        check!(membership.get("segment.ms").map(String::as_str) == Some("22000"));
    }

    #[test]
    fn topic_partition_count_rejects_values_kafka_cannot_represent() {
        assert!(topic_partition_count(i32::MAX.cast_unsigned()).ok() == Some(i32::MAX));
        assert!(topic_partition_count(i32::MAX.cast_unsigned() + 1).is_err());
    }
}
