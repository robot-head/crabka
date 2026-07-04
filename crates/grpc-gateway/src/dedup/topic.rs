//! Idempotent creation of the internal compacted dedup-claim topic.
//! `cleanup.policy=compact,delete` + `retention.ms=window` bounds both the
//! topic size and the dedup horizon.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::error::GatewayError;

const INVALID_REPLICATION_FACTOR: i16 = 38;
const TOPIC_ALREADY_EXISTS: i16 = 36;

pub async fn ensure_dedup_topic(
    bootstrap: &str,
    name: &str,
    partitions: u32,
    window_ms: i64,
    replication: i16,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_secured(&addrs, security)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact,delete".to_string());
    configs.insert("retention.ms".to_string(), window_ms.to_string());
    configs.insert("min.cleanable.dirty.ratio".to_string(), "0.01".to_string());
    configs.insert("segment.ms".to_string(), "60000".to_string());

    create_with_rf(&mut admin, name, partitions, replication, &configs).await
}

/// Idempotently create the compacted, single-partition membership topic.
/// `cleanup.policy=compact` (no delete) keeps one live record per node until a
/// tombstone supersedes it. Single partition ⇒ all publishes are totally
/// ordered, so the routing table's offset tiebreak is exact.
pub async fn ensure_membership_topic(
    bootstrap: &str,
    name: &str,
    replication: i16,
    security: Option<crabka_client_core::security::ClientSecurity>,
) -> Result<(), GatewayError> {
    let addrs: Vec<String> = bootstrap.split(',').map(|s| s.trim().to_string()).collect();
    let mut admin = AdminClient::connect_secured(&addrs, security)
        .await
        .map_err(|e| GatewayError::Other(format!("admin connect: {e}")))?;

    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert("min.cleanable.dirty.ratio".to_string(), "0.01".to_string());
    configs.insert("segment.ms".to_string(), "60000".to_string());

    create_with_rf(&mut admin, name, 1, replication, &configs).await
}

async fn create_with_rf(
    admin: &mut AdminClient,
    name: &str,
    partitions: u32,
    rf: i16,
    configs: &BTreeMap<String, String>,
) -> Result<(), GatewayError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: i32::try_from(partitions).unwrap_or(i32::MAX),
        replicas: i32::from(rf),
        configs: configs.clone(),
    };
    let outcomes = admin
        .create_topics(&[spec], 10_000)
        .await
        .map_err(|e| GatewayError::Other(format!("create_topics: {e}")))?;
    for o in outcomes {
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0 | TOPIC_ALREADY_EXISTS) => {}
            Some(INVALID_REPLICATION_FACTOR) if rf > 1 => {
                return Box::pin(create_with_rf(admin, name, partitions, 1, configs)).await;
            }
            Some(code) => {
                return Err(GatewayError::Other(format!(
                    "create dedup topic failed: code {code}"
                )));
            }
        }
    }
    Ok(())
}
