//! Idempotent topic-create for the rebalancer's state topic. Run once
//! at startup; existing topic is left alone.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};
use tracing::warn;

use crate::state_topic::error::StateTopicError;

/// Kafka error code for "replication factor exceeds available brokers".
const INVALID_REPLICATION_FACTOR: i16 = 38;

/// Create the state topic if missing, with the compaction configs the
/// loader expects. `replication_factor` is the requested value; the
/// broker may downgrade it (or reject) based on the live broker count.
///
/// If the broker rejects the requested replication factor as invalid
/// (`INVALID_REPLICATION_FACTOR`, code 38 — returned when RF > broker count),
/// the call retries with RF=1 so single-broker development clusters work
/// without any extra configuration.
///
/// Idempotent: if the topic already exists with any config, this is a
/// no-op (the existing topic's configs are NOT updated; that's a
/// separate operator slice).
pub async fn ensure_topic(
    admin: &mut AdminClient,
    name: &str,
    replication_factor: i16,
) -> Result<(), StateTopicError> {
    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert("min.cleanable.dirty.ratio".to_string(), "0.01".to_string());
    configs.insert("segment.ms".to_string(), "60000".to_string());

    let effective_rf = try_create_topic(admin, name, replication_factor, &configs).await?;
    if effective_rf != replication_factor {
        warn!(
            topic = %name,
            requested_rf = replication_factor,
            effective_rf,
            "replication factor downgraded; cluster has fewer brokers than requested"
        );
    }
    Ok(())
}

/// Inner helper: attempt to create with `rf`; if the broker returns
/// `INVALID_REPLICATION_FACTOR` and `rf > 1`, retry with `rf = 1`.
/// Returns the replication factor that succeeded.
async fn try_create_topic(
    admin: &mut AdminClient,
    name: &str,
    rf: i16,
    configs: &BTreeMap<String, String>,
) -> Result<i16, StateTopicError> {
    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: 1,
        replicas: i32::from(rf),
        configs: configs.clone(),
    };
    let outcomes = admin
        .create_topics(&[spec], /* timeout_ms */ 10_000)
        .await?;
    for o in outcomes {
        // 36 = TOPIC_ALREADY_EXISTS  → idempotent, treat as success.
        // 38 = INVALID_REPLICATION_FACTOR → retry with rf=1 if rf > 1.
        // 0 / None → success.
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0 | 36) => {}
            Some(INVALID_REPLICATION_FACTOR) if rf > 1 => {
                return Box::pin(try_create_topic(admin, name, 1, configs)).await;
            }
            Some(code) => return Err(StateTopicError::ProduceErrorCode { code }),
        }
    }
    Ok(rf)
}
