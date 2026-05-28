//! Idempotent topic-create for the rebalancer's state topic. Run once
//! at startup; existing topic is left alone.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};

use crate::state_topic::error::StateTopicError;

/// Create the state topic if missing, with the compaction configs the
/// loader expects. `replication_factor` is the requested value; the
/// broker may downgrade it (or reject) based on the live broker count.
///
/// Idempotent: if the topic already exists with any config, this is a
/// no-op (the existing topic's configs are NOT updated; that's a
/// separate operator slice).
#[allow(dead_code)]
pub async fn ensure_topic(
    admin: &mut AdminClient,
    name: &str,
    replication_factor: i16,
) -> Result<(), StateTopicError> {
    let mut configs = BTreeMap::new();
    configs.insert("cleanup.policy".to_string(), "compact".to_string());
    configs.insert(
        "min.cleanable.dirty.ratio".to_string(),
        "0.01".to_string(),
    );
    configs.insert("segment.ms".to_string(), "60000".to_string());

    let spec = CreateTopicSpec {
        name: name.to_string(),
        partitions: 1,
        replicas: i32::from(replication_factor),
        configs,
    };
    let outcomes = admin.create_topics(&[spec], /* timeout_ms */ 10_000).await?;
    for o in outcomes {
        // The exact "topic already exists" error code is 36 (TOPIC_ALREADY_EXISTS);
        // treat it as success. Anything else is a hard error.
        match o.error.as_ref().map(|e| e.code) {
            None | Some(0 | 36) => {}
            Some(code) => return Err(StateTopicError::ProduceErrorCode { code }),
        }
    }
    Ok(())
}
