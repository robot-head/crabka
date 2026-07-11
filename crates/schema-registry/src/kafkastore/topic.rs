//! Ensure the `_schemas` compacted topic exists; resolve its `topic_id`
//! (needed by Fetch v>=13). Mirrors `remote-storage-topic`'s `ensure_topic`.

use std::collections::BTreeMap;

use crabka_client_admin::{AdminClient, CreateTopicSpec};
use crabka_client_core::ClientSecurity;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::config::RegistryConfig;

const TOPIC_ALREADY_EXISTS: i16 = 36;

/// Create `_schemas` (1 partition, cleanup.policy=compact) if absent and return
/// its `topic_id`. Idempotent.
#[tracing::instrument(
    level = "info",
    name = "kafkastore.ensure_schemas_topic",
    skip_all,
    fields(topic = %cfg.schemas_topic, replicas = cfg.schemas_topic_rf),
    err
)]
/// # Errors
/// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
pub async fn ensure_schemas_topic(
    cfg: &RegistryConfig,
    security: Option<ClientSecurity>,
) -> anyhow::Result<WireUuid> {
    let bootstrap: Vec<String> = cfg
        .bootstrap
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let mut admin = AdminClient::connect_secured(&bootstrap, security).await?;

    let spec = CreateTopicSpec {
        name: cfg.schemas_topic.clone(),
        partitions: 1,
        replicas: cfg.schemas_topic_rf,
        configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
    };
    let outcomes = admin.create_topics(&[spec], 15_000).await?;
    if let Some(o) = outcomes.into_iter().next() {
        match o.error {
            None => {
                if let Some(id) = o.topic_id {
                    return Ok(to_wire_uuid(id));
                }
            }
            Some(e) if e.code == TOPIC_ALREADY_EXISTS => {}
            Some(e) => anyhow::bail!("create _schemas failed: {} ({})", e.name, e.code),
        }
    }
    let md = admin.metadata(&[cfg.schemas_topic.as_str()]).await?;
    let entry = md
        .topics
        .into_iter()
        .find(|t| t.name == cfg.schemas_topic)
        .ok_or_else(|| anyhow::anyhow!("_schemas not found after create"))?;
    Ok(entry.topic_id.map_or(WireUuid::ZERO, to_wire_uuid))
}

/// Convert admin's `uuid::Uuid` to the protocol `WireUuid` (same byte order).
fn to_wire_uuid(id: uuid::Uuid) -> WireUuid {
    WireUuid(*id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::to_wire_uuid;

    #[test]
    fn uuid_bytes_preserved() {
        let u = uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1122_3344_5566_7788);
        let wire = to_wire_uuid(u);
        assert2::assert!(wire.0 == *u.as_bytes());
    }
}
