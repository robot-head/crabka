//! Position recovery: persist the worker's [`SourceOffset`] to a compacted
//! internal topic on the TARGET cluster, keyed by flow name.
//!
//! On restart, [`InternalTopicCheckpointStore::load`] reads the last value for
//! the flow's key out of the compacted topic, recovering the exact position the
//! worker had reached before it stopped.

use async_trait::async_trait;
use bytes::Bytes;
use crabka_connect::{CheckpointStore, ConnectError, SourceOffset};

use crate::config::ClientResourcePolicy;

/// The internal compacted topic used to store replicator checkpoints.
const STATE_TOPIC: &str = "crabka-replicator-offsets";

/// A [`CheckpointStore`] backed by a compacted internal Kafka topic on the
/// target cluster.
///
/// Each flow gets its own key (`flow_name`) within the shared compacted topic
/// the internal state topic. On restart, [`load`](Self::load) fetches the last value for
/// that key, recovering the exact partition offsets the worker had reached.
pub struct InternalTopicCheckpointStore {
    producer: crabka_client_producer::Producer,
    target_bootstrap: String,
    topic: String,
    key: String,
    security: Option<crabka_client_core::security::ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
}

impl InternalTopicCheckpointStore {
    /// Ensure the compacted offset topic exists on the target cluster, build a
    /// producer, and return a store keyed by `flow_name`.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Offset`] if the topic cannot be created or the
    /// producer cannot connect.
    pub async fn start(
        target_bootstrap: &str,
        flow_name: &str,
        security: Option<crabka_client_core::security::ClientSecurity>,
    ) -> Result<Self, ConnectError> {
        Self::start_with_policy(
            target_bootstrap,
            flow_name,
            security,
            ClientResourcePolicy::default(),
        )
        .await
    }

    /// Start with the deployment's client resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Offset`] if the topic cannot be created or the
    /// producer cannot connect.
    pub async fn start_with_policy(
        target_bootstrap: &str,
        flow_name: &str,
        security: Option<crabka_client_core::security::ClientSecurity>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ConnectError> {
        crate::admin_util::ensure_compacted_topic_with_policy(
            target_bootstrap,
            STATE_TOPIC,
            security.clone(),
            client_resource_policy,
        )
        .await
        .map_err(ConnectError::Offset)?;

        let builder = crabka_client_producer::Producer::builder()
            .bootstrap(target_bootstrap)
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .enable_idempotence(false)
            .acks(crabka_client_producer::Acks::All);

        let producer = match security.clone() {
            Some(s) => builder.security(s).build().await,
            None => builder.build().await,
        }
        .map_err(|e| ConnectError::Offset(e.to_string()))?;

        Ok(Self {
            producer,
            target_bootstrap: target_bootstrap.to_string(),
            topic: STATE_TOPIC.into(),
            key: flow_name.into(),
            security,
            client_resource_policy,
        })
    }
}

#[async_trait]
impl CheckpointStore for InternalTopicCheckpointStore {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(key = %self.key, positions = offset.position.len()),
        err,
    )]
    async fn save(&self, offset: &SourceOffset) -> Result<(), ConnectError> {
        let bytes = serde_json::to_vec(offset).map_err(|e| ConnectError::Offset(e.to_string()))?;

        self.producer
            .send(crabka_client_producer::ProducerRecord {
                topic: self.topic.clone(),
                partition: None,
                key: Some(Bytes::copy_from_slice(self.key.as_bytes())),
                value: Some(Bytes::from(bytes)),
                headers: vec![],
                timestamp_ms: None,
            })
            .await
            .await
            .map_err(|e| ConnectError::Offset(e.to_string()))?
            .map_err(|e| ConnectError::Offset(e.to_string()))?;

        self.producer
            .flush()
            .await
            .map_err(|e| ConnectError::Offset(e.to_string()))?;

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all, fields(key = %self.key), err)]
    async fn load(&self) -> Result<Option<SourceOffset>, ConnectError> {
        let latest = crate::admin_util::read_last_value_for_key_with_policy(
            &self.target_bootstrap,
            &self.topic,
            self.key.as_bytes(),
            self.security.clone(),
            self.client_resource_policy,
        )
        .await
        .map_err(ConnectError::Offset)?;

        match latest {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).map_err(|e| ConnectError::Offset(e.to_string()))?,
            )),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crabka_connect::{CheckpointStore, OffsetValue, SourceOffset};

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persists_and_reloads_position_from_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = crabka_broker::Broker::start(crabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = broker.listen_addr().to_string();

        let store = InternalTopicCheckpointStore::start(&target, "flow1", None)
            .await
            .unwrap();

        let mut pos = BTreeMap::new();
        pos.insert("orders-0".to_string(), OffsetValue::Long(42));
        let off = SourceOffset::new(BTreeMap::new().into(), pos.into());

        store.save(&off).await.unwrap();

        let store2 = InternalTopicCheckpointStore::start(&target, "flow1", None)
            .await
            .unwrap();
        let loaded = store2.load().await.unwrap().unwrap();
        assert2::assert!(loaded == off);
    }
}
