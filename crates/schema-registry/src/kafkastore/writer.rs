//! Primary-only writer: serialises a `_schemas` record and produces it,
//! returning the produced offset for read-your-writes gating.

use bytes::Bytes;
use crabka_client_core::ClientSecurity;
use crabka_client_producer::{Acks, Producer, ProducerRecord};

use crate::config::RegistryConfig;

pub struct SchemaWriter {
    producer: Producer,
    topic: String,
}

impl SchemaWriter {
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn start(
        cfg: &RegistryConfig,
        security: Option<ClientSecurity>,
    ) -> anyhow::Result<Self> {
        let producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-writer", cfg.client_id))
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(security)
            .build()
            .await?;
        Ok(Self {
            producer,
            topic: cfg.schemas_topic.clone(),
        })
    }

    /// Produce one keyed `_schemas` record; return the assigned offset.
    #[tracing::instrument(level = "debug", name = "schema_writer.produce", skip_all, fields(topic = %self.topic, key_len = key.len(), value_len = value.len(), offset = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn produce(&self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<i64> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                key: Some(Bytes::from(key)),
                value: Some(Bytes::from(value)),
                ..Default::default()
            })
            .await;
        let meta = rx
            .await
            .map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
        tracing::Span::current().record("offset", meta.offset);
        Ok(meta.offset)
    }

    /// Produce a tombstone (null value) for `key`; return the assigned offset.
    /// Used for permanent deletes and mode-clears (compaction reclaims the key).
    #[tracing::instrument(level = "debug", name = "schema_writer.produce_tombstone", skip_all, fields(topic = %self.topic, key_len = key.len(), offset = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn produce_tombstone(&self, key: Vec<u8>) -> anyhow::Result<i64> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                key: Some(Bytes::from(key)),
                value: None,
                ..Default::default()
            })
            .await;
        let meta = rx
            .await
            .map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
        tracing::Span::current().record("offset", meta.offset);
        Ok(meta.offset)
    }
}
