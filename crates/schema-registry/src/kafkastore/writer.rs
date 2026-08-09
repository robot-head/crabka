//! Primary-only writer. It serialises a `_schemas` record and produces it, then
//! returns the produced offset for read-your-writes gating.

use bytes::Bytes;
use crabka_client_core::ClientSecurity;
use crabka_client_producer::{Acks, ConsumerGroupMetadata, Producer, ProducerRecord};
use tokio::sync::Mutex;

use crate::config::RegistryConfig;

pub struct SchemaWriter {
    producer: Producer,
    fenced_producer: Producer,
    fenced_state: Mutex<FencedState>,
    topic: String,
}

#[derive(Default)]
struct FencedState {
    generation_id: Option<i32>,
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
            .dispatch_queue_capacity(cfg.runtime.client_dispatch_queue_capacity.get())
            .frame_max(cfg.runtime.client_frame_max.size())
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(security)
            .build()
            .await?;
        // All primaries for one election group share this transactional id.
        // A newly elected primary initializes a new producer epoch and fences
        // the previous primary's in-flight writes.
        let fenced_producer = Producer::builder()
            .bootstrap(cfg.bootstrap.clone())
            .client_id(format!("{}-fenced-writer", cfg.client_id))
            .transactional_id(format!("crabka-schema-registry-{}", cfg.group_id))
            .dispatch_queue_capacity(cfg.runtime.client_dispatch_queue_capacity.get())
            .frame_max(cfg.runtime.client_frame_max.size())
            .enable_idempotence(true)
            .acks(Acks::All)
            .maybe_security(cfg.security.client.clone())
            .build()
            .await?;
        Ok(Self {
            producer,
            fenced_producer,
            fenced_state: Mutex::new(FencedState::default()),
            topic: cfg.schemas_topic.clone(),
        })
    }

    /// Produce one keyed `_schemas` record and return the assigned offset.
    #[tracing::instrument(level = "debug", name = "schema_writer.produce", skip_all, fields(topic = %self.topic, key_len = key.len(), value_len = value.len(), offset = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn produce(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        group: Option<&ConsumerGroupMetadata>,
    ) -> anyhow::Result<i64> {
        if let Some(group) = group {
            return self.produce_fenced(key, Some(value), group).await;
        }
        self.produce_unfenced(key, Some(value)).await
    }

    /// Produce a non-transactional ordering barrier. The reader waits for this
    /// offset before a primary derives ids or versions from its local state.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be produced or acknowledged.
    pub async fn barrier(&self) -> anyhow::Result<i64> {
        self.produce_unfenced(
            br#"{"keytype":"NOOP","magic":0}"#.to_vec(),
            Some(b"{}".to_vec()),
        )
        .await
    }

    async fn produce_unfenced(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> anyhow::Result<i64> {
        let rx = self
            .producer
            .send(ProducerRecord {
                topic: self.topic.clone(),
                key: Some(Bytes::from(key)),
                value: value.map(Bytes::from),
                ..Default::default()
            })
            .await;
        let meta = rx
            .await
            .map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
        tracing::Span::current().record("offset", meta.offset);
        Ok(meta.offset)
    }

    async fn produce_fenced(
        &self,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        group: &ConsumerGroupMetadata,
    ) -> anyhow::Result<i64> {
        let mut state = self.fenced_state.lock().await;
        if state.generation_id != Some(group.generation_id) {
            self.fenced_producer.init_transactions().await?;
            state.generation_id = Some(group.generation_id);
        }
        let transaction = self.fenced_producer.begin_transaction().await?;
        let result = async {
            let metadata = self
                .fenced_producer
                .send(ProducerRecord {
                    topic: self.topic.clone(),
                    key: Some(Bytes::from(key)),
                    value: value.map(Bytes::from),
                    ..Default::default()
                })
                .await
                .await
                .map_err(|_| anyhow::anyhow!("producer dropped ack"))??;
            // An empty TxnOffsetCommit still validates the election group's
            // generation and member id. A stale primary is rejected before
            // EndTxn can commit its schema record.
            self.fenced_producer
                .send_offsets_to_transaction(std::iter::empty(), group)
                .await?;
            Ok::<_, anyhow::Error>(metadata.offset)
        }
        .await;
        match result {
            Ok(offset) => {
                transaction
                    .commit()
                    .await
                    .map_err(|error| anyhow::Error::new(error.source))?;
                tracing::Span::current().record("offset", offset);
                Ok(offset)
            }
            Err(error) => {
                let _ = transaction.abort().await;
                Err(error)
            }
        }
    }

    /// Produce a tombstone (null value) for `key`; return the assigned offset.
    /// Used for permanent deletes and mode-clears (compaction reclaims the key).
    #[tracing::instrument(level = "debug", name = "schema_writer.produce_tombstone", skip_all, fields(topic = %self.topic, key_len = key.len(), offset = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when a schema is invalid or incompatible, registry storage fails, or serialized data does not conform to the selected schema.
    pub async fn produce_tombstone(
        &self,
        key: Vec<u8>,
        group: Option<&ConsumerGroupMetadata>,
    ) -> anyhow::Result<i64> {
        if let Some(group) = group {
            self.produce_fenced(key, None, group).await
        } else {
            self.produce_unfenced(key, None).await
        }
    }
}
