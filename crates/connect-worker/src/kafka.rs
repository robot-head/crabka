//! Kafka sink and compacted-topic checkpoint store.

use std::collections::HashSet;

use async_trait::async_trait;
use bytes::Bytes;
use crabka_client_core::security::ClientSecurity;
use crabka_client_producer::{Acks, Header, Producer, ProducerRecord};
use crabka_connect::{CheckpointStore, ConnectError, ConnectRecord, Sink, SourceOffset};
use crabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::sync::oneshot::Receiver;

use crate::metrics::WorkerMetrics;

/// Compacted Kafka topic holding the most recent source offset per connector.
pub const CHECKPOINT_TOPIC: &str = "__crabka_connect_offsets";

#[derive(Clone)]
pub(crate) struct KafkaClientConfig {
    pub(crate) bootstrap: String,
    pub(crate) security: Option<ClientSecurity>,
    pub(crate) dispatch_queue_capacity: usize,
    pub(crate) frame_max_bytes: u64,
}

async fn build_producer(
    config: &KafkaClientConfig,
    client_id: String,
) -> Result<Producer, ConnectError> {
    let builder = Producer::builder()
        .bootstrap(config.bootstrap.clone())
        .client_id(client_id)
        .dispatch_queue_capacity(config.dispatch_queue_capacity)
        .frame_max(ByteSize::from_bytes(config.frame_max_bytes))
        .enable_idempotence(true)
        .acks(Acks::All);
    match config.security.clone() {
        Some(security) => builder.security(security).build().await,
        None => builder.build().await,
    }
    .map_err(|error| ConnectError::Backend(error.to_string()))
}

/// At-least-once Kafka sink backed by an idempotent `acks=all` producer.
pub struct KafkaSink {
    producer: Producer,
    client: KafkaClientConfig,
    topic_prefix: String,
    ensured_topics: HashSet<String>,
    pending: Vec<
        Receiver<
            Result<crabka_client_producer::RecordMetadata, crabka_client_producer::ProducerError>,
        >,
    >,
    metrics: WorkerMetrics,
}

impl KafkaSink {
    /// Connect an idempotent Kafka producer.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Backend`] when the producer cannot connect.
    pub async fn start(
        bootstrap: impl Into<String>,
        topic_prefix: impl Into<String>,
    ) -> Result<Self, ConnectError> {
        let client = KafkaClientConfig {
            bootstrap: bootstrap.into(),
            security: None,
            dispatch_queue_capacity: 64,
            frame_max_bytes: 100 * 1024 * 1024,
        };
        Self::start_with_config(client, topic_prefix.into(), WorkerMetrics::new()).await
    }

    pub(crate) async fn start_with_config(
        client: KafkaClientConfig,
        topic_prefix: String,
        metrics: WorkerMetrics,
    ) -> Result<Self, ConnectError> {
        let producer = build_producer(&client, "crabka-connect-worker-data".to_owned()).await?;
        Ok(Self {
            producer,
            client,
            topic_prefix,
            ensured_topics: HashSet::new(),
            pending: Vec::new(),
            metrics,
        })
    }
}

#[async_trait]
impl Sink<Bytes, Bytes> for KafkaSink {
    async fn put(&mut self, records: Vec<ConnectRecord<Bytes, Bytes>>) -> Result<(), ConnectError> {
        for record in records {
            let output = to_producer_record(record, &self.topic_prefix)?;
            if self.ensured_topics.insert(output.topic.clone()) {
                crabka_replicator::admin_util::ensure_topic(
                    &self.client.bootstrap,
                    &output.topic,
                    1,
                    self.client.security.clone(),
                )
                .await
                .map_err(ConnectError::Backend)?;
            }
            self.pending.push(self.producer.send(output).await);
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ConnectError> {
        let mut first_error = None;
        for acknowledgement in std::mem::take(&mut self.pending) {
            match acknowledgement.await {
                Ok(Ok(_)) => self.metrics.record_produced(),
                Ok(Err(error)) => {
                    first_error.get_or_insert_with(|| error.to_string());
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        "producer dropped acknowledgement sender".to_owned()
                    });
                }
            }
        }
        if let Err(error) = self.producer.flush().await {
            first_error.get_or_insert_with(|| error.to_string());
        }
        if let Some(error) = first_error {
            self.metrics.record_error();
            Err(ConnectError::Backend(error))
        } else {
            Ok(())
        }
    }

    async fn close(&mut self) -> Result<(), ConnectError> {
        self.flush().await
    }
}

fn to_producer_record(
    record: ConnectRecord<Bytes, Bytes>,
    topic_prefix: &str,
) -> Result<ProducerRecord, ConnectError> {
    let source_topic = record.topic.ok_or_else(|| {
        ConnectError::Backend("source record is missing destination topic".to_owned())
    })?;
    if matches!(record.partition, Some(partition) if partition < 0) {
        return Err(ConnectError::Backend(
            "source record has a negative destination partition".to_owned(),
        ));
    }
    Ok(ProducerRecord {
        topic: prefixed_topic(topic_prefix, &source_topic),
        partition: record.partition,
        key: record.key,
        value: record.value,
        headers: record
            .headers
            .into_iter()
            .map(|header| Header {
                key: header.key,
                value: header.value,
            })
            .collect(),
        timestamp_ms: record.timestamp,
    })
}

fn prefixed_topic(prefix: &str, topic: &str) -> String {
    let prefix = prefix.trim_end_matches('.');
    if prefix.is_empty() {
        topic.to_owned()
    } else {
        format!("{prefix}.{}", topic.trim_start_matches('.'))
    }
}

/// Durable connector checkpoint store keyed by connector ID.
pub struct KafkaCheckpointStore {
    producer: Producer,
    client: KafkaClientConfig,
    connector_id: String,
    metrics: WorkerMetrics,
}

impl KafkaCheckpointStore {
    /// Ensure the compacted checkpoint topic and connect an idempotent producer.
    ///
    /// # Errors
    ///
    /// Returns [`ConnectError::Offset`] when topic creation or producer startup fails.
    pub async fn start(
        bootstrap: impl Into<String>,
        connector_id: impl Into<String>,
    ) -> Result<Self, ConnectError> {
        let client = KafkaClientConfig {
            bootstrap: bootstrap.into(),
            security: None,
            dispatch_queue_capacity: 64,
            frame_max_bytes: 100 * 1024 * 1024,
        };
        Self::start_with_config(client, connector_id.into(), WorkerMetrics::new()).await
    }

    pub(crate) async fn start_with_config(
        client: KafkaClientConfig,
        connector_id: String,
        metrics: WorkerMetrics,
    ) -> Result<Self, ConnectError> {
        crabka_replicator::admin_util::ensure_compacted_topic(
            &client.bootstrap,
            CHECKPOINT_TOPIC,
            client.security.clone(),
        )
        .await
        .map_err(ConnectError::Offset)?;
        let producer = build_producer(&client, "crabka-connect-worker-checkpoint".to_owned())
            .await
            .map_err(|error| ConnectError::Offset(error.to_string()))?;
        Ok(Self {
            producer,
            client,
            connector_id,
            metrics,
        })
    }
}

#[async_trait]
impl CheckpointStore for KafkaCheckpointStore {
    async fn save(&self, offset: &SourceOffset) -> Result<(), ConnectError> {
        let value =
            serde_json::to_vec(offset).map_err(|error| ConnectError::Offset(error.to_string()))?;
        let acknowledgement = self
            .producer
            .send(ProducerRecord {
                topic: CHECKPOINT_TOPIC.to_owned(),
                partition: Some(0),
                key: Some(Bytes::copy_from_slice(self.connector_id.as_bytes())),
                value: Some(Bytes::from(value)),
                headers: Vec::new(),
                timestamp_ms: None,
            })
            .await;
        acknowledgement
            .await
            .map_err(|_| {
                ConnectError::Offset(
                    "checkpoint producer dropped acknowledgement sender".to_owned(),
                )
            })?
            .map_err(|error| ConnectError::Offset(error.to_string()))?;
        self.producer
            .flush()
            .await
            .map_err(|error| ConnectError::Offset(error.to_string()))?;
        self.metrics.record_checkpoint();
        Ok(())
    }

    async fn load(&self) -> Result<Option<SourceOffset>, ConnectError> {
        let value = crabka_replicator::admin_util::read_last_value_for_key(
            &self.client.bootstrap,
            CHECKPOINT_TOPIC,
            self.connector_id.as_bytes(),
            self.client.security.clone(),
        )
        .await
        .map_err(ConnectError::Offset)?;
        value
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .map_err(|error| ConnectError::Offset(error.to_string()))
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn record_mapping_preserves_kafka_fields_and_applies_prefix() {
        let source = ConnectRecord::new(
            Some(Bytes::from_static(b"key")),
            Some(Bytes::from_static(b"value")),
        )
        .with_topic("public.orders")
        .with_partition(2)
        .with_timestamp(42)
        .with_header("trace", Some(Bytes::from_static(b"abc")));
        let output = to_producer_record(source, "db.").expect("record maps");
        assert!(
            output
                == ProducerRecord {
                    topic: "db.public.orders".to_owned(),
                    partition: Some(2),
                    key: Some(Bytes::from_static(b"key")),
                    value: Some(Bytes::from_static(b"value")),
                    headers: vec![Header {
                        key: "trace".to_owned(),
                        value: Some(Bytes::from_static(b"abc")),
                    }],
                    timestamp_ms: Some(42),
                }
        );
    }

    #[test]
    fn record_mapping_preserves_tombstone_and_rejects_missing_topic() {
        let tombstone =
            ConnectRecord::new(Some(Bytes::from_static(b"key")), None).with_topic("orders");
        let output = to_producer_record(tombstone, "").expect("tombstone maps");
        assert!(output.value.is_none());
        assert!(output.topic == "orders");
        assert!(to_producer_record(ConnectRecord::new(None, None), "db").is_err());
    }
}
