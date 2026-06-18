//! Kafka-native audit sink: appends OCSF records to this broker's partition of
//! the internal audit topic (broker-affinity write path).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use crabka_audit::{AuditError, AuditRecord, AuditSink};
use crabka_protocol::records::{Record, RecordBatch, RecordHeader};

use crate::metrics::BrokerMetrics;
use crate::partition_registry::PartitionRegistry;

/// Writes audit records to a single partition of the audit topic that this
/// broker leads. Slice 1: the partition index is resolved once at construction.
pub struct KafkaTopicAuditSink {
    partitions: Arc<PartitionRegistry>,
    topic: String,
    partition_index: i32,
    metrics: BrokerMetrics,
}

impl std::fmt::Debug for KafkaTopicAuditSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaTopicAuditSink")
            .field("topic", &self.topic)
            .field("partition_index", &self.partition_index)
            .finish_non_exhaustive()
    }
}

impl KafkaTopicAuditSink {
    #[must_use]
    pub(crate) fn new(
        partitions: Arc<PartitionRegistry>,
        topic: String,
        partition_index: i32,
        metrics: BrokerMetrics,
    ) -> Self {
        Self {
            partitions,
            topic,
            partition_index,
            metrics,
        }
    }
}

#[async_trait]
impl AuditSink for KafkaTopicAuditSink {
    async fn write(&self, record: AuditRecord) -> Result<(), AuditError> {
        let Some(partition) = self.partitions.get(&self.topic, self.partition_index) else {
            self.metrics.audit_write_failures_total.inc();
            return Err(AuditError::Sink(format!(
                "audit partition {}-{} not local",
                self.topic, self.partition_index
            )));
        };

        let headers = record
            .headers
            .into_iter()
            .map(|(k, v)| RecordHeader {
                key: k,
                value: Some(Bytes::from(v)),
            })
            .collect();
        let mut batch = RecordBatch::default();
        batch.records.push(Record {
            offset_delta: 0,
            key: None,
            value: Some(Bytes::from(record.value)),
            headers,
            ..Default::default()
        });
        batch.last_offset_delta = 0;

        match partition.produce_batch(batch).await {
            Ok(_) => {
                self.metrics.audit_events_total.inc();
                Ok(())
            }
            Err(e) => {
                self.metrics.audit_write_failures_total.inc();
                Err(AuditError::Sink(e.to_string()))
            }
        }
    }
}
