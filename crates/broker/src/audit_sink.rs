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
    // cargo-mutants: Debug formatting, no behavioral contract
    #[cfg_attr(test, mutants::skip)]
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
        // `offset_delta`/`key` are left at their `Record::default()` values (0 /
        // None) — a single audit record with no key. Spelling them out would only
        // create equivalent "delete field" mutants.
        batch.records.push(Record {
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;
    use crabka_log::{Log, LogConfig};
    use std::sync::Arc;

    fn fixture_partition(
        log_dir: &std::path::Path,
        topic: &str,
        partition: i32,
    ) -> Arc<crate::partition::Partition> {
        let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
        std::fs::create_dir_all(&part_dir).expect("create partition dir");
        let log = Log::open(&part_dir, LogConfig::default()).expect("open log");
        crate::broker::spawn_partition(
            topic.to_string(),
            partition,
            log_dir.to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
        )
    }

    #[tokio::test]
    async fn write_appends_record_value_and_headers_to_local_partition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let partitions = Arc::new(PartitionRegistry::new());
        let partition = fixture_partition(dir.path(), "__audit", 0);
        partitions.insert("__audit".to_string(), 0, Arc::clone(&partition));
        let sink =
            KafkaTopicAuditSink::new(partitions, "__audit".to_string(), 0, BrokerMetrics::new());

        sink.write(AuditRecord {
            class: crabka_audit::AuditEventClass::ApiActivity,
            value: b"{\"ok\":true}".to_vec(),
            headers: vec![("event_class".to_string(), b"admin".to_vec())],
        })
        .await
        .expect("write audit record");

        let out = partition
            .read_log(0, 1 << 20)
            .expect("read audit partition");
        let records: Vec<_> = out.batches.iter().flat_map(|b| &b.records).collect();

        assert!(
            (
                records.len(),
                records[0].value.as_deref(),
                records[0].headers.len(),
                records[0].headers[0].key.as_str(),
                records[0].headers[0].value.as_deref(),
            ) == (
                1,
                Some(&b"{\"ok\":true}"[..]),
                1,
                "event_class",
                Some(&b"admin"[..]),
            )
        );
    }
}
