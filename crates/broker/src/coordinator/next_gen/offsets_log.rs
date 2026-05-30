use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crabka_protocol::records::RecordBatch;

use crate::error::BrokerError;
use crate::partition::{ProduceJob, WriterMessage};
use crate::partition_registry::PartitionRegistry;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

#[async_trait]
pub trait OffsetsLog: Send + Sync + std::fmt::Debug {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError>;
}

/// Resolves `__consumer_offsets-0` at every `append` call. The partition
/// is registered by bootstrap *after* `NextGenCoordinator` is constructed,
/// so a snapshot taken at construction time would be permanently empty.
#[derive(Debug)]
pub(crate) struct ProductionOffsetsLog {
    partitions: Arc<PartitionRegistry>,
}

impl ProductionOffsetsLog {
    #[must_use]
    pub(crate) fn new(partitions: Arc<PartitionRegistry>) -> Self {
        Self { partitions }
    }
}

#[async_trait]
impl OffsetsLog for ProductionOffsetsLog {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
        let Some(partition) = self.partitions.get(OFFSETS_TOPIC, OFFSETS_PARTITION) else {
            return Err(BrokerError::PartitionWriterDied {
                topic: OFFSETS_TOPIC.into(),
                partition: OFFSETS_PARTITION,
            });
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if partition
            .writer_tx
            .send(WriterMessage::Produce(ProduceJob { batch, ack: ack_tx }))
            .await
            .is_err()
        {
            return Err(BrokerError::PartitionWriterDied {
                topic: OFFSETS_TOPIC.into(),
                partition: OFFSETS_PARTITION,
            });
        }
        match ack_rx.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(BrokerError::PartitionWriterDied {
                topic: OFFSETS_TOPIC.into(),
                partition: OFFSETS_PARTITION,
            }),
        }
    }
}

pub mod fake {
    use super::{BrokerError, OFFSETS_PARTITION, OFFSETS_TOPIC, OffsetsLog, async_trait};
    use crabka_protocol::records::RecordBatch;
    use tokio::sync::Mutex;

    #[derive(Debug, Default)]
    pub struct InMemoryOffsetsLog {
        pub appended: Mutex<Vec<RecordBatch>>,
        pub fail_next: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl OffsetsLog for InMemoryOffsetsLog {
        async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError> {
            if self
                .fail_next
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::PartitionWriterDied {
                    topic: OFFSETS_TOPIC.into(),
                    partition: OFFSETS_PARTITION,
                });
            }
            self.appended.lock().await.push(batch);
            Ok(())
        }
    }

    impl InMemoryOffsetsLog {
        pub async fn batches(&self) -> Vec<RecordBatch> {
            self.appended.lock().await.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[tokio::test]
    async fn fake_records_in_order() {
        let log = fake::InMemoryOffsetsLog::default();
        let b1 = RecordBatch::default();
        let b2 = RecordBatch {
            max_timestamp: 42,
            ..Default::default()
        };
        log.append(b1.clone()).await.unwrap();
        log.append(b2.clone()).await.unwrap();
        let got = log.batches().await;
        assert!(got.len() == 2);
        assert!(got[1].max_timestamp == 42);
    }

    #[tokio::test]
    async fn fake_fails_when_armed() {
        let log = fake::InMemoryOffsetsLog::default();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(log.append(RecordBatch::default()).await.is_err());
        assert!(log.append(RecordBatch::default()).await.is_ok());
    }
}
