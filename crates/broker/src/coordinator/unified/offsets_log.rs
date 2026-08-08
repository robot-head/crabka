use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::PartitionIndex;
use crabka_protocol::records::RecordBatch;
use tokio::sync::oneshot;

use crate::{
    error::BrokerError,
    partition::{ProduceData, ProduceJob, WriterMessage},
    partition_registry::PartitionRegistry,
};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";
pub const OFFSETS_PARTITION: i32 = 0;

#[async_trait]
pub trait OffsetsLog: Send + Sync + std::fmt::Debug {
    async fn append(&self, batch: RecordBatch) -> Result<(), BrokerError>;
}

/// Resolves `__consumer_offsets-0` on every `append` call. Bootstrap registers
/// the partition *after* the broker constructs `GroupCoordinator`, so a
/// snapshot taken at construction time would stay empty forever.
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
        let Some(partition) = self
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(OFFSETS_PARTITION))
        else {
            return Err(BrokerError::PartitionWriterDied {
                topic: OFFSETS_TOPIC.into(),
                partition: OFFSETS_PARTITION,
            });
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        if partition
            .writer_tx
            .send(WriterMessage::Produce(ProduceJob {
                data: ProduceData::Owned(batch),
                ack: ack_tx,
            }))
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
    use crabka_protocol::records::RecordBatch;
    use tokio::sync::Mutex;

    use super::{BrokerError, OFFSETS_PARTITION, OFFSETS_TOPIC, OffsetsLog, async_trait};

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

        /// Returns `true` if and only if an appended record tombstones the
        /// classic k2 `GroupMetadata` record for `group_id`. Such a record has
        /// a key whose leading `i16` version is `2`, for classic
        /// `GroupMetadata`, and a null value. Tests read it to assert that the
        /// upgrade flip removed the classic group record atomically.
        pub async fn has_classic_group_metadata_tombstone(&self, group_id: &str) -> bool {
            use crate::coordinator::unified::persistence::{Key, parse_key};
            self.appended.lock().await.iter().any(|batch| {
                batch.records.iter().any(|rec| {
                    rec.value.is_none()
                        && rec.key.as_ref().is_some_and(|k| {
                            matches!(
                                parse_key(k),
                                Ok(Key::GroupMetadata { group_id: ref gid }) if gid == group_id
                            )
                        })
                })
            })
        }

        /// Returns `true` if and only if an appended record tombstones the
        /// next-gen k3 `GroupMetadata` record for `group_id`. Such a record has
        /// a key whose leading `i16` version is `3`, for next-gen
        /// `GroupMetadata`, and a null value. Tests read it to assert that the
        /// downgrade flip removed the next-gen group record atomically.
        /// `parse_key` dispatches version 3 to the next-gen family.
        pub async fn has_next_gen_group_metadata_tombstone(&self, group_id: &str) -> bool {
            use crate::coordinator::unified::{
                persistence::{Key, parse_key},
                persistence_next_gen::NextGenKey,
            };
            self.appended.lock().await.iter().any(|batch| {
                batch.records.iter().any(|rec| {
                    rec.value.is_none()
                        && rec.key.as_ref().is_some_and(|k| {
                            matches!(
                                parse_key(k),
                                Ok(Key::NextGen(NextGenKey::GroupMetadata { group_id: ref gid }))
                                    if gid == group_id
                            )
                        })
                })
            })
        }

        /// Returns `true` if and only if an appended record tombstones the
        /// group-level next-gen k6 `TargetAssignmentMetadata` record for
        /// `group_id`. Such a record has a key whose leading `i16` version is
        /// `6`, and a null value.
        ///
        /// Tests read it to assert that the downgrade flip also drops the
        /// group-level target metadata. That metadata would otherwise survive
        /// log compaction and bring the group back as next-gen on replay.
        /// `parse_key` dispatches version 6 to the next-gen family.
        pub async fn has_next_gen_target_metadata_tombstone(&self, group_id: &str) -> bool {
            use crate::coordinator::unified::{
                persistence::{Key, parse_key},
                persistence_next_gen::NextGenKey,
            };
            self.appended.lock().await.iter().any(|batch| {
                batch.records.iter().any(|rec| {
                    rec.value.is_none()
                        && rec.key.as_ref().is_some_and(|k| {
                            matches!(
                                parse_key(k),
                                Ok(Key::NextGen(NextGenKey::TargetAssignmentMetadata {
                                    group_id: ref gid
                                })) if gid == group_id
                            )
                        })
                })
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

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
