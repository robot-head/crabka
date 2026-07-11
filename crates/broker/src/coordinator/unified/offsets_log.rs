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

/// Resolves `__consumer_offsets-0` at every `append` call. The partition
/// is registered by bootstrap *after* `GroupCoordinator` is constructed,
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

        /// `true` iff some appended record tombstones the classic k2
        /// `GroupMetadata` record for `group_id` — a key whose leading `i16`
        /// version is `2` (classic `GroupMetadata`) with a null value. Used to
        /// assert the upgrade flip atomically removed the classic group record.
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

        /// `true` iff some appended record tombstones the next-gen k3
        /// `GroupMetadata` record for `group_id` — a key whose leading `i16`
        /// version is `3` (next-gen `GroupMetadata`) with a null value. Used to
        /// assert the downgrade flip atomically removed the next-gen group
        /// record. `parse_key` dispatches version 3 to the next-gen family.
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

        /// `true` iff some appended record tombstones the group-level next-gen
        /// k6 `TargetAssignmentMetadata` record for `group_id` — a key whose
        /// leading `i16` version is `6` with a null value. Used to assert the
        /// downgrade flip also drops the group-level target metadata, which
        /// would otherwise survive log compaction and resurrect the group as
        /// next-gen on replay. `parse_key` dispatches version 6 to the next-gen
        /// family.
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
        assert2::assert!(got == vec![b1, b2]);
    }

    #[tokio::test]
    async fn fake_fails_when_armed() {
        let log = fake::InMemoryOffsetsLog::default();
        log.fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert2::assert!(log.append(RecordBatch::default()).await.is_err());
        assert2::assert!(log.append(RecordBatch::default()).await.is_ok());
    }
}
