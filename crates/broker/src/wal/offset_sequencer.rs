//! Controller-backed offset assignment for diskless WAL partitions.

use std::sync::Arc;

use async_trait::async_trait;
use crabka_ids::{Offset, PartitionIndex};
use crabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

use crate::{error::BrokerError, metadata_source::MetadataSource};

#[async_trait]
pub(crate) trait OffsetSequencer: Send + Sync {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError>;
}

pub(crate) struct ControllerSequencer {
    metadata: Arc<dyn MetadataSource>,
}

impl ControllerSequencer {
    #[must_use]
    pub(crate) fn new(metadata: Arc<dyn MetadataSource>) -> Self {
        Self { metadata }
    }
}

#[async_trait]
impl OffsetSequencer for ControllerSequencer {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError> {
        let result = self
            .metadata
            .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: topic.to_owned(),
                    partition: partition.0,
                    count: i64::from(count),
                },
            )])
            .await
            .map_err(|error| BrokerError::Replication(format!("offset sequencer: {error}")))?;

        let [reservation] = result.offset_reservations.as_slice() else {
            return Err(BrokerError::Replication(format!(
                "offset sequencer: expected one reservation, got {}",
                result.offset_reservations.len()
            )));
        };
        if reservation.topic != topic
            || reservation.partition != partition.0
            || reservation.count != i64::from(count)
        {
            return Err(BrokerError::Replication(
                "offset sequencer: reservation does not match request".to_string(),
            ));
        }
        Ok(Offset(reservation.base_offset))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

    use crabka_metadata::{MetadataImage, MetadataRecord};
    use crabka_raft::{
        AddVoter, Node, NodeId, OffsetReservation, QuorumState, RaftError, ReconfigOutcome,
        RemoveVoter, SnapshotRange, SubmitChangeResult, UpdateVoter,
    };
    use tokio::sync::watch;

    use super::*;

    struct FakeMetadataSource {
        result: SubmitChangeResult,
    }

    #[async_trait]
    impl MetadataSource for FakeMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            Arc::new(MetadataImage::default())
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.current_image());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            let (_tx, rx) = watch::channel(None);
            rx
        }

        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: 0,
                last_applied_index: 0,
                current_leader: None,
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        async fn submit_change(
            &self,
            records: Vec<MetadataRecord>,
        ) -> Result<SubmitChangeResult, RaftError> {
            assert!(matches!(
                records.as_slice(),
                [MetadataRecord::V1PartitionOffsetAdvance(record)]
                    if record.topic == "topic" && record.partition == 0 && record.count == 3
            ));
            Ok(self.result.clone())
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            "127.0.0.1:0".parse().unwrap()
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn cancel(&self) {}
    }

    #[tokio::test]
    async fn controller_sequencer_uses_returned_reservation_base() {
        let sequencer = ControllerSequencer::new(Arc::new(FakeMetadataSource {
            result: SubmitChangeResult {
                offset_reservations: vec![OffsetReservation {
                    topic: "topic".to_string(),
                    partition: 0,
                    base_offset: 11,
                    count: 3,
                }],
            },
        }));

        let base = sequencer
            .assign("topic", PartitionIndex(0), 3)
            .await
            .unwrap();

        assert_eq!(base, Offset(11));
    }
}
