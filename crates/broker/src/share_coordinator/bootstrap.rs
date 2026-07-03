//! Lazy creation of the `__share_group_state` internal topic (KIP-932).
//! Mirrors the `__transaction_state` bootstrap.

use std::sync::Arc;

use crabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use crabka_raft::RaftError;
use uuid::Uuid;

pub const TOPIC: &str = "__share_group_state";
pub const NUM_PARTITIONS: i32 = 50;

/// Ensure `__share_group_state` exists in the controller's metadata.
/// No-op if it already does. Tolerate `TopicExists` in case a concurrent
/// `FindCoordinator(SHARE)` already created it.
pub(crate) async fn ensure_topic(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
) -> Result<(), crate::error::BrokerError> {
    let image = controller.current_image();
    if image.topic(TOPIC).is_some() {
        return Ok(());
    }

    // Collect registered brokers for round-robin replica assignment. The
    // broker count drives the replication factor, capped at 3.
    let mut sorted: Vec<NodeId> = image.brokers().map(|b| b.node_id).collect();
    if sorted.is_empty() {
        return Err(crate::error::BrokerError::Share(
            "no brokers registered; cannot bootstrap __share_group_state".into(),
        ));
    }
    sorted.sort_unstable();

    let k = sorted.len();
    // k is already capped at 3 so try_from cannot fail; use u8 as an
    // intermediate to satisfy clippy without a silent truncation.
    let rf_usize = k.min(3);
    let rf = i16::try_from(rf_usize).expect("rf <= 3 always fits in i16");

    let mut records: Vec<MetadataRecord> = Vec::new();
    let topic_id = Uuid::new_v4();
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_string(),
        topic_id,
        partitions: NUM_PARTITIONS,
        replication_factor: rf,
    }));

    for p in 0..NUM_PARTITIONS {
        let mut replicas = Vec::with_capacity(rf_usize);
        // p >= 0 (i32 literal range), k >= 1; safe to cast.
        let base = usize::try_from(p).expect("partition index fits in usize");
        for i in 0..rf_usize {
            replicas.push(sorted[(base + i) % k]);
        }
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.to_string(),
            partition: p,
            leader: replicas[0],
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: crabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }

    match controller.submit_change(records).await {
        Ok(()) | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(crate::error::BrokerError::Share(format!(
            "submit_change failed: {e}"
        ))),
    }
}
