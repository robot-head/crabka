//! Lazy creation of the `__transaction_state` internal topic.
//!
//! This mirrors the `__consumer_offsets` bootstrap.

use std::sync::Arc;

use crabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use crabka_raft::RaftError;
use uuid::Uuid;

pub const TOPIC: &str = "__transaction_state";

/// Make sure `__transaction_state` exists in the controller's metadata.
///
/// The function does nothing if the topic already exists. It tolerates
/// `TopicExists`, because a concurrent `FindCoordinator(TRANSACTION)` can
/// create the topic first.
pub(crate) async fn ensure_topic(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    num_partitions: i32,
    replication_factor: i16,
) -> Result<(), crate::error::BrokerError> {
    let image = controller.current_image();
    if image.topic(TOPIC).is_some() {
        return Ok(());
    }

    // Collect registered brokers for round-robin replica assignment.
    let mut sorted: Vec<NodeId> = image.brokers().map(|b| b.node_id).collect();
    if sorted.is_empty() {
        return Err(crate::error::BrokerError::Txn(
            "no brokers registered; cannot bootstrap __transaction_state".into(),
        ));
    }
    sorted.sort_unstable();

    let k = sorted.len();
    let rf_usize = crate::bootstrap::internal_topic_replication_factor(replication_factor, k);
    let rf = i16::try_from(rf_usize).expect("bounded by configured i16 replication factor");

    let mut records: Vec<MetadataRecord> = Vec::new();
    let topic_id = Uuid::new_v4();
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_string(),
        topic_id,
        partitions: num_partitions,
        replication_factor: rf,
    }));

    for p in 0..num_partitions {
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
        Ok(_) | Err(RaftError::Metadata(crabka_metadata::MetadataError::TopicExists(_))) => Ok(()),
        Err(e) => Err(crate::error::BrokerError::Txn(format!(
            "submit_change failed: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;
    use crate::{broker::Broker, config::BrokerConfig};

    #[tokio::test]
    async fn nondefault_partition_count_controls_created_topic() {
        let dir = tempdir().unwrap();
        let handle = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
            .await
            .expect("start broker");
        let broker = handle.broker_arc_for_test();

        ensure_topic(&broker.controller, 7, 3)
            .await
            .expect("create transaction-state topic");

        let image = handle.controller_image_for_test();
        let topic = image.topic(TOPIC).expect("transaction-state topic");
        assert!(topic.partitions == 7);
        assert!(topic.replication_factor == 1);
        assert!(image.partitions_of(TOPIC).count() == 7);
        handle.shutdown().await;
    }
}
