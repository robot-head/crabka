//! Narrow KIP-853 metadata-quorum administration used by the operator.

use crabka_protocol::owned::{
    describe_quorum_request::{
        DescribeQuorumRequest, PartitionData as RequestPartition, TopicData as RequestTopic,
    },
    remove_raft_voter_request::RemoveRaftVoterRequest,
};

use crate::{AdminClient, AdminError, kafka_error_name};

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplica {
    pub node_id: i32,
    pub directory_id: uuid::Uuid,
    pub log_end_offset: i64,
    pub last_fetch_timestamp: i64,
    pub last_caught_up_timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataQuorum {
    pub leader_id: i32,
    pub leader_epoch: i32,
    pub high_watermark: i64,
    pub voters: Vec<QuorumReplica>,
    pub observers: Vec<QuorumReplica>,
}

fn broker_error(api: &'static str, code: i16, message: Option<String>) -> AdminError {
    AdminError::Broker {
        api,
        code,
        name: kafka_error_name(code),
        message,
    }
}

fn replica(
    value: &crabka_protocol::owned::common::describe_quorum_response::replica_state::ReplicaState,
) -> QuorumReplica {
    QuorumReplica {
        node_id: value.replica_id,
        directory_id: uuid::Uuid::from_bytes(value.replica_directory_id.0),
        log_end_offset: value.log_end_offset,
        last_fetch_timestamp: value.last_fetch_timestamp,
        last_caught_up_timestamp: value.last_caught_up_timestamp,
    }
}

impl AdminClient {
    /// Return the live `__cluster_metadata` partition-zero quorum view.
    ///
    /// # Errors
    /// Returns a transport, protocol, or Kafka error from `DescribeQuorum`.
    pub async fn describe_metadata_quorum(&mut self) -> Result<MetadataQuorum, AdminError> {
        let response = self
            .conn
            .send(DescribeQuorumRequest {
                topics: vec![RequestTopic {
                    topic_name: CLUSTER_METADATA_TOPIC.into(),
                    partitions: vec![RequestPartition {
                        partition_index: 0,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await?;
        if response.error_code != 0 {
            return Err(broker_error(
                "DescribeQuorum",
                response.error_code,
                response.error_message,
            ));
        }
        let partition = response
            .topics
            .into_iter()
            .find(|topic| topic.topic_name == CLUSTER_METADATA_TOPIC)
            .and_then(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .find(|partition| partition.partition_index == 0)
            })
            .ok_or_else(|| {
                AdminError::Protocol("DescribeQuorum omitted __cluster_metadata partition 0".into())
            })?;
        if partition.error_code != 0 {
            return Err(broker_error(
                "DescribeQuorum",
                partition.error_code,
                partition.error_message,
            ));
        }
        Ok(MetadataQuorum {
            leader_id: partition.leader_id,
            leader_epoch: partition.leader_epoch,
            high_watermark: partition.high_watermark,
            voters: partition.current_voters.iter().map(replica).collect(),
            observers: partition.observers.iter().map(replica).collect(),
        })
    }

    /// Remove one exact node and directory identity from the metadata quorum.
    ///
    /// # Errors
    /// Returns a transport, protocol, or Kafka error from `RemoveRaftVoter`.
    pub async fn remove_raft_voter(
        &mut self,
        cluster_id: uuid::Uuid,
        node_id: i32,
        directory_id: uuid::Uuid,
    ) -> Result<(), AdminError> {
        let response = self
            .conn
            .send(RemoveRaftVoterRequest {
                cluster_id: Some(cluster_id.to_string()),
                voter_id: node_id,
                voter_directory_id: crabka_protocol::primitives::uuid::Uuid(
                    *directory_id.as_bytes(),
                ),
                ..Default::default()
            })
            .await?;
        if response.error_code != 0 {
            return Err(broker_error(
                "RemoveRaftVoter",
                response.error_code,
                response.error_message,
            ));
        }
        Ok(())
    }
}
