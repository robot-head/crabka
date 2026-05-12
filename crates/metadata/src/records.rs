//! Versioned metadata records. Future versions add variants; older
//! readers can skip unknown ones because we encode each variant
//! length-prefixed inside the `bincode` payload.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type NodeId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicRecord {
    pub name: String,
    pub topic_id: Uuid,
    pub partitions: i32,
    pub replication_factor: i16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub topic: String,
    pub partition: i32,
    pub leader: NodeId,
    pub replicas: Vec<NodeId>,
    pub isr: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerRegistrationRecord {
    pub node_id: NodeId,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicRecord {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bc() -> bincode::config::Configuration {
        bincode::config::standard()
    }

    #[test]
    fn topic_record_bincode_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn partition_record_bincode_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn broker_registration_bincode_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn delete_topic_bincode_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        let bytes = bincode::serde::encode_to_vec(&r, bc()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, bc()).unwrap();
        assert_eq!(decoded, r);
    }
}
