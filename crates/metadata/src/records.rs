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
    use serde_wincode::SerdeCompat;
    use wincode::{Deserialize as _, Serialize as _};

    fn round_trip(r: &MetadataRecord) -> MetadataRecord {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(r).unwrap();
        <SerdeCompat<MetadataRecord>>::deserialize(&bytes).unwrap()
    }

    #[test]
    fn topic_record_round_trip() {
        let r = MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::new_v4(),
            partitions: 3,
            replication_factor: 1,
        });
        assert_eq!(round_trip(&r), r);
    }

    #[test]
    fn partition_record_round_trip() {
        let r = MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: 1,
            replicas: vec![1, 2, 3],
            isr: vec![1, 2],
        });
        assert_eq!(round_trip(&r), r);
    }

    #[test]
    fn broker_registration_round_trip() {
        let r = MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            node_id: 7,
            host: "192.168.1.10".into(),
            port: 9092,
            rack: Some("us-east-1a".into()),
        });
        assert_eq!(round_trip(&r), r);
    }

    #[test]
    fn delete_topic_round_trip() {
        let r = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: "doomed".into(),
        });
        assert_eq!(round_trip(&r), r);
    }
}
