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
    /// Per-partition leader epoch. Bumped on every leader change.
    /// Slice-10b adds this; older on-disk metadata is not migrated.
    pub leader_epoch: i32,
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

/// Mutable topic configuration overrides. Authoritative target state:
/// each `V1TopicConfig` record fully replaces the previous override map
/// for `topic`. Empty map = clear all overrides. Merging happens at the
/// `AlterConfigs` handler before the record is submitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicConfigRecord {
    pub topic: String,
    pub overrides: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MetadataRecord {
    V1Topic(TopicRecord),
    V1Partition(PartitionRecord),
    V1BrokerRegistration(BrokerRegistrationRecord),
    V1DeleteTopic(DeleteTopicRecord),
    V1TopicConfig(TopicConfigRecord),
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
            leader_epoch: 0,
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

    #[test]
    fn topic_config_record_round_trip() {
        let mut overrides = std::collections::BTreeMap::new();
        overrides.insert("retention.ms".to_string(), "60000".to_string());
        overrides.insert("segment.bytes".to_string(), "1048576".to_string());
        let r = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides,
        });
        assert_eq!(round_trip(&r), r);
    }
}
