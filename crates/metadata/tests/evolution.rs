//! Proptest harness for `MetadataRecord` schema evolution. Today
//! everything is V1; the future-version policy is "decode v2 →
//! re-encode v1 round-trips for the fields v1 understands." We seed
//! that contract by asserting v1 ↔ v1 round-trips here.

use crabka_metadata::{
    BrokerRegistrationRecord, DeleteTopicRecord, LeaderEpoch, MetadataRecord, NodeId,
    PartitionRecord, TopicRecord,
};
use proptest::prelude::*;
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::{Deserialize as _, Serialize as _};

prop_compose! {
    fn arb_topic()(
        name in "[a-zA-Z][a-zA-Z0-9_-]{0,32}",
        partitions in 1..256i32,
        replication_factor in 1..16i16,
    ) -> TopicRecord {
        TopicRecord {
            name,
            topic_id: Uuid::new_v4(),
            partitions,
            replication_factor,
        }
    }
}

prop_compose! {
    fn arb_partition()(
        topic in "[a-zA-Z][a-zA-Z0-9_-]{0,32}",
        partition in 0..1024i32,
        replicas in prop::collection::vec(0..32u64, 1..6),
        leader_epoch in 0..i32::MAX,
    ) -> PartitionRecord {
        let replicas: Vec<NodeId> = replicas.into_iter().map(NodeId).collect();
        let leader = replicas[0];
        let isr = replicas.clone();
        PartitionRecord { topic, partition, leader, replicas, isr, leader_epoch: LeaderEpoch(leader_epoch), adding_replicas: vec![], removing_replicas: vec![], directories: vec![], partition_epoch: 0 }
    }
}

prop_compose! {
    fn arb_broker()(
        node_id in 0..1024u64,
        host in "[a-zA-Z][a-zA-Z0-9.-]{0,32}",
        port in 1024..65535u16,
        rack in prop::option::of("[a-zA-Z][a-zA-Z0-9-]{0,16}"),
    ) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord { node_id: NodeId(node_id), broker_epoch: 0, incarnation_id: uuid::Uuid::nil(), host, port, rack, endpoints: vec![] }
    }
}

fn arb_record() -> impl Strategy<Value = MetadataRecord> {
    prop_oneof![
        arb_topic().prop_map(MetadataRecord::V1Topic),
        arb_partition().prop_map(MetadataRecord::V1Partition),
        arb_broker().prop_map(MetadataRecord::V1BrokerRegistration),
        "[a-zA-Z][a-zA-Z0-9_-]{0,32}"
            .prop_map(|name| { MetadataRecord::V1DeleteTopic(DeleteTopicRecord { name }) }),
    ]
}

proptest! {
    #[test]
    fn record_round_trips_wincode(r in arb_record()) {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(&r).unwrap();
        let decoded: MetadataRecord =
            <SerdeCompat<MetadataRecord>>::deserialize(&bytes).unwrap();
        prop_assert_eq!(decoded, r);
    }

    #[test]
    fn batch_round_trips_wincode(records in prop::collection::vec(arb_record(), 0..32)) {
        let bytes = <SerdeCompat<Vec<MetadataRecord>>>::serialize(&records).unwrap();
        let decoded: Vec<MetadataRecord> =
            <SerdeCompat<Vec<MetadataRecord>>>::deserialize(&bytes).unwrap();
        prop_assert_eq!(decoded, records);
    }
}
