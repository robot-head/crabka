//! Proptest harness for `MetadataRecord` schema evolution. Today
//! everything is V1; the future-version policy is "decode v2 →
//! re-encode v1 round-trips for the fields v1 understands." We seed
//! that contract by asserting v1 ↔ v1 round-trips here.

use bincode::config::standard;
use crabka_metadata::{
    BrokerRegistrationRecord, DeleteTopicRecord, MetadataRecord, PartitionRecord, TopicRecord,
};
use proptest::prelude::*;
use uuid::Uuid;

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
    ) -> PartitionRecord {
        let leader = replicas[0];
        let isr = replicas.clone();
        PartitionRecord { topic, partition, leader, replicas, isr }
    }
}

prop_compose! {
    fn arb_broker()(
        node_id in 0..1024u64,
        host in "[a-zA-Z][a-zA-Z0-9.-]{0,32}",
        port in 1024..65535u16,
        rack in prop::option::of("[a-zA-Z][a-zA-Z0-9-]{0,16}"),
    ) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord { node_id, host, port, rack }
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
    fn record_round_trips_bincode(r in arb_record()) {
        let bytes = bincode::serde::encode_to_vec(&r, standard()).unwrap();
        let (decoded, _): (MetadataRecord, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).unwrap();
        prop_assert_eq!(decoded, r);
    }

    #[test]
    fn batch_round_trips_bincode(records in prop::collection::vec(arb_record(), 0..32)) {
        let bytes = bincode::serde::encode_to_vec(&records, standard()).unwrap();
        let (decoded, _): (Vec<MetadataRecord>, _) =
            bincode::serde::decode_from_slice(&bytes, standard()).unwrap();
        prop_assert_eq!(decoded, records);
    }
}
