//! `CodSpeed` microbenchmarks for `crabka-metadata`.
//!
//! Covers the hot paths for the Raft state machine and for request handlers:
//!
//! - `MetadataImage::apply` for the common record kinds.
//! - `MetadataImage::partitions_of` / `all_partitions` — per-topic lookups
//!   and the reconcile-loop scan, at many-topic cluster scale.
//! - `MetadataImage::matching_acls` — called per `authorize()`.
//! - `MetadataRecord` serialize/deserialize via wincode.

use crabka_metadata::{
    AclEntry, AclOperation, BrokerEndpoint, BrokerRegistrationRecord, LeaderEpoch, MetadataImage,
    MetadataRecord, NodeId, PartitionRecord, PatternType, PermissionType, ResourceType,
    TopicRecord,
};
use crabka_security::ListenerProtocol;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_wincode::SerdeCompat;
use uuid::Uuid;
use wincode::{Deserialize as _, Serialize as _};

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn topic_record(name: &str, parts: i32) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.to_string(),
        topic_id: Uuid::nil(),
        partitions: parts,
        replication_factor: 3,
    })
}

fn partition_record(topic: &str, p: i32) -> MetadataRecord {
    MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.to_string(),
        partition: p,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        leader_epoch: LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    })
}

fn broker_record(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        node_id: NodeId(node_id),
        broker_epoch: 0,
        incarnation_id: Uuid::nil(),
        host: format!("broker-{node_id}.example.com"),
        port: 9092,
        rack: Some("us-east-1a".to_string()),
        endpoints: vec![BrokerEndpoint {
            name: "PLAINTEXT".to_string(),
            host: format!("broker-{node_id}"),
            port: 9092,
            protocol: ListenerProtocol::Plaintext,
        }],
    })
}

fn acl_entry(name: &str, prefixed: bool) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: name.to_string(),
        pattern_type: if prefixed {
            PatternType::Prefixed
        } else {
            PatternType::Literal
        },
        principal: "User:alice".to_string(),
        host: "*".to_string(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }
}

fn image_with_acls(num_literal: usize, num_prefixed: usize) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for i in 0..num_literal {
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_entry(
            &format!("topic-{i:04}"),
            false,
        )));
    }
    for i in 0..num_prefixed {
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_entry(
            &format!("prefix-{i:02}-"),
            true,
        )));
    }
    img
}

// ---------------------------------------------------------------------------
// MetadataImage::apply — bulk replay of records.
// ---------------------------------------------------------------------------

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/apply");

    let topics: Vec<MetadataRecord> = (0..100)
        .map(|i| topic_record(&format!("t-{i}"), 8))
        .collect();
    let partitions: Vec<MetadataRecord> = (0..100)
        .flat_map(|t| (0..8).map(move |p| partition_record(&format!("t-{t}"), p)))
        .collect();
    let brokers: Vec<MetadataRecord> = (0..16).map(broker_record).collect();

    group.bench_function("replay_100_topics", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&topics) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    group.bench_function("replay_100topics_800partitions", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&topics) {
                img.apply(r);
            }
            for r in black_box(&partitions) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    group.bench_function("replay_16_brokers", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            for r in black_box(&brokers) {
                img.apply(r);
            }
            black_box(img)
        });
    });

    let single_topic = topic_record("t-0", 8);
    group.bench_function("single_topic", |b| {
        b.iter(|| {
            let mut img = MetadataImage::new(Uuid::nil());
            img.apply(black_box(&single_topic));
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// partitions_of / all_partitions — the per-topic and reconcile hot paths.
//
// Pins the scaling contract for many-topic clusters (the Gres per-tenant-topic
// model): per-topic lookups must stay flat as the topic count grows (the
// 1k-topic and 10k-topic `partitions_of` numbers must match — a regression to
// a full-map filter scan makes the 10k case ~10× slower), and one
// reconcile-shaped pass over the whole image must stay O(P), not O(topics×P).
// ---------------------------------------------------------------------------

fn image_with_single_partition_topics(n: usize) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    for i in 0..n {
        let name = format!("tenant-topic-{i:05}");
        img.apply(&topic_record(&name, 1));
        img.apply(&partition_record(&name, 0));
    }
    img
}

fn bench_partitions_by_topic(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/partitions_by_topic");

    for n in [1_000usize, 10_000] {
        let img = image_with_single_partition_topics(n);
        let probe = format!("tenant-topic-{:05}", n / 2);

        group.bench_function(format!("partitions_of/{n}_topics"), |b| {
            b.iter(|| img.partitions_of(black_box(&probe)).count());
        });
        group.bench_function(format!("topic_partition_count/{n}_topics"), |b| {
            b.iter(|| black_box(&img).topic_partition_count(black_box(&probe)));
        });
        // The replication supervisor's desired-follower-set shape: one pass
        // over every partition per metadata-image change.
        group.bench_function(format!("reconcile_scan/{n}_topics"), |b| {
            b.iter(|| {
                black_box(&img)
                    .all_partitions()
                    .filter(|p| p.replicas.contains(&NodeId(2)) && p.leader != NodeId(2))
                    .count()
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// matching_acls — the authorize() hot path.
// ---------------------------------------------------------------------------

fn bench_matching_acls(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_image/matching_acls");

    let img_small = image_with_acls(10, 4);
    let img_large = image_with_acls(1000, 50);

    group.bench_function("small_literal_hit", |b| {
        b.iter(|| {
            let count = img_small
                .matching_acls(ResourceType::Topic, black_box("topic-0005"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("small_no_match", |b| {
        b.iter(|| {
            let count = img_small
                .matching_acls(ResourceType::Topic, black_box("nope"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("large_literal_hit", |b| {
        b.iter(|| {
            let count = img_large
                .matching_acls(ResourceType::Topic, black_box("topic-0500"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("large_prefix_hit", |b| {
        b.iter(|| {
            let count = img_large
                .matching_acls(ResourceType::Topic, black_box("prefix-25-xyz"))
                .count();
            black_box(count)
        });
    });

    group.bench_function("all_acls_iter_large", |b| {
        b.iter(|| black_box(&img_large).all_acls().count());
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Serialization (wincode via serde-wincode).
// ---------------------------------------------------------------------------

fn bench_record_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_record/serde");

    let topic = topic_record("benchmarks", 32);
    let partition = partition_record("benchmarks", 5);
    let broker = broker_record(7);

    let topic_bytes = <SerdeCompat<MetadataRecord>>::serialize(&topic).unwrap();
    let partition_bytes = <SerdeCompat<MetadataRecord>>::serialize(&partition).unwrap();
    let broker_bytes = <SerdeCompat<MetadataRecord>>::serialize(&broker).unwrap();

    group.bench_function("serialize_topic", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&topic)).unwrap());
    });
    group.bench_function("serialize_partition", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&partition)).unwrap());
    });
    group.bench_function("serialize_broker", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::serialize(black_box(&broker)).unwrap());
    });
    group.bench_function("deserialize_topic", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&topic_bytes)).unwrap());
    });
    group.bench_function("deserialize_partition", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&partition_bytes)).unwrap());
    });
    group.bench_function("deserialize_broker", |b| {
        b.iter(|| <SerdeCompat<MetadataRecord>>::deserialize(black_box(&broker_bytes)).unwrap());
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_apply,
    bench_partitions_by_topic,
    bench_matching_acls,
    bench_record_serde,
);
criterion_main!(benches);
