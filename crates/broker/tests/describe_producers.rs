// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-664 `DescribeProducers` admin RPC (`api_key` 61). It reports the
//! broker's in-memory producer-state snapshot.
//!
//! Tests:
//!   * an empty partition returns an empty `active_producers` list
//!   * after an idempotent `Produce`, the response carries the producer's id,
//!     epoch, `last_sequence`, and `last_timestamp`
//!   * several producers on the same partition all appear
//!   * an unknown topic, or a partition out of range, gives
//!     `UNKNOWN_TOPIC_OR_PARTITION (3)` for that partition

use assert2::{assert, check};
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        describe_producers_request::{DescribeProducersRequest, TopicRequest},
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        write_txn_markers_request::{
            WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
        },
    },
    records::{Attributes, Record, RecordBatch},
};

async fn topic_id_for(
    p: &support::InProcess,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
    let resp = p
        .client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

async fn create_topic(p: &support::InProcess, name: &str, partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "{name} create: {resp:?}");
}

async fn init_producer(p: &support::InProcess) -> (i64, i16) {
    let init = p
        .client
        .send(InitProducerIdRequest::default())
        .await
        .expect("InitProducerId");
    (init.producer_id, init.producer_epoch)
}

fn batch(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    let n = i32::try_from(values.len()).expect("values.len fits i32");
    let records = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            offset_delta: i32::try_from(i).expect("index fits i32"),
            value: Some(bytes::Bytes::from(v.to_string())),
            ..Default::default()
        })
        .collect();
    RecordBatch {
        producer_id: pid,
        producer_epoch: epoch,
        base_sequence: base_seq,
        last_offset_delta: n - 1,
        max_timestamp: i64::from(n),
        records,
        ..Default::default()
    }
}

fn transactional_batch(pid: i64, epoch: i16, base_seq: i32, values: &[&str]) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(true),
        ..batch(pid, epoch, base_seq, values)
    }
}

#[tokio::test]
async fn empty_partition_returns_no_active_producers() {
    let p = support::start().await;
    create_topic(&p, "fresh", 1).await;

    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "fresh".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name == "fresh");
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    check!(
        part.error_code == 0,
        "fresh partition must succeed: {part:?}"
    );
    check!(part.partition_index == 0);
    check!(
        part.active_producers.is_empty(),
        "no produce has happened — list must be empty: {:?}",
        part.active_producers,
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn after_idempotent_produce_describe_returns_the_producer() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let topic_id = topic_id_for(&p, "t").await;

    let (pid, epoch) = init_producer(&p).await;
    assert!(pid >= 0);

    let pr = p
        .client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "t".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch(pid, epoch, 0, &["a", "b", "c"]).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    assert!(pr.responses[0].partition_responses[0].error_code == 0);

    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "t".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    assert!(part.error_code == 0, "{part:?}");
    assert!(
        part.active_producers.len() == 1,
        "expected exactly one tracked producer, got {:?}",
        part.active_producers
    );
    let producer = &part.active_producers[0];
    check!(producer.producer_id == pid);
    check!(producer.producer_epoch == i32::from(epoch));
    // base_seq=0, last_offset_delta=n-1=2 → last_sequence = 2.
    check!(producer.last_sequence == 2);
    // An idempotent (non-transactional) producer has no transaction fields.
    check!(producer.coordinator_epoch == -1);
    check!(producer.current_txn_start_offset == -1);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn transactional_fields_follow_open_and_completed_transactions() {
    let p = support::start().await;
    create_topic(&p, "transactions", 1).await;
    let topic_id = topic_id_for(&p, "transactions").await;
    let (pid, epoch) = init_producer(&p).await;

    let produce_response = p
        .client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "transactions".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(transactional_batch(pid, epoch, 0, &["first"]).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers during first transaction");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == 0);
    check!(producer_row.coordinator_epoch == -1);

    let marker = p
        .client
        .send(WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: pid,
                producer_epoch: epoch,
                transaction_result: true,
                coordinator_epoch: 17,
                transaction_version: 1,
                topics: vec![WritableTxnMarkerTopic {
                    name: "transactions".into(),
                    partition_indexes: vec![0],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("WriteTxnMarkers");
    assert!(marker.markers[0].topics[0].partitions[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers after marker");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == -1);
    check!(producer_row.coordinator_epoch == 17);

    let produce_response = p
        .client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "transactions".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(transactional_batch(pid, epoch, 1, &["second"]).into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("second transactional Produce");
    assert!(produce_response.responses[0].partition_responses[0].error_code == 0);

    let describe = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "transactions".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers during second transaction");
    let producer_row = &describe.topics[0].partitions[0].active_producers[0];
    check!(producer_row.current_txn_start_offset == 2);
    check!(producer_row.coordinator_epoch == 17);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn multiple_producers_on_same_partition_all_surfaced() {
    let p = support::start().await;
    create_topic(&p, "shared", 1).await;
    let topic_id = topic_id_for(&p, "shared").await;

    let (pid_a, epoch_a) = init_producer(&p).await;
    let (pid_b, epoch_b) = init_producer(&p).await;
    assert!(
        pid_a != pid_b,
        "InitProducerId must return distinct ids on back-to-back calls"
    );

    for (pid, epoch) in [(pid_a, epoch_a), (pid_b, epoch_b)] {
        let pr = p
            .client
            .send(ProduceRequest {
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: "shared".into(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(batch(pid, epoch, 0, &["x"]).into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        assert!(pr.responses[0].partition_responses[0].error_code == 0);
    }

    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "shared".into(),
                partition_indexes: vec![0],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    let producers = &resp.topics[0].partitions[0].active_producers;
    assert!(
        producers.len() == 2,
        "expected both producers: {producers:?}"
    );
    let seen: std::collections::HashSet<i64> = producers.iter().map(|p| p.producer_id).collect();
    assert!(seen.contains(&pid_a) && seen.contains(&pid_b));

    p.broker.shutdown().await;
}

#[tokio::test]
async fn unknown_topic_returns_unknown_topic_or_partition() {
    let p = support::start().await;

    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "ghost".into(),
                partition_indexes: vec![0, 1],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].partitions.len() == 2);
    for part in &resp.topics[0].partitions {
        assert!(
            part.error_code == 3,
            "unknown topic must surface UNKNOWN_TOPIC_OR_PARTITION (3) per partition, got {part:?}"
        );
        assert!(part.active_producers.is_empty());
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn out_of_range_partition_returns_unknown_topic_or_partition() {
    let p = support::start().await;
    create_topic(&p, "small", 1).await;

    // Partition 5 doesn't exist (topic was created with 1 partition).
    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "small".into(),
                partition_indexes: vec![0, 5],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    assert!(resp.topics[0].partitions.len() == 2);
    // Partition 0 exists → error_code 0.
    let p0 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 0)
        .expect("p0");
    assert!(p0.error_code == 0, "{p0:?}");
    // Partition 5 doesn't → UNKNOWN_TOPIC_OR_PARTITION.
    let p5 = resp.topics[0]
        .partitions
        .iter()
        .find(|p| p.partition_index == 5)
        .expect("p5");
    assert!(p5.error_code == 3, "{p5:?}");

    p.broker.shutdown().await;
}

#[tokio::test]
async fn metadata_known_partition_not_hosted_locally_returns_not_leader() {
    let p = support::start().await;
    create_topic(&p, "remote", 2).await;
    assert!(p.broker.partition_exists_for_test("remote", 1));
    p.broker.remove_local_partition_for_test("remote", 1);

    let resp = p
        .client
        .send(DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: "remote".into(),
                partition_indexes: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DescribeProducers");

    let partition = &resp.topics[0].partitions[0];
    assert!(partition.partition_index == 1);
    check!(partition.error_code == 6, "expected NOT_LEADER_OR_FOLLOWER");
    check!(partition.active_producers.is_empty());

    p.broker.shutdown().await;
}
