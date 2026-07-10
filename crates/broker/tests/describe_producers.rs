// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.
#![allow(clippy::pedantic)]

//! KIP-664 `DescribeProducers` admin RPC (api_key 61) — surfaces the
//! broker's in-memory producer-state snapshot.
//!
//! Tests:
//!   * empty partition returns an empty `active_producers` list
//!   * after an idempotent `Produce`, the producer's id / epoch /
//!     last_sequence / last_timestamp ride out on the response
//!   * multiple producers on the same partition all show
//!   * unknown topic / out-of-range partition → per-partition
//!     `UNKNOWN_TOPIC_OR_PARTITION (3)`

use assert2::{assert, check};
mod support;

use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        describe_producers_request::{DescribeProducersRequest, TopicRequest},
        init_producer_id_request::InitProducerIdRequest,
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
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

    let part = &resp.topics[0].partitions[0];
    check!(
        (
            resp.topics.len(),
            resp.topics[0].name.as_str(),
            resp.topics[0].partitions.len(),
            part.error_code,
            part.partition_index,
            part.active_producers.is_empty(),
        ) == (1, "fresh", 1, 0, 0, true),
        "fresh partition response mismatch: {part:?}"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn after_idempotent_produce_describe_returns_the_producer() {
    let p = support::start().await;
    create_topic(&p, "t", 1).await;
    let topic_id = topic_id_for(&p, "t").await;

    let (pid, epoch) = init_producer(&p).await;
    assert!(pid > 0);

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

    let part = &resp.topics[0].partitions[0];
    let producer = &part.active_producers[0];
    check!(
        (
            resp.topics.len(),
            resp.topics[0].partitions.len(),
            part.error_code,
            part.active_producers.len(),
            producer.producer_id,
            producer.producer_epoch,
            producer.last_sequence,
            producer.coordinator_epoch,
            producer.current_txn_start_offset,
        ) == (1, 1, 0, 1, pid, i32::from(epoch), 2, -1, -1),
        "unexpected tracked producer response: {resp:?}"
    );

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

    let partitions: Vec<_> = resp.topics[0]
        .partitions
        .iter()
        .map(|part| (part.error_code, part.active_producers.is_empty()))
        .collect();
    assert!(
        (resp.topics.len(), partitions) == (1, vec![(3, true), (3, true)]),
        "unknown topic must surface UNKNOWN_TOPIC_OR_PARTITION (3) per partition: {resp:?}"
    );

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
