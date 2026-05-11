mod support;

use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::delete_topics_request::{DeleteTopicState, DeleteTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::find_coordinator_request::FindCoordinatorRequest;
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use crabka_protocol::owned::metadata_request::MetadataRequest;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

/// Build a single `RecordBatch` carrying `n` empty records with sequential
/// offset deltas.
fn one_record_batch(n: i32) -> RecordBatch {
    let mut b = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    for i in 0..n {
        b.records.push(Record {
            offset_delta: i,
            ..Default::default()
        });
    }
    b
}

async fn create_topic(p: &support::InProcess, name: &str, num_partitions: i32) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 0, "CreateTopics for {name}");
}

/// Resolve a topic's UUID via a Metadata round trip. Produce v ≥ 13 sends
/// only `topic_id` on the wire, so tests need this to drive the broker
/// with a non-zero UUID.
async fn topic_id_for(
    p: &support::InProcess,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
    use crabka_protocol::owned::metadata_request::MetadataRequestTopic;
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
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

#[tokio::test]
async fn api_versions_round_trip() {
    let p = support::start().await;
    let resp = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka-test".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .expect("ApiVersions");
    assert_eq!(resp.error_code, 0);
    // Must include ApiVersions itself.
    assert!(resp.api_keys.iter().any(|k| k.api_key == 18));
    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_then_delete_topic_round_trip() {
    let p = support::start().await;

    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "alpha".into(),
            num_partitions: 2,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert_eq!(resp.topics.len(), 1);
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].num_partitions, 2);

    let delete = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("alpha".into()),
            ..Default::default()
        }],
        topic_names: vec!["alpha".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let dresp = p.client.send(delete).await.expect("DeleteTopics");
    assert_eq!(dresp.responses.len(), 1);
    assert_eq!(dresp.responses[0].error_code, 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn create_topic_with_zero_partitions_errors() {
    let p = support::start().await;
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "zero".into(),
            num_partitions: 0,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = p.client.send(create).await.expect("CreateTopics");
    assert_eq!(resp.topics[0].error_code, 37); // INVALID_PARTITIONS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn duplicate_create_returns_topic_already_exists() {
    let p = support::start().await;
    let req = || CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "dup".into(),
            num_partitions: 1,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let r1 = p.client.send(req()).await.expect("CreateTopics 1");
    assert_eq!(r1.topics[0].error_code, 0);
    let r2 = p.client.send(req()).await.expect("CreateTopics 2");
    assert_eq!(r2.topics[0].error_code, 36); // TOPIC_ALREADY_EXISTS
    p.broker.shutdown().await;
}

#[tokio::test]
async fn metadata_returns_this_broker_and_listed_topics() {
    let p = support::start().await;
    // Create a topic first.
    let create = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: "beta".into(),
            num_partitions: 3,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let _ = p.client.send(create).await.unwrap();

    let resp = p
        .client
        .send(MetadataRequest::default())
        .await
        .expect("Metadata");
    assert_eq!(resp.brokers.len(), 1);
    let topic = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("beta"))
        .unwrap();
    assert_eq!(topic.partitions.len(), 3);
    for (i, part) in topic.partitions.iter().enumerate() {
        assert_eq!(part.error_code, 0);
        assert_eq!(part.partition_index, i32::try_from(i).unwrap());
        assert_eq!(part.leader_id, 1);
    }
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_assigns_base_offsets() {
    let p = support::start().await;
    create_topic(&p, "prod", 1).await;
    let topic_id = topic_id_for(&p, "prod").await;

    // First produce: 3 records → base 0.
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce 1");
    assert_eq!(resp.responses.len(), 1);
    assert_eq!(resp.responses[0].partition_responses.len(), 1);
    assert_eq!(resp.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(resp.responses[0].partition_responses[0].base_offset, 0);

    // Second produce: 2 records → base 3.
    let req2 = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "prod".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(2)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp2 = p.client.send(req2).await.expect("Produce 2");
    assert_eq!(resp2.responses[0].partition_responses[0].error_code, 0);
    assert_eq!(resp2.responses[0].partition_responses[0].base_offset, 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_to_unknown_topic_returns_3() {
    let p = support::start().await;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "nope".into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(1)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce unknown");
    assert_eq!(resp.responses[0].partition_responses[0].error_code, 3);
    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_then_fetch_round_trip() {
    let p = support::start().await;
    create_topic(&p, "round", 1).await;
    let topic_id = topic_id_for(&p, "round").await;

    let prod = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "round".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch(3)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let presp = p.client.send(prod).await.expect("Produce");
    assert_eq!(presp.responses[0].partition_responses[0].error_code, 0);

    let fetch = FetchRequest {
        max_wait_ms: 100,
        min_bytes: 1,
        topics: vec![FetchTopic {
            topic: "round".into(),
            topic_id,
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let fresp = p.client.send(fetch).await.expect("Fetch");
    assert_eq!(fresp.responses.len(), 1);
    let part = &fresp.responses[0].partitions[0];
    assert_eq!(part.error_code, 0);
    let batch = part
        .records
        .as_ref()
        .expect("records must be present after produce");
    assert_eq!(batch.records.len(), 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn list_offsets_earliest_and_latest() {
    let p = support::start().await;
    create_topic(&p, "empty", 1).await;

    let mk = |ts: i64| ListOffsetsRequest {
        replica_id: -1,
        topics: vec![ListOffsetsTopic {
            name: "empty".into(),
            partitions: vec![ListOffsetsPartition {
                partition_index: 0,
                timestamp: ts,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let earliest = p.client.send(mk(-2)).await.expect("ListOffsets earliest");
    let latest = p.client.send(mk(-1)).await.expect("ListOffsets latest");
    assert_eq!(earliest.topics[0].partitions[0].error_code, 0);
    assert_eq!(latest.topics[0].partitions[0].error_code, 0);
    assert_eq!(earliest.topics[0].partitions[0].offset, 0);
    assert_eq!(latest.topics[0].partitions[0].offset, 0);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn find_coordinator_always_unavailable() {
    let p = support::start().await;
    let req = FindCoordinatorRequest {
        key: "legacy".into(),
        coordinator_keys: vec!["grp-a".into(), "grp-b".into()],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("FindCoordinator");
    // Negotiated version is v6 (max in both client and broker): the
    // top-level error_code field is not on the wire at v ≥ 4 — only the
    // per-coordinator array is. Assert on the per-key field.
    assert_eq!(resp.coordinators.len(), 2);
    for c in &resp.coordinators {
        assert_eq!(c.error_code, 15);
    }
    p.broker.shutdown().await;
}
