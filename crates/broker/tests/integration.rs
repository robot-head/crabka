//! Multi-RPC sequences against an in-process broker, driven through
//! `crabka-client-core`. These run on every push (no Docker required).

mod support;

use bytes::Bytes;
use crabka_protocol::owned::api_versions_request::ApiVersionsRequest;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::list_offsets_request::{
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic,
};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::records::{Record, RecordBatch};

/// Build a `RecordBatch` with one entry per provided value. Codegen's
/// `PartitionProduceData.records` is `Option<RecordBatch>` (NOT raw bytes
/// — that was the plan's first draft), so callers pass the batch by value.
fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(i).expect("test fixture small enough for i32"),
            value: Some(Bytes::from(v.to_string())),
            ..Default::default()
        });
    }
    batch
}

/// Round-trip a Metadata request to learn the topic's assigned UUID.
/// Produce / Fetch at v ≥ 13 carry only `topic_id` on the wire, so the
/// caller must plumb the real UUID through.
async fn topic_id_for(
    client: &crabka_client_core::Client,
    name: &str,
) -> crabka_protocol::primitives::uuid::Uuid {
    let resp = client
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
async fn end_to_end_create_produce_fetch_delete() {
    let p = support::start().await;

    // 1. ApiVersions.
    let v = p
        .client
        .send(ApiVersionsRequest {
            client_software_name: "crabka".into(),
            client_software_version: "0.0.0".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(v.error_code, 0);

    // 2. CreateTopics.
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "e2e".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(cr.topics[0].error_code, 0);

    // 3. Metadata — confirm topic is visible and grab its UUID.
    let meta = p.client.send(MetadataRequest::default()).await.unwrap();
    assert!(meta.topics.iter().any(|t| t.name.as_deref() == Some("e2e")));
    let topic_id = topic_id_for(&p.client, "e2e").await;

    // 4. Produce 3 records.
    let pr = p
        .client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "e2e".into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(record_batch_with_values(&["a", "b", "c"])),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(pr.responses[0].partition_responses[0].error_code, 0);

    // 5. ListOffsets — latest after producing 3 records is 3.
    let lo = p
        .client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: "e2e".into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1, // latest
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(lo.topics[0].partitions[0].error_code, 0);
    assert_eq!(lo.topics[0].partitions[0].offset, 3);

    // 6. Fetch and confirm 3 records are returned.
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "e2e".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let part = &fr.responses[0].partitions[0];
    assert_eq!(part.error_code, 0);
    let batch = part
        .records
        .as_ref()
        .expect("records present after produce");
    assert_eq!(batch.records.len(), 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn second_open_recovers_partitions_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    {
        let config = crabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = crabka_broker::Broker::start(config).await.unwrap();
        let bootstrap = handle.listen_addr().to_string();
        let client = crabka_client_core::Client::builder()
            .bootstrap(&bootstrap)
            .client_id("recovery-test")
            .build()
            .await
            .unwrap();
        let cr = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: "persisted".into(),
                    num_partitions: 2,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(cr.topics[0].error_code, 0);
        handle.shutdown().await;
    }
    // Reopen on the same log_dir. Must use Rejoin because the raft log
    // already exists from the first run; Bootstrap would be rejected.
    let mut config = crabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    config.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    let handle = crabka_broker::Broker::start(config).await.unwrap();
    let bootstrap = handle.listen_addr().to_string();
    let client = crabka_client_core::Client::builder()
        .bootstrap(&bootstrap)
        .client_id("recovery-test")
        .build()
        .await
        .unwrap();
    let meta = client.send(MetadataRequest::default()).await.unwrap();
    let t = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("persisted"))
        .expect("recovered topic visible in metadata");
    assert_eq!(t.partitions.len(), 2);
    handle.shutdown().await;
}
