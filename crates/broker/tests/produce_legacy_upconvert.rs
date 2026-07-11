//! Produce up-conversion from v0/v1 `MessageSet` to v2.
//!
//! The broker's Produce handler now accepts a `RecordsPayload::Legacy`
//! arm: incoming v0/v1 `MessageSet` bytes are passed through
//! `crabka_records_legacy::legacy_to_v2` and the resulting v2 batch is
//! handed to the existing log-append path. These tests exercise the
//! conversion without going through the wire protocol's version
//! negotiation — they construct a Legacy payload directly and assert
//! the broker stores the up-converted records, then fetches them back
//! in the modern v2 form.

mod support;

use bytes::{Bytes, BytesMut};
use crabka_ids::Offset;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::RecordsPayload,
};
use crabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set};

async fn create_topic(p: &support::InProcess, name: &str) {
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(resp.topics[0].error_code == 0);
}

async fn topic_id_for(p: &support::InProcess, name: &str) -> Uuid {
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
        .expect("topic in Metadata response")
}

/// Build a flat (uncompressed) v1 `MessageSet` carrying `values` as
/// successive records at offsets 0..N-1.
fn build_v1_message_set(values: &[&[u8]]) -> Bytes {
    let recs: Vec<ParsedRecord> = values
        .iter()
        .enumerate()
        .map(|(i, v)| ParsedRecord {
            offset: Offset(i64::try_from(i).expect("offset fits in i64")),
            timestamp: Some(1_700_000_000 + i64::try_from(i).expect("ts offset fits in i64")),
            key: None,
            value: Some(Bytes::copy_from_slice(v)),
        })
        .collect();
    let mut buf = BytesMut::new();
    encode_flat_message_set(recs, Magic::V1, &mut buf);
    buf.freeze()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_v1_message_set_is_upconverted_and_round_trips() {
    let p = support::start().await;
    create_topic(&p, "legacy").await;
    let topic_id = topic_id_for(&p, "legacy").await;

    let legacy_bytes = build_v1_message_set(&[b"alpha", b"beta", b"gamma"]);

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "legacy".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::Legacy(legacy_bytes)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    assert2::assert!(pr.error_code == 0);

    // Fetch the stored records back and verify they survived the
    // up-conversion. The wire response is v2, so the fetched batch
    // carries the same values.
    let fr = p
        .client
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "legacy".into(),
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
        .expect("Fetch");
    let part = &fr.responses[0].partitions[0];
    assert2::assert!(part.error_code == 0);
    let batch = part
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(<[_]>::first)
        .expect("Fetch returned a v2 batch");
    assert2::assert!(batch.records.len() == 3);
    let values: Vec<&[u8]> = batch
        .records
        .iter()
        .map(|r| r.value.as_deref().unwrap_or(&[]))
        .collect();
    assert2::assert!(values == vec![&b"alpha"[..], &b"beta"[..], &b"gamma"[..]]);

    p.broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_malformed_legacy_bytes_returns_invalid_record() {
    let p = support::start().await;
    create_topic(&p, "bad").await;
    let topic_id = topic_id_for(&p, "bad").await;

    // 100 bytes of garbage that look superficially like a legacy
    // MessageSet (byte 16 != 2 → routed to Legacy arm) but fail CRC
    // when parsed. The handler must surface INVALID_RECORD (87), not
    // panic or wedge.
    let mut garbage = vec![0u8; 100];
    garbage[16] = 0; // explicit: not v2
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: "bad".into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::Legacy(Bytes::from(garbage))),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = p.client.send(req).await.expect("Produce");
    let pr = &resp.responses[0].partition_responses[0];
    assert2::assert!(pr.error_code == 87);

    p.broker.shutdown().await;
}
