//! Increment D end-to-end validation: produce a large (>64 KiB) records run,
//! then consume it over the real loopback TCP socket and assert the record
//! values round-trip **byte-for-byte**.
//!
//! On Linux this fetch crosses the 32 KiB `sendfile` threshold on a plaintext
//! `TcpStream`, so the records region is transmitted via the kernel
//! `sendfile(2)` zero-copy path. If sendfile transmitted the wrong file range
//! (or a partial-write loop bug dropped/duplicated bytes), the consumer's CRC
//! check and the value comparison below would fail. On Windows / TLS the same
//! test exercises the portable vectored (Increment C) fallback — the wire bytes
//! are identical either way, so the assertions hold on every platform.

use assert2::assert;
mod support;

use bytes::Bytes;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

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
    assert!(resp.topics[0].error_code == 0);
}

async fn topic_id_for(p: &support::InProcess, name: &str) -> WireUuid {
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

/// Build `n` records whose values are distinct, sizeable, and content-addressed
/// by index so any byte misplacement is detectable.
fn large_records(n: i32, value_len: usize) -> (RecordBatch, Vec<Bytes>) {
    let mut batch = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    let mut expected = Vec::with_capacity(usize::try_from(n.max(0)).unwrap_or(0));
    for i in 0..n {
        // Fill each value with a per-record byte pattern so a swapped/duplicated
        // range is caught, not just a length mismatch.
        let mut v = vec![0u8; value_len];
        let tag = (u8::try_from(i & 0xff).unwrap_or(0))
            .wrapping_mul(31)
            .wrapping_add(7);
        for (j, b) in v.iter_mut().enumerate() {
            *b = tag ^ u8::try_from(j & 0xff).unwrap_or(0);
        }
        let value = Bytes::from(v);
        expected.push(value.clone());
        batch.records.push(Record {
            offset_delta: i,
            key: Some(Bytes::from(format!("key-{i}"))),
            value: Some(value),
            ..Default::default()
        });
    }
    (batch, expected)
}

#[tokio::test]
async fn large_message_fetch_round_trips_byte_exact() {
    let p = support::start().await;
    create_topic(&p, "big").await;
    let tid = topic_id_for(&p, "big").await;

    // 64 records × 2 KiB ≈ 128 KiB of records — well over the 32 KiB sendfile
    // threshold, so the Linux plaintext fetch goes zero-copy.
    let (batch, expected) = large_records(64, 2 * 1024);

    let prod = p
        .client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: "big".into(),
                topic_id: tid,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Produce");
    assert!(prod.responses[0].partition_responses[0].error_code == 0);

    // Fetch with a generous byte budget so the whole run comes back in one go.
    let r = p
        .client
        .send(FetchRequest {
            max_wait_ms: 200,
            min_bytes: 1,
            max_bytes: 8 * 1024 * 1024,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopic {
                topic: "big".into(),
                topic_id: tid,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 8 * 1024 * 1024,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Fetch");

    assert!((r.error_code, r.responses.len()) == (0, 1));
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(crabka_protocol::records::RecordsPayload::as_v2)
        .expect("v2 records decoded from the fetch response");

    // Flatten all returned records and compare their values byte-for-byte to
    // what we produced. Any sendfile range/partial-write bug corrupts this.
    let got_values: Vec<&Bytes> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|rec| rec.value.as_ref())
        .collect();
    assert!(
        got_values.len() == expected.len(),
        "expected {} records, got {}",
        expected.len(),
        got_values.len()
    );
    for (i, (got, want)) in got_values.iter().zip(expected.iter()).enumerate() {
        assert!(
            got.as_ref() == &want[..],
            "record {i} value mismatch (sendfile byte corruption?)"
        );
    }

    p.broker.shutdown().await;
}
