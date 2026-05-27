//! End-to-end: produce a v2 batch via the modern Produce path, then
//! Fetch v3 and expect a v0/v1 `MessageSet` on the wire that decodes
//! back to the same records. Includes a zstd-compressed batch case
//! that must come back as snappy.
//!
//! The control-record drop path is verified via unit tests in
//! `fetch_downconvert.rs` rather than end-to-end (the standard producer
//! path doesn't produce control batches).

#![allow(clippy::too_many_lines)]

mod support;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_compression::CompressionType;
use crabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::owned::produce_response::ProduceResponse;
use crabka_protocol::primitives::uuid::Uuid as WireUuid;
use crabka_protocol::records::{Attributes, Record, RecordBatch, RecordsPayload};
use crabka_protocol::{Decode, Encode};
use crabka_records_legacy::decode_message_set;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ── Wire helpers ──────────────────────────────────────────────────────────────

/// Send a non-flexible (v0-11) Kafka request frame and return the response body
/// bytes with `correlation_id` already stripped. No tagged-fields bytes for
/// either direction since v3 is non-flexible.
async fn round_trip_nonflexible(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    body: &[u8],
) -> Vec<u8> {
    let client_id = "legacy-fetch-test";
    let mut frame = BytesMut::with_capacity(12 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(client_id.len()).expect("fits in i16"));
    frame.put_slice(client_id.as_bytes());
    // non-flexible: NO trailing tagged-fields byte in request header
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await
        .expect("write frame length");
    stream.write_all(&frame).await.expect("write frame body");
    stream.flush().await.expect("flush");

    let resp_len = stream.read_u32().await.expect("read resp length");
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.expect("read resp body");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32(); // strip correlation_id
    // non-flexible response header: just the 4-byte correlation_id, nothing more
    cur.to_vec()
}

// ── Topic helpers ─────────────────────────────────────────────────────────────

#[allow(dead_code)]
async fn topic_id_for(
    client: &crabka_client_core::Client,
    name: &str,
) -> WireUuid {
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

/// Produce a single v2 batch to (topic, partition=0) via a modern flexible
/// `ProduceRequest` (version 9). Returns `Ok(())` on success.
async fn produce_batch(addr: std::net::SocketAddr, topic: &str, batch: RecordBatch) {
    const PRODUCE_VERSION: i16 = 9;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(batch)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION).expect("encode ProduceRequest v9");

    let mut stream = TcpStream::connect(addr).await.expect("connect for produce");
    stream.set_nodelay(true).ok();
    // ProduceRequest v9 is flexible (FLEXIBLE_MIN = 9).
    let client_id = "legacy-fetch-produce";
    let mut frame = BytesMut::new();
    frame.put_i16(0); // api_key = Produce
    frame.put_i16(PRODUCE_VERSION);
    frame.put_i32(99); // correlation_id
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id.as_bytes());
    frame.put_u8(0); // flexible request header: empty tagged fields
    frame.put_slice(&body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .expect("write produce frame length");
    stream.write_all(&frame).await.expect("write produce frame");
    stream.flush().await.expect("flush produce");

    let resp_len = stream.read_u32().await.expect("read produce resp len");
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.expect("read produce resp");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _tagged = cur.get_u8(); // flexible response header tagged fields
    let produce_resp =
        ProduceResponse::decode(&mut cur, PRODUCE_VERSION).expect("decode ProduceResponse v9");
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert_eq!(
        part_resp.error_code, 0,
        "produce error: {}",
        part_resp.error_code
    );
}

/// Send a Fetch v3 request for (topic, partition=0) from offset 0 and
/// return the raw response body bytes (`correlation_id` stripped).
async fn fetch_v3_raw(addr: std::net::SocketAddr, topic: &str) -> Vec<u8> {
    use crabka_protocol::kafka_3_6_2::owned::fetch_request::{
        FetchPartition, FetchRequest, FetchTopic,
    };
    const FETCH_VERSION: i16 = 3;

    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, FETCH_VERSION).expect("encode FetchRequest v3");

    let mut stream = TcpStream::connect(addr).await.expect("connect for fetch v3");
    stream.set_nodelay(true).ok();
    round_trip_nonflexible(&mut stream, 1, FETCH_VERSION, 42, &body).await
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn fetch_v3_downconverts_v2_batch_to_v0_messageset() {
    let p = support::start().await;

    // 1. Create topic.
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "legacy_fetch_basic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(
        cr.topics[0].error_code, 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );

    // 2. Produce 2 records via modern path (ProduceRequest v9).
    let batch = RecordBatch {
        records: vec![
            Record {
                offset_delta: 0,
                key: Some(Bytes::from_static(b"key0")),
                value: Some(Bytes::from_static(b"val0")),
                ..Default::default()
            },
            Record {
                offset_delta: 1,
                key: Some(Bytes::from_static(b"key1")),
                value: Some(Bytes::from_static(b"val1")),
                ..Default::default()
            },
        ],
        last_offset_delta: 1,
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_basic", batch).await;

    // 3. Fetch v3 via raw TCP.
    let resp_body = fetch_v3_raw(addr, "legacy_fetch_basic").await;

    // 4. Decode as LegacyFetchResponse (Fetch v3 is non-flexible).
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    assert_eq!(
        fetch_resp.responses.len(),
        1,
        "expected 1 topic in fetch response"
    );
    let part = &fetch_resp.responses[0].partitions[0];
    assert_eq!(
        part.error_code, 0,
        "fetch partition error: {}",
        part.error_code
    );

    // 5. The records field should be a Legacy MessageSet.
    let records_payload = part
        .records
        .as_ref()
        .expect("records field should be Some");
    let legacy_bytes = match records_payload {
        crabka_protocol::records::RecordsPayload::Legacy(b) => b.clone(),
        crabka_protocol::records::RecordsPayload::V2(_) => {
            panic!("expected Legacy MessageSet in Fetch v3 response, got V2 batch")
        }
    };

    // 6. Decode the MessageSet and verify key/value pairs.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs =
        decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");

    assert_eq!(recs.len(), 2, "expected 2 records in MessageSet; got {}", recs.len());
    assert_eq!(
        recs[0].key.as_deref(),
        Some(b"key0".as_ref()),
        "record 0 key mismatch"
    );
    assert_eq!(
        recs[0].value.as_deref(),
        Some(b"val0".as_ref()),
        "record 0 value mismatch"
    );
    assert_eq!(
        recs[1].key.as_deref(),
        Some(b"key1".as_ref()),
        "record 1 key mismatch"
    );
    assert_eq!(
        recs[1].value.as_deref(),
        Some(b"val1".as_ref()),
        "record 1 value mismatch"
    );

    p.broker.shutdown().await;
}

#[tokio::test]
async fn fetch_v3_recompresses_zstd_as_snappy() {
    let p = support::start().await;

    // 1. Create topic.
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "legacy_fetch_zstd".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert_eq!(
        cr.topics[0].error_code, 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );

    // 2. Produce 50 zstd-compressed records so compression is exercised.
    let records: Vec<Record> = (0i32..50)
        .map(|i| Record {
            offset_delta: i,
            timestamp_delta: i64::from(i) * 1000,
            key: Some(Bytes::from(format!("key-{i:04}"))),
            value: Some(Bytes::from(format!(
                "val-{i:04} hello world this is a repeated test value"
            ))),
            ..Default::default()
        })
        .collect();
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Zstd),
        last_offset_delta: 49,
        records,
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_zstd", batch).await;

    // 3. Fetch v3 via raw TCP.
    let resp_body = fetch_v3_raw(addr, "legacy_fetch_zstd").await;

    // 4. Decode as LegacyFetchResponse.
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    let part = &fetch_resp.responses[0].partitions[0];
    assert_eq!(
        part.error_code, 0,
        "fetch partition error: {}",
        part.error_code
    );

    // 5. Get the raw legacy bytes.
    let records_payload = part
        .records
        .as_ref()
        .expect("records field should be Some");
    let legacy_bytes = match records_payload {
        crabka_protocol::records::RecordsPayload::Legacy(b) => b.clone(),
        crabka_protocol::records::RecordsPayload::V2(_) => {
            panic!("expected Legacy MessageSet in Fetch v3 response, got V2 batch")
        }
    };

    // 6. The outer wrapper message's attributes byte should carry snappy (2).
    // MessageSet format: offset(8) + message_size(4) + crc(4) + magic(1) + attributes(1)
    // So attributes byte is at index 17.
    assert!(
        legacy_bytes.len() > 17,
        "expected non-empty legacy bytes, got len={}",
        legacy_bytes.len()
    );
    let codec = legacy_bytes[17] & 0x07;
    assert_eq!(
        codec, 2,
        "expected snappy codec id (2) in wrapper message attributes, got {codec}"
    );

    // 7. Verify the records decode correctly by round-tripping through decode_message_set.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len())
        .expect("decode_message_set on snappy-recompressed payload");
    assert_eq!(recs.len(), 50, "expected 50 records after snappy decompression");
    assert_eq!(
        recs[0].key.as_deref(),
        Some(b"key-0000".as_ref()),
        "first record key mismatch"
    );
    assert_eq!(
        recs[49].key.as_deref(),
        Some(b"key-0049".as_ref()),
        "last record key mismatch"
    );

    p.broker.shutdown().await;
}
