//! End-to-end tests: produce a v2 batch through the modern Produce path, then
//! Fetch on a legacy version. The wire must carry a v0 or v1 `MessageSet` that
//! decodes back to the same records. The tests cover:
//!   - Fetch v3, which gives `Magic::V1` and keeps the KIP-32 timestamps.
//!   - Fetch v0, which gives `Magic::V0` and strips the per-message
//!     timestamps.
//!   - zstd-compressed batches, which the broker re-compresses as snappy.
//!   - control batches, which the broker drops from the down-converted
//!     response.

use assert2::{assert, check};
mod support;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_client_producer::{Producer, ProducerRecord};
use crabka_compression::CompressionType;
use crabka_protocol::{
    Decode, Encode,
    kafka_3_6_2::owned::fetch_response::FetchResponse as LegacyFetchResponse,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Attributes, Record, RecordBatch, RecordsPayload},
};
use crabka_records_legacy::decode_message_set;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// ── Wire helpers ──────────────────────────────────────────────────────────────

/// Sends a non-flexible Kafka request frame, v0 to v11, and returns the
/// response body bytes with the `correlation_id` already stripped. Neither
/// direction carries tagged-fields bytes, because v3 is non-flexible.
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
async fn topic_id_for(client: &crabka_client_core::Client, name: &str) -> WireUuid {
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

/// Creates a single-partition topic with the modern client and asserts that it
/// succeeds.
async fn create_topic(client: &crabka_client_core::Client, name: &str) {
    let cr = client
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
    assert!(
        cr.topics[0].error_code == 0,
        "CreateTopics error: {}",
        cr.topics[0].error_code
    );
}

/// Produces a single v2 batch to (topic, partition=0) with a modern flexible
/// `ProduceRequest`, version 9. Returns `Ok(())` on success.
async fn produce_batch(addr: std::net::SocketAddr, topic: &str, batch: RecordBatch) {
    const PRODUCE_VERSION: i16 = 9;
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![batch])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .expect("encode ProduceRequest v9");

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
    stream
        .read_exact(&mut resp)
        .await
        .expect("read produce resp");

    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _tagged = cur.get_u8(); // flexible response header tagged fields
    let produce_resp =
        ProduceResponse::decode(&mut cur, PRODUCE_VERSION).expect("decode ProduceResponse v9");
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part_resp.error_code == 0,
        "produce error: {}",
        part_resp.error_code
    );
}

/// Sends a Fetch request at the given legacy `version` for (topic,
/// partition=0) from offset 0, and returns the raw response body bytes with
/// the `correlation_id` stripped.
///
/// Encoding at a low version drops the fields that version does not have, for
/// example `max_bytes`, which is v3+. One struct therefore works for v0 to
/// v3.
async fn fetch_legacy_raw(addr: std::net::SocketAddr, topic: &str, version: i16) -> Vec<u8> {
    fetch_legacy_raw_at(addr, topic, version, 0).await
}

async fn fetch_legacy_raw_at(
    addr: std::net::SocketAddr,
    topic: &str,
    version: i16,
    fetch_offset: i64,
) -> Vec<u8> {
    use crabka_protocol::kafka_3_6_2::owned::fetch_request::{
        FetchPartition, FetchRequest, FetchTopic,
    };

    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, version)
        .expect("encode legacy FetchRequest");

    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect for legacy fetch");
    stream.set_nodelay(true).ok();
    round_trip_nonflexible(&mut stream, 1, version, 42, &body).await
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
    assert!(
        cr.topics[0].error_code == 0,
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
    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_basic", 3).await;

    // 4. Decode as LegacyFetchResponse (Fetch v3 is non-flexible).
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    assert!(
        fetch_resp.responses.len() == 1,
        "expected 1 topic in fetch response"
    );
    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );

    // 5. The records field should be a Legacy MessageSet.
    let records_payload = part.records.as_ref().expect("records field should be Some");
    let legacy_bytes = match records_payload {
        crabka_protocol::records::RecordsPayload::Legacy(b) => b.clone(),
        _ => {
            panic!("expected Legacy MessageSet in Fetch v3 response, got non-Legacy payload")
        }
    };

    // 6. Decode the MessageSet and verify key/value pairs.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");

    assert!(
        recs.len() == 2,
        "expected 2 records in MessageSet; got {}",
        recs.len()
    );
    for (i, key, value) in [
        (0usize, b"key0" as &[u8], b"val0" as &[u8]),
        (1, b"key1", b"val1"),
    ] {
        check!(
            recs[i].key.as_deref() == Some(key),
            "record {i} key mismatch"
        );
        check!(
            recs[i].value.as_deref() == Some(value),
            "record {i} value mismatch"
        );
    }

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
    assert!(
        cr.topics[0].error_code == 0,
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
    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_zstd", 3).await;

    // 4. Decode as LegacyFetchResponse.
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );

    // 5. Get the raw legacy bytes.
    let records_payload = part.records.as_ref().expect("records field should be Some");
    let legacy_bytes = match records_payload {
        crabka_protocol::records::RecordsPayload::Legacy(b) => b.clone(),
        _ => {
            panic!("expected Legacy MessageSet in Fetch v3 response, got non-Legacy payload")
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
    assert!(
        codec == 2,
        "expected snappy codec id (2) in wrapper message attributes, got {codec}"
    );

    // 7. Verify the records decode correctly by round-tripping through decode_message_set.
    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len())
        .expect("decode_message_set on snappy-recompressed payload");
    assert!(
        recs.len() == 50,
        "expected 50 records after snappy decompression"
    );
    check!(
        recs[0].key.as_deref() == Some(b"key-0000".as_ref()),
        "first record key mismatch"
    );
    check!(
        recs[49].key.as_deref() == Some(b"key-0049".as_ref()),
        "last record key mismatch"
    );

    p.broker.shutdown().await;
}

/// Fetch v0 maps to `Magic::V0`, which has no per-message timestamp.
///
/// The test produces a batch with timestamps through the modern path, then
/// fetches at v0 and confirms that the down-converted `MessageSet` strips
/// them. This drives the `request_version < 2` branch of
/// `down_convert_for_fetch` through the full Fetch handler, not through the
/// unit helper.
#[tokio::test]
async fn fetch_v0_downconverts_to_magic_v0_without_timestamps() {
    let p = support::start().await;
    create_topic(&p.client, "legacy_fetch_v0").await;

    // base_timestamp + per-record delta give a non-zero create-time that
    // a v1 MessageSet would carry but v0 must drop.
    let batch = RecordBatch {
        base_timestamp: 1_700_000_000,
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            timestamp_delta: 500,
            key: Some(Bytes::from_static(b"k")),
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    };
    let addr = p.broker.listen_addr();
    produce_batch(addr, "legacy_fetch_v0", batch).await;

    let resp_body = fetch_legacy_raw(addr, "legacy_fetch_v0", 0).await;
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 0).expect("decode LegacyFetchResponse v0");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );

    let legacy_bytes = match part.records.as_ref().expect("records field should be Some") {
        RecordsPayload::Legacy(b) => b.clone(),
        _ => {
            panic!("expected Legacy MessageSet in Fetch v0 response")
        }
    };

    // MessageSet layout: offset(8) + message_size(4) + crc(4) + magic(1).
    // The magic byte sits at index 16 and must be 0 for a v0 MessageSet.
    assert!(legacy_bytes.len() > 16, "legacy bytes too short");
    assert!(legacy_bytes[16] == 0, "expected v0 MessageSet magic byte 0");

    let mut ms_cur: &[u8] = &legacy_bytes;
    let recs = decode_message_set(&mut ms_cur, legacy_bytes.len()).expect("decode_message_set");
    assert!(recs.len() == 1, "expected 1 record");
    check!(recs[0].key.as_deref() == Some(b"k".as_ref()));
    check!(recs[0].value.as_deref() == Some(b"v".as_ref()));
    check!(
        recs[0].timestamp == None,
        "v0 MessageSet must carry no timestamp"
    );

    p.broker.shutdown().await;
}

/// Control batches, that is txn markers, have no representation in the v0 and
/// v1 `MessageSet` format, so a down-converted Fetch response must drop them.
///
/// The test commits a real transaction, fetches from its marker offset at v3,
/// and confirms that the partition comes back with no records and no error.
/// This drives the `Ok(None)` arm of the Fetch handler's down-conversion loop
/// without violating Kafka's rule that clients cannot produce control batches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_v3_drops_control_batch() {
    let p = support::start().await;
    create_topic(&p.client, "legacy_fetch_ctrl").await;

    let addr = p.broker.listen_addr();
    let producer = Producer::builder()
        .bootstrap(addr.to_string())
        .transactional_id("legacy-fetch-control-marker")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let transaction = producer.begin_transaction().await.unwrap();
    producer
        .send(ProducerRecord {
            topic: "legacy_fetch_ctrl".into(),
            value: Some(Bytes::from_static(b"data-before-marker")),
            ..Default::default()
        })
        .await
        .await
        .expect("delivery channel")
        .expect("transactional produce");
    transaction.commit().await.expect("commit transaction");
    p.broker
        .wait_until_local_log_end_offset("legacy_fetch_ctrl", 0, 2)
        .await;

    // Offset 0 is the data batch; offset 1 is the internally generated marker.
    let resp_body = fetch_legacy_raw_at(addr, "legacy_fetch_ctrl", 3, 1).await;
    let mut cur: &[u8] = &resp_body;
    let fetch_resp =
        LegacyFetchResponse::decode(&mut cur, 3).expect("decode LegacyFetchResponse v3");

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch partition error: {}",
        part.error_code
    );
    assert!(
        part.records.is_none(),
        "control batch must be dropped, leaving no records on the wire"
    );

    producer.close().await.unwrap();
    p.broker.shutdown().await;
}
