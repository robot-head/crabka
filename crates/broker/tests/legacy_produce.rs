//! End-to-end: a hand-crafted Produce v0 request goes through up-conversion
//! and lands on disk as a v2 `RecordBatch`. Fetching back via the typed client
//! (whatever version it negotiates) should return the key/value we sent.

#![allow(clippy::too_many_lines)]

use assert2::assert;
mod support;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_ids::Offset;
use crabka_protocol::{
    Decode, Encode,
    kafka_3_6_2::owned::{
        produce_request::{
            PartitionProduceData as LegacyPartitionProduceData,
            ProduceRequest as LegacyProduceRequest, TopicProduceData as LegacyTopicProduceData,
        },
        produce_response::ProduceResponse as LegacyProduceResponse,
    },
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    records::RecordsPayload,
};
use crabka_records_legacy::{Magic, ParsedRecord, encode_flat_message_set};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

// ── Raw TCP wire helpers ──────────────────────────────────────────────────────

/// Build a v0 `MessageSet` containing one record per `(key, value)` pair.
/// Uses `crabka_records_legacy::encode_flat_message_set` so we don't
/// hand-roll CRC logic.
fn build_v0_messageset(pairs: &[(&str, &str)]) -> Bytes {
    let records: Vec<ParsedRecord> = pairs
        .iter()
        .enumerate()
        .map(|(i, (k, v))| ParsedRecord {
            offset: Offset(i64::try_from(i).expect("index fits in i64")),
            timestamp: None, // v0 has no timestamps
            key: Some(Bytes::copy_from_slice(k.as_bytes())),
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
        })
        .collect();
    let mut buf = BytesMut::new();
    encode_flat_message_set(records, Magic::V0, &mut buf);
    buf.freeze()
}

/// Send a single length-prefixed request frame and return the response body
/// bytes (`correlation_id` and any response-header bytes already stripped).
///
/// For Produce v0 the request is non-flexible: the request header has no
/// trailing tagged-fields byte, and the response header is v0 (4-byte
/// `correlation_id` only, no tagged-fields byte).
async fn round_trip_v0(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    body: &[u8],
) -> Vec<u8> {
    let client_id = "legacy-produce-test";
    let mut frame = BytesMut::with_capacity(12 + client_id.len() + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(client_id.len()).expect("fits in i16"));
    frame.put_slice(client_id.as_bytes());
    // v0: non-flexible, so NO trailing tagged-fields byte in request header
    frame.put_slice(body);

    // Length-prefix framing (4-byte big-endian).
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
    let _corr = cur.get_i32(); // strip `correlation_id`
    // v0 response header: no tagged-fields byte — nothing more to strip
    cur.to_vec()
}

// ── Topic helpers ─────────────────────────────────────────────────────────────

/// Send a Metadata request to learn the topic's UUID (needed for Fetch on
/// high versions that use `topic_id`).
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

// ── Test ──────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn produce_v0_upconverts_and_is_readable_via_fetch() {
    let p = support::start().await;

    // 1. Create topic "legacy_v0" with 1 partition using the typed client.
    let cr = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "legacy_v0".into(),
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

    // 2. Open a raw TCP connection to the broker to send the v0 Produce.
    let addr = p.broker.listen_addr();
    let mut stream = TcpStream::connect(addr)
        .await
        .expect("connect for v0 Produce");
    stream.set_nodelay(true).ok();

    // 3. Build the v0 MessageSet with key="k", value="v".
    let messageset_bytes = build_v0_messageset(&[("k", "v")]);

    // 4. Build the kafka_3_6_2 ProduceRequest at v0 and encode it.
    let legacy_req = LegacyProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![LegacyTopicProduceData {
            name: "legacy_v0".to_string(),
            partition_data: vec![LegacyPartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::Legacy(messageset_bytes)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    legacy_req
        .encode(&mut body, 0)
        .expect("encode ProduceRequest v0");

    // 5. Round-trip over raw TCP.
    let resp_body = round_trip_v0(
        &mut stream,
        0, // api_key = Produce
        0, // api_version = 0
        1, // correlation_id
        &body,
    )
    .await;

    // 6. Decode the ProduceResponse v0.
    let mut cur: &[u8] = &resp_body;
    let produce_resp =
        LegacyProduceResponse::decode(&mut cur, 0).expect("decode ProduceResponse v0");
    assert!(
        produce_resp.responses.len() == 1,
        "expected 1 topic in response"
    );
    let part_resp = &produce_resp.responses[0].partition_responses[0];
    assert!(
        part_resp.error_code == 0,
        "produce v0 error_code: {}",
        part_resp.error_code
    );

    // 7. Fetch via the typed client (which negotiates the highest supported
    //    Fetch version). The data stored on disk is a v2 RecordBatch.
    let topic_id = topic_id_for(&p.client, "legacy_v0").await;
    let fetch_resp = p
        .client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "legacy_v0".into(),
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

    let part = &fetch_resp.responses[0].partitions[0];
    assert!(
        part.error_code == 0,
        "fetch error_code: {}",
        part.error_code
    );

    let batch = part
        .records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(<[_]>::first)
        .expect("v2 RecordBatch present after up-converted produce");

    assert!(
        batch.records.len() == 1,
        "expected 1 record in fetched batch"
    );
    let rec = &batch.records[0];
    assert!(
        (rec.key.as_deref(), rec.value.as_deref()) == (Some(b"k".as_ref()), Some(b"v".as_ref())),
        "decoded record key/value mismatch: key={:?} value={:?}",
        rec.key,
        rec.value,
    );

    p.broker.shutdown().await;
}
