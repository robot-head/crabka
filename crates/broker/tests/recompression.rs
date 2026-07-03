// rustc 1.95 clippy ICEs on `clippy::pedantic` in test files (same
// upstream bug as `tests/compaction.rs` / `tests/mtls.rs`).
#![allow(clippy::pedantic)]

//! Broker-side recompression.
//!
//! Produces a gzip-compressed batch to a topic configured with
//! `compression.type=lz4`, fetches it back, and asserts the served
//! batch's attributes report `lz4` — proving the broker re-encoded
//! before writing. Also verifies the record payload survives the
//! round-trip intact (gzip-decompressed on the broker, lz4-compressed,
//! lz4-decompressed by the client).
//!
//! Gated to non-Windows to match the multi-broker test convention used
//! by the other replication and compaction tests.

use assert2::{assert, check};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_compression::CompressionType;
use crabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use crabka_protocol::owned::create_topics_response::CreateTopicsResponse;
use crabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use crabka_protocol::owned::fetch_response::FetchResponse;
use crabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};
use crabka_protocol::owned::metadata_response::MetadataResponse;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::owned::produce_response::ProduceResponse;
use crabka_protocol::primitives::uuid::Uuid;
use crabka_protocol::records::{Attributes, Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENT_ID: &str = "crabka-recompression-test";

async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    flexible: bool,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(corr_id);
    frame.put_i16(i16::try_from(CLIENT_ID.len()).unwrap());
    frame.put_slice(CLIENT_ID.as_bytes());
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    if flexible && api_key != 18 {
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

async fn start_broker() -> (BrokerHandle, SocketAddr) {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    std::mem::forget(log_dir);
    (handle, addr)
}

async fn create_topic_with_compression(addr: SocketAddr, topic: &str, codec: &str) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.into(),
            num_partitions: 1,
            replication_factor: 1,
            configs: vec![CreatableTopicConfig {
                name: "compression.type".into(),
                value: Some(codec.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    const VERSION: i16 = 7;
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 19, VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = CreateTopicsResponse::decode(&mut cur, VERSION).unwrap();
    assert!(
        r.topics[0].error_code == 0,
        "CreateTopics must succeed for compression.type={codec}: {:?}",
        r.topics[0]
    );
}

async fn get_topic_id(addr: SocketAddr, topic: &str) -> Uuid {
    let req = MetadataRequest {
        topics: Some(vec![MetadataRequestTopic {
            name: Some(topic.into()),
            ..Default::default()
        }]),
        ..Default::default()
    };
    const VERSION: i16 = 12;
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 3, VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = MetadataResponse::decode(&mut cur, VERSION).unwrap();
    r.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .map(|t| t.topic_id)
        .expect("topic in Metadata response")
}

async fn produce_gzip(addr: SocketAddr, topic: &str, topic_id: Uuid, value: &[u8]) {
    let batch = RecordBatch {
        attributes: Attributes::default().with_compression(CompressionType::Gzip),
        records: vec![Record {
            offset_delta: 0,
            value: Some(Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    };
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    const VERSION: i16 = 9;
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 0, VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = ProduceResponse::decode(&mut cur, VERSION).unwrap();
    let part = &r.responses[0].partition_responses[0];
    assert!(part.error_code == 0, "Produce must succeed: {part:?}");
}

async fn fetch_first_batch(addr: SocketAddr, topic: &str, topic_id: Uuid) -> RecordBatch {
    let req = FetchRequest {
        replica_id: -1,
        max_wait_ms: 500,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.into(),
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
    };
    const VERSION: i16 = 12;
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp = round_trip(&mut stream, 1, VERSION, 1, true, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = FetchResponse::decode(&mut cur, VERSION).unwrap();
    let part = &r.responses[0].partitions[0];
    assert!(part.error_code == 0, "Fetch error: {}", part.error_code);
    part.records
        .as_ref()
        .and_then(|p| p.as_v2())
        .and_then(|batches| batches.first().cloned())
        .expect("Fetch returned at least one v2 batch")
}

async fn wait_for_compression(
    handle: &BrokerHandle,
    topic: &str,
    expected: Option<CompressionType>,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(cfg) = handle.partition_log_config_for_test(topic, 0)
            && cfg.compression_type == expected
        {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "compression_type={expected:?} never propagated to partition LogConfig within 10s"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_lz4_recompresses_producer_gzip_batch() {
    const TOPIC: &str = "recompress-target";
    let (handle, addr) = start_broker().await;

    create_topic_with_compression(addr, TOPIC, "lz4").await;
    // The CreateTopics path → metadata → replicator-supervisor
    // reconcile loop pushes the LogConfig override into the partition
    // writer's log. Wait until the writer has applied it; otherwise
    // the produce can land before the override and the broker passes
    // gzip through unmodified.
    wait_for_compression(&handle, TOPIC, Some(CompressionType::Lz4)).await;

    let topic_id = get_topic_id(addr, TOPIC).await;
    let payload = b"broker-side recompression smoke";
    produce_gzip(addr, TOPIC, topic_id, payload).await;

    let served = fetch_first_batch(addr, TOPIC, topic_id).await;
    check!(
        served.attributes.compression() == CompressionType::Lz4,
        "broker must re-encode the gzip batch to lz4 before write"
    );
    assert!(served.records.len() == 1);
    check!(
        served.records[0].value.as_deref() == Some(payload.as_slice()),
        "record payload must survive the recompress round-trip"
    );

    handle.shutdown().await;
}

/// Sanity check: when `compression.type=producer` (the Kafka default),
/// the broker does NOT recompress — the served batch keeps the
/// producer's gzip flag verbatim. Without this guard a regression that
/// always recompresses would still satisfy the lz4 happy-path test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_compression_producer_preserves_producer_gzip() {
    const TOPIC: &str = "passthrough";
    let (handle, addr) = start_broker().await;

    create_topic_with_compression(addr, TOPIC, "producer").await;
    wait_for_compression(&handle, TOPIC, None).await;

    let topic_id = get_topic_id(addr, TOPIC).await;
    let payload = b"passthrough payload";
    produce_gzip(addr, TOPIC, topic_id, payload).await;

    let served = fetch_first_batch(addr, TOPIC, topic_id).await;
    assert!(
        served.attributes.compression() == CompressionType::Gzip,
        "compression.type=producer must preserve the producer's gzip flag"
    );
    assert!(served.records[0].value.as_deref() == Some(payload.as_slice()));

    handle.shutdown().await;
}
