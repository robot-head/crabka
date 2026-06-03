// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by jbod.rs / compaction.rs.
#![allow(clippy::pedantic)]
#![cfg(not(target_os = "windows"))]

//! KIP-112 runtime log-dir failure path.
//!
//! Boots a single broker with two log dirs (primary + extra), creates a
//! 6-partition topic so JBOD placement spreads partitions across both dirs,
//! flips the `extra` dir offline via the test seam, then asserts:
//!
//!   1. A Produce to a partition that lives on the now-offline `extra` dir
//!      returns `KAFKA_STORAGE_ERROR` (error code 56).
//!   2. A Produce to a partition that lives on the still-online `primary` dir
//!      returns error code 0.

use assert2::assert;
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::create_topics_response::CreateTopicsResponse;
use crabka_protocol::owned::produce_request::{
    PartitionProduceData, ProduceRequest, TopicProduceData,
};
use crabka_protocol::owned::produce_response::ProduceResponse;
use crabka_protocol::records::{Record, RecordBatch};
use crabka_protocol::{Decode, Encode};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENT_ID: &str = "crabka-jbod-disk-failure-test";
const PRODUCE_VERSION: i16 = 9; // flexible, acks=1

/// Raw wire round-trip: send a framed request, read the response, strip
/// the correlation-id + tagged-fields header prefix, and return the body.
async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(1); // correlation id
    frame.put_i16(i16::try_from(CLIENT_ID.len()).unwrap());
    frame.put_slice(CLIENT_ID.as_bytes());
    frame.put_u8(0); // header tagged-fields (flexible APIs)
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
    let _tagged = cur.get_u8(); // v1 response header tagged-fields
    Ok(cur.to_vec())
}

async fn start_two_dir_broker() -> (BrokerHandle, TempDir, TempDir, SocketAddr) {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();
    (handle, primary, extra, addr)
}

async fn create_topic(addr: SocketAddr, topic: &str, partitions: i32) {
    const VERSION: i16 = 7;
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 19, VERSION, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, VERSION).unwrap();
    assert!(resp.topics[0].error_code == 0, "CreateTopics must succeed");
}

async fn wait_all_partitions(handle: &BrokerHandle, topic: &str, n: i32) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut all = true;
        for p in 0..n {
            if !handle.has_partition(topic, p).await {
                all = false;
                break;
            }
        }
        if all {
            return;
        }
        assert!(Instant::now() <= deadline, "partitions never materialized");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// List partition indices for `topic` whose data dir lives directly under `dir`.
fn partitions_in_dir(dir: &std::path::Path, topic: &str) -> Vec<i32> {
    let prefix = format!("{topic}-");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            s.strip_prefix(&prefix)
                .and_then(|suffix| suffix.parse::<i32>().ok())
        })
        .collect()
}

/// Produce one record to `(topic, partition)` and return the per-partition
/// `error_code` from the response.
async fn produce_and_get_error(addr: SocketAddr, topic: &str, partition: i32) -> i16 {
    let batch = RecordBatch {
        records: vec![Record {
            offset_delta: 0,
            value: Some(bytes::Bytes::from_static(b"kip-112-test")),
            ..Default::default()
        }],
        ..Default::default()
    };
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(batch.into()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION).unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(&mut stream, 0, PRODUCE_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, PRODUCE_VERSION).unwrap();
    resp.responses[0].partition_responses[0].error_code
}

#[tokio::test]
async fn produce_to_partition_on_offline_dir_returns_storage_error() {
    const TOPIC: &str = "kip112-offline";
    // 6 partitions: jbod.rs shows this is enough to guarantee spread across
    // both dirs under the least-loaded placement algorithm.
    const N: i32 = 6;

    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    create_topic(addr, TOPIC, N).await;
    wait_all_partitions(&handle, TOPIC, N).await;

    // Confirm spread: both dirs must hold at least one partition of the topic.
    let in_extra = partitions_in_dir(extra.path(), TOPIC);
    let in_primary = partitions_in_dir(primary.path(), TOPIC);
    assert!(
        !in_extra.is_empty(),
        "test premise: at least one partition must land on the extra dir \
         (primary={} extra={})",
        in_primary.len(),
        in_extra.len()
    );
    assert!(
        !in_primary.is_empty(),
        "test premise: at least one partition must land on the primary dir"
    );

    // Pick the smallest partition index on each dir for determinism.
    let mut extra_parts = in_extra.clone();
    extra_parts.sort_unstable();
    let offline_partition = extra_parts[0];

    let mut primary_parts = in_primary.clone();
    primary_parts.sort_unstable();
    let online_partition = primary_parts[0];

    // Flip ONLY the extra dir offline. The primary dir stays online, so the
    // broker does NOT trigger the all-dirs-offline self-shutdown path.
    assert!(
        handle.test_mark_log_dir_offline(extra.path()),
        "mark_offline must return true (dir was registered and online)"
    );

    // Case 1: Produce to the offline-dir partition must return KAFKA_STORAGE_ERROR (56).
    let code = produce_and_get_error(addr, TOPIC, offline_partition).await;
    assert!(
        code == 56,
        "partition {offline_partition} on offline extra dir must return \
         KAFKA_STORAGE_ERROR (56); got {code}"
    );

    // Case 2 (sanity): Produce to the still-online primary-dir partition must succeed.
    let code = produce_and_get_error(addr, TOPIC, online_partition).await;
    assert!(
        code == 0,
        "partition {online_partition} on online primary dir must succeed (0); got {code}"
    );

    handle.shutdown().await;
}
