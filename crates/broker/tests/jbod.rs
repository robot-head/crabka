// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by compaction.rs / elect_leaders.rs.

//! JBOD / multi-log-dir + `DescribeLogDirs` (KIP-113) end-to-end.
//!
//! Boots a single broker with two log directories, creates a 6-partition
//! topic, and asserts:
//!   1. partition data is spread across both directories (least-loaded
//!      placement), and
//!   2. `DescribeLogDirs` reports one result per directory whose union
//!      covers every partition, consistent with what's on disk.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const CLIENT_ID: &str = "crabka-jbod-test";

async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(1);
    frame.put_i16(i16::try_from(CLIENT_ID.len()).unwrap());
    frame.put_slice(CLIENT_ID.as_bytes());
    frame.put_u8(0); // header tagged-fields (all APIs here are flexible)
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
    let version: i16 = 7;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let resp_bytes = round_trip(&mut stream, 19, version, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, version).unwrap();
    assert!(resp.topics[0].error_code == 0, "CreateTopics must succeed");
}

async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let req = DescribeLogDirsRequest {
        topics: None,
        ..Default::default()
    };
    let version: i16 = 4;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let resp_bytes = round_trip(&mut stream, 35, version, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    DescribeLogDirsResponse::decode(&mut cur, version).unwrap()
}

async fn wait_all_partitions(handle: &BrokerHandle, topic: &str, n: i32) {
    // The on-disk / DescribeLogDirs assertions below read partition directories
    // straight from the log dirs, so wait for each partition's LOCAL writer-actor
    // to materialize (which creates its dir) — not just the metadata image, which
    // can name the partition before the local replica exists. `min = 0` waits only
    // for the local replica/writer to appear.
    for p in 0..n {
        handle.wait_until_local_log_end_offset(topic, p, 0).await;
    }
}

/// Count `topic-partition` subdirs for `topic` directly under `dir`.
fn count_topic_dirs(dir: &std::path::Path, topic: &str) -> usize {
    let prefix = format!("{topic}-");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(&prefix))
        })
        .count()
}

#[tokio::test]
async fn partitions_spread_across_dirs_and_describe_log_dirs_reports_them() {
    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    let n: i32 = 6;
    create_topic(addr, "t", n).await;
    wait_all_partitions(&handle, "t", n).await;

    // 1. Placement spread: both directories hold at least one partition of `t`.
    let in_primary = count_topic_dirs(primary.path(), "t");
    let in_extra = count_topic_dirs(extra.path(), "t");
    assert!(
        in_primary + in_extra == usize::try_from(n).unwrap(),
        "all partitions on disk"
    );
    assert!(
        in_primary > 0 && in_extra > 0,
        "partitions must spread across both dirs: primary={in_primary} extra={in_extra}"
    );

    // 2. DescribeLogDirs reports one result per configured dir, and the
    //    union of `t` partitions across results is the full 0..n set.
    let resp = describe_log_dirs(addr).await;
    assert!(resp.error_code == 0);
    assert!(resp.results.len() == 2, "one result per log dir");

    let mut reported: Vec<i32> = Vec::new();
    for result in &resp.results {
        assert!(result.error_code == 0);
        for topic in &result.topics {
            if topic.name == "t" {
                for p in &topic.partitions {
                    reported.push(p.partition_index);
                    assert!(p.partition_size >= 0);
                    assert!(!p.is_future_key);
                }
            }
        }
    }
    reported.sort_unstable();
    assert!(
        reported == (0..n).collect::<Vec<_>>(),
        "all partitions reported"
    );

    handle.shutdown().await;
}
