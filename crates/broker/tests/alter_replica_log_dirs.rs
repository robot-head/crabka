// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by `jbod.rs`.
#![allow(clippy::pedantic)]

//! KIP-113 — `AlterReplicaLogDirs` (api_key 34) end-to-end.
//!
//! Spins up a single broker with two `log.dirs`, creates a 2-partition
//! topic, then issues `AlterReplicaLogDirs` to move both partitions
//! into the second directory. Asserts:
//!   1. the partition directories migrate on disk to the target dir,
//!   2. `DescribeLogDirs` polls converge with `is_future_key = false`
//!      in the target dir for both partitions,
//!   3. invalid target / missing replica return the right Kafka
//!      error codes.

use assert2::{assert, check};
use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_protocol::owned::alter_replica_log_dirs_request::{
    AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
};
use crabka_protocol::owned::alter_replica_log_dirs_response::AlterReplicaLogDirsResponse;
use crabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use crabka_protocol::owned::create_topics_response::CreateTopicsResponse;
use crabka_protocol::owned::describe_log_dirs_request::DescribeLogDirsRequest;
use crabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResponse;
use crabka_protocol::{Decode, Encode};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const CLIENT_ID: &str = "crabka-arld-test";
const ALTER_VERSION: i16 = 2;
const DESCRIBE_VERSION: i16 = 4;

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
    frame.put_u8(0); // header tagged-fields (every API here is flexible)
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
    const VERSION: i16 = 7;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 19, VERSION, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, VERSION).unwrap();
    assert!(resp.topics[0].error_code == 0, "CreateTopics must succeed");
}

async fn alter_replica_log_dirs(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    partitions: Vec<i32>,
) -> AlterReplicaLogDirsResponse {
    let req = AlterReplicaLogDirsRequest {
        dirs: vec![AlterReplicaLogDir {
            path: target_dir.to_string_lossy().to_string(),
            topics: vec![AlterReplicaLogDirTopic {
                name: topic.to_string(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, ALTER_VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 34, ALTER_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    AlterReplicaLogDirsResponse::decode(&mut cur, ALTER_VERSION).unwrap()
}

async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let req = DescribeLogDirsRequest {
        topics: None,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 35, DESCRIBE_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    DescribeLogDirsResponse::decode(&mut cur, DESCRIBE_VERSION).unwrap()
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

fn count_topic_dirs(dir: &std::path::Path, topic: &str) -> usize {
    let prefix = format!("{topic}-");
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|n| n.starts_with(&prefix) && !n.ends_with("-future"))
                .unwrap_or(false)
        })
        .count()
}

/// Wait until `DescribeLogDirs` reports both partitions of `topic`
/// under `target_dir` with `is_future_key == false`.
async fn wait_for_move_complete(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    expected: &[i32],
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let target_canon = std::fs::canonicalize(target_dir).unwrap();
    loop {
        let resp = describe_log_dirs(addr).await;
        let mut current_in_target: Vec<i32> = Vec::new();
        let mut any_future = false;
        for result in &resp.results {
            let result_canon = std::fs::canonicalize(&result.log_dir)
                .unwrap_or_else(|_| std::path::PathBuf::from(&result.log_dir));
            if result_canon != target_canon {
                continue;
            }
            for t in &result.topics {
                if t.name != topic {
                    continue;
                }
                for p in &t.partitions {
                    if p.is_future_key {
                        any_future = true;
                    } else if expected.contains(&p.partition_index) {
                        current_in_target.push(p.partition_index);
                    }
                }
            }
        }
        current_in_target.sort_unstable();
        current_in_target.dedup();
        if !any_future && current_in_target == expected {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "move never completed: in_target={current_in_target:?} any_future={any_future}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn alter_replica_log_dirs_moves_partitions_to_target_dir() {
    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    const N: i32 = 2;
    create_topic(addr, "t", N).await;
    wait_all_partitions(&handle, "t", N).await;

    // Identify which dir holds which partitions today (placement is
    // least-loaded; with N=2 each dir gets one). Pick the source dir
    // that DOES hold partition 0 and move both partitions to the
    // OTHER directory.
    let primary_has_0 = primary.path().join("t-0").exists();
    let target_dir = if primary_has_0 {
        extra.path()
    } else {
        primary.path()
    };

    let resp = alter_replica_log_dirs(addr, target_dir, "t", vec![0, 1]).await;
    let topic_results: Vec<_> = resp
        .results
        .iter()
        .filter(|t| t.topic_name == "t")
        .collect();
    assert!(
        topic_results.len() == 1,
        "topic must be present in response"
    );
    for p in &topic_results[0].partitions {
        assert!(
            p.error_code == 0,
            "partition {} ack must be NONE, got {}",
            p.partition_index,
            p.error_code
        );
    }

    wait_for_move_complete(addr, target_dir, "t", &[0, 1]).await;

    // Both partitions now live in the target dir; the source is empty.
    let source_dir = if primary_has_0 {
        primary.path()
    } else {
        extra.path()
    };
    assert!(count_topic_dirs(target_dir, "t") == 2);
    assert!(count_topic_dirs(source_dir, "t") == 0);
    // No future dirs should remain anywhere.
    for d in [primary.path(), extra.path()] {
        for entry in std::fs::read_dir(d).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                !name.ends_with("-future"),
                "future dir lingered in {}: {name}",
                d.display()
            );
        }
    }

    // A second call to the same target is a no-op success.
    let resp2 = alter_replica_log_dirs(addr, target_dir, "t", vec![0]).await;
    let topic2 = resp2
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("response includes t");
    assert!(topic2.partitions[0].error_code == 0);

    handle.shutdown().await;
}

#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_target() {
    let (handle, _primary, _extra, addr) = start_two_dir_broker().await;
    create_topic(addr, "t", 1).await;
    wait_all_partitions(&handle, "t", 1).await;

    let bogus = tempfile::tempdir().unwrap();
    let resp = alter_replica_log_dirs(addr, bogus.path(), "t", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic in response");
    // 57 == LOG_DIR_NOT_FOUND
    assert!(topic.partitions[0].error_code == 57);

    handle.shutdown().await;
}

#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_replica() {
    let (handle, _primary, extra, addr) = start_two_dir_broker().await;

    // Don't create the topic — naming a partition we don't host
    // should return REPLICA_NOT_AVAILABLE.
    let resp = alter_replica_log_dirs(addr, extra.path(), "missing", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "missing")
        .expect("topic in response");
    // 11 == REPLICA_NOT_AVAILABLE
    assert!(topic.partitions[0].error_code == 11);

    handle.shutdown().await;
}

/// Boot a two-dir broker with `SimpleAclAuthorizer` + no super-users +
/// no ACL grants — every authorize() returns Deny. Cluster.Alter on
/// AlterReplicaLogDirs (api_key 34) must come back as
/// CLUSTER_AUTHORIZATION_FAILED for every listed partition.
#[tokio::test]
async fn alter_replica_log_dirs_denied_without_cluster_alter() {
    use crabka_broker::authorizer::SimpleAclAuthorizer;

    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    // Default authorizer for `for_tests` is AllowAll; swap in a deny-
    // everything SimpleAclAuthorizer (empty super-users + empty ACL
    // image) so the Cluster Alter gate engages.
    cfg.super_users = std::collections::HashSet::new();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    // We never create the topic — irrelevant; the ACL gate fires
    // before the partition lookup, so the per-partition row is
    // CLUSTER_AUTHORIZATION_FAILED regardless of whether the replica
    // exists locally.
    let resp = alter_replica_log_dirs(addr, extra.path(), "t", vec![0, 1]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic in response");
    assert!(topic.partitions.len() == 2);
    for p in &topic.partitions {
        // 31 == CLUSTER_AUTHORIZATION_FAILED
        assert!(
            p.error_code == 31,
            "partition {} must be denied, got {}",
            p.partition_index,
            p.error_code
        );
    }

    handle.shutdown().await;
}

/// Produce a few batches, move the partition to the other dir, then
/// consume and verify every produced record survives. Exercises the
/// `catch_up` batch-copy path in `future_log.rs` (the empty-log move
/// hits zero-batch catch-up, which doesn't run the `append_at` loop).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_replica_log_dirs_preserves_records_across_move() {
    use bytes::Bytes;
    use crabka_client_consumer::{AutoOffsetReset, Consumer};
    use crabka_client_producer::{Producer, ProducerRecord};

    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    create_topic(addr, "t", 1).await;
    wait_all_partitions(&handle, "t", 1).await;

    let bootstrap = addr.to_string();
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .expect("producer build");
    for i in 0..50i32 {
        // `Producer::send` returns a `oneshot::Receiver` for the ack;
        // drop it and let `flush` synchronize before the alter. This
        // matches the pattern in `crates/broker/tests/durability.rs`.
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.expect("flush");

    // Pick the OTHER dir as the move target.
    let primary_has_0 = primary.path().join("t-0").exists();
    let target_dir = if primary_has_0 {
        extra.path()
    } else {
        primary.path()
    };

    let resp = alter_replica_log_dirs(addr, target_dir, "t", vec![0]).await;
    let topic = resp
        .results
        .iter()
        .find(|t| t.topic_name == "t")
        .expect("topic");
    assert!(topic.partitions[0].error_code == 0);
    wait_for_move_complete(addr, target_dir, "t", &[0]).await;

    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id("arld-move-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["t".to_string()])
        .build()
        .await
        .expect("consumer build");

    let mut consumed: Vec<String> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while consumed.len() < 50 && Instant::now() < deadline {
        for r in consumer
            .poll(Duration::from_millis(200))
            .await
            .expect("poll")
        {
            if let Some(v) = r.value {
                consumed.push(String::from_utf8(v.to_vec()).unwrap());
            }
        }
    }
    consumed.sort();
    let mut expected: Vec<String> = (0..50).map(|i| format!("v{i}")).collect();
    expected.sort();
    assert!(consumed == expected, "all records survived the move");

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    handle.shutdown().await;
}

/// Boot a broker, create a topic, produce records, shut down, then
/// plant a `<topic>-<partition>-future/` directory in the OTHER log
/// dir before restarting. The restart must (a) re-discover the
/// stranded future log via `log_dir::scan_future`, (b) call
/// `future_log::resume_move` for the real partition, and (c) drive
/// the move to completion so `DescribeLogDirs` ends up reporting the
/// partition in the target dir with `is_future_key=false`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_resumes_move_for_existing_partition() {
    use bytes::Bytes;
    use crabka_client_producer::{Producer, ProducerRecord};

    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();

    // First boot: create topic, produce a handful of records, then
    // shut down cleanly so the partition directory is left on disk.
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    let handle = Broker::start(cfg).await.expect("first boot");
    let addr = handle.listen_addr();
    create_topic(addr, "t", 1).await;
    wait_all_partitions(&handle, "t", 1).await;

    let producer = Producer::builder()
        .bootstrap(addr.to_string())
        .build()
        .await
        .expect("producer");
    for i in 0..5i32 {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(Bytes::from(format!("v{i}"))),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.expect("flush");
    producer.close().await.expect("producer close");

    // Find which dir holds `t-0` and pick the OTHER as the target.
    let primary_has_0 = primary.path().join("t-0").exists();
    let (current_dir, target_dir) = if primary_has_0 {
        (primary.path(), extra.path())
    } else {
        (extra.path(), primary.path())
    };
    handle.shutdown().await;

    // Plant an empty future dir to simulate a crash mid-ARLD before
    // the move task got a chance to copy anything. On restart, the
    // broker discovers it and resumes the move, copying the
    // already-produced batches into it.
    let future_path = target_dir.join("t-0-future");
    std::fs::create_dir_all(&future_path).expect("plant future dir");
    assert!(future_path.exists());
    assert!(
        current_dir.join("t-0").exists(),
        "source must still be here"
    );

    // Restart against the same dirs. `BootstrapMode::Rejoin`
    // because the raft log from the first boot is still on disk.
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    cfg.bootstrap_mode = crabka_broker::BootstrapMode::Rejoin;
    let handle = Broker::start(cfg).await.expect("restart");
    let addr = handle.listen_addr();

    // Wait for the resumed move to converge: partition lives in
    // target dir with no remaining future entries.
    wait_for_move_complete(addr, target_dir, "t", &[0]).await;
    check!(count_topic_dirs(target_dir, "t") == 1);
    check!(count_topic_dirs(current_dir, "t") == 0);
    check!(!future_path.exists(), "future dir must be renamed away");

    handle.shutdown().await;
}

/// Plant a `<topic>-<partition>-future` directory in one of the
/// configured log.dirs for a topic that doesn't exist, then start the
/// broker. The startup scan in `Broker::start` must remove the
/// stranded future dir; `DescribeLogDirs` reports no future entries.
#[tokio::test]
async fn startup_cleans_up_stranded_future_dir() {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();

    // Stranded future dir: topic "ghost" was never created.
    let stranded = extra.path().join("ghost-0-future");
    std::fs::create_dir_all(&stranded).unwrap();
    assert!(stranded.exists());

    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    // Broker startup must have swept the stranded future dir.
    assert!(
        !stranded.exists(),
        "startup must remove stranded future dir at {}",
        stranded.display()
    );

    // DescribeLogDirs surfaces no future entries.
    let resp = describe_log_dirs(addr).await;
    let any_future = resp
        .results
        .iter()
        .flat_map(|r| r.topics.iter())
        .flat_map(|t| t.partitions.iter())
        .any(|p| p.is_future_key);
    assert!(!any_future, "no future entries should remain after sweep");

    handle.shutdown().await;
}
