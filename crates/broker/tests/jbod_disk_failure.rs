// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by jbod.rs / compaction.rs.
#![allow(clippy::pedantic)]

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

use std::{io, net::SocketAddr};

use assert2::{assert, check};
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_protocol::{
    Decode, Encode,
    owned::{
        assign_replicas_to_dirs_request::{
            AssignReplicasToDirsRequest, DirectoryData as ReqDirData, PartitionData as ReqPartData,
            TopicData as ReqTopicData,
        },
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as ProtocolUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

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
    // Wait until every partition of `topic` has materialized in the image.
    for p in 0..n {
        handle.wait_until_partition_present(topic, p).await;
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

/// KIP-112: when ALL configured log dirs go offline the broker must
/// self-shutdown (latch `should_shutdown` to `true`).  This test uses a
/// single-dir broker so flipping that one dir offline immediately
/// satisfies the all-dirs condition.
///
/// The `for_tests` heartbeat interval is 200 ms, so the check fires well
/// within the 15-second timeout below.
#[tokio::test]
async fn all_log_dirs_offline_triggers_self_shutdown() {
    let primary = tempfile::tempdir().unwrap();
    // Single-dir broker: no extra_log_dirs.
    let cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker start");

    // Subscribe before flipping so we can't miss the transition.
    let mut shutdown_rx = handle.should_shutdown_rx();

    // Flip the only log dir offline. This is the all-dirs condition.
    assert!(
        handle.test_mark_log_dir_offline(primary.path()),
        "mark_offline must return true (dir was registered and online)"
    );

    // Wait up to 15 s for the heartbeat client to detect the all-dirs
    // condition and latch should_shutdown to true.
    let woke = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if *shutdown_rx.borrow_and_update() {
                return;
            }
            if shutdown_rx.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(
        woke.is_ok(),
        "broker did not signal self-shutdown when all log dirs went offline"
    );
    assert!(
        *shutdown_rx.borrow(),
        "should_shutdown must be true after all dirs offline"
    );

    // Shutdown should complete without hanging: the supervisor was already
    // cancelled by the self-shutdown path, and cancelling an already-
    // cancelled token is idempotent.
    handle.shutdown().await;
}

/// KIP-112 / KIP-858: `AssignReplicasToDirs` (api_key=73) is accepted by the
/// controller-leader broker, records the assignment, and echoes the request
/// back with `error_code=0` on every partition.
///
/// This exercises the real async `handle` path: decode → leader gate →
/// `collect_assignment_changes` → `submit_change` → `build_echo_response` →
/// encode.
#[tokio::test]
async fn assign_replicas_to_dirs_reports_and_echoes() {
    const TOPIC: &str = "kip112-assign";
    const N: i32 = 2;
    // Use a single-dir broker so the broker IS the controller leader.
    let primary = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    create_topic(addr, TOPIC, N).await;
    wait_all_partitions(&handle, TOPIC, N).await;

    // Look up the topic UUID from the controller image so we can reference
    // the partition correctly in the request.
    let image = handle.controller_image_for_test();
    let topic_uuid = image
        .topics()
        .find(|t| t.name == TOPIC)
        .map(|t| t.topic_id)
        .expect("topic must be in the image after wait_all_partitions");

    // Choose an arbitrary dir UUID to assign partition 0 on broker 1.
    let dir_uuid = uuid::Uuid::from_u128(0xCAFE_BABE);

    const VERSION: i16 = 0; // AssignReplicasToDirs only has version 0
    let req = AssignReplicasToDirsRequest {
        broker_id: 1, // for_tests default broker_id
        broker_epoch: -1,
        directories: vec![ReqDirData {
            id: ProtocolUuid(dir_uuid.into_bytes()),
            topics: vec![ReqTopicData {
                topic_id: ProtocolUuid(topic_uuid.into_bytes()),
                partitions: vec![ReqPartData {
                    partition_index: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(&mut stream, 73, VERSION, &body).await.unwrap();

    let mut cur: &[u8] = &resp_bytes;
    let resp = AssignReplicasToDirsResponse::decode(&mut cur, VERSION).unwrap();

    check!(
        resp.error_code == 0,
        "AssignReplicasToDirs top-level error_code must be NONE (0), got {}",
        resp.error_code
    );
    assert!(
        !resp.directories.is_empty(),
        "response must echo at least one directory"
    );
    check!(
        resp.directories[0].topics[0].partitions[0].error_code == 0,
        "per-partition error_code must be NONE (0)"
    );

    handle.shutdown().await;
}

/// KIP-112: a `BrokerHeartbeat` (api_key=63) with `offline_log_dirs` set is
/// accepted by the controller. For a single-broker cluster with no ISR peers
/// the failover scan finds no alive ISR alternative → plan.changes is empty →
/// `submit_change` is skipped → response `error_code=0`.
///
/// This exercises the heartbeat handler's offline-dir failover block end-to-end
/// (the no-change path).
#[tokio::test]
async fn heartbeat_with_offline_log_dirs_is_accepted() {
    use crabka_protocol::owned::broker_heartbeat_request::MAX_VERSION as HB_MAX_VERSION;

    let primary = tempfile::tempdir().unwrap();
    let cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("broker start");
    let addr = handle.listen_addr();

    // Wait until the broker has registered itself and elected a raft leader
    // (so the heartbeat handler reaches the leader branch, not NOT_CONTROLLER).
    handle.wait_until_controller_leader().await;

    // Send a heartbeat with a made-up offline dir UUID. The broker is the
    // only replica so alive_isr is empty → no change → no error.
    let fake_offline_dir = uuid::Uuid::from_u128(0xDEAD_1234);
    let req = BrokerHeartbeatRequest {
        broker_id: 1, // for_tests default broker_id
        broker_epoch: -1,
        current_metadata_offset: 0,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: vec![ProtocolUuid(fake_offline_dir.into_bytes())],
        cordoned_log_dirs: None,
        ..Default::default()
    };

    let mut body = BytesMut::new();
    req.encode(&mut body, HB_MAX_VERSION).unwrap();

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(&mut stream, 63, HB_MAX_VERSION, &body)
        .await
        .unwrap();

    let mut cur: &[u8] = &resp_bytes;
    let resp = BrokerHeartbeatResponse::decode(&mut cur, HB_MAX_VERSION).unwrap();

    assert!(
        resp.error_code == 0,
        "BrokerHeartbeat with offline_log_dirs must be accepted (error_code=0), got {}",
        resp.error_code
    );

    handle.shutdown().await;
}
