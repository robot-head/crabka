// Rust 1.95 annotate-snippets ICE on `clippy::pedantic` (see
// elect_leaders.rs / acl_handlers.rs preamble).

//! `BrokerHandle::controlled_shutdown` integration test.
//!
//! The test uses a 3-broker PLAINTEXT cluster and an rf=3 topic. Broker 1 is
//! the preferred leader of every partition. `controlled_shutdown(broker 1)`
//! must:
//! 1. Move leadership of every partition off broker 1, and
//! 2. Return `Ok(())` after the controller acknowledges
//!    `should_shut_down=true`. Broker 1 then leads zero partitions.
//!
//! The test is gated to non-Windows to match the multi-broker convention from
//! slices 10b/12b. The openraft `debug_assert!` races on the hosted Windows
//! task scheduler are unrelated to the protocol under test.

use std::{io, net::SocketAddr, time::Duration};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use crabka_broker::BrokerHandle;
use crabka_protocol::{
    Decode, Encode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

mod support;

const CREATE_TOPICS_API_KEY: i16 = 19;
const CREATE_TOPICS_VERSION: i16 = 7;

/// Length-prefixed PLAINTEXT request/response over a single TCP
/// stream. Mirrors the helper in `tests/elect_leaders.rs`.
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
    let client_id = "crabka-controlled-shutdown-test";
    frame.put_i16(i16::try_from(client_id.len()).expect("client_id fits"));
    frame.put_slice(client_id.as_bytes());
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).expect("frame fits in u32"))
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    if flexible {
        let _tagged = cur.get_u8();
    }
    Ok(cur.to_vec())
}

async fn create_topic(addr: SocketAddr, name: &str, partitions: i32, rf: i16) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor: rf,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION)
        .expect("encode CreateTopics");
    let resp_bytes = round_trip(
        &mut stream,
        CREATE_TOPICS_API_KEY,
        CREATE_TOPICS_VERSION,
        1,
        true,
        &body,
    )
    .await
    .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION)
        .expect("decode CreateTopicsResponse");
    assert!(resp.topics.len() == 1);
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

/// Waits until `(topic, partition)` appears in the metadata image of
/// `handle`.
async fn wait_partition_exists(handle: &BrokerHandle, topic: &str, partition: i32) {
    handle.wait_until_partition_present(topic, partition).await;
}

/// Injects a `PartitionRecord` that makes `target` the leader for every
/// partition of `topic`. It uses `submit_metadata_record_for_test` to bypass
/// the public wire path, so the test does not have to drive `ElectLeaders`.
/// It returns after the image shows `leader=target` for every partition.
async fn force_leadership_for_test(
    leader_handle: &BrokerHandle,
    topic: &str,
    partitions: i32,
    target: u64,
    replicas: &[u64],
) {
    use crabka_metadata::{MetadataRecord, PartitionRecord};

    let image = leader_handle.controller_image_for_test();
    for p in 0..partitions {
        let Some(pr) = image.partition(topic, p) else {
            continue;
        };
        let record = MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_string(),
            partition: p,
            leader: crabka_metadata::NodeId(target),
            replicas: replicas
                .iter()
                .copied()
                .map(crabka_metadata::NodeId)
                .collect(),
            isr: replicas
                .iter()
                .copied()
                .map(crabka_metadata::NodeId)
                .collect(),
            leader_epoch: pr.leader_epoch.next(),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        });
        leader_handle
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit forced leadership");
    }

    // Wait until every partition reports `target` as leader.
    leader_handle
        .wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target)
            })
        })
        .await;
}

/// Returns the number of partitions in `topic` that `target` currently leads,
/// according to the image of `observer`.
fn leader_count(observer: &BrokerHandle, topic: &str, partitions: i32, target: u64) -> usize {
    let mut count = 0usize;
    for p in 0..partitions {
        if observer.partition_leader_for_test(topic, p) == Some(target) {
            count += 1;
        }
    }
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controlled_shutdown_drains_leadership_and_returns_ok() {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let lock = LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let _g = lock.lock().await;

    support::init_tracing();

    let mut cluster = support::start_n_node_with_retry(3).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    // Resolve the raft (controller) leader. We then pick a *follower*
    // broker as the controlled-shutdown target. Picking the controller
    // leader would also exercise the path — its heartbeat to itself is
    // a local round-trip — but the bootstrap broker is also the sole
    // replica of `__consumer_offsets/0` (rf=1, replicas=[node_id]),
    // which cannot be drained. Targeting a follower keeps the test
    // focused on the controllable wire path.
    let raft_leader_idx = {
        let leader = cluster[0].0.wait_until_controller_leader().await;
        cluster
            .iter()
            .position(|(_, cfg, _)| cfg.node_id == leader)
            .expect("controller leader must be a cluster member")
    };
    let target_idx = (raft_leader_idx + 1) % cluster.len();
    let target_node_id = cluster[target_idx].1.node_id;
    eprintln!(
        "controlled shutdown target: broker_id={target_node_id} (cluster idx {target_idx}, \
         raft leader is idx {raft_leader_idx})"
    );

    // Create topic on the cluster leader's listen addr.
    let addr = cluster[raft_leader_idx].1.listen_addr;
    let topic: &str = "drain-me";
    let partitions: i32 = 4;
    create_topic(addr, topic, partitions, 3).await;
    for p in 0..partitions {
        for (h, _, _) in &cluster {
            wait_partition_exists(h, topic, p).await;
        }
    }

    // Force the target to be leader of every partition. The natural
    // round-robin assignment with 3 brokers / 4 partitions would
    // leader-balance across the cluster — we want to prove draining is
    // exhaustive, so concentrate every leader onto the target first.
    let mut replicas: Vec<u64> = (1..=3).collect();
    // Put target first so its `replicas[0]` is itself (preferred).
    replicas.sort_by_key(|n| i32::from(*n != target_node_id));
    force_leadership_for_test(
        &cluster[raft_leader_idx].0,
        topic,
        partitions,
        target_node_id.0,
        &replicas,
    )
    .await;

    // Sanity: target leads everything before we start. Use the raft
    // leader's image since the target is about to be drained.
    for (h, _, _) in &cluster {
        h.wait_for_image(|img| {
            (0..partitions).all(|p| {
                img.partition(topic, p)
                    .is_some_and(|pr| pr.leader == target_node_id)
            })
        })
        .await;
    }
    assert!(
        leader_count(
            &cluster[raft_leader_idx].0,
            topic,
            partitions,
            target_node_id.0
        ) == usize::try_from(partitions).unwrap(),
        "target should lead all partitions before shutdown"
    );

    // Pop the target out of the cluster vec — `controlled_shutdown`
    // consumes the handle, and we need to keep the surviving brokers
    // alive afterward to verify the post-shutdown image.
    let (target_handle, target_cfg, target_dir) = cluster.remove(target_idx);
    drop(target_cfg);
    drop(target_dir);

    // Drive controlled shutdown. The handler-side leader transfer
    // submits records on each heartbeat tick (default 200ms in
    // for_tests config). Allow several ticks plus raft commit
    // latency.
    target_handle
        .controlled_shutdown(Duration::from_secs(30))
        .await
        .expect("controlled_shutdown should drain and exit cleanly");

    // After the controlled-shutdown future resolves, the target's
    // heartbeat client has observed should_shut_down=true. That means
    // the controller's image, at the moment of that response, had zero
    // partitions led by the target. Verify on a surviving broker.
    for (observer, _, _) in &cluster {
        observer
            .wait_for_image(|img| {
                (0..partitions)
                    .all(|p| img.partition(topic, p).map(|pr| pr.leader) != Some(target_node_id))
            })
            .await;
    }

    // Tidy up surviving brokers.
    for (h, _, _) in cluster {
        h.shutdown().await;
    }
}
