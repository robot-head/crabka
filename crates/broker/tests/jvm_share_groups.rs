#![allow(clippy::pedantic)]

//! JVM differential / interop test for KIP-932 share groups.
//!
//! Drives a REAL Apache Kafka 4.x `kafka-console-share-consumer.sh`
//! (a `KafkaShareConsumer` under the hood) inside an `mirror.gcr.io/apache/kafka:4.1.0`
//! container against an in-process Crabka broker running on the host. This
//! exercises Crabka's share-group wire protocol end-to-end against the real
//! JVM client:
//!
//! - `ApiVersions` negotiation (key 18; `share.version` advertised by Crabka),
//! - `FindCoordinator(GROUP/SHARE)` (key 10),
//! - `ShareGroupHeartbeat` (key 76) membership + assignment,
//! - `ShareFetch` (key 78) acquire + record bytes,
//! - `ShareAcknowledge` (key 79) implicit-ack on poll.
//!
//! The JVM share consumer joins a fresh share group with
//! `group.share.auto.offset.reset=earliest` so the share-partition start
//! offset begins at 0 and it reads every produced record. We assert its
//! stdout carries each produced value.
//!
//! Gated `#[ignore = "requires Docker"]`; run with `--ignored`.
//!
//! Networking mirrors `jvm_consumer_group_next_gen.rs` / `jvm_acceptance.rs`:
//! the broker binds `0.0.0.0:9092` and advertises `host.docker.internal:9092`;
//! the container reaches it via `--add-host=host.docker.internal:host-gateway`.

use std::{
    process::{Command, Stdio},
    time::Duration,
};

use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_log::LogConfig;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

/// Port the broker binds on the host and that the container reaches via
/// `host.docker.internal`.
const HOST_PORT: u16 = 9092;
const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
/// Official Apache Kafka image. Ships KIP-932 share groups (GA in 4.x) plus
/// the `kafka-console-share-consumer.sh` / `kafka-share-groups.sh` tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const SHARE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-share-consumer.sh";
const SHARE_GROUPS: &str = "/opt/kafka/bin/kafka-share-groups.sh";

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

/// Boot one broker bound to `0.0.0.0:9092`, advertising `host.docker.internal:
/// 9092` so the Docker container's post-Metadata connect targets a hostname it
/// can resolve. Mirrors `jvm_consumer_group_next_gen.rs::start_host_broker`.
async fn start_host_broker() -> (BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: std::net::SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: std::net::SocketAddr = "0.0.0.0:9093".parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: BOOTSTRAP.into(),
        log_dir: dir.path().to_path_buf(),
        log_config: LogConfig::default(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: std::time::Duration::from_secs(5),
        controller_heartbeat_interval: std::time::Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!(
        "CRABKA[test] broker started listen={LISTEN} advertised={BOOTSTRAP} port={HOST_PORT}"
    );
    (handle, dir)
}

async fn connect() -> Client {
    Client::builder()
        .bootstrap(format!("127.0.0.1:{HOST_PORT}"))
        .client_id("crabka-share-test")
        .build()
        .await
        .expect("client build")
}

fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

/// Create `topic` (1 partition) and wait until this broker leads partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str) -> uuid::Uuid {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert2::assert!(resp.topics[0].error_code == 0);
    broker.wait_until_partition_present(topic, 0).await;
    assert2::assert!(broker.has_partition(topic, 0).await);
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

/// Pre-create `__share_group_state` (as a KIP-932 client would, lazily via
/// `FindCoordinator(SHARE)`) and wait until every state partition is local, so
/// the share coordinator is write-ready before the JVM consumer drives
/// `ShareFetch`/`ShareAcknowledge`.
async fn bootstrap_share_state(broker: &BrokerHandle, client: &Client, key: &str) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2, // SHARE
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert2::assert!(resp.coordinators[0].error_code == 0);
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Produce the supplied `values` as one batch into `(topic, 0)`, retrying while
/// the freshly-created partition is still materializing its leader.
async fn produce(client: &Client, topic: &str, tid: uuid::Uuid, values: &[&str]) {
    for _ in 0..40 {
        let records: Vec<Record> = values
            .iter()
            .enumerate()
            .map(|(i, v)| Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(bytes::Bytes::copy_from_slice(v.as_bytes())),
                ..Default::default()
            })
            .collect();
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: 0,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: i32::try_from(values.len() - 1).unwrap(),
                                records,
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        if p.error_code == 0 {
            return;
        }
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER.
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-RPC retry. The failure means the
            // local writer-actor has not materialized yet even though the image
            // already names this broker leader; that local readiness is not in
            // the metadata image and `produce` holds no broker handle to await.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}");
}

/// Run a docker container against the host broker and return its output. The
/// share consumer exits non-zero on idle-timeout even after consuming, so
/// callers check stdout, not exit status.
fn docker_run(args: &[&str]) -> std::process::Output {
    let out = Command::new("docker")
        .arg("run")
        .arg("--rm")
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(KAFKA_IMAGE)
        .args(args)
        .stderr(Stdio::piped())
        .stdout(Stdio::piped())
        .output()
        .expect("docker run");
    eprintln!(
        "CRABKA[test] docker {args:?} status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );
    out
}

/// The headline differential test: a real JVM `KafkaShareConsumer` joins a
/// fresh Crabka share group, reads every produced record, and implicit-acks on
/// poll. We assert each produced value appears in its stdout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_consumer_reads_crabka() {
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm";
    let group = "jvm-share-g";
    let values = ["share-alpha", "share-bravo", "share-charlie", "share-delta"];

    let client = connect().await;
    let tid = create_topic(&broker, &client, topic).await;
    bootstrap_share_state(&broker, &client, &format!("{group}:{tid}:0")).await;
    produce(&client, topic, tid, &values).await;

    // Drive the real JVM KafkaShareConsumer. A fresh share group with
    // `group.share.auto.offset.reset=earliest` starts the share-partition at
    // offset 0, so it must read all produced records. `--timeout-ms` makes it
    // exit after the idle window once it has drained the partition.
    let consumed = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {BOOTSTRAP} \
                --topic {topic} \
                --group {group} \
                --consumer-property group.share.auto.offset.reset=earliest \
                --timeout-ms 20000 \
                --max-messages {}",
            values.len()
        ),
    ]);
    let stdout = String::from_utf8_lossy(&consumed.stdout);
    eprintln!("CRABKA[test] share-consumer stdout:\n{stdout}");

    for v in values {
        assert2::assert!(stdout.contains(v));
    }
}

/// `kafka-share-groups.sh --describe --state` surfaces the share group after
/// the JVM consumer has joined, proving Crabka serves the share-group admin
/// path (`ShareGroupDescribe`, `api_key` 77) to the real JVM tooling: the tool
/// resolves the share coordinator, sends `ShareGroupDescribe`, and renders the
/// group's coordinator + state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_groups_describe_state() {
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-d";
    let group = "jvm-share-gd";
    let values = ["d-one", "d-two"];

    let client = connect().await;
    let tid = create_topic(&broker, &client, topic).await;
    bootstrap_share_state(&broker, &client, &format!("{group}:{tid}:0")).await;
    produce(&client, topic, tid, &values).await;

    // Join + read so the group is registered with the coordinator.
    let _ = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {BOOTSTRAP} \
                --topic {topic} \
                --group {group} \
                --consumer-property group.share.auto.offset.reset=earliest \
                --timeout-ms 15000 \
                --max-messages {}",
            values.len()
        ),
    ]);

    // `--describe --state` drives ShareGroupDescribe (api_key 77). Renders e.g.
    //   GROUP         COORDINATOR (ID)              STATE   #MEMBERS
    //   jvm-share-gd  host.docker.internal:9092 (1) Empty   0
    let state = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_GROUPS} --bootstrap-server {BOOTSTRAP} --describe --state --group {group}"
        ),
    ]);
    let state_out = String::from_utf8_lossy(&state.stdout);
    eprintln!("CRABKA[test] share-groups --describe --state stdout:\n{state_out}");
    assert2::assert!(state_out.contains(group));
}

/// `kafka-share-groups.sh --list` drives `ListGroups` (`api_key` 16) with
/// `types_filter = ["share"]`. After a real JVM `KafkaShareConsumer` has joined
/// a share group on the Crabka broker, the share group id must appear in the
/// tool's `--list` stdout. Before the `ListGroups` share pass landed, the JVM
/// tool's `types_filter=["share"]` matched nothing and `--list` was EMPTY; this
/// asserts the regression is closed against the real Apache Kafka 4.1.0 tool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_share_groups_list() {
    let (broker, _dir) = start_host_broker().await;
    let topic = "kip932-jvm-l";
    let group = "jvm-share-gl";
    let values = ["l-one", "l-two"];

    let client = connect().await;
    let tid = create_topic(&broker, &client, topic).await;
    bootstrap_share_state(&broker, &client, &format!("{group}:{tid}:0")).await;
    produce(&client, topic, tid, &values).await;

    // Join + read so the share group is registered with the coordinator. The
    // share-group actor stays in the coordinator's share registry after the
    // consumer's idle-timeout exit (it is only removed on a delete-groups
    // tombstone), so `--list` below still sees a live group entry.
    let _ = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{SHARE_CONSUMER} \
                --bootstrap-server {BOOTSTRAP} \
                --topic {topic} \
                --group {group} \
                --consumer-property group.share.auto.offset.reset=earliest \
                --timeout-ms 15000 \
                --max-messages {}",
            values.len()
        ),
    ]);

    // `--list` drives ListGroups(16) with types_filter=["share"]. The share
    // group id must appear in stdout.
    let listed = docker_run(&[
        "bash",
        "-c",
        &format!("{SHARE_GROUPS} --bootstrap-server {BOOTSTRAP} --list"),
    ]);
    let list_out = String::from_utf8_lossy(&listed.stdout);
    eprintln!("CRABKA[test] share-groups --list stdout:\n{list_out}");
    assert2::assert!(list_out.contains(group));
}
