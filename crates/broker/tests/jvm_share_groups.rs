//! JVM differential / interop test for KIP-932 share groups.
//!
//! This test drives a REAL Apache Kafka 4.x
//! `kafka-console-share-consumer.sh`, which runs a `KafkaShareConsumer`,
//! inside a `mirror.gcr.io/apache/kafka:4.1.0` container. It runs against an
//! in-process Crabka broker on the host. This exercises Crabka's share-group
//! wire protocol end to end against the real JVM client:
//!
//! - `ApiVersions` negotiation (key 18; `share.version` advertised by Crabka),
//! - `FindCoordinator(GROUP/SHARE)` (key 10),
//! - `ShareGroupHeartbeat` (key 76) membership + assignment,
//! - `ShareFetch` (key 78) acquire + record bytes,
//! - `ShareAcknowledge` (key 79) implicit-ack on poll.
//!
//! The JVM share consumer joins a fresh share group with
//! `group.share.auto.offset.reset=earliest`, so the share-partition start
//! offset begins at 0 and the consumer reads every produced record. The test
//! asserts that its stdout carries each produced value.
//!
//! The test is gated with `#[ignore = "requires Docker"]`. Run it with
//! `--ignored`.
//!
//! The networking mirrors `jvm_consumer_group_next_gen.rs` and
//! `jvm_acceptance.rs`. The broker binds `0.0.0.0:9092` and advertises
//! `host.docker.internal:9092`. The container reaches it through
//! `--add-host=host.docker.internal:host-gateway`.

use std::{
    process::{Command, Stdio},
    time::Duration,
};

use assert2::assert;
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

/// Port that the broker binds on the host, and that the container reaches
/// through `host.docker.internal`.
const HOST_PORT: u16 = 9092;
const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
/// Official Apache Kafka image. It ships KIP-932 share groups, which are GA in
/// 4.x, and the `kafka-console-share-consumer.sh` and `kafka-share-groups.sh`
/// tools.
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const SHARE_CONSUMER: &str = "/opt/kafka/bin/kafka-console-share-consumer.sh";
const SHARE_GROUPS: &str = "/opt/kafka/bin/kafka-share-groups.sh";

const SHARE_STATE_TOPIC: &str = "__share_group_state";
const SHARE_STATE_PARTITIONS: i32 = 50;

/// Boots one broker bound to `0.0.0.0:9092` that advertises
/// `host.docker.internal:9092`. The Docker container's connect after Metadata
/// then targets a hostname it can resolve. This mirrors
/// `jvm_consumer_group_next_gen.rs::start_host_broker`.
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
        heartbeat_interval: crabka_units::millis(3_000),
        heartbeat_timeout: crabka_units::millis(9_000),
        replica_lag_time_max: crabka_units::millis(30_000),
        controller_election_timeout: crabka_units::secs(5),
        controller_heartbeat_interval: crabka_units::millis(500),
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

/// Creates `topic` with 1 partition and waits until this broker leads
/// partition 0.
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
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
    broker.wait_until_partition_present(topic, 0).await;
    assert!(broker.has_partition(topic, 0), "partition never led");
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

/// Creates `__share_group_state` in advance, as a KIP-932 client would create
/// it lazily through `FindCoordinator(SHARE)`. It then waits until every state
/// partition is local, so the share coordinator can accept writes before the
/// JVM consumer drives `ShareFetch` and `ShareAcknowledge`.
async fn bootstrap_share_state(broker: &BrokerHandle, client: &Client, key: &str) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2, // SHARE
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE) error: {}",
        resp.coordinators[0].error_code
    );
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Produces the supplied `values` as one batch into `(topic, 0)`. It retries
/// while the new partition still materializes its leader.
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

/// Runs a docker container against the host broker and returns its output. The
/// share consumer exits with a non-zero status on an idle timeout, even after
/// it consumed records, so callers check stdout and not the exit status.
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

/// The main differential test. A real JVM `KafkaShareConsumer` joins a fresh
/// Crabka share group, reads every produced record, and acknowledges
/// implicitly on poll. The test asserts that each produced value appears in
/// its stdout.
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
        assert!(
            stdout.contains(v),
            "JVM share consumer must read produced value {v:?}; got stdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&consumed.stderr),
        );
    }
}

/// `kafka-share-groups.sh --describe --state` reports the share group after
/// the JVM consumer joined. That proves Crabka serves the share-group admin
/// path (`ShareGroupDescribe`, `api_key` 77) to the real JVM tooling. The tool
/// resolves the share coordinator, sends `ShareGroupDescribe`, and prints the
/// group's coordinator and state.
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
    assert!(
        state_out.contains(group),
        "share group {group} must appear in --describe --state output; got:\n{state_out}\nstderr:\n{}",
        String::from_utf8_lossy(&state.stderr),
    );
}

/// `kafka-share-groups.sh --list` drives `ListGroups` (`api_key` 16) with
/// `types_filter = ["share"]`. After a real JVM `KafkaShareConsumer` joins a
/// share group on the Crabka broker, the share group id must appear in the
/// tool's `--list` stdout.
///
/// Before the `ListGroups` share pass, the JVM tool's `types_filter=["share"]`
/// matched nothing and `--list` was EMPTY. This test asserts that the
/// regression is closed against the real Apache Kafka 4.1.0 tool.
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
    assert!(
        list_out.contains(group),
        "share group {group} must appear in --list output; got:\n{list_out}\nstderr:\n{}",
        String::from_utf8_lossy(&listed.stderr),
    );
}
