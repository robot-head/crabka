#![allow(clippy::pedantic)]

//! JVM differential / interop test for KIP-1071 streams groups (the Streams
//! Rebalance Protocol).
//!
//! Drives the REAL Apache Kafka 4.1.0 `kafka-streams-groups.sh` admin tool
//! (a `KafkaStreamsGroupsCommand` wrapping the JVM `AdminClient`) inside an
//! `mirror.gcr.io/apache/kafka:4.1.0` container against an in-process Crabka broker running
//! on the host. The container has a JRE-only Kafka image (no `javac`/`jshell`),
//! so we cannot compile a custom KafkaStreams app; instead we use the native
//! `crabka-client-core` client to make a streams group EXIST on Crabka (finalize
//! `streams.version=1`, create a source topic, drive a `StreamsGroupHeartbeat`
//! so the group has a live member with an assignment), then point the bundled
//! JVM admin tool at Crabka and prove it round-trips the streams-group admin
//! wire path. The flow the real `apache-kafka-java` 4.1.0 `AdminClient` drives
//! (read empirically from its DEBUG wire log) is:
//!
//! - `ApiVersions` negotiation (key 18) — Crabka advertises api keys 88/89 and
//!   the finalized `streams.version` feature,
//! - `Metadata` (key 13) to discover the broker set,
//! - `ListGroups` (key 16, v5) with `typesFilter=[Streams]` — the KIP-1071
//!   `ListGroupsOptions.forStreamsGroups()` filter; Crabka returns the live
//!   streams group,
//! - `FindCoordinator` (key 10) for the group,
//! - `StreamsGroupDescribe` (key 89) — Crabka returns the full `DescribedGroup`
//!   (group state/epochs, the resolved topology, and the member with its active
//!   task assignment), which the JVM `DescribeStreamsGroupsHandler` accepts.
//!
//! Gated `#[ignore = "requires Docker"]`; run with `--ignored`.
//!
//! Networking mirrors `jvm_share_groups.rs`: the broker binds `0.0.0.0:9092`
//! and advertises `host.docker.internal:9092`; the container reaches it via
//! `--add-host=host.docker.internal:host-gateway`.

use std::{
    process::{Command, Stdio},
    time::Duration,
};

use assert2::{assert, check};
use crabka_broker::{Broker, BrokerConfig, BrokerHandle};
use crabka_client_core::Client;
use crabka_log::LogConfig;
use crabka_protocol::owned::{
    common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    streams_group_heartbeat_request::{StreamsGroupHeartbeatRequest, Subtopology, Topology},
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
};

/// Port the broker binds on the host and that the container reaches via
/// `host.docker.internal`.
const HOST_PORT: u16 = 9092;
const BOOTSTRAP: &str = "host.docker.internal:9092";
const LISTEN: &str = "0.0.0.0:9092";
/// Official Apache Kafka image. Ships KIP-1071 streams groups plus the
/// `kafka-streams-groups.sh` admin tool (StreamsGroupDescribe / ListGroups).
const KAFKA_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.1.0";
const STREAMS_GROUPS: &str = "/opt/kafka/bin/kafka-streams-groups.sh";
/// Kafka `COORDINATOR_LOAD_IN_PROGRESS` — the first-join heartbeat is retried
/// while the coordinator is still loading.
const ERR_COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

/// Boot one broker bound to `0.0.0.0:9092`, advertising `host.docker.internal:
/// 9092` so the Docker container's post-Metadata connect targets a hostname it
/// can resolve. Mirrors `jvm_share_groups.rs::start_host_broker`.
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

/// Native client connecting to the broker's local loopback listener (the
/// container reaches the same broker via `host.docker.internal`).
async fn connect() -> Client {
    Client::builder()
        .bootstrap(format!("127.0.0.1:{HOST_PORT}"))
        .client_id("crabka-streams-test")
        .build()
        .await
        .expect("client build")
}

/// Create `topic` (`partitions` partitions) and wait until this broker leads
/// partition 0.
async fn create_topic(broker: &BrokerHandle, client: &Client, topic: &str, partitions: i32) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
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
    assert!(broker.has_partition(topic, 0).await, "partition never led");
}

/// Finalize `streams.version` to level 1 so the heartbeat/describe handlers
/// stop returning `UNSUPPORTED_VERSION`. `upgrade_type: 1` is UPGRADE.
async fn finalize_streams_version(client: &Client) {
    let resp = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "streams.version".into(),
                max_version_level: 1,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        resp.error_code == 0,
        "streams.version finalize failed: {resp:?}"
    );
}

/// A single-subtopology topology subscribing to one source topic (stateless).
fn topology(source_topic: &str) -> Topology {
    Topology {
        epoch: 0,
        subtopologies: vec![Subtopology {
            subtopology_id: "0".into(),
            source_topics: vec![source_topic.into()],
            state_changelog_topics: vec![],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// First-join heartbeat: empty member id (server mints one), epoch 0,
/// process id + rebalance timeout + the supplied topology.
fn first_join(group: &str, topo: Topology) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: String::new(),
        member_epoch: 0,
        process_id: Some("p1".into()),
        rebalance_timeout_ms: 30_000,
        topology: Some(topo),
        ..Default::default()
    }
}

/// Follow-up heartbeat: known member id + its current epoch, echoing back the
/// owned active tasks (as a steady-state member would).
fn follow_up(
    group: &str,
    member_id: &str,
    epoch: i32,
    active: Option<Vec<ReqTaskIds>>,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group.into(),
        member_id: member_id.into(),
        member_epoch: epoch,
        active_tasks: active,
        ..Default::default()
    }
}

/// Sum of all active-task partitions in a heartbeat response.
fn active_partition_count(resp: &StreamsGroupHeartbeatResponse) -> usize {
    resp.active_tasks
        .as_ref()
        .map(|v| v.iter().map(|t| t.partitions.len()).sum())
        .unwrap_or(0)
}

/// Drive a single member to its first join, then re-heartbeat until it owns
/// `want_active` partitions (steady state). Returns the minted `member_id`.
async fn join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    let mut resp = client
        .send(first_join(group, topo.clone()))
        .await
        .expect("first heartbeat");
    let mut member_id = resp.member_id.clone();

    for _ in 0..tries {
        // COORDINATOR_LOAD_IN_PROGRESS: retry the first join.
        if resp.error_code == ERR_COORDINATOR_LOAD_IN_PROGRESS {
            resp = client
                .send(first_join(group, topo.clone()))
                .await
                .expect("retry first heartbeat");
            member_id = resp.member_id.clone();
            continue;
        }
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        if active_partition_count(&resp) >= want_active {
            break;
        }
        // intentional: streams-group task assignment is coordinator-local state,
        // not in the metadata image and exposed by no metric — bounded backoff
        // between heartbeat RPCs is the only way to observe convergence.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let active = resp.active_tasks.clone().map(|v| {
            v.into_iter()
                .map(|t| ReqTaskIds {
                    subtopology_id: t.subtopology_id,
                    partitions: t.partitions,
                    ..Default::default()
                })
                .collect()
        });
        resp = client
            .send(follow_up(group, &member_id, resp.member_epoch, active))
            .await
            .expect("follow-up heartbeat");
        member_id = resp.member_id.clone();
    }
    (member_id, resp)
}

/// Heartbeat once more to keep the live member's session fresh while the JVM
/// admin tool runs (so the group stays non-Empty with a member + assignment).
async fn keepalive(client: &Client, group: &str, member_id: &str, epoch: i32) {
    let active = Some(vec![ReqTaskIds {
        subtopology_id: "0".into(),
        partitions: vec![0, 1],
        ..Default::default()
    }]);
    let _ = client
        .send(follow_up(group, member_id, epoch, active))
        .await;
}

/// Run a docker container against the host broker and return its output. The
/// admin tool may exit non-zero even on a successful round-trip (mirrors the
/// `jvm_share_groups.rs` note), so callers check stdout, not exit status.
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

/// A DEBUG-level log4j2 config (written into the container at `/tmp/d.yaml`) so
/// the JVM tool's `NetworkClient`/`KafkaAdminClient` logs every request +
/// response. The tool's own stdout is empty when no streams group surfaces (see
/// the test below), so the wire-level interop checkpoints we assert on are read
/// from these DEBUG lines, captured via `2>&1`.
// NOTE: YAML is indentation-sensitive, so this is written WITHOUT Rust
// line-continuation (`\` at EOL eats the next line's leading spaces). Each
// `\n` is a real newline and the two/four/six-space indents are literal.
const TOOL_DEBUG_PREAMBLE: &str = concat!(
    "cat > /tmp/d.yaml <<'YAML'\n",
    "Configuration:\n",
    "  Appenders:\n",
    "    Console:\n",
    "      name: STDERR\n",
    "      target: SYSTEM_ERR\n",
    "      PatternLayout:\n",
    "        Pattern: \"%d %p %c %m%n\"\n",
    "  Loggers:\n",
    "    Root:\n",
    "      level: DEBUG\n",
    "      AppenderRef:\n",
    "        ref: STDERR\n",
    "YAML\n",
    "export KAFKA_LOG4J_OPTS='-Dlog4j2.configurationFile=/tmp/d.yaml'\n",
);

/// The headline differential test: make a KIP-1071 streams group live on Crabka
/// via the native `crabka-client-core` client (`StreamsGroupHeartbeat`, api 88),
/// then drive the REAL Apache Kafka 4.1.0 `kafka-streams-groups.sh` admin tool
/// (the JVM `StreamsGroupCommand` wrapping `AdminClient`) against Crabka and
/// prove it round-trips the streams-group admin wire path.
///
/// We assert these checkpoints, read from the JVM tool's own DEBUG wire logs:
///
///  1. The JVM `AdminClient` negotiated `ApiVersions` with Crabka and the
///     response advertised `StreamsGroupDescribe (apiKey=89)` plus the finalized
///     `streams.version` feature (so the KIP-1071 admin surface is visible to
///     the real client).
///  2. The tool issued `LIST_GROUPS apiVersion=5` with `typesFilter=[Streams]`
///     (the KIP-1071 `ListGroupsOptions.forStreamsGroups()` filter) and Crabka
///     answered with the live streams group (`errorCode=0`).
///  3. `StreamsGroupCommand.describeGroups()` then resolved the coordinator and
///     issued `StreamsGroupDescribe` (api 89); Crabka returned the full group —
///     state/epochs, the resolved topology, and the member with its active task
///     assignment — which the JVM `DescribeStreamsGroupsHandler` accepts (it
///     rejects a describe whose topology is absent, so the topology must be
///     populated; checkpoint 3 guards against regressing that).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn jvm_streams_groups_admin_round_trips_crabka() {
    let (broker, _dir) = start_host_broker().await;
    let topic = "streams-input";
    let group = "jvm-streams-g";

    let client = connect().await;
    finalize_streams_version(&client).await;
    create_topic(&broker, &client, topic, 2).await;

    // Make a streams group EXIST on Crabka: a lone member owns both partitions
    // of the single subtopology over `streams-input` (native StreamsGroupHeartbeat
    // / api 88).
    let (member_id, resp) = join_and_converge(&client, group, topology(topic), 2, 12).await;
    check!(
        resp.error_code == 0,
        "lone member must join cleanly, got member_id={member_id:?}, {resp:?}"
    );
    check!(
        !member_id.is_empty(),
        "lone member must get a broker-minted member id, got member_id={member_id:?}, {resp:?}"
    );
    check!(
        active_partition_count(&resp) == 2,
        "lone member must own both input partitions, got member_id={member_id:?}, {resp:?}"
    );
    let epoch = resp.member_epoch;
    keepalive(&client, group, &member_id, epoch).await;

    // Drive the JVM tool with DEBUG wire logging so we can read the actual
    // request/response frames it exchanges with Crabka. `--describe` exercises
    // the full KIP-1071 admin flow: ApiVersions (18) -> Metadata (13) ->
    // ListGroups (16, typesFilter=[Streams]) -> [StreamsGroupDescribe (89)].
    let described = docker_run(&[
        "bash",
        "-c",
        &format!(
            "{TOOL_DEBUG_PREAMBLE}\
             {STREAMS_GROUPS} --bootstrap-server {BOOTSTRAP} --describe --group {group} 2>&1; \
             echo EXIT=$?"
        ),
    ]);
    // With `2>&1` the DEBUG wire log lands on the container's stdout.
    let wire = String::from_utf8_lossy(&described.stdout);
    eprintln!("CRABKA[test] streams-groups --describe (DEBUG wire log):\n{wire}");

    // Checkpoint 1: the ApiVersions handshake with Crabka advertised the
    // KIP-1071 StreamsGroupDescribe API (apiKey=89) and the finalized
    // streams.version feature — the streams-group admin surface is visible to a
    // real Apache Kafka 4.1.0 AdminClient.
    //
    // Checkpoint 2: the tool issued the KIP-1071 streams-group LIST_GROUPS
    // request (typesFilter=[Streams]) and Crabka answered it cleanly. This is
    // the real JVM streams-group admin client round-tripping against Crabka.
    //
    // Checkpoint 3: the streams group now surfaces in Crabka's ListGroups reply,
    // so `StreamsGroupCommand.describeGroups()` proceeds past `listStreamsGroups()`,
    // resolves the coordinator, and issues the KIP-1071 `StreamsGroupDescribe`
    // (api 89). Crabka answers with the full group — topology + member + active
    // task assignment — completing the JVM admin round-trip end to end. The
    // describe response must carry the resolved topology ("missing the topology
    // information" must NOT appear) — the real JVM `DescribeStreamsGroupsHandler`
    // logs an ERROR and rejects a describe whose topology is absent.
    let group_needle = format!("groupId='{group}'");
    // (needle, expected presence in the DEBUG wire log)
    let cases = [
        // Checkpoint 1.
        ("Received API_VERSIONS response", true),
        ("apiKey=89", true),
        ("FinalizedFeatureKey(name='streams.version'", true),
        // Checkpoint 2.
        ("Sending LIST_GROUPS request", true),
        ("typesFilter=[Streams]", true),
        ("Received LIST_GROUPS response", true),
        ("errorCode=0", true),
        // Checkpoint 3. The describe response must carry the resolved topology,
        // so "missing the topology information" must NOT appear.
        ("Received STREAMS_GROUP_DESCRIBE response", true),
        ("missing the topology information", false),
        ("subtopologyId='0'", true),
        (group_needle.as_str(), true),
    ];
    for (needle, expected) in cases {
        assert!(
            wire.contains(needle) == expected,
            "JVM streams-group admin round-trip checkpoint failed: wire log must {} \
             {needle:?}; wire log:\n{wire}",
            if expected { "contain" } else { "not contain" },
        );
    }
}
