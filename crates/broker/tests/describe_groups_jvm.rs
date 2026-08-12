//! JVM Kafka cross-validation for `DescribeGroups` (`api_key=15`) metadata.
//!
//! The test boots two single-node real Kafka brokers in Docker, one at a time.
//! `mirror.gcr.io/confluentinc/cp-kafka:7.4.0` forms a CLASSIC consumer group
//! with the `RangeAssignor`. `mirror.gcr.io/apache/kafka:4.0.0` forms a
//! next-generation consumer group with `group.protocol=consumer`. The host
//! sends `DescribeGroupsRequest` to each broker with
//! `crabka_client_core::Client` and captures the responses. JVM Kafka is the
//! authority. This proves the spec premise that real Kafka populates the fields
//! Crabka's handler now surfaces. See `describe_groups_metadata.rs` for the
//! in-process byte-exact echo, and the calibration cross-check below:
//!
//!   * `protocol_type == "consumer"` for an active classic consumer group;
//!   * `protocol_data == "range"`, the SELECTED assignor name, NON-empty;
//!   * `member_metadata` NON-empty: the encoded `ConsumerProtocolSubscription`
//!     the consumer sent in its `JoinGroup`;
//!   * a TYPELESS group, which only commits offsets and never had a protocol,
//!     reports `protocol_type == ""`. This settles the `unwrap_or_default()`
//!     projection in `handlers/describe_groups.rs`.
//!   * the classic API rejects a live next-generation group with
//!     `GROUP_ID_NOT_FOUND`: its state is `Dead` and its classic protocol and
//!     member projections are empty. Next-generation clients must use
//!     `ConsumerGroupDescribe` (`api_key=69`).
//!
//! The test writes the capture to
//! `tests/fixtures/describe_groups/real_kafka_{classic,next_gen}.json`. String
//! fields are verbatim and byte fields are hex plus UTF-8-lossy. A new run
//! regenerates both files.
//!
//! ```text
//! cargo test -p crabka-broker --test describe_groups_jvm -- --ignored --nocapture
//! ```
//!
//! Networking: each Kafka container publishes two PLAINTEXT listeners. `PLAINTEXT` on
//! `9092` is advertised as `localhost:9092` for the in-container admin and
//! consumer CLI. `EXTERNAL` on `19092` is advertised as `localhost:19092` and
//! published to host port 19092, so the host `Client` reaches it on
//! `127.0.0.1:19092`. The sole broker serves a single `DescribeGroups` because
//! it is the group coordinator, so no `FindCoordinator` redirect is needed.

use std::{
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use assert2::assert;
use crabka_client_core::Client;
use crabka_protocol::owned::{
    describe_groups_request::DescribeGroupsRequest,
    describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
};

const CLASSIC_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";
const NEXT_GEN_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";
const CONTAINER: &str = "crabka-describe-groups-jvm";
/// Fixed host port the `EXTERNAL` listener is published on.
const HOST_PORT: u16 = 19092;
const HOST_BOOTSTRAP: &str = "127.0.0.1:19092";
/// Stable classic consumer group.
const GROUP: &str = "g";
/// Next-generation consumer-protocol group.
const NEXT_GEN_GROUP: &str = "g-next";
/// Offset-commit-only group. It never carries a protocol type.
const TYPELESS_GROUP: &str = "simple-typeless";
const TOPIC: &str = "t";

// ── fixture ─────────────────────────────────────────────────────────────────

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("describe_groups")
}

fn write_fixture(name: &str, body: &str) {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create dir {}: {e}", dir.display()));
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
    eprintln!("CAPTURE wrote {} ({} bytes)", path.display(), body.len());
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Convert one described group to JSON.
///
/// String fields stay verbatim. Member byte fields become hex plus UTF-8-lossy,
/// so the fixture is both diffable and readable.
fn group_json(
    g: &crabka_protocol::owned::describe_groups_response::DescribedGroup,
) -> serde_json::Value {
    let members: Vec<serde_json::Value> = g
        .members
        .iter()
        .map(|m| {
            serde_json::json!({
                "member_id": m.member_id,
                "client_id": m.client_id,
                "client_host": m.client_host,
                "member_metadata_len": m.member_metadata.len(),
                "member_metadata_hex": hex(&m.member_metadata),
                "member_metadata_lossy": String::from_utf8_lossy(&m.member_metadata),
                "member_assignment_len": m.member_assignment.len(),
                "member_assignment_hex": hex(&m.member_assignment),
                "member_assignment_lossy": String::from_utf8_lossy(&m.member_assignment),
            })
        })
        .collect();
    serde_json::json!({
        "group_id": g.group_id,
        "error_code": g.error_code,
        "group_state": g.group_state,
        "protocol_type": g.protocol_type,
        "protocol_data": g.protocol_data,
        "members": members,
    })
}

// ── docker helpers ────────────────────────────────────────────────────────────

fn docker_pull(image: &str) {
    eprintln!("CAPTURE docker pull {image} (large; may take minutes)...");
    let out = Command::new("docker")
        .args(["pull", image])
        .output()
        .expect("spawn docker pull");
    assert!(
        out.status.success(),
        "docker pull {image} failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

fn docker_rm_f(name: &str) {
    let _ = Command::new("docker").args(["rm", "-f", name]).output();
}

/// Boot single-node Kafka in `KRaft` mode with the dual listener layout.
///
/// Docker publishes the `EXTERNAL` listener to the fixed host port, so the host
/// `Client` can dial it directly.
fn docker_run_kafka(image: &str, enable_consumer_protocol: bool) {
    docker_rm_f(CONTAINER);
    let mut command = Command::new("docker");
    command.args([
            "run",
            "-d",
            "--name",
            CONTAINER,
            "-p",
            &format!("{HOST_PORT}:{HOST_PORT}"),
            "-e",
            "KAFKA_NODE_ID=1",
            "-e",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "-e",
            &format!(
                "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:{HOST_PORT},CONTROLLER://0.0.0.0:9093"
            ),
            "-e",
            &format!(
                "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://localhost:{HOST_PORT}"
            ),
            "-e",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "-e",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "-e",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            "-e",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "-e",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "-e",
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
        ]);
    if enable_consumer_protocol {
        command.args([
            "-e",
            "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer",
        ]);
    }
    let out = command.arg(image).output().expect("spawn docker run kafka");
    assert!(
        out.status.success(),
        "docker run kafka failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    eprintln!(
        "CAPTURE kafka container started id={}",
        String::from_utf8_lossy(&out.stdout).trim()
    );
}

/// Run a command inside the broker container and return its `Output`.
fn exec(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .arg("exec")
        .arg(CONTAINER)
        .args(args)
        .output()
        .expect("spawn docker exec")
}

/// Detach a long-running command inside the container with `docker exec -d`.
fn exec_detached(script: &str) {
    let out = Command::new("docker")
        .args(["exec", "-d", CONTAINER, "bash", "-c", script])
        .output()
        .expect("spawn docker exec -d");
    assert!(
        out.status.success(),
        "docker exec -d failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
}

fn docker_logs() -> String {
    let out = Command::new("docker")
        .args(["logs", CONTAINER])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    )
}

struct ContainerGuard;
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(CONTAINER);
        eprintln!("CAPTURE removed container {CONTAINER}");
    }
}

// ── readiness + group setup (in-container CLI on the PLAINTEXT listener) ─────────

fn wait_for_broker(api_versions_tool: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
    while Instant::now() < deadline {
        if exec(&[api_versions_tool, "--bootstrap-server", "localhost:9092"])
            .status
            .success()
        {
            eprintln!("CAPTURE broker READY");
            return;
        }
        // intentional: polls the external JVM cp-kafka container via its admin CLI; no in-process crabka metric/image to await.
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "Kafka never became ready within 120s.\ncontainer logs:\n{}",
        docker_logs()
    );
}

/// Wait until a group reports `Stable` with a member.
///
/// The check uses the JVM admin tool `kafka-consumer-groups --describe
/// --state`.
fn wait_for_group_stable(group: &str, consumer_groups_tool: &str) {
    let deadline = Instant::now() + Duration::from_mins(1);
    let mut last = String::new();
    while Instant::now() < deadline {
        let out = exec(&[
            consumer_groups_tool,
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            group,
            "--state",
        ]);
        last = String::from_utf8_lossy(&out.stdout).into_owned();
        if last.contains("Stable") {
            eprintln!("CAPTURE group {group} STABLE:\n{last}");
            return;
        }
        // intentional: polls the external JVM broker's group state via kafka-consumer-groups; no in-process crabka metric/image to await.
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "group {group} never reached Stable within 60s.\nlast --state:\n{last}\nlogs:\n{}",
        docker_logs()
    );
}

fn prepare_classic_groups() {
    let created = exec(&[
        "kafka-topics",
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        TOPIC,
        "--partitions",
        "2",
        "--replication-factor",
        "1",
    ]);
    assert!(
        created.status.success(),
        "create topic failed: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    let produced = exec(&[
        "bash",
        "-c",
        &format!(
            "printf 'r1\\nr2\\nr3\\nr4\\n' | kafka-console-producer --bootstrap-server localhost:9092 --topic {TOPIC}"
        ),
    ]);
    assert!(
        produced.status.success(),
        "produce failed: {}",
        String::from_utf8_lossy(&produced.stderr)
    );
    exec_detached(&format!(
        "kafka-console-consumer --bootstrap-server localhost:9092 --topic {TOPIC} --group {GROUP} \
         --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor \
         --from-beginning --timeout-ms 180000 > /tmp/consumer.out 2>&1"
    ));
    wait_for_group_stable(GROUP, "kafka-consumer-groups");

    let typeless = exec(&[
        "kafka-consumer-groups",
        "--bootstrap-server",
        "localhost:9092",
        "--group",
        TYPELESS_GROUP,
        "--topic",
        TOPIC,
        "--reset-offsets",
        "--to-earliest",
        "--execute",
    ]);
    assert!(
        typeless.status.success(),
        "create typeless group failed: {}",
        String::from_utf8_lossy(&typeless.stderr)
    );
}

fn prepare_next_gen_group() {
    let created = exec(&[
        "/opt/kafka/bin/kafka-topics.sh",
        "--create",
        "--if-not-exists",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        TOPIC,
        "--partitions",
        "2",
        "--replication-factor",
        "1",
    ]);
    assert!(
        created.status.success(),
        "create topic failed: {}",
        String::from_utf8_lossy(&created.stderr)
    );

    let produced = exec(&[
        "bash",
        "-c",
        &format!(
            "printf 'r1\\nr2\\nr3\\nr4\\n' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic {TOPIC}"
        ),
    ]);
    assert!(
        produced.status.success(),
        "produce failed: {}",
        String::from_utf8_lossy(&produced.stderr)
    );
    exec_detached(&format!(
        "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic {TOPIC} \
         --group {NEXT_GEN_GROUP} --consumer-property group.protocol=consumer \
         --from-beginning --timeout-ms 180000 > /tmp/consumer.out 2>&1"
    ));
    wait_for_group_stable(NEXT_GEN_GROUP, "/opt/kafka/bin/kafka-consumer-groups.sh");
}

async fn describe_real_groups(groups: &[&str]) -> DescribeGroupsResponse {
    let client = Client::builder()
        .bootstrap(HOST_BOOTSTRAP)
        .client_id("cap")
        .build()
        .await
        .expect("client build against real kafka");
    let response = client
        .send(DescribeGroupsRequest {
            groups: groups.iter().map(|group| (*group).to_string()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("DescribeGroups against real kafka");
    client.close();
    response
}

fn assert_classic_group(classic: &DescribedGroup) {
    assert!(
        classic.error_code == 0,
        "classic group describe error: {classic:?}"
    );
    assert!(classic.protocol_type == "consumer");
    assert!(classic.protocol_data == "range");
    assert!(classic.group_state == "Stable");
    assert!(classic.members.len() == 1);
    let member = &classic.members[0];
    assert!(!member.member_metadata.is_empty());
    assert!(&member.member_metadata[..2] == [0x00, 0x03]);
}

fn persist_and_assert(response: &DescribeGroupsResponse) {
    let classic = response
        .groups
        .iter()
        .find(|group| group.group_id == GROUP)
        .unwrap_or_else(|| panic!("group {GROUP} missing: {response:?}"));
    let typeless = response
        .groups
        .iter()
        .find(|group| group.group_id == TYPELESS_GROUP)
        .unwrap_or_else(|| panic!("group {TYPELESS_GROUP} missing: {response:?}"));
    let fixture = serde_json::json!({
        "provenance": {
            "image": CLASSIC_IMAGE,
            "api_key": 15,
            "note": "Real cp-kafka DescribeGroups. cp/JVM is the authority for protocol_type / protocol_data / member_metadata.",
        },
        "classic_consumer_group": group_json(classic),
        "typeless_group": group_json(typeless),
    });
    write_fixture(
        "real_kafka_classic.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
    assert_classic_group(classic);
    assert!(typeless.error_code == 0);
    assert!(typeless.protocol_type == "");
    assert!(typeless.protocol_data == "");
}

fn persist_and_assert_next_gen(response: &DescribeGroupsResponse) {
    let next_gen = response
        .groups
        .iter()
        .find(|group| group.group_id == NEXT_GEN_GROUP)
        .unwrap_or_else(|| panic!("group {NEXT_GEN_GROUP} missing: {response:?}"));
    let fixture = serde_json::json!({
        "provenance": {
            "image": NEXT_GEN_IMAGE,
            "api_key": 15,
            "note": "Real Apache Kafka next-generation consumer-group DescribeGroups authority capture.",
        },
        "next_gen_consumer_group": group_json(next_gen),
    });
    write_fixture(
        "real_kafka_next_gen.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
    assert!(next_gen.error_code == crabka_broker::codes::GROUP_ID_NOT_FOUND);
    assert!(next_gen.error_message.as_deref() == Some("Group g-next is not a classic group."));
    assert!(next_gen.protocol_type.is_empty());
    assert!(next_gen.protocol_data.is_empty());
    assert!(next_gen.group_state == "Dead");
    assert!(next_gen.members.is_empty());
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures classic and next-generation real-Kafka DescribeGroups metadata"]
async fn capture_real_kafka_describe_groups() {
    docker_pull(CLASSIC_IMAGE);
    {
        docker_run_kafka(CLASSIC_IMAGE, false);
        let _guard = ContainerGuard;
        wait_for_broker("kafka-broker-api-versions");
        prepare_classic_groups();
        let response = describe_real_groups(&[GROUP, TYPELESS_GROUP]).await;
        persist_and_assert(&response);
    }

    docker_pull(NEXT_GEN_IMAGE);
    docker_run_kafka(NEXT_GEN_IMAGE, true);
    let _guard = ContainerGuard;
    wait_for_broker("/opt/kafka/bin/kafka-broker-api-versions.sh");
    prepare_next_gen_group();
    let response = describe_real_groups(&[NEXT_GEN_GROUP]).await;
    persist_and_assert_next_gen(&response);
}
