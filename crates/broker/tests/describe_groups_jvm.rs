#![allow(clippy::pedantic)]

//! cp/JVM cross-validation for `DescribeGroups` (`api_key=15`) metadata.
//!
//! Boots a single-node real Kafka (`mirror.gcr.io/confluentinc/cp-kafka:7.4.0`, `KRaft`) in
//! Docker, forms a CLASSIC consumer group `g` with the `RangeAssignor`, then
//! sends a `DescribeGroupsRequest` to the real broker FROM THE HOST via
//! `crabka_client_core::Client` and captures the response. cp/JVM is the
//! authority: this proves the spec premise that real Kafka populates the
//! fields Crabka's handler now surfaces (see `describe_groups_metadata.rs` for
//! the in-process byte-exact echo, and the calibration cross-check below):
//!
//!   * `protocol_type == "consumer"` for an active classic consumer group;
//!   * `protocol_data == "range"` — the SELECTED assignor name, NON-empty;
//!   * `member_metadata` NON-empty — the encoded `ConsumerProtocolSubscription`
//!     the consumer sent in its `JoinGroup`;
//!   * a TYPELESS group (offset-commit-only, never had a protocol) reports
//!     `protocol_type == ""`, settling the `unwrap_or_default()` projection in
//!     `handlers/describe_groups.rs`.
//!
//! Scope: CLASSIC groups only. cp-kafka 7.4.0 is Kafka 3.4 server-side, which
//! predates KIP-848 — a next-gen (consumer-protocol) group's `member_metadata`
//! via classic `DescribeGroups` needs a next-gen-capable image (Kafka 3.7+) and
//! is deferred.
//!
//! The capture is written to
//! `tests/fixtures/describe_groups/real_kafka_classic.json` (string fields
//! verbatim, byte fields as hex + UTF-8-lossy). Re-running regenerates it.
//!
//! ```text
//! cargo test -p crabka-broker --test describe_groups_jvm -- --ignored --nocapture
//! ```
//!
//! Networking: cp-kafka publishes two PLAINTEXT listeners — `PLAINTEXT` on
//! `9092` advertised as `localhost:9092` for the in-container admin/consumer
//! CLI, and `EXTERNAL` on `19092` advertised as `localhost:19092`, published to
//! host port 19092 so the host `Client` reaches it on `127.0.0.1:19092`. A
//! single `DescribeGroups` is served by the sole broker (it is the group
//! coordinator), so no `FindCoordinator` redirect is needed.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crabka_client_core::Client;
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;
use crabka_protocol::owned::describe_groups_response::DescribeGroupsResponse;

const KAFKA_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";
const CONTAINER: &str = "crabka-describe-groups-jvm";
/// Fixed host port the `EXTERNAL` listener is published on.
const HOST_PORT: u16 = 19092;
const HOST_BOOTSTRAP: &str = "127.0.0.1:19092";
/// Stable classic consumer group.
const GROUP: &str = "g";
/// Offset-commit-only group — never carries a protocol type.
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

/// JSON-ify one described group: string fields verbatim, member byte fields as
/// hex + UTF-8-lossy so the fixture is both diffable and human-readable.
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

/// Boot single-node cp-kafka in KRaft mode with the dual listener layout. The
/// `EXTERNAL` listener is published to the fixed host port so the host `Client`
/// can dial it directly.
fn docker_run_kafka() {
    docker_rm_f(CONTAINER);
    let out = Command::new("docker")
        .args([
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
            KAFKA_IMAGE,
        ])
        .output()
        .expect("spawn docker run kafka");
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

/// Run a command inside the broker container, returning its `Output`.
fn exec(args: &[&str]) -> std::process::Output {
    Command::new("docker")
        .arg("exec")
        .arg(CONTAINER)
        .args(args)
        .output()
        .expect("spawn docker exec")
}

/// Detach a long-running command inside the container (`docker exec -d`).
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

fn wait_for_broker() {
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        if exec(&[
            "kafka-broker-api-versions",
            "--bootstrap-server",
            "localhost:9092",
        ])
        .status
        .success()
        {
            eprintln!("CAPTURE broker READY");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "cp-kafka never became ready within 120s.\ncontainer logs:\n{}",
        docker_logs()
    );
}

/// Wait until classic group `g` reports `Stable` with a member, via the JVM
/// admin tool (`kafka-consumer-groups --describe --state`).
fn wait_for_group_stable() {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = String::new();
    while Instant::now() < deadline {
        let out = exec(&[
            "kafka-consumer-groups",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            GROUP,
            "--state",
        ]);
        last = String::from_utf8_lossy(&out.stdout).into_owned();
        if last.contains("Stable") {
            eprintln!("CAPTURE group {GROUP} STABLE:\n{last}");
            return;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
    panic!(
        "group {GROUP} never reached Stable within 60s.\nlast --state:\n{last}\nlogs:\n{}",
        docker_logs()
    );
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures real cp-kafka DescribeGroups (api_key=15) metadata"]
async fn capture_real_kafka_describe_groups() {
    docker_pull(KAFKA_IMAGE);
    docker_run_kafka();
    let _guard = ContainerGuard;
    wait_for_broker();

    // Topic with 2 partitions so the classic assignment is non-trivial.
    let out = exec(&[
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
        out.status.success(),
        "create topic failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // Produce a few records so the consumer actually has data + an assignment.
    let out = exec(&[
        "bash",
        "-c",
        &format!(
            "printf 'r1\\nr2\\nr3\\nr4\\n' | kafka-console-producer --bootstrap-server localhost:9092 --topic {TOPIC}"
        ),
    ]);
    assert!(
        out.status.success(),
        "produce failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // ── A classic consumer group `g` with the RangeAssignor. Detached with a
    // generous read timeout so the group stays Stable across the capture. ──
    exec_detached(&format!(
        "kafka-console-consumer --bootstrap-server localhost:9092 --topic {TOPIC} --group {GROUP} \
         --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor \
         --from-beginning --timeout-ms 180000 > /tmp/consumer.out 2>&1"
    ));
    wait_for_group_stable();

    // ── A TYPELESS group: an offset-commit-only ("simple") group that never
    // joined with a protocol. cp reports protocol_type == "" for it. ──
    let out = exec(&[
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
        out.status.success(),
        "create typeless group failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // ── Capture from the HOST via crabka_client_core::Client. ──
    let client = Client::builder()
        .bootstrap(HOST_BOOTSTRAP)
        .client_id("cap")
        .build()
        .await
        .expect("client build against real kafka");
    let resp: DescribeGroupsResponse = client
        .send(DescribeGroupsRequest {
            groups: vec![GROUP.to_string(), TYPELESS_GROUP.to_string()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("DescribeGroups must round-trip against real kafka");
    client.close();

    // ── Persist the fixture (the oracle). ──
    let classic = resp
        .groups
        .iter()
        .find(|g| g.group_id == GROUP)
        .unwrap_or_else(|| panic!("group {GROUP} missing from response: {resp:?}"));
    let typeless = resp
        .groups
        .iter()
        .find(|g| g.group_id == TYPELESS_GROUP)
        .unwrap_or_else(|| panic!("group {TYPELESS_GROUP} missing from response: {resp:?}"));
    let fixture = serde_json::json!({
        "provenance": {
            "image": KAFKA_IMAGE,
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

    // ── Report the captured authority values verbatim. ──
    eprintln!(
        "CAPTURE classic g: state={:?} protocol_type={:?} protocol_data={:?} members={}",
        classic.group_state,
        classic.protocol_type,
        classic.protocol_data,
        classic.members.len()
    );
    let m = classic
        .members
        .first()
        .unwrap_or_else(|| panic!("classic group {GROUP} has no member: {classic:?}"));
    eprintln!(
        "CAPTURE classic member: meta_len={} meta_head={:02x?} assign_len={} assign_head={:02x?}",
        m.member_metadata.len(),
        &m.member_metadata[..m.member_metadata.len().min(16)],
        m.member_assignment.len(),
        &m.member_assignment[..m.member_assignment.len().min(16)],
    );
    eprintln!(
        "CAPTURE typeless {TYPELESS_GROUP}: state={:?} protocol_type={:?} protocol_data={:?}",
        typeless.group_state, typeless.protocol_type, typeless.protocol_data
    );

    // ── ASSERT the real-Kafka authority semantics (the spec premise). ──
    assert_eq!(
        classic.error_code, 0,
        "classic group describe error: {classic:?}"
    );
    assert_eq!(
        classic.protocol_type, "consumer",
        "real Kafka: an active classic consumer group reports protocol_type=='consumer'"
    );
    assert_eq!(
        classic.protocol_data, "range",
        "real Kafka populates the SELECTED protocol name (the RangeAssignor's 'range')"
    );
    assert_eq!(
        classic.group_state, "Stable",
        "classic group must be Stable"
    );
    assert_eq!(classic.members.len(), 1, "exactly one member: {classic:?}");
    assert!(
        !m.member_metadata.is_empty(),
        "real Kafka populates member_metadata (the ConsumerProtocolSubscription); got empty"
    );
    // The subscription begins with the ConsumerProtocol version (i16) followed
    // by the subscribed-topics array. cp-kafka 7.4.0 emits version 3.
    assert_eq!(
        &m.member_metadata[..2],
        &[0x00, 0x03],
        "ConsumerProtocolSubscription header version (i16=3); got {:02x?}",
        &m.member_metadata[..m.member_metadata.len().min(2)]
    );

    // ── The typeless-group authority: protocol_type == "" (settles the
    // unwrap_or_default() projection in handlers/describe_groups.rs). ──
    assert_eq!(
        typeless.error_code, 0,
        "typeless group describe error: {typeless:?}"
    );
    assert_eq!(
        typeless.protocol_type, "",
        "real Kafka: an offset-commit-only / typeless group reports protocol_type==\"\""
    );
    assert_eq!(
        typeless.protocol_data, "",
        "typeless group has no selected protocol"
    );
}
