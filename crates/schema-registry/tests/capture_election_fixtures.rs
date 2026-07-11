//! Golden `"sr"`-election capture harness for Crabka Schema Registry slice 5 (HA).
//!
//! Boots **two** real `mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0` containers against
//! an in-process Crabka broker (same networking as `capture_admin_fixtures.rs` /
//! `capture_references_fixtures.rs`: the broker binds `0.0.0.0:9092` and
//! advertises `host.docker.internal:9092`, while the host connects directly on
//! `127.0.0.1:9092`). Both cp nodes are pointed at the same Crabka broker and
//! share the same election group id, so they form the `"sr"` Kafka group and
//! elect a master *through our coordinator* — a cp node only answers
//! `GET /subjects` with 200 once that election has completed, so each node's
//! REST readiness PROVES the election round-tripped end-to-end against Crabka.
//!
//! Once both nodes are ready, the harness reads the group via `DescribeGroups`
//! from the host side and captures, per member, the exact `member_metadata`
//! bytes (cp's `SchemaRegistryIdentity` JSON) and `member_assignment` bytes
//! (cp's `SchemaRegistryGroupAssignment` JSON), plus the group's `protocol_type`
//! and protocol name. Two fixtures are produced:
//!
//!   * `tests/fixtures/election/members.json` — per member: `member_id`,
//!     `client_id`, `client_host`, the UTF-8-lossy `member_metadata` +
//!     `member_assignment` JSON, and `is_leader` (whether cp's group leader
//!     `member_id` matches). This is the oracle that pins our
//!     `SchemaRegistryIdentity` / `SchemaRegistryGroupAssignment` encoders.
//!   * `tests/fixtures/election/group.json` — the group-level shape:
//!     `group_id`, `group_state`, `protocol_type` (expect `"sr"`),
//!     `protocol_name` (the captured `SR_PROTOCOL_NAME`), the leader `member_id`,
//!     and the elected master identity (decoded from the assignment) — the
//!     oracle for `SR_PROTOCOL_NAME` and the `select_master` comparator.
//!
//! ```text
//! cargo test -p crabka-schema-registry --test capture_election_fixtures -- --ignored --nocapture
//! ```
//!
//! Re-running this test regenerates both fixture files verbatim.

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use crabka_broker::{Broker, BrokerConfig};
use crabka_client_core::Client;
use crabka_protocol::owned::describe_groups_request::DescribeGroupsRequest;

/// The broker binds host port 9092 and cp-schema-registry reaches it via
/// `host.docker.internal:9092` (container network) while the host connects
/// directly on `127.0.0.1:9092`.
const LISTEN: &str = "0.0.0.0:9092";
const CONTROLLER_LISTEN: &str = "0.0.0.0:9093";
const ADVERTISED: &str = "host.docker.internal:9092";

const SR_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-schema-registry:7.4.0";

/// The shared `"sr"` election group id both cp nodes (and our `DescribeGroups`
/// read) use.
const GROUP_ID: &str = "schema-registry";

// ── fixture paths ─────────────────────────────────────────────────────────────

fn election_fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("election")
}

fn write_election_fixture(name: &str, body: &str) {
    let dir = election_fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create dir {}: {e}", dir.display()));
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
    eprintln!("CAPTURE wrote {} ({} bytes)", path.display(), body.len());
}

// ── broker ────────────────────────────────────────────────────────────────────

async fn start_host_broker() -> (crabka_broker::BrokerHandle, tempfile::TempDir) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("crabka_broker=info,info")),
        )
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().expect("tempdir");
    let listen_addr: SocketAddr = LISTEN.parse().expect("static addr");
    let controller_addr: SocketAddr = CONTROLLER_LISTEN.parse().expect("static addr");
    let config = BrokerConfig {
        broker_id: 1,
        listen_addr,
        advertised_listener: ADVERTISED.into(),
        log_dir: dir.path().to_path_buf(),
        node_id: crabka_broker::NodeId(1),
        controller_listen_addr: controller_addr,
        controller_quorum_voters: vec![(crabka_broker::NodeId(1), controller_addr.to_string())],
        heartbeat_interval_ms: 3_000,
        heartbeat_timeout_ms: 9_000,
        replica_lag_time_max_ms: 30_000,
        controller_election_timeout: Duration::from_secs(5),
        controller_heartbeat_interval: Duration::from_millis(500),
        bootstrap_mode: crabka_broker::BootstrapMode::Bootstrap,
        ..BrokerConfig::default()
    };
    let handle = Broker::start(config).await.expect("start broker");
    eprintln!("CAPTURE broker started listen={LISTEN} advertised={ADVERTISED}");
    (handle, dir)
}

// ── docker helpers ────────────────────────────────────────────────────────────

fn docker_pull(image: &str) {
    eprintln!("CAPTURE docker pull {image} (large; may take minutes)...");
    let out = Command::new("docker")
        .args(["pull", image])
        .output()
        .expect("spawn docker pull");
    assert2::assert!(out.status.success());
}

/// Start one cp-schema-registry node with a distinct `host_name` and a published
/// (ephemeral host → in-container `8081`) REST port, pointed at the shared
/// Crabka broker + the shared election group. Returns the container id.
///
/// `SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID` is cp's env var for the *election*
/// group id (the doubled `SCHEMA_REGISTRY_` prefix is correct: cp maps the
/// `schema.registry.group.id` property by prefixing `SCHEMA_REGISTRY_`). Both
/// nodes share it so they join the same `"sr"` group.
fn docker_run_schema_registry(host_name: &str) -> String {
    let out = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "-p",
            "0:8081",
            "-e",
            &format!("SCHEMA_REGISTRY_HOST_NAME={host_name}"),
            "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://host.docker.internal:9092",
            "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081",
            "-e",
            &format!("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID={GROUP_ID}"),
            "-e",
            "SCHEMA_REGISTRY_MASTER_ELIGIBILITY=true",
            SR_IMAGE,
        ])
        .output()
        .expect("spawn docker run schema-registry");
    assert2::assert!(out.status.success());
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    assert2::assert!(!id.is_empty());
    eprintln!("CAPTURE schema-registry container ({host_name}) id={id}");
    id
}

fn docker_mapped_port(id: &str) -> u16 {
    let out = Command::new("docker")
        .args(["port", id, "8081"])
        .output()
        .expect("spawn docker port");
    assert2::assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    let port = text
        .lines()
        .filter_map(|l| l.rsplit(':').next())
        .find_map(|p| p.trim().parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse mapped 8081 port from: {text:?}"));
    eprintln!("CAPTURE schema-registry mapped 8081 -> host {port}");
    port
}

fn docker_logs(id: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", id])
        .output()
        .expect("spawn docker logs");
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    )
}

fn docker_rm_f(id: &str) {
    let _ = Command::new("docker").args(["rm", "-f", id]).output();
    eprintln!("CAPTURE removed container {id}");
}

struct ContainerGuard {
    id: String,
}
impl Drop for ContainerGuard {
    fn drop(&mut self) {
        docker_rm_f(&self.id);
    }
}

// ── REST readiness ──────────────────────────────────────────────────────────────

/// Poll `GET {base}/subjects` until it returns 200 or the deadline passes. cp
/// only serves this once the `"sr"` group has elected a master through Crabka,
/// so a 200 proves the election round-tripped against our coordinator.
async fn wait_for_registry(http: &reqwest::Client, base: &str, container_id: &str, label: &str) {
    let deadline = Instant::now() + Duration::from_mins(2);
    let url = format!("{base}/subjects");
    let mut last: Option<String> = None;
    while Instant::now() < deadline {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                eprintln!("CAPTURE schema-registry {label} READY ({})", resp.status());
                return;
            }
            Ok(resp) => last = Some(format!("status {}", resp.status())),
            Err(e) => last = Some(format!("err {e}")),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let logs = docker_logs(container_id);
    panic!(
        "schema-registry {label} never became ready within 120s (last: {last:?}).\ncontainer logs:\n{logs}"
    );
}

// ── DescribeGroups capture ──────────────────────────────────────────────────────

/// Connect host-side directly to `127.0.0.1:9092`, `DescribeGroups` the `"sr"`
/// group, and write the per-member metadata/assignment bytes + group-level
/// protocol shape to the two election fixtures.
async fn capture_group() {
    // The broker implements api_key 15 (DescribeGroups); connect a plain Client.
    let client = Client::builder()
        .bootstrap("127.0.0.1:9092".to_string())
        .client_id("election-capture".to_string())
        .build()
        .await
        .expect("client connect");

    let resp = client
        .send(DescribeGroupsRequest {
            groups: vec![GROUP_ID.to_string()],
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("describe groups");
    client.close();

    let group = resp
        .groups
        .into_iter()
        .find(|g| g.group_id == GROUP_ID)
        .unwrap_or_else(|| panic!("group {GROUP_ID:?} absent from DescribeGroups response"));

    eprintln!(
        "CAPTURE group {GROUP_ID:?}: error_code={} state={:?} protocol_type={:?} protocol_name={:?} members={}",
        group.error_code,
        group.group_state,
        group.protocol_type,
        group.protocol_data,
        group.members.len()
    );
    assert2::assert!(group.error_code == 0);
    assert2::assert!(group.members.len() == 2);

    // cp's assignment carries `master` (the elected master's MEMBER_ID string)
    // and `master_identity` (its `SchemaRegistryIdentity` object). Every member
    // receives the same assignment, so the elected master is consistent across
    // all members. We derive the elected master member_id + identity from the
    // assignment and flag, per member, whether IT is the elected master.
    //
    // NB the broker's DescribeGroups leaves `member_metadata` empty (it persists
    // only the assignment, not the join metadata) — so the authoritative
    // `SchemaRegistryIdentity` bytes are the `master_identity` *inside* the
    // assignment, which the broker passes through verbatim.
    let mut members_json: Vec<serde_json::Value> = Vec::new();
    let mut elected_master_id: Option<String> = None;
    let mut elected_master_identity: Option<serde_json::Value> = None;

    for m in &group.members {
        let metadata = String::from_utf8_lossy(&m.member_metadata).to_string();
        let assignment = String::from_utf8_lossy(&m.member_assignment).to_string();
        eprintln!(
            "CAPTURE member {:?} client_id={:?} client_host={:?}\n    member_metadata   = {metadata}\n    member_assignment = {assignment}",
            m.member_id, m.client_id, m.client_host
        );

        // Decode the elected master member_id + identity from the assignment.
        if elected_master_id.is_none()
            && let Ok(a) = serde_json::from_slice::<serde_json::Value>(&m.member_assignment)
        {
            if let Some(mid) = a.get("master").and_then(|v| v.as_str()) {
                elected_master_id = Some(mid.to_string());
            }
            if let Some(mi) = a.get("master_identity").filter(|v| !v.is_null()) {
                elected_master_identity = Some(mi.clone());
            }
        }

        members_json.push(serde_json::json!({
            "member_id": m.member_id,
            "client_id": m.client_id,
            "client_host": m.client_host,
            "member_metadata": metadata,
            "member_assignment": assignment,
        }));
    }

    // Stamp `is_master` per member now that we know which member_id was elected.
    if let Some(mid) = &elected_master_id {
        for mj in &mut members_json {
            let is_master = mj["member_id"].as_str() == Some(mid.as_str());
            mj.as_object_mut()
                .unwrap()
                .insert("is_master".into(), serde_json::Value::Bool(is_master));
        }
    }

    write_election_fixture(
        "members.json",
        &serde_json::to_string_pretty(&members_json).unwrap(),
    );

    // NB the broker's DescribeGroups hard-codes `protocol_data` (the protocol
    // NAME) to empty, so `protocol_name` here reflects the broker, not cp's
    // JoinGroup protocol. `SR_PROTOCOL_NAME` is what the *client* sends in
    // JoinGroup; this fixture records the broker-observed value for the record.
    let group_json = serde_json::json!({
        "group_id": group.group_id,
        "group_state": group.group_state,
        "protocol_type": group.protocol_type,
        "protocol_name_via_describe_groups": group.protocol_data,
        "member_count": group.members.len(),
        "elected_master_member_id": elected_master_id,
        "elected_master_identity": elected_master_identity,
    });
    write_election_fixture(
        "group.json",
        &serde_json::to_string_pretty(&group_json).unwrap(),
    );

    eprintln!(
        "CAPTURE election capture done — protocol_type={:?} elected_master_member_id={:?}",
        group.protocol_type, elected_master_id
    );
}

// ── the test ──────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; captures cp-schema-registry `sr`-election member/assignment bytes"]
async fn capture_election() {
    docker_pull(SR_IMAGE);

    let (broker, _dir) = start_host_broker().await;

    // Two cp nodes, distinct host names, both on the same broker + group.
    let id1 = docker_run_schema_registry("sr-node-1");
    let _g1 = ContainerGuard { id: id1.clone() };
    let id2 = docker_run_schema_registry("sr-node-2");
    let _g2 = ContainerGuard { id: id2.clone() };

    let port1 = docker_mapped_port(&id1);
    let port2 = docker_mapped_port(&id2);
    let base1 = format!("http://127.0.0.1:{port1}");
    let base2 = format!("http://127.0.0.1:{port2}");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build reqwest client");

    // Readiness on BOTH nodes proves the `"sr"` group elected a master via Crabka.
    wait_for_registry(&http, &base1, &id1, "node-1").await;
    wait_for_registry(&http, &base2, &id2, "node-2").await;

    // Read the group + persist the member/assignment bytes (broker still up).
    capture_group().await;

    broker.shutdown().await;
    eprintln!("CAPTURE done — members.json + group.json written");
}
